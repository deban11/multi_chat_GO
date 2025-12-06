#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <sys/wait.h>
#include <errno.h>
#include <pthread.h>

/* ---------------- CONFIGURATION ---------------- */
#define MAX_CLIENTS 20          // Thread Pool Size
#define QUEUE_SIZE 100          // Job Queue Size
#define MAX_BYTES 4096          // Buffer size for reading/writing
#define MAX_CACHE_SIZE 200 * (1 << 20) // 200MB Max Cache
#define MAX_ELEMENT_SIZE 10 * (1 << 20) // 10MB Max Object

/* ---------------- STRUCTURES ---------------- */

// 1. Linked List for Cache
typedef struct cache_element {
    char *data;             // The actual HTTP response body
    int len;                // Length of data
    char *url;              // The key (URL)
    time_t lru_time_track;  // For LRU Eviction
    struct cache_element *next;
} cache_element;

// 2. Parsed Request Info (Replaces proxy_parse.h)
struct RequestInfo {
    char *method;
    char *host;
    char *port;
    char *path;
    char *version;
};

/* ---------------- GLOBALS ---------------- */

// Cache Globals
cache_element *head = NULL;
int current_cache_size = 0;
pthread_rwlock_t cache_lock; // READ-WRITE LOCK (The Fix)

// Thread Pool Globals
pthread_t thread_pool[MAX_CLIENTS];
pthread_mutex_t queue_lock;
pthread_cond_t condition_var;

// Job Queue
int client_socket_queue[QUEUE_SIZE];
int q_front = 0, q_rear = 0, q_count = 0;

/* ---------------- FUNCTION PROTOTYPES ---------------- */
void *worker_thread(void *arg);
void handle_connection(int client_socket);
int connect_remote_server(char *host, int port);

// Cache Functions
int add_cache_element(char *url, char *data, int len);
cache_element *find_and_copy(char *url); // Returns a COPY to prevent race conditions
void remove_cache_element(); // Internal helper

// Queue Functions
void enqueue(int client_socket);
int dequeue();

// Utility
struct RequestInfo *parse_request(char *buffer);
void free_request_info(struct RequestInfo *req);

/* ---------------- MAIN ---------------- */
int main(int argc, char *argv[]) {
    int port_number = 8080;
    if (argc == 2) port_number = atoi(argv[1]);

    printf("Starting Proxy Server on port %d...\n", port_number);

    // 1. Initialize Synchronization Primitives
    pthread_mutex_init(&queue_lock, NULL);
    pthread_cond_init(&condition_var, NULL);
    pthread_rwlock_init(&cache_lock, NULL); // Init the RWLock

    // 2. Create Thread Pool
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (pthread_create(&thread_pool[i], NULL, worker_thread, NULL) != 0) {
            perror("Failed to create thread");
            exit(1);
        }
    }

    // 3. Setup Server Socket
    int proxy_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (proxy_socket < 0) { perror("Socket failed"); exit(1); }

    int reuse = 1;
    setsockopt(proxy_socket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port_number);

    if (bind(proxy_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed"); exit(1);
    }
    if (listen(proxy_socket, QUEUE_SIZE) < 0) {
        perror("Listen failed"); exit(1);
    }

    // 4. Accept Loop (Producer)
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);

    while (1) {
        int client_fd = accept(proxy_socket, (struct sockaddr *)&client_addr, &client_len);
        if (client_fd < 0) {
            perror("Accept failed");
            continue;
        }
        enqueue(client_fd);
    }

    return 0;
}

/* ---------------- THREAD POOL & QUEUE ---------------- */
void enqueue(int client_socket) {
    pthread_mutex_lock(&queue_lock);
    if (q_count == QUEUE_SIZE) {
        printf("Queue Full. Dropping connection.\n");
        close(client_socket);
    } else {
        client_socket_queue[q_rear] = client_socket;
        q_rear = (q_rear + 1) % QUEUE_SIZE;
        q_count++;
        pthread_cond_signal(&condition_var);
    }
    pthread_mutex_unlock(&queue_lock);
}

int dequeue() {
    // Caller must hold lock
    int socket = -1;
    if (q_count > 0) {
        socket = client_socket_queue[q_front];
        q_front = (q_front + 1) % QUEUE_SIZE;
        q_count--;
    }
    return socket;
}

void *worker_thread(void *arg) {
    while (1) {
        pthread_mutex_lock(&queue_lock);
        while (q_count == 0) {
            pthread_cond_wait(&condition_var, &queue_lock);
        }
        int client_fd = dequeue();
        pthread_mutex_unlock(&queue_lock);

        if (client_fd != -1) {
            handle_connection(client_fd);
        }
    }
}

/* ---------------- CACHE LOGIC (The "OS" Core) ---------------- */

// Helper: Finds the oldest element. Caller must hold Write Lock.
void remove_cache_element() {
    cache_element *p, *q, *temp;
    if (head != NULL) {
        // Find oldest based on lru_time_track
        for (q = head, p = head, temp = head; q->next != NULL; q = q->next) {
            if (((q->next)->lru_time_track) < (temp->lru_time_track)) {
                temp = q->next;
                p = q;
            }
        }
        
        // Unlink temp
        if (temp == head) head = head->next;
        else p->next = temp->next;

        current_cache_size -= (temp->len + sizeof(cache_element) + strlen(temp->url) + 1);
        
        free(temp->data);
        free(temp->url);
        free(temp);
    }
}

int add_cache_element(char *url, char *data, int len) {
    // 1. Acquire WRITE LOCK - Only one thread can write
    pthread_rwlock_wrlock(&cache_lock);

    int element_size = len + 1 + strlen(url) + sizeof(cache_element);
    
    if (element_size > MAX_ELEMENT_SIZE) {
        pthread_rwlock_unlock(&cache_lock);
        return 0;
    }

    // Eviction Policy
    while (current_cache_size + element_size > MAX_CACHE_SIZE) {
        remove_cache_element();
    }

    cache_element *element = (cache_element *)malloc(sizeof(cache_element));
    element->data = (char *)malloc(len + 1);
    memcpy(element->data, data, len);
    element->data[len] = '\0';
    
    element->url = strdup(url);
    element->lru_time_track = time(NULL);
    element->len = len;

    // Insert at Head
    element->next = head;
    head = element;
    current_cache_size += element_size;

    // 2. Release WRITE LOCK
    pthread_rwlock_unlock(&cache_lock);
    return 1;
}

// THE FIX: Returns a dynamically allocated COPY of the element
cache_element *find_and_copy(char *url) {
    cache_element *copy = NULL;

    // 1. Acquire READ LOCK - Multiple threads can be here
    pthread_rwlock_rdlock(&cache_lock);

    cache_element *ptr = head;
    while (ptr != NULL) {
        if (strcmp(ptr->url, url) == 0) {
            // FOUND!
            // Update LRU (Technically a write, but often ignored for performance in loose LRU)
            // Ideally, we'd use an atomic here or just ignore exact precision.
            ptr->lru_time_track = time(NULL);

            // DEEP COPY STRATEGY
            // We copy data inside the lock, so we are safe if it gets deleted later.
            copy = (cache_element *)malloc(sizeof(cache_element));
            copy->data = (char *)malloc(ptr->len + 1);
            memcpy(copy->data, ptr->data, ptr->len);
            copy->data[ptr->len] = '\0';
            copy->len = ptr->len;
            copy->url = NULL; // We don't need the URL for the copy sent to client
            copy->next = NULL;
            break;
        }
        ptr = ptr->next;
    }

    // 2. Release READ LOCK
    pthread_rwlock_unlock(&cache_lock);
    return copy;
}

/* ---------------- PROXY LOGIC ---------------- */
void handle_connection(int client_fd) {
    char *buffer = (char *)malloc(MAX_BYTES);
    if (!buffer) { close(client_fd); return; }
    memset(buffer, 0, MAX_BYTES);

    int bytes_read = recv(client_fd, buffer, MAX_BYTES, 0);
    if (bytes_read <= 0) {
        close(client_fd);
        free(buffer);
        return;
    }

    // 1. Parse Request
    struct RequestInfo *req = parse_request(buffer);
    if (req == NULL) {
        printf("Bad Request\n");
        close(client_fd);
        free(buffer);
        return;
    }

    // Construct URL Key for Cache
    char url[1024];
    snprintf(url, sizeof(url), "%s%s", req->host, req->path);
    
    // 2. CHECK CACHE
    cache_element *cached_res = find_and_copy(url);
    
    if (cached_res != NULL) {
        // CACHE HIT
        printf("[CACHE HIT] %s\n", url);
        send(client_fd, cached_res->data, cached_res->len, 0);
        free(cached_res->data);
        free(cached_res); // Free the COPY
    } 
    else {
        // CACHE MISS
        printf("[CACHE MISS] %s\n", url);
        
        // Prepare request to remote
        int remote_port = (req->port) ? atoi(req->port) : 80;
        int remote_fd = connect_remote_server(req->host, remote_port);

        if (remote_fd >= 0) {
            // Forward Request
            send(remote_fd, buffer, bytes_read, 0);

            // Receive Response & Cache it
            char *response_buf = (char *)malloc(MAX_ELEMENT_SIZE);
            int total_len = 0;
            int current_max = MAX_ELEMENT_SIZE;
            int n;

            while ((n = recv(remote_fd, buffer, MAX_BYTES, 0)) > 0) {
                // Stream to client
                send(client_fd, buffer, n, 0);
                
                // Store in buffer for cache (with boundary check)
                if (total_len + n < current_max) {
                    memcpy(response_buf + total_len, buffer, n);
                    total_len += n;
                }
            }
            
            // Add to Cache
            add_cache_element(url, response_buf, total_len);
            
            free(response_buf);
            close(remote_fd);
        } else {
            printf("Failed to connect to remote\n");
        }
    }

    free_request_info(req);
    free(buffer);
    close(client_fd);
}

int connect_remote_server(char *host, int port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) return -1;

    struct hostent *server = gethostbyname(host);
    if (server == NULL) {
        close(sockfd);
        return -1;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    bcopy((char *)server->h_addr, (char *)&serv_addr.sin_addr.s_addr, server->h_length);
    serv_addr.sin_port = htons(port);

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        close(sockfd);
        return -1;
    }
    return sockfd;
}

/* ---------------- PARSING HELPER ---------------- */
struct RequestInfo *parse_request(char *buffer) {
    struct RequestInfo *req = malloc(sizeof(struct RequestInfo));
    if (!req) return NULL;
    memset(req, 0, sizeof(struct RequestInfo));

    char *buf_copy = strdup(buffer);
    char *line = strtok(buf_copy, "\r\n"); // First Line
    
    // Parse "GET http://host:port/path HTTP/1.1"
    char *method = strtok(line, " ");
    char *full_url = strtok(NULL, " ");
    char *version = strtok(NULL, " ");

    if (!method || !full_url || !version) {
        free(buf_copy); free(req); return NULL;
    }

    req->method = strdup(method);
    req->version = strdup(version);

    // Handle http://
    char *url_ptr = full_url;
    if (strncmp(url_ptr, "http://", 7) == 0) url_ptr += 7;

    // Split Host and Path
    char *path_start = strchr(url_ptr, '/');
    if (path_start) {
        req->path = strdup(path_start);
        *path_start = '\0'; // Terminate host string
    } else {
        req->path = strdup("/");
    }

    // Split Host and Port
    char *port_start = strchr(url_ptr, ':');
    if (port_start) {
        req->port = strdup(port_start + 1);
        *port_start = '\0';
        req->host = strdup(url_ptr);
    } else {
        req->port = NULL;
        req->host = strdup(url_ptr);
    }

    free(buf_copy);
    return req;
}

void free_request_info(struct RequestInfo *req) {
    if (req) {
        if (req->method) free(req->method);
        if (req->host) free(req->host);
        if (req->port) free(req->port);
        if (req->path) free(req->path);
        if (req->version) free(req->version);
        free(req);
    }
}

