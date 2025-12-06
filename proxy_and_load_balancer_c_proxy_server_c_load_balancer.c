/*
Combined project files in one document.

Files included below (copy each into its own .c file as named):
 - proxy_server.c   (your HTTP proxy with LRU cache)
 - load_balancer.c  (round-robin load balancer that forwards clients to proxy instances)
 - Makefile (simple)

Build instructions (example):
  gcc -pthread -o proxy_server proxy_server.c
  gcc -pthread -o load_balancer load_balancer.c

Run example:
  ./proxy_server 8081        # start proxy instance on 8081
  ./proxy_server 8082        # start second proxy on 8082
  ./load_balancer 8080 8081 8082   # load balancer on 8080 forwarding to proxies

Note: proxy_server.c depends on "proxy_parse.h" and its implementation (ParsedRequest API) which you already had in your project.
*/

/* ---------------------- proxy_server.c ---------------------- */

#include <stdio.h>
#include <sys/socket.h>
#include <semaphore.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string.h>
#include <netdb.h> // gethostbyname
#include <time.h>
#include "proxy_parse.h"

#define MAX_CLIENTS 10
#define MAX_BYTES 4096
#define MAX_SIZE 200*(1<<20)     //size of the cache (bytes)
#define MAX_ELEMENT_SIZE 10*(1<<20)     //max size of an element in cache

typedef struct cache_element cache_element;

struct cache_element {
    char *data;
    int len;
    char *url;
    time_t lru_time_track;
    cache_element *next;
};

cache_element *head = NULL;
int cache_size = 0;

cache_element *find_cache(char *url);
int add_cache_element(char *url, char *data, int len);
void remove_cache_element();

int port_number = 8080;
int proxy_socketId;

pthread_t tid[MAX_CLIENTS];
sem_t semaphore;
pthread_mutex_t cache_lock; // mutex for cache

int checkHTTPversion(char *msg)
{
    int version = -1;

    if(msg == NULL) return -1;
    if(strncmp(msg, "HTTP/1.1", 8) == 0)
    {
        version = 1;
    }
    else if(strncmp(msg, "HTTP/1.0", 8) == 0)            
    {
        version = 1;                                        
    }
    else
        version = -1;

    return version;
}

int connectRemoteServer(char *host_addr,int port){
    int remotesocket=socket(AF_INET,SOCK_STREAM,0);
    if(remotesocket<0){
        perror("Failed to create socket to remote server");
        return -1;
    }
    struct hostent* host=gethostbyname(host_addr);
    if(host==NULL){
        fprintf(stderr, "Failed to resolve hostname %s\n", host_addr);
        close(remotesocket);
        return -1;
    }
    struct sockaddr_in server_addr;
    bzero(&server_addr,sizeof(server_addr));
    server_addr.sin_family=AF_INET;
    server_addr.sin_port=htons(port);

    bcopy((char *)host->h_addr, (char *)&server_addr.sin_addr.s_addr, host->h_length);
    if(connect(remotesocket,(struct sockaddr *)&server_addr,sizeof(server_addr))<0){
        perror("Failed to connect to remote server");
        close(remotesocket);
        return -1;
    }
    return remotesocket;
}

int handle_request(int client_socketId, struct ParsedRequest *request, char *tempreq){
    char *buf=(char *)malloc(sizeof(char)*MAX_BYTES);
    if(!buf) return -1;
    memset(buf,0,MAX_BYTES);
    strcpy(buf,"GET");
    strcat(buf,request->path);
    strcat(buf," ");
    strcat(buf,request->version);
    strcat(buf,"\r\n");

    size_t len=strlen(buf);

    if(ParsedHeader_set(request,"Connection","close")<0){
        // continue even if set fails
    }

    if(ParsedHeader_get(request,"Host")==NULL){
        if(request->host && ParsedHeader_set(request,"Host",request->host)<0){
            // ignore
        }
    }

    if(ParsedRequest_unparse_headers(request,buf+len,MAX_BYTES-len)<0){
        // ignore
    }

    int server_port=80;
    if(request->port!=NULL){
        server_port=atoi(request->port);
    }
    int remote_socketid=connectRemoteServer(request->host,server_port);
    if(remote_socketid<0){
        free(buf);
        return -1;
    }
    int bytes_sent_server=send(remote_socketid,buf,strlen(buf),0);
    if(bytes_sent_server<0){
        perror("send to remote failed");
        free(buf);
        close(remote_socketid);
        return -1;
    }

    bzero(buf,MAX_BYTES);
    bytes_sent_server=recv(remote_socketid,buf,MAX_BYTES,0);
    char *temp_buffer=(char *)malloc(sizeof(char)*MAX_BYTES);
    if(!temp_buffer){
        free(buf);
        close(remote_socketid);
        return -1;
    }
    int temp_buffer_size=MAX_BYTES;
    int temp_buffer_index=0;
    while(bytes_sent_server>0){
        int to_send=bytes_sent_server;
        int sent_total=0;
        while(sent_total<to_send){
            int s=send(client_socketId, buf+sent_total, to_send-sent_total, 0);
            if(s<=0){
                perror("Failed to send data to client");
                free(buf);
                free(temp_buffer);
                close(remote_socketid);
                return -1;
            }
            sent_total += s;
        }
        // append to temp_buffer
        if(temp_buffer_index + bytes_sent_server > temp_buffer_size){
            temp_buffer_size = temp_buffer_index + bytes_sent_server + MAX_BYTES;
            temp_buffer = (char *)realloc(temp_buffer, temp_buffer_size);
            if(!temp_buffer){
                perror("realloc failed");
                free(buf);
                close(remote_socketid);
                return -1;
            }
        }
        memcpy(temp_buffer + temp_buffer_index, buf, bytes_sent_server);
        temp_buffer_index += bytes_sent_server;

        bzero(buf,MAX_BYTES);
        bytes_sent_server=recv(remote_socketid,buf,MAX_BYTES,0);
    }
    if(temp_buffer_index>=0){
        temp_buffer[temp_buffer_index]='\0';
    }
    add_cache_element(tempreq,temp_buffer,temp_buffer_index);
    free(buf);
    close(remote_socketid);
    // note: temp_buffer is freed inside cache management or on failure
    return 0;
}

void *thread_fn(void *socketnew){
    int local_socket = *((int *)socketnew);
    free(socketnew);

    sem_wait(&semaphore);
    int p;
    sem_getvalue(&semaphore,&p);
    printf("Thread %lu: Active connections: %d\n",(unsigned long)pthread_self(),MAX_CLIENTS-p);
    int client_socketId=local_socket;
    int bytes_send_client,len;
    char *buffer=calloc(1, MAX_BYTES);
    if(!buffer){
        close(client_socketId);
        sem_post(&semaphore);
        pthread_exit(NULL);
    }
    memset(buffer, 0, MAX_BYTES);
    bytes_send_client=recv(client_socketId, buffer, MAX_BYTES, 0);

    while(bytes_send_client>0){
        len=strlen(buffer);
        if(strstr(buffer,"\r\n\r\n")==NULL){
            int r=recv(client_socketId, buffer+bytes_send_client, MAX_BYTES-bytes_send_client, 0);
            if(r<=0) break;
            bytes_send_client+=r;
        }
        else{
            break;
        }
    }

    char *tempReq=(char *)malloc(strlen(buffer)+1);
    if(!tempReq){ free(buffer); sem_post(&semaphore); pthread_exit(NULL); }
    strcpy(tempReq,buffer);

    cache_element *cached = find_cache(tempReq);
    if(cached!=NULL){
        int size=cached->len;
        int pos=0;
        while(pos<size){
            int chunk = (size-pos) < MAX_BYTES ? (size-pos) : MAX_BYTES;
            int sent_total = 0;
            while(sent_total < chunk){
                int s = send(client_socketId, cached->data + pos + sent_total, chunk - sent_total, 0);
                if(s<=0) break;
                sent_total += s;
            }
            if(sent_total<=0) break;
            pos += chunk;
        }
        printf("Data served from cache to client\n");
    }
    else if(bytes_send_client>0){
        len=strlen(buffer);
        struct ParsedRequest *request=ParsedRequest_create();
        if(ParsedRequest_parse(request,buffer,len)<0){
            printf("Failed to parse request\n");
            close(client_socketId);
            free(buffer);
            free(tempReq);
            sem_post(&semaphore);
            ParsedRequest_destroy(request);
            pthread_exit(NULL);
        }
        else{
            bzero(buffer, MAX_BYTES);
            if(!strcmp(request->method,"GET")){
                if(request->host && request->path && checkHTTPversion(request->version)==1){
                    int rc = handle_request(client_socketId,request,tempReq);
                    if(rc<0){
                        printf("Failed to handle request\n");
                    }
                }
                else{
                     printf("Failed to handle request: missing host/path/version\n");
                }
            }
            else{
                printf("Method not supported\n");
            }
        }
        ParsedRequest_destroy(request);
    }
    else if(bytes_send_client==0){
        printf("Client disconnected\n");
    }
    else{
        printf("Failed to receive data from client\n");
    }
    shutdown(client_socketId, SHUT_RDWR);
    close(client_socketId);
    free(buffer);
    free(tempReq);
    sem_post(&semaphore);
    pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
    int client_socketId,client_len;
    struct sockaddr_in server_addr, client_addr;
    sem_init(&semaphore, 0, MAX_CLIENTS);
    pthread_mutex_init(&cache_lock, NULL);

    if(argc==2){
        port_number=atoi(argv[1]);
    }
    else{
        fprintf(stderr, "Usage: %s <port_number>\n", argv[0]);
        exit(1);
    }

    printf("Starting Proxy Server on port %d\n",port_number);

    proxy_socketId = socket(AF_INET, SOCK_STREAM, 0);
    if (proxy_socketId < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }
    if(setsockopt(proxy_socketId, SOL_SOCKET, SO_REUSEADDR, &(int){1}, sizeof(int)) < 0) {
        perror("setsockopt(SO_REUSEADDR) failed");
        exit(EXIT_FAILURE);
    }
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port_number);

    if (bind(proxy_socketId, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("Bind failed");
        close(proxy_socketId);
        exit(EXIT_FAILURE);
    }
    if (listen(proxy_socketId, MAX_CLIENTS) < 0) {
        perror("Listen failed");
        close(proxy_socketId);
        exit(EXIT_FAILURE);
    }
    int i=0;

    while (1) {
        client_len = sizeof(client_addr);
        client_socketId = accept(proxy_socketId, (struct sockaddr *)&client_addr, (socklen_t*)&client_len);
        if (client_socketId < 0) {
            perror("Accept failed");
            continue;
        }

        struct sockaddr_in *client_pt=(struct sockaddr_in *)&client_addr;
        struct in_addr ip_addr=client_pt->sin_addr;
        char str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET,&ip_addr,str,sizeof(str));
        printf("Connection accepted from %s:%d\n",str,ntohs(client_pt->sin_port));

        int *pclient = malloc(sizeof(int));
        if(!pclient){ close(client_socketId); continue; }
        *pclient = client_socketId;

        pthread_create(&tid[i % MAX_CLIENTS], NULL, thread_fn, pclient);
        pthread_detach(tid[i % MAX_CLIENTS]);
        i++;
    }
    close(proxy_socketId);
    return 0;
}

/* Cache functions */

cache_element* find_cache(char* url){
    cache_element* site=NULL;
    int temp_lock_val = pthread_mutex_lock(&cache_lock);
    (void)temp_lock_val;
    if(head!=NULL){
        site = head;
        while (site!=NULL)
        {
            if(!strcmp(site->url,url)){
                site->lru_time_track = time(NULL);
                break;
            }
            site=site->next;
        }       
    }
    temp_lock_val = pthread_mutex_unlock(&cache_lock);
    (void)temp_lock_val;
    return site;
}

int add_cache_element(char* url,char* data,int size){
    int temp_lock_val = pthread_mutex_lock(&cache_lock);
    (void)temp_lock_val;
    int element_size = size + 1 + strlen(url) + sizeof(cache_element);
    if(element_size>MAX_ELEMENT_SIZE){
        temp_lock_val = pthread_mutex_unlock(&cache_lock);
        (void)temp_lock_val;
        return 0;
    }
    else
    {
        while(cache_size+element_size>MAX_SIZE){
            remove_cache_element();
        }
        cache_element* element = (cache_element*) malloc(sizeof(cache_element));
        if(!element){ pthread_mutex_unlock(&cache_lock); return 0; }
        element->data= (char*)malloc(size+1);
        if(!element->data){ free(element); pthread_mutex_unlock(&cache_lock); return 0; }
        memcpy(element->data, data, size);
        element->data[size]='\0';
        element->url = (char*)malloc(1+( strlen( url )*sizeof(char)  ));
        if(!element->url){ free(element->data); free(element); pthread_mutex_unlock(&cache_lock); return 0; }
        strcpy( element->url, url );
        element->lru_time_track=time(NULL);
        element->next=head; 
        element->len=size;
        head=element;
        cache_size+=element_size;
        temp_lock_val = pthread_mutex_unlock(&cache_lock);
        (void)temp_lock_val;
        return 1;
    }
    return 0;
}

void remove_cache_element(){
    int temp_lock_val = pthread_mutex_lock(&cache_lock);
    (void)temp_lock_val;
    if( head != NULL) {
        cache_element *p = head;
        cache_element *q = head;
        cache_element *temp = head;
        for (q = head; q->next != NULL; q = q->next) {
            if((q->next)->lru_time_track < temp->lru_time_track){
                temp = q->next;
                p = q;
            }
        }
        if(temp == head) {
            head = head -> next;
        } else {
            p->next = temp->next; 
        }
        cache_size = cache_size - (temp -> len) - sizeof(cache_element) - strlen(temp -> url) - 1;
        free(temp->data);
        free(temp->url);
        free(temp);
    }
    temp_lock_val = pthread_mutex_unlock(&cache_lock);
    (void)temp_lock_val;
}

/* ---------------------- load_balancer.c ---------------------- */

/*
Simple round-robin load balancer in C.
Usage: ./load_balancer <listen_port> <proxy1_port> [proxy2_port] ...
Example: ./load_balancer 8080 8081 8082
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/select.h>

#define MAX_PROXIES 128
#define MAX_BUFFER 4096

typedef struct {
    char ip[INET_ADDRSTRLEN];
    int port;
    int active; // reserved for future health checks
    int connections; // optional counter
} backend_proxy;

backend_proxy proxies[MAX_PROXIES];
int proxy_count = 0;
int current_index = 0;
pthread_mutex_t rr_lock;

void *handle_client_lb(void *arg);
int connect_to_proxy(char *ip, int port);

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <listen_port> <proxy1_port> [proxy2_port] ...\n", argv[0]);
        return 1;
    }

    int listen_port = atoi(argv[1]);
    proxy_count = argc - 2;

    if(proxy_count > MAX_PROXIES) {
        fprintf(stderr, "Too many proxies (max %d)\n", MAX_PROXIES);
        return 1;
    }

    for (int i = 0; i < proxy_count; i++) {
        strncpy(proxies[i].ip, "127.0.0.1", INET_ADDRSTRLEN);
        proxies[i].port = atoi(argv[i + 2]);
        proxies[i].active = 1;
        proxies[i].connections = 0;
    }

    pthread_mutex_init(&rr_lock, NULL);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("Socket creation failed");
        exit(1);
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(listen_port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("Bind failed");
        close(server_fd);
        exit(1);
    }

    if (listen(server_fd, 128) < 0) {
        perror("Listen failed");
        close(server_fd);
        exit(1);
    }

    printf("✅ Load balancer started on port %d\n", listen_port);
    printf("🔁 Managing %d backend proxies:\n", proxy_count);
    for (int i = 0; i < proxy_count; i++)
        printf("  -> %s:%d\n", proxies[i].ip, proxies[i].port);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            perror("Accept failed");
            continue;
        }

        int *client_sock = malloc(sizeof(int));
        if(!client_sock){ close(client_fd); continue; }
        *client_sock = client_fd;
        pthread_t tid;
        pthread_create(&tid, NULL, handle_client_lb, client_sock);
        pthread_detach(tid);
    }

    close(server_fd);
    pthread_mutex_destroy(&rr_lock);
    return 0;
}

void *handle_client_lb(void *arg) {
    int client_fd = *((int*)arg);
    free(arg);

    pthread_mutex_lock(&rr_lock);
    int index = current_index;
    current_index = (current_index + 1) % proxy_count;
    pthread_mutex_unlock(&rr_lock);

    backend_proxy proxy = proxies[index];
    int proxy_fd = connect_to_proxy(proxy.ip, proxy.port);
    if (proxy_fd < 0) {
        fprintf(stderr, "Failed to connect to proxy %s:%d\n", proxy.ip, proxy.port);
        close(client_fd);
        return NULL;
    }

    // optional: increment connection counter
    // proxies[index].connections++;

    printf("Forwarding client fd=%d to proxy %s:%d\n", client_fd, proxy.ip, proxy.port);

    fd_set fds;
    char buffer[MAX_BUFFER];
    int maxfd = (client_fd > proxy_fd ? client_fd : proxy_fd) + 1;

    while (1) {
        FD_ZERO(&fds);
        FD_SET(client_fd, &fds);
        FD_SET(proxy_fd, &fds);

        int activity = select(maxfd, &fds, NULL, NULL, NULL);
        if (activity <= 0) break;

        if (FD_ISSET(client_fd, &fds)) {
            int bytes = recv(client_fd, buffer, MAX_BUFFER, 0);
            if (bytes <= 0) break;
            int sent_total = 0;
            while(sent_total < bytes){
                int s = send(proxy_fd, buffer + sent_total, bytes - sent_total, 0);
                if(s<=0) break;
                sent_total += s;
            }
            if(sent_total <= 0) break;
        }

        if (FD_ISSET(proxy_fd, &fds)) {
            int bytes = recv(proxy_fd, buffer, MAX_BUFFER, 0);
            if (bytes <= 0) break;
            int sent_total = 0;
            while(sent_total < bytes){
                int s = send(client_fd, buffer + sent_total, bytes - sent_total, 0);
                if(s<=0) break;
                sent_total += s;
            }
            if(sent_total <= 0) break;
        }
    }

    close(client_fd);
    close(proxy_fd);
    // optional: decrement connection counter
    // proxies[index].connections--;
    printf("Connection closed (client_fd=%d)\n", client_fd);
    return NULL;
}

int connect_to_proxy(char *ip, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip, &addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(sock);
        return -1;
    }
    return sock;
}

/* ---------------------- Makefile (optional) ---------------------- */
/*
all:
	gcc -pthread -o proxy_server proxy_server.c
	gcc -pthread -o load_balancer load_balancer.c

clean:
	rm -f proxy_server load_balancer
*/
