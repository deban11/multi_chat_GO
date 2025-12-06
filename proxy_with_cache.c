#include <stdio.h>
#include <sys/socket.h>
#include <semaphore.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <string.h>
#include <netdb.h> // Include this header for gethostbyname
#include "proxy_parse.h"

#define MAX_CLIENTS 10
#define MAX_BYTES 4096
#define MAX_SIZE 200*(1<<20)     //size of the cache
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

cache_element *find(char *url);
int add_cache_element(char *url, char *data, int len);
void remove_cache_element();

int port_number = 8080;

int proxy_socketId;

pthread_t tid[MAX_CLIENTS];

sem_t semaphore;

pthread_mutex_t cache_lock; // Declare the mutex variable


int checkHTTPversion(char *msg)
{
	int version = -1;

	if(strncmp(msg, "HTTP/1.1", 8) == 0)
	{
		version = 1;
	}
	else if(strncmp(msg, "HTTP/1.0", 8) == 0)			
	{
		version = 1;										// Handling this similar to version 1.1
	}
	else
		version = -1;

	return version;
}

int connectRemoteServer(char *host_addr,int port){
    int remotesocket=socket(AF_INET,SOCK_STREAM,0);
    if(remotesocket<0){
        printf("Failed to create socket to remote server\n");
        return -1;
    }
    struct hostent* host=gethostbyname(host_addr);
    if(host==NULL){
        printf("Failed to resolve hostname\n");
        close(remotesocket);
        return -1;
    }
    struct sockaddr_in server_addr;
    bzero(&server_addr,sizeof(server_addr));
    server_addr.sin_family=AF_INET;
    server_addr.sin_port=htons(port);
    struct in_addr addr;

    bcopy((char *)host->h_addr, (char *)&server_addr.sin_addr.s_addr, host->h_length);
    if(connect(remotesocket,(struct sockaddr *)&server_addr,sizeof(server_addr))<0){
        printf("Failed to connect to remote server\n");
        close(remotesocket);
        return -1;
    }
    return remotesocket;
    


}

int handle_request(int client_socketId, struct ParsedRequest *request, char *tempreq){
    char *buf=(char *)malloc(sizeof(char)*MAX_BYTES);
    memset(buf,0,MAX_BYTES);
    strcpy(buf,"GET");
    strcat(buf,request->path);
    strcat(buf," ");
    strcat(buf,request->version);
    strcat(buf,"\r\n");

    size_t len=strlen(buf);

    if(ParsedHeader_set(request,"Connection","close")<0){
        printf("Failed to set header\n");
    }

    if(ParsedHeader_get(request,"Host")==NULL){
        if(ParsedHeader_set(request,"Host",request->host)<0){
            printf("Failed to set header\n");
        }
    }

    if(ParsedRequest_unparse_headers(request,buf+len,MAX_BYTES-len)<0){
        printf("Failed to unparse headers\n");
       
    }

    int server_port=80;
    if(request->port!=NULL){
        server_port=atoi(request->port);
    }
    int remote_socketid=connectRemoteServer(request->host,server_port);
    if(remote_socketid<0){
        printf("Failed to connect to remote server\n");
        free(buf);
        return -1;
    }
    int bytes_sent_server=send(remote_socketid,buf,strlen(buf),0);
    bzero(buf,MAX_BYTES);
    bytes_sent_server=recv(remote_socketid,buf,MAX_BYTES,0);
    char *temp_buffer=(char *)malloc(sizeof(char)*MAX_BYTES);
    int temp_buffer_size=MAX_BYTES;
    int temp_buffer_index=0;
    while(bytes_sent_server>0){
        bytes_sent_server=send(client_socketId,buf,bytes_sent_server,0);
        for(int i=0;i<bytes_sent_server/sizeof(char) && temp_buffer_index<temp_buffer_size;i++,temp_buffer_index++){
            temp_buffer[temp_buffer_index]=buf[i];
        }
        temp_buffer_size+=MAX_BYTES;
        temp_buffer=(char *)realloc(temp_buffer,sizeof(char)*temp_buffer_size);
        if(bytes_sent_server<0){
            printf("Failed to send data to client\n");
            free(buf);
            free(temp_buffer);
            close(remote_socketid);
            return -1;
        }
        bzero(buf,MAX_BYTES);
        bytes_sent_server=recv(remote_socketid,buf,MAX_BYTES,0);




    }
    temp_buffer[temp_buffer_index]='\0';
    printf("Data received from remote server and sent to client\n");
    add_cache_element(temp_buffer,tempreq,strlen(temp_buffer));
    free(buf);
    close(remote_socketid);
    return 0;


    

}

void *thread_fn(void *socketnew){
    sem_wait(&semaphore);
    int p;
    sem_getvalue(&semaphore,&p);
    printf("Thread %ld: Active connections: %d\n",pthread_self(),MAX_CLIENTS-p);
    int client_socketId=*((int *)socketnew);
    int bytes_send_client,len;
    char *buffer=calloc(1, MAX_BYTES);
    memset(buffer, 0, MAX_BYTES);
    bytes_send_client=recv(client_socketId, buffer, MAX_BYTES, 0);

    while(bytes_send_client>0){
        len=strlen(buffer);
        if(strstr(buffer,"\r\n\r\n")==NULL){
            bytes_send_client+=recv(client_socketId, buffer+bytes_send_client, MAX_BYTES-len, 0);
        }
        else{
            break;
        }
    }

    char *tempReq=(char *)malloc(strlen(buffer)*sizeof(char));
    strcpy(tempReq,buffer);
    struct cache_element *temp=find(tempReq);
    if(temp!=NULL){
        int size=temp->len/sizeof(char);
        int pos=0;
        char response[MAX_BYTES];
        while(pos<size){
            bzero(response, MAX_BYTES);
            for(int i=0;i<MAX_BYTES && pos<size;i++,pos++){
                response[i]=temp->data[pos];
            }
            send(client_socketId,response,MAX_BYTES,0);



        }
        printf("Data received from cache and sent to client\n");
        printf("%s\n",response);
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
            pthread_exit(NULL);
        }
        else{
            bzero(buffer, MAX_BYTES);
            if(!strcmp(request->method,"GET")){
                if(request->host && request->path && checkHTTPversion(request->version)==1){
                    bytes_send_client=handle_request(client_socketId,request,tempReq);
                    if(bytes_send_client>0){
                        add_cache_element(tempReq,buffer,bytes_send_client);
                    }
                    else{
                        printf("Failed to handle request\n");
                    }

                }
                else{
                     printf("Failed to handle request\n");
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

    //  /proxy <port_number>
    if(argv==2){
        port_number=atoi(argv[1]);
        
    }
    else{
        printf("TOO few ARGS\n");
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
    int Connected_socket_id[MAX_CLIENTS];

    while (1) {
        client_len = sizeof(client_addr);
        client_socketId = accept(proxy_socketId, (struct sockaddr *)&client_addr, (socklen_t*)&client_len);
        if (client_socketId < 0) {
            perror("Accept failed");
            exit(1);
            
        }
        else{
            Connected_socket_id[i]=client_socketId;
        }
        sem_wait(&semaphore);
        Connected_socket_id[i]=client_socketId;

        struct sockaddr_in *client_pt=(struct sockaddr_in *)&client_addr;
        struct in_addr ip_addr=client_pt->sin_addr;
        char str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET,&ip_addr,str,sizeof(str));
        printf("Connection accepted from %s:%d\n",str,ntohs(client_pt->sin_port));



        pthread_create(&tid[i], NULL, thread_fn, &Connected_socket_id[i]);
        i++;
    }
    close(proxy_socketId);
    return 0;
}

cache_element* find(char* url){
// Checks for url in the cache if found returns pointer to the respective cache element or else returns NULL
    cache_element* site=NULL;
	//sem_wait(&cache_lock);
    int temp_lock_val = pthread_mutex_lock(&cache_lock);
	printf("Remove Cache Lock Acquired %d\n",temp_lock_val); 
    if(head!=NULL){
        site = head;
        while (site!=NULL)
        {
            if(!strcmp(site->url,url)){
				printf("LRU Time Track Before : %ld", site->lru_time_track);
                printf("\nurl found\n");
				// Updating the time_track
				site->lru_time_track = time(NULL);
				printf("LRU Time Track After : %ld", site->lru_time_track);
				break;
            }
            site=site->next;
        }       
    }
	else {
    printf("\nurl not found\n");
	}
	//sem_post(&cache_lock);
    temp_lock_val = pthread_mutex_unlock(&cache_lock);
	printf("Remove Cache Lock Unlocked %d\n",temp_lock_val); 
    return site;
}

int add_cache_element(char* url,char* data,int size){
    // Adds element to the cache
	// sem_wait(&cache_lock);
    int temp_lock_val = pthread_mutex_lock(&cache_lock);
	printf("Add Cache Lock Acquired %d\n", temp_lock_val);
    int element_size=size+1+strlen(url)+sizeof(cache_element); // Size of the new element which will be added to the cache
    if(element_size>MAX_ELEMENT_SIZE){
		//sem_post(&cache_lock);
        // If element size is greater than MAX_ELEMENT_SIZE we don't add the element to the cache
        temp_lock_val = pthread_mutex_unlock(&cache_lock);
		printf("Add Cache Lock Unlocked %d\n", temp_lock_val);
		// free(data);
		// printf("--\n");
		// free(url);
        return 0;
    }
    else
    {   while(cache_size+element_size>MAX_SIZE){
            // We keep removing elements from cache until we get enough space to add the element
            remove_cache_element();
        }
        cache_element* element = (cache_element*) malloc(sizeof(cache_element)); // Allocating memory for the new cache element
        element->data= (char*)malloc(size+1); // Allocating memory for the response to be stored in the cache element
		strcpy(element->data,data); 
        element -> url = (char*)malloc(1+( strlen( url )*sizeof(char)  )); // Allocating memory for the request to be stored in the cache element (as a key)
		strcpy( element -> url, url );
		element->lru_time_track=time(NULL);    // Updating the time_track
        element->next=head; 
        element->len=size;
        head=element;
        cache_size+=element_size;
        temp_lock_val = pthread_mutex_unlock(&cache_lock);
		printf("Add Cache Lock Unlocked %d\n", temp_lock_val);
		//sem_post(&cache_lock);
		// free(data);
		// printf("--\n");
		// free(url);
        return 1;
    }
    return 0;
}

void remove_cache_element(){
    // If cache is not empty searches for the node which has the least lru_time_track and deletes it
    cache_element * p ;  	// Cache_element Pointer (Prev. Pointer)
	cache_element * q ;		// Cache_element Pointer (Next Pointer)
	cache_element * temp;	// Cache element to remove
    //sem_wait(&cache_lock);
    int temp_lock_val = pthread_mutex_lock(&cache_lock);
	printf("Remove Cache Lock Acquired %d\n",temp_lock_val); 
	if( head != NULL) { // Cache != empty
		for (q = head, p = head, temp =head ; q -> next != NULL; 
			q = q -> next) { // Iterate through entire cache and search for oldest time track
			if(( (q -> next) -> lru_time_track) < (temp -> lru_time_track)) {
				temp = q -> next;
				p = q;
			}
		}
		if(temp == head) { 
			head = head -> next; /*Handle the base case*/
		} else {
			p->next = temp->next;	
		}
		cache_size = cache_size - (temp -> len) - sizeof(cache_element) - 
		strlen(temp -> url) - 1;     //updating the cache size
		free(temp->data);     		
		free(temp->url); // Free the removed element 
		free(temp);
	} 
	//sem_post(&cache_lock);
    temp_lock_val = pthread_mutex_unlock(&cache_lock);
	printf("Remove Cache Lock Unlocked %d\n",temp_lock_val); 
}
