/**
 * threadpool.c
 *
 * This file will contain your implementation of a threadpool.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

#include "threadpool.h"

// _threadpool is the internal threadpool structure that is
// cast to type "threadpool" before it given out to callers


 
 

typedef struct job {
  dispatch_fn function_given;
  void * arguement;
  struct job * next_j;
} job;


typedef struct _threadpool_st {
  pthread_t * ListOfThreads;
  volatile int threads_left;
  volatile int job_queue_sz;
  volatile int threads_destroyed;
  job * head;
  job * tail;
  int totalNumThread;
  pthread_cond_t last_thread_exit;
  pthread_cond_t doWork;
  pthread_cond_t allThreadsAreWorking;
  pthread_mutex_t mutex;
  pthread_cond_t noThreadsAreWorking;

  char requestDestroy;

} _threadpool;


void *worker_thread(void *args) {
  _threadpool *pool = (_threadpool *) args;

    while (1) {

       // wait for a signal


        if (pthread_mutex_lock(&(pool->mutex))!= 0) {
          perror("mutex lock failed");
          exit(-1);
        }
        
        //every thread comes here before a signal.
        if (pool->job_queue_sz <= 0) {
           pthread_cond_wait(&(pool->doWork),&(pool->mutex));
        }
        
        if (pool->requestDestroy == 't' && pool->job_queue_sz == 0) {
          //printf("thread leaving\n");
          pthread_mutex_unlock(&pool->mutex);
           
          break;
        }

        job * j; 

        j = pool->head;

       if (pool->job_queue_sz == 0) {
          pool->head = NULL;
          pool->tail = NULL;

          }
       else {
          pool->head = j->next_j;
        }
        
        pool->job_queue_sz--;
        
        if (pthread_mutex_unlock(&(pool->mutex))) {
          perror("mutex unlock failed");
          exit(-1);
        }
        j->function_given(j->arguement);
        
        pthread_mutex_lock(&pool->mutex);
        free(j);
        pool->threads_left++;
        if (pool->threads_left==1) {
          pthread_mutex_unlock(&pool->mutex); 
          pthread_cond_signal(&pool->allThreadsAreWorking);
        }
        
        else if (pool->threads_left == pool->totalNumThread && pool->requestDestroy=='t' && pool->job_queue_sz==0) {
          //printf("Last guy done\n");
          //tell destroy pool that last guy has finished it's work
          pthread_cond_signal(&pool->noThreadsAreWorking);
          pthread_mutex_unlock(&pool->mutex);
          break;
          
          //break;          
        }
        else {
          pthread_mutex_unlock(&pool->mutex); 
        }
        //pthread_mutex_unlock(&pool->mutex); 

  }

  
  pthread_mutex_lock(&pool->mutex);
  pool->totalNumThread--;
  printf("leaving\n");
  if (pool->totalNumThread==0) {
    //printf("total num thread left: %d\n",pool->totalNumThread);

    pthread_cond_signal(&pool->last_thread_exit);

  }
   pthread_mutex_unlock(&pool->mutex);

   
  
  return NULL;
  }



threadpool create_threadpool(int num_threads_in_pool) {
  _threadpool *pool;
  int i;

  // sanity check the argument
  if ((num_threads_in_pool <= 0) || (num_threads_in_pool > MAXT_IN_POOL)) {
      return NULL;
  }
  //initialize _threadpool
  pool = (_threadpool *) malloc(sizeof(_threadpool));
  if (pool == NULL) {
    fprintf(stderr, "Out of memory creating a new threadpool!\n");
    return NULL;
  }

  //initialize _threadpool struct attributes
  pool->threads_left = num_threads_in_pool;
  pool->totalNumThread = num_threads_in_pool;
  pool->job_queue_sz = 0;
  pool->head = NULL;
  pool->tail = NULL;
  pool->requestDestroy = 'f';


  if(pthread_cond_init(&(pool->last_thread_exit),NULL)) {
    fprintf(stderr, "error with init and condvar!\n");  
  return NULL;
  }

   if(pthread_cond_init(&(pool->noThreadsAreWorking),NULL)) {
    fprintf(stderr, "error with init and condvar!\n");  
  return NULL;
  }

 
  if(pthread_cond_init(&(pool->doWork),NULL)) {
    fprintf(stderr, "error with init and condvar!\n");  
  return NULL;
  }
  if(pthread_cond_init(&(pool->allThreadsAreWorking),NULL)) {
    fprintf(stderr, "error with init and condvar!\n");  
  return NULL;
  }

  if(pthread_mutex_init(&pool->mutex,NULL)) {
    fprintf(stderr, "error with init and mutex!\n");
  return NULL;
  }
  
  //make a list of threads
  pool->ListOfThreads = (pthread_t*) malloc (sizeof(pthread_t) * num_threads_in_pool);
  //create each of the threads
  for (i=0;i < num_threads_in_pool;i++) {
    if (pthread_create(&(pool->ListOfThreads[i]), NULL,&worker_thread, pool) != 0) {
      perror("thread creation failed:");
      exit(-1);
    }
  }
  

  return (threadpool) pool;
}

void dispatch(threadpool from_me, dispatch_fn dispatch_to_here,
        void *arg) {
    _threadpool *pool = (_threadpool *) from_me;
    int jqs; 
    int tl;
    job * j; 
    j = (job*) malloc(sizeof(job));

    // add your code here to dispatch a thread
    //dispatch a thread by calling the desired function inside worker_thread;

    //critical section
    if (pthread_mutex_lock(&(pool->mutex)) != 0) {
        perror("mutex lock failed");
        exit(-1);
    }
   
    tl = pool->threads_left;
    jqs = pool->job_queue_sz;
    j->function_given = dispatch_to_here;
    j->arguement = arg;
    j->next_j = NULL;

    pool->job_queue_sz++;
    pool->threads_left--;
    
    if (jqs==0) {
      pool->head = j;
      pool->tail = j;
    }
    else {
      pool->tail->next_j = j;
      pool->tail = j;
    }
    
    pthread_cond_signal(&(pool->doWork));
    
    
    if (pool->threads_left <= 0) {
      pthread_cond_wait(&pool->allThreadsAreWorking,&pool->mutex);
    }

    pthread_mutex_unlock(&pool->mutex);
}

void destroy_threadpool(threadpool destroyme) {
  _threadpool *pool = (_threadpool *) destroyme;
  // add your code here to kill a threadpool

  pthread_mutex_lock(&pool->mutex);
  pool->requestDestroy = 't';
  //wait for all threads to finish it's work
  if (pool->totalNumThread > pool->threads_left) {
    //printf("wait\n");
    pthread_cond_wait(&pool->noThreadsAreWorking,&pool->mutex);
  }
  //printf("last guy is out");
  //the last guy has finished it's work now

  
  
  pthread_mutex_unlock(&pool->mutex);
 
  pthread_cond_broadcast(&pool->doWork);
  //wait for broadcast to finnish
  pthread_mutex_lock(&pool->mutex);
  pthread_cond_wait(&pool->last_thread_exit, &pool->mutex);
  pthread_mutex_unlock(&pool->mutex);
  printf("detach\n");
  int i;
  for (i=0;i < pool->totalNumThread;i++) {
    pthread_detach(pool->ListOfThreads[i]);

  }
  for (i=0;i < pool->totalNumThread;i++) {
    free(pool->ListOfThreads[i]);
  }
  
  free(pool);
  
  
}

