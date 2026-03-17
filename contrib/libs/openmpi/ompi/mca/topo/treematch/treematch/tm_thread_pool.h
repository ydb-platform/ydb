#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <pthread.h>
#include <hwloc.h>


typedef struct _work_t{
  int nb_args;
  void (*task)(int nb_args, void **args, int thread_id);
  void **args;
  struct _work_t *next;
  pthread_cond_t work_done;
  pthread_mutex_t mutex;
  int done;
  int thread_id;
}work_t;

typedef struct {
  int id;
  hwloc_topology_t topology;
  work_t *working_list;
  pthread_cond_t *cond_var;
  pthread_mutex_t *list_lock;
}local_thread_t;


typedef struct _thread_pool_t{
  int nb_threads;
  pthread_t *thread_list;
  work_t *working_list;
  pthread_cond_t *cond_var;
  pthread_mutex_t *list_lock;
  local_thread_t *local;
  hwloc_topology_t topology;
}thread_pool_t;

int get_nb_threads(void);
int submit_work(work_t *work, int thread_id);
void wait_work_completion(work_t *work);
void terminate_thread_pool(void);
work_t *create_work(int nb_args, void **args, void (int, void **, int));
int test_main(void);




#endif /* THREAD_POOL_H */
