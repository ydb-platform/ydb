#include <pthread.h>
#include "tm_thread_pool.h"
#include "tm_verbose.h"
#include <hwloc.h>
#include "tm_verbose.h"
#include "tm_tree.h"
#include <errno.h>
#include <limits.h>

typedef enum _mapping_policy {COMPACT, SCATTER} mapping_policy_t;

static mapping_policy_t mapping_policy = COMPACT;
static int verbose_level = ERROR;
static thread_pool_t *pool = NULL;
static unsigned int max_nb_threads = INT_MAX;

static thread_pool_t *get_thread_pool(void);
static void execute_work(work_t *work);
static int bind_myself_to_core(hwloc_topology_t topology, int id);
static void *thread_loop(void *arg);
static void add_work(pthread_mutex_t *list_lock, pthread_cond_t *cond_var, work_t *working_list, work_t *work);
static thread_pool_t *create_threads(void);

static void f1 (int nb_args, void **args, int thread_id);
static void f2 (int nb_args, void **args, int thread_id);
static void destroy_work(work_t *work);

#define MIN(a, b) ((a)<(b)?(a):(b))
#define MAX(a, b) ((a)>(b)?(a):(b))



void tm_set_max_nb_threads(unsigned int val){
  max_nb_threads = val;
}

void execute_work(work_t *work){
  work->task(work->nb_args, work->args, work->thread_id);
}

int bind_myself_to_core(hwloc_topology_t topology, int id){
  hwloc_cpuset_t cpuset;
  hwloc_obj_t obj;
  char *str;
  int binding_res;
  int depth = hwloc_topology_get_depth(topology);
  int nb_cores = hwloc_get_nbobjs_by_depth(topology, depth-1);
  int my_core;
  int nb_threads = get_nb_threads();
  /* printf("depth=%d\n",depth); */

  switch (mapping_policy){
  case SCATTER:
    my_core = id*(nb_cores/nb_threads);
    break;
  default:
    if(verbose_level>=WARNING){
      printf("Wrong scheduling policy. Using COMPACT\n");
    }
  case COMPACT:
    my_core = id%nb_cores;
  }

    if(verbose_level>=INFO){
       printf("Mapping thread %d on core %d\n",id,my_core);
   }

    /* Get my core. */
    obj = hwloc_get_obj_by_depth(topology, depth-1, my_core);
    if (obj) {
      /* Get a copy of its cpuset that we may modify. */
      cpuset = hwloc_bitmap_dup(obj->cpuset);

      /* Get only one logical processor (in case the core is
	 SMT/hyperthreaded). */
      hwloc_bitmap_singlify(cpuset);


      /*hwloc_bitmap_asprintf(&str, cpuset);
      printf("Binding thread %d to cpuset %s\n", my_core,str);
      FREE(str);
      */

      /* And try  to bind ourself there. */
      binding_res = hwloc_set_cpubind(topology, cpuset, HWLOC_CPUBIND_THREAD);
      if (binding_res == -1){
	int error = errno;
	hwloc_bitmap_asprintf(&str, obj->cpuset);
	if(verbose_level>=WARNING)
	  printf("Thread %d couldn't bind to cpuset %s: %s.\n This thread is not bound to any core...\n", my_core, str, strerror(error));
	free(str); /* str is allocated by hlwoc, free it normally*/
	return 0;
      }
      /* FREE our cpuset copy */
      hwloc_bitmap_free(cpuset);
      return 1;
    }else{
      if(verbose_level>=WARNING)
	printf("No valid object for core id %d!\n",my_core);
      return 0;
    }
}




void *thread_loop(void *arg){
  local_thread_t *local=(local_thread_t*)arg;
  int id = local->id;
  hwloc_topology_t topology= local->topology;
  work_t *start_working_list = local ->working_list;
  pthread_cond_t *cond_var = local->cond_var;
  pthread_mutex_t *list_lock = local->list_lock;
  work_t *work;
  int *ret = (int *)MALLOC(sizeof(int));

  bind_myself_to_core(topology,id);



  while(1){
    pthread_mutex_lock(list_lock);
    while(start_working_list->next == NULL) {
      pthread_cond_wait(cond_var, list_lock);
    }

    work = start_working_list->next;
    start_working_list->next = work-> next;
    pthread_mutex_unlock(list_lock);

    if(!work->task){
      *ret = 0;
      pthread_exit(ret);
    }

    execute_work(work);
    pthread_mutex_lock(&work->mutex);
    work->done=1;
    pthread_mutex_unlock(&work->mutex);
    pthread_cond_signal(&work->work_done);
  }

}

void add_work(pthread_mutex_t *list_lock, pthread_cond_t *cond_var, work_t *working_list, work_t *work){

  work_t *elem = working_list;
  pthread_mutex_lock(list_lock);
  while(elem->next!=NULL){
    elem=elem->next;
  }
  elem->next=work;
  work -> next = NULL;
  work -> done = 0;
  pthread_cond_signal(cond_var);
  pthread_mutex_unlock(list_lock);
}


void wait_work_completion(work_t *work){
  pthread_mutex_lock(&work->mutex);
  while(!work->done)
    pthread_cond_wait(&work->work_done, &work->mutex);

}


int submit_work(work_t *work, int thread_id){
  if( (thread_id>=0) && (thread_id< pool->nb_threads)){
    work->thread_id = thread_id;
    add_work(&pool->list_lock[thread_id], &pool->cond_var[thread_id], &pool->working_list[thread_id], work);
    return 1;
  }
  return 0;
}

thread_pool_t *create_threads(){
  hwloc_topology_t topology;
  int i;
  local_thread_t *local;
  int nb_threads;
  unsigned int nb_cores;
  int depth;

  verbose_level = tm_get_verbose_level();

    /*Get number of cores: set 1 thread per core*/
  /* Allocate and initialize topology object. */
  hwloc_topology_init(&topology);
  /* Only keep relevant levels
     hwloc_topology_ignore_all_keep_structure(topology);*/
  /* Perform the topology detection. */
  hwloc_topology_load(topology);
  depth = hwloc_topology_get_depth(topology);
  if (depth == -1 ) {
    if(verbose_level>=CRITICAL)
      fprintf(stderr,"Error: HWLOC unable to find the depth of the topology of this node!\n");
    exit(-1);
  }



  /* at depth 'depth' it is necessary a PU/core where we can execute things*/
  nb_cores = hwloc_get_nbobjs_by_depth(topology, depth-1);
  nb_threads = MIN(nb_cores,  max_nb_threads);

  if(verbose_level>=INFO)
    printf("nb_threads = %d\n",nb_threads);

  pool = (thread_pool_t*) MALLOC(sizeof(thread_pool_t));
  pool -> topology = topology;
  pool -> nb_threads = nb_threads;
  pool -> thread_list = (pthread_t*)MALLOC(sizeof(pthread_t)*nb_threads);
  pool -> working_list = (work_t*)CALLOC(nb_threads,sizeof(work_t));
  pool -> cond_var = (pthread_cond_t*)MALLOC(sizeof(pthread_cond_t)*nb_threads);
  pool -> list_lock = (pthread_mutex_t*)MALLOC(sizeof(pthread_mutex_t)*nb_threads);

  local=(local_thread_t*)MALLOC(sizeof(local_thread_t)*nb_threads);
  pool->local = local;

  for (i=0;i<nb_threads;i++){
    local[i].topology = topology;
    local[i].id = i;
    local[i].working_list = &pool->working_list[i];
    pthread_cond_init(pool->cond_var +i, NULL);
    local[i].cond_var = pool->cond_var +i;
    pthread_mutex_init(pool->list_lock +i, NULL);
    local[i].list_lock = pool->list_lock+i;
    if (pthread_create (pool->thread_list+i, NULL, thread_loop, local+i) < 0) {
      if(verbose_level>=CRITICAL)
	fprintf(stderr, "pthread_create error for exec thread %d\n",i);
      return NULL;
    }
  }
  return pool;
}

thread_pool_t *get_thread_pool(){;
  if (pool == NULL)
    return create_threads();

  return pool;
}

void terminate_thread_pool(){
  int id;
  int *ret=NULL;
  work_t work;

  if(pool){
    work.task=NULL;
    for (id=0;id<pool->nb_threads;id++){
      submit_work(&work,id);
    }


    for (id=0;id<pool->nb_threads;id++){
      pthread_join(pool->thread_list[id],(void **) &ret);
      FREE(ret);
      pthread_cond_destroy(pool->cond_var +id);
      pthread_mutex_destroy(pool->list_lock +id);
      if (pool->working_list[id].next != NULL)
	if(verbose_level >= WARNING)
	  printf("Working list of thread %d not empty!\n",id);
    }

    hwloc_topology_destroy(pool->topology);
    FREE(pool -> thread_list);
    FREE(pool -> working_list);
    FREE(pool -> cond_var);
    FREE(pool -> list_lock);
    FREE(pool -> local);
    FREE(pool);
    pool = NULL;
  }
}




int get_nb_threads(){
  pool = get_thread_pool();
  return pool -> nb_threads;
}


work_t *create_work(int nb_args, void **args, void (*task) (int, void **, int)){
  work_t *work;
  work = MALLOC(sizeof(work_t));
  work -> nb_args = nb_args;
  work -> args = args;
  work -> task = task;
  work -> done = 0;
  pthread_cond_init (&work->work_done, NULL);
  pthread_mutex_init(&work->mutex,     NULL);
  if( verbose_level >= DEBUG)
    printf("work %p created\n",(void *)work);
  return work;
}


void destroy_work(work_t *work){
  pthread_cond_destroy(&work->work_done);
  pthread_mutex_destroy(&work->mutex);
  FREE(work);
}

/* CODE example 2 functions  and  test driver*/

void f1 (int nb_args, void **args, int thread_id){
  int a, b;
  a = *(int*)args[0];
  b = *(int*)args[1];
  printf("id: %d, nb_args=%d, a=%d, b=%d\n",thread_id, nb_args,a,b);
}


void f2 (int nb_args, void **args, int thread_id){
  int n, *tab;
  int *res;
  int i,j;
  n = *(int*)args[0];
  tab = (int*)args[1];
  res=(int*)args[2];

  for(j=0;j<1000000;j++){
    *res=0;
    for (i=0;i<n;i++)
      *res+=tab[i];
  }

  printf("id: %d, done: %d!\n",thread_id, nb_args);
}



int test_main(void){

  int a=3, c;
  int b=-5;
  void *args1[3];
  void *args2[3];
  int tab[100];
  int i,res;
  work_t *work1,*work2,*work3,*work4;
  int nb_threads = get_nb_threads();


  printf("nb_threads= %d\n", nb_threads);


  args1[0] = &a;
  args1[1] = &b;
  work1 = create_work(2,args1,f1);


  for (i=0;i<100;i++)
    tab[i]=i;

  c=100;
  args2[0] = &c;
  args2[1] = tab;
  args2[2] = &res;

  work2 = create_work(3, args2, f2);
  work3 = create_work(4, args2, f2);
  work4 = create_work(5, args2, f2);

  submit_work(work1,0);
  submit_work(work2,1);
  submit_work(work3,1);
  submit_work(work4,1);



  terminate_thread_pool();
  wait_work_completion(work1);
  wait_work_completion(work2);
  wait_work_completion(work3);
  wait_work_completion(work4);

  printf("res=%d\n",res);

  destroy_work(work1);
  destroy_work(work2);
  destroy_work(work3);
  destroy_work(work4);
  return 0;
}
