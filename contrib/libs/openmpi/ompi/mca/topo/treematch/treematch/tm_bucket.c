#include <stdio.h>
#include <float.h>
#include <math.h>
#include <assert.h>
#include "tm_tree.h"
#include "tm_bucket.h"
#include "tm_timings.h"
#include "tm_verbose.h"
#include "tm_thread_pool.h"
#include "tm_mt.h"
#ifdef _WIN32
#include <windows.h>
#include <winbase.h>
#endif

#ifndef __CHARMC__
#define __CHARMC__ 0
#endif

#if __CHARMC__
#error #include "converse.h"
#else
static int ilog2(int val)
{
  int i = 0;
  for( ; val != 0; val >>= 1, i++ );
  return i;
}
#define CmiLog2(VAL)  ilog2((int)(VAL))
#endif

static int verbose_level = ERROR;

static bucket_list_t global_bl;

int tab_cmp(const void*,const void*);
int old_bucket_id(int,int,bucket_list_t);
int bucket_id(int,int,bucket_list_t);
void display_bucket(bucket_t *);
void check_bucket(bucket_t *,double **,double, double);
void display_pivots(bucket_list_t);
void display_bucket_list(bucket_list_t);
void add_to_bucket(int,int,int,bucket_list_t);
void dfs(int,int,int,double *,double *,int,int);
void built_pivot_tree(bucket_list_t);
void fill_buckets(bucket_list_t);
int is_power_of_2(int);
void partial_sort(bucket_list_t *,double **,int);
void next_bucket_elem(bucket_list_t,int *,int *);
int add_edge_3(tm_tree_t *,tm_tree_t *,int,int,int *);
void free_bucket(bucket_t *);
void free_tab_bucket(bucket_t **,int);
void free_bucket_list(bucket_list_t);
void partial_update_val (int nb_args, void **args, int thread_id);
double bucket_grouping(tm_affinity_mat_t *,tm_tree_t *, tm_tree_t *, int ,int);
int tab_cmp(const void* x1,const void* x2)
{
  int *e1 = NULL,*e2 = NULL,i1,i2,j1,j2;
  double **tab = NULL;
  bucket_list_t bl;

  bl = global_bl;

  e1 = ((int *)x1);
  e2 = ((int *)x2);

  tab = bl->tab;

  i1 = e1[0];
  j1 = e1[1];
  i2 = e2[0];
  j2 = e2[1];

  if(tab[i1][j1]==tab[i2][j2]){
    if(i1==i2){
      return (j1 > j2) ? -1 : 1;
    }else{
      return (i1 > i2) ? -1 : 1;
    }
  }
  return (tab[i1][j1] > tab[i2][j2]) ? -1 : 1;
}


int old_bucket_id(int i,int j,bucket_list_t bucket_list)
{
  double *pivot = NULL,val;
  int n,sup,inf,p;

  pivot = bucket_list->pivot;
  n = bucket_list->nb_buckets;
  val = bucket_list->tab[i][j];

  inf = -1;
  sup = n;

  while( (sup - inf) > 1){
    p = (sup + inf)/2;
    /* printf("%f [%d,%d,%d]=%f\n",val,inf,p,sup,pivot[p]); */
    if( val < pivot[p] ){
      inf = p;
      if( inf == sup )
	inf--;
    } else {
      sup = p;
      if( sup == inf )
	sup++;
    }
  }
  /*exit(-1);*/
  return sup;
}

int bucket_id(int i,int j,bucket_list_t bucket_list)
{
  double *pivot_tree = NULL,val;
  int p,k;

  pivot_tree = bucket_list->pivot_tree;
  val = bucket_list->tab[i][j];


  p = 1;
  for( k = 0 ; k < bucket_list->max_depth ; k++){
    if( val > pivot_tree[p] )
      p = p*2;
    else
      p = p*2 + 1;
  }

  return (int)pivot_tree[p];
}

void  display_bucket(bucket_t *b)
{
  printf("\tb.bucket=%p\n",(void *)b->bucket);
  printf("\tb.bucket_len=%d\n",(int)b->bucket_len);
  printf("\tb.nb_elem=%d\n",(int)b->nb_elem);
}

void check_bucket(bucket_t *b,double **tab,double inf, double sup)
{
  int i,j,k;
  for( k = 0 ; k < b->nb_elem ; k++ ){
    i = b->bucket[k].i;
    j = b->bucket[k].j;
    if((tab[i][j] < inf) || (tab[i][j] > sup)){
      if(verbose_level >= CRITICAL)
	fprintf(stderr,"[%d] (%d,%d):%f not in [%f,%f]\n",k,i,j,tab[i][j],inf,sup);
      exit(-1);
    }
  }
}

void display_pivots(bucket_list_t bucket_list)
{
  int i;
  for( i = 0 ; i < bucket_list->nb_buckets-1 ; i++)
    printf("pivot[%d]=%f\n",i,bucket_list->pivot[i]);
  printf("\n");
}

void display_bucket_list(bucket_list_t bucket_list)
{
  int i;
  double inf,sup;

  /*display_pivots(bucket_list);*/

  for(i = 0 ; i < bucket_list->nb_buckets ; i++){
    inf = bucket_list->pivot[i];
    sup = bucket_list->pivot[i-1];
    if( i == 0 )
      sup=DBL_MAX;
    if( i == bucket_list->nb_buckets - 1 )
      inf = 0;
    if(verbose_level >= DEBUG){
      printf("Bucket %d:\n",i);
      display_bucket(bucket_list->bucket_tab[i]);
      printf("\n");
    }
    check_bucket(bucket_list->bucket_tab[i],bucket_list->tab,inf,sup);
  }

}

void add_to_bucket(int id,int i,int j,bucket_list_t bucket_list)
{
  bucket_t *bucket = NULL;
  int N,n,size;

  bucket = bucket_list->bucket_tab[id];
  /* display_bucket(bucket);*/

  if( bucket->bucket_len == bucket->nb_elem ){
    N = bucket_list->N;
    n = bucket_list->nb_buckets;
    size = N*N/n;
    /* display_bucket(bucket);*/
    if(verbose_level >= DEBUG){
      printf("Extending bucket %d (%p) from size %d to size %d!\n",
             id, (void*)bucket->bucket, bucket->nb_elem, bucket->nb_elem+size);
    }

    bucket->bucket = (coord*)REALLOC(bucket->bucket,sizeof(coord)*(size + bucket->bucket_len));
    bucket->bucket_len += size;

    /* if(verbose_level >= DEBUG){ */
    /*   printf("MALLOC/realloc: %d\n",id); */
    /*   printf("(%d,%d)\n",i,j); */
    /*   display_bucket(bucket); */
    /*   printf("\n"); */
    /* } */

  }

 bucket->bucket[bucket->nb_elem].i=i;
 bucket->bucket[bucket->nb_elem].j=j;
 bucket->nb_elem++;

 /* printf("\n"); */
 /* exit(-1); */
}

void dfs(int i,int inf,int sup,double *pivot,double *pivot_tree,int depth,int max_depth)
{
  int p;
  if( depth == max_depth )
    return;

  p = (inf + sup)/2;
  pivot_tree[i] = pivot[p-1];

  dfs(2*i,inf,p-1,pivot,pivot_tree,depth+1,max_depth);
  dfs(2*i+1,p+1,sup,pivot,pivot_tree,depth+1,max_depth);
}

void  built_pivot_tree(bucket_list_t bucket_list)
{
  double *pivot_tree = NULL,*pivot = NULL;
  int n,i,k;

  pivot = bucket_list->pivot;
  n = bucket_list->nb_buckets;
  pivot_tree = (double*)MALLOC(sizeof(double)*2*n);
  bucket_list->max_depth = (int)CmiLog2(n) - 1;

  dfs(1,1,n-1,pivot,pivot_tree,0,bucket_list->max_depth);

  k = 0;
  pivot_tree[0] = -1;
  for( i = n ; i < 2*n ; i++)
    pivot_tree[i] = k++;

  bucket_list->pivot_tree = pivot_tree;

  if(verbose_level >= DEBUG){
    for(i=0;i<2*n;i++)
      printf("%d:%f\t",i,pivot_tree[i]);
    printf("\n");
  }
}

void fill_buckets(bucket_list_t bucket_list)
{
  int N,i,j,id;

  N = bucket_list->N;

  for( i = 0 ; i < N ; i++ )
    for( j = i+1 ; j < N ; j++ ){
      id = bucket_id(i,j,bucket_list);
      add_to_bucket(id,i,j,bucket_list);
    }
}

int is_power_of_2(int val)
{
  int n = 1;
  do{
    if( n == val)
      return 1;
    n <<= 1;
  }while( n > 0);
  return 0;
}


void partial_sort(bucket_list_t *bl,double **tab,int N)
{
  double *pivot = NULL;
  int *sample = NULL;
  int i,j,k,n,id;
  bucket_list_t bucket_list;
  int nb_buckets, nb_bits;

  if( N <= 0){
    if(verbose_level >= ERROR )
      fprintf(stderr,"Error: tryng to group a matrix of size %d<=0!\n",N);
    return;
  }

  /* after these operations, nb_buckets is a power of 2 interger close to log2(N)*/

  nb_buckets = (int)floor(CmiLog2(N));

  nb_bits = (int)ceil(CmiLog2(nb_buckets));
  nb_buckets = nb_buckets >> (nb_bits-1);
  nb_buckets = nb_buckets << (nb_bits-1);

  /* check the result*/
  if(!is_power_of_2(nb_buckets)){
    if(verbose_level >= ERROR)
      fprintf(stderr,"Error! Paramater nb_buckets is: %d and should be a power of 2\n",nb_buckets);
    exit(-1);
  }

  bucket_list = (bucket_list_t)MALLOC(sizeof(_bucket_list_t));
  bucket_list->tab = tab;
  bucket_list->N = N;

  n = pow(nb_buckets,2);
  if(verbose_level >= INFO)
    printf("N=%d, n=%d\n",N,n);
  sample = (int*)MALLOC(2*sizeof(int)*n);

  for( k =  0 ; k < n ; k++ ){
    i = genrand_int32()%(N-2)+1;
    if( i == N-2 )
      j = N-1;
    else
      j = genrand_int32()%(N-i-2)+i+1;
    if(verbose_level >= DEBUG)
      printf("i=%d, j=%d\n",i,j);
    assert( i != j );
    assert( i < j );
    assert( i < N );
    assert( j < N );
    sample[2*k] = i;
    sample[2*k+1] = j;
  }

  /* printf("k=%d\n",k); */
  global_bl = bucket_list;
  qsort(sample,n,2*sizeof(int),tab_cmp);

  if(verbose_level >= DEBUG)
    for(k=0;k<n;k++){
      i=sample[2*k];
      j=sample[2*k+1];
      printf("%f\n",tab[i][j]);
    }


  pivot = (double*)MALLOC(sizeof(double)*nb_buckets-1);
  id = 1;
  for( k = 1 ; k < nb_buckets ; k++ ){
    /* fprintf(stderr,"k=%d, id=%d\n",k,id); */
    i = sample[2*(id-1)];
    j = sample[2*(id-1)+1];
    id *= 2;

    /*    i=sample[k*N/nb_buckets]/N;
	  j=sample[k*N/nb_buckets]%N;*/
    pivot[k-1] = tab[i][j];
    /* printf("pivot[%d]=%f\n",k-1,tab[i][j]); */
  }

  bucket_list->pivot = pivot;
  bucket_list->nb_buckets = nb_buckets;
  built_pivot_tree(bucket_list);

  bucket_list->bucket_tab = (bucket_t**)MALLOC(nb_buckets*sizeof(bucket_t*));
  for( i  = 0 ; i < nb_buckets ; i++ )
    bucket_list->bucket_tab[i] = (bucket_t*)CALLOC(1,sizeof(bucket_t));

  fill_buckets(bucket_list);

  /* display_bucket_list(bucket_list); */

  bucket_list->cur_bucket = 0;
  bucket_list->bucket_indice = 0;

  FREE(sample);

  *bl = bucket_list;
}

void next_bucket_elem(bucket_list_t bucket_list,int *i,int *j)
{
  bucket_t *bucket = bucket_list->bucket_tab[bucket_list->cur_bucket];

  /*  display_bucket_list(bucket_list);
      printf("nb_elem: %d, indice: %d, bucket_id: %d\n",(int)bucket->nb_elem,bucket_list->bucket_indice,bucket_list->cur_bucket);
  */
  while( bucket->nb_elem <= bucket_list->bucket_indice ){
    bucket_list->bucket_indice = 0;
    bucket_list->cur_bucket++;
    bucket = bucket_list->bucket_tab[bucket_list->cur_bucket];
    if(verbose_level >= DEBUG){
      printf("### From bucket %d to bucket %d\n",bucket_list->cur_bucket-1,bucket_list->cur_bucket);
      printf("nb_elem: %d, indice: %d, bucket_id: %d\n",(int)bucket->nb_elem,bucket_list->bucket_indice,bucket_list->cur_bucket);
    }
  }

  if(!bucket->sorted){
    global_bl = bucket_list;
    qsort(bucket->bucket,bucket->nb_elem,2*sizeof(int),tab_cmp);
    bucket->sorted = 1;
  }

  *i = bucket->bucket[bucket_list->bucket_indice].i;
  *j = bucket->bucket[bucket_list->bucket_indice].j;
  bucket_list->bucket_indice++;
}


int add_edge_3(tm_tree_t *tab_node, tm_tree_t *parent,int i,int j,int *nb_groups)
{
  /* printf("%d <-> %d ?\n",tab_node[i].id,tab_node[j].id); */
  if((!tab_node[i].parent) && (!tab_node[j].parent)){
    if(parent){
      parent->child[0] = &tab_node[i];
      parent->child[1] = &tab_node[j];
      tab_node[i].parent = parent;
      tab_node[j].parent = parent;

      if(verbose_level >= DEBUG)
	printf("%d: %d-%d\n",*nb_groups,parent->child[0]->id,parent->child[1]->id);

      return 1;
    }
    return 0;
  }

  if( tab_node[i].parent && (!tab_node[j].parent) ){
    parent = tab_node[i].parent;
    if(!parent->child[2]){
      parent->child[2] = &tab_node[j];
      tab_node[j].parent = parent;

      if(verbose_level >= DEBUG)
	printf("%d: %d-%d-%d\n",*nb_groups,parent->child[0]->id,parent->child[1]->id,parent->child[2]->id);

      (*nb_groups)++;
    }
    return 0;
  }

  if(tab_node[j].parent && (!tab_node[i].parent)){
    parent = tab_node[j].parent;
    if(!parent->child[2]){
      parent->child[2] = &tab_node[i];
      tab_node[i].parent = parent;

      if(verbose_level >= DEBUG)
	printf("%d: %d-%d-%d\n",*nb_groups,parent->child[0]->id,parent->child[1]->id,parent->child[2]->id);

      (*nb_groups)++;
    }
    return 0;
  }

  return 0;
}

int try_add_edge(tm_tree_t *tab_node, tm_tree_t *parent,int arity,int i,int j,int *nb_groups)
{
  assert( i != j );

  switch(arity){
  case 2:
    if(tab_node[i].parent)
      return 0;
    if(tab_node[j].parent)
      return 0;

    parent->child[0] = &tab_node[i];
    parent->child[1] = &tab_node[j];
    tab_node[i].parent = parent;
    tab_node[j].parent = parent;

    (*nb_groups)++;

    return 1;
  case 3:
    return add_edge_3(tab_node,parent,i,j,nb_groups);
  default:
    if(verbose_level >= ERROR)
      fprintf(stderr,"Cannot handle arity %d\n",parent->arity);
    exit(-1);
  }
}

void free_bucket(bucket_t *bucket)
{
  FREE(bucket->bucket);
  FREE(bucket);
}

void free_tab_bucket(bucket_t **bucket_tab,int N)
{
  int i;
  for( i = 0 ; i < N ; i++ )
    free_bucket(bucket_tab[i]);
  FREE(bucket_tab);
}

void free_bucket_list(bucket_list_t bucket_list)
{
  /* Do not free the tab field it is used elsewhere */
  free_tab_bucket(bucket_list->bucket_tab,bucket_list->nb_buckets);
  FREE(bucket_list->pivot);
  FREE(bucket_list->pivot_tree);
  FREE(bucket_list);
}

void partial_update_val (int nb_args, void **args, int thread_id){
  int inf = *(int*)args[0];
  int sup = *(int*)args[1];
  tm_affinity_mat_t *aff_mat = (tm_affinity_mat_t*)args[2];
  tm_tree_t *new_tab_node = (tm_tree_t*)args[3];
  double *res=(double*)args[4];
  int l;

  if(nb_args != 5){
    if(verbose_level >= ERROR)
      fprintf(stderr,"(Thread: %d) Wrong number of args in %s: %d\n",thread_id, __func__, nb_args);
    exit(-1);
  }

  for( l = inf ; l < sup ; l++ ){
      update_val(aff_mat,&new_tab_node[l]);
      *res += new_tab_node[l].val;
    }
}

double bucket_grouping(tm_affinity_mat_t *aff_mat,tm_tree_t *tab_node, tm_tree_t *new_tab_node,
		     int arity,int M)
{
  bucket_list_t bucket_list;
  double duration,val = 0;
  int l,i,j,nb_groups;
  double gr1_1=0;
  double gr1_2=0;
  double gr1, gr2, gr3;
  int N = aff_mat->order;
  double **mat = aff_mat->mat;

  verbose_level = tm_get_verbose_level();
  if(verbose_level >= INFO )
    printf("starting sort of N=%d elements\n",N);



  TIC;
  partial_sort(&bucket_list,mat,N);
  duration = TOC;
  if(verbose_level >= INFO)
    printf("Partial sorting=%fs\n",duration);
  if(verbose_level >= DEBUG)
    display_pivots(bucket_list);

  TIC;
  l = 0;
  i = 0;
  nb_groups = 0;


  TIC;
  if(verbose_level >= INFO){
    while( l < M ){
      TIC;
      next_bucket_elem(bucket_list,&i,&j);
      if(verbose_level >= DEBUG)
	printf("elem[%d][%d]=%f ",i,j,mat[i][j]);
      gr1_1 += TOC;
      TIC;
      if(try_add_edge(tab_node,&new_tab_node[l],arity,i,j,&nb_groups)){
	l++;
      }
      gr1_2 += TOC;
    }
  }else{
    while( l < M ){
      next_bucket_elem(bucket_list,&i,&j);
      if(try_add_edge(tab_node,&new_tab_node[l],arity,i,j,&nb_groups)){
	l++;
      }
    }
  }

  gr1=TOC;
  if(verbose_level >= INFO)
    printf("Grouping phase 1=%fs (%fs+%fs) \n",gr1, gr1_1, gr1_2);

  if(verbose_level >= DEBUG)
    printf("l=%d,nb_groups=%d\n",l,nb_groups);

  TIC;
  while( nb_groups < M ){
    next_bucket_elem(bucket_list,&i,&j);
    try_add_edge(tab_node,NULL,arity,i,j,&nb_groups);
  }

  gr2=TOC;
  if(verbose_level >= INFO)
    printf("Grouping phase 2=%fs\n",gr2);

  if(verbose_level >= DEBUG)
    printf("l=%d,nb_groups=%d\n",l,nb_groups);

  TIC;


  if(M>512){ /* perform this part in parallel*/
    int id;
    int nb_threads;
    work_t **works;
    int *inf;
    int *sup;
    double *tab_val;

    nb_threads = get_nb_threads();
    works = (work_t**)MALLOC(sizeof(work_t*)*nb_threads);
    inf = (int*)MALLOC(sizeof(int)*nb_threads);
    sup = (int*)MALLOC(sizeof(int)*nb_threads);
    tab_val = (double*)CALLOC(nb_threads,sizeof(double));
    for(id=0;id<nb_threads;id++){
      void **args=(void**)MALLOC(sizeof(void*)*5);
      inf[id]=id*M/nb_threads;
      sup[id]=(id+1)*M/nb_threads;
      if(id == nb_threads-1) sup[id]=M;
      args[0]=(void*)(inf+id);
      args[1]=(void*)(sup+id);
      args[2]=(void*)aff_mat;
      args[3]=(void*)new_tab_node;
      args[4]=(void*)(tab_val+id);

      works[id]= create_work(5,args,partial_update_val);
      if(verbose_level >= DEBUG)
	printf("Executing %p\n",(void *)works[id]);

      submit_work( works[id], id);
    }

    for(id=0;id<nb_threads;id++){
      wait_work_completion(works[id]);
      val+=tab_val[id];
      FREE(works[id]->args);
    }


    FREE(inf);
    FREE(sup);
    FREE(tab_val);
    FREE(works);
  }else{
    for( l = 0 ; l < M ; l++ ){

      update_val(aff_mat,&new_tab_node[l]);
      val += new_tab_node[l].val;
    }
  }
  gr3=TOC;
  if(verbose_level >= INFO)
    printf("Grouping phase 3=%fs\n",gr3);
  /* printf("val=%f\n",val);exit(-1);	 */

  duration = TOC;
  if(verbose_level >= INFO)
    printf("Grouping =%fs\n",duration);

  if(verbose_level >= DEBUG){
    printf("Bucket: %d, indice:%d\n",bucket_list->cur_bucket,bucket_list->bucket_indice);
    printf("val=%f\n",val);
  }
  free_bucket_list(bucket_list);

  return val;
}

