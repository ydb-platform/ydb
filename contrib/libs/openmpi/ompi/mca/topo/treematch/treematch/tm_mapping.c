#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>
#include <ctype.h>
#include <math.h>
#include <assert.h>

#include "tm_mt.h"
#include "tm_mapping.h"
#include "tm_timings.h"
#include "tm_thread_pool.h"
#include "tm_tree.h"

#ifdef _WIN32
#include <windows.h>
#include <winbase.h>
#endif

#define TEST_ERROR(n) do{ \
    if( (n) != 0 ){       \
       fprintf(stderr,"Error %d Line %d\n",n,__LINE__); \
       exit(-1);} \
  }while(0)

#define LINE_SIZE (1000000)


typedef struct {
  double val;
  int key1;
  int key2;
} hash2_t;


/* compute the number of leaves of any subtree starting froma node of depth depth*/
int compute_nb_leaves_from_level(int depth,tm_topology_t *topology)
{
  int res = 1;

  while(depth < topology->nb_levels-1)
    res *= topology->arity[depth++];

  return res;
}

void tm_finalize(){
  terminate_thread_pool();
  tm_mem_check();
}

int nb_processing_units(tm_topology_t *topology)
{
  return topology->nb_proc_units;
}


void print_1D_tab(int *tab,int N)
{
  int i;
  for (i = 0; i < N; i++) {
    printf("%d", tab[i]);
    if( i < (N-1) )
      printf(",");
  }
  printf("\n");
}

int nb_lines(char *filename)
{
  FILE *pf = NULL;
  char line[LINE_SIZE];
  int N = 0;

  if(!(pf = fopen(filename,"r"))){
    if(tm_get_verbose_level() >= CRITICAL)
      fprintf(stderr,"Cannot open %s\n",filename);
    exit(-1);
  }

  while(fgets(line,LINE_SIZE,pf))
    N++;

  if(tm_get_verbose_level() >= DEBUG)
    printf("Number of lines of file %s = %d\n",filename,N);

  fclose(pf);
  return N;
}

void init_mat(char *filename,int N, double **mat, double *sum_row)
{
  FILE *pf = NULL;
  char *ptr= NULL;
  char line[LINE_SIZE];
  int i,j;
  unsigned int vl = tm_get_verbose_level();


  if(!(pf=fopen(filename,"r"))){
    if(vl >= CRITICAL)
      fprintf(stderr,"Cannot open %s\n",filename);
    exit(-1);
  }

  j = -1;
  i = 0;


  while(fgets(line,LINE_SIZE,pf)){
    char *l = line;
    j = 0;
    sum_row[i] = 0;
    while((ptr=strtok(l," \t"))){
      l = NULL;
      if((ptr[0]!='\n')&&(!isspace(ptr[0]))&&(*ptr)){
  	mat[i][j] = atof(ptr);
  	sum_row[i] += mat [i][j];
  	if(mat[i][j]<0){
  	  if(vl >= WARNING)
  	    fprintf(stderr,"Warning: negative value in com matrix! mat[%d][%d]=%f\n",i,j,mat[i][j]);
  	}
  	j++;
      }
    }
    if( j != N){
      if(vl >= CRITICAL)
  	fprintf(stderr,"Error at %d %d (%d!=%d). Too many columns for %s\n",i,j,j,N,filename);
      exit(-1);
    }
    i++;
  }


  if( i != N ){
    if(vl >= CRITICAL)
      fprintf(stderr,"Error at %d %d. Too many rows for %s\n",i,j,filename);
    exit(-1);
  }

  fclose (pf);
}

tm_affinity_mat_t * new_affinity_mat(double **mat, double *sum_row, int order){
  tm_affinity_mat_t * aff_mat;

  aff_mat = (tm_affinity_mat_t *) MALLOC(sizeof(tm_affinity_mat_t));
  aff_mat -> mat     = mat;
  aff_mat -> sum_row = sum_row;
  aff_mat -> order   = order;

  return aff_mat;
}


tm_affinity_mat_t * tm_build_affinity_mat(double **mat, int order){
  double *sum_row = NULL;
  int i,j;
  sum_row = (double*)MALLOC(order*sizeof(double));

  for( i = 0 ; i < order ; i++){
    sum_row[i] = 0;
    for(j = 0 ; j < order ; j++)
      sum_row[i] += mat [i][j];
  }

  return new_affinity_mat(mat, sum_row, order);
}





void tm_free_affinity_mat(tm_affinity_mat_t *aff_mat){
  int i;
  int n = aff_mat->order;

  for(i = 0 ; i < n ; i++)
    FREE(aff_mat->mat[i]);

  FREE(aff_mat->mat);
  FREE(aff_mat->sum_row);
  FREE(aff_mat);
}


tm_affinity_mat_t *tm_load_aff_mat(char *filename)
{
  double **mat = NULL;
  double *sum_row = NULL;
  int i, order;

  if(tm_get_verbose_level() >= INFO)
    printf("Reading matrix file: %s\n",filename);

  order = nb_lines(filename);

  sum_row = (double*)MALLOC(order*sizeof(double));
  mat = (double**)MALLOC(order*sizeof(double*));
  for( i = 0 ; i < order ; i++)
    /* the last column stores the sum of the line*/
    mat[i] = (double*)MALLOC((order)*sizeof(double));
  init_mat(filename,order, mat, sum_row);


  if(tm_get_verbose_level() >= INFO)
    printf("Affinity matrix built from %s!\n",filename);

  return new_affinity_mat(mat, sum_row, order);


}





/* void map_tree(tm_tree_t* t1,tm_tree_t *t2) */
/* { */
  /*  double x1,x2;
  if((!t1->left)&&(!t1->right)){
    printf ("%d -> %d\n",t1->id,t2->id);
    sigma[t2->id]=t1->id;
   return;
  }
  x1=t2->right->val/t1->right->val+t2->left->val/t1->left->val;
  x2=t2->left->val/t1->right->val+t2->right->val/t1->left->val;
  if(x1<x2){
    map_tree(t1->left,t2->left);
    map_tree(t1->right,t2->right);
  }else{
    map_tree(t1->right,t2->left);
    map_tree(t1->left,t2->right);
    }*/
/* } */

void depth_first(tm_tree_t *comm_tree, int *proc_list,int *i)
{
  int j;
  if(!comm_tree->child){
    proc_list[(*i)++] = comm_tree->id;
    return;
  }

  for( j  = 0 ; j < comm_tree->arity ; j++ )
    depth_first(comm_tree->child[j],proc_list,i);
}

int nb_leaves(tm_tree_t *comm_tree)
{
  int j,n=0;

  if(!comm_tree->child)
    return 1;

  for( j = 0 ; j < comm_tree->arity ; j++)
    n += nb_leaves(comm_tree->child[j]);

  return n;
}

/* find the first '-1 in the array of size n and put the value there*/
static void set_val(int *tab, int val, int n){
  int i = 0;

  while (i < n ){
    if(tab[i] ==- 1){
      tab[i] = val;
      return;
    }
    i++;
  }

  if(tm_get_verbose_level() >= CRITICAL){
    fprintf(stderr,"Error while assigning value %d to k\n",val);
  }

  exit(-1);

}
/*Map topology to cores:
 sigma_i is such that  process i is mapped on core sigma_i
 k_i is such that core i exectutes process k_i

 size of sigma is the number of process "nb_processes"
 size of k is the number of cores/nodes "nb_compute_units"

 We must have numbe of process<=number of cores

 k_i =-1 if no process is mapped on core i
*/

void map_topology(tm_topology_t *topology,tm_tree_t *comm_tree, int level,
		  int *sigma, int nb_processes, int **k, int nb_compute_units)
{
  int *nodes_id = NULL;
  int *proc_list = NULL;
  int i,j,N,M,block_size;

  unsigned int vl = tm_get_verbose_level();
  M = nb_leaves(comm_tree);
  nodes_id = topology->node_id[level];
  N = topology->nb_nodes[level];

  if(vl >= INFO){
    printf("nb_leaves=%d\n",M);
    printf("level=%d, nodes_id=%p, N=%d\n",level,(void *)nodes_id,N);
    printf("N=%d,nb_compute_units=%d\n",N,nb_compute_units);
  }

  /* The number of node at level "level" in the tree should be equal to the number of processors*/
  assert(N==nb_compute_units*topology->oversub_fact);

  proc_list = (int*)MALLOC(sizeof(int)*M);
  i = 0;
  depth_first(comm_tree,proc_list,&i);

  block_size = M/N;

  if(k){/*if we need the k vector*/
    if(vl >= INFO)
      printf("M=%d, N=%d, BS=%d\n",M,N,block_size);
    for( i = 0 ; i < nb_processing_units(topology) ; i++ )
      for(j = 0 ; j < topology->oversub_fact ; j++){
	k[i][j] = -1;
    }

    for( i = 0 ; i < M ; i++ )
      if(proc_list[i] != -1){
	if(vl >= DEBUG)
	  printf ("%d->%d\n",proc_list[i],nodes_id[i/block_size]);

	if( proc_list[i] < nb_processes ){
	  sigma[proc_list[i]] = nodes_id[i/block_size];
	  set_val(k[nodes_id[i/block_size]], proc_list[i], topology->oversub_fact);
	}
      }
  }else{
    if(vl >= INFO)
      printf("M=%d, N=%d, BS=%d\n",M,N,block_size);
    for( i = 0 ; i < M ; i++ )
      if(proc_list[i] != -1){
	if(vl >= DEBUG)
	  printf ("%d->%d\n",proc_list[i],nodes_id[i/block_size]);
	if( proc_list[i] < nb_processes )
	  sigma[proc_list[i]] = nodes_id[i/block_size];
      }
  }

  if((vl >= DEBUG) && (k)){
    printf("k: ");
    for( i = 0 ; i < nb_processing_units(topology) ; i++ ){
      printf("Procesing unit %d: ",i);
      for (j = 0 ; j<topology->oversub_fact; j++){
	if( k[i][j] == -1)
	  break;
	printf("%d ",k[i][j]);
      }
      printf("\n");
    }
  }

  FREE(proc_list);
}

tm_solution_t * tm_compute_mapping(tm_topology_t *topology,tm_tree_t *comm_tree)
{
  size_t i;
  tm_solution_t *solution;
  int *sigma, **k;
  size_t sigma_length  = comm_tree->nb_processes;
  size_t k_length      = nb_processing_units(topology);

  solution =  (tm_solution_t *)MALLOC(sizeof(tm_solution_t));
  sigma    =  (int*)  MALLOC(sizeof(int) * sigma_length);
  k        =  (int**) MALLOC(sizeof(int*) * k_length);
  for (i=0 ; i < k_length ; i++){
    k[i] =  (int*) MALLOC(sizeof(int) * topology->oversub_fact);
  }

  map_topology(topology, comm_tree, topology->nb_levels-1, sigma, sigma_length ,k, k_length);

  solution->sigma         = sigma;
  solution->sigma_length  = sigma_length;
  solution->k             = k;
  solution->k_length      = k_length;
  solution->oversub_fact  = topology->oversub_fact;

  return solution;
}



void update_comm_speed(double **comm_speed,int old_size,int new_size)
{
  double *old_tab = NULL,*new_tab= NULL;
  int i;
  unsigned int vl = tm_get_verbose_level();

  if(vl >= DEBUG)
    printf("comm speed [%p]: ",(void *)*comm_speed);

  old_tab = *comm_speed;
  new_tab = (double*)MALLOC(sizeof(double)*new_size);
  *comm_speed = new_tab;

  for( i = 0 ; i < new_size ; i++ ){
    if( i < old_size)
      new_tab[i] = old_tab[i];
    else
      new_tab[i] = new_tab[i-1];

    if(vl >= DEBUG)
      printf("%f ",new_tab[i]);
  }
  if(vl >= DEBUG)
    printf("\n");
}






/*
   copy element of tab in *new_tab from start to end and shift negativeley them
   allocates *new_tab
*/
int  fill_tab(int **new_tab,int *tab, int n, int start, int max_val, int shift)
{
  int *res = NULL,i,j,end;

  if(!n){
    *new_tab = NULL;
    return 0;
  }
  end = start;

  /* find how many cell to copy*/
  while( end < n ){
    if(tab[end] >= max_val)
      break;
    end++;
  }

  /* if none return */
  if( start == end ){
    *new_tab = NULL;
    return end;
  }

  /* allocate the result*/
  res = (int*) MALLOC (sizeof(int)*(end-start));

  /* copy and shift*/
  j = 0;
  for( i = start ; i < end ; i++ ){
    res[j] = tab[i] - shift;
    j++;
  }

  /* set the pointer passed in parameter and return */
  *new_tab = res;
  return end;
}
