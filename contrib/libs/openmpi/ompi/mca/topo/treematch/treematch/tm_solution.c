#include <ctype.h>
#include <float.h>
#include "tm_solution.h"
#include "tm_mt.h"
#include "tm_mapping.h"

typedef struct {
  int  val;
  long key;
} hash_t;


void tm_free_solution(tm_solution_t *sol){
  int i,n;

  n = sol->k_length;

  if(sol->k)
    for(i=0 ; i<n ; i++)
      FREE(sol->k[i]);

  FREE(sol->k);
  FREE(sol->sigma);
  FREE(sol);
}

/*
   Compute the distance in the tree
   between node i and j : the farther away node i and j, the
   larger the returned value.

   The algorithm looks at the largest level, starting from the top,
   for which node i and j are still in the same subtree. This is done
   by iteratively dividing their numbering by the arity of the levels
*/
int distance(tm_topology_t *topology,int i, int j)
{
  int level = 0;
  int arity;
  int f_i, f_j ;
  int vl = tm_get_verbose_level();
  int depth = topology->nb_levels-1;

  f_i = topology->node_rank[depth][i];
  f_j = topology->node_rank[depth][j];

  if(vl >= DEBUG)
    printf("i=%d, j=%d Level = %d f=(%d,%d)\n",i ,j, level, f_i, f_j);


  do{
    level++;
    arity = topology->arity[level];
    if( arity == 0 )
      arity = 1;
    f_i = f_i/arity;
    f_j = f_j/arity;
  } while((f_i!=f_j) && (level < depth));

  if(vl >= DEBUG)
    printf("distance(%d,%d):%d\n",topology->node_rank[depth][i], topology->node_rank[depth][j], level);
  /* exit(-1); */
  return level;
}

double display_sol_sum_com(tm_topology_t *topology, tm_affinity_mat_t *aff_mat, int *sigma)
{
  double a,c,sol;
  int i,j;
  double *cost = topology->cost;
  double **mat = aff_mat->mat;
  int N = aff_mat->order;
  int depth = topology->nb_levels - 1;


  sol = 0;
  for ( i = 0 ; i < N ; i++ )
    for ( j = i+1 ; j < N ; j++){
      c = mat[i][j];
      /*
	   Compute cost in funvtion of the inverse of the distance
	   This is due to the fact that the cost matrix is numbered
	   from top to bottom : cost[0] is the cost of the longest distance.
      */
      a = cost[depth-distance(topology,sigma[i],sigma[j])];
      if(tm_get_verbose_level() >= DEBUG)
	printf("T_%d_%d %f*%f=%f\n",i,j,c,a,c*a);
      sol += c*a;
    }

  for (i = 0; i < N; i++) {
    printf("%d", sigma[i]);
    if(i<N-1)
      printf(",");
  }
  printf(" : %g\n",sol);

  return sol;
}


static double display_sol_max_com(tm_topology_t *topology, tm_affinity_mat_t *aff_mat, int *sigma)
{
  double a,c,sol;
  int i,j;
  double *cost = topology->cost;
  double **mat = aff_mat->mat;
  int N = aff_mat->order;
  int vl = tm_get_verbose_level();
  int depth = topology->nb_levels - 1;

  sol = 0;
  for ( i = 0 ; i < N ; i++ )
    for ( j = i+1 ; j < N ; j++){
      c = mat[i][j];
      /*
	   Compute cost in funvtion of the inverse of the distance
	   This is due to the fact that the cost matrix is numbered
	   from top to bottom : cost[0] is the cost of the longest distance.
      */
      a = cost[depth-distance(topology,sigma[i],sigma[j])];
      if(vl >= DEBUG)
	printf("T_%d_%d %f*%f=%f\n",i,j,c,a,c*a);
      if(c*a > sol)
	sol = c*a;
    }

  for (i = 0; i < N; i++) {
    printf("%d", sigma[i]);
    if(i<N-1)
      printf(",");
  }
  printf(" : %g\n",sol);

  return sol;
}

static double display_sol_hop_byte(tm_topology_t *topology, tm_affinity_mat_t *aff_mat, int *sigma)
{
  double c,sol;
  int nb_hops;
  int i,j;
  double **mat = aff_mat->mat;
  int N = aff_mat->order;

  sol = 0;
  for ( i = 0 ; i < N ; i++ )
    for ( j = i+1 ; j < N ; j++){
      c = mat[i][j];
      nb_hops = 2*distance(topology,sigma[i],sigma[j]);
      if(tm_get_verbose_level() >= DEBUG)
	printf("T_%d_%d %f*%d=%f\n",i,j,c,nb_hops,c*nb_hops);
      sol += c*nb_hops;
    }

  for (i = 0; i < N; i++) {
    printf("%d", sigma[i]);
    if(i<N-1)
      printf(",");
  }
  printf(" : %g\n",sol);

  return sol;
}



double display_sol(tm_topology_t *topology, tm_affinity_mat_t *aff_mat, int *sigma, tm_metric_t metric){
  switch (metric){
  case TM_METRIC_SUM_COM:
    return display_sol_sum_com(topology, aff_mat, sigma);
  case TM_METRIC_MAX_COM:
    return display_sol_max_com(topology, aff_mat, sigma);
  case TM_METRIC_HOP_BYTE:
    return display_sol_hop_byte(topology, aff_mat, sigma);
  default:
    if(tm_get_verbose_level() >= ERROR){
      fprintf(stderr,"Error printing solution: metric %d not implemented\n",metric);
      return -1;
    }
  }
  return -1;
}

double tm_display_solution(tm_topology_t *topology, tm_affinity_mat_t *aff_mat, tm_solution_t *sol,
			   tm_metric_t metric){

  int i,j;
  int **k = sol->k;


  if(tm_get_verbose_level() >= DEBUG){
    printf("k: \n");
    for( i = 0 ; i < nb_processing_units(topology) ; i++ ){
      if(k[i][0] != -1){
	printf("\tProcessing unit %d: ",i);
	for (j = 0 ; j<topology->oversub_fact; j++){
	  if( k[i][j] == -1)
	    break;
	  printf("%d ",k[i][j]);
	}
	printf("\n");
      }
    }
  }


  return display_sol(topology, aff_mat, sol->sigma, metric);
}

void tm_display_other_heuristics(tm_topology_t *topology, tm_affinity_mat_t *aff_mat, tm_metric_t metric)
{
  int *sigma = NULL;
  int N  = aff_mat->order;

  sigma = (int*)MALLOC(sizeof(int)*N);

  map_Packed(topology, N, sigma);
  printf("Packed: ");
  display_sol(topology, aff_mat, sigma, metric);

  map_RR(topology, N, sigma);
  printf("RR: ");
  display_sol(topology, aff_mat, sigma, metric);

/*   double duration; */
/*   CLOCK_T time1,time0; */
/*   CLOCK(time0); */
/*   map_MPIPP(topology,1,N,sigma,comm,arch); */
/*   CLOCK(time1); */
/*   duration=CLOCK_DIFF(time1,time0); */
/*   printf("MPIPP-1-D:%f\n",duration); */
/*   printf("MPIPP-1: "); */
/*   if (TGT_flag == 1)  */
/*     print_sigma_inv(N,sigma,comm,arch); */
/*   else */
/*   print_sigma(N,sigma,comm,arch); */

/*   CLOCK(time0); */
/*   map_MPIPP(topology,5,N,sigma,comm,arch); */
/*   CLOCK(time1); */
/*   duration=CLOCK_DIFF(time1,time0); */
/*   printf("MPIPP-5-D:%f\n",duration); */
/*   printf("MPIPP-5: "); */
/*   if (TGT_flag == 1)  */
/*     print_sigma_inv(N,sigma,comm,arch); */
/*   else */
/*   print_sigma(N,sigma,comm,arch); */

  FREE(sigma);
}


int in_tab(int *tab, int n, int val){
  int i;
  for( i = 0; i < n ; i++)
    if(tab[i] == val)
      return 1;

  return 0;
}

void map_Packed(tm_topology_t *topology, int N, int *sigma)
{
  size_t i;
  int j = 0,depth;
  int vl = tm_get_verbose_level();

  depth = topology->nb_levels-1;

  for( i = 0 ; i < topology->nb_nodes[depth] ; i++){
    /* printf ("%d -> %d\n",objs[i]->os_index,i); */
    if((!topology->constraints) || (in_tab(topology->constraints, topology->nb_constraints, topology->node_id[depth][i]))){
      if(vl >= DEBUG)
	printf ("%lu: %d -> %d\n", i, j, topology->node_id[depth][i]);
      sigma[j++]=topology->node_id[depth][i];
      if(j == N)
	break;
    }
  }
}

void map_RR(tm_topology_t *topology, int N,int *sigma)
{
  int i;
  int vl = tm_get_verbose_level();

  for( i = 0 ; i < N ; i++ ){
    if(topology->constraints)
      sigma[i]=topology->constraints[i%topology->nb_constraints];
    else
      sigma[i]=i%topology->nb_proc_units;
    if(vl >= DEBUG)
      printf ("%d -> %d (%d)\n",i,sigma[i],topology->nb_proc_units);
  }
}

int hash_asc(const void* x1,const void* x2)
{
  hash_t *e1 = NULL,*e2 = NULL;

  e1 = ((hash_t*)x1);
  e2 = ((hash_t*)x2);

  return (e1->key < e2->key) ? -1 : 1;
}


int *generate_random_sol(tm_topology_t *topology,int N,int level,int seed)
{
  hash_t *hash_tab = NULL;
  int *sol = NULL;
  int *nodes_id= NULL;
  int i;

  nodes_id = topology->node_id[level];

  hash_tab = (hash_t*)MALLOC(sizeof(hash_t)*N);
  sol = (int*)MALLOC(sizeof(int)*N);

  init_genrand(seed);

  for( i = 0 ; i < N ; i++ ){
    hash_tab[i].val = nodes_id[i];
    hash_tab[i].key = genrand_int32();
  }

  qsort(hash_tab,N,sizeof(hash_t),hash_asc);
  for( i = 0 ; i < N ; i++ )
    sol[i] = hash_tab[i].val;

  FREE(hash_tab);
  return sol;
}


double eval_sol(int *sol,int N,double **comm, double **arch)
{
  double a,c,res;
  int i,j;

  res = 0;
  for ( i = 0 ; i < N ; i++ )
    for ( j = i+1 ; j < N ; j++ ){
      c = comm[i][j];
      a = arch[sol[i]][sol[j]];
      res += c/a;
    }

  return res;
}

void exchange(int *sol,int i,int j)
{
  int tmp;
  tmp = sol[i];
  sol[i] = sol[j];
  sol[j] = tmp;
}

double gain_exchange(int *sol,int l,int m,double eval1,int N,double **comm, double **arch)
{
  double eval2;
  if( l == m )
    return 0;
  exchange(sol,l,m);
  eval2 = eval_sol(sol,N,comm,arch);
  exchange(sol,l,m);

  return eval1-eval2;
}

void select_max(int *l,int *m,double **gain,int N,int *state)
{
  double max;
  int i,j;

  max = -DBL_MAX;

  for( i = 0 ; i < N ; i++ )
    if(!state[i])
      for( j = 0 ; j < N ; j++ )
	if( (i != j) && (!state[j]) ){
	  if(gain[i][j] > max){
	    *l = i;
	    *m = j;
	    max=gain[i][j];
	  }
	}
}


void compute_gain(int *sol,int N,double **gain,double **comm, double **arch)
{
  double eval1;
  int i,j;

  eval1 = eval_sol(sol,N,comm,arch);
  for( i = 0 ; i < N ; i++ )
    for( j = 0 ; j <= i ; j++)
      gain[i][j] = gain[j][i] = gain_exchange(sol,i,j,eval1,N,comm,arch);
}


/* Randomized Algorithm of
Hu Chen, Wenguang Chen, Jian Huang ,Bob Robert,and H.Kuhn. Mpipp: an automatic profile-guided
parallel process placement toolset for smp clusters and multiclusters. In
Gregory K. Egan and Yoichi Muraoka, editors, ICS, pages 353-360. ACM, 2006.
 */

void map_MPIPP(tm_topology_t *topology,int nb_seed,int N,int *sigma,double **comm, double **arch)
{
  int *sol = NULL;
  int *state = NULL;
  double **gain = NULL;
  int **history = NULL;
  double *temp = NULL;
  int i,j,t,l=0,m=0,seed=0;
  double max,sum,best_eval,eval;

  gain = (double**)MALLOC(sizeof(double*)*N);
  history = (int**)MALLOC(sizeof(int*)*N);
  for( i = 0 ; i < N ; i++){
    gain[i] = (double*)MALLOC(sizeof(double)*N);
    history[i] = (int*)MALLOC(sizeof(int)*3);
  }

  state = (int*)MALLOC(sizeof(int)*N);
  temp = (double*)MALLOC(sizeof(double)*N);

  sol = generate_random_sol(topology,N,topology->nb_levels-1,seed++);
  for( i = 0 ; i < N ; i++)
    sigma[i] = sol[i];

  best_eval = DBL_MAX;
  while(seed <= nb_seed){
    do{
      for( i =  0 ; i < N ; i++ ){
	state[i] = 0;
	/* printf("%d ",sol[i]); */
      }
      /* printf("\n"); */
      compute_gain(sol,N,gain,comm,arch);
      /*
      display_tab(gain,N);
      exit(-1);
      */
      for( i = 0 ; i < N/2 ; i++ ){
	select_max(&l,&m,gain,N,state);
	/* printf("%d: %d <=> %d : %f\n",i,l,m,gain[l][m]); */
	state[l] = 1;
	state[m] = 1;
	exchange(sol,l,m);
	history[i][1] = l;
	history[i][2] = m;
	temp[i] = gain[l][m];
	compute_gain(sol,N,gain,comm,arch);
      }

      t = -1;
      max = 0;
      sum = 0;
      for(i = 0 ; i < N/2 ; i++ ){
	sum += temp[i];
	if( sum > max ){
	  max = sum;
	  t = i;
	}
      }
      /*for(j=0;j<=t;j++)
	printf("exchanging: %d with %d for gain: %f\n",history[j][1],history[j][2],temp[j]); */
      for( j = t+1 ; j < N/2 ; j++ ){
	exchange(sol,history[j][1],history[j][2]);
	/* printf("Undoing: %d with %d for gain: %f\n",history[j][1],history[j][2],temp[j]);  */
      }
      /* printf("max=%f\n",max); */

      /*for(i=0;i<N;i++){
	printf("%d ",sol[i]);
	}
	printf("\n");*/
      eval = eval_sol(sol,N,comm,arch);
      if(eval < best_eval){
	best_eval = eval;
	for(i = 0 ; i < N ; i++)
	  sigma[i] = sol[i];
	/* print_sol(N); */
      }
    }while( max > 0 );
    FREE(sol);
    sol=generate_random_sol(topology,N,topology->nb_levels-1,seed++);
  }


  FREE(sol);
  FREE(temp);
  FREE(state);
  for( i = 0 ; i < N ; i++){
    FREE(gain[i]);
    FREE(history[i]);
  }
  FREE(gain);
  FREE(history);
}
