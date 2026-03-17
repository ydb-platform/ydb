#include <stdlib.h>
#include <stdio.h>
#include "k-partitioning.h"
#include "tm_mt.h"
#include "tm_verbose.h"

void memory_allocation(PriorityQueue ** Q, PriorityQueue ** Qinst, double *** D, int n, int k);
void initialization(int * const part, double ** const matrice, PriorityQueue * const Qpart, PriorityQueue * const Q, PriorityQueue * const Qinst, double ** const D, int n, int k, int * const deficit, int * const surplus);
void algo(int * const part, double ** const matrice, PriorityQueue * const Qpart, PriorityQueue * const Q, PriorityQueue * const Qinst, double ** const D, int n,  int * const deficit, int * const surplus);
double nextGain(PriorityQueue * const Qpart, PriorityQueue * const Q, int * const deficit, int * const surplus);
void balancing(int n, int deficit, int surplus, double ** const D, int * const part);
void destruction(PriorityQueue * Qpart, PriorityQueue * Q, PriorityQueue * Qinst, double ** D, int n, int k);

void allocate_vertex2(int u, int *res, double **comm, int n, int *size, int max_size);
double eval_cost2(int *,int,double **);
int  *kpartition_greedy2(int k, double **comm, int n, int nb_try_max, int *constraints, int nb_constraints);
int*  build_p_vector(double **comm, int n, int k, int greedy_trials, int * constraints, int nb_constraints);

int* kPartitioning(double ** comm, int n, int k, int * constraints, int nb_constraints, int greedy_trials)
{
  /* ##### declarations & allocations ##### */

  PriorityQueue Qpart, *Q = NULL, *Qinst = NULL;
  double **D = NULL;
  int deficit, surplus, *part = NULL;
  int real_n = n-nb_constraints;

  part = build_p_vector(comm, n, k, greedy_trials, constraints, nb_constraints);

  memory_allocation(&Q, &Qinst, &D, real_n, k);

  /* ##### Initialization ##### */

  initialization(part, comm, &Qpart, Q, Qinst, D, real_n, k, &deficit, &surplus);

  /* ##### Main loop ##### */
  while((nextGain(&Qpart, Q, &deficit, &surplus))>0)
    {
      algo(part, comm, &Qpart, Q, Qinst, D, real_n, &deficit, &surplus);
    }

  /* ##### Balancing the partition  ##### */
  balancing(real_n, deficit, surplus, D, part); /*if partition isn't balanced we have to make one last move*/

  /* ##### Memory deallocation ##### */
  destruction(&Qpart, Q, Qinst, D, real_n, k);

  return part;
}

void memory_allocation(PriorityQueue ** Q, PriorityQueue ** Qinst, double *** D, int n, int k)
{
  int i;
  *Q = calloc(k, sizeof(PriorityQueue)); /*one Q for each partition*/
  *Qinst = calloc(n, sizeof(PriorityQueue)); /*one Qinst for each vertex*/
  *D = malloc(sizeof(double *) * n); /*D's size is n * k*/
  for(i=0; i < n; ++i)
    (*D)[i] = calloc(k, sizeof(double));
}

void initialization(int * const part, double ** const matrice, PriorityQueue * const Qpart, PriorityQueue * const Q, PriorityQueue * const Qinst, double ** const D, int n, int k, int * const deficit, int * const surplus)
{
  int i,j;

  /* ##### PriorityQueue initializations ##### */
  /* We initialize Qpart with a size of k because it contains the subsets's indexes. */
  PQ_init(Qpart, k);

  /* We initialize each Q[i] with a size of n because each vertex is in one of these queue at any time. */
  /* However we could set a size of (n/k)+1 as this is the maximum size of a subset when the partition is not balanced. */
  for(i=0; i<k; ++i)
    PQ_init(&Q[i], n);

  /* We initialize each Qinst[i] with a size of k because fo each vertex i, Qinst[i] contains the D(i,j) values for j = 0...(k-1) */
  for(i=0; i<n; ++i)
    PQ_init(&Qinst[i], k);

  /* ##### Computing the D(i,j) values ##### */
  for(i=0; i < n; ++i) /*for each vertex i*/
    {
      for(j=0; j < n; ++j) /*and for each vertex j*/
	{
	  D[i][part[j]] += matrice[i][j];
	}
    }

  /* ##### Filling up the queues ##### */
  /* ### Qinst ### */
  for(i=0; i < n; ++i) /*for each vertex i*/
    for(j=0; j < k; ++j) /*and for each subset j*/
      PQ_insert(&Qinst[i], j, D[i][j]); /*we insert the corresponding D(i,j) value in Qinst[i]*/

  /* ### Q ### */
  for(i=0; i<n; ++i) /*for each vertex i*/
    PQ_insert(&Q[part[i]], i, PQ_findMaxKey(&Qinst[i])-D[i][part[i]]); /*we insert in Q[part[i]] the vertex i with its highest possible gain*/

  /* ### Qpart ### */
  for(i=0; i < k; ++i) /*for each subset i*/
    PQ_insert(Qpart, i, PQ_findMaxKey(&Q[i])); /*we insert it in Qpart with the highest possible gain by one of its vertex as key*/


  /* ##### Initialization of deficit/surplus ##### */
  *surplus = *deficit = 0;
}

void algo(int * const part, double ** const matrice, PriorityQueue * const Qpart, PriorityQueue * const Q, PriorityQueue * const Qinst, double ** const D, int n, int * const deficit, int * const surplus)
{
  int p,u,v,j;
  double d;
  if(*deficit == *surplus) /*if the current partition is balanced*/
    {
      p = PQ_deleteMax(Qpart); /*we get the subset with the highest possible gain in p and remove it from Qpart*/
      u = PQ_deleteMax(&Q[p]); /*then we get the vertex with this highest possible gain in u and remove it from Q[p] */
      *deficit = part[u]; /*p becomes the deficit */
    }
  else /*the current partition is not balanced*/
    {
      u = PQ_deleteMax(&Q[*surplus]); /*we get the vertex with the highest possible gain in surplus and remove it from Q[surplus] */
      PQ_delete(Qpart, part[u]); /*then we remove surplus from Qpart  (note that u is from surplus so part[u] is surplus) */
    }
  d = PQ_findMaxKey(&Q[part[u]]); /*we get the next highest possible gain in part[u] (without taking u in account as we already removed it from Q[part[u])*/
  PQ_insert(Qpart, part[u], d); /*we put part[u] back in Qpart with its new highest possible gain*/
  j = PQ_deleteMax(&Qinst[u]); /*we get from Qinst[u] the subset in which we have to move u to get the highest gain.*/
  if ( j < 0){
    if(tm_get_verbose_level() >= CRITICAL)
      fprintf(stderr,"Error Max element in priority queue negative!\n");
    exit(-1);
  }
  *surplus = j; /*this subset becomes surplus*/

  for(v=0; v < n; ++v) /*we scan though all edges (u,v) */
    {
      j = part[u]; /*we set j to the starting subset */
      D[v][j]= D[v][j] - matrice[u][v]; /*we compute the new D[v, i] (here j has the value of the starting subset of u, that's why we say i) */
      PQ_adjustKey(&Qinst[v], j, D[v][j]); /*we update this gain in Qinst[v]*/
      j = *surplus; /*we put back the arrival subset in j*/
      D[v][j] = D[v][j] + matrice[u][v]; /*matrice[u][v]; we compute the new D[v, j]*/
      PQ_adjustKey(&Qinst[v], j, D[v][j]);/*we update this gain in Qinst[v]*/
      d = PQ_findMaxKey(&Qinst[v]) - D[v][part[v]]; /*we compute v's new highest possible gain*/
      PQ_adjustKey(&Q[part[v]], v, d); /*we update it in Q[p[v]]*/
      d = PQ_findMaxKey(&Q[part[v]]); /*we get the highest possible gain in v's subset*/
      PQ_adjustKey(Qpart, part[v], d); /*we update it in Qpart*/
    }
  part[u] = *surplus; /*we move u from i to j (here surplus has the value of j the arrival subset)*/

  d = PQ_findMaxKey(&Qinst[u]) - D[u][part[u]]; /*we compute the new u's highest possible gain*/
  if(!PQ_isEmpty(&Qinst[u])) /*if at least one more move of u is possible*/
    PQ_insert(&Q[part[u]], u, d); /*we insert u in the Q queue of its new subset*/
  PQ_adjustKey(Qpart, part[u], d); /*we update the new highest possible gain in u's subset*/
}

double nextGain(PriorityQueue * const Qpart, PriorityQueue * const Q, int * const deficit, int * const surplus)
{
  double res;
  if(*deficit == *surplus) /*if the current partition is balanced*/
    res = PQ_findMaxKey(Qpart); /*we get the highest possible gain*/
  else /*the current partition is not balanced*/
    res = PQ_findMaxKey(&Q[*surplus]); /*we get the highest possible gain from surplus*/
  return res;
}

void balancing(int n, int deficit, int surplus, double ** const D, int * const part)
{
  if(surplus != deficit) /*if the current partition is not balanced*/
    {
      int i;
      PriorityQueue moves; /*we use a queue to store the possible moves from surplus to deficit*/
      PQ_init(&moves, n);
      for(i=0; i<n; ++i) /*for each vertex*/
	{
	  if(part[i] == surplus) /*if i is from surplus*/
	    PQ_insert(&moves, i, D[i][deficit]-D[i][surplus]); /*we insert i in moves with the gain we get from moving i from surplus to deficit as key */
	}
      part[PQ_deleteMax(&moves)] = deficit; /*we put the i from moves with the highest gain in deficit*/
      PQ_exit(&moves);
    }
}

void destruction(PriorityQueue * Qpart, PriorityQueue * Q, PriorityQueue * Qinst, double ** D, int n, int k)
{
  int i;
  PQ_exit(Qpart);
  for(i=0; i<k; ++i)
    PQ_exit(&Q[i]);
  free(Q);
  for(i=0; i<n; ++i)
    {
      PQ_exit(&Qinst[i]);
    }
  free(Qinst);

  for(i=0; i<n; ++i)
    free(D[i]);
  free(D);
}


int  *kpartition_greedy2(int k, double **comm, int n, int nb_try_max, int *constraints, int nb_constraints)
{
  int *res = NULL, *best_res=NULL, *size = NULL;
  int i,j,nb_trials;
  int max_size;
  double cost, best_cost = -1;

  for( nb_trials = 0 ; nb_trials < nb_try_max ; nb_trials++ ){
    res = (int *)malloc(sizeof(int)*n);
    for ( i = 0 ; i < n ; ++i )
      res[i] = -1;

    size = (int *)calloc(k,sizeof(int));
    max_size = n/k;

    /* put "dumb" vertices in the correct partition if there are any*/
    if (nb_constraints){ /*if there are at least one constraint*/
      int nb_real_nodes = n-nb_constraints; /*this is the number of "real" nodes by opposition to the dumb ones*/
      for(i=0; i<nb_constraints; ++i) /*for each constraint*/
	{
	  int i_part = constraints[i]/max_size; /*we compute its partition*/
	  res[nb_real_nodes+i] = i_part; /*and we set it in partition vector*/
	  size[i_part]++; /*we update the partition's size*/
	}
    }

    /* choose k initial "true" vertices at random and put them in a different partition */
    for ( i = 0 ; i < k ; ++i ){
      /* if the partition is full of dumb vertices go to next partition*/
      if(size[i] >= max_size)
	continue;
      /* find a vertex not already partitionned*/
      do{
	/* call the mersenne twister PRNG of tm_mt.c*/
	j =  genrand_int32() % n;
      } while ( res[j] != -1 );
      /* allocate and update size of partition*/
      res[j] = i;
      /* printf("random: %d -> %d\n",j,i); */
      size[i]++;
    }

    /* allocate each unallocated vertices in the partition that maximize the communication*/
    for( i = 0 ;  i < n ; ++i )
      if( res[i] == -1)
	allocate_vertex2(i, res, comm, n-nb_constraints, size, max_size);

    cost = eval_cost2(res,n-nb_constraints,comm);
    /*print_1D_tab(res,n);
    printf("cost=%.2f\n",cost);*/
    if((cost<best_cost) || (best_cost == -1)){
      best_cost=cost;
      free(best_res);
      best_res=res;
    }else
      free(res);

    free(size);
  }

  /*print_1D_tab(best_res,n);
  printf("best_cost=%.2f\n",best_cost);
  */
  return best_res;
}

void allocate_vertex2(int u, int *res, double **comm, int n, int *size, int max_size)
{
  int i,best_part = -1;
  double cost, best_cost = -1;

  /*printf("\n");
    print_1D_tab(res,n);*/
  for( i = 0 ; i < n ; ++i){
    if (( res[i] != -1 ) && ( size[res[i]] < max_size )){
      cost = comm[u][i];
      if (( cost > best_cost)){
	best_cost = cost;
	best_part = res[i];
      }
    }
  }

  /*  printf("size[%d]: %d\n",best_part, size[best_part]);*/
  /* printf("putting(%.2f): %d -> %d\n",best_cost, u, best_part); */

  res[u] = best_part;
  size[best_part]++;
}

double eval_cost2(int *partition, int n, double **comm)
{
  double cost = 0;
  int i,j;

  for( i = 0 ; i < n ; ++i )
    for( j = i+1 ; j < n ; ++j )
      if(partition[i] != partition[j])
	cost += comm[i][j];

  return cost;
}

int* build_p_vector(double **comm, int n, int k, int greedy_trials, int * constraints, int nb_constraints)
{
  int * part = NULL;
  if(greedy_trials>0) /*if greedy_trials > 0 then we use kpartition_greedy with greedy_trials trials*/
    {
      part = kpartition_greedy2(k, comm, n, greedy_trials, constraints, nb_constraints);
    }
  else
    {
      int * size = calloc(k, sizeof(int));
      int i,j;
      int nodes_per_part = n/k;
      int nb_real_nodes = n-nb_constraints;
      part = malloc(sizeof(int) * n);
      for(i=0; i<nb_constraints; i++) /*for each constraints*/
	{
	  int i_part = constraints[i]/nodes_per_part; /*we compute the partition where we have to put this constraint*/
	  part[nb_real_nodes+i] = i_part;
	  size[i_part]++;
	}
      j=0;
      /* now we have to fill the partitions with the "real" nodes */
      for(i=0; i<nb_real_nodes; i++) /*for each node*/
	{
	  if(size[j] < nodes_per_part) /*if j partition isn't full*/
	    {
	      size[j]++;
	      part[i] = j; /*then we put the node in this part*/
	    }
	  else /*otherwise we decrement i to get the same node in the next loop*/
	    {
	      i--;
	    }
	  j = (j+1)%k; /*and we change j to the next partition*/
	}
      free(size);
    }
  return part;
}
