#include <float.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <assert.h>
#include <pthread.h>

#include "treematch.h"
#include "tm_tree.h"
#include "tm_mapping.h"
#include "tm_timings.h"
#include "tm_bucket.h"
#include "tm_kpartitioning.h"
#include "tm_verbose.h"
#include "tm_thread_pool.h"


#define MIN(a, b) ((a)<(b)?(a):(b))
#define MAX(a, b) ((a)>(b)?(a):(b))

#ifndef __CHARMC__
#define __CHARMC__ 0
#endif

#if __CHARMC__
#error #include "converse.h"
#else
#define CmiLog2(VAL)  log2((double)(VAL))
#endif



static int verbose_level = ERROR;
static int exhaustive_search_flag = 0;

void free_list_child(tm_tree_t *);void free_tab_child(tm_tree_t *);
double choose (long, long);void display_node(tm_tree_t *);
void clone_tree(tm_tree_t *, tm_tree_t *);
double *aggregate_obj_weight(tm_tree_t *, double *, int);
tm_affinity_mat_t *aggregate_com_mat(tm_tree_t *, tm_affinity_mat_t *, int);
double eval_grouping(tm_affinity_mat_t *, tm_tree_t **, int);
group_list_t *new_group_list(tm_tree_t **, double, group_list_t *);
void add_to_list(group_list_t *, tm_tree_t **, int, double);
void  list_all_possible_groups(tm_affinity_mat_t *, tm_tree_t *, int, int, int, tm_tree_t **, group_list_t *);
int independent_groups(group_list_t **, int, group_list_t *, int);
void display_selection (group_list_t**, int, int, double);
void display_grouping (tm_tree_t *, int, int, double);
int recurs_select_independent_groups(group_list_t **, int, int, int, int,
				     int, double, double *, group_list_t **, group_list_t **);
int test_independent_groups(group_list_t **, int, int, int, int, int, double, double *,
			    group_list_t **, group_list_t **);
void delete_group_list(group_list_t *);
int group_list_id(const void*, const void*);
int group_list_asc(const void*, const void*);
int group_list_dsc(const void*, const void*);
int weighted_degree_asc(const void*, const void*);
int weighted_degree_dsc(const void*, const void*);
int  select_independent_groups(group_list_t **, int, int, int, double *, group_list_t **, int, double);
int  select_independent_groups_by_largest_index(group_list_t **, int, int, int, double *,
						group_list_t **, int, double);
void list_to_tab(group_list_t *, group_list_t **, int);
void display_tab_group(group_list_t **, int, int);
int independent_tab(tm_tree_t **, tm_tree_t **, int);
void compute_weighted_degree(group_list_t **, int, int);
void  group(tm_affinity_mat_t *, tm_tree_t *, tm_tree_t *, int, int, int, double *, tm_tree_t **);
void  fast_group(tm_affinity_mat_t *, tm_tree_t *, tm_tree_t *, int, int, int, double *, tm_tree_t **, int *, int);
int adjacency_asc(const void*, const void*);
int adjacency_dsc(const void*, const void*);
		 void super_fast_grouping(tm_affinity_mat_t *, tm_tree_t *, tm_tree_t *, int, int);
tm_affinity_mat_t *build_cost_matrix(tm_affinity_mat_t *, double *, double);
void group_nodes(tm_affinity_mat_t *, tm_tree_t *, tm_tree_t *, int , int, double*, double);
double fast_grouping(tm_affinity_mat_t *, tm_tree_t *, tm_tree_t *, int, int, double);
void complete_aff_mat(tm_affinity_mat_t **, int, int);
void complete_obj_weight(double **, int, int);
void create_dumb_tree(tm_tree_t *, int, tm_topology_t *);
void complete_tab_node(tm_tree_t **, int, int, int, tm_topology_t *);
void set_deb_tab_child(tm_tree_t *, tm_tree_t *, int);
tm_tree_t *build_level_topology(tm_tree_t *, tm_affinity_mat_t *, int, int, tm_topology_t *, double *, double *);
int check_constraints(tm_topology_t  *, int **);
tm_tree_t *bottom_up_build_tree_from_topology(tm_topology_t *, tm_affinity_mat_t *, double *, double *);
void free_non_constraint_tree(tm_tree_t *);
void free_constraint_tree(tm_tree_t *);
void free_tab_double(double**, int);
void free_tab_int(int**, int );
void partial_aggregate_aff_mat (int, void **, int);
void free_affinity_mat(tm_affinity_mat_t *aff_mat);
int int_cmp_inc(const void* x1, const void* x2);





void tm_set_exhaustive_search_flag(int new_val){
  exhaustive_search_flag = new_val;
}

int tm_get_exhaustive_search_flag(){
  return exhaustive_search_flag;
}


void free_affinity_mat(tm_affinity_mat_t *aff_mat){
  free_tab_double(aff_mat->mat, aff_mat->order);
  FREE(aff_mat->sum_row);
  FREE(aff_mat);
}



void free_list_child(tm_tree_t *tree)
{
  int i;

  if(tree){
    for(i=0;i<tree->arity;i++)
      free_list_child(tree->child[i]);

    FREE(tree->child);
    if(tree->dumb)
      FREE(tree);
  }
}
void free_tab_child(tm_tree_t *tree)
{
  if(tree){
    free_tab_child(tree->tab_child);
    FREE(tree->tab_child);
  }
}

void free_non_constraint_tree(tm_tree_t *tree)
{
  int d = tree->dumb;

  free_tab_child(tree);
  free_list_child(tree);
  if(!d)
    FREE(tree);
}

void free_constraint_tree(tm_tree_t *tree)
{
  int i;
  if(tree){
    for(i=0;i<tree->arity;i++)
      free_constraint_tree(tree->child[i]);
    FREE(tree->child);
    FREE(tree);
  }
}


void tm_free_tree(tm_tree_t *tree)
{
  if(tree->constraint)
    free_constraint_tree(tree);
  else
    free_non_constraint_tree(tree);
}

double choose (long n, long k)
{
  /* compute C_n_k */
  double res = 1;
  int i;

  for( i = 0 ; i < k ; i++ ){
    res *= ((double)(n-i)/(double)(k-i));
  }
  return res;
}

void set_node(tm_tree_t *node, tm_tree_t ** child, int arity, tm_tree_t *parent,
	      int id, double val, tm_tree_t *tab_child, int depth)
{
  static int uniq = 0;
  node->child = child;
  node->arity = arity;
  node->tab_child = tab_child;
  node->parent = parent;
  node->id = id;
  node->val = val;
  node->uniq = uniq++;
  node->depth= depth;
  node->dumb = 0;
}

void display_node(tm_tree_t *node)
{
  if (verbose_level >= DEBUG)
    printf("child : %p\narity : %d\nparent : %p\nid : %d\nval : %f\nuniq : %d\n\n",
	   (void *)(node->child), node->arity, (void *)(node->parent), node->id, node->val, node->uniq);
}

void clone_tree(tm_tree_t *new, tm_tree_t *old)
{
  int i;
  new->child = old->child;
  new->parent = old->parent;
  new->tab_child = old->tab_child;
  new->val = old->val;
  new->arity = old->arity;
  new->depth = old->depth;
  new->id = old->id;
  new->uniq = old->uniq;
  new->dumb = old->dumb;
  for( i = 0 ; i < new->arity ; i++ )
    new->child[i]->parent = new;
}


double *aggregate_obj_weight(tm_tree_t *new_tab_node, double *tab, int M)
{
  int i, i1, id1;
  double *res = NULL;

  if(!tab)
    return NULL;

  res = (double*)MALLOC(M*sizeof(double));

  for( i = 0 ; i < M ; i++ ){
    res[i] = 0.0;
    for( i1 = 0 ; i1 < new_tab_node[i].arity ; i1++ ){
      id1 = new_tab_node[i].child[i1]->id;
      res[i] += tab[id1];
    }
  }
  return res;
}



void partial_aggregate_aff_mat (int nb_args, void **args, int thread_id){
  int inf = *(int*)args[0];
  int sup = *(int*)args[1];
  double **old_mat = (double**)args[2];
  tm_tree_t *tab_node = (tm_tree_t*)args[3];
  int M = *(int*)args[4];
  double **mat = (double**)args[5];
  double *sum_row = (double*)args[6];
  int i, j, i1, j1;
  int id1, id2;


  if(nb_args != 7){
    if(verbose_level >= ERROR)
      fprintf(stderr, "Thread %d: Wrong number of args in %s: %d\n", thread_id, __func__, nb_args);
    exit(-1);
  }

  if(verbose_level >= INFO)
    printf("Aggregate in parallel (%d-%d)\n", inf, sup-1);

  for( i = inf ; i < sup ; i++ )
    for( j = 0 ; j < M ; j++ ){
      if(i != j){
	for( i1 = 0 ; i1 < tab_node[i].arity ; i1++ ){
	  id1 = tab_node[i].child[i1]->id;
	  for( j1 = 0 ; j1 < tab_node[j].arity ; j1++ ){
	    id2 = tab_node[j].child[j1]->id;
	    mat[i][j] += old_mat[id1][id2];
	    /* printf("mat[%d][%d]+=old_mat[%d][%d]=%f\n", i, j, id1, id2, old_mat[id1][id2]);*/
	  }
	  sum_row[i] += mat[i][j];
	}
      }
    }
}


static tm_affinity_mat_t *aggregate_aff_mat(tm_tree_t *tab_node, tm_affinity_mat_t *aff_mat, int M)
{
  int i, j, i1, j1, id1, id2;
  double **new_mat = NULL, **old_mat = aff_mat->mat;
  double *sum_row = NULL;

  new_mat = (double**)MALLOC(M*sizeof(double*));
  for( i = 0 ; i < M ; i++ )
    new_mat[i] = (double*)CALLOC((M), sizeof(double));

  sum_row = (double*)CALLOC(M, sizeof(double));

  if(M>512){ /* perform this part in parallel*/
    int id;
    int nb_threads;
    work_t **works;
    int *inf;
    int *sup;

    nb_threads = MIN(M/512, get_nb_threads());
    works = (work_t**)MALLOC(sizeof(work_t*)*nb_threads);
    inf = (int*)MALLOC(sizeof(int)*nb_threads);
    sup = (int*)MALLOC(sizeof(int)*nb_threads);
    for(id=0;id<nb_threads;id++){
      void **args=(void**)MALLOC(sizeof(void*)*7);
      inf[id]=id*M/nb_threads;
      sup[id]=(id+1)*M/nb_threads;
      if(id == nb_threads-1) sup[id]=M;
      args[0]=(void*)(inf+id);
      args[1]=(void*)(sup+id);
      args[2]=(void*)old_mat;
      args[3]=(void*)tab_node;
      args[4]=&M;
      args[5]=(void*)new_mat;
      args[6]=(void*)sum_row;

      works[id]= create_work(7, args, partial_aggregate_aff_mat);
      if(verbose_level >= DEBUG)
	printf("Executing %p\n", (void *)works[id]);

      submit_work( works[id], id);
    }

    for(id=0;id<nb_threads;id++){
      wait_work_completion(works[id]);
      FREE(works[id]->args);
    }


    FREE(inf);
    FREE(sup);
    FREE(works);

  }else{
  for( i = 0 ; i < M ; i++ )
    for( j = 0 ; j < M ; j++ ){
      if(i != j){
	for( i1 = 0 ; i1 < tab_node[i].arity ; i1++ ){
	  id1 = tab_node[i].child[i1]->id;
	  for( j1 = 0 ; j1 < tab_node[j].arity ; j1++ ){
	    id2 = tab_node[j].child[j1]->id;
	    new_mat[i][j] += old_mat[id1][id2];
	    /* printf("mat[%d][%d]+=old_mat[%d][%d]=%f\n", i, j, id1, id2, old_mat[id1][id2]);*/
	  }
	  sum_row[i] += new_mat[i][j];
	}
      }
    }
  }
  return new_affinity_mat(new_mat, sum_row, M);
}

void free_tab_double(double**tab, int mat_order)
{
  int i;
  for( i = 0 ; i < mat_order ; i++ )
    FREE(tab[i]);
  FREE(tab);
}

void free_tab_int(int**tab, int mat_order)
{
  int i;
  for( i = 0 ; i < mat_order ; i++ )
    FREE(tab[i]);
  FREE(tab);
}

void display_tab(double **tab, int mat_order)
{
  int i, j;
  double line, total = 0;
  int vl = tm_get_verbose_level();

  for( i = 0 ; i < mat_order ; i++ ){
    line = 0;
    for( j = 0 ; j < mat_order ; j++ ){
      if(vl >= WARNING)
	printf("%g ", tab[i][j]);
      else
	fprintf(stderr, "%g ", tab[i][j]);
      line += tab[i][j];
    }
    total += line;
    /* printf(": %g", line);*/
    if(vl >= WARNING)
      printf("\n");
    else
      fprintf(stderr, "\n");
  }
  /* printf("Total: %.2f\n", total);*/
}


double eval_grouping(tm_affinity_mat_t *aff_mat, tm_tree_t **cur_group, int arity)
{
  double res = 0;
  int i, j, id, id1, id2;
  double **mat = aff_mat->mat;
  double * sum_row = aff_mat -> sum_row;

  /*display_tab(tab, mat_order);*/

  for( i = 0 ; i < arity ; i++ ){
    id = cur_group[i]->id;
    res += sum_row[id];
  }

  for( i = 0 ; i < arity ; i++ ){
    id1 = cur_group[i]->id;
    for( j = 0 ; j < arity ; j++ ){
      id2 = cur_group[j]->id;
      /*printf("res-=tab[%d][%d]=%f\n", id1, id2, tab[id1][id2]);*/
      res -= mat[id1][id2];
    }
  }
  /*printf(" = %f\n", res);*/
  return res;
}


group_list_t *new_group_list(tm_tree_t **tab, double val, group_list_t *next)
{
  group_list_t *res = NULL;

  res = (group_list_t *)MALLOC(sizeof(group_list_t));
  res->tab = tab;
  res->val = val;
  res->next = next;
  res->sum_neighbour = 0;
  return res;
}


void add_to_list(group_list_t *list, tm_tree_t **cur_group, int arity, double val)
{
  group_list_t *elem = NULL;
  tm_tree_t **tab = NULL;
  int i;

  tab=(tm_tree_t **)MALLOC(sizeof(tm_tree_t *)*arity);

  for( i = 0 ; i < arity ; i++ ){
    tab[i] = cur_group[i];
    if(verbose_level>=DEBUG)
      printf("cur_group[%d]=%d ", i, cur_group[i]->id);
  }
  if(verbose_level>=DEBUG)
    printf(": %f\n", val);

  /*printf("\n");*/
  elem = new_group_list(tab, val, list->next);
  list->next = elem;
  list->val++;
}


void  list_all_possible_groups(tm_affinity_mat_t *aff_mat, tm_tree_t *tab_node, int id, int arity, int depth,
			       tm_tree_t **cur_group, group_list_t *list)
{
  double val;
  int i;
  int mat_order = aff_mat->order;

  if(depth == arity){
    val = eval_grouping(aff_mat, cur_group, arity);
    add_to_list(list, cur_group, arity, val);
    return;
  }else if( (mat_order+depth) >= (arity+id) ){
    /*}else if(1){*/
    for( i = id ; i < mat_order ; i++ ){
      if(tab_node[i].parent)
	continue;
      cur_group[depth] = &tab_node[i];
      if(verbose_level>=DEBUG)
	printf("%d<-%d\n", depth, i);
      list_all_possible_groups(aff_mat, tab_node, i+1, arity, depth+1, cur_group, list);
    }
  }
}

void update_val(tm_affinity_mat_t *aff_mat, tm_tree_t *parent)
{
  /* int i; */

  parent->val = eval_grouping(aff_mat, parent->child, parent->arity);
  /*printf("connecting: ");*/
  /*for( i = 0 ; i < parent->arity ; i++ ){ */
    /*printf("%d ", parent->child[i]->id);*/
    /*  if(parent->child[i]->parent!=parent){
	parent->child[i]->parent=parent;
	}else{
	fprintf(stderr, "redundant operation!\n");
	exit(-1);
	}*/
  /* } */
  /*printf(": %f\n", parent->val);*/
}

int independent_groups(group_list_t **selection, int d, group_list_t *elem, int arity)
{
  int i, j, k;

  if(d == 0)
    return 1;

  for( i = 0 ; i < arity ; i++ )
    for( j = 0 ; j < d ; j++ )
      for( k = 0 ; k < arity ; k++ )
	if(elem->tab[i]->id == selection[j]->tab[k]->id)
	  return 0;
  return 1;
}



void display_selection (group_list_t** selection, int M, int arity, double val)
{
  int i, j;
  double local_val = 0;

  if(verbose_level < INFO)
    return;


  for( i = 0 ; i < M ; i++ ) {
    for( j = 0 ; j < arity ; j++ )
      printf("%d ", selection[i]->tab[j]->id);
    printf("(%d)-- ", selection[i]->id);
    local_val+=selection[i]->val;
  }
  printf(":%f -- %f\n", val, local_val);
}


void display_grouping (tm_tree_t *father, int M, int arity, double val)
{
  int i, j;

  if(verbose_level < INFO)
    return;

  printf("Grouping : ");
  for( i = 0  ; i < M ; i++ ){
    for( j = 0 ; j < arity ; j++ )
      printf("%d ", father[i].child[j]->id);
    printf("-- ");
  }
  printf(":%f\n", val);
}


int recurs_select_independent_groups(group_list_t **tab, int i, int n, int arity, int d, int M, double val, double *best_val, group_list_t **selection, group_list_t **best_selection)
{
  group_list_t *elem = NULL;
  /*
    if(val>=*best_val)
    return 0;
  */

  if( d == M ){
    if(verbose_level >= DEBUG)
      display_selection(selection, M, arity, val);
    if( val < *best_val ){
      *best_val = val;
      for( i = 0 ; i < M ; i++ )
	best_selection[i] = selection[i];
      return 1;
    }
    return 0;
  }

  while( i < n ){
    elem = tab[i];
    if(independent_groups(selection, d, elem, arity)){
      if(verbose_level >= DEBUG)
	printf("%d: %d\n", d, i);
      selection[d] = elem;
      val += elem->val;
      return recurs_select_independent_groups(tab, i+1, n, arity, d+1, M, val, best_val, selection, best_selection);
    }
    i++;
  }
  return 0;
}



int test_independent_groups(group_list_t **tab, int i, int n, int arity, int d, int M, double val, double *best_val, group_list_t **selection, group_list_t **best_selection)
{
  group_list_t *elem = NULL;

  if( d == M ){
    /*display_selection(selection, M, arity, val);*/
    return 1;
  }

  while( i < n ){
    elem = tab[i];
    if(independent_groups(selection, d, elem, arity)){
      /*printf("%d: %d\n", d, i);*/
      selection[d] = elem;
      val += elem->val;
      return recurs_select_independent_groups(tab, i+1, n, arity, d+1, M, val, best_val, selection, best_selection);
    }
    i++;
  }
  return 0;
}

void  delete_group_list(group_list_t *list)
{

  if(list){
    delete_group_list(list->next);
    FREE(list->tab);
    FREE(list);
  }
}

int group_list_id(const void* x1, const void* x2)
{
  group_list_t *e1 = NULL, *e2= NULL;

  e1 = *((group_list_t**)x1);
  e2 = *((group_list_t**)x2);

  return (e1->tab[0]->id < e2->tab[0]->id) ? - 1 : 1;
}

int group_list_asc(const void* x1, const void* x2)
{
  group_list_t *e1 = NULL, *e2 = NULL;

  e1 = *((group_list_t**)x1);
  e2 = *((group_list_t**)x2);

  return (e1->val < e2->val) ? - 1 : 1;
}

int group_list_dsc(const void* x1, const void* x2)
{
  group_list_t *e1 = NULL, *e2 = NULL;

  e1 = *((group_list_t**)x1);
  e2 = *((group_list_t**)x2);

  return (e1->val > e2->val) ? -1 : 1;
}

int weighted_degree_asc(const void* x1, const void* x2)
{
  group_list_t *e1= NULL, *e2 = NULL;

  e1 = *((group_list_t**)x1);
  e2 = *((group_list_t**)x2);

  return (e1->wg > e2->wg) ? 1 : -1;
}

int weighted_degree_dsc(const void* x1, const void* x2)
{
  group_list_t *e1 = NULL, *e2 = NULL;

  e1 = *((group_list_t**)x1);
  e2 = *((group_list_t**)x2);

  return (e1->wg > e2->wg) ? - 1 : 1;
}

int  select_independent_groups(group_list_t **tab_group, int n, int arity, int M, double *best_val,
			       group_list_t **best_selection, int bound, double max_duration)
{
  int i, j;
  group_list_t **selection = NULL;
  double val, duration;
  CLOCK_T time1, time0;

  if(verbose_level>=DEBUG){
    for(i=0;i<n;i++){
      for(j=0;j<arity;j++){
	printf("%d ", tab_group[i]->tab[j]->id);
      }
      printf(" : %f\n", tab_group[i]->val);
    }
  }



  selection = (group_list_t **)MALLOC(sizeof(group_list_t*)*M);
  CLOCK(time0);
  for( i = 0 ; i < MIN(bound, n) ; i++ ){
    /* if(!(i%100)) {printf("%d/%d ", i, MIN(bound, n)); fflush(stdout);} */
    selection[0] = tab_group[i];
    val = tab_group[i]->val;
    recurs_select_independent_groups(tab_group, i+1, n, arity, 1, M, val, best_val, selection, best_selection);
    if((!(i%5)) && (max_duration>0)){
     CLOCK(time1);
      duration = CLOCK_DIFF(time1, time0);
      if(duration>max_duration){
	FREE(selection);
	return 1;
      }
    }
  }
  FREE(selection);


  if(verbose_level>=INFO)
    display_selection(best_selection, M, arity, *best_val);
  return 0;
}


static int8_t** init_independent_group_mat(int n, group_list_t **tab_group, int arity){
  int i, j, ii, jj;
  int8_t **indep_mat = (int8_t **)MALLOC(sizeof(int8_t*) *n);

  for( i=0 ; i<n ; i++){
    indep_mat[i] = (int8_t *)MALLOC(sizeof(int8_t) *(i+1));

    /* always i>j in indep_mat[i][j] */
    for(j=0 ; j<i+1 ; j++){
      group_list_t *elem1 = tab_group[i];
      group_list_t *elem2 = tab_group[j];
      for( ii = 0 ; ii < arity ; ii++ ){
	for( jj = 0 ; jj < arity ; jj++ ){
	  if(elem1->tab[ii]->id == elem2->tab[jj]->id){
	    indep_mat[i][j] = 0;
	    goto done;
	  }
	}
      }
      indep_mat[i][j] = 1;
    done: ;
    }
  }


  return indep_mat;
}

static int independent_groups_mat(group_list_t **selection, int selection_size, group_list_t *elem, int8_t **indep_mat)
{
  int i;
  int id_elem = elem->id;
  int id_select;


  if(selection_size == 0)
    return 1;

  for(i=0; i<selection_size; i++){
    id_select = selection[i] -> id;
    /* I know that id_elem > id_select, always */
    if(indep_mat[id_elem][id_select] == 0 )
      return 0;
  }
  return 1;
}

  static long int x=0;
  static long int y=0;


static int thread_derecurs_exhaustive_search(group_list_t **tab_group, int i, int nb_groups, int arity, int depth, int solution_size,
				      double val, double *best_val, group_list_t **selection, group_list_t **best_selection,
				      int8_t **indep_mat, pthread_mutex_t *lock, int thread_id, int *tab_i, int start_depth){


  group_list_t *elem = NULL;
  int nb_groups_to_find =0;
  int nb_available_groups = 0;

 stack:
  nb_groups_to_find = solution_size - depth;
  nb_available_groups = nb_groups - i;
  if( depth == solution_size ){
    if(verbose_level >= DEBUG)
      display_selection(selection, solution_size, arity, val);
    if( val < *best_val ){
      pthread_mutex_lock(lock);
      if(verbose_level >= INFO)
	printf("\n---------%d: best_val= %f\n", thread_id, val);
      *best_val = val;
      for( i = 0 ; i < solution_size ; i++ )
	best_selection[i] = selection[i];
      pthread_mutex_unlock(lock);
    }
    if(depth>2)
      goto unstack;
    else
      return 0;
  }

  if(nb_groups_to_find > nb_available_groups){ /*if there not enough groups available*/
    if(depth>start_depth)
      goto unstack;
    else
      return 0;
  }



  while( i < nb_groups ){
    elem = tab_group[i];
    y++;
    if(val+elem->val < *best_val){
      if(val+elem->bound[nb_groups_to_find]>*best_val){
	x++;
	/* printf("\ni=%d, val=%.0f, elem->val = %.0f, elem->bound[%d] = %.0f, best_val = %.0f\n", */
	/*        i,val,elem->val,nb_groups_to_find,elem->bound[nb_groups_to_find],*best_val); */
	/* exit(-1); */

	/* printf("x=%ld y=%ld\n",x,y); */
	if(depth>start_depth)
	  goto unstack;
	else
	  return 0;
      }

      if(independent_groups_mat(selection, depth, elem, indep_mat)){
	if(verbose_level >= DEBUG)
	  printf("%d: %d\n", depth, i);
	selection[depth] = elem;
	val += selection[depth]->val;
	tab_i[depth]=i;
	depth ++;
	i++;
	goto stack;
      unstack:
	depth --;
	val -= selection[depth]->val;
	i=tab_i[depth];
      }
    }
    i++;
    nb_available_groups = nb_groups - i;
    nb_groups_to_find = solution_size - depth;
    if(nb_groups_to_find > nb_available_groups){ /*if there not enough groups available*/
      if(depth>start_depth)
	goto unstack;
      else
	return 0;
    }
  }

  if(depth>start_depth)
    goto unstack;

  return 0;
}

#if 0
static group_list_t * group_dup(group_list_t *group, int nb_groups){
   group_list_t *elem = NULL;
   /* tm_tree_t **tab = NULL; */
   double *bound;
   size_t bound_size = nb_groups-group->id+2;

   /* tab = (tm_tree_t **)MALLOC(sizeof(tm_tree_t *)*arity); */
   /* memcpy(tab, group->tab, sizeof(tm_tree_t *)*arity); */

   bound = (double*) MALLOC(bound_size*sizeof(double));
   memcpy(bound, group->bound, bound_size*sizeof(double));

   elem = (group_list_t*) MALLOC(sizeof(group_list_t));

   elem-> tab            = group->tab;
   elem-> val            = group->val;
   elem-> sum_neighbour  = group->sum_neighbour;
   elem-> wg             = group ->wg;
   elem-> id             = group->id;
   elem-> bound          = bound;
   elem-> next           = NULL;
   return elem;

}
#endif

#if 0
static group_list_t **  tab_group_dup(group_list_t **tab_group, int nb_groups){
  group_list_t **res;
  int i;

  res = (group_list_t**)MALLOC(sizeof(group_list_t*)*nb_groups);

  for(i=0 ; i<nb_groups ; i++){
    res[i] = group_dup(tab_group[i], nb_groups);
    if(i)
      res[i-1]->next = res[i];
  }

  return res;
}
#endif

#if 0
static int8_t **indep_mat_dup(int8_t** mat, int n){
  int i;
  int8_t ** res = (int8_t**)MALLOC(sizeof(int8_t*)*n);
  int row_len;
  /* use indep_mat[i][j] with i<j only*/
  for( i=0 ; i<n ; i++){
    row_len = n-i;
    res[i] = (int8_t*)MALLOC(sizeof(int8_t)*row_len);
    memcpy(res[i], mat[i], sizeof(int8_t)*row_len);
  }

  return res;
}
#endif

static void  partial_exhaustive_search(int nb_args, void **args, int thread_id){
  int i, j;
  group_list_t **selection = NULL;
  double val;
  int n = *(int*) args[1];
  int arity = *(int*) args[2];
  /* group_list_t **tab_group = tab_group_dup((group_list_t **) args[0], n, arity); */
  group_list_t **tab_group = (group_list_t **) args[0];
  int solution_size = *(int*) args[3];
  double *best_val= (double *) args[4];
  group_list_t **best_selection = (group_list_t **) args[5];
  /* int8_t **indep_mat = indep_mat_dup((int8_t **) args[6],n); */
  int8_t **indep_mat = (int8_t **) args[6];
  work_unit_t *work = (work_unit_t *) args[7];
  pthread_mutex_t *lock = (pthread_mutex_t *) args[8];
  int *tab_i;
  int id, id1, id2;
  int total_work = work->nb_work;
  int cur_work = 0;

  TIC;

  if(nb_args!=9){
    if(verbose_level>=ERROR){
      fprintf(stderr, "Id: %d: bad number of argument for function %s: %d instead of 9\n", thread_id, __func__, nb_args);
      return;
    }
  }

  pthread_mutex_lock(lock);
  TIC;
  pthread_mutex_unlock(lock);

  tab_i = (int*) MALLOC(sizeof(int)*solution_size);
  selection = (group_list_t **)MALLOC(sizeof(group_list_t*)*solution_size);



  while(work->tab_group){
      pthread_mutex_lock(lock);
      if(!work->done){
	work->done = 1;
	pthread_mutex_unlock(lock);
      }else{
	pthread_mutex_unlock(lock);
	work=work->next;
	cur_work++;
	continue;
      }

      /* for(i=0;i<work->nb_groups;i++){ */
      /* 	printf("%d ",work->tab_group[i]); */
      /* } */
      if(verbose_level>=INFO){
	fprintf(stdout, "\r%d: %.2f%% of search space explored...", thread_id,(100.0*cur_work)/total_work);
	fflush(stdout);
      }
      for(i=0;i<work->nb_groups;i++){
	id1 = work->tab_group[i];
	for(j=i+1;j<work->nb_groups;j++){
	  id2 = work->tab_group[j];
	  if(!indep_mat[id2][id1]){
	    goto next_work;
	  }
	}
      }


      val = 0;
      for(i=0;i<work->nb_groups;i++){
	id = work->tab_group[i];
	selection[i] = tab_group[id];
	val +=  tab_group[id]->val;
      }
      thread_derecurs_exhaustive_search(tab_group, id+1, n, arity, work->nb_groups, solution_size, val, best_val, selection, best_selection, indep_mat, lock, thread_id, tab_i, work->nb_groups);
  next_work:
      work=work->next;
      cur_work++;
  }





  /* for( i=0 ; i<n ; i++){ */
  /*   /\* FREE(tab_group[i]->tab); *\/ */
  /*   FREE(tab_group[i]->bound); */
  /*   FREE(tab_group[i]); */
  /* } */
  /* FREE(tab_group); */
  FREE(selection);
  FREE(tab_i);
  /* for( i=0 ; i<n ; i++){ */
  /*   FREE(indep_mat[i]); */
  /* } */

  /* FREE(indep_mat);*/

  pthread_mutex_lock(lock);
  double duration = TOC;
  pthread_mutex_unlock(lock);
  if(verbose_level>=INFO){
    printf("Thread %d done in %.3f!\n" , thread_id, duration);
  }
}


#if 0
static int dbl_cmp_dec(const void* x1,const void* x2)
{
  return *((double *)x1) > *((double *)x2) ? -1 : 1;
}
#endif
static int dbl_cmp_inc(const void* x1,const void* x2)
{
  return *((double *)x1) < *((double *)x2) ? -1 : 1;
}



static double *build_bound_array(double *tab, int n){
  int i;
  double *bound;

  if (n==0)
    return NULL;

  bound = (double *)MALLOC(sizeof(double)*(n+2));
  qsort(tab, n, sizeof(double), dbl_cmp_inc);



  if(verbose_level>=DEBUG){
    printf("T(%d): ",n);
    for(i = 0; i<n ; i++)
      printf("%.0f ",tab[i]);
    printf("\n");
  }
  bound[0] = 0;
  bound[1] = tab[0];
  for(i = 2; i<n+1 ; i++){
    bound[i] = bound[i-1] + tab[i-1];
  }

  bound[n+1] = DBL_MAX;

  return bound;
}

static work_unit_t *create_work_unit(work_unit_t *cur,  int *tab,int size){
  work_unit_t *res = (work_unit_t *) CALLOC(1,sizeof(work_unit_t));
  int *tab_group = MALLOC(size*sizeof(int));
  memcpy(tab_group, tab, size*sizeof(int));
  cur->tab_group = tab_group;
  cur->nb_groups = size;
  cur->done = 0;
  cur->next = res;
  return res;
}

static work_unit_t *generate_work_units(work_unit_t *cur,  int i, int id, int *tab_group,int size, int id_max){

  tab_group[i] = id;
  if(i==size-1){
    return create_work_unit(cur,tab_group,size);
  }

  if(id == id_max-1){
    return cur;
  }

  id++;
  for(;id < id_max;id++){
    cur = generate_work_units(cur,i+1,id,tab_group, size, id_max);
  }

  return cur;
}


static work_unit_t *create_tab_work(int n){
  int work_size = 4;
  int i;
  work_unit_t *cur,*res = (work_unit_t *) CALLOC(1,sizeof(work_unit_t));
  int *tab_group = MALLOC(work_size*sizeof(int));
  cur = res;
  cur = generate_work_units(cur,0,0,tab_group,3,n);
  cur = generate_work_units(cur,0,1,tab_group,2,n);
  cur = generate_work_units(cur,0,2,tab_group,2,n);

  for(i=3;i<n;i++)
    cur = generate_work_units(cur,0,i,tab_group,1,n);

  for(cur = res; cur->tab_group; cur = cur-> next)
    res->nb_work++;

  printf("nb_work= %d\n",res->nb_work);

  FREE(tab_group);

  return res;
}


static int thread_exhaustive_search(group_list_t **tab_group, int nb_groups, int arity, int solution_size, double *best_val,
			 group_list_t **best_selection){

  pthread_mutex_t lock;
  int nb_threads;
  work_t **works;
  int i, j;
  int id;
  /* matrix of indepedency between groups (i.e; 2 groups are independent if they
  are composed of different ids) */
  int8_t **indep_mat;
  double *val_array;
  double duration;
  work_unit_t *work_list;
  TIC;

  pthread_mutex_init(&lock, NULL);
  nb_threads   = get_nb_threads();
  nb_threads   = 4;
  works        = (work_t**)MALLOC(sizeof(work_t*)*nb_threads);

  work_list = create_tab_work(nb_groups);

  if(verbose_level>=DEBUG){
    for(i=0;i<nb_groups;i++){
      for(j=0;j<arity;j++){
	printf("%d ", tab_group[i]->tab[j]->id);
      }
      printf(" : %.0f\nb_groups", tab_group[i]->val);
    }
  }

  fflush(stderr);

  val_array = (double *)MALLOC(nb_groups*sizeof(double));

  for( i=nb_groups-1 ; i>=0 ; i--){
    val_array[nb_groups-i-1] = tab_group[i]->val;
    /* this is allocated here and therefore released here*/
    tab_group[i]->bound = build_bound_array(val_array,nb_groups-i);

    if(verbose_level>=DEBUG){
      printf("-->(%d--%d) %.0f: ", i, nb_groups-i-1, tab_group[i]->val);
      for(j=1 ; j<nb_groups-i;j++){
	printf("%.0f - ",tab_group[i]->bound[j]);
      }
      printf("\n");
    }
  }

  FREE(val_array);

  indep_mat = init_independent_group_mat(nb_groups, tab_group, arity);

  for(id=0;id<nb_threads;id++){
    void **args=(void**)MALLOC(sizeof(void*)*9);
    args[0]=(void*)tab_group;
    args[1]=(void*)&nb_groups;
    args[2]=(void*)&arity;
    args[3]=(void*)&solution_size;
    args[4]=(void*)best_val;
    args[5]=(void*)best_selection;
    args[6]=(void*)indep_mat;
    args[7]=(void*)work_list;
    args[8]=(void*)&lock;
    works[id]= create_work(9, args, partial_exhaustive_search);
    if(verbose_level >= DEBUG)
      printf("Executing %p\n", (void *)works[id]);

    submit_work( works[id], id);
  }

  for(id=0;id<nb_threads;id++){
    wait_work_completion(works[id]);
    FREE(works[id]->args);
  }

  exit(-1);

  if(verbose_level>=INFO)
    fprintf(stdout, "\nx=%ld, y=%ld\n",x,y);


  for( i=0 ; i<nb_groups ; i++){
    FREE(indep_mat[i]);
    /* released of allocation done in build_bound_array*/
    if(i!=nb_groups-1)
      FREE(tab_group[i]->bound);
  }

  FREE(indep_mat);
  /* FREE(search_space); */
  FREE(works);

  if(verbose_level>=INFO)
    display_selection(best_selection, solution_size, arity, *best_val);

  duration = TOC;
  printf("Thread exhaustive search = %g\n",duration);
  exit(-1);
  return 0;
}

#if 0
static int old_recurs_exhaustive_search(group_list_t **tab, int i, int n, int arity, int d, int solution_size, double val, double *best_val, group_list_t **selection, group_list_t **best_selection, int8_t **indep_mat)
{
  group_list_t *elem = NULL;



  if( d == solution_size ){
    if(verbose_level >= DEBUG)
      display_selection(selection, solution_size, arity, val);
    if( val < *best_val ){
      *best_val = val;
      for( i = 0 ; i < solution_size ; i++ )
	best_selection[i] = selection[i];
      return 1;
    }
    return 0;
  }

  if(solution_size-d>n-i){ /*if there not enough groups available*/
    return 0;
  }

  while( i < n ){
    elem = tab[i];
    if(val+elem->val<*best_val){
      if(independent_groups_mat(selection, d, elem, indep_mat)){
	if(verbose_level >= DEBUG)
	  printf("%d: %d\n", d, i);
	selection[d] = elem;
	val += elem->val;
	old_recurs_exhaustive_search(tab, i+1, n, arity, d+1, solution_size, val, best_val, selection, best_selection, indep_mat);
      val -= elem->val;
      }
    }
    i++;
  }

  return 0;
}
#endif

#if 0
static int recurs_exhaustive_search(group_list_t **tab, int i, int n, int arity, int d, int solution_size, double val, double *best_val, group_list_t **selection, group_list_t **best_selection, int8_t **indep_mat, int* tab_i)
{
  group_list_t *elem = NULL;

 check:
  if( d == solution_size ){
    if(verbose_level >= DEBUG)
      display_selection(selection, solution_size, arity, val);
    if( val < *best_val ){
      *best_val = val;
      for( i = 0 ; i < solution_size ; i++ )
	best_selection[i] = selection[i];
      goto uncheck;
    }
    goto uncheck;
  }

  if(solution_size-d>n-i){ /*if there not enough groups available*/
    if(d>1)
      goto uncheck;
    else
      return 0;
  }

  while( i < n ){
    elem = tab[i];
    if(val+elem->val<*best_val){
      if(independent_groups_mat(selection, d, elem, indep_mat)){
	if(verbose_level >= DEBUG)
	  printf("%d: %d\n", d, i);
	selection[d] = elem;
	val += selection[d]->val;
	tab_i[d]=i;
	d++;
	i++;
	goto check;
      uncheck:
	d--;
	val -= selection[d]->val;
	i=tab_i[d];
      }
    }
    i++;
  }

  if(d>1)
    goto uncheck;

  return 0;
}
#endif

#if 0
static int  exhaustive_search(group_list_t **tab_group, int n, int arity, int solution_size, double *best_val,
			       group_list_t **best_selection)
{
  int i, j;
  group_list_t **selection = NULL;
  double val;
/* matrix of indepedency between groups (i.e; 2 groups are independent if they
  are composed of different ids): lazy data structure filled only once we have
  already computed if two groups are independent. otherwise it is initialized at
  -1*/
  int8_t **indep_mat;
  int *tab_i = (int*) MALLOC(sizeof(int)*solution_size);
  double duration;
  TIC;

  if(verbose_level>=DEBUG){
    for(i=0;i<n;i++){
      for(j=0;j<arity;j++){
	printf("%d ", tab_group[i]->tab[j]->id);
      }
      printf(" : %f\n", tab_group[i]->val);
    }
  }



  indep_mat = init_independent_group_mat(n, tab_group, arity);

  selection = (group_list_t **)MALLOC(sizeof(group_list_t*)*solution_size);
  for( i = 0 ; i < n ; i++ ){
    if(verbose_level>=INFO){
      fprintf(stdout, "\r%.2f%% of search space explored...", (100.0*i)/n);
      fflush(stdout);
    }
    selection[0] = tab_group[i];
    val = tab_group[i]->val;
    /* recurs_exhaustive_search(tab_group, i+1, n, arity, 1, solution_size, val, best_val, selection, best_selection, indep_mat, tab_i); */
    old_recurs_exhaustive_search(tab_group, i+1, n, arity, 1, solution_size, val, best_val, selection, best_selection, indep_mat);
  }

  if(verbose_level>=INFO)
    fprintf(stdout, "\n");

  FREE(selection);

  for( i=0 ; i<n ; i++)
    FREE(indep_mat[i]);
  FREE(indep_mat);

  FREE(tab_i);


  if(verbose_level>=INFO)
    display_selection(best_selection, solution_size, arity, *best_val);
  duration = TOC;
  printf("Seq exhaustive search = %g\n",duration);
  exit(-1);

  return 0;
}
#endif


int  select_independent_groups_by_largest_index(group_list_t **tab_group, int n, int arity, int solution_size, double *best_val, group_list_t **best_selection, int bound, double max_duration)
{
  int i, dec, nb_groups=0;
  group_list_t **selection = NULL;
  double val, duration;
  CLOCK_T time1, time0;

  selection = (group_list_t **)MALLOC(sizeof(group_list_t*)*solution_size);
  CLOCK(time0);

  dec = MAX(n/10000, 2);
  for( i = n-1 ; i >= 0 ; i -= dec*dec){
    selection[0] = tab_group[i];
    val = tab_group[i]->val;
    nb_groups += test_independent_groups(tab_group, i+1, n, arity, 1, solution_size, val, best_val, selection, best_selection);
    if(verbose_level>=DEBUG)
      printf("%d:%d\n", i, nb_groups);

    if(nb_groups >= bound){
      FREE(selection);
      return 0;
    }
    if((!(i%5)) && (max_duration>0)){
      CLOCK(time1);
      duration=CLOCK_DIFF(time1, time0);
      if(duration>max_duration){
	FREE(selection);
	return 1;
      }
    }
  }

  FREE(selection);

  if(verbose_level>=INFO)
    display_selection(best_selection, solution_size, arity, *best_val);

  return 0;
}

void list_to_tab(group_list_t *list, group_list_t **tab, int n)
{
  int i;
  for( i = 0 ; i < n ; i++ ){
    if(!list){
      if(verbose_level>=CRITICAL)
	fprintf(stderr, "Error not enough elements. Only %d on %d\n", i, n);
      exit(-1);
    }
    tab[n-i-1] = list;
    tab[n-i-1]->id = n-i-1;
    list = list->next;
  }
  if(list){
    if(verbose_level>=CRITICAL)
      fprintf(stderr, "Error too many elements\n");
    exit(-1);
  }
}

void display_tab_group(group_list_t **tab, int n, int arity)
{
  int i, j;
  if(verbose_level<DEBUG)
    return;
  for( i = 0 ; i < n ; i++ ){
    for( j = 0 ; j < arity ; j++ )
      printf("%d ", tab[i]->tab[j]->id);
    printf(": %.2f %.2f\n", tab[i]->val, tab[i]->wg);
  }
}

int independent_tab(tm_tree_t **tab1, tm_tree_t **tab2, int arity)
{
  int ii, jj;
  for( ii = 0 ; ii < arity ; ii++ ){
    for( jj = 0 ; jj < arity ; jj++ ){
      if(tab1[ii]->id == tab2[jj]->id){
	return 0;
      }
    }
  }
  return 1;
}

void compute_weighted_degree(group_list_t **tab, int n, int arity)
{
  int i, j;
  for( i = 0 ; i < n ; i++)
    tab[i]->sum_neighbour = 0;
  for( i = 0 ; i < n ; i++ ){
    /*printf("%d/%d=%f%%\n", i, n, (100.0*i)/n);*/
    for( j = i+1 ; j < n ; j++ )
      /*if(!independent_groups(&tab[i], 1, tab[j], arity)){*/
      if(!independent_tab(tab[i]->tab, tab[j]->tab, arity)){
	tab[i]->sum_neighbour += tab[j]->val;
	tab[j]->sum_neighbour += tab[i]->val;
      }

    tab[i]->wg = tab[i]->sum_neighbour/tab[i]->val;
    if(tab[i]->sum_neighbour == 0)
      tab[i]->wg = 0;
    /*printf("%d:%f/%f=%f\n", i, tab[i]->sum_neighbour, tab[i]->val, tab[i]->wg);*/
  }
}

/*
  aff_mat : the affiity matrix at the considered level (used to evaluate a grouping)
  tab_node: array of the node to group
  parent: node to which attached the computed group
  id: current considered node of tab_node
  arity: number of children of parent (i.e.) size of the group to compute
  best_val: current value of th grouping
  cur_group: current grouping
  mat_order: size of tab and tab_node. i.e. number of nodes at the considered level
 */
void  fast_group(tm_affinity_mat_t *aff_mat, tm_tree_t *tab_node, tm_tree_t *parent, int id, int arity, int n,
		 double *best_val, tm_tree_t **cur_group, int *nb_groups, int max_groups)
{
  double val;
  int i;
  int mat_order = aff_mat->order;

  /* printf("Max groups=%d, nb_groups= %d, n= %d, arity = %d\n", max_groups, *nb_groups, n, arity); */

  /*if we have found enough node in the group*/
  if( n == arity ){
    (*nb_groups)++;
    /*evaluate this group*/
    val = eval_grouping(aff_mat, cur_group, arity);
    if(verbose_level>=DEBUG)
      printf("Grouping %d: %f\n", *nb_groups, val);
    /* If we improve compared to previous grouping: uodate the children of parent accordingly*/
    if( val < *best_val ){
      *best_val = val;
      for( i = 0 ; i < arity ; i++ )
	parent->child[i] = cur_group[i];

      parent->arity = arity;
    }
    return;
  }

  /*
    If we need more node in the group
    Continue to explore avilable nodes
  */
  for( i = id+1 ; i < mat_order ; i++ ){
    /* If this node is allready in a group: skip it*/
    if(tab_node[i].parent)
      continue;
    /*Otherwise, add it to the group at place n */
    cur_group[n] = &tab_node[i];
    /*
    printf("%d<-%d %d/%d\n", n, i, *nb_groups, max_groups);
    exit(-1);
    recursively add the next element to this group
    */
    fast_group(aff_mat, tab_node, parent, i, arity, n+1, best_val, cur_group, nb_groups, max_groups);
    if(*nb_groups > max_groups)
      return;
  }
}



  

double fast_grouping(tm_affinity_mat_t *aff_mat, tm_tree_t *tab_node, tm_tree_t *new_tab_node, int arity, int solution_size, double nb_groups)
{
  tm_tree_t **cur_group = NULL;
  int l, i, nb_done;
  double best_val, val=0;

  cur_group = (tm_tree_t**)MALLOC(sizeof(tm_tree_t*)*arity);
  for( l = 0 ; l < solution_size ; l++ ){
    best_val = DBL_MAX;
    nb_done = 0;
    /*printf("nb_groups%d/%d, nb_groups=%ld\n", l, M, nb_groups);*/
    /* select the best greedy grouping among the 10 first one*/
    /*fast_group(tab, tab_node, &new_tab_node[l], -1, arity, 0, &best_val, cur_group, mat_order, &nb_done, MAX(2, (int)(50-log2(nb_groups))-M/10));*/
    fast_group(aff_mat, tab_node, &new_tab_node[l], -1, arity, 0, &best_val, cur_group, &nb_done, MAX(10, (int)(50-CmiLog2(nb_groups))-solution_size/10));
    val += best_val;
    for( i = 0 ; i < new_tab_node[l].arity ; i++ )
      new_tab_node[l].child[i]->parent=&new_tab_node[l];
    update_val(aff_mat, &new_tab_node[l]);
    if(new_tab_node[l].val != best_val){
          if(verbose_level>=CRITICAL)
	    printf("Error: best_val = %f, new_tab_node[%d].val = %f\n", best_val, l, new_tab_node[l].val);
      exit(-1);
    }
  }

  FREE(cur_group);

  return val;
}

static double k_partition_grouping(tm_affinity_mat_t *aff_mat, tm_tree_t *tab_node, tm_tree_t *new_tab_node, int arity, int solution_size) {
  int *partition = NULL;
  int n = aff_mat->order;
  com_mat_t com_mat;
  int i,j,k;
  double val = 0;

  com_mat.comm = aff_mat->mat;
  com_mat.n = n;

  if(verbose_level>=DEBUG)
    printf("K-Partitionning: n=%d, solution_size=%d, arity=%d\n",n, solution_size,arity);

  partition = kpartition(solution_size, &com_mat, n, NULL, 0);

  /* new_tab_node[i]->child[j] = &tab_node[k] where 0<=i< solution size, 0<=j<arity and partition[k]=i and the jth occurence of i in the partition*/

  int *j_tab = (int*) CALLOC(solution_size,sizeof(int));

  for( k = 0 ;  k < n ; k++){
    i = partition[k];
    j = j_tab[i];
    j_tab[i]++;
    new_tab_node[i].child[j]         = &tab_node[k];
    new_tab_node[i].child[j]->parent = &new_tab_node[i];    
  }
  
  for( i = 0 ; i < solution_size ; i++ ){
    new_tab_node[i].arity = arity;
    update_val(aff_mat, &new_tab_node[i]);
    val += new_tab_node[i].val;
  }

  FREE(j_tab);
  FREE(partition);

  return val;

}

int adjacency_asc(const void* x1, const void* x2)
{
  adjacency_t *e1 = NULL, *e2 = NULL;

  e1 = ((adjacency_t*)x1);
  e2 = ((adjacency_t*)x2);

  return (e1->val < e2->val) ? - 1 : 1;
}

int adjacency_dsc(const void* x1, const void* x2)
{
  adjacency_t *e1 = NULL, *e2 = NULL;

  e1 = ((adjacency_t*)x1);
  e2 = ((adjacency_t*)x2);


  return (e1->val > e2->val) ? -1 : 1;
}

void super_fast_grouping(tm_affinity_mat_t *aff_mat, tm_tree_t *tab_node, tm_tree_t *new_tab_node, int arity, int solution_size)
{
  double val = 0, duration;
  adjacency_t *graph;
  int i, j, e, l, nb_groups;
  int mat_order = aff_mat->order;
  double **mat = aff_mat->mat;

  assert( 2 == arity);

  TIC;
  graph = (adjacency_t*)MALLOC(sizeof(adjacency_t)*((mat_order*mat_order-mat_order)/2));
  e = 0;
  for( i = 0 ; i < mat_order ; i++ )
    for( j = i+1 ; j < mat_order ; j++){
      graph[e].i = i;
      graph[e].j = j;
      graph[e].val = mat[i][j];
      e++;
    }

  duration = TOC;
  if(verbose_level>=DEBUG)
    printf("linearization=%fs\n", duration);


  assert( e == (mat_order*mat_order-mat_order)/2);
  TIC;
  qsort(graph, e, sizeof(adjacency_t), adjacency_dsc);
  duration = TOC;
  if(verbose_level>=DEBUG)
    printf("sorting=%fs\n", duration);

  TIC;

TIC;
  l = 0;
  nb_groups = 0;
  for( i = 0 ; (i < e) && (l < solution_size) ; i++ )
    if(try_add_edge(tab_node, &new_tab_node[l], arity, graph[i].i, graph[i].j, &nb_groups))
      l++;

  for( l = 0 ; l < solution_size ; l++ ){
    update_val(aff_mat, &new_tab_node[l]);
    val += new_tab_node[l].val;
  }

  duration = TOC;
  if(verbose_level>=DEBUG)
    printf("Grouping=%fs\n", duration);


  if(verbose_level>=DEBUG)
    printf("val=%f\n", val);


  display_grouping(new_tab_node, solution_size, arity, val);

  FREE(graph);
}


tm_affinity_mat_t *build_cost_matrix(tm_affinity_mat_t *aff_mat, double* obj_weight, double comm_speed)
{
  double **mat = NULL, *sum_row;
  double **old_mat;
  double avg;
  int i, j, mat_order;

  if(!obj_weight)
    return aff_mat;

  mat_order = aff_mat->order;
  old_mat = aff_mat -> mat;

  mat = (double**)MALLOC(mat_order*sizeof(double*));
  for( i = 0 ; i < mat_order ; i++ )
    mat[i] = (double*)MALLOC(mat_order*sizeof(double));

  sum_row = (double*)CALLOC(mat_order, sizeof(double));



  avg = 0;
  for( i = 0 ; i < mat_order ; i++ )
    avg += obj_weight[i];
  avg /= mat_order;


  if(verbose_level>=DEBUG)
    printf("avg=%f\n", avg);

  for( i = 0 ; i < mat_order ; i++ )
    for( j = 0 ; j < mat_order ; j++){
      if( i == j )
	mat[i][j] = 0;
      else{
	mat[i][j] = 1e-4*old_mat[i][j]/comm_speed-fabs(avg-(obj_weight[i]+obj_weight[j])/2);
	sum_row[i] += mat[i][j];
      }
    }
  return new_affinity_mat(mat, sum_row, mat_order);

}


/*
  aff_mat: affinity matrix at the considered level (use to evaluate a grouping)
  tab_node: array of the node to group
  new_tab_node: array of nodes at the next level (the parents of the node in tab_node once the grouping will be done).
  arity: number of children of parent (i.e.) size of the group to compute
  solution_size: size of new_tab_node (i.e) the number of parents
*/
void group_nodes(tm_affinity_mat_t *aff_mat, tm_tree_t *tab_node, tm_tree_t *new_tab_node,
		 int arity, int solution_size, double* obj_weigth, double comm_speed){

 /*
   mat_order: size of tab and tab_node. i.e. number of nodes at the considered level
    Hence we have: M*arity=mat_order
 */
  int mat_order = aff_mat -> order;
  tm_tree_t **cur_group = NULL;
  int j, l;
  unsigned long int list_size; 
  unsigned long int  i;
  group_list_t list, **best_selection = NULL, **tab_group = NULL;
  double best_val, last_best;
  int timeout;
  tm_affinity_mat_t *cost_mat = NULL; /*cost matrix taking into account the communiocation cost but also the weight of the object*/
  double duration;
  double val;
  double nbg;
  TIC;



  /* might return aff_mat (if obj_weight==NULL): do not free this tab in this case*/
  cost_mat = build_cost_matrix(aff_mat, obj_weigth, comm_speed);

  nbg = choose(mat_order, arity);

  if(verbose_level>=INFO)
    printf("Number of possible groups:%.0lf\n", nbg);

  /* Todo: check if the depth is a criteria for speeding up the computation*/
  /*  if(nb_groups>30000||depth>5){*/
  if( nbg > 30000 ){

    double duration;

    TIC;
    if( arity <= 2 ){
      /*super_fast_grouping(tab, tab_node, new_tab_node, arity, mat_order, solution_size, k);*/
      if(verbose_level >= INFO )
	printf("Bucket Grouping...\n");
      val = bucket_grouping(cost_mat, tab_node, new_tab_node, arity, solution_size);
    }else if( arity <= 5){
      if(verbose_level >= INFO)
	printf("Fast Grouping...\n");
      val = fast_grouping(cost_mat, tab_node, new_tab_node, arity, solution_size, nbg);
    } else{
      if(verbose_level >= INFO)
	printf("K-partition Grouping...\n");
      val = k_partition_grouping(cost_mat, tab_node, new_tab_node, arity, solution_size);
    }

    duration = TOC;
    if(verbose_level >= INFO)
      printf("Fast grouping duration=%f\n", duration);

    if(verbose_level >= INFO)
      display_grouping(new_tab_node, solution_size, arity, val);

  }else{
    unsigned long int  nb_groups = (unsigned long int) nbg;
    if(verbose_level >= INFO)
      printf("Grouping nodes...\n");
    list.next = NULL;
    list.val = 0; /*number of elements in the list*/
    cur_group = (tm_tree_t**)MALLOC(sizeof(tm_tree_t*)*arity);
    best_selection = (group_list_t **)MALLOC(sizeof(group_list_t*)*solution_size);

    list_all_possible_groups(cost_mat, tab_node, 0, arity, 0, cur_group, &list);
    list_size = (int)list.val;
    assert( list_size == nb_groups);
    tab_group = (group_list_t**)MALLOC(sizeof(group_list_t*)*nb_groups);
    list_to_tab(list.next, tab_group, nb_groups);
    if(verbose_level>=INFO)
      printf("List to tab done\n");

    best_val = DBL_MAX;

    /* perform the pack mapping fist*/
    /* timeout = select_independent_groups(tab_group, n, arity, M, &best_val, best_selection, 1, 0.1); */
    timeout = select_independent_groups(tab_group, nb_groups, arity, solution_size, &best_val, best_selection, 1, 100);
    if(verbose_level>=INFO)
      if(timeout)
	printf("Packed mapping timeout!\n");
    /* give this mapping an exra credit (in general MPI application are made such that
       neighbour process communicates more than distant ones) */
    best_val /= 1.001;
    /* best_val *= 1.001; */
    if(verbose_level>=INFO)
      printf("Packing computed\n");



    /* perform a mapping trying to use group that cost less first*/
    qsort(tab_group, nb_groups, sizeof(group_list_t*), group_list_asc);
    last_best = best_val;
    timeout = select_independent_groups(tab_group, nb_groups, arity, solution_size, &best_val, best_selection, 10, 0.1);
    /* timeout = select_independent_groups(tab_group, n, arity, solution_size, &best_val, best_selection, n, 0); */
    if(verbose_level>=INFO){
      if(timeout){
	printf("Cost less first timeout!\n");
      }
      if(last_best>best_val){
	printf("Cost less first Impoved solution\n");
      }
    }
    /* perform a mapping trying to minimize the use of groups that cost a lot */
    qsort(tab_group, nb_groups, sizeof(group_list_t*), group_list_dsc);
    last_best=best_val;
    timeout=select_independent_groups_by_largest_index(tab_group, nb_groups, arity, solution_size, &best_val, best_selection, 10, 0.1);
    if(verbose_level>=INFO){
      if(timeout)
	printf("Cost most last timeout!\n");
      if(last_best>best_val)
	printf("Cost most last impoved solution\n");
    }
    if( nb_groups < 1000000 ){
      /* perform a mapping in the weighted degree order */


      if(verbose_level>=INFO)
	printf("----WG----\n");


      compute_weighted_degree(tab_group, nb_groups, arity);

      if(verbose_level>=INFO)
	printf("Weigted degree computed\n");

      qsort(tab_group, nb_groups, sizeof(group_list_t*), weighted_degree_dsc);

      for( i=0 ; i<nb_groups ; i++)
	tab_group[i]->id = i;

      /* display_tab_group(tab_group, n, arity);*/
      last_best = best_val;
      timeout = select_independent_groups(tab_group, nb_groups, arity, solution_size, &best_val, best_selection, 10, 0.1);
      /* timeout = select_independent_groups(tab_group, n, arity, solution_size, &best_val, best_selection, n, 0); */

      if(verbose_level>=INFO){
	if(timeout)
	  printf("WG timeout!\n");
	if(last_best>best_val)
	  printf("WG impoved solution\n");
      }
    }

    if(tm_get_exhaustive_search_flag()){
      if(verbose_level>=INFO)
	printf("Running exhaustive search on %ld groups, please wait...\n",nb_groups);

      last_best = best_val;
      thread_exhaustive_search(tab_group, nb_groups, arity, solution_size, &best_val, best_selection);
      /* exhaustive_search(tab_group, nb_groups, arity, solution_size, &best_val, best_selection); */
      if(verbose_level>=INFO){
	if(last_best>best_val){
	  printf("Exhaustive search improved solution by: %.3f\n",(last_best-best_val)/last_best);
	} else {
	  printf("Exhaustive search did not improved solution\n");
	}
      }
    }
 
    /* Reorder solution and apply it to new_tab_node: returned array */
    qsort(best_selection, solution_size, sizeof(group_list_t*), group_list_id);

    for( l = 0 ; l < solution_size ; l++ ){
      for( j = 0 ; j < arity ; j++ ){
	new_tab_node[l].child[j]         = best_selection[l]->tab[j];
	new_tab_node[l].child[j]->parent = &new_tab_node[l];
      }
      new_tab_node[l].arity = arity;

      /* printf("arity=%d\n", new_tab_node[l].arity); */
      update_val(cost_mat, &new_tab_node[l]);
    }

    delete_group_list((&list)->next);
    FREE(best_selection);
    FREE(tab_group);
    FREE(cur_group);
  }

  if(cost_mat != aff_mat){
    free_affinity_mat(cost_mat);
  }

  duration = TOC;


  if(verbose_level>=INFO)
    printf("Grouping done in %.4fs!\n", duration);
}

void complete_aff_mat(tm_affinity_mat_t **aff_mat , int mat_order, int K)
{
  double **old_mat = NULL, **new_mat = NULL; double *sum_row;
  int M, i;

  old_mat = (*aff_mat) -> mat;

  M = mat_order+K;
  new_mat = (double**)MALLOC(M*sizeof(double*));
  for( i = 0 ; i < M ; i++ )
    new_mat[i] = (double*)CALLOC((M), sizeof(double));

  sum_row = (double*) CALLOC(M, sizeof(double));

  for( i = 0 ; i < mat_order ; i++ ){
    memcpy(new_mat[i], old_mat[i], mat_order*sizeof(double));
    sum_row[i] = (*aff_mat)->sum_row[i];
  }

  *aff_mat = new_affinity_mat(new_mat, sum_row, M);
}

void complete_obj_weight(double **tab, int mat_order, int K)
{
  double *old_tab = NULL, *new_tab = NULL, avg;
  int M, i;

  old_tab = *tab;

  if(!old_tab)
    return;

  avg = 0;
  for( i = 0 ; i < mat_order ; i++ )
    avg += old_tab[i];
  avg /= mat_order;

  M = mat_order+K;
  new_tab = (double*)MALLOC(M*sizeof(double));

  *tab = new_tab;
  for( i = 0 ; i < M ; i++ )
    if(i < mat_order)
      new_tab[i] = old_tab[i];
    else
      new_tab[i] = avg;
}

void create_dumb_tree(tm_tree_t *node, int depth, tm_topology_t *topology)
{
  tm_tree_t **list_child = NULL;
  int arity, i;

  if( depth == topology->nb_levels-1) {
    set_node(node, NULL, 0, NULL, -1, 0, NULL, depth);
    return;
  }

  arity = topology->arity[depth];
  assert(arity>0);
  list_child = (tm_tree_t**)CALLOC(arity, sizeof(tm_tree_t*));
  for( i = 0 ; i < arity ; i++ ){
    list_child[i] = (tm_tree_t*)MALLOC(sizeof(tm_tree_t));
    create_dumb_tree(list_child[i], depth+1, topology);
    list_child[i]->parent = node;
    list_child[i]->dumb = 1;
  }

  set_node(node, list_child, arity, NULL, -1, 0, list_child[0], depth);
}
void complete_tab_node(tm_tree_t **tab, int mat_order, int K, int depth, tm_topology_t *topology)
{
  tm_tree_t *old_tab = NULL, *new_tab = NULL;
  int M, i;

  if( K == 0 )
    return;

  old_tab = *tab;

  M = mat_order+K;
  new_tab = (tm_tree_t*)MALLOC(M*sizeof(tm_tree_t));

  *tab = new_tab;
  for( i = 0 ; i < M ; i++ )
    if(i < mat_order)
      clone_tree(&new_tab[i], &old_tab[i]);
    else{
      create_dumb_tree(&new_tab[i], depth, topology);
      new_tab[i].id = i;
    }

  /* do not suppress tab if you are at the depth-most level it will be used at the mapping stage */
  FREE(old_tab);
}

void set_deb_tab_child(tm_tree_t *tree, tm_tree_t *child, int depth)
{
  /* printf("depth=%d\t%p\t%p\n", depth, child, tree);*/
  if( depth > 0 )
    set_deb_tab_child(tree->tab_child, child, depth-1);
  else
    tree->tab_child=child;
}

/*
Build the tree of the matching. It is a bottom up algorithm: it starts from the bottom of the tree on proceed by decreasing the depth
It groups nodes of the matrix tab and link these groups to the nodes of the under level.
Then it calls recursively the function to prefrom the grouping at the above level.

tab_node: array of nodes of the under level.
aff_mat: local affinity matrix
arity: arity of the nodes of the above level.
depth: current depth of the algorithm
toplogy: description of the hardware topology.
constraints:  set of constraints: core ids where to bind the processes
*/
tm_tree_t *build_level_topology(tm_tree_t *tab_node, tm_affinity_mat_t *aff_mat, int arity, int depth, tm_topology_t *topology,
			     double *obj_weight, double *comm_speed)
{

  /* mat_order: number of nodes. Order of com_mat, size of obj_weight */
  int mat_order=aff_mat->order ;
  int i, K=0, M; /*M = mat_order/Arity: number the groups*/
  tm_tree_t *new_tab_node = NULL; /*array of node for this level (of size M): there will be linked to the nodes of tab_nodes*/
  tm_affinity_mat_t * new_aff_mat= NULL; /*New communication matrix (after grouyping nodes together)*/
  tm_tree_t *res = NULL; /*resulting tree*/
  int completed = 0;
  double speed; /* communication speed at this level*/
  double *new_obj_weight = NULL;
  double duration;

  if( 0 == depth ){
    if((1 == mat_order) && (0 == depth))
      return &tab_node[0];
    else {
      if(verbose_level >= CRITICAL)
	fprintf(stderr, "Error: matrix size: %d and depth:%d (should be 1 and -1 respectively)\n", mat_order, depth);
      exit(-1);
    }
  }

  /* If the number of nodes does not divide the arity: we add K nodes  */
  if( mat_order%arity != 0 ){
    TIC;
    K = arity*((mat_order/arity)+1)-mat_order;
    /*printf("****mat_order=%d arity=%d K=%d\n", mat_order, arity, K);  */
    /*display_tab(tab, mat_order);*/
    /* add K rows and columns to comm_matrix*/
    complete_aff_mat(&aff_mat, mat_order, K);
    /* add K element to the object weight*/
    complete_obj_weight(&obj_weight, mat_order, K);
    /*display_tab(tab, mat_order+K);*/
    /* add a dumb tree to the K new "virtual nodes"*/
    complete_tab_node(&tab_node, mat_order, K, depth, topology);
    completed = 1; /*flag this addition*/
    mat_order += K; /*increase the number of nodes accordingly*/
    duration = TOC;
    if(verbose_level >= INFO)
      printf("Completing matrix duration= %fs\n ", duration);
  } /*display_tab(tab, mat_order);*/

  M = mat_order/arity;
  if(verbose_level >= INFO)
    printf("Depth=%d\tnb_nodes=%d\tnb_groups=%d\tsize of groups(arity)=%d\n", depth, mat_order, M, arity);

  TIC;
  /*create the new nodes*/
  new_tab_node = (tm_tree_t*)MALLOC(sizeof(tm_tree_t)*M);
  /*intitialize each node*/
  for( i = 0 ; i < M ; i++ ){
    tm_tree_t **list_child = NULL;
    list_child = (tm_tree_t**)CALLOC(arity, sizeof(tm_tree_t*));
    set_node(&new_tab_node[i], list_child, arity, NULL, i, 0, tab_node, depth);
  }
  duration = TOC;
  if(verbose_level >= INFO)
    printf("New nodes creation= %fs\n ", duration);

  /*Core of the algorithm: perfrom the grouping*/
  if(comm_speed)
    speed = comm_speed[depth];
  else
    speed = -1;
  group_nodes(aff_mat, tab_node, new_tab_node, arity, M, obj_weight, speed);

  TIC;
  /*based on that grouping aggregate the communication matrix*/
  new_aff_mat = aggregate_aff_mat(new_tab_node, aff_mat, M);
  duration = TOC;
  if(verbose_level >= INFO)
    printf("Aggregate_com_mat= %fs\n", duration);
  TIC;


  /*based on that grouping aggregate the object weight matrix*/
  new_obj_weight = aggregate_obj_weight(new_tab_node, obj_weight, M);
  duration = TOC;
  if(verbose_level >= INFO)
    printf("Aggregate obj_weight= %fs\n ", duration);

  /* set ID of virtual nodes to -1*/
  for( i = mat_order-K ; i < mat_order ; i++ )
    tab_node[i].id = -1;
  /*
  for(i=0;i<mat_order;i++)
    display_node(&tab_node[i]);
  display_tab(new_com_mat, M);
  */

  /* decrease depth and compute arity of the above level*/
  depth--;
  if(depth > 0)
    arity = topology->arity[depth-1];
  else
    arity = 1;
  /* assume all objects have the same arity*/
  res = build_level_topology(new_tab_node, new_aff_mat, arity, depth, topology, new_obj_weight, comm_speed);

  set_deb_tab_child(res, tab_node, depth);

  /* if we have extended the matrix with zero, free the data here as they are local to this recursive step only*/
  if(completed){
    free_affinity_mat(aff_mat);
    FREE(obj_weight);
  }
  free_affinity_mat(new_aff_mat);

  FREE(new_obj_weight);

  return res;
}




tm_tree_t *bottom_up_build_tree_from_topology(tm_topology_t *topology, tm_affinity_mat_t *aff_mat,
					      double *obj_weight, double *comm_speed){
  int depth, i;
  tm_tree_t *res = NULL, *tab_node = NULL;
  int mat_order = aff_mat->order;

  tab_node = (tm_tree_t*)MALLOC(sizeof(tm_tree_t)*mat_order);
  depth = topology->nb_levels;
  for( i = 0 ; i < mat_order ; i++ )
    set_node(&tab_node[i], NULL, 0, NULL, i, 0, NULL, depth);


  if(verbose_level >= INFO)
    printf("nb_levels=%d\n", depth);
  /* assume all objects have the same arity*/
  res = build_level_topology(tab_node, aff_mat , topology->arity[depth-2], depth-1, topology, obj_weight, comm_speed);
  if(verbose_level >= INFO)
    printf("Build (top down) tree done!\n");

  /* tell the system it is not a constraint tree, this is usefull for freeing pointers*/
  res->constraint = 0;

  return res;
}




/*
   The function returns the number of constraints (leaves that can be used)
   and their numbers (in increasing order) in the array pointed by contraints

   Also take into account the oversubscribing factor to expand the constraints tab
   to fit with oversuscibing of the nodes.

*/

int check_constraints(tm_topology_t  *topology, int **constraints)
{

  int sorted = 1;
  int last = -1;
  int i, shift;
  int nb_constraints = topology->nb_constraints*topology->oversub_fact;
  if(nb_constraints && topology->constraints){
    *constraints = (int*)MALLOC(sizeof(int)*(nb_constraints));
    /* renumber constarints logically as it is the way the k-partitionner use it*/
    for(i = 0 ; i < nb_constraints ; i++){
      /* in case of oversubscrining node ids at topology->nb_levels-1 are as follows (for the logocal numbering case):
	 0, 0, .., 0, 1, 1, ..., 1, 2, 2, 2, ..., 2, ... where the number of identical consecutive number is topology->oversub_fact.
	 However, topology->node_rank refers only to the last rank of the id. Hence,
	 topology->node_rank[topology->nb_levels-1][i] == i*topology->oversub_fact
	 In order to have all the ranks of a given id we need to shift them as follows:
      */
      shift = 1 + i%topology->oversub_fact - topology->oversub_fact;
      (*constraints)[i] = topology->node_rank[topology->nb_levels-1][topology->constraints[i/topology->oversub_fact]] +shift;
      if((*constraints)[i] < last)
	sorted = 0;
      last  = (*constraints)[i];
    }

    if(!sorted){
      qsort(*constraints, nb_constraints , sizeof(int), int_cmp_inc);
    }

  }else{
    *constraints = NULL;
  }

  return nb_constraints;
}





tm_tree_t * tm_build_tree_from_topology(tm_topology_t *topology, tm_affinity_mat_t *aff_mat, double *obj_weight, double *com_speed)
{
  int *constraints = NULL, nb_constraints;
  tm_tree_t * result;
  int npu, nb_processes, oversub_fact, nb_slots;

  verbose_level = tm_get_verbose_level();

  oversub_fact   = topology->oversub_fact;
  /* Here constraints expended to take into account the oversuscribing factor */
  nb_constraints = check_constraints (topology, &constraints);
  nb_processes   = aff_mat->order;
  npu            = nb_processing_units(topology);
  nb_slots       = npu * oversub_fact;

  if(verbose_level >= INFO){
    printf("Com matrix size      : %d\n", nb_processes);
    printf("nb_constraints       : %d\n", nb_constraints);
    if(constraints)
      print_1D_tab(constraints, nb_constraints);
    printf("nb_processing units  : %d\n", npu);
    printf("Oversubscrbing factor: %d\n", oversub_fact);
    printf("Nb of slots          : %d\n", nb_slots);
  }

  if(nb_processes > nb_constraints){
    if(verbose_level >= CRITICAL){
      fprintf(stderr, "Error : Not enough slots/constraints (%d) for the communication matrix order (%d)!\n",
	     nb_constraints, nb_processes);
    }
    exit(-1);
  }

  if(nb_constraints == nb_slots)
    {
      if(verbose_level >= INFO){
	printf("No need to use %d constraints for %d slots!\n", nb_constraints, nb_slots);
      }

      nb_constraints = 0;
      FREE(constraints);
    }

  if(nb_constraints){
    if(verbose_level >= INFO){
      printf("Partitionning with constraints\n");
    }
    result = kpartition_build_tree_from_topology(topology, aff_mat->mat, nb_processes, constraints, nb_constraints,
						 obj_weight, com_speed);
    result->nb_processes = aff_mat->order;
    FREE(constraints);
    return result;
  }
  else{
    if(verbose_level >= INFO){
      printf("Partitionning without constraints\n");
    }

    result = bottom_up_build_tree_from_topology(topology, aff_mat, obj_weight, com_speed);
    result->nb_processes = aff_mat->order;
    return result;
  }
}
