#ifndef __BUCKET_H__
#define __BUCKET_H__

typedef struct{
  int i;
  int j;
}coord;

typedef struct{
  coord * bucket; /* store i,j */
  int bucket_len; /* allocated size in the heap */
  int nb_elem;    /* number of usefull elements (nb_elem should be lower than bucket_len) */
  int sorted;
}bucket_t;

typedef struct{
  bucket_t **bucket_tab;
  int nb_buckets;
  double **tab;
  int N;/* length of tab */
  /* For iterating over the buckets */
  int cur_bucket;
  int bucket_indice;
  double *pivot;
  double *pivot_tree;
  int max_depth;
}_bucket_list_t;

typedef _bucket_list_t *bucket_list_t;

double bucket_grouping(tm_affinity_mat_t *aff_mat,tm_tree_t *tab_node, tm_tree_t *new_tab_node, 
		       int arity,int M);
int try_add_edge(tm_tree_t *tab_node, tm_tree_t *parent,int arity,int i,int j,int *nb_groups);
#endif

