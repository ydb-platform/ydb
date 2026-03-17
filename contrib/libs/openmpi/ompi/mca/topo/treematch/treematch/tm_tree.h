#ifndef __TM_TREE_H__
#define __TM_TREE_H__
#include <stdlib.h>
#include "treematch.h"

void update_val(tm_affinity_mat_t *aff_mat,tm_tree_t *parent);
void display_tab(double **tab,int N);
void set_node(tm_tree_t *node,tm_tree_t ** child, int arity,tm_tree_t *parent,
	      int id,double val,tm_tree_t *tab_child,int depth);


typedef struct _group_list_t{
  struct _group_list_t *next;
  tm_tree_t **tab;
  double val;
  double sum_neighbour;
  double wg;
  int id;
  double *bound;
}group_list_t;


typedef struct{
  int i;
  int j;
  double val;
}adjacency_t;


typedef struct _work_unit_t{
  int nb_groups;
  int *tab_group;
  int done;
  int nb_work;
  struct _work_unit_t *next;
}work_unit_t;

#endif

