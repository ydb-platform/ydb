#ifndef __TM_MAPPING_H__
#define __TM_MAPPING_H__
#include "tm_tree.h"
#include "tm_topology.h"
#include "tm_timings.h"
#include "tm_verbose.h"

tm_affinity_mat_t * new_affinity_mat(double **mat, double *sum_row, int order);
void   build_synthetic_proc_id(tm_topology_t *topology);
tm_topology_t  *build_synthetic_topology(int *arity, int nb_levels, int *core_numbering, int nb_core_per_nodes);
int compute_nb_leaves_from_level(int depth,tm_topology_t *topology);
void depth_first(tm_tree_t *comm_tree, int *proc_list,int *i);
int  fill_tab(int **new_tab,int *tab, int n, int start, int max_val, int shift);
void init_mat(char *filename,int N, double **mat, double *sum_row);
void map_topology(tm_topology_t *topology,tm_tree_t *comm_tree, int level,
		  int *sigma, int nb_processes, int **k, int nb_compute_units);
int nb_leaves(tm_tree_t *comm_tree);
int nb_lines(char *filename);
int nb_processing_units(tm_topology_t *topology);
void print_1D_tab(int *tab,int N);
tm_solution_t * tm_compute_mapping(tm_topology_t *topology,tm_tree_t *comm_tree);
void tm_free_affinity_mat(tm_affinity_mat_t *aff_mat);
tm_affinity_mat_t *tm_load_aff_mat(char *filename);
void update_comm_speed(double **comm_speed,int old_size,int new_size);

/* use to split a constaint into subconstraint according the tree*/
typedef struct{
  int *constraints; /* the subconstraints*/
  int length; /*length of *constraints*/
  int id;  /* id of the corresponding subtree*/
}constraint_t;

#endif
