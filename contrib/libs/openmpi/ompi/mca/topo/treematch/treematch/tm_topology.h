#include <hwloc.h>
#include "tm_tree.h"

tm_topology_t* get_local_topo_with_hwloc(void);
tm_topology_t* hwloc_to_tm(char *filename);
int int_cmp_inc(const void* x1,const void* x2);
void optimize_arity(int **arity, double **cost, int *nb_levels,int n);
int symetric(hwloc_topology_t topology);
tm_topology_t * tgt_to_tm(char *filename);
void tm_display_arity(tm_topology_t *topology);
void tm_display_topology(tm_topology_t *topology);
void tm_free_topology(tm_topology_t *topology);
tm_topology_t *tm_load_topology(char *arch_filename, tm_file_type_t arch_file_type);
void tm_optimize_topology(tm_topology_t **topology);
int  tm_topology_add_binding_constraints(char *constraints_filename, tm_topology_t *topology);
int topo_nb_proc(hwloc_topology_t topology,int N);
void topology_arity(tm_topology_t *topology,int **arity,int *nb_levels);
void topology_constraints(tm_topology_t *topology,int **constraints,int *nb_constraints);
void topology_cost(tm_topology_t *topology,double **cost);
void topology_numbering(tm_topology_t *topology,int **numbering,int *nb_nodes);
double ** topology_to_arch(hwloc_topology_t topology);

