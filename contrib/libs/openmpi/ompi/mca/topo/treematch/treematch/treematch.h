#ifndef __TREEMATCH_H__
#define __TREEMATCH_H__

/* size_t definition */
#include <stddef.h>
#include "tm_verbose.h"

/********* TreeMatch Public Enum **********/

/*type of topology files that can be read*/
typedef enum{
  TM_FILE_TYPE_UNDEF,
  TM_FILE_TYPE_XML,
  TM_FILE_TYPE_TGT
} tm_file_type_t;

/* different metrics to evaluate the solution */
typedef enum{
  TM_METRIC_SUM_COM  = 1,
  TM_METRIC_MAX_COM  = 2,
  TM_METRIC_HOP_BYTE = 3
} tm_metric_t;


/********* TreeMatch Public Structures **********/

typedef struct _job_info_t{
  int submit_date;
  int job_id;
  int finish_date;
} tm_job_info_t;

typedef struct _tree_t{
  int constraint; /* tells if the tree has been constructed with constraints on the nodes or not.
		     Usefull for freeing it. needs to be set on the root only*/
  struct _tree_t **child;
  struct _tree_t *parent;
  struct _tree_t *tab_child; /*the pointer to be freed*/
  double val;
  int arity;
  int depth;
  int id;
  int uniq;
  int dumb; /* 1 if the node belongs to a dumb tree: hence has to be freed separately*/
  tm_job_info_t *job_info;
  int nb_processes; /* number of grouped processes (i.e. the order of the affinity matrix). Set at the root only*/
}tm_tree_t; /* FT : changer le nom : tm_grouap_hierachy_t ?*/

/* Maximum number of levels in the tree*/
#define TM_MAX_LEVELS 100

typedef struct {
  int *arity;         /* arity of the nodes of each level*/
  int nb_levels;      /*number of levels of the tree. Levels are numbered from top to bottom starting at 0*/
  size_t *nb_nodes;      /*nb of nodes of each level*/
  int **node_id;      /*ID of the nodes of the tree for each level*/
  int **node_rank ;   /*rank of the nodes of the tree for each level given its ID: this is the inverse tab of node_id*/
  size_t *nb_free_nodes; /*nb of available nodes of each level*/
  int **free_nodes;   /*tab of node that are free: useful to simulate batch scheduler*/
  double *cost;       /*cost of the communication depending on the distance:
			cost[i] is the cost for communicating at distance nb_levels-i*/
  int *constraints;   /* array of constraints: id of the nodes where it is possible to map processes */
  int nb_constraints; /* Size of the above array */
  int oversub_fact;   /* maximum number of processes to be mapped on a given node */
  int nb_proc_units;  /* the real number of units used for computation */
}tm_topology_t;


typedef struct {
  double ** mat;
  double *  sum_row;
  int order;
} tm_affinity_mat_t;

/*
 sigma_i is such that  process i is mapped on core sigma_i
 k_i is such that core i exectutes process k_i_j (0<=j<<=oversubscribing factor - 1)

 size of sigma is the number of processes (nb_objs)
 size of k is the number of cores/nodes   (nb_compute_units)
 size of k[i] is the number of process we can execute per nodes (1 if no oversubscribing)

 We must have numbe of process<=number of cores

 k[i] == NULL if no process is mapped on core i
*/

typedef struct {
  int *sigma;
  size_t sigma_length;
  int **k;
  size_t k_length;
  int oversub_fact;
}tm_solution_t;


/************ TreeMatch Public API ************/

/* load XML or TGT topology */
tm_topology_t *tm_load_topology(char *arch_filename, tm_file_type_t arch_file_type);
/*
   Alternatively, build a synthetic balanced topology.

   nb_levels : number of levels of the topology +1 (the last level must be of cost 0 and arity 0).
   arity : array of arity of the first nb_level (of size nb_levels)
   cost : array of costs between the levels (of size nb_levels)
   core_numbering: numbering of the core by the system. Array of size nb_core_per_node

   nb_core_per_nodes: number of cores of a given node. Size of the array core_numbering

   both arity and cost are copied inside tm_build_synthetic_topology

   The numbering of the cores is done in round robin fashion after a width traversal of the topology.
   for example:
       {0,1,2,3} becomes 0,1,2,3,4,5,6,7...
   and
       {0,2,1,3} becomes 0,2,1,3,4,6,5,7,...

   Example of call to build the 128.tgt file: tleaf 4 16 500 2 100 2 50 2 10

   double cost[5] = {500,100,50,10,0};
   int arity[5] = {16,2,2,2,0};
   int cn[5]={0,1};

   topology = tm_build_synthetic_topology(arity,cost,5,cn,2);

 */
tm_topology_t  *tm_build_synthetic_topology(int *arity, double *cost, int nb_levels, int *core_numbering, int nb_core_per_nodes);
/* load affinity matrix */
tm_affinity_mat_t *tm_load_aff_mat(char *com_filename);
/*
   Alternativelly, build the affinity matrix from a array of array of matrix of size order by order
   For performance reason mat is not copied.
*/
tm_affinity_mat_t * tm_build_affinity_mat(double **mat, int order);
/* Add constraints to toplogy
   Return 1 on success and 0  if the constari,ts id are not compatible withe nodes id */
int tm_topology_add_binding_constraints(char *bind_filename, tm_topology_t *topology);
/* Alternatively, set the constraints from an array.
   Return 1 on success and 0  if the constari,ts id are not compatible withe nodes id

   The array constraints is copied inside tm_topology_set_binding_constraints

*/
int tm_topology_set_binding_constraints(int *constraints, int nb_constraints, tm_topology_t *topology);
/* display arity of the topology */
void  tm_display_arity(tm_topology_t *topology);
/* display the full topology */
void  tm_display_topology(tm_topology_t *topology);
/* Optimize the topology by decomposing arities */
void tm_optimize_topology(tm_topology_t **topology);
/* Manage oversubscribing */
void tm_enable_oversubscribing(tm_topology_t *topology, unsigned int oversub_fact);
/* core of the treematch: compute the solution tree */
tm_tree_t *tm_build_tree_from_topology(tm_topology_t *topology, tm_affinity_mat_t *aff_mat, double *obj_weight, double *com_speed);
/* compute the mapping according to teh tree an dthe core numbering*/
tm_solution_t *tm_compute_mapping(tm_topology_t *topology, tm_tree_t *comm_tree);
/* display the solution*/
double tm_display_solution(tm_topology_t *topology, tm_affinity_mat_t *aff_mat, tm_solution_t *sol, tm_metric_t metric);
/* display RR, packed, MPIPP*/
void tm_display_other_heuristics(tm_topology_t *topology, tm_affinity_mat_t *aff_mat, tm_metric_t metric);
/* free TM strutures*/
void tm_free_topology(tm_topology_t *topology);
void tm_free_tree(tm_tree_t *comm_tree);
void tm_free_solution(tm_solution_t *sol);
void tm_free_affinity_mat(tm_affinity_mat_t *aff_mat);
/* manage verbosity of TM*/
void tm_set_verbose_level(unsigned int level);
unsigned int  tm_get_verbose_level(void);
/* finalize treematch :check memory if necessary, and free internal variables (thread pool)*/
void tm_finalize(void);

/*
Ask for exhaustive search: may be very long
   new_val == 0 : no exhuative search
   new_val != 0 : exhuative search
*/
void tm_set_exhaustive_search_flag(int new_val);
int tm_get_exhaustive_search_flag(void);


/* Setting the maximum number of threads you want to use in parallel parts of TreeMatch */
void tm_set_max_nb_threads(unsigned int val);


#include "tm_malloc.h"

#endif
