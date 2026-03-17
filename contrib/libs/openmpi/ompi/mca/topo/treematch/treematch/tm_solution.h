#ifndef TM_SOLUION_H
#define TM_SOLUION_H

#include "treematch.h"

void tm_free_solution(tm_solution_t *sol);
int distance(tm_topology_t *topology,int i, int j);
double display_sol_sum_com(tm_topology_t *topology, tm_affinity_mat_t *aff_mat, int *sigma);
  double display_sol(tm_topology_t *topology, tm_affinity_mat_t *aff_mat, int *sigma, tm_metric_t metric);
double tm_display_solution(tm_topology_t *topology, tm_affinity_mat_t *aff_mat, tm_solution_t *sol,
			   tm_metric_t metric);
void tm_display_other_heuristics(tm_topology_t *topology, tm_affinity_mat_t *aff_mat, tm_metric_t metric);
int in_tab(int *tab, int n, int val);
void map_Packed(tm_topology_t *topology, int N, int *sigma);
void map_RR(tm_topology_t *topology, int N, int *sigma);
int hash_asc(const void* x1,const void* x2);
int *generate_random_sol(tm_topology_t *topology,int N,int level,int seed);
double eval_sol(int *sol,int N,double **comm, double **arch);
void exchange(int *sol,int i,int j);
double gain_exchange(int *sol,int l,int m,double eval1,int N,double **comm, double **arch);
void select_max(int *l,int *m,double **gain,int N,int *state);
void compute_gain(int *sol,int N,double **gain,double **comm, double **arch);
void map_MPIPP(tm_topology_t *topology,int nb_seed,int N,int *sigma,double **comm, double **arch);


#endif
