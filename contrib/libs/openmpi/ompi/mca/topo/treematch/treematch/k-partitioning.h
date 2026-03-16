#ifndef K_PARTITIONING
#define K_PARTITIONING

#include "PriorityQueue.h"

/*
  kPartitioning : function to call the k-partitioning algorithm
      - comm : the communication matrix
      - n : the number of vertices (including dumb  vertices)
      - k : the number of partitions
      - constraints : the list of constraints
      - nb_constraints : the number of constraints
      - greedy_trials : the number of trials to build the partition vector with kpartition_greedy
          - 0 : cyclic distribution of vertices
	  - > 0 : use of kpartition_greedy with greedy_trials number of trials 
 */
             
int* kPartitioning(double ** comm, int n, int k, int * const constraints, int nb_constraints, int greedy_trials);

#endif /*K_PARTITIONING*/
