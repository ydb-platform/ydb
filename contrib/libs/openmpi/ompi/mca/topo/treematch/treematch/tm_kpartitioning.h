typedef struct _com_mat_t{
  double **comm;
  int n;  /*comm is of size n by n the other element are zeroes*/

} com_mat_t;


int  *kpartition(int, com_mat_t*, int, int *, int);
tm_tree_t * kpartition_build_tree_from_topology(tm_topology_t *topology,double **com_mat,int N, int *constraints, int nb_constraints, double *obj_weight, double *com_speed);

#define HAVE_LIBSCOTCH 0  // missing configure setup?
