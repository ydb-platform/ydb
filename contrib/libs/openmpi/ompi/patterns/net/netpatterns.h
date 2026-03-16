/*
 * Copyright (c) 2009-2012 Mellanox Technologies.  All rights reserved.
 * Copyright (c) 2009-2012 Oak Ridge National Laboratory.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved.
  * Copyright (c) 2017      IBM Corporation. All rights reserved.
  * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef COMM_PATTERNS_H
#define COMM_PATTERNS_H

#include "ompi_config.h"

#include "ompi/mca/rte/rte.h"
#include "netpatterns_knomial_tree.h"

BEGIN_C_DECLS

int ompi_netpatterns_base_err(const char* fmt, ...);
int ompi_netpatterns_register_mca_params(void);

#if OPAL_ENABLE_DEBUG
extern int ompi_netpatterns_base_verbose; /* disabled by default */
OMPI_DECLSPEC extern int ompi_netpatterns_base_err(const char*, ...) __opal_attribute_format__(__printf__, 1, 2);
#define NETPATTERNS_VERBOSE(args)                                \
    do {                                                         \
        if(ompi_netpatterns_base_verbose > 0) {           \
            ompi_netpatterns_base_err("[%s]%s[%s:%d:%s] ",\
                    ompi_process_info.nodename,                  \
                    OMPI_NAME_PRINT(OMPI_PROC_MY_NAME),          \
                    __FILE__, __LINE__, __func__);               \
            ompi_netpatterns_base_err args;               \
            ompi_netpatterns_base_err("\n");              \
        }                                                        \
    } while(0);
#else
#define NETPATTERNS_VERBOSE(args)
#endif

#define FIND_BASE(base,myid,level,k)    \
    do {                                \
        int temp = 1;                   \
        int jj;                         \
        int knt2;                       \
                                        \
        base = 0;                       \
        for( jj = 0; jj < level; jj++) {\
            temp *= k;                  \
        }                               \
        knt2 = 1;                       \
        while(myid >= knt2*temp){       \
            knt2++;                     \
        }                               \
        base = knt2*temp - temp;        \
    } while(0)                          \




/* enum for node type */
enum {
    ROOT_NODE,
    LEAF_NODE,
    INTERIOR_NODE
};


/*
 * N-order tree node description
 */
struct netpatterns_tree_node_t {
    /* my rank within the group */
    int my_rank;
    /* my node type - root, leaf, or interior */
    int my_node_type;
    /* number of nodes in the tree */
    int tree_size;
    /* number of parents (0/1) */
    int n_parents;
    /* number of children */
    int n_children;
    /* parent rank within the group */
    int parent_rank;
    /* chidren ranks within the group */
    int *children_ranks;
};
typedef struct netpatterns_tree_node_t netpatterns_tree_node_t;

struct netpatterns_k_exchange_node_t;
/*
 * N-order + knominal tree node description
 */
struct netpatterns_narray_knomial_tree_node_t {
    /* my rank within the group */
    int my_rank;
    /* my node type - root, leaf, or interior */
    int my_node_type;
    /* number of nodes in the tree */
    int tree_size;
    /* number of parents (0/1) */
    int n_parents;
    /* number of children */
    int n_children;
    /* parent rank within the group */
    int parent_rank;
    /* chidren ranks within the group */
    int *children_ranks;
    /* Total number of ranks on this specific level */
    int level_size;
    /* Rank on this node inside of level */
    int rank_on_level;
    /* Knomial recursive gather information */
    struct netpatterns_k_exchange_node_t k_node;
};
typedef struct netpatterns_narray_knomial_tree_node_t
netpatterns_narray_knomial_tree_node_t;


/* Init code for common_netpatterns */
OMPI_DECLSPEC int ompi_netpatterns_init(void);

/* setup an n-array tree */
OMPI_DECLSPEC int ompi_netpatterns_setup_narray_tree(int tree_order, int my_rank, int num_nodes,
        netpatterns_tree_node_t *my_node);
/* setup an n-array tree with k-nomial levels */
OMPI_DECLSPEC int ompi_netpatterns_setup_narray_knomial_tree( int tree_order, int my_rank, int num_nodes,
        netpatterns_narray_knomial_tree_node_t *my_node);
/* cleanup an n-array tree setup by the above function */
OMPI_DECLSPEC void ompi_netpatterns_cleanup_narray_knomial_tree (netpatterns_narray_knomial_tree_node_t *my_node);

/* setup an multi-nomial tree - for each node in the tree
 *  this returns it's parent, and it's children
 */
OMPI_DECLSPEC int ompi_netpatterns_setup_multinomial_tree(int tree_order, int num_nodes,
        netpatterns_tree_node_t *tree_nodes);

OMPI_DECLSPEC int ompi_netpatterns_setup_narray_tree_contigous_ranks(int tree_order,
        int num_nodes, netpatterns_tree_node_t **tree_nodes);

/* calculate the nearest power of radix that is equal to or greater
 * than size, with the specified radix.  The resulting tree is of
 * depth n_lvls.
 */
OMPI_DECLSPEC int ompi_roundup_to_power_radix( int radix, int size, int *n_lvls );

END_C_DECLS

#endif /* COMM_PATTERNS_H */
