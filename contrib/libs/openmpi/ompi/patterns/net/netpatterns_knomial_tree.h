/*
 * Copyright (c) 2009-2012 Mellanox Technologies.  All rights reserved.
 * Copyright (c) 2009-2012 Oak Ridge National Laboratory.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef COMM_PATTERNS_KNOMIAL_TREE_H
#define COMM_PATTERNS_KNOMIAL_TREE_H

#include "ompi_config.h"

BEGIN_C_DECLS


/*
 * Pair-wise data exchange
 */

/* enum for node type */
enum {
    EXCHANGE_NODE,
    EXTRA_NODE
};

struct netpatterns_pair_exchange_node_t {

    /* Order of a node in the tree - usually 2 */
    int tree_order;

    /* number of nodes this node will exchange data with */
    int n_exchanges;

    /* ranks of nodes involved in data exchnge */
    int *rank_exchanges;

    /* number of extra sources of data - outside largest power of 2 in
     *  this group */
    int n_extra_sources;

    /* rank of the extra source */
    /* deprecated */ int rank_extra_source;
    int *rank_extra_sources_array;

    /* number of tags needed per stripe */
    int n_tags;

    /* log 2 of largest full power of 2 for this node set */
    /* deprecated */ int log_2;
    int log_tree_order;

    /* largest power of 2 that fits in this group */
    /* deprecated */ int n_largest_pow_2;
    int n_largest_pow_tree_order;

    /* node type */
    int node_type;

};
typedef struct netpatterns_pair_exchange_node_t netpatterns_pair_exchange_node_t;

struct netpatterns_payload_t {
    int s_len;
    int r_len;
    int s_offset;
    int r_offset;
};
typedef struct netpatterns_payload_t netpatterns_payload_t;

struct netpatterns_k_exchange_node_t {
    /* Order of a node in the tree - usually 2 */
    int tree_order;
    /* number of nodes this node will exchange data with */
    int n_exchanges;
    /* total number of exchanges that I actually participate in */
    int n_actual_exchanges;
    /* ranks of nodes involved in data exchnge */
    int **rank_exchanges;
    /* number of extra sources of data - outside largest power of 2 in
     *  this group */
    int n_extra_sources;
    /* rank/s of the extra source */
    int *rank_extra_sources_array;
    /* number of tags needed per stripe */
    int n_tags;
    /* log k of largest full power of k for this node set */
    int log_tree_order;
    /* largest power of k that fits in this group */
    int n_largest_pow_tree_order;
    /* node type */
    int node_type;
    /* start of extra ranks k_nomial */
    int k_nomial_stray;
    /* reindex map */
    int *reindex_map;
    /* inverse of reindex map, i.e. given a reindexed id find out its actual rank */
    int *inv_reindex_map;
    /* reindexed node_rank */
    int reindex_myid;
    /* 2-d array that hold payload info for each level of recursive k-ing */
    netpatterns_payload_t **payload_info;
};
typedef struct netpatterns_k_exchange_node_t
               netpatterns_k_exchange_node_t;

OMPI_DECLSPEC int ompi_netpatterns_setup_recursive_doubling_n_tree_node(int num_nodes, int node_rank, int tree_order,
    netpatterns_pair_exchange_node_t *exchange_node);

OMPI_DECLSPEC void ompi_netpatterns_cleanup_recursive_doubling_tree_node(
    netpatterns_pair_exchange_node_t *exchange_node);

OMPI_DECLSPEC int ompi_netpatterns_setup_recursive_doubling_tree_node(int num_nodes, int node_rank,
    netpatterns_pair_exchange_node_t *exchange_node);

OMPI_DECLSPEC int ompi_netpatterns_setup_recursive_knomial_tree_node(
   int num_nodes, int node_rank, int tree_order,
   netpatterns_k_exchange_node_t *exchange_node);

OMPI_DECLSPEC void ompi_netpatterns_cleanup_recursive_knomial_tree_node(
   netpatterns_k_exchange_node_t *exchange_node);

OMPI_DECLSPEC int ompi_netpatterns_setup_recursive_knomial_allgather_tree_node(
        int num_nodes, int node_rank, int tree_order, int *hier_ranks,
        netpatterns_k_exchange_node_t *exchange_node);

OMPI_DECLSPEC void ompi_netpatterns_cleanup_recursive_knomial_allgather_tree_node(
        netpatterns_k_exchange_node_t *exchange_node);

/* Input: k_exchange_node structure
      Output: index in rank_exchanges array that points
      to the "start_point" for outgoing send.

      Please see below example of usage:
      for (i = start_point ; i > 0; i--)
          for (k = 0; k < tree_radix; k++)
              send messages to exchange_node->rank_exchanges[i][k];
*/

static inline __opal_attribute_always_inline__
int netpatterns_get_knomial_level(
    int my_rank, int src_rank,
    int radix,   int size,
    int *k_level)
{
    int distance,
        pow_k;
    int logk_level = 0;

    /* Calculate disctance from source of data */
    distance = src_rank - my_rank;

    /* Wrap around */
    if (0 > distance) {
        distance += size;
    }

    pow_k = 1;
    while(distance / (pow_k * radix)) {
        pow_k *= radix;
        ++logk_level;
    }
    --logk_level;

    *k_level = pow_k;
    return logk_level;
}

/* Input: my_rank, root, radix, size
 * Output: source of the data, offset in power of K
 */
static inline __opal_attribute_always_inline__
int netpatterns_get_knomial_data_source(
    int my_rank, int root, int radix, int size,
    int *k_level, int *logk_level)
{
    int level = radix;
    int step = 0;

    /* Calculate source of the data */
    while((0 == (root - my_rank) % level)
            && (level <= size)) {
        level *= radix;
        ++step;
    }

    *k_level = level/radix;
    *logk_level = step;
    return my_rank - (my_rank % level - root % level);
}

/* Input: my_rank, radix,
 *        k_level - that you get from netpatterns_get_knomial_data_source
 *        k_step - some integer
 * Output: peer - next children in the tree
 * Usage:
 *         src = netpatterns_get_knomial_data_source(
 *                  my_rank, root, radix, size,
 *                  &k_level, &logk_level)
 *         recv_from(src......);
 *
 *         MCA_COMMON_NETPATTERNS_GET_NEXT_KNOMIAL_INIT(step_info, k_level, my_rank);
 *         while(MCA_COMMON_NETPATTERNS_GET_NEXT_KNOMIAL_PEER_CHECK_LEVEL(step_info)) {
 *              MCA_COMMON_NETPATTERNS_GET_NEXT_KNOMIAL_PEER(my_rank, radix, step_info, peer);
 *              send_to(peer....);
 *         }
 * for more example please grep in ptpcoll bcol bcast files
 */

typedef struct netpatterns_knomial_step_info_t {
    int k_step;
    int k_level;
    int k_tmp_peer;
} netpatterns_knomial_step_info_t;

#define MCA_COMMON_NETPATTERNS_GET_NEXT_KNOMIAL_UPDATE_LEVEL_FOR_BCAST(step_info, radix)\
do {                                                                                    \
    if (1 != step_info.k_step) {                                                        \
        step_info.k_level /= radix;                                                     \
    }                                                                                   \
} while (0)                                                                             \

#define MCA_COMMON_NETPATTERNS_GET_NEXT_KNOMIAL_INIT(step_info, in_k_level, in_peer)\
do {                                                                                \
    step_info.k_step  = 1;                                                          \
    step_info.k_level = in_k_level;                                                 \
    step_info.k_tmp_peer = in_peer;                                                 \
} while (0)

#define MCA_COMMON_NETPATTERNS_GET_NEXT_KNOMIAL_PEER_CHECK_LEVEL(step_info) \
                                                    (step_info.k_level > 1)

#define MCA_COMMON_NETPATTERNS_GET_NEXT_KNOMIAL_PEER(my_rank, radix, step_info, peer)           \
do {                                                                                            \
    int rank_radix_base = my_rank/step_info.k_level;                                            \
                                                                                                \
    peer = step_info.k_tmp_peer + step_info.k_level/radix;                                      \
    if (rank_radix_base != peer/step_info.k_level) {                                            \
        /* Wraparound the number */                                                             \
        peer -= step_info.k_level;                                                              \
        assert(peer >=0);                                                                       \
    }                                                                                           \
    ++step_info.k_step;                                                                         \
    if (radix == step_info.k_step) {                                                            \
        step_info.k_level /= radix;                                                             \
        step_info.k_step = 1;                                                                   \
        step_info.k_tmp_peer = my_rank;                                                         \
    } else {                                                                                    \
        step_info.k_tmp_peer = peer;                                                            \
    }                                                                                           \
                                                                                                \
} while (0)

END_C_DECLS
#endif
