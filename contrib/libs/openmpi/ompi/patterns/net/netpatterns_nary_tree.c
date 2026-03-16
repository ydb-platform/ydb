/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2009-2012 Mellanox Technologies.  All rights reserved.
 * Copyright (c) 2009-2012 Oak Ridge National Laboratory.  All rights reserved.
 * Copyright (c) 2014      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <sys/types.h>
#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>

#include "ompi/constants.h"
#include "netpatterns.h"

/*
 * Create mmaped shared file
 */

/* setup an n-array tree */

int ompi_netpatterns_setup_narray_tree(int tree_order, int my_rank, int num_nodes,
        netpatterns_tree_node_t *my_node)
{
    /* local variables */
    int n_levels, result;
    int my_level_in_tree, cnt;
    int lvl,cum_cnt, my_rank_in_my_level,n_lvls_in_tree;
    int start_index,end_index;

    /* sanity check */
    if( 1 >= tree_order ) {
        goto Error;
    }

    my_node->my_rank=my_rank;
    my_node->tree_size=num_nodes;

    /* figure out number of levels in tree */
    n_levels=0;
    result=num_nodes-1;
    while (0 < result ) {
        result/=tree_order;
        n_levels++;
    };

    /* figure out who my children and parents are */
    my_level_in_tree=-1;
    result=my_rank;
    /* cnt - number of ranks in given level */
    cnt=1;
    /*  cummulative count of ranks */
    while( 0 <= result ) {
        result-=cnt;
        cnt*=tree_order;
        my_level_in_tree++;
    };
    /* int my_level_in_tree, n_children, n_parents; */

    if( 0 == my_rank ) {
        my_node->n_parents=0;
        my_node->parent_rank=-1;
        my_rank_in_my_level=0;
    } else {
        my_node->n_parents=1;
        cnt=1;
        cum_cnt=0;
        for (lvl = 0 ; lvl < my_level_in_tree ; lvl ++ ) {
            /* cummulative count up to this level */
            cum_cnt+=cnt;
            /* number of ranks in this level */
            cnt*=tree_order;
        }
        my_rank_in_my_level=my_rank-cum_cnt;
        /* tree_order consecutive ranks have the same parent */
        my_node->parent_rank=cum_cnt-cnt/tree_order+my_rank_in_my_level/tree_order;
    }

    /* figure out number of levels in the tree */
    n_lvls_in_tree=0;
    result=num_nodes;
    /* cnt - number of ranks in given level */
    cnt=1;
    /*  cummulative count of ranks */
    while( 0 < result ) {
        result-=cnt;
        cnt*=tree_order;
        n_lvls_in_tree++;
    };

    my_node->children_ranks=(int *)NULL;

    /* get list of children */
    if( my_level_in_tree == (n_lvls_in_tree -1 ) ) {
        /* last level has no children */
        my_node->n_children=0;
    } else {
        cum_cnt=0;
        cnt=1;
        for( lvl=0 ; lvl <= my_level_in_tree ; lvl++ ) {
            cum_cnt+=cnt;
            cnt*=tree_order;
        }
        start_index=cum_cnt+my_rank_in_my_level*tree_order;
        end_index=start_index+tree_order-1;

        /* don't go out of bounds at the end of the list */
        if( end_index >= num_nodes ) {
            end_index = num_nodes-1;
        }

        if( start_index <= (num_nodes-1) ) {
            my_node->n_children=end_index-start_index+1;
        } else {
            my_node->n_children=0;
        }

        my_node->children_ranks=NULL;
        if( 0 < my_node->n_children ) {
            my_node->children_ranks=
                (int *)malloc( sizeof(int)*my_node->n_children);
            if( NULL == my_node->children_ranks) {
                goto Error;
            }
            for (lvl= start_index ; lvl <= end_index ; lvl++ ) {
                my_node->children_ranks[lvl-start_index]=lvl;
            }
        }
    }
    /* set node type */
    if( 0 == my_node->n_parents ) {
        my_node->my_node_type=ROOT_NODE;
    } else if ( 0 == my_node->n_children ) {
        my_node->my_node_type=LEAF_NODE;
    } else {
        my_node->my_node_type=INTERIOR_NODE;
    }


    /* successful return */
    return OMPI_SUCCESS;

Error:

    /* error return */
    return OMPI_ERROR;
}

void ompi_netpatterns_cleanup_narray_knomial_tree (netpatterns_narray_knomial_tree_node_t *my_node)
{
    if (my_node->children_ranks) {
	free (my_node->children_ranks);
	my_node->children_ranks = NULL;
    }

    if (0 != my_node->my_rank) {
	ompi_netpatterns_cleanup_recursive_knomial_tree_node (&my_node->k_node);
    }
}

int ompi_netpatterns_setup_narray_knomial_tree(
        int tree_order, int my_rank, int num_nodes,
        netpatterns_narray_knomial_tree_node_t *my_node)
{
    /* local variables */
    int n_levels, result;
    int my_level_in_tree, cnt ;
    int lvl,cum_cnt, my_rank_in_my_level,n_lvls_in_tree;
    int start_index,end_index;
    int rc;

    /* sanity check */
    if( 1 >= tree_order ) {
        goto Error;
    }

    my_node->my_rank=my_rank;
    my_node->tree_size=num_nodes;

    /* figure out number of levels in tree */
    n_levels=0;
    result=num_nodes-1;
    while (0 < result ) {
        result/=tree_order;
        n_levels++;
    };

    /* figure out who my children and parents are */
    my_level_in_tree=-1;
    result=my_rank;
    /* cnt - number of ranks in given level */
    cnt=1;
    /*  cummulative count of ranks */
    while( 0 <= result ) {
        result-=cnt;
        cnt*=tree_order;
        my_level_in_tree++;
    };
    /* int my_level_in_tree, n_children, n_parents; */

    if( 0 == my_rank ) {
        my_node->n_parents=0;
        my_node->parent_rank=-1;
        my_rank_in_my_level=0;
    } else {
        my_node->n_parents=1;
        cnt=1;
        cum_cnt=0;
        for (lvl = 0 ; lvl < my_level_in_tree ; lvl ++ ) {
            /* cummulative count up to this level */
            cum_cnt+=cnt;
            /* number of ranks in this level */
            cnt*=tree_order;
        }

        my_node->rank_on_level =
            my_rank_in_my_level =
            my_rank-cum_cnt;
        my_node->level_size = cnt;

        rc = ompi_netpatterns_setup_recursive_knomial_tree_node(
                my_node->level_size, my_node->rank_on_level,
                tree_order, &my_node->k_node);
        if (OMPI_SUCCESS != rc) {
            goto Error;
        }

        /* tree_order consecutive ranks have the same parent */
        my_node->parent_rank=cum_cnt-cnt/tree_order+my_rank_in_my_level/tree_order;
    }

    /* figure out number of levels in the tree */
    n_lvls_in_tree=0;
    result=num_nodes;
    /* cnt - number of ranks in given level */
    cnt=1;
    /*  cummulative count of ranks */
    while( 0 < result ) {
        result-=cnt;
        cnt*=tree_order;
        n_lvls_in_tree++;
    };

    if(result < 0) {
        /* reset the size on group */
        num_nodes = cnt / tree_order;
    }

    my_node->children_ranks=(int *)NULL;

    /* get list of children */
    if( my_level_in_tree == (n_lvls_in_tree -1 ) ) {
        /* last level has no children */
        my_node->n_children=0;
    } else {
        cum_cnt=0;
        cnt=1;
        for( lvl=0 ; lvl <= my_level_in_tree ; lvl++ ) {
            cum_cnt+=cnt;
            cnt*=tree_order;
        }
        start_index=cum_cnt+my_rank_in_my_level*tree_order;
        end_index=start_index+tree_order-1;

        /* don't go out of bounds at the end of the list */
        if( end_index >= num_nodes ) {
            end_index = num_nodes-1;
        }

        if( start_index <= (num_nodes-1) ) {
            my_node->n_children=end_index-start_index+1;
        } else {
            my_node->n_children=0;
        }

        my_node->children_ranks=NULL;
        if( 0 < my_node->n_children ) {
            my_node->children_ranks=
                (int *)malloc( sizeof(int)*my_node->n_children);
            if( NULL == my_node->children_ranks) {
                goto Error;
            }
            for (lvl= start_index ; lvl <= end_index ; lvl++ ) {
                my_node->children_ranks[lvl-start_index]=lvl;
            }
        }
    }
    /* set node type */
    if( 0 == my_node->n_parents ) {
        my_node->my_node_type=ROOT_NODE;
    } else if ( 0 == my_node->n_children ) {
        my_node->my_node_type=LEAF_NODE;
    } else {
        my_node->my_node_type=INTERIOR_NODE;
    }


    /* successful return */
    return OMPI_SUCCESS;

Error:

    /* error return */
    return OMPI_ERROR;
}

/* calculate the nearest power of radix that is equal to or greater
 * than size, with the specified radix.  The resulting tree is of
 * depth n_lvls.
 */
int ompi_roundup_to_power_radix ( int radix, int size, int *n_lvls )
{
    int n_levels=0, return_value=1;
    int result;
    if( 1 > size ) {
        return 0;
    }

    result=size-1;
    while (0 < result ) {
        result/=radix;
        n_levels++;
        return_value*=radix;
    };
    *n_lvls=n_levels;
    return return_value;
}

static int fill_in_node_data(int tree_order, int num_nodes, int my_node,
        netpatterns_tree_node_t *nodes_data)
{
    /* local variables */
    int rc, num_ranks_per_child, num_children, n_extra;
    int child, rank, n_to_offset, n_ranks_to_child;

    /* figure out who are my children */
    num_ranks_per_child=num_nodes/tree_order;
    if( num_ranks_per_child ) {
        num_children=tree_order;
        n_extra=num_nodes-num_ranks_per_child*tree_order;
    } else {
        num_children=num_nodes;
        /* each child has the same number of descendents - 1 */
        n_extra=0;
        /* when there is a child, there is at least one
         * descendent */
        num_ranks_per_child=1;
    }

    nodes_data[my_node].n_children=num_children;
    if( num_children ) {
        nodes_data[my_node].children_ranks=(int *)
            malloc(sizeof(int)*num_children);
        if(!nodes_data[my_node].children_ranks) {

            if ( NULL == nodes_data[my_node].children_ranks )
            {
                fprintf(stderr, "Cannot allocate memory for children_ranks.\n");
                rc = OMPI_ERR_OUT_OF_RESOURCE;
                goto error;
            }
        }
    }

    rank = my_node;
    for( child=0 ; child < num_children ; child ++ ) {

    /* set parent information */
        nodes_data[rank].n_parents=1;
        nodes_data[rank].parent_rank=my_node;
        if( n_extra ) {
            n_to_offset=child;
            if( n_to_offset > n_extra){
                n_to_offset=n_extra;
            }
        } else {
            n_to_offset=0;
        }

        rank=my_node+1+child*num_ranks_per_child;
        rank+=n_to_offset;

        /* set parent information */
        nodes_data[rank].n_parents=1;
        nodes_data[rank].parent_rank=my_node;

        n_ranks_to_child=num_ranks_per_child;
        if(n_extra && (child < n_extra) ) {
            n_ranks_to_child++;
        }

        /* set child information */
        nodes_data[my_node].children_ranks[child]=rank;

        /* remove the child from the list of ranks */
        n_ranks_to_child--;
        rc=fill_in_node_data(tree_order, n_ranks_to_child, rank, nodes_data);
        if( OMPI_SUCCESS != rc ) {
            goto error;
        }

    }

    /* return */
    return OMPI_SUCCESS;

    /* Error */
error:
    return rc;

}

/*
 * This routine sets up the array describing the communication tree for
 * a k-ary tree where the children form a contiguous range of ranks at
 * each level.  The assumption here is that rank 0 is always the root -
 * ranks may be rotated based on who the actual root is, to obtain the
 * appropriate communication pattern for such roots.
 */
OMPI_DECLSPEC int ompi_netpatterns_setup_narray_tree_contigous_ranks(
        int tree_order, int num_nodes,
        netpatterns_tree_node_t **tree_nodes)
{
    /* local variables */
    int num_descendent_ranks=num_nodes-1;
    int rc=OMPI_SUCCESS;

    *tree_nodes=(netpatterns_tree_node_t *)malloc(
            sizeof(netpatterns_tree_node_t)*
            num_nodes);
    if(!(*tree_nodes) ) {
        fprintf(stderr, "Cannot allocate memory for tree_nodes.\n");
        rc = OMPI_ERR_OUT_OF_RESOURCE;
        return rc;
    }

    (*tree_nodes)[0].n_parents=0;
    rc=fill_in_node_data(tree_order,
            num_descendent_ranks, 0, *tree_nodes);

    /* successful return */
    return rc;

}
