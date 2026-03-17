/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2009-2012 Mellanox Technologies.  All rights reserved.
 * Copyright (c) 2009-2012 Oak Ridge National Laboratory.  All rights reserved.
 * Copyright (c) 2014      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
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
#include <stdlib.h>
#include <assert.h>

#include "ompi/constants.h"

#include "ompi/mca/rte/rte.h"

#include "netpatterns.h"

/* setup recursive doubleing tree node */

OMPI_DECLSPEC int ompi_netpatterns_setup_recursive_knomial_allgather_tree_node(
        int num_nodes, int node_rank, int tree_order, int *hier_ranks,
        netpatterns_k_exchange_node_t *exchange_node)
{
    /* local variables */
    int i, j, cnt, i_temp;
    int knt,knt2,kk, ex_node, stray;
    int n_levels,pow_k;
    int k_temp1;
    int k_temp2;
    int myid, reindex_myid = 0;
    int base, peer_base,base_temp;
    int peer;
    int *prev_data = NULL;
    int *current_data = NULL;
    int *group_info = NULL;


    NETPATTERNS_VERBOSE(
            ("Enter ompi_netpatterns_setup_recursive_knomial_tree_node(num_nodes=%d, node_rank=%d, tree_order=%d)",
                num_nodes, node_rank, tree_order));

    assert(num_nodes > 1);
    assert(tree_order > 1);
    if (tree_order > num_nodes) {
        tree_order = num_nodes;
    }

    /* k-nomial radix */
    exchange_node->tree_order = tree_order;

    /* Calculate the number of levels in the tree for
     * the largest power of tree_order less than or
     * equal to the group size
     */
    n_levels = 0;
    cnt=1;
    while ( num_nodes > cnt ) {
        cnt *= tree_order;
        n_levels++;
    }
    /* this is the actual number of recusive k-ing steps
     * we will perform, the last step may not be a full
     * step depending on the outcome of the next conditional
     */
    pow_k = n_levels;

    /* figure out the largest power of tree_order that is less than or equal to
     * num_nodes */
    if ( cnt > num_nodes) {
        cnt /= tree_order;
        n_levels--;
    }

    /*exchange_node->log_tree_order = n_levels;*/
    exchange_node->log_tree_order = pow_k;
    exchange_node->n_largest_pow_tree_order = cnt;


    /* find the number of complete groups of size tree_order, tree_order^2, tree_order^3,...,tree_order^pow_k */
    /* I don't think we need to cache this info this group_info array */
    group_info = (int *) calloc(pow_k , sizeof(int));
    group_info[0] = num_nodes/tree_order;
    /*fprintf(stderr,"Number of complete groups of power 1 is %d\n",group_info[0]);*/
    for ( i = 1; i < pow_k; i ++) {
        group_info[i] = group_info[i-1]/tree_order;
        /*fprintf(stderr,"Number of complete groups of power %d is %d\n",i+1,group_info[i]);*/

    }

    /* find number of incomplete groups and number of ranks belonging to those ranks */
    knt=0;
    while (knt <= (pow_k - 1) && group_info[knt] > 0) {
        knt++;
    }
    knt--;
    /*fprintf(stderr,"Maximal power of k is %d and the number of incomplete groups is %d \n", knt+1 ,tree_order - group_info[knt] );*/

    /* k_temp is a synonym for cnt which is the largest full power of k group */
    /* now, start the calculation to find the first stray rank aka "extra" rank */
    stray = 0;
    /*fprintf(stderr,"Maximal power of k %d, first stragler rank is %d and the number of straglers is %d\n",cnt,
                                                                           cnt*group_info[knt],
                                                                           num_nodes - cnt*group_info[knt]);*/


    /* cache this info, it's muy importante */
    stray = cnt*group_info[knt];
    exchange_node->k_nomial_stray = stray;



    /* before we do this, we need to first reindex */
    /* reindexing phase */
     /* this is the reindex phase */
    exchange_node->reindex_map = (int *) malloc(num_nodes*sizeof(int));
    /* this is the inverse map */
    exchange_node->inv_reindex_map = (int *) malloc(num_nodes*sizeof(int));
    /*int reindex_myid;*/
    /* reindex */
    if( stray < num_nodes ) {
        /* find the first proxy rank */
        peer = stray - cnt;
        /* fix all ranks prior to this rank */
        for( i = 0; i < peer; i++){
            exchange_node->reindex_map[i] = i;
        }
        /* now, start the swap */
        exchange_node->reindex_map[peer] = peer;
        for( i = (peer+1); i < (peer + (num_nodes - stray)+1); i++) {
            exchange_node->reindex_map[i] = exchange_node->reindex_map[i-1] + 2;
        }
        i_temp = i;
        for( i = i_temp; i < stray; i++) {
            exchange_node->reindex_map[i] = exchange_node->reindex_map[i-1] + 1;
        }
        /* now, finish it off */
        exchange_node->reindex_map[stray] = peer + 1;
        for( i = (stray+1); i < num_nodes; i++) {
            exchange_node->reindex_map[i] = exchange_node->reindex_map[i-1] + 2;
        }
        /* debug print */
        /*
        for( i = 0; i < np; i++){
            fprintf(stderr,"%d ",reindex_map[i]);
        }
        fprintf(stderr,"\n");
        */
    } else {
        /* we have no extras, trivial reindexing */
        for( i = 0; i < num_nodes; i++){
            exchange_node->reindex_map[i] = i;
        }
    }
    /* finished reindexing */

    /* Now, I need to get my rank in the new indexing */
    for( i = 0; i < num_nodes; i++ ){
        if( node_rank == exchange_node->reindex_map[i] ){
            exchange_node->reindex_myid = i;
            break;
        }
    }
    /* Now, let's compute the inverse mapping here */
    for( i = 0; i < num_nodes; i++){
        j = 0;
        while(exchange_node->reindex_map[j] != i ){
            j++;
        }
        exchange_node->inv_reindex_map[i] = j;
    }


    /* Now we get the data sizes we should expect at each level */
    /* now get the size of the data I am to receive from each peer */
    /*int **payload_info;*/
    prev_data = (int *) malloc( num_nodes*sizeof(int) );
    if( NULL == prev_data ) {
        goto Error;
    }

    current_data = (int *) malloc( num_nodes*sizeof(int) );
    if( NULL == current_data ) {
        goto Error;
    }


    exchange_node->payload_info = (netpatterns_payload_t **) malloc(sizeof(netpatterns_payload_t *)*pow_k);
    if( NULL == exchange_node->payload_info) {
        goto Error;
    }

    for(i = 0; i < pow_k; i++){
        exchange_node->payload_info[i] = (netpatterns_payload_t *) malloc(sizeof(netpatterns_payload_t)*(tree_order-1));
        if( NULL == exchange_node->payload_info[i]) {
            goto Error;
        }

    }
    /* intialize the payload array
       This is the money struct, just need to initialize this with
       the subgroup information */
    /*
    for(i = 0; i < num_nodes; i++){
        prev_data[i] = 1;
        current_data[i] = 1;
    }
    */

    for(i = 0; i < num_nodes; i++){
        prev_data[i] = hier_ranks[i];
        current_data[i] = hier_ranks[i];
    }

    /* everyone will need to do this loop over all ranks
     * Phase I calculate the contribution from the extra ranks
     */
    for( myid = 0; myid < num_nodes; myid++) {
        /* get my new rank */
        for( j = 0; j < num_nodes; j++ ){
            /* this will be satisfied for one of the indices */
            if( myid == exchange_node->reindex_map[j] ){
                reindex_myid = j;
                break;
            }
        }

        for( j = stray; j < num_nodes; j++) {
            if(reindex_myid == ( j - cnt )) {
                /* then this is a proxy rank */
                prev_data[myid] += prev_data[exchange_node->reindex_map[j]];
                break;
            }

        }
    }

    /* Phase II calculate the contribution from each recursive k - ing level
     *
     */
    k_temp1 = tree_order; /* k^1 */
    k_temp2 = 1;   /* k^0 */
    peer_base = 0;
    base_temp = 0;
    for( i = 0; i < pow_k; i++) {
        /* get my new rank */
        for( myid = 0; myid < num_nodes; myid++){
            current_data[myid] = prev_data[myid];
            /*fprintf(stderr,"my current data at level %d is %d\n",i+1,current_data[myid]);*/
            for( j = 0; j < num_nodes; j++ ){
                if( myid == exchange_node->reindex_map[j] ){
                    reindex_myid = j;
                    break;
                }
            }
            if( reindex_myid < stray ) {
                /* now start the actual algorithm */
                FIND_BASE(base,reindex_myid,i+1,tree_order);
                for( j = 0; j < ( tree_order - 1 ); j ++ ) {
                    peer = base + (reindex_myid + k_temp2*(j+1))%k_temp1;
                    if( peer < stray ) {
                        /*fprintf(stderr,"getting %d bytes \n",prev_data[reindex_map[peer]]);*/
                        /* then get the data */
                        if( node_rank == myid ){
                            exchange_node->payload_info[i][j].r_len = prev_data[exchange_node->reindex_map[peer]];
                            /*fprintf(stderr,"exchange_node->payload_info[%d][%d].r_len %d\n",i,j,prev_data[exchange_node->reindex_map[peer]]);*/
                            if( i > 0 ) {

                                /* find my len and offset */
                                FIND_BASE(peer_base,peer,i,tree_order);
                                /* I do not want to mess with this, but it seems that I have no choice */
                               ex_node = exchange_node->reindex_map[peer_base];
                               /* now, find out how far down the line this guy really is */
                               knt2 =0;
                               for(kk = 0; kk < ex_node; kk++){
                                   knt2 += hier_ranks[kk];
                               }
                                exchange_node->payload_info[i][j].r_offset = knt2;
                                /*fprintf(stderr,"exchange_node->payload_info[%d][%d].r_offset %d\n",i,j,exchange_node->payload_info[i][j].r_offset);*/

                                FIND_BASE(base_temp,reindex_myid,i,tree_order);
                                ex_node = exchange_node->reindex_map[base_temp];
                                knt2 = 0;
                                for( kk = 0; kk < ex_node; kk++){
                                    knt2 += hier_ranks[kk];
                                }
                                exchange_node->payload_info[i][j].s_offset =
                                                                  knt2; /* exchange_node->reindex_map[base_temp]; */
                                /*fprintf(stderr,"exchange_node->payload_info[%d][%d].s_offset %d\n",i,j,exchange_node->payload_info[i][j].s_offset);*/
                            } else {
                                ex_node = exchange_node->reindex_map[peer];
                                knt2 =0;
                                for(kk = 0; kk < ex_node; kk++){
                                    knt2 += hier_ranks[kk];
                                }
                                exchange_node->payload_info[i][j].r_offset =
                                    knt2; /*exchange_node->reindex_map[peer]; */
                                /*fprintf(stderr,"exchange_node->payload_info[%d][%d].r_offset %d\n",i,j,exchange_node->payload_info[i][j].r_offset);*/
                                knt2 = 0;
                                for(kk = 0; kk < myid; kk++){
                                    knt2 += hier_ranks[kk];
                                }
                                exchange_node->payload_info[i][j].s_offset = knt2;
                                /*fprintf(stderr,"exchange_node->payload_info[%d][%d].s_offset %d\n",i,j, exchange_node->payload_info[i][j].s_offset);*/
                            }
                            /* how much I am to receive from this peer on this level */
                            /* how much I am to send to this peer on this level */
                            exchange_node->payload_info[i][j].s_len = prev_data[node_rank];
                            /*fprintf(stderr,"exchange_node->payload_info[%d][%d].s_len %d\n",i,j,prev_data[node_rank]);*/
                            /*fprintf(stderr,"I am rank %d receiveing %d bytes from rank %d at level %d\n",node_rank,
                                                                        prev_data[exchange_node->reindex_map[peer]],
                                                                        exchange_node->reindex_map[peer], i+1);*/
                            /*fprintf(stderr,"I am rank %d sending %d bytes to rank %d at level %d\n",node_rank,prev_data[myid],
                                      exchange_node->reindex_map[peer],i+1);*/
                        }

                        current_data[myid] += prev_data[exchange_node->reindex_map[peer]];
                    }
                }
            }


        }
        k_temp1 *= tree_order;
        k_temp2 *= tree_order;
        /* debug print */
       /* fprintf(stderr,"Level %d current data ",i+1);*/
        for( j = 0; j < num_nodes; j++){
           /* fprintf(stderr,"%d ",current_data[j]); */
            prev_data[j] = current_data[j];
        }
       /* fprintf(stderr,"\n");*/

    }


    /* this is the natural way to do recursive k-ing */
    /* should never have more than one extra rank per proxy */
    if( exchange_node->reindex_myid >= stray ){
        /*fprintf(stderr,"Rank %d is mapped onto proxy rank %d \n",exchange_node->reindex_myid,exchange_node->reindex_myid - cnt);*/
        exchange_node->node_type = EXTRA_NODE;
    } else {
        exchange_node->node_type = EXCHANGE_NODE;
    }

    /* set node characteristics - node that is not within the largest
     * power of tree_order will just send its data to node that will participate
     * in the recursive k-ing, and get the result back at the end.
     * set the initial and final data exchanges - those that are not
     * part of the recursive k-ing.
     */
    if (EXCHANGE_NODE == exchange_node->node_type)  {
        exchange_node->n_extra_sources = 0;
        for( i = stray; i < num_nodes; i++) {
            if(exchange_node->reindex_myid == ( i - cnt )) {
                /* then I am a proxy rank and there is only a
                 * single extra source
                 */
                exchange_node->n_extra_sources = 1;
                break;
            }
        }

        if (exchange_node->n_extra_sources > 0) {
            exchange_node->rank_extra_sources_array = (int *) malloc
                (exchange_node->n_extra_sources * sizeof(int));
            if( NULL == exchange_node->rank_extra_sources_array ) {
                goto Error;
            }
            /* you broke above */
            exchange_node->rank_extra_sources_array[0] = exchange_node->reindex_map[i];
        } else {
            exchange_node->rank_extra_sources_array = NULL;
        }
    } else {
        /* I am an extra rank, find my proxy rank */
        exchange_node->n_extra_sources = 1;

        exchange_node->rank_extra_sources_array = (int *) malloc
            (exchange_node->n_extra_sources * sizeof(int));
        if( NULL == exchange_node->rank_extra_sources_array ) {
            goto Error;
        }
        exchange_node->rank_extra_sources_array[0] = exchange_node->reindex_map[exchange_node->reindex_myid - cnt];
    }


    /* set the exchange pattern */
    if (EXCHANGE_NODE == exchange_node->node_type) {
        /* yep, that's right PLUS 1 */
        exchange_node->n_exchanges = n_levels + 1;
        /* initialize this */
        exchange_node->n_actual_exchanges = 0;
        /* Allocate 2 dimension array thak keeps
         rank exchange information for each step*/
        exchange_node->rank_exchanges = (int **) malloc
            (exchange_node->n_exchanges * sizeof(int *));
        if(NULL == exchange_node->rank_exchanges) {
            goto Error;
        }
        for (i = 0; i < exchange_node->n_exchanges; i++) {
            exchange_node->rank_exchanges[i] = (int *) malloc
                ((tree_order - 1) * sizeof(int));
            if( NULL == exchange_node->rank_exchanges ) {
                goto Error;
            }
        }
        k_temp1 = tree_order;
        k_temp2 = 1;
        /* fill in exchange partners */
        /* Ok, now we start with the actual algorithm */
        for( i = 0; i < exchange_node->n_exchanges; i ++) {
            /*fprintf(stderr,"Starting Level %d\n",i+1);*/

            FIND_BASE(base,exchange_node->reindex_myid,i+1,tree_order);
            /*fprintf(stderr,"Myid %d base %d\n",node_rank,base);*/
            for( j = 0; j < (tree_order-1); j ++ ) {
                peer = base + (exchange_node->reindex_myid + k_temp2*(j+1))%k_temp1;
                if ( peer < stray ) {
                    exchange_node->rank_exchanges[i][j] = exchange_node->reindex_map[peer];
                    /* an actual exchange occurs, bump the counter */

                } else {
                    /* out of range, skip it - do not bump the n_actual_exchanges counter */
                    exchange_node->rank_exchanges[i][j] = -1;
                }

            }
            k_temp1 *= tree_order;
            k_temp2 *= tree_order;
        }
        for(i = 0; i < pow_k; i++){
            for(j = 0; j < (tree_order-1); j++){
                if(-1 != exchange_node->rank_exchanges[i][j]){
                    /* then bump the counter */
                    exchange_node->n_actual_exchanges++;
                }
            }
        }

    } else {
        /* we are extra ranks and we don't participate in the exchange :( */
        exchange_node->n_exchanges=0;
        exchange_node->rank_exchanges=NULL;
    }


    /* set the number of tags needed per stripe - this must be the
     *   same across all procs in the communicator.
     */
    /* do we need this one */
    exchange_node->n_tags = tree_order * n_levels + 1;

    free(prev_data);
    free(current_data);
    free(group_info);

    /* successful return */
    return OMPI_SUCCESS;

Error:

    if (NULL != exchange_node->rank_extra_sources_array) {
        free(exchange_node->rank_extra_sources_array);
    }

    if (NULL != exchange_node->rank_exchanges) {
        for (i = 0; i < exchange_node->n_exchanges; i++) {
            if (NULL != exchange_node->rank_exchanges[i]) {
                free(exchange_node->rank_exchanges[i]);
            }
        }
        free(exchange_node->rank_exchanges);
    }

    if (NULL != prev_data ){
        free(prev_data);
    }

    if(NULL != current_data) {
        free(current_data);
    }

    if(NULL != group_info) {
        free(group_info);
    }

    /* error return */
    return OMPI_ERROR;
}

OMPI_DECLSPEC void ompi_netpatterns_cleanup_recursive_knomial_allgather_tree_node(
        netpatterns_k_exchange_node_t *exchange_node)
{
    int i;

    free(exchange_node->reindex_map);
    free(exchange_node->inv_reindex_map);
    if (exchange_node->n_extra_sources > 0) {
        free(exchange_node->rank_extra_sources_array) ;
        exchange_node->n_extra_sources = 0;
        exchange_node->rank_extra_sources_array = NULL;
    }
    if (exchange_node->n_exchanges > 0) {
        for (i=0; i < exchange_node->n_exchanges; i++) {
            free(exchange_node->rank_exchanges[i]);
            exchange_node->rank_exchanges[i] = NULL;
        }
        free(exchange_node->rank_exchanges);
        exchange_node->rank_exchanges = NULL;
        exchange_node->n_exchanges = 0;
    }
    for(i = 0; i < exchange_node->log_tree_order; i++){
        free(exchange_node->payload_info[i]);
    }
    free(exchange_node->payload_info);
}

OMPI_DECLSPEC int ompi_netpatterns_setup_recursive_knomial_tree_node(
        int num_nodes, int node_rank, int tree_order,
        netpatterns_k_exchange_node_t *exchange_node)
{
    /* local variables */
    int i, j, tmp, cnt;
    int n_levels;
    int k_base, kpow_num, peer;

    NETPATTERNS_VERBOSE(
            ("Enter ompi_netpatterns_setup_recursive_knomial_tree_node(num_nodes=%d, node_rank=%d, tree_order=%d)",
                num_nodes, node_rank, tree_order));

    assert(num_nodes > 1);
    assert(tree_order > 1);
    if (tree_order > num_nodes) {
        tree_order = num_nodes;
    }

    exchange_node->tree_order = tree_order;

    /* figure out number of levels in the tree */
    n_levels = 0;
    /* cnt - number of ranks in given level */
    cnt=1;
    while ( num_nodes > cnt ) {
        cnt *= tree_order;
        n_levels++;
    };

    /* figure out the largest power of tree_order that is less than or equal to
     * num_nodes */
    if ( cnt > num_nodes) {
        cnt /= tree_order;
        n_levels--;
    }

    exchange_node->log_tree_order = n_levels;
    exchange_node->n_largest_pow_tree_order = cnt;

    /* set node characteristics - node that is not within the largest
     *  power of tree_order will just send it's data to node that will participate
     *  in the recursive doubling, and get the result back at the end.
     */
    if (node_rank + 1 > cnt) {
        exchange_node->node_type = EXTRA_NODE;
    } else {
        exchange_node->node_type = EXCHANGE_NODE;
    }


    /* set the initial and final data exchanges - those that are not
     *   part of the recursive doubling.
     */
    if (EXCHANGE_NODE == exchange_node->node_type)  {
        exchange_node->n_extra_sources = 0;
        for (i = 0, tmp = node_rank * (tree_order - 1) + cnt + i;
                tmp < num_nodes && i < tree_order - 1;
                ++i, ++tmp) {
            ++exchange_node->n_extra_sources;
        }

        assert(exchange_node->n_extra_sources < tree_order);

        if (exchange_node->n_extra_sources > 0) {
            exchange_node->rank_extra_sources_array = (int *) malloc
                (exchange_node->n_extra_sources * sizeof(int));
            if( NULL == exchange_node->rank_extra_sources_array ) {
                goto Error;
            }
            for (i = 0, tmp = node_rank * (tree_order - 1) + cnt;
                    i < tree_order - 1 && tmp < num_nodes; ++i, ++tmp) {
                NETPATTERNS_VERBOSE(("extra_source#%d = %d", i, tmp));
                exchange_node->rank_extra_sources_array[i] = tmp;
            }
        } else {
            exchange_node->rank_extra_sources_array = NULL;
        }
    } else {
        exchange_node->n_extra_sources = 1;
        exchange_node->rank_extra_sources_array = (int *) malloc (sizeof(int));
        if( NULL == exchange_node->rank_extra_sources_array ) {
            goto Error;
        }
        exchange_node->rank_extra_sources_array[0] = (node_rank - cnt) / (tree_order - 1);
        NETPATTERNS_VERBOSE(("extra_source#%d = %d", 0,
                    exchange_node->rank_extra_sources_array[0] ));
    }

    /* set the exchange pattern */
    if (EXCHANGE_NODE == exchange_node->node_type) {
        exchange_node->n_exchanges = n_levels;
        /* Allocate 2 dimension array thak keeps
         rank exchange information for each step*/
        exchange_node->rank_exchanges = (int **) malloc
            (exchange_node->n_exchanges * sizeof(int *));
        if(NULL == exchange_node->rank_exchanges) {
            goto Error;
        }
        for (i = 0; i < exchange_node->n_exchanges; i++) {
            exchange_node->rank_exchanges[i] = (int *) malloc
                ((tree_order - 1) * sizeof(int));
            if( NULL == exchange_node->rank_exchanges ) {
                goto Error;
            }
        }
        /* fill in exchange partners */
        for(i = 0, kpow_num = 1; i < exchange_node->n_exchanges;
                                      i++, kpow_num *= tree_order) {
            k_base = node_rank / (kpow_num * tree_order);
            for(j = 1; j < tree_order; j++) {
                peer = node_rank + kpow_num * j;
                if (k_base != peer/(kpow_num * tree_order)) {
                    /* Wraparound the number */
                    peer = k_base * (kpow_num * tree_order)  +
                        peer % (kpow_num * tree_order);
                }
                exchange_node->rank_exchanges[i][j - 1] = peer;
                NETPATTERNS_VERBOSE(("rank_exchanges#(%d,%d)/%d = %d",
                            i, j, tree_order, peer));
            }
        }
    } else {
        exchange_node->n_exchanges=0;
        exchange_node->rank_exchanges=NULL;
    }

    /* set the number of tags needed per stripe - this must be the
     *   same across all procs in the communicator.
     */
    /* do we need this one */
    exchange_node->n_tags = tree_order * n_levels + 1;

    /* successful return */
    return OMPI_SUCCESS;

Error:

    ompi_netpatterns_cleanup_recursive_knomial_tree_node (exchange_node);

    /* error return */
    return OMPI_ERROR;
}

OMPI_DECLSPEC void ompi_netpatterns_cleanup_recursive_knomial_tree_node(
        netpatterns_k_exchange_node_t *exchange_node)
{
    int i;

    if (exchange_node->n_extra_sources > 0) {
        free(exchange_node->rank_extra_sources_array);
        exchange_node->rank_extra_sources_array = NULL;
        exchange_node->n_extra_sources = 0;
    }
    if (exchange_node->n_exchanges > 0) {
        for (i=0 ; i<exchange_node->n_exchanges; i++) {
            free(exchange_node->rank_exchanges[i]);
            exchange_node->rank_exchanges[i] = NULL;
        }
        free(exchange_node->rank_exchanges);
        exchange_node->rank_exchanges = NULL;
        exchange_node->n_exchanges = 0;
    }
}

#if 1
OMPI_DECLSPEC int ompi_netpatterns_setup_recursive_doubling_n_tree_node(int num_nodes, int node_rank, int tree_order,
        netpatterns_pair_exchange_node_t *exchange_node)
{
    /* local variables */
    int i, tmp, cnt;
    int n_levels;
    int shift, mask;

    NETPATTERNS_VERBOSE(("Enter ompi_netpatterns_setup_recursive_doubling_n_tree_node(num_nodes=%d, node_rank=%d, tree_order=%d)", num_nodes, node_rank, tree_order));

    assert(num_nodes > 1);
    while (tree_order > num_nodes) {
        tree_order /= 2;
    }

    exchange_node->tree_order = tree_order;
    /* We support only tree_order that are power of two */
    assert(0 == (tree_order & (tree_order - 1)));

    /* figure out number of levels in the tree */
    n_levels = 0;
    /* cnt - number of ranks in given level */
    cnt=1;
    while ( num_nodes > cnt ) {
        cnt *= tree_order;
        n_levels++;
    };

    /* figure out the largest power of tree_order that is less than or equal to
     * num_nodes */
    if ( cnt > num_nodes) {
        cnt /= tree_order;
        n_levels--;
    }
    exchange_node->log_tree_order = n_levels;
    if (2 == tree_order) {
        exchange_node->log_2 = exchange_node->log_tree_order;
    }

    tmp=1;
    for (i=0 ; i < n_levels ; i++ ) {
        tmp *= tree_order;
    }
    /* Ishai: I see no reason for calculating tmp. Add an assert before deleting it */
    assert(tmp == cnt);

    exchange_node->n_largest_pow_tree_order = tmp;
    if (2 == tree_order) {
        exchange_node->n_largest_pow_2 = exchange_node->n_largest_pow_tree_order;
    }

    /* set node characteristics - node that is not within the largest
     *  power of tree_order will just send it's data to node that will participate
     *  in the recursive doubling, and get the result back at the end.
     */
    if ( node_rank + 1 > cnt ) {
        exchange_node->node_type = EXTRA_NODE;
    } else {
        exchange_node->node_type = EXCHANGE_NODE;
    }

    /* set the initial and final data exchanges - those that are not
     *   part of the recursive doubling.
     */
    if ( EXCHANGE_NODE == exchange_node->node_type ) {
        exchange_node->n_extra_sources = 0;
        for (tmp = node_rank + cnt; tmp < num_nodes; tmp += cnt) {
            ++exchange_node->n_extra_sources;
        }
        if (exchange_node->n_extra_sources > 0) {
            exchange_node->rank_extra_sources_array = (int *) malloc
                (exchange_node->n_extra_sources * sizeof(int));
            if( NULL == exchange_node->rank_extra_sources_array ) {
                goto Error;
            }
            for (i = 0, tmp = node_rank + cnt; tmp < num_nodes; ++i, tmp += cnt) {
                NETPATTERNS_VERBOSE(("extra_source#%d = %d", i, tmp));
                exchange_node->rank_extra_sources_array[i] = tmp;
            }
        } else {
            exchange_node->rank_extra_sources_array = NULL;
        }
    } else {
        exchange_node->n_extra_sources = 1;
        exchange_node->rank_extra_sources_array = (int *) malloc (sizeof(int));
        if( NULL == exchange_node->rank_extra_sources_array ) {
            goto Error;
        }
        exchange_node->rank_extra_sources_array[0] = node_rank & (cnt - 1);
        NETPATTERNS_VERBOSE(("extra_source#%d = %d", 0, node_rank & (cnt - 1)));
    }

    /* Ishai: To be compatable with the old structure - should be remoived later */
    if (1 == exchange_node->n_extra_sources) {
        exchange_node->rank_extra_source = exchange_node->rank_extra_sources_array[0];
    } else {
        exchange_node->rank_extra_source = -1;
    }

    /* set the exchange pattern */
    if ( EXCHANGE_NODE == exchange_node->node_type ) {
        exchange_node->n_exchanges = n_levels * (tree_order - 1);
        exchange_node->rank_exchanges = (int *) malloc
            (exchange_node->n_exchanges * sizeof(int));
        if( NULL == exchange_node->rank_exchanges ) {
            goto Error;
        }

        /* fill in exchange partners */
        for ( i = 0, shift = 1 ; i < exchange_node->n_exchanges ; shift *= tree_order ) {
            for ( mask = 1 ; mask < tree_order ; ++mask, ++i ) {
                exchange_node->rank_exchanges[i] = node_rank ^ (mask * shift);
                NETPATTERNS_VERBOSE(("rank_exchanges#%d/%d = %d", i, tree_order, node_rank ^ (mask * shift)));
            }
        }

    } else {

        exchange_node->n_exchanges=0;
        exchange_node->rank_exchanges=NULL;

    }

    /* set the number of tags needed per stripe - this must be the
     *   same across all procs in the communicator.
     */
    /* Ishai: Need to find out what is n_tags */
    exchange_node->n_tags = tree_order * n_levels + 1;

    /* successful return */
    return OMPI_SUCCESS;

Error:
    if (exchange_node->rank_extra_sources_array != NULL) {
        free(exchange_node->rank_extra_sources_array);
    }

    /* error return */
    return OMPI_ERROR;
}

OMPI_DECLSPEC void ompi_netpatterns_cleanup_recursive_doubling_tree_node(
    netpatterns_pair_exchange_node_t *exchange_node)
{
    NETPATTERNS_VERBOSE(("About to release rank_extra_sources_array and rank_exchanges"));
    if (exchange_node->rank_extra_sources_array != NULL) {
        free(exchange_node->rank_extra_sources_array);
    }

    if (exchange_node->rank_exchanges != NULL) {
        free(exchange_node->rank_exchanges);
    }
}
#endif

OMPI_DECLSPEC int ompi_netpatterns_setup_recursive_doubling_tree_node(int num_nodes, int node_rank,
        netpatterns_pair_exchange_node_t *exchange_node)
{
    return ompi_netpatterns_setup_recursive_doubling_n_tree_node(num_nodes, node_rank, 2, exchange_node);
}

#if 0
/*OMPI_DECLSPEC int old_netpatterns_setup_recursive_doubling_tree_node(int num_nodes, int node_rank,*/
OMPI_DECLSPEC int ompi_netpatterns_setup_recursive_doubling_n_tree_node(int num_nodes, int node_rank,int tree_order,
        netpatterns_pair_exchange_node_t *exchange_node)
{
    /* local variables */
    /*int tree_order;*/
    int i,tmp,cnt,result,n_extra_nodes;
    int n_exchanges;

    /* figure out number of levels in the tree */

    n_exchanges=0;
    result=num_nodes;
/*    tree_order=2;*/
    /* cnt - number of ranks in given level */
    cnt=1;
    while( num_nodes > cnt ) {
        cnt*=tree_order;
        n_exchanges++;
    };

    /* figure out the largest power of 2 that is less than or equal to
     * num_nodes */
    if( cnt > num_nodes) {
        cnt/=tree_order;
        n_exchanges--;
    }
    exchange_node->log_2=n_exchanges;

    tmp=1;
    for(i=0 ; i < n_exchanges ; i++ ) {
        tmp*=2;
    }
    exchange_node->n_largest_pow_2=tmp;

    /* set node characteristics - node that is not within the largest
     *  power of 2 will just send it's data to node that will participate
     *  in the recursive doubling, and get the result back at the end.
     */
    if( node_rank+1 > cnt ) {
        exchange_node->node_type=EXTRA_NODE;
    } else {
        exchange_node->node_type=EXCHANGE_NODE;
    }

    /* set the initial and final data exchanges - those that are not
     *   part of the recursive doubling.
     */
    n_extra_nodes=num_nodes-cnt;

    if ( EXCHANGE_NODE == exchange_node->node_type ) {

        if( node_rank < n_extra_nodes ) {
            exchange_node->n_extra_sources=1;
            exchange_node->rank_extra_source=cnt+node_rank;
        } else {
            exchange_node->n_extra_sources=0;
            exchange_node->rank_extra_source=-1;
        }

    } else {
            exchange_node->n_extra_sources=1;
            exchange_node->rank_extra_source=node_rank-cnt;
    }

    /* set the exchange pattern */
    if( EXCHANGE_NODE == exchange_node->node_type ) {

        exchange_node->n_exchanges=n_exchanges;
        exchange_node->rank_exchanges=(int *) malloc
            (n_exchanges*sizeof(int));
        if( NULL == exchange_node->rank_exchanges ) {
            goto Error;
        }

        /* fill in exchange partners */
        result=1;
        tmp=node_rank;
        for( i=0 ; i < n_exchanges ; i++ ) {
            if(tmp & 1 ) {
                exchange_node->rank_exchanges[i]=
                    node_rank-result;
            } else {
                exchange_node->rank_exchanges[i]=
                    node_rank+result;
            }
            result*=2;
            tmp/=2;
        }

    } else {

        exchange_node->n_exchanges=0;
        exchange_node->rank_exchanges=NULL;

    }

    /* set the number of tags needed per stripe - this must be the
     *   same across all procs in the communicator.
     */
    exchange_node->n_tags=2*n_exchanges+1;

    /* Ishai: to make sure free will work also for people that call this function */
    exchange_node->rank_extra_sources_array = NULL;

    /* successful return */
    return OMPI_SUCCESS;

Error:

    /* error return */
    return OMPI_ERROR;
}
#endif

