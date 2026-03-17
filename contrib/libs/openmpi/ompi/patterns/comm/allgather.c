/*
 * Copyright (c) 2009-2012 Mellanox Technologies.  All rights reserved.
 * Copyright (c) 2009-2012 Oak Ridge National Laboratory.  All rights reserved.
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file */

#include "ompi_config.h"

#include "ompi/constants.h"
#include "ompi/op/op.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/communicator/communicator.h"
#include "opal/include/opal/sys/atomic.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/patterns/net/netpatterns.h"
#include "coll_ops.h"

/**
 * All-reduce - subgroup in communicator
 */
OMPI_DECLSPEC int ompi_comm_allgather_pml(void *src_buf, void *dest_buf, int count,
        ompi_datatype_t *dtype, int my_rank_in_group,
        int n_peers, int *ranks_in_comm,ompi_communicator_t *comm)
{
    /* local variables */
    int rc=OMPI_SUCCESS,msg_cnt;
    int pair_rank,exchange,extra_rank, n_extra_nodes,n_extra;
    int proc_block,extra_start,extra_end,iovec_len;
    int remote_data_start_rank,remote_data_end_rank;
    int local_data_start_rank;
    netpatterns_pair_exchange_node_t my_exchange_node;
    size_t message_extent,current_data_extent,current_data_count;
    size_t dt_size;
    ptrdiff_t dt_extent;
    char *src_buf_current;
    char *dest_buf_current;
    struct iovec send_iov[2] = {{0,0},{0,0}},
                 recv_iov[2] = {{0,0},{0,0}};
    ompi_request_t *requests[4];

    /* get size of data needed - same layout as user data, so that
     *   we can apply the reudction routines directly on these buffers
     */
    rc = ompi_datatype_type_size(dtype, &dt_size);
    if( OMPI_SUCCESS != rc ) {
        goto Error;
    }

    rc = ompi_datatype_type_extent(dtype, &dt_extent);
    if( OMPI_SUCCESS != rc ) {
        goto Error;
    }
    message_extent = dt_extent*count;

    /* place my data in the correct destination buffer */
    rc=ompi_datatype_copy_content_same_ddt(dtype,count,
            (char *)dest_buf+my_rank_in_group*message_extent,
            (char *)src_buf);
    if( OMPI_SUCCESS != rc ) {
        goto Error;
    }

    /* 1 process special case */
    if(1 == n_peers) {
        return OMPI_SUCCESS;
    }

    /* get my reduction communication pattern */
    memset(&my_exchange_node, 0, sizeof(netpatterns_pair_exchange_node_t));
    rc = ompi_netpatterns_setup_recursive_doubling_tree_node(n_peers,
            my_rank_in_group, &my_exchange_node);
    if(OMPI_SUCCESS != rc){
        return rc;
    }

    n_extra_nodes=n_peers-my_exchange_node.n_largest_pow_2;

    /* get the data from the extra sources */
    if(0 < my_exchange_node.n_extra_sources)  {

        if ( EXCHANGE_NODE == my_exchange_node.node_type ) {

            /*
             ** Receive data from extra node
             */

            extra_rank=my_exchange_node.rank_extra_source;
            /* receive the data into the correct location - will use 2
             * messages in the recursive doubling phase */
            dest_buf_current=(char *)dest_buf+message_extent*extra_rank;
            rc=MCA_PML_CALL(recv(dest_buf_current,
                    count,dtype,ranks_in_comm[extra_rank],
                    -OMPI_COMMON_TAG_ALLREDUCE,
                    comm, MPI_STATUSES_IGNORE));
            if( 0 > rc ) {
                goto  Error;
            }

        } else {

            /*
             ** Send data to "partner" node
             */
            extra_rank=my_exchange_node.rank_extra_source;
            src_buf_current=(char *)src_buf;
            rc=MCA_PML_CALL(send(src_buf_current,
                    count,dtype,ranks_in_comm[extra_rank],
                    -OMPI_COMMON_TAG_ALLREDUCE,
                    MCA_PML_BASE_SEND_STANDARD,
                    comm));
            if( 0 > rc ) {
                goto  Error;
            }
        }
    }

    current_data_extent=message_extent;
    current_data_count=count;
    src_buf_current=(char *)dest_buf+my_rank_in_group*message_extent;
    proc_block=1;
    local_data_start_rank=my_rank_in_group;
    /* loop over data exchanges */
    for(exchange=0 ; exchange < my_exchange_node.n_exchanges ; exchange++) {

        /* is the remote data read */
        pair_rank=my_exchange_node.rank_exchanges[exchange];
        msg_cnt=0;

        /*
         * Power of 2 data segment
         */
        /* post non-blocking receive */
        if(pair_rank > my_rank_in_group ){
            recv_iov[0].iov_base=src_buf_current+current_data_extent;
            recv_iov[0].iov_len=current_data_extent;
            iovec_len=1;
            remote_data_start_rank=local_data_start_rank+proc_block;
            remote_data_end_rank=remote_data_start_rank+proc_block-1;
        } else {
            recv_iov[0].iov_base=src_buf_current-current_data_extent;
            recv_iov[0].iov_len=current_data_extent;
            iovec_len=1;
            remote_data_start_rank=local_data_start_rank-proc_block;
            remote_data_end_rank=remote_data_start_rank+proc_block-1;
        }
        /* the data from the non power of 2 ranks */
        if(remote_data_start_rank<n_extra_nodes) {
            /* figure out how much data is at the remote rank */
            /* last rank with data */
            extra_start=remote_data_start_rank;
            extra_end=remote_data_end_rank;
            if(extra_end >= n_extra_nodes ) {
                /* if last rank exceeds the ranks with extra data,
                 * adjust this.
                 */
                extra_end=n_extra_nodes-1;
            }
            /* get the number of ranks whos data is to be grabbed */
            n_extra=extra_end-extra_start+1;

            recv_iov[1].iov_base=(char *)dest_buf+
                (extra_start+my_exchange_node.n_largest_pow_2)*message_extent;
            recv_iov[1].iov_len=n_extra*count;
            iovec_len=2;
        }

        rc=MCA_PML_CALL(irecv(recv_iov[0].iov_base,
                    current_data_count,dtype,ranks_in_comm[pair_rank],
                    -OMPI_COMMON_TAG_ALLREDUCE,
                    comm,&(requests[msg_cnt])));
        if( 0 > rc ) {
            goto Error;
        }
        msg_cnt++;

        if(iovec_len > 1 ) {
            rc=MCA_PML_CALL(irecv(recv_iov[1].iov_base,
                        recv_iov[1].iov_len,dtype,ranks_in_comm[pair_rank],
                        -OMPI_COMMON_TAG_ALLREDUCE,
                        comm,&(requests[msg_cnt])));
            if( 0 > rc ) {
                goto Error;
            }
            msg_cnt++;
        }

        /* post non-blocking send */
        send_iov[0].iov_base=src_buf_current;
        send_iov[0].iov_len=current_data_extent;
        iovec_len=1;
        /* the data from the non power of 2 ranks */
        if(local_data_start_rank<n_extra_nodes) {
            /* figure out how much data is at the remote rank */
            /* last rank with data */
            extra_start=local_data_start_rank;
            extra_end=extra_start+proc_block-1;
            if(extra_end >= n_extra_nodes ) {
                /* if last rank exceeds the ranks with extra data,
                 * adjust this.
                 */
                extra_end=n_extra_nodes-1;
            }
            /* get the number of ranks whos data is to be grabbed */
            n_extra=extra_end-extra_start+1;

            send_iov[1].iov_base=(char *)dest_buf+
                (extra_start+my_exchange_node.n_largest_pow_2)*message_extent;
            send_iov[1].iov_len=n_extra*count;
            iovec_len=2;
        }

        rc=MCA_PML_CALL(isend(send_iov[0].iov_base,
                    current_data_count,dtype,ranks_in_comm[pair_rank],
                    -OMPI_COMMON_TAG_ALLREDUCE,MCA_PML_BASE_SEND_STANDARD,
                    comm,&(requests[msg_cnt])));
        if( 0 > rc ) {
            goto Error;
        }
        msg_cnt++;
        if( iovec_len > 1 ) {
            rc=MCA_PML_CALL(isend(send_iov[1].iov_base,
                        send_iov[1].iov_len,dtype,ranks_in_comm[pair_rank],
                        -OMPI_COMMON_TAG_ALLREDUCE,MCA_PML_BASE_SEND_STANDARD,
                        comm,&(requests[msg_cnt])));
            if( 0 > rc ) {
                goto Error;
            }
            msg_cnt++;
        }

        /* prepare the source buffer for the next iteration */
        if(pair_rank < my_rank_in_group ){
            src_buf_current-=current_data_extent;
            local_data_start_rank-=proc_block;
        }
        proc_block*=2;
        current_data_extent*=2;
        current_data_count*=2;

        /* wait on send and receive completion */
        ompi_request_wait_all(msg_cnt,requests,MPI_STATUSES_IGNORE);
    }

    /* copy data in from the "extra" source, if need be */
    if(0 < my_exchange_node.n_extra_sources)  {

        if ( EXTRA_NODE == my_exchange_node.node_type ) {
            /*
             ** receive the data
             ** */
            extra_rank=my_exchange_node.rank_extra_source;

            rc=MCA_PML_CALL(recv(dest_buf,
                    count*n_peers,dtype,ranks_in_comm[extra_rank],
                    -OMPI_COMMON_TAG_ALLREDUCE,
                    comm,MPI_STATUSES_IGNORE));
            if(0 > rc ) {
                goto  Error;
            }
        } else {
            /* send the data to the pair-rank outside of the power of 2 set
             ** of ranks
             */

            extra_rank=my_exchange_node.rank_extra_source;
            rc=MCA_PML_CALL(send(dest_buf,
                    count*n_peers,dtype,ranks_in_comm[extra_rank],
                    -OMPI_COMMON_TAG_ALLREDUCE,
                    MCA_PML_BASE_SEND_STANDARD,
                    comm));
            if( 0 > rc ) {
                goto  Error;
            }
        }
    }

    ompi_netpatterns_cleanup_recursive_doubling_tree_node(&my_exchange_node);

    /* return */
    return OMPI_SUCCESS;

Error:
    return rc;
}
