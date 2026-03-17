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
#include "ompi/mca/coll/base/coll_base_util.h"
#include "coll_ops.h"
#include "commpatterns.h"

/**
 * All-reduce for contigous primitive types
 */
OMPI_DECLSPEC int ompi_comm_allreduce_pml(void *sbuf, void *rbuf, int count,
        ompi_datatype_t *dtype, int my_rank_in_group,
        struct ompi_op_t *op, int n_peers,int *ranks_in_comm,
        ompi_communicator_t *comm)
{
    /* local variables */
    int rc=OMPI_SUCCESS,n_dts_per_buffer,n_data_segments,stripe_number;
    int pair_rank,exchange,extra_rank;
    netpatterns_pair_exchange_node_t my_exchange_node;
    int count_processed,count_this_stripe;
    size_t dt_size,dt_extent;
    char scratch_bufers[2][MAX_TMP_BUFFER];
    int send_buffer=0,recv_buffer=1;
    char *sbuf_current, *rbuf_current;

    /* get size of data needed - same layout as user data, so that
     *   we can apply the reudction routines directly on these buffers
     */
    rc = opal_datatype_type_size((opal_datatype_t *)dtype, &dt_size);
    if( OMPI_SUCCESS != rc ) {
        goto Error;
    }
    rc = ompi_datatype_type_extent(dtype, (ptrdiff_t *)&dt_extent);
    if( OMPI_SUCCESS != rc ) {
        goto Error;
    }

    /* 1 process special case */
    if(1 == n_peers) {
        /* place my data in the correct destination buffer */
        rc=ompi_datatype_copy_content_same_ddt(dtype,count,
                (char *)rbuf, (char *)sbuf);
        if( OMPI_SUCCESS != rc ) {
            goto Error;
        }
        return OMPI_SUCCESS;
    }

    /* number of data types copies that the scratch buffer can hold */
    n_dts_per_buffer=((int) MAX_TMP_BUFFER)/dt_extent;
    if ( 0 == n_dts_per_buffer ) {
        rc=OMPI_ERROR;
        goto Error;
    }

    /* compute number of stripes needed to process this collective */
    n_data_segments=(count+n_dts_per_buffer -1 ) / n_dts_per_buffer ;

    /* get my reduction communication pattern */
    memset(&my_exchange_node, 0, sizeof(netpatterns_pair_exchange_node_t));
    rc = ompi_netpatterns_setup_recursive_doubling_tree_node(n_peers,
            my_rank_in_group, &my_exchange_node);
    if(OMPI_SUCCESS != rc){
        return rc;
    }

    count_processed=0;

    /* get a pointer to the shared-memory working buffer */
    /* NOTE: starting with a rather synchronous approach */
    for( stripe_number=0 ; stripe_number < n_data_segments ; stripe_number++ ) {

        /* get number of elements to process in this stripe */
        count_this_stripe=n_dts_per_buffer;
        if( count_processed + count_this_stripe > count )
            count_this_stripe=count-count_processed;

        /* copy data from the input buffer into the temp buffer */
        sbuf_current=(char *)sbuf+count_processed*dt_extent;
        rc=ompi_datatype_copy_content_same_ddt(dtype,count_this_stripe,
                scratch_bufers[send_buffer], sbuf_current);
        if( OMPI_SUCCESS != rc ) {
            goto Error;
        }

        /* copy data in from the "extra" source, if need be */
        if(0 < my_exchange_node.n_extra_sources)  {

            if ( EXCHANGE_NODE == my_exchange_node.node_type ) {

                /*
                ** Receive data from extra node
                */
                extra_rank=my_exchange_node.rank_extra_source;
                rc=MCA_PML_CALL(recv(scratch_bufers[recv_buffer],
                            count_this_stripe,dtype,ranks_in_comm[extra_rank],
                            -OMPI_COMMON_TAG_ALLREDUCE, comm,
                            MPI_STATUSES_IGNORE));
                if( 0 > rc ) {
                    fprintf(stderr,"  first recv failed in ompi_comm_allreduce_pml \n");
                    fflush(stderr);
                    goto  Error;
                }


                /* apply collective operation to first half of the data */
                if( 0 < count_this_stripe ) {
                    ompi_op_reduce(op,
                            (void *)scratch_bufers[send_buffer],
                            (void *)scratch_bufers[recv_buffer],
                            count_this_stripe,dtype);
                }


            } else {

                /*
                ** Send data to "partner" node
                */
                extra_rank=my_exchange_node.rank_extra_source;
                rc=MCA_PML_CALL(send(scratch_bufers[send_buffer],
                            count_this_stripe,dtype,ranks_in_comm[extra_rank],
                            -OMPI_COMMON_TAG_ALLREDUCE, MCA_PML_BASE_SEND_STANDARD,
                            comm));
                if( 0 > rc ) {
                    fprintf(stderr,"  first send failed in ompi_comm_allreduce_pml \n");
                    fflush(stderr);
                    goto  Error;
                }
            }

            /* change pointer to scratch buffer - this was we can send data
            ** that we have summed w/o a memory copy, and receive data into the
            ** other buffer, w/o fear of over writting data that has not yet
            ** completed being send
            */
            recv_buffer^=1;
            send_buffer^=1;
        }

        /* loop over data exchanges */
        for(exchange=0 ; exchange < my_exchange_node.n_exchanges ; exchange++) {

            /* is the remote data read */
            pair_rank=my_exchange_node.rank_exchanges[exchange];

            rc=ompi_coll_base_sendrecv_actual(scratch_bufers[send_buffer],
                                              count_this_stripe,dtype, ranks_in_comm[pair_rank],
                                              -OMPI_COMMON_TAG_ALLREDUCE,
                                              scratch_bufers[recv_buffer],
                                              count_this_stripe,dtype,ranks_in_comm[pair_rank],
                                              -OMPI_COMMON_TAG_ALLREDUCE,
                                              comm, MPI_STATUS_IGNORE);
            if( 0 > rc ) {
                fprintf(stderr,"  irecv failed in  ompi_comm_allreduce_pml at iterations %d \n",
                        exchange);
                fflush(stderr);
                goto Error;
            }

            /* reduce the data */
            if( 0 < count_this_stripe ) {
                ompi_op_reduce(op,
                        (void *)scratch_bufers[send_buffer],
                        (void *)scratch_bufers[recv_buffer],
                        count_this_stripe,dtype);
            }
            /* get ready for next step */
            recv_buffer^=1;
            send_buffer^=1;

        }

        /* copy data in from the "extra" source, if need be */
        if(0 < my_exchange_node.n_extra_sources)  {

            if ( EXTRA_NODE == my_exchange_node.node_type ) {
                /*
                ** receive the data
                ** */
                extra_rank=my_exchange_node.rank_extra_source;
                rc=MCA_PML_CALL(recv(scratch_bufers[recv_buffer],
                            count_this_stripe,dtype,ranks_in_comm[extra_rank],
                            -OMPI_COMMON_TAG_ALLREDUCE, comm,
                            MPI_STATUSES_IGNORE));
                if( 0 > rc ) {
                    fprintf(stderr,"  last recv failed in ompi_comm_allreduce_pml \n");
                    fflush(stderr);
                    goto  Error;
                }

                recv_buffer^=1;
                send_buffer^=1;
            } else {
                /* send the data to the pair-rank outside of the power of 2 set
                ** of ranks
                */

                extra_rank=my_exchange_node.rank_extra_source;
                rc=MCA_PML_CALL(send((char *)scratch_bufers[send_buffer],
                            count_this_stripe,dtype,ranks_in_comm[extra_rank],
                            -OMPI_COMMON_TAG_ALLREDUCE, MCA_PML_BASE_SEND_STANDARD,
                            comm));
                if( 0 > rc ) {
                    fprintf(stderr,"  last send failed in ompi_comm_allreduce_pml \n");
                    fflush(stderr);
                    goto  Error;
                }
            }
        }

        /* copy data from the temp buffer into the output buffer */
        rbuf_current = (char *) rbuf + count_processed * dt_size;
        memcpy(rbuf_current,scratch_bufers[send_buffer], count_this_stripe*dt_size);

        /* update the count of elements processed */
        count_processed += count_this_stripe;
    }

    ompi_netpatterns_cleanup_recursive_doubling_tree_node(&my_exchange_node);

    /* return */
    return OMPI_SUCCESS;

Error:
    return rc;
}
