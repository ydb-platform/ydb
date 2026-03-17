/*
 * Copyright (c) 2018      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 *
 * Copyright (c) 2018      Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_spc.h"

opal_timer_t sys_clock_freq_mhz = 0;

static void ompi_spc_dump(void);

/* Array for converting from SPC indices to MPI_T indices */
OMPI_DECLSPEC int mpi_t_offset = -1;
OMPI_DECLSPEC bool mpi_t_enabled = false;

OPAL_DECLSPEC ompi_communicator_t *comm = NULL;

typedef struct ompi_spc_event_t {
    const char* counter_name;
    const char* counter_description;
} ompi_spc_event_t;

#define SET_COUNTER_ARRAY(NAME, DESC)   [NAME] = { .counter_name = #NAME, .counter_description = DESC }

static ompi_spc_event_t ompi_spc_events_names[OMPI_SPC_NUM_COUNTERS] = {
    SET_COUNTER_ARRAY(OMPI_SPC_SEND, "The number of times MPI_Send was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_BSEND, "The number of times MPI_Bsend was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_RSEND, "The number of times MPI_Rsend was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_SSEND, "The number of times MPI_Ssend was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_RECV, "The number of times MPI_Recv was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_MRECV, "The number of times MPI_Mrecv was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ISEND, "The number of times MPI_Isend was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IBSEND, "The number of times MPI_Ibsend was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IRSEND, "The number of times MPI_Irsend was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ISSEND, "The number of times MPI_Issend was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IRECV, "The number of times MPI_Irecv was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_SENDRECV, "The number of times MPI_Sendrecv was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_SENDRECV_REPLACE, "The number of times MPI_Sendrecv_replace was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_PUT, "The number of times MPI_Put was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_RPUT, "The number of times MPI_Rput was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_GET, "The number of times MPI_Get was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_RGET, "The number of times MPI_Rget was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_PROBE, "The number of times MPI_Probe was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IPROBE, "The number of times MPI_Iprobe was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_BCAST, "The number of times MPI_Bcast was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IBCAST, "The number of times MPI_Ibcast was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_BCAST_INIT, "The number of times MPIX_Bcast_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_REDUCE, "The number of times MPI_Reduce was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_REDUCE_SCATTER, "The number of times MPI_Reduce_scatter was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_REDUCE_SCATTER_BLOCK, "The number of times MPI_Reduce_scatter_block was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IREDUCE, "The number of times MPI_Ireduce was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IREDUCE_SCATTER, "The number of times MPI_Ireduce_scatter was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IREDUCE_SCATTER_BLOCK, "The number of times MPI_Ireduce_scatter_block was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_REDUCE_INIT, "The number of times MPIX_Reduce_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_REDUCE_SCATTER_INIT, "The number of times MPIX_Reduce_scatter_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_REDUCE_SCATTER_BLOCK_INIT, "The number of times MPIX_Reduce_scatter_block_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ALLREDUCE, "The number of times MPI_Allreduce was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IALLREDUCE, "The number of times MPI_Iallreduce was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ALLREDUCE_INIT, "The number of times MPIX_Allreduce_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_SCAN, "The number of times MPI_Scan was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_EXSCAN, "The number of times MPI_Exscan was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ISCAN, "The number of times MPI_Iscan was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IEXSCAN, "The number of times MPI_Iexscan was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_SCAN_INIT, "The number of times MPIX_Scan_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_EXSCAN_INIT, "The number of times MPIX_Exscan_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_SCATTER, "The number of times MPI_Scatter was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_SCATTERV, "The number of times MPI_Scatterv was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ISCATTER, "The number of times MPI_Iscatter was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ISCATTERV, "The number of times MPI_Iscatterv was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_SCATTER_INIT, "The number of times MPIX_Scatter_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_SCATTERV_INIT, "The number of times MPIX_Scatterv_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_GATHER, "The number of times MPI_Gather was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_GATHERV, "The number of times MPI_Gatherv was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IGATHER, "The number of times MPI_Igather was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IGATHERV, "The number of times MPI_Igatherv was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_GATHER_INIT, "The number of times MPIX_Gather_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_GATHERV_INIT, "The number of times MPIX_Gatherv_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ALLTOALL, "The number of times MPI_Alltoall was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ALLTOALLV, "The number of times MPI_Alltoallv was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ALLTOALLW, "The number of times MPI_Alltoallw was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IALLTOALL, "The number of times MPI_Ialltoall was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IALLTOALLV, "The number of times MPI_Ialltoallv was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IALLTOALLW, "The number of times MPI_Ialltoallw was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ALLTOALL_INIT, "The number of times MPIX_Alltoall_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ALLTOALLV_INIT, "The number of times MPIX_Alltoallv_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ALLTOALLW_INIT, "The number of times MPIX_Alltoallw_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_NEIGHBOR_ALLTOALL, "The number of times MPI_Neighbor_alltoall was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_NEIGHBOR_ALLTOALLV, "The number of times MPI_Neighbor_alltoallv was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_NEIGHBOR_ALLTOALLW, "The number of times MPI_Neighbor_alltoallw was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_INEIGHBOR_ALLTOALL, "The number of times MPI_Ineighbor_alltoall was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_INEIGHBOR_ALLTOALLV, "The number of times MPI_Ineighbor_alltoallv was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_INEIGHBOR_ALLTOALLW, "The number of times MPI_Ineighbor_alltoallw was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_NEIGHBOR_ALLTOALL_INIT, "The number of times MPIX_Neighbor_alltoall_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_NEIGHBOR_ALLTOALLV_INIT, "The number of times MPIX_Neighbor_alltoallv_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_NEIGHBOR_ALLTOALLW_INIT, "The number of times MPIX_Neighbor_alltoallw_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ALLGATHER, "The number of times MPI_Allgather was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ALLGATHERV, "The number of times MPI_Allgatherv was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IALLGATHER, "The number of times MPI_Iallgather was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IALLGATHERV, "The number of times MPI_Iallgatherv was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ALLGATHER_INIT, "The number of times MPIX_Allgather_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_ALLGATHERV_INIT, "The number of times MPIX_Allgatherv_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_NEIGHBOR_ALLGATHER, "The number of times MPI_Neighbor_allgather was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_NEIGHBOR_ALLGATHERV, "The number of times MPI_Neighbor_allgatherv was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_INEIGHBOR_ALLGATHER, "The number of times MPI_Ineighbor_allgather was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_INEIGHBOR_ALLGATHERV, "The number of times MPI_Ineighbor_allgatherv was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_NEIGHBOR_ALLGATHER_INIT, "The number of times MPIX_Neighbor_allgather_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_NEIGHBOR_ALLGATHERV_INIT, "The number of times MPIX_Neighbor_allgatherv_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_TEST, "The number of times MPI_Test was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_TESTALL, "The number of times MPI_Testall was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_TESTANY, "The number of times MPI_Testany was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_TESTSOME, "The number of times MPI_Testsome was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_WAIT, "The number of times MPI_Wait was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_WAITALL, "The number of times MPI_Waitall was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_WAITANY, "The number of times MPI_Waitany was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_WAITSOME, "The number of times MPI_Waitsome was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_BARRIER, "The number of times MPI_Barrier was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_IBARRIER, "The number of times MPI_Ibarrier was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_BARRIER_INIT, "The number of times MPIX_Barrier_init was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_WTIME, "The number of times MPI_Wtime was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_CANCEL, "The number of times MPI_Cancel was called."),
    SET_COUNTER_ARRAY(OMPI_SPC_BYTES_RECEIVED_USER, "The number of bytes received by the user through point-to-point communications. Note: Includes bytes transferred using internal RMA operations."),
    SET_COUNTER_ARRAY(OMPI_SPC_BYTES_RECEIVED_MPI, "The number of bytes received by MPI through collective, control, or other internal communications."),
    SET_COUNTER_ARRAY(OMPI_SPC_BYTES_SENT_USER, "The number of bytes sent by the user through point-to-point communications.  Note: Includes bytes transferred using internal RMA operations."),
    SET_COUNTER_ARRAY(OMPI_SPC_BYTES_SENT_MPI, "The number of bytes sent by MPI through collective, control, or other internal communications."),
    SET_COUNTER_ARRAY(OMPI_SPC_BYTES_PUT, "The number of bytes sent/received using RMA Put operations both through user-level Put functions and internal Put functions."),
    SET_COUNTER_ARRAY(OMPI_SPC_BYTES_GET, "The number of bytes sent/received using RMA Get operations both through user-level Get functions and internal Get functions."),
    SET_COUNTER_ARRAY(OMPI_SPC_UNEXPECTED, "The number of messages that arrived as unexpected messages."),
    SET_COUNTER_ARRAY(OMPI_SPC_OUT_OF_SEQUENCE, "The number of messages that arrived out of the proper sequence."),
    SET_COUNTER_ARRAY(OMPI_SPC_MATCH_TIME, "The number of microseconds spent matching unexpected messages.  Note: The timer used on the back end is in cycles, which could potentially be problematic on a system where the clock frequency can change.  On such a system, this counter could be inaccurate since we assume a fixed clock rate."),
    SET_COUNTER_ARRAY(OMPI_SPC_UNEXPECTED_IN_QUEUE, "The number of messages that are currently in the unexpected message queue(s) of an MPI process."),
    SET_COUNTER_ARRAY(OMPI_SPC_OOS_IN_QUEUE, "The number of messages that are currently in the out of sequence message queue(s) of an MPI process."),
    SET_COUNTER_ARRAY(OMPI_SPC_MAX_UNEXPECTED_IN_QUEUE, "The maximum number of messages that the unexpected message queue(s) within an MPI process "
                                                    "contained at once since the last reset of this counter. Note: This counter is reset each time it is read."),
    SET_COUNTER_ARRAY(OMPI_SPC_MAX_OOS_IN_QUEUE, "The maximum number of messages that the out of sequence message queue(s) within an MPI process "
                                             "contained at once since the last reset of this counter. Note: This counter is reset each time it is read.")
};

/* An array of integer values to denote whether an event is activated (1) or not (0) */
static uint32_t ompi_spc_attached_event[OMPI_SPC_NUM_COUNTERS / sizeof(uint32_t)] = { 0 };
/* An array of integer values to denote whether an event is timer-based (1) or not (0) */
static uint32_t ompi_spc_timer_event[OMPI_SPC_NUM_COUNTERS / sizeof(uint32_t)] = { 0 };
/* An array of event structures to store the event data (name and value) */
static ompi_spc_t *ompi_spc_events = NULL;

static inline void SET_SPC_BIT(uint32_t* array, int32_t pos)
{
    assert(pos < OMPI_SPC_NUM_COUNTERS);
    array[pos / (8 * sizeof(uint32_t))] |= (1U << (pos % (8 * sizeof(uint32_t))));
}

static inline bool IS_SPC_BIT_SET(uint32_t* array, int32_t pos)
{
    assert(pos < OMPI_SPC_NUM_COUNTERS);
    return !!(array[pos / (8 * sizeof(uint32_t))] & (1U << (pos % (8 * sizeof(uint32_t)))));
}

static inline void CLEAR_SPC_BIT(uint32_t* array, int32_t pos)
{
    assert(pos < OMPI_SPC_NUM_COUNTERS);
    array[pos / (8 * sizeof(uint32_t))] &= ~(1U << (pos % (8 * sizeof(uint32_t))));
}

/* ##############################################################
 * ################# Begin MPI_T Functions ######################
 * ##############################################################
 */
static int ompi_spc_notify(mca_base_pvar_t *pvar, mca_base_pvar_event_t event, void *obj_handle, int *count)
    __opal_attribute_unused__;

static int ompi_spc_notify(mca_base_pvar_t *pvar, mca_base_pvar_event_t event, void *obj_handle, int *count)
{
    int index;

    if(OPAL_LIKELY(!mpi_t_enabled)) {
        return MPI_SUCCESS;
    }

    /* For this event, we need to set count to the number of long long type
     * values for this counter.  All SPC counters are one long long, so we
     * always set count to 1.
     */
    if(MCA_BASE_PVAR_HANDLE_BIND == event) {
        *count = 1;
    }
    /* For this event, we need to turn on the counter */
    else if(MCA_BASE_PVAR_HANDLE_START == event) {
        /* Convert from MPI_T pvar index to SPC index */
        index = pvar->pvar_index - mpi_t_offset;
        SET_SPC_BIT(ompi_spc_attached_event, index);
    }
    /* For this event, we need to turn off the counter */
    else if(MCA_BASE_PVAR_HANDLE_STOP == event) {
        /* Convert from MPI_T pvar index to SPC index */
        index = pvar->pvar_index - mpi_t_offset;
        CLEAR_SPC_BIT(ompi_spc_attached_event, index);
    }

    return MPI_SUCCESS;
}

/* ##############################################################
 * ################# Begin SPC Functions ########################
 * ##############################################################
 */

/* This function returns the current count of an SPC counter that has been retistered
 * as an MPI_T pvar.  The MPI_T index is not necessarily the same as the SPC index,
 * so we need to convert from MPI_T index to SPC index and then set the 'value' argument
 * to the correct value for this pvar.
 */
static int ompi_spc_get_count(const struct mca_base_pvar_t *pvar, void *value, void *obj_handle)
    __opal_attribute_unused__;

static int ompi_spc_get_count(const struct mca_base_pvar_t *pvar, void *value, void *obj_handle)
{
    long long *counter_value = (long long*)value;

    if(OPAL_LIKELY(!mpi_t_enabled)) {
        *counter_value = 0;
        return MPI_SUCCESS;
    }

    /* Convert from MPI_T pvar index to SPC index */
    int index = pvar->pvar_index - mpi_t_offset;
    /* Set the counter value to the current SPC value */
    *counter_value = (long long)ompi_spc_events[index].value;
    /* If this is a timer-based counter, convert from cycles to microseconds */
    if( IS_SPC_BIT_SET(ompi_spc_timer_event, index) ) {
        *counter_value /= sys_clock_freq_mhz;
    }
    /* If this is a high watermark counter, reset it after it has been read */
    if(index == OMPI_SPC_MAX_UNEXPECTED_IN_QUEUE || index == OMPI_SPC_MAX_OOS_IN_QUEUE) {
        ompi_spc_events[index].value = 0;
    }

    return MPI_SUCCESS;
}

/* Initializes the events data structure and allocates memory for it if needed. */
void ompi_spc_events_init(void)
{
    int i;

    /* If the events data structure hasn't been allocated yet, allocate memory for it */
    if(NULL == ompi_spc_events) {
        ompi_spc_events = (ompi_spc_t*)malloc(OMPI_SPC_NUM_COUNTERS * sizeof(ompi_spc_t));
        if(ompi_spc_events == NULL) {
            opal_show_help("help-mpi-runtime.txt", "lib-call-fail", true,
                           "malloc", __FILE__, __LINE__);
            return;
        }
    }
    /* The data structure has been allocated, so we simply initialize all of the counters
     * with their names and an initial count of 0.
     */
    for(i = 0; i < OMPI_SPC_NUM_COUNTERS; i++) {
        ompi_spc_events[i].name = (char*)ompi_spc_events_names[i].counter_name;
        ompi_spc_events[i].value = 0;
    }

    ompi_comm_dup(&ompi_mpi_comm_world.comm, &comm);
}

/* Initializes the SPC data structures and registers all counters as MPI_T pvars.
 * Turns on only the counters that were specified in the mpi_spc_attach MCA parameter.  
 */
void ompi_spc_init(void)
{
    int i, j, ret, found = 0, all_on = 0;

    /* Initialize the clock frequency variable as the CPU's frequency in MHz */
    sys_clock_freq_mhz = opal_timer_base_get_freq() / 1000000;

    ompi_spc_events_init();

    /* Get the MCA params string of counters to turn on */
    char **arg_strings = opal_argv_split(ompi_mpi_spc_attach_string, ',');
    int num_args       = opal_argv_count(arg_strings);

    /* If there is only one argument and it is 'all', then all counters
     * should be turned on.  If the size is 0, then no counters will be enabled.
     */
    if(1 == num_args) {
        if(strcmp(arg_strings[0], "all") == 0) {
            all_on = 1;
        }
    }

    /* Turn on only the counters that were specified in the MCA parameter */
    for(i = 0; i < OMPI_SPC_NUM_COUNTERS; i++) {
        if(all_on) {
            SET_SPC_BIT(ompi_spc_attached_event, i);
            mpi_t_enabled = true;
            found++;
        } else {
            /* Note: If no arguments were given, this will be skipped */
            for(j = 0; j < num_args; j++) {
                if( 0 == strcmp(ompi_spc_events_names[i].counter_name, arg_strings[j]) ) {
                    SET_SPC_BIT(ompi_spc_attached_event, i);
                    mpi_t_enabled = true;
                    found++;
                    break;
                }
            }
        }

        /* ########################################################################
         * ################## Add Timer-Based Counter Enums Here ##################
         * ########################################################################
         */
        CLEAR_SPC_BIT(ompi_spc_timer_event, i);

        /* Registers the current counter as an MPI_T pvar regardless of whether it's been turned on or not */
        ret = mca_base_pvar_register("ompi", "runtime", "spc", ompi_spc_events_names[i].counter_name, ompi_spc_events_names[i].counter_description,
                                     OPAL_INFO_LVL_4, MPI_T_PVAR_CLASS_SIZE,
                                     MCA_BASE_VAR_TYPE_UNSIGNED_LONG_LONG, NULL, MPI_T_BIND_NO_OBJECT,
                                     MCA_BASE_PVAR_FLAG_READONLY | MCA_BASE_PVAR_FLAG_CONTINUOUS,
                                     ompi_spc_get_count, NULL, ompi_spc_notify, NULL);

        /* Check to make sure that ret is a valid index and not an error code.
         */
        if( ret >= 0 ) {
            if( mpi_t_offset == -1 ) {
                mpi_t_offset = ret;
            }
        }
        if( (ret < 0) || (ret != (mpi_t_offset + found - 1)) ) {
            mpi_t_enabled = false;
            opal_show_help("help-mpi-runtime.txt", "spc: MPI_T disabled", true);
            break;
        }
    }
    /* If this is a timer event, sent the corresponding timer_event entry to 1 */
    SET_SPC_BIT(ompi_spc_timer_event, OMPI_SPC_MATCH_TIME);
    opal_argv_free(arg_strings);
}

/* Gathers all of the SPC data onto rank 0 of MPI_COMM_WORLD and prints out all
 * of the counter values to stdout.
 */
static void ompi_spc_dump(void)
{
    int i, j, world_size, offset;
    long long *recv_buffer = NULL, *send_buffer;

    int rank = ompi_comm_rank(comm);
    world_size = ompi_comm_size(comm);

    /* Convert from cycles to usecs before sending */
    for(i = 0; i < OMPI_SPC_NUM_COUNTERS; i++) {
        if( IS_SPC_BIT_SET(ompi_spc_timer_event, i) ) {
            SPC_CYCLES_TO_USECS(&ompi_spc_events[i].value);
        }
    }

    /* Aggregate all of the information on rank 0 using MPI_Gather on MPI_COMM_WORLD */
    send_buffer = (long long*)malloc(OMPI_SPC_NUM_COUNTERS * sizeof(long long));
    if (NULL == send_buffer) {
        opal_show_help("help-mpi-runtime.txt", "lib-call-fail", true,
                       "malloc", __FILE__, __LINE__);
        return;
    }
    for(i = 0; i < OMPI_SPC_NUM_COUNTERS; i++) {
        send_buffer[i] = (long long)ompi_spc_events[i].value;
    }
    if( 0 == rank ) {
        recv_buffer = (long long*)malloc(world_size * OMPI_SPC_NUM_COUNTERS * sizeof(long long));
        if (NULL == recv_buffer) {
            opal_show_help("help-mpi-runtime.txt", "lib-call-fail", true,
                           "malloc", __FILE__, __LINE__);
            return;
        }
    }
    (void)comm->c_coll->coll_gather(send_buffer, OMPI_SPC_NUM_COUNTERS, MPI_LONG_LONG,
                                    recv_buffer, OMPI_SPC_NUM_COUNTERS, MPI_LONG_LONG,
                                    0, comm,
                                    comm->c_coll->coll_gather_module);

    /* Once rank 0 has all of the information, print the aggregated counter values for each rank in order */
    if(rank == 0) {
        opal_output(0, "Open MPI Software-based Performance Counters:\n");
        offset = 0; /* Offset into the recv_buffer for each rank */
        for(j = 0; j < world_size; j++) {
            opal_output(0, "MPI_COMM_WORLD Rank %d:\n", j);
            for(i = 0; i < OMPI_SPC_NUM_COUNTERS; i++) {
                /* If this is a timer-based counter, we need to covert from cycles to usecs */
                if( 0 == recv_buffer[offset+i] ) {
                    continue;
                }
                opal_output(0, "%s -> %lld\n", ompi_spc_events[i].name, recv_buffer[offset+i]);
            }
            opal_output(0, "\n");
            offset += OMPI_SPC_NUM_COUNTERS;
        }
        printf("###########################################################################\n");
        printf("NOTE: Any counters not shown here were either disabled or had a value of 0.\n");
        printf("###########################################################################\n");

        free(recv_buffer);
    }
    free(send_buffer);

    comm->c_coll->coll_barrier(comm, comm->c_coll->coll_barrier_module);
}

/* Frees any dynamically alocated OMPI SPC data structures */
void ompi_spc_fini(void)
{
    if (SPC_ENABLE == 1 && ompi_mpi_spc_dump_enabled) {
        ompi_spc_dump();
    }

    free(ompi_spc_events); ompi_spc_events = NULL;
    ompi_comm_free(&comm);
}

/* Records an update to a counter using an atomic add operation. */
void ompi_spc_record(unsigned int event_id, ompi_spc_value_t value)
{
    /* Denoted unlikely because counters will often be turned off. */
    if( OPAL_UNLIKELY(IS_SPC_BIT_SET(ompi_spc_attached_event, event_id)) ) {
        OPAL_THREAD_ADD_FETCH_SIZE_T(&(ompi_spc_events[event_id].value), value);
    }
}

/* Starts cycle-precision timer and stores the start value in the 'cycles' argument.
 * Note: This assumes that the 'cycles' argument is initialized to 0 if the timer
 *       hasn't been started yet.
 */
void ompi_spc_timer_start(unsigned int event_id, opal_timer_t *cycles)
{
    /* Check whether cycles == 0.0 to make sure the timer hasn't started yet.
     * This is denoted unlikely because the counters will often be turned off.
     */
    if( OPAL_UNLIKELY(IS_SPC_BIT_SET(ompi_spc_attached_event, event_id) && *cycles == 0) ) {
        *cycles = opal_timer_base_get_cycles();
    }
}

/* Stops a cycle-precision timer and calculates the total elapsed time
 * based on the starting time in 'cycles' and stores the result in the
 * 'cycles' argument.
 */
void ompi_spc_timer_stop(unsigned int event_id, opal_timer_t *cycles)
{
    /* This is denoted unlikely because the counters will often be turned off. */
    if( OPAL_UNLIKELY(IS_SPC_BIT_SET(ompi_spc_attached_event, event_id)) ) {
        *cycles = opal_timer_base_get_cycles() - *cycles;
        OPAL_THREAD_ADD_FETCH_SIZE_T(&ompi_spc_events[event_id].value, (ompi_spc_value_t)*cycles);
    }
}

/* Checks a tag, and records the user version of the counter if it's greater
 * than or equal to 0 and records the mpi version of the counter otherwise.
 */
void ompi_spc_user_or_mpi(int tag, ompi_spc_value_t value, unsigned int user_enum, unsigned int mpi_enum)
{
    SPC_RECORD( (tag >= 0 ? user_enum : mpi_enum), value);
}

/* Checks whether the counter denoted by value_enum exceeds the current value of the
 * counter denoted by watermark_enum, and if so sets the watermark_enum counter to the
 * value of the value_enum counter.
 */
void ompi_spc_update_watermark(unsigned int watermark_enum, unsigned int value_enum)
{
    /* Denoted unlikely because counters will often be turned off. */
    if( OPAL_UNLIKELY(IS_SPC_BIT_SET(ompi_spc_attached_event, watermark_enum) &&
                      IS_SPC_BIT_SET(ompi_spc_attached_event, value_enum)) ) {
        /* WARNING: This assumes that this function was called while a lock has already been taken.
         *          This function is NOT thread safe otherwise!
         */
        if(ompi_spc_events[value_enum].value > ompi_spc_events[watermark_enum].value) {
            ompi_spc_events[watermark_enum].value = ompi_spc_events[value_enum].value;
        }
    }
}

/* Converts a counter value that is in cycles to microseconds.
 */
void ompi_spc_cycles_to_usecs(ompi_spc_value_t *cycles)
{
    *cycles = *cycles / sys_clock_freq_mhz;
}
