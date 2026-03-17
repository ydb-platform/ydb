/*
 * Copyright (c) 2018      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST).  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_SPC
#define OMPI_SPC

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dlfcn.h>

#include "ompi/communicator/communicator.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/runtime/params.h"
#include "opal/mca/timer/timer.h"
#include "opal/mca/base/mca_base_pvar.h"
#include "opal/util/argv.h"
#include "opal/util/show_help.h"
#include "opal/util/output.h"

#include MCA_timer_IMPLEMENTATION_HEADER

/* INSTRUCTIONS FOR ADDING COUNTERS
 * 1.) Add a new counter name in the ompi_spc_counters_t enum before
 *     OMPI_SPC_NUM_COUNTERS below.
 * 2.) Add corresponding counter name and descriptions to the
 *     counter_names and counter_descriptions arrays in
 *     ompi_spc.c  NOTE: The names and descriptions
 *     MUST be in the same array location as where you added the
 *     counter name in step 1.
 * 3.) If this counter is based on a timer, add its enum name to
 *     the logic for timer-based counters in the ompi_spc_init
 *     function in ompi_spc.c
 * 4.) Instrument the Open MPI code base where it makes sense for
 *     your counter to be modified using the SPC_RECORD macro.
 *     Note: If your counter is timer-based you should use the
 *     SPC_TIMER_START and SPC_TIMER_STOP macros to record
 *     the time in cycles to then be converted to microseconds later
 *     in the ompi_spc_get_count function when requested by MPI_T
 */

/* This enumeration serves as event ids for the various events */
typedef enum ompi_spc_counters {
    OMPI_SPC_SEND,
    OMPI_SPC_BSEND,
    OMPI_SPC_RSEND,
    OMPI_SPC_SSEND,
    OMPI_SPC_RECV,
    OMPI_SPC_MRECV,
    OMPI_SPC_ISEND,
    OMPI_SPC_IBSEND,
    OMPI_SPC_IRSEND,
    OMPI_SPC_ISSEND,
    OMPI_SPC_IRECV,
    OMPI_SPC_SENDRECV,
    OMPI_SPC_SENDRECV_REPLACE,
    OMPI_SPC_PUT,
    OMPI_SPC_RPUT,
    OMPI_SPC_GET,
    OMPI_SPC_RGET,
    OMPI_SPC_PROBE,
    OMPI_SPC_IPROBE,
    OMPI_SPC_BCAST,
    OMPI_SPC_IBCAST,
    OMPI_SPC_BCAST_INIT,
    OMPI_SPC_REDUCE,
    OMPI_SPC_REDUCE_SCATTER,
    OMPI_SPC_REDUCE_SCATTER_BLOCK,
    OMPI_SPC_IREDUCE,
    OMPI_SPC_IREDUCE_SCATTER,
    OMPI_SPC_IREDUCE_SCATTER_BLOCK,
    OMPI_SPC_REDUCE_INIT,
    OMPI_SPC_REDUCE_SCATTER_INIT,
    OMPI_SPC_REDUCE_SCATTER_BLOCK_INIT,
    OMPI_SPC_ALLREDUCE,
    OMPI_SPC_IALLREDUCE,
    OMPI_SPC_ALLREDUCE_INIT,
    OMPI_SPC_SCAN,
    OMPI_SPC_EXSCAN,
    OMPI_SPC_ISCAN,
    OMPI_SPC_IEXSCAN,
    OMPI_SPC_SCAN_INIT,
    OMPI_SPC_EXSCAN_INIT,
    OMPI_SPC_SCATTER,
    OMPI_SPC_SCATTERV,
    OMPI_SPC_ISCATTER,
    OMPI_SPC_ISCATTERV,
    OMPI_SPC_SCATTER_INIT,
    OMPI_SPC_SCATTERV_INIT,
    OMPI_SPC_GATHER,
    OMPI_SPC_GATHERV,
    OMPI_SPC_IGATHER,
    OMPI_SPC_IGATHERV,
    OMPI_SPC_GATHER_INIT,
    OMPI_SPC_GATHERV_INIT,
    OMPI_SPC_ALLTOALL,
    OMPI_SPC_ALLTOALLV,
    OMPI_SPC_ALLTOALLW,
    OMPI_SPC_IALLTOALL,
    OMPI_SPC_IALLTOALLV,
    OMPI_SPC_IALLTOALLW,
    OMPI_SPC_ALLTOALL_INIT,
    OMPI_SPC_ALLTOALLV_INIT,
    OMPI_SPC_ALLTOALLW_INIT,
    OMPI_SPC_NEIGHBOR_ALLTOALL,
    OMPI_SPC_NEIGHBOR_ALLTOALLV,
    OMPI_SPC_NEIGHBOR_ALLTOALLW,
    OMPI_SPC_INEIGHBOR_ALLTOALL,
    OMPI_SPC_INEIGHBOR_ALLTOALLV,
    OMPI_SPC_INEIGHBOR_ALLTOALLW,
    OMPI_SPC_NEIGHBOR_ALLTOALL_INIT,
    OMPI_SPC_NEIGHBOR_ALLTOALLV_INIT,
    OMPI_SPC_NEIGHBOR_ALLTOALLW_INIT,
    OMPI_SPC_ALLGATHER,
    OMPI_SPC_ALLGATHERV,
    OMPI_SPC_IALLGATHER,
    OMPI_SPC_IALLGATHERV,
    OMPI_SPC_ALLGATHER_INIT,
    OMPI_SPC_ALLGATHERV_INIT,
    OMPI_SPC_NEIGHBOR_ALLGATHER,
    OMPI_SPC_NEIGHBOR_ALLGATHERV,
    OMPI_SPC_INEIGHBOR_ALLGATHER,
    OMPI_SPC_INEIGHBOR_ALLGATHERV,
    OMPI_SPC_NEIGHBOR_ALLGATHER_INIT,
    OMPI_SPC_NEIGHBOR_ALLGATHERV_INIT,
    OMPI_SPC_TEST,
    OMPI_SPC_TESTALL,
    OMPI_SPC_TESTANY,
    OMPI_SPC_TESTSOME,
    OMPI_SPC_WAIT,
    OMPI_SPC_WAITALL,
    OMPI_SPC_WAITANY,
    OMPI_SPC_WAITSOME,
    OMPI_SPC_BARRIER,
    OMPI_SPC_IBARRIER,
    OMPI_SPC_BARRIER_INIT,
    OMPI_SPC_WTIME,
    OMPI_SPC_CANCEL,
    OMPI_SPC_BYTES_RECEIVED_USER,
    OMPI_SPC_BYTES_RECEIVED_MPI,
    OMPI_SPC_BYTES_SENT_USER,
    OMPI_SPC_BYTES_SENT_MPI,
    OMPI_SPC_BYTES_PUT,
    OMPI_SPC_BYTES_GET,
    OMPI_SPC_UNEXPECTED,
    OMPI_SPC_OUT_OF_SEQUENCE,
    OMPI_SPC_MATCH_TIME,
    OMPI_SPC_UNEXPECTED_IN_QUEUE,
    OMPI_SPC_OOS_IN_QUEUE,
    OMPI_SPC_MAX_UNEXPECTED_IN_QUEUE,
    OMPI_SPC_MAX_OOS_IN_QUEUE,
    OMPI_SPC_NUM_COUNTERS /* This serves as the number of counters.  It must be last. */
} ompi_spc_counters_t;

/* There is currently no support for atomics on long long values so we will default to
 * size_t for now until support for such atomics is implemented.
 */
typedef size_t ompi_spc_value_t;

/* A structure for storing the event data */
typedef struct ompi_spc_s{
    char *name;
    ompi_spc_value_t value;
} ompi_spc_t;

/* Events data structure initialization function */
void ompi_spc_events_init(void);

/* OMPI SPC utility functions */
void ompi_spc_init(void);
void ompi_spc_fini(void);
void ompi_spc_record(unsigned int event_id, ompi_spc_value_t value);
void ompi_spc_timer_start(unsigned int event_id, opal_timer_t *cycles);
void ompi_spc_timer_stop(unsigned int event_id, opal_timer_t *cycles);
void ompi_spc_user_or_mpi(int tag, ompi_spc_value_t value, unsigned int user_enum, unsigned int mpi_enum);
void ompi_spc_cycles_to_usecs(ompi_spc_value_t *cycles);
void ompi_spc_update_watermark(unsigned int watermark_enum, unsigned int value_enum);

/* Macros for using the SPC utility functions throughout the codebase.
 * If SPC_ENABLE is not 1, the macros become no-ops.
 */
#if SPC_ENABLE == 1

#define SPC_INIT()  \
    ompi_spc_init()

#define SPC_FINI()  \
    ompi_spc_fini()

#define SPC_RECORD(event_id, value)  \
    ompi_spc_record(event_id, value)

#define SPC_TIMER_START(event_id, usec)  \
    ompi_spc_timer_start(event_id, usec)

#define SPC_TIMER_STOP(event_id, usec)  \
    ompi_spc_timer_stop(event_id, usec)

#define SPC_USER_OR_MPI(tag, value, enum_if_user, enum_if_mpi) \
    ompi_spc_user_or_mpi(tag, value, enum_if_user, enum_if_mpi)

#define SPC_CYCLES_TO_USECS(cycles) \
    ompi_spc_cycles_to_usecs(cycles)

#define SPC_UPDATE_WATERMARK(watermark_enum, value_enum) \
    ompi_spc_update_watermark(watermark_enum, value_enum)

#else /* SPCs are not enabled */

#define SPC_INIT()  \
    ((void)0)

#define SPC_FINI()  \
    ((void)0)

#define SPC_RECORD(event_id, value)  \
    ((void)0)

#define SPC_TIMER_START(event_id, usec)  \
    ((void)0)

#define SPC_TIMER_STOP(event_id, usec)  \
    ((void)0)

#define SPC_USER_OR_MPI(tag, value, enum_if_user, enum_if_mpi) \
    ((void)0)

#define SPC_CYCLES_TO_USECS(cycles) \
    ((void)0)

#define SPC_UPDATE_WATERMARK(watermark_enum, value_enum) \
    ((void)0)

#endif

#endif
