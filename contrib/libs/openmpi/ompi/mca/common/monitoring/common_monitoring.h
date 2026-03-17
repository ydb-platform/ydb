/*
 * Copyright (c) 2016-2018 Inria.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef MCA_COMMON_MONITORING_H
#define MCA_COMMON_MONITORING_H

BEGIN_C_DECLS

#include <ompi_config.h>
#include <ompi/proc/proc.h>
#include <ompi/group/group.h>
#include <ompi/communicator/communicator.h>
#include <opal/class/opal_hash_table.h>
#include <opal/mca/base/mca_base_pvar.h>

#define MCA_MONITORING_MAKE_VERSION                                     \
    MCA_BASE_MAKE_VERSION(component, OMPI_MAJOR_VERSION, OMPI_MINOR_VERSION, OMPI_RELEASE_VERSION)

#define OPAL_MONITORING_VERBOSE(x, ...)                                 \
    OPAL_OUTPUT_VERBOSE((x, mca_common_monitoring_output_stream_id, __VA_ARGS__))

/* When built in debug mode, always display error messages */
#if OPAL_ENABLE_DEBUG
#define OPAL_MONITORING_PRINT_ERR(...)          \
    OPAL_MONITORING_VERBOSE(0, __VA_ARGS__)
#else /* if( ! OPAL_ENABLE_DEBUG ) */
#define OPAL_MONITORING_PRINT_ERR(...)          \
    OPAL_MONITORING_VERBOSE(1, __VA_ARGS__)
#endif /* OPAL_ENABLE_DEBUG */

#define OPAL_MONITORING_PRINT_WARN(...)         \
    OPAL_MONITORING_VERBOSE(5, __VA_ARGS__)

#define OPAL_MONITORING_PRINT_INFO(...)         \
    OPAL_MONITORING_VERBOSE(10, __VA_ARGS__)

extern int mca_common_monitoring_output_stream_id;
extern int mca_common_monitoring_enabled;
extern int mca_common_monitoring_current_state;
extern opal_hash_table_t *common_monitoring_translation_ht;

OMPI_DECLSPEC void mca_common_monitoring_register(void*pml_monitoring_component);
OMPI_DECLSPEC int mca_common_monitoring_init( void );
OMPI_DECLSPEC void mca_common_monitoring_finalize( void );
OMPI_DECLSPEC int mca_common_monitoring_add_procs(struct ompi_proc_t **procs, size_t nprocs);

/* Records PML communication */
OMPI_DECLSPEC void mca_common_monitoring_record_pml(int world_rank, size_t data_size, int tag);

/* SEND corresponds to data emitted from the current proc to the given
 * one. RECV represents data emitted from the given proc to the
 * current one.
 */
enum mca_monitoring_osc_direction { SEND, RECV };

/* Records OSC communications. */
OMPI_DECLSPEC void mca_common_monitoring_record_osc(int world_rank, size_t data_size,
                                                    enum mca_monitoring_osc_direction dir);

/* Records COLL communications. */
OMPI_DECLSPEC void mca_common_monitoring_record_coll(int world_rank, size_t data_size);

/* Translate the rank from the given rank of a process to its rank in MPI_COMM_RANK. */
static inline int mca_common_monitoring_get_world_rank(int dest, ompi_group_t *group,
                                                           int *world_rank)
{
    opal_process_name_t tmp;

    /* find the processor of the destination */
    ompi_proc_t *proc = ompi_group_get_proc_ptr(group, dest, true);
    if( ompi_proc_is_sentinel(proc) ) {
        tmp = ompi_proc_sentinel_to_name((uintptr_t)proc);
    } else {
        tmp = proc->super.proc_name;
    }

    /* find its name*/
    uint64_t rank, key = *((uint64_t*)&tmp);
    /**
     * If this fails the destination is not part of my MPI_COM_WORLD
     * Lookup its name in the rank hastable to get its MPI_COMM_WORLD rank
     */
    int ret = opal_hash_table_get_value_uint64(common_monitoring_translation_ht,
                                               key, (void *)&rank);

    /* Use intermediate variable to avoid overwriting while looking up in the hashtbale. */
    if( ret == OPAL_SUCCESS ) *world_rank = (int)rank;
    return ret;
}

/* Return the current status of the monitoring system 0 if off or the
 * seperation between internal tags and external tags is disabled. Any
 * other positive value if the segregation between point-to-point and
 * collective is enabled.
 */
static inline int mca_common_monitoring_filter( void )
{
    return 1 < mca_common_monitoring_current_state;
}

/* Collective operation monitoring */
struct mca_monitoring_coll_data_t;
typedef struct mca_monitoring_coll_data_t mca_monitoring_coll_data_t;
OMPI_DECLSPEC OBJ_CLASS_DECLARATION(mca_monitoring_coll_data_t);

OMPI_DECLSPEC mca_monitoring_coll_data_t*mca_common_monitoring_coll_new(ompi_communicator_t*comm);
OMPI_DECLSPEC int  mca_common_monitoring_coll_cache_name(ompi_communicator_t*comm);
OMPI_DECLSPEC void mca_common_monitoring_coll_release(mca_monitoring_coll_data_t*data);
OMPI_DECLSPEC void mca_common_monitoring_coll_o2a(size_t size, mca_monitoring_coll_data_t*data);
OMPI_DECLSPEC void mca_common_monitoring_coll_a2o(size_t size, mca_monitoring_coll_data_t*data);
OMPI_DECLSPEC void mca_common_monitoring_coll_a2a(size_t size, mca_monitoring_coll_data_t*data);

END_C_DECLS

#endif  /* MCA_COMMON_MONITORING_H */
