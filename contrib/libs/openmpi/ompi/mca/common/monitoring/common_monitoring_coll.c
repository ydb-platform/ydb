/*
 * Copyright (c) 2013-2016 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2013-2018 Inria.  All rights reserved.
 * Copyright (c) 2015      Bull SAS.  All rights reserved.
 * Copyright (c) 2016-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include <ompi_config.h>
#include "common_monitoring.h"
#include "common_monitoring_coll.h"
#include <ompi/constants.h>
#include <ompi/communicator/communicator.h>
#include <opal/mca/base/mca_base_component_repository.h>
#include <opal/class/opal_hash_table.h>
#include <assert.h>

/*** Monitoring specific variables ***/
struct mca_monitoring_coll_data_t {
    opal_object_t super;
    char*procs;
    char*comm_name;
    int world_rank;
    int is_released;
    ompi_communicator_t*p_comm;
    size_t o2a_count;
    size_t o2a_size;
    size_t a2o_count;
    size_t a2o_size;
    size_t a2a_count;
    size_t a2a_size;
};

/* Collectives operation monitoring */
static opal_hash_table_t *comm_data = NULL;

int mca_common_monitoring_coll_cache_name(ompi_communicator_t*comm)
{
    mca_monitoring_coll_data_t*data;
    int ret = opal_hash_table_get_value_uint64(comm_data, *((uint64_t*)&comm), (void*)&data);
    if( OPAL_SUCCESS == ret ) {
        data->comm_name = strdup(comm->c_name);
        data->p_comm = NULL;
    }
    return ret;
}

static inline void mca_common_monitoring_coll_cache(mca_monitoring_coll_data_t*data)
{
    if( -1 == data->world_rank ) {
        /* Get current process world_rank */
        mca_common_monitoring_get_world_rank(ompi_comm_rank(data->p_comm),
					     data->p_comm->c_remote_group,
                                             &data->world_rank);
    }
    /* Only list procs if the hashtable is already initialized,
       i.e. if the previous call worked */
    if( (-1 != data->world_rank) && (NULL == data->procs || 0 == strlen(data->procs)) ) {
        int i, pos = 0, size, world_size = -1, max_length, world_rank;
        char*tmp_procs;
        size = ompi_comm_size(data->p_comm);
        world_size = ompi_comm_size((ompi_communicator_t*)&ompi_mpi_comm_world) - 1;
        assert( 0 < size );
        /* Allocate enough space for list (add 1 to keep the final '\0' if already exact size) */
        max_length = snprintf(NULL, 0, "%d,", world_size - 1) + 1;
        tmp_procs = malloc((1 + max_length * size) * sizeof(char));
        if( NULL == tmp_procs ) {
            OPAL_MONITORING_PRINT_ERR("Cannot allocate memory for caching proc list.");
        } else {
            tmp_procs[0] = '\0';
            /* Build procs list */
            for(i = 0; i < size; ++i) {
                if( OPAL_SUCCESS == mca_common_monitoring_get_world_rank(i, data->p_comm->c_remote_group, &world_rank) )
                    pos += sprintf(&tmp_procs[pos], "%d,", world_rank);
            }
            tmp_procs[pos - 1] = '\0'; /* Remove final coma */
            data->procs = realloc(tmp_procs, pos * sizeof(char)); /* Adjust to size required */
        }
    }
}

mca_monitoring_coll_data_t*mca_common_monitoring_coll_new( ompi_communicator_t*comm )
{
    mca_monitoring_coll_data_t*data = OBJ_NEW(mca_monitoring_coll_data_t);
    if( NULL == data ) {
        OPAL_MONITORING_PRINT_ERR("coll: new: data structure cannot be allocated");
        return NULL;
    }

    data->p_comm      = comm;

    /* Allocate hashtable */
    if( NULL == comm_data ) {
        comm_data = OBJ_NEW(opal_hash_table_t);
        if( NULL == comm_data ) {
            OPAL_MONITORING_PRINT_ERR("coll: new: failed to allocate hashtable");
            return data;
        }
        opal_hash_table_init(comm_data, 2048);
    }

    /* Insert in hashtable */
    uint64_t key = *((uint64_t*)&comm);
    if( OPAL_SUCCESS != opal_hash_table_set_value_uint64(comm_data, key, (void*)data) ) {
        OPAL_MONITORING_PRINT_ERR("coll: new: failed to allocate memory or "
                                  "growing the hash table");
    }

    /* Cache data so the procs can be released without affecting the output */
    mca_common_monitoring_coll_cache(data);

    return data;
}

void mca_common_monitoring_coll_release(mca_monitoring_coll_data_t*data)
{
#if OPAL_ENABLE_DEBUG
    if( NULL == data ) {
        OPAL_MONITORING_PRINT_ERR("coll: release: data structure empty or already desallocated");
        return;
    }
#endif /* OPAL_ENABLE_DEBUG */

    /* not flushed yet */
    data->is_released = 1;
    mca_common_monitoring_coll_cache(data);
}

static void mca_common_monitoring_coll_cond_release(mca_monitoring_coll_data_t*data)
{
#if OPAL_ENABLE_DEBUG
    if( NULL == data ) {
        OPAL_MONITORING_PRINT_ERR("coll: release: data structure empty or already desallocated");
        return;
    }
#endif /* OPAL_ENABLE_DEBUG */

    if( data->is_released ) { /* if the communicator is already released */
        opal_hash_table_remove_value_uint64(comm_data, *((uint64_t*)&data->p_comm));
        data->p_comm = NULL;
        free(data->comm_name);
        free(data->procs);
        OBJ_RELEASE(data);
    }
}

void mca_common_monitoring_coll_finalize( void )
{
    if( NULL != comm_data ) {
        opal_hash_table_remove_all( comm_data );
        OBJ_RELEASE(comm_data);
    }
}

void mca_common_monitoring_coll_flush(FILE *pf, mca_monitoring_coll_data_t*data)
{
    /* Flush data */
    fprintf(pf,
            "D\t%s\tprocs: %s\n"
            "O2A\t%" PRId32 "\t%zu bytes\t%zu msgs sent\n"
            "A2O\t%" PRId32 "\t%zu bytes\t%zu msgs sent\n"
            "A2A\t%" PRId32 "\t%zu bytes\t%zu msgs sent\n",
            data->comm_name ? data->comm_name : data->p_comm ?
            data->p_comm->c_name : "(no-name)", data->procs,
            data->world_rank, data->o2a_size, data->o2a_count,
            data->world_rank, data->a2o_size, data->a2o_count,
            data->world_rank, data->a2a_size, data->a2a_count);
}

void mca_common_monitoring_coll_flush_all(FILE *pf)
{
    if( NULL == comm_data ) return; /* No hashtable */

    uint64_t key;
    mca_monitoring_coll_data_t*previous = NULL, *data;

    OPAL_HASH_TABLE_FOREACH(key, uint64, data, comm_data) {
        if( NULL != previous && NULL == previous->p_comm ) {
            /* Phase flushed -> free already released once coll_data_t */
            mca_common_monitoring_coll_cond_release(previous);
        }
        mca_common_monitoring_coll_flush(pf, data);
        previous = data;
    }
    mca_common_monitoring_coll_cond_release(previous);
}


void mca_common_monitoring_coll_reset(void)
{
    if( NULL == comm_data ) return; /* No hashtable */

    uint64_t key;
    mca_monitoring_coll_data_t*data;

    OPAL_HASH_TABLE_FOREACH(key, uint64, data, comm_data) {
        data->o2a_count = 0; data->o2a_size  = 0;
        data->a2o_count = 0; data->a2o_size  = 0;
        data->a2a_count = 0; data->a2a_size  = 0;
    }
}

int mca_common_monitoring_coll_messages_notify(mca_base_pvar_t *pvar,
                                               mca_base_pvar_event_t event,
                                               void *obj_handle,
                                               int *count)
{
    switch (event) {
    case MCA_BASE_PVAR_HANDLE_BIND:
        *count = 1;
    case MCA_BASE_PVAR_HANDLE_UNBIND:
        return OMPI_SUCCESS;
    case MCA_BASE_PVAR_HANDLE_START:
        mca_common_monitoring_current_state = mca_common_monitoring_enabled;
        return OMPI_SUCCESS;
    case MCA_BASE_PVAR_HANDLE_STOP:
        mca_common_monitoring_current_state = 0;
        return OMPI_SUCCESS;
    }

    return OMPI_ERROR;
}

void mca_common_monitoring_coll_o2a(size_t size, mca_monitoring_coll_data_t*data)
{
    if( 0 == mca_common_monitoring_current_state ) return; /* right now the monitoring is not started */
#if OPAL_ENABLE_DEBUG
    if( NULL == data ) {
        OPAL_MONITORING_PRINT_ERR("coll: o2a: data structure empty");
        return;
    }
#endif /* OPAL_ENABLE_DEBUG */
    opal_atomic_add_fetch_size_t(&data->o2a_size, size);
    opal_atomic_add_fetch_size_t(&data->o2a_count, 1);
}

int mca_common_monitoring_coll_get_o2a_count(const struct mca_base_pvar_t *pvar,
                                             void *value,
                                             void *obj_handle)
{
    ompi_communicator_t *comm = (ompi_communicator_t *) obj_handle;
    size_t *value_size = (size_t*) value;
    mca_monitoring_coll_data_t*data;
    int ret = opal_hash_table_get_value_uint64(comm_data, *((uint64_t*)&comm), (void*)&data);
    if( OPAL_SUCCESS == ret ) {
        *value_size = data->o2a_count;
    }
    return ret;
}

int mca_common_monitoring_coll_get_o2a_size(const struct mca_base_pvar_t *pvar,
                                            void *value,
                                            void *obj_handle)
{
    ompi_communicator_t *comm = (ompi_communicator_t *) obj_handle;
    size_t *value_size = (size_t*) value;
    mca_monitoring_coll_data_t*data;
    int ret = opal_hash_table_get_value_uint64(comm_data, *((uint64_t*)&comm), (void*)&data);
    if( OPAL_SUCCESS == ret ) {
        *value_size = data->o2a_size;
    }
    return ret;
}

void mca_common_monitoring_coll_a2o(size_t size, mca_monitoring_coll_data_t*data)
{
    if( 0 == mca_common_monitoring_current_state ) return; /* right now the monitoring is not started */
#if OPAL_ENABLE_DEBUG
    if( NULL == data ) {
        OPAL_MONITORING_PRINT_ERR("coll: a2o: data structure empty");
        return;
    }
#endif /* OPAL_ENABLE_DEBUG */
    opal_atomic_add_fetch_size_t(&data->a2o_size, size);
    opal_atomic_add_fetch_size_t(&data->a2o_count, 1);
}

int mca_common_monitoring_coll_get_a2o_count(const struct mca_base_pvar_t *pvar,
                                             void *value,
                                             void *obj_handle)
{
    ompi_communicator_t *comm = (ompi_communicator_t *) obj_handle;
    size_t *value_size = (size_t*) value;
    mca_monitoring_coll_data_t*data;
    int ret = opal_hash_table_get_value_uint64(comm_data, *((uint64_t*)&comm), (void*)&data);
    if( OPAL_SUCCESS == ret ) {
        *value_size = data->a2o_count;
    }
    return ret;
}

int mca_common_monitoring_coll_get_a2o_size(const struct mca_base_pvar_t *pvar,
                                            void *value,
                                            void *obj_handle)
{
    ompi_communicator_t *comm = (ompi_communicator_t *) obj_handle;
    size_t *value_size = (size_t*) value;
    mca_monitoring_coll_data_t*data;
    int ret = opal_hash_table_get_value_uint64(comm_data, *((uint64_t*)&comm), (void*)&data);
    if( OPAL_SUCCESS == ret ) {
        *value_size = data->a2o_size;
    }
    return ret;
}

void mca_common_monitoring_coll_a2a(size_t size, mca_monitoring_coll_data_t*data)
{
    if( 0 == mca_common_monitoring_current_state ) return; /* right now the monitoring is not started */
#if OPAL_ENABLE_DEBUG
    if( NULL == data ) {
        OPAL_MONITORING_PRINT_ERR("coll: a2a: data structure empty");
        return;
    }
#endif /* OPAL_ENABLE_DEBUG */
    opal_atomic_add_fetch_size_t(&data->a2a_size, size);
    opal_atomic_add_fetch_size_t(&data->a2a_count, 1);
}

int mca_common_monitoring_coll_get_a2a_count(const struct mca_base_pvar_t *pvar,
                                             void *value,
                                             void *obj_handle)
{
    ompi_communicator_t *comm = (ompi_communicator_t *) obj_handle;
    size_t *value_size = (size_t*) value;
    mca_monitoring_coll_data_t*data;
    int ret = opal_hash_table_get_value_uint64(comm_data, *((uint64_t*)&comm), (void*)&data);
    if( OPAL_SUCCESS == ret ) {
        *value_size = data->a2a_count;
    }
    return ret;
}

int mca_common_monitoring_coll_get_a2a_size(const struct mca_base_pvar_t *pvar,
                                            void *value,
                                            void *obj_handle)
{
    ompi_communicator_t *comm = (ompi_communicator_t *) obj_handle;
    size_t *value_size = (size_t*) value;
    mca_monitoring_coll_data_t*data;
    int ret = opal_hash_table_get_value_uint64(comm_data, *((uint64_t*)&comm), (void*)&data);
    if( OPAL_SUCCESS == ret ) {
        *value_size = data->a2a_size;
    }
    return ret;
}

static void mca_monitoring_coll_construct (mca_monitoring_coll_data_t*coll_data)
{
    coll_data->procs       = NULL;
    coll_data->comm_name   = NULL;
    coll_data->world_rank  = -1;
    coll_data->p_comm      = NULL;
    coll_data->is_released = 0;
    coll_data->o2a_count   = 0;
    coll_data->o2a_size    = 0;
    coll_data->a2o_count   = 0;
    coll_data->a2o_size    = 0;
    coll_data->a2a_count   = 0;
    coll_data->a2a_size    = 0;
}

static void mca_monitoring_coll_destruct (mca_monitoring_coll_data_t*coll_data){}

OBJ_CLASS_INSTANCE(mca_monitoring_coll_data_t, opal_object_t, mca_monitoring_coll_construct, mca_monitoring_coll_destruct);
