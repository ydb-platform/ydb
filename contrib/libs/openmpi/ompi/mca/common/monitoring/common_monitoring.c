/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2013-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2013-2017 Inria.  All rights reserved.
 * Copyright (c) 2015      Bull SAS.  All rights reserved.
 * Copyright (c) 2016-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      Los Alamos National Security, LLC. All rights
 *                         reserved.
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
#include <opal/util/output.h>
#include <math.h>

#if SIZEOF_LONG_LONG == SIZEOF_SIZE_T
#define MCA_MONITORING_VAR_TYPE MCA_BASE_VAR_TYPE_UNSIGNED_LONG_LONG
#elif SIZEOF_LONG == SIZEOF_SIZE_T
#define MCA_MONITORING_VAR_TYPE MCA_BASE_VAR_TYPE_UNSIGNED_LONG
#endif

/*** Monitoring specific variables ***/
/* Keep tracks of how many components are currently using the common part */
static int32_t mca_common_monitoring_hold = 0;
/* Output parameters */
int mca_common_monitoring_output_stream_id = -1;
static opal_output_stream_t mca_common_monitoring_output_stream_obj = {
    .lds_verbose_level = 0,
    .lds_want_syslog = false,
    .lds_prefix = NULL,
    .lds_suffix = NULL,
    .lds_is_debugging = true,
    .lds_want_stdout = false,
    .lds_want_stderr = true,
    .lds_want_file = false,
    .lds_want_file_append = false,
    .lds_file_suffix = NULL
};

/*** MCA params to mark the monitoring as enabled. ***/
/* This signals that the monitoring will highjack the PML, OSC and COLL */
int mca_common_monitoring_enabled = 0;
int mca_common_monitoring_current_state = 0;
/* Signals there will be an output of the monitored data at component close */
static int mca_common_monitoring_output_enabled = 0;
/* File where to output the monitored data */
static char* mca_common_monitoring_initial_filename = "";
static char* mca_common_monitoring_current_filename = NULL;

/* array for stroring monitoring data*/
static size_t* pml_data = NULL;
static size_t* pml_count = NULL;
static size_t* filtered_pml_data = NULL;
static size_t* filtered_pml_count = NULL;
static size_t* osc_data_s = NULL;
static size_t* osc_count_s = NULL;
static size_t* osc_data_r = NULL;
static size_t* osc_count_r = NULL;
static size_t* coll_data = NULL;
static size_t* coll_count = NULL;

static size_t* size_histogram = NULL;
static const int max_size_histogram = 66;
static double log10_2 = 0.;

static int rank_world = -1;
static int nprocs_world = 0;

opal_hash_table_t *common_monitoring_translation_ht = NULL;

/* Reset all the monitoring arrays */
static void mca_common_monitoring_reset ( void );

/* Flushes the monitored data and reset the values */
static int mca_common_monitoring_flush (int fd, char* filename);

/* Retreive the PML recorded count of messages sent */
static int mca_common_monitoring_get_pml_count (const struct mca_base_pvar_t *pvar,
                                                void *value, void *obj_handle);

/* Retreive the PML recorded amount of data sent */
static int mca_common_monitoring_get_pml_size (const struct mca_base_pvar_t *pvar,
                                               void *value, void *obj_handle);

/* Retreive the OSC recorded count of messages sent */
static int mca_common_monitoring_get_osc_sent_count (const struct mca_base_pvar_t *pvar,
                                                     void *value, void *obj_handle);

/* Retreive the OSC recorded amount of data sent */
static int mca_common_monitoring_get_osc_sent_size (const struct mca_base_pvar_t *pvar,
                                                    void *value, void *obj_handle);

/* Retreive the OSC recorded count of messages received */
static int mca_common_monitoring_get_osc_recv_count (const struct mca_base_pvar_t *pvar,
                                                     void *value, void *obj_handle);

/* Retreive the OSC recorded amount of data received */
static int mca_common_monitoring_get_osc_recv_size (const struct mca_base_pvar_t *pvar,
                                                    void *value, void *obj_handle);

/* Retreive the COLL recorded count of messages sent */
static int mca_common_monitoring_get_coll_count (const struct mca_base_pvar_t *pvar,
                                                 void *value, void *obj_handle);

/* Retreive the COLL recorded amount of data sent */
static int mca_common_monitoring_get_coll_size (const struct mca_base_pvar_t *pvar,
                                                void *value, void *obj_handle);

/* Set the filename where to output the monitored data */
static int mca_common_monitoring_set_flush(struct mca_base_pvar_t *pvar,
                                           const void *value, void *obj);

/* Does nothing, as the pml_monitoring_flush pvar has no point to be read */
static int mca_common_monitoring_get_flush(const struct mca_base_pvar_t *pvar,
                                           void *value, void *obj);

/* pml_monitoring_count, pml_monitoring_size,
   osc_monitoring_sent_count, osc_monitoring sent_size,
   osc_monitoring_recv_size and osc_monitoring_recv_count pvar notify
   function */
static int mca_common_monitoring_comm_size_notify(mca_base_pvar_t *pvar,
                                                  mca_base_pvar_event_t event,
                                                  void *obj_handle, int *count);

/* pml_monitoring_flush pvar notify function */
static int mca_common_monitoring_notify_flush(struct mca_base_pvar_t *pvar,
                                              mca_base_pvar_event_t event,
                                              void *obj, int *count);

static int mca_common_monitoring_set_flush(struct mca_base_pvar_t *pvar,
                                           const void *value, void *obj)
{
    if( NULL != mca_common_monitoring_current_filename ) {
        free(mca_common_monitoring_current_filename);
    }
    if( NULL == *(char**)value || 0 == strlen((char*)value) ) {  /* No more output */
        mca_common_monitoring_current_filename = NULL;
    } else {
        mca_common_monitoring_current_filename = strdup((char*)value);
        if( NULL == mca_common_monitoring_current_filename )
            return OMPI_ERROR;
    }
    return OMPI_SUCCESS;
}

static int mca_common_monitoring_get_flush(const struct mca_base_pvar_t *pvar,
                                           void *value, void *obj)
{
    return OMPI_SUCCESS;
}

static int mca_common_monitoring_notify_flush(struct mca_base_pvar_t *pvar,
                                              mca_base_pvar_event_t event,
                                              void *obj, int *count)
{
    switch (event) {
    case MCA_BASE_PVAR_HANDLE_BIND:
        mca_common_monitoring_reset();
        *count = (NULL == mca_common_monitoring_current_filename
                  ? 0 : strlen(mca_common_monitoring_current_filename));
    case MCA_BASE_PVAR_HANDLE_UNBIND:
        return OMPI_SUCCESS;
    case MCA_BASE_PVAR_HANDLE_START:
        mca_common_monitoring_current_state = mca_common_monitoring_enabled;
        mca_common_monitoring_output_enabled = 0;  /* we can't control the monitoring via MPIT and
                                                    * expect accurate answer upon MPI_Finalize. */
        return OMPI_SUCCESS;
    case MCA_BASE_PVAR_HANDLE_STOP:
        return mca_common_monitoring_flush(3, mca_common_monitoring_current_filename);
    }
    return OMPI_ERROR;
}

static int mca_common_monitoring_comm_size_notify(mca_base_pvar_t *pvar,
                                                  mca_base_pvar_event_t event,
                                                  void *obj_handle,
                                                  int *count)
{
    switch (event) {
    case MCA_BASE_PVAR_HANDLE_BIND:
        /* Return the size of the communicator as the number of values */
        *count = ompi_comm_size ((ompi_communicator_t *) obj_handle);
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

int mca_common_monitoring_init( void )
{
    if( !mca_common_monitoring_enabled ) return OMPI_ERROR;
    if( 1 < opal_atomic_add_fetch_32(&mca_common_monitoring_hold, 1) ) return OMPI_SUCCESS; /* Already initialized */

    char hostname[OPAL_MAXHOSTNAMELEN] = "NA";
    /* Initialize constant */
    log10_2 = log10(2.);
    /* Open the opal_output stream */
    gethostname(hostname, sizeof(hostname));
    asprintf(&mca_common_monitoring_output_stream_obj.lds_prefix,
             "[%s:%06d] monitoring: ", hostname, getpid());
    mca_common_monitoring_output_stream_id =
        opal_output_open(&mca_common_monitoring_output_stream_obj);
    /* Initialize proc translation hashtable */
    common_monitoring_translation_ht = OBJ_NEW(opal_hash_table_t);
    opal_hash_table_init(common_monitoring_translation_ht, 2048);
    return OMPI_SUCCESS;
}

void mca_common_monitoring_finalize( void )
{
    if( ! mca_common_monitoring_enabled || /* Don't release if not last */
        0 < opal_atomic_sub_fetch_32(&mca_common_monitoring_hold, 1) ) return;
    
    OPAL_MONITORING_PRINT_INFO("common_component_finish");
    /* Dump monitoring informations */
    mca_common_monitoring_flush(mca_common_monitoring_output_enabled,
                                mca_common_monitoring_current_filename);
    /* Disable all monitoring */
    mca_common_monitoring_enabled = 0;
    /* Close the opal_output stream */
    opal_output_close(mca_common_monitoring_output_stream_id);
    free(mca_common_monitoring_output_stream_obj.lds_prefix);
    /* Free internal data structure */
    free(pml_data);  /* a single allocation */
    opal_hash_table_remove_all( common_monitoring_translation_ht );
    OBJ_RELEASE(common_monitoring_translation_ht);
    mca_common_monitoring_coll_finalize();
    if( NULL != mca_common_monitoring_current_filename ) {
        free(mca_common_monitoring_current_filename);
        mca_common_monitoring_current_filename = NULL;
    }
}

void mca_common_monitoring_register(void*pml_monitoring_component)
{
    /* Because we are playing tricks with the component close, we should not
     * use mca_base_component_var_register but instead stay with the basic
     * version mca_base_var_register.
     */
    (void)mca_base_var_register("ompi", "pml", "monitoring", "enable",
                                "Enable the monitoring at the PML level. A value of 0 "
                                "will disable the monitoring (default). A value of 1 will "
                                "aggregate all monitoring information (point-to-point and "
                                "collective). Any other value will enable filtered monitoring",
                                MCA_BASE_VAR_TYPE_INT, NULL, MPI_T_BIND_NO_OBJECT,
                                MCA_BASE_VAR_FLAG_DWG, OPAL_INFO_LVL_4,
                                MCA_BASE_VAR_SCOPE_READONLY,
                                &mca_common_monitoring_enabled);

    mca_common_monitoring_current_state = mca_common_monitoring_enabled;
    
    (void)mca_base_var_register("ompi", "pml", "monitoring", "enable_output",
                                "Enable the PML monitoring textual output at MPI_Finalize "
                                "(it will be automatically turned off when MPIT is used to "
                                "monitor communications). This value should be different "
                                "than 0 in order for the output to be enabled (default disable)",
                                MCA_BASE_VAR_TYPE_INT, NULL, MPI_T_BIND_NO_OBJECT,
                                MCA_BASE_VAR_FLAG_DWG, OPAL_INFO_LVL_9,
                                MCA_BASE_VAR_SCOPE_READONLY,
                                &mca_common_monitoring_output_enabled);
    
    (void)mca_base_var_register("ompi", "pml", "monitoring", "filename",
                                /*&mca_common_monitoring_component.pmlm_version, "filename",*/
                                "The name of the file where the monitoring information "
                                "should be saved (the filename will be extended with the "
                                "process rank and the \".prof\" extension). If this field "
                                "is NULL the monitoring will not be saved.",
                                MCA_BASE_VAR_TYPE_STRING, NULL, MPI_T_BIND_NO_OBJECT,
                                MCA_BASE_VAR_FLAG_DWG, OPAL_INFO_LVL_9,
                                MCA_BASE_VAR_SCOPE_READONLY,
                                &mca_common_monitoring_initial_filename);

    /* Now that the MCA variables are automatically unregistered when
     * their component close, we need to keep a safe copy of the
     * filename.  
     * Keep the copy completely separated in order to let the initial
     * filename to be handled by the framework. It's easier to deal
     * with the string lifetime.
     */
    if( NULL != mca_common_monitoring_initial_filename )
        mca_common_monitoring_current_filename = strdup(mca_common_monitoring_initial_filename);

    /* Register PVARs */

    /* PML PVARs */
    (void)mca_base_pvar_register("ompi", "pml", "monitoring", "flush", "Flush the monitoring "
                                 "information in the provided file. The filename is append with "
                                 "the .%d.prof suffix, where %d is replaced with the processus "
                                 "rank in MPI_COMM_WORLD.",
                                 OPAL_INFO_LVL_1, MCA_BASE_PVAR_CLASS_GENERIC,
                                 MCA_BASE_VAR_TYPE_STRING, NULL, MPI_T_BIND_NO_OBJECT, MCA_BASE_PVAR_FLAG_IWG,
                                 mca_common_monitoring_get_flush, mca_common_monitoring_set_flush,
                                 mca_common_monitoring_notify_flush, NULL);

    (void)mca_base_pvar_register("ompi", "pml", "monitoring", "messages_count", "Number of "
                                 "messages sent to each peer through the PML framework.",
                                 OPAL_INFO_LVL_4, MPI_T_PVAR_CLASS_SIZE,
                                 MCA_MONITORING_VAR_TYPE, NULL, MPI_T_BIND_MPI_COMM,
                                 MCA_BASE_PVAR_FLAG_READONLY | MCA_BASE_PVAR_FLAG_IWG,
                                 mca_common_monitoring_get_pml_count, NULL,
                                 mca_common_monitoring_comm_size_notify, NULL);

    (void)mca_base_pvar_register("ompi", "pml", "monitoring", "messages_size", "Size of messages "
                                 "sent to each peer in a communicator through the PML framework.",
                                 OPAL_INFO_LVL_4, MPI_T_PVAR_CLASS_SIZE,
                                 MCA_MONITORING_VAR_TYPE, NULL, MPI_T_BIND_MPI_COMM,
                                 MCA_BASE_PVAR_FLAG_READONLY | MCA_BASE_PVAR_FLAG_IWG,
                                 mca_common_monitoring_get_pml_size, NULL,
                                 mca_common_monitoring_comm_size_notify, NULL);

    /* OSC PVARs */
    (void)mca_base_pvar_register("ompi", "osc", "monitoring", "messages_sent_count", "Number of "
                                 "messages sent through the OSC framework with each peer.",
                                 OPAL_INFO_LVL_4, MPI_T_PVAR_CLASS_SIZE,
                                 MCA_MONITORING_VAR_TYPE, NULL, MPI_T_BIND_MPI_COMM,
                                 MCA_BASE_PVAR_FLAG_READONLY | MCA_BASE_PVAR_FLAG_IWG,
                                 mca_common_monitoring_get_osc_sent_count, NULL,
                                 mca_common_monitoring_comm_size_notify, NULL);
    
    (void)mca_base_pvar_register("ompi", "osc", "monitoring", "messages_sent_size", "Size of "
                                 "messages sent through the OSC framework with each peer.",
                                 OPAL_INFO_LVL_4, MPI_T_PVAR_CLASS_SIZE,
                                 MCA_MONITORING_VAR_TYPE, NULL, MPI_T_BIND_MPI_COMM,
                                 MCA_BASE_PVAR_FLAG_READONLY | MCA_BASE_PVAR_FLAG_IWG,
                                 mca_common_monitoring_get_osc_sent_size, NULL,
                                 mca_common_monitoring_comm_size_notify, NULL);

    (void)mca_base_pvar_register("ompi", "osc", "monitoring", "messages_recv_count", "Number of "
                                 "messages received through the OSC framework with each peer.",
                                 OPAL_INFO_LVL_4, MPI_T_PVAR_CLASS_SIZE,
                                 MCA_MONITORING_VAR_TYPE, NULL, MPI_T_BIND_MPI_COMM,
                                 MCA_BASE_PVAR_FLAG_READONLY | MCA_BASE_PVAR_FLAG_IWG,
                                 mca_common_monitoring_get_osc_recv_count, NULL,
                                 mca_common_monitoring_comm_size_notify, NULL);

    (void)mca_base_pvar_register("ompi", "osc", "monitoring", "messages_recv_size", "Size of "
                                 "messages received through the OSC framework with each peer.",
                                 OPAL_INFO_LVL_4, MPI_T_PVAR_CLASS_SIZE,
                                 MCA_MONITORING_VAR_TYPE, NULL, MPI_T_BIND_MPI_COMM,
                                 MCA_BASE_PVAR_FLAG_READONLY | MCA_BASE_PVAR_FLAG_IWG,
                                 mca_common_monitoring_get_osc_recv_size, NULL,
                                 mca_common_monitoring_comm_size_notify, NULL);

    /* COLL PVARs */
    (void)mca_base_pvar_register("ompi", "coll", "monitoring", "messages_count", "Number of "
                                 "messages exchanged through the COLL framework with each peer.",
                                 OPAL_INFO_LVL_4, MPI_T_PVAR_CLASS_SIZE,
                                 MCA_MONITORING_VAR_TYPE, NULL, MPI_T_BIND_MPI_COMM,
                                 MCA_BASE_PVAR_FLAG_READONLY | MCA_BASE_PVAR_FLAG_IWG,
                                 mca_common_monitoring_get_coll_count, NULL,
                                 mca_common_monitoring_comm_size_notify, NULL);

    (void)mca_base_pvar_register("ompi", "coll", "monitoring", "messages_size", "Size of "
                                 "messages exchanged through the COLL framework with each peer.",
                                 OPAL_INFO_LVL_4, MPI_T_PVAR_CLASS_SIZE,
                                 MCA_MONITORING_VAR_TYPE, NULL, MPI_T_BIND_MPI_COMM,
                                 MCA_BASE_PVAR_FLAG_READONLY | MCA_BASE_PVAR_FLAG_IWG,
                                 mca_common_monitoring_get_coll_size, NULL,
                                 mca_common_monitoring_comm_size_notify, NULL);

    (void)mca_base_pvar_register("ompi", "coll", "monitoring", "o2a_count", "Number of messages "
                                 "exchanged as one-to-all operations in a communicator.",
                                 OPAL_INFO_LVL_4, MPI_T_PVAR_CLASS_COUNTER,
                                 MCA_MONITORING_VAR_TYPE, NULL, MPI_T_BIND_MPI_COMM,
                                 MCA_BASE_PVAR_FLAG_READONLY | MCA_BASE_PVAR_FLAG_IWG,
                                 mca_common_monitoring_coll_get_o2a_count, NULL,
                                 mca_common_monitoring_coll_messages_notify, NULL);
    
    (void)mca_base_pvar_register("ompi", "coll", "monitoring", "o2a_size", "Size of messages "
                                 "exchanged as one-to-all operations in a communicator.",
                                 OPAL_INFO_LVL_4, MPI_T_PVAR_CLASS_AGGREGATE,
                                 MCA_MONITORING_VAR_TYPE, NULL, MPI_T_BIND_MPI_COMM,
                                 MCA_BASE_PVAR_FLAG_READONLY | MCA_BASE_PVAR_FLAG_IWG,
                                 mca_common_monitoring_coll_get_o2a_size, NULL,
                                 mca_common_monitoring_coll_messages_notify, NULL);

    (void)mca_base_pvar_register("ompi", "coll", "monitoring", "a2o_count", "Number of messages "
                                 "exchanged as all-to-one operations in a communicator.",
                                 OPAL_INFO_LVL_4, MPI_T_PVAR_CLASS_COUNTER,
                                 MCA_MONITORING_VAR_TYPE, NULL, MPI_T_BIND_MPI_COMM,
                                 MCA_BASE_PVAR_FLAG_READONLY | MCA_BASE_PVAR_FLAG_IWG,
                                 mca_common_monitoring_coll_get_a2o_count, NULL,
                                 mca_common_monitoring_coll_messages_notify, NULL);
    
    (void)mca_base_pvar_register("ompi", "coll", "monitoring", "a2o_size", "Size of messages "
                                 "exchanged as all-to-one operations in a communicator.",
                                 OPAL_INFO_LVL_4, MPI_T_PVAR_CLASS_AGGREGATE,
                                 MCA_MONITORING_VAR_TYPE, NULL, MPI_T_BIND_MPI_COMM,
                                 MCA_BASE_PVAR_FLAG_READONLY | MCA_BASE_PVAR_FLAG_IWG,
                                 mca_common_monitoring_coll_get_a2o_size, NULL,
                                 mca_common_monitoring_coll_messages_notify, NULL);

    (void)mca_base_pvar_register("ompi", "coll", "monitoring", "a2a_count", "Number of messages "
                                 "exchanged as all-to-all operations in a communicator.",
                                 OPAL_INFO_LVL_4, MPI_T_PVAR_CLASS_COUNTER,
                                 MCA_MONITORING_VAR_TYPE, NULL, MPI_T_BIND_MPI_COMM,
                                 MCA_BASE_PVAR_FLAG_READONLY | MCA_BASE_PVAR_FLAG_IWG,
                                 mca_common_monitoring_coll_get_a2a_count, NULL,
                                 mca_common_monitoring_coll_messages_notify, NULL);
    
    (void)mca_base_pvar_register("ompi", "coll", "monitoring", "a2a_size", "Size of messages "
                                 "exchanged as all-to-all operations in a communicator.",
                                 OPAL_INFO_LVL_4, MPI_T_PVAR_CLASS_AGGREGATE,
                                 MCA_MONITORING_VAR_TYPE, NULL, MPI_T_BIND_MPI_COMM,
                                 MCA_BASE_PVAR_FLAG_READONLY | MCA_BASE_PVAR_FLAG_IWG,
                                 mca_common_monitoring_coll_get_a2a_size, NULL,
                                 mca_common_monitoring_coll_messages_notify, NULL);
}

/**
 * This PML monitors only the processes in the MPI_COMM_WORLD. As OMPI is now lazily
 * adding peers on the first call to add_procs we need to check how many processes
 * are in the MPI_COMM_WORLD to create the storage with the right size.
 */
int mca_common_monitoring_add_procs(struct ompi_proc_t **procs,
                                    size_t nprocs)
{
    opal_process_name_t tmp, wp_name;
    size_t i;
    int peer_rank;
    uint64_t key;
    if( 0 > rank_world )
        rank_world = ompi_comm_rank((ompi_communicator_t*)&ompi_mpi_comm_world);
    if( !nprocs_world )
        nprocs_world = ompi_comm_size((ompi_communicator_t*)&ompi_mpi_comm_world);

    if( NULL == pml_data ) {
        int array_size = (10 + max_size_histogram) * nprocs_world;
        pml_data           = (size_t*)calloc(array_size, sizeof(size_t));
        pml_count          = pml_data + nprocs_world;
        filtered_pml_data  = pml_count + nprocs_world;
        filtered_pml_count = filtered_pml_data + nprocs_world;
        osc_data_s         = filtered_pml_count + nprocs_world;
        osc_count_s        = osc_data_s + nprocs_world;
        osc_data_r         = osc_count_s + nprocs_world;
        osc_count_r        = osc_data_r + nprocs_world;
        coll_data          = osc_count_r + nprocs_world;
        coll_count         = coll_data + nprocs_world;

        size_histogram     = coll_count + nprocs_world;
    }

    /* For all procs in the same MPI_COMM_WORLD we need to add them to the hash table */
    for( i = 0; i < nprocs; i++ ) {

        /* Extract the peer procname from the procs array */
        if( ompi_proc_is_sentinel(procs[i]) ) {
            tmp = ompi_proc_sentinel_to_name((uintptr_t)procs[i]);
        } else {
            tmp = procs[i]->super.proc_name;
        }
        if( tmp.jobid != ompi_proc_local_proc->super.proc_name.jobid )
            continue;

        /* each process will only be added once, so there is no way it already exists in the hash */
        for( peer_rank = 0; peer_rank < nprocs_world; peer_rank++ ) {
            wp_name = ompi_group_get_proc_name(((ompi_communicator_t*)&ompi_mpi_comm_world)->c_remote_group, peer_rank);
            if( 0 != opal_compare_proc( tmp, wp_name ) )
                continue;

            key = *((uint64_t*)&tmp);
            /* save the rank of the process in MPI_COMM_WORLD in the hash using the proc_name as the key */
            if( OPAL_SUCCESS != opal_hash_table_set_value_uint64(common_monitoring_translation_ht,
                                                                 key, (void*)(uintptr_t)peer_rank) ) {
                return OMPI_ERR_OUT_OF_RESOURCE;  /* failed to allocate memory or growing the hash table */
            }
            break;
        }
    }
    return OMPI_SUCCESS;
}

static void mca_common_monitoring_reset( void )
{
    int array_size = (10 + max_size_histogram) * nprocs_world;
    memset(pml_data, 0, array_size * sizeof(size_t));
    mca_common_monitoring_coll_reset();
}

void mca_common_monitoring_record_pml(int world_rank, size_t data_size, int tag)
{
    if( 0 == mca_common_monitoring_current_state ) return;  /* right now the monitoring is not started */

    /* Keep tracks of the data_size distribution */
    if( 0 == data_size ) {
        opal_atomic_add_fetch_size_t(&size_histogram[world_rank * max_size_histogram], 1);
    } else {
        int log2_size = log10(data_size)/log10_2;
        if(log2_size > max_size_histogram - 2) /* Avoid out-of-bound write */
            log2_size = max_size_histogram - 2;
        opal_atomic_add_fetch_size_t(&size_histogram[world_rank * max_size_histogram + log2_size + 1], 1);
    }
        
    /* distinguishses positive and negative tags if requested */
    if( (tag < 0) && (mca_common_monitoring_filter()) ) {
        opal_atomic_add_fetch_size_t(&filtered_pml_data[world_rank], data_size);
        opal_atomic_add_fetch_size_t(&filtered_pml_count[world_rank], 1);
    } else { /* if filtered monitoring is not activated data is aggregated indifferently */
        opal_atomic_add_fetch_size_t(&pml_data[world_rank], data_size);
        opal_atomic_add_fetch_size_t(&pml_count[world_rank], 1);
    }
}

static int mca_common_monitoring_get_pml_count(const struct mca_base_pvar_t *pvar,
                                               void *value,
                                               void *obj_handle)
{
    ompi_communicator_t *comm = (ompi_communicator_t *) obj_handle;
    int i, comm_size = ompi_comm_size (comm);
    size_t *values = (size_t*) value;

    if(comm != &ompi_mpi_comm_world.comm || NULL == pml_count)
        return OMPI_ERROR;

    for (i = 0 ; i < comm_size ; ++i) {
        values[i] = pml_count[i];
    }

    return OMPI_SUCCESS;
}

static int mca_common_monitoring_get_pml_size(const struct mca_base_pvar_t *pvar,
                                              void *value,
                                              void *obj_handle)
{
    ompi_communicator_t *comm = (ompi_communicator_t *) obj_handle;
    int comm_size = ompi_comm_size (comm);
    size_t *values = (size_t*) value;
    int i;

    if(comm != &ompi_mpi_comm_world.comm || NULL == pml_data)
        return OMPI_ERROR;

    for (i = 0 ; i < comm_size ; ++i) {
        values[i] = pml_data[i];
    }

    return OMPI_SUCCESS;
}

void mca_common_monitoring_record_osc(int world_rank, size_t data_size,
                                      enum mca_monitoring_osc_direction dir)
{
    if( 0 == mca_common_monitoring_current_state ) return;  /* right now the monitoring is not started */

    if( SEND == dir ) {
        opal_atomic_add_fetch_size_t(&osc_data_s[world_rank], data_size);
        opal_atomic_add_fetch_size_t(&osc_count_s[world_rank], 1);
    } else {
        opal_atomic_add_fetch_size_t(&osc_data_r[world_rank], data_size);
        opal_atomic_add_fetch_size_t(&osc_count_r[world_rank], 1);
    }
}

static int mca_common_monitoring_get_osc_sent_count(const struct mca_base_pvar_t *pvar,
                                                    void *value,
                                                    void *obj_handle)
{
    ompi_communicator_t *comm = (ompi_communicator_t *) obj_handle;
    int i, comm_size = ompi_comm_size (comm);
    size_t *values = (size_t*) value;

    if(comm != &ompi_mpi_comm_world.comm || NULL == pml_count)
        return OMPI_ERROR;

    for (i = 0 ; i < comm_size ; ++i) {
        values[i] = osc_count_s[i];
    }

    return OMPI_SUCCESS;
}

static int mca_common_monitoring_get_osc_sent_size(const struct mca_base_pvar_t *pvar,
                                                   void *value,
                                                   void *obj_handle)
{
    ompi_communicator_t *comm = (ompi_communicator_t *) obj_handle;
    int comm_size = ompi_comm_size (comm);
    size_t *values = (size_t*) value;
    int i;

    if(comm != &ompi_mpi_comm_world.comm || NULL == pml_data)
        return OMPI_ERROR;

    for (i = 0 ; i < comm_size ; ++i) {
        values[i] = osc_data_s[i];
    }

    return OMPI_SUCCESS;
}

static int mca_common_monitoring_get_osc_recv_count(const struct mca_base_pvar_t *pvar,
                                                    void *value,
                                                    void *obj_handle)
{
    ompi_communicator_t *comm = (ompi_communicator_t *) obj_handle;
    int i, comm_size = ompi_comm_size (comm);
    size_t *values = (size_t*) value;

    if(comm != &ompi_mpi_comm_world.comm || NULL == pml_count)
        return OMPI_ERROR;

    for (i = 0 ; i < comm_size ; ++i) {
        values[i] = osc_count_r[i];
    }

    return OMPI_SUCCESS;
}

static int mca_common_monitoring_get_osc_recv_size(const struct mca_base_pvar_t *pvar,
                                                   void *value,
                                                   void *obj_handle)
{
    ompi_communicator_t *comm = (ompi_communicator_t *) obj_handle;
    int comm_size = ompi_comm_size (comm);
    size_t *values = (size_t*) value;
    int i;

    if(comm != &ompi_mpi_comm_world.comm || NULL == pml_data)
        return OMPI_ERROR;

    for (i = 0 ; i < comm_size ; ++i) {
        values[i] = osc_data_r[i];
    }

    return OMPI_SUCCESS;
}

void mca_common_monitoring_record_coll(int world_rank, size_t data_size)
{
    if( 0 == mca_common_monitoring_current_state ) return;  /* right now the monitoring is not started */

    opal_atomic_add_fetch_size_t(&coll_data[world_rank], data_size);
    opal_atomic_add_fetch_size_t(&coll_count[world_rank], 1);
}

static int mca_common_monitoring_get_coll_count(const struct mca_base_pvar_t *pvar,
                                                void *value,
                                                void *obj_handle)
{
    ompi_communicator_t *comm = (ompi_communicator_t *) obj_handle;
    int i, comm_size = ompi_comm_size (comm);
    size_t *values = (size_t*) value;

    if(comm != &ompi_mpi_comm_world.comm || NULL == pml_count)
        return OMPI_ERROR;

    for (i = 0 ; i < comm_size ; ++i) {
        values[i] = coll_count[i];
    }

    return OMPI_SUCCESS;
}

static int mca_common_monitoring_get_coll_size(const struct mca_base_pvar_t *pvar,
                                               void *value,
                                               void *obj_handle)
{
    ompi_communicator_t *comm = (ompi_communicator_t *) obj_handle;
    int comm_size = ompi_comm_size (comm);
    size_t *values = (size_t*) value;
    int i;

    if(comm != &ompi_mpi_comm_world.comm || NULL == pml_data)
        return OMPI_ERROR;

    for (i = 0 ; i < comm_size ; ++i) {
        values[i] = coll_data[i];
    }

    return OMPI_SUCCESS;
}

static void mca_common_monitoring_output( FILE *pf, int my_rank, int nbprocs )
{
    /* Dump outgoing messages */
    fprintf(pf, "# POINT TO POINT\n");
    for (int i = 0 ; i < nbprocs ; i++) {
        if(pml_count[i] > 0) {
            fprintf(pf, "E\t%" PRId32 "\t%" PRId32 "\t%zu bytes\t%zu msgs sent\t",
                    my_rank, i, pml_data[i], pml_count[i]);
            for(int j = 0 ; j < max_size_histogram ; ++j)
                fprintf(pf, "%zu%s", size_histogram[i * max_size_histogram + j],
                        j < max_size_histogram - 1 ? "," : "\n");
        }
    }

    /* Dump outgoing synchronization/collective messages */
    if( mca_common_monitoring_filter() ) {
        for (int i = 0 ; i < nbprocs ; i++) {
            if(filtered_pml_count[i] > 0) {
                fprintf(pf, "I\t%" PRId32 "\t%" PRId32 "\t%zu bytes\t%zu msgs sent%s",
                        my_rank, i, filtered_pml_data[i], filtered_pml_count[i],
                        0 == pml_count[i] ? "\t" : "\n");
                /* 
                 * In the case there was no external messages
                 * exchanged between the two processes, the histogram
                 * has not yet been dumpped. Then we need to add it at
                 * the end of the internal category.
                 */
                if(0 == pml_count[i]) {
                    for(int j = 0 ; j < max_size_histogram ; ++j)
                        fprintf(pf, "%zu%s", size_histogram[i * max_size_histogram + j],
                                j < max_size_histogram - 1 ? "," : "\n");
                }
            }
        }
    }

    /* Dump incoming messages */
    fprintf(pf, "# OSC\n");
    for (int i = 0 ; i < nbprocs ; i++) {
        if(osc_count_s[i] > 0) {
            fprintf(pf, "S\t%" PRId32 "\t%" PRId32 "\t%zu bytes\t%zu msgs sent\n",
                    my_rank, i, osc_data_s[i], osc_count_s[i]);
        }
        if(osc_count_r[i] > 0) {
            fprintf(pf, "R\t%" PRId32 "\t%" PRId32 "\t%zu bytes\t%zu msgs sent\n",
                    my_rank, i, osc_data_r[i], osc_count_r[i]);
        }
    }

    /* Dump collectives */
    fprintf(pf, "# COLLECTIVES\n");
    for (int i = 0 ; i < nbprocs ; i++) {
        if(coll_count[i] > 0) {
            fprintf(pf, "C\t%" PRId32 "\t%" PRId32 "\t%zu bytes\t%zu msgs sent\n",
                    my_rank, i, coll_data[i], coll_count[i]);
        }
    }
    mca_common_monitoring_coll_flush_all(pf);
}

/*
 * Flushes the monitoring into filename
 * Useful for phases (see example in test/monitoring)
 */
static int mca_common_monitoring_flush(int fd, char* filename)
{
    /* If we are not drived by MPIT then dump the monitoring information */
    if( 0 == mca_common_monitoring_current_state || 0 == fd ) /* if disabled do nothing */
        return OMPI_SUCCESS;

    if( 1 == fd ) {
        OPAL_MONITORING_PRINT_INFO("Proc %" PRId32 " flushing monitoring to stdout", rank_world);
        mca_common_monitoring_output( stdout, rank_world, nprocs_world );
    } else if( 2 == fd ) {
        OPAL_MONITORING_PRINT_INFO("Proc %" PRId32 " flushing monitoring to stderr", rank_world);
        mca_common_monitoring_output( stderr, rank_world, nprocs_world );
    } else {
        FILE *pf = NULL;
        char* tmpfn = NULL;

        if( NULL == filename ) { /* No filename */
            OPAL_MONITORING_PRINT_ERR("Error while flushing: no filename provided");
            return OMPI_ERROR;
        } else {
            asprintf(&tmpfn, "%s.%" PRId32 ".prof", filename, rank_world);
            pf = fopen(tmpfn, "w");
            free(tmpfn);
        }

        if(NULL == pf) {  /* Error during open */
            OPAL_MONITORING_PRINT_ERR("Error while flushing to: %s.%" PRId32 ".prof",
                                      filename, rank_world);
            return OMPI_ERROR;
        }

        OPAL_MONITORING_PRINT_INFO("Proc %d flushing monitoring to: %s.%" PRId32 ".prof",
                                   rank_world, filename, rank_world);

        mca_common_monitoring_output( pf, rank_world, nprocs_world );

        fclose(pf);
    }
    /* Reset to 0 all monitored data */
    mca_common_monitoring_reset();
    return OMPI_SUCCESS;
}
