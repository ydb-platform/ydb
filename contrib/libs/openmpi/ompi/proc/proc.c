/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2012-2015 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2013-2015 Intel, Inc. All rights reserved
 * Copyright (c) 2014-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015-2017 Mellanox Technologies. All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <string.h>
#include <strings.h>

#include "ompi/constants.h"
#include "opal/datatype/opal_convertor.h"
#include "opal/threads/mutex.h"
#include "opal/dss/dss.h"
#include "opal/util/arch.h"
#include "opal/util/show_help.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/mca/pmix/pmix.h"
#include "opal/util/argv.h"

#include "ompi/proc/proc.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/runtime/mpiruntime.h"
#include "ompi/runtime/params.h"
#include "ompi/mca/pml/pml.h"

opal_list_t  ompi_proc_list = {{0}};
static opal_mutex_t ompi_proc_lock;
static opal_hash_table_t ompi_proc_hash;

ompi_proc_t* ompi_proc_local_proc = NULL;

static void ompi_proc_construct(ompi_proc_t* proc);
static void ompi_proc_destruct(ompi_proc_t* proc);
static ompi_proc_t *ompi_proc_for_name_nolock (const opal_process_name_t proc_name);

OBJ_CLASS_INSTANCE(
    ompi_proc_t,
    opal_proc_t,
    ompi_proc_construct,
    ompi_proc_destruct
);


void ompi_proc_construct(ompi_proc_t* proc)
{
    bzero(proc->proc_endpoints, sizeof(proc->proc_endpoints));

    /* By default all processors are supposedly having the same architecture as me. Thus,
     * by default we run in a homogeneous environment. Later, when the RTE can tell us
     * the arch of the remote nodes, we will have to set the convertors to the correct
     * architecture.
     */
    OBJ_RETAIN( ompi_mpi_local_convertor );
    proc->super.proc_convertor = ompi_mpi_local_convertor;
}


void ompi_proc_destruct(ompi_proc_t* proc)
{
    /* As all the convertors are created with OBJ_NEW we can just call OBJ_RELEASE. All, except
     * the local convertor, will get destroyed at some point here. If the reference count is correct
     * the local convertor (who has the reference count increased in the datatype) will not get
     * destroyed here. It will be destroyed later when the ompi_datatype_finalize is called.
     */
    OBJ_RELEASE( proc->super.proc_convertor );
    if (NULL != proc->super.proc_hostname) {
        free(proc->super.proc_hostname);
    }
    opal_mutex_lock (&ompi_proc_lock);
    opal_list_remove_item(&ompi_proc_list, (opal_list_item_t*)proc);
    opal_hash_table_remove_value_ptr (&ompi_proc_hash, &proc->super.proc_name, sizeof (proc->super.proc_name));
    opal_mutex_unlock (&ompi_proc_lock);
}

/**
 * Allocate a new ompi_proc_T for the given jobid/vpid
 *
 * @param[in]  jobid Job identifier
 * @param[in]  vpid  Process identifier
 * @param[out] procp New ompi_proc_t structure
 *
 * This function allocates a new ompi_proc_t and inserts it into
 * the process list and hash table.
 */
static int ompi_proc_allocate (ompi_jobid_t jobid, ompi_vpid_t vpid, ompi_proc_t **procp) {
    ompi_proc_t *proc = OBJ_NEW(ompi_proc_t);

    opal_list_append(&ompi_proc_list, (opal_list_item_t*)proc);

    OMPI_CAST_RTE_NAME(&proc->super.proc_name)->jobid = jobid;
    OMPI_CAST_RTE_NAME(&proc->super.proc_name)->vpid = vpid;

    opal_hash_table_set_value_ptr (&ompi_proc_hash, &proc->super.proc_name, sizeof (proc->super.proc_name),
                                   proc);

    /* by default we consider process to be remote */
    proc->super.proc_flags = OPAL_PROC_NON_LOCAL;
    *procp = proc;

    return OMPI_SUCCESS;
}

/**
 * Finish setting up an ompi_proc_t
 *
 * @param[in] proc ompi process structure
 *
 * This function contains the core code of ompi_proc_complete_init() and
 * ompi_proc_refresh(). The tasks performed by this function include
 * retrieving the hostname (if below the modex cutoff), determining the
 * remote architecture, and calculating the locality of the process.
 */
int ompi_proc_complete_init_single (ompi_proc_t *proc)
{
    int ret;

    if ((OMPI_CAST_RTE_NAME(&proc->super.proc_name)->jobid == OMPI_PROC_MY_NAME->jobid) &&
        (OMPI_CAST_RTE_NAME(&proc->super.proc_name)->vpid  == OMPI_PROC_MY_NAME->vpid)) {
        /* nothing else to do */
        return OMPI_SUCCESS;
    }

    /* we can retrieve the hostname at no cost because it
     * was provided at startup - but make it optional so
     * we don't chase after it if some system doesn't
     * provide it */
    proc->super.proc_hostname = NULL;
    OPAL_MODEX_RECV_VALUE_OPTIONAL(ret, OPAL_PMIX_HOSTNAME, &proc->super.proc_name,
                                   (char**)&(proc->super.proc_hostname), OPAL_STRING);

#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    /* get the remote architecture - this might force a modex except
     * for those environments where the RM provides it */
    {
        uint32_t *ui32ptr;
        ui32ptr = &(proc->super.proc_arch);
        OPAL_MODEX_RECV_VALUE(ret, OPAL_PMIX_ARCH, &proc->super.proc_name,
                              (void**)&ui32ptr, OPAL_UINT32);
        if (OPAL_SUCCESS == ret) {
            /* if arch is different than mine, create a new convertor for this proc */
            if (proc->super.proc_arch != opal_local_arch) {
                OBJ_RELEASE(proc->super.proc_convertor);
                proc->super.proc_convertor = opal_convertor_create(proc->super.proc_arch, 0);
            }
        } else if (OMPI_ERR_NOT_IMPLEMENTED == ret) {
            proc->super.proc_arch = opal_local_arch;
        } else {
            return ret;
        }
    }
#else
    /* must be same arch as my own */
    proc->super.proc_arch = opal_local_arch;
#endif

    return OMPI_SUCCESS;
}

opal_proc_t *ompi_proc_lookup (const opal_process_name_t proc_name)
{
    ompi_proc_t *proc = NULL;
    int ret;

    /* try to lookup the value in the hash table */
    ret = opal_hash_table_get_value_ptr (&ompi_proc_hash, &proc_name, sizeof (proc_name), (void **) &proc);

    if (OPAL_SUCCESS == ret) {
        return &proc->super;
    }

    return NULL;
}

static ompi_proc_t *ompi_proc_for_name_nolock (const opal_process_name_t proc_name)
{
    ompi_proc_t *proc = NULL;
    int ret;

    /* double-check that another competing thread has not added this proc */
    ret = opal_hash_table_get_value_ptr (&ompi_proc_hash, &proc_name, sizeof (proc_name), (void **) &proc);
    if (OPAL_SUCCESS == ret) {
        goto exit;
    }

    /* allocate a new ompi_proc_t object for the process and insert it into the process table */
    ret = ompi_proc_allocate (proc_name.jobid, proc_name.vpid, &proc);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        /* allocation fail */
        goto exit;
    }

    /* finish filling in the important proc data fields */
    ret = ompi_proc_complete_init_single (proc);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        goto exit;
    }
exit:
    return proc;
}

opal_proc_t *ompi_proc_for_name (const opal_process_name_t proc_name)
{
    ompi_proc_t *proc = NULL;
    int ret;

    /* try to lookup the value in the hash table */
    ret = opal_hash_table_get_value_ptr (&ompi_proc_hash, &proc_name, sizeof (proc_name), (void **) &proc);
    if (OPAL_SUCCESS == ret) {
        return &proc->super;
    }

    opal_mutex_lock (&ompi_proc_lock);
    proc = ompi_proc_for_name_nolock (proc_name);
    opal_mutex_unlock (&ompi_proc_lock);

    return (opal_proc_t *) proc;
}

int ompi_proc_init(void)
{
    int opal_proc_hash_init_size = (ompi_process_info.num_procs < ompi_add_procs_cutoff) ? ompi_process_info.num_procs :
        1024;
    ompi_proc_t *proc;
    int ret;

    OBJ_CONSTRUCT(&ompi_proc_list, opal_list_t);
    OBJ_CONSTRUCT(&ompi_proc_lock, opal_mutex_t);
    OBJ_CONSTRUCT(&ompi_proc_hash, opal_hash_table_t);

    ret = opal_hash_table_init (&ompi_proc_hash, opal_proc_hash_init_size);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }

    /* create a proc for the local process */
    ret = ompi_proc_allocate (OMPI_PROC_MY_NAME->jobid, OMPI_PROC_MY_NAME->vpid, &proc);
    if (OMPI_SUCCESS != ret) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    /* set local process data */
    ompi_proc_local_proc = proc;
    proc->super.proc_flags = OPAL_PROC_ALL_LOCAL;
    proc->super.proc_hostname = strdup(ompi_process_info.nodename);
    proc->super.proc_arch = opal_local_arch;
    /* Register the local proc with OPAL */
    opal_proc_local_set(&proc->super);
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
    /* add our arch to the modex */
    OPAL_MODEX_SEND_VALUE(ret, OPAL_PMIX_GLOBAL,
                          OPAL_PMIX_ARCH, &opal_local_arch, OPAL_UINT32);
    if (OPAL_SUCCESS != ret) {
        return ret;
    }
#endif

    return OMPI_SUCCESS;
}

static int ompi_proc_compare_vid (opal_list_item_t **a, opal_list_item_t **b)
{
    ompi_proc_t *proca = (ompi_proc_t *) *a;
    ompi_proc_t *procb = (ompi_proc_t *) *b;

    if (proca->super.proc_name.vpid > procb->super.proc_name.vpid) {
        return 1;
    } else {
        return -1;
    }

    /* they should never be equal */
}

/**
 * The process creation is split into two steps. The second step
 * is the important one, it sets the properties of the remote
 * process, such as architecture, node name and locality flags.
 *
 * This function is to be called __only__ after the modex exchange
 * has been performed, in order to allow the modex to carry the data
 * instead of requiring the runtime to provide it.
 */
int ompi_proc_complete_init(void)
{
    opal_process_name_t wildcard_rank;
    ompi_proc_t *proc;
    int ret, errcode = OMPI_SUCCESS;
    char *val;

    opal_mutex_lock (&ompi_proc_lock);

    /* Add all local peers first */
    wildcard_rank.jobid = OMPI_PROC_MY_NAME->jobid;
    wildcard_rank.vpid = OMPI_NAME_WILDCARD->vpid;
    /* retrieve the local peers */
    OPAL_MODEX_RECV_VALUE(ret, OPAL_PMIX_LOCAL_PEERS,
                          &wildcard_rank, &val, OPAL_STRING);
    if (OPAL_SUCCESS == ret && NULL != val) {
        char **peers = opal_argv_split(val, ',');
        int i;
        free(val);
        for (i=0; NULL != peers[i]; i++) {
            ompi_vpid_t local_rank = strtoul(peers[i], NULL, 10);
            uint16_t u16, *u16ptr = &u16;
            if (OMPI_PROC_MY_NAME->vpid == local_rank) {
                continue;
            }
            ret = ompi_proc_allocate (OMPI_PROC_MY_NAME->jobid, local_rank, &proc);
            if (OMPI_SUCCESS != ret) {
                return ret;
            }
            /* get the locality information - all RTEs are required
             * to provide this information at startup */
            OPAL_MODEX_RECV_VALUE_OPTIONAL(ret, OPAL_PMIX_LOCALITY, &proc->super.proc_name, &u16ptr, OPAL_UINT16);
            if (OPAL_SUCCESS == ret) {
                proc->super.proc_flags = u16;
            }
        }
        opal_argv_free(peers);
    }

    /* Complete initialization of node-local procs */
    OPAL_LIST_FOREACH(proc, &ompi_proc_list, ompi_proc_t) {
        ret = ompi_proc_complete_init_single (proc);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            errcode = ret;
            break;
        }
    }

    /* if cutoff is larger than # of procs - add all processes
     * NOTE that local procs will be automatically skipped as they
     * are already in the hash table
     */
    if (ompi_process_info.num_procs < ompi_add_procs_cutoff) {
        /* sinse ompi_proc_for_name is locking internally -
         * we need to release lock here
         */
        opal_mutex_unlock (&ompi_proc_lock);

        for (ompi_vpid_t i = 0 ; i < ompi_process_info.num_procs ; ++i ) {
            opal_process_name_t proc_name;
            proc_name.jobid = OMPI_PROC_MY_NAME->jobid;
            proc_name.vpid = i;
            (void) ompi_proc_for_name (proc_name);
        }

        /* acquire lock back for the next step - sort */
        opal_mutex_lock (&ompi_proc_lock);
    }

    opal_list_sort (&ompi_proc_list, ompi_proc_compare_vid);

    opal_mutex_unlock (&ompi_proc_lock);

    return errcode;
}

int ompi_proc_finalize (void)
{
    ompi_proc_t *proc;

    /* Unregister the local proc from OPAL */
    opal_proc_local_set(NULL);

    /* remove all items from list and destroy them. Since we cannot know
     * the reference count of the procs for certain, it is possible that
     * a single OBJ_RELEASE won't drive the count to zero, and hence will
     * not release the memory. Accordingly, we cycle through the list here,
     * calling release on each item.
     *
     * This will cycle until it forces the reference count of each item
     * to zero, thus causing the destructor to run - which will remove
     * the item from the list!
     *
     * We cannot do this under the thread lock as the destructor will
     * call it when removing the item from the list. However, this function
     * is ONLY called from MPI_Finalize, and all threads are prohibited from
     * calling an MPI function once ANY thread has called MPI_Finalize. Of
     * course, multiple threads are allowed to call MPI_Finalize, so this
     * function may get called multiple times by various threads. We believe
     * it is thread safe to do so...though it may not -appear- to be so
     * without walking through the entire list/destructor sequence.
     */
    while ((ompi_proc_t *)opal_list_get_end(&ompi_proc_list) != (proc = (ompi_proc_t *)opal_list_get_first(&ompi_proc_list))) {
        OBJ_RELEASE(proc);
    }
    /* now destruct the list and thread lock */
    OBJ_DESTRUCT(&ompi_proc_list);
    OBJ_DESTRUCT(&ompi_proc_lock);
    OBJ_DESTRUCT(&ompi_proc_hash);

    return OMPI_SUCCESS;
}

int ompi_proc_world_size (void)
{
    return ompi_process_info.num_procs;
}

ompi_proc_t **ompi_proc_get_allocated (size_t *size)
{
    ompi_proc_t **procs;
    ompi_proc_t *proc;
    size_t count = 0;
    ompi_rte_cmp_bitmask_t mask;
    ompi_process_name_t my_name;

    /* check bozo case */
    if (NULL == ompi_proc_local_proc) {
        return NULL;
    }
    mask = OMPI_RTE_CMP_JOBID;
    my_name = *OMPI_CAST_RTE_NAME(&ompi_proc_local_proc->super.proc_name);

    /* First count how many match this jobid */
    opal_mutex_lock (&ompi_proc_lock);
    OPAL_LIST_FOREACH(proc, &ompi_proc_list, ompi_proc_t) {
        if (OPAL_EQUAL == ompi_rte_compare_name_fields(mask, OMPI_CAST_RTE_NAME(&proc->super.proc_name), &my_name)) {
            ++count;
        }
    }

    /* allocate an array */
    procs = (ompi_proc_t**) malloc(count * sizeof(ompi_proc_t*));
    if (NULL == procs) {
        opal_mutex_unlock (&ompi_proc_lock);
        return NULL;
    }

    /* now save only the procs that match this jobid */
    count = 0;
    OPAL_LIST_FOREACH(proc, &ompi_proc_list, ompi_proc_t) {
        if (OPAL_EQUAL == ompi_rte_compare_name_fields(mask, &proc->super.proc_name, &my_name)) {
            /* DO NOT RETAIN THIS OBJECT - the reference count on this
             * object will be adjusted by external callers. The intent
             * here is to allow the reference count to drop to zero if
             * the app no longer desires to communicate with this proc.
             * For example, the proc may call comm_disconnect on all
             * communicators involving this proc. In such cases, we want
             * the proc object to be removed from the list. By not incrementing
             * the reference count here, we allow this to occur.
             *
             * We don't implement that yet, but we are still safe for now as
             * the OBJ_NEW in ompi_proc_init owns the initial reference
             * count which cannot be released until ompi_proc_finalize is
             * called.
             */
            procs[count++] = proc;
        }
    }
    opal_mutex_unlock (&ompi_proc_lock);

    *size = count;
    return procs;
}

ompi_proc_t **ompi_proc_world (size_t *size)
{
    ompi_proc_t **procs;
    size_t count = 0;

    /* check bozo case */
    if (NULL == ompi_proc_local_proc) {
        return NULL;
    }

    /* First count how many match this jobid (we already know this from our process info) */
    count = ompi_process_info.num_procs;

    /* allocate an array */
    procs = (ompi_proc_t **) malloc (count * sizeof(ompi_proc_t*));
    if (NULL == procs) {
        return NULL;
    }

    /* now get/allocate all the procs in this jobid */
    for (size_t i = 0 ; i < count ; ++i) {
        opal_process_name_t name = {.jobid = OMPI_CAST_RTE_NAME(&ompi_proc_local_proc->super.proc_name)->jobid,
                                    .vpid = i};

        /* DO NOT RETAIN THIS OBJECT - the reference count on this
         * object will be adjusted by external callers. The intent
         * here is to allow the reference count to drop to zero if
         * the app no longer desires to communicate with this proc.
         * For example, the proc may call comm_disconnect on all
         * communicators involving this proc. In such cases, we want
         * the proc object to be removed from the list. By not incrementing
         * the reference count here, we allow this to occur.
         *
         * We don't implement that yet, but we are still safe for now as
         * the OBJ_NEW in ompi_proc_init owns the initial reference
         * count which cannot be released until ompi_proc_finalize is
         * called.
         */
        procs[i] = (ompi_proc_t*)ompi_proc_for_name (name);
    }

    *size = count;

    return procs;
}


ompi_proc_t** ompi_proc_all(size_t* size)
{
    ompi_proc_t **procs =
        (ompi_proc_t**) malloc(opal_list_get_size(&ompi_proc_list) * sizeof(ompi_proc_t*));
    ompi_proc_t *proc;
    size_t count = 0;

    if (NULL == procs) {
        return NULL;
    }

    opal_mutex_lock (&ompi_proc_lock);
    OPAL_LIST_FOREACH(proc, &ompi_proc_list, ompi_proc_t) {
        /* We know this isn't consistent with the behavior in ompi_proc_world,
         * but we are leaving the RETAIN for now because the code using this function
         * assumes that the results need to be released when done. It will
         * be cleaned up later as the "fix" will impact other places in
         * the code
         */
        OBJ_RETAIN(proc);
        procs[count++] = proc;
    }
    opal_mutex_unlock (&ompi_proc_lock);
    *size = count;
    return procs;
}


ompi_proc_t** ompi_proc_self(size_t* size)
{
    ompi_proc_t **procs = (ompi_proc_t**) malloc(sizeof(ompi_proc_t*));
    if (NULL == procs) {
        return NULL;
    }
    /* We know this isn't consistent with the behavior in ompi_proc_world,
     * but we are leaving the RETAIN for now because the code using this function
     * assumes that the results need to be released when done. It will
     * be cleaned up later as the "fix" will impact other places in
     * the code
     */
    OBJ_RETAIN(ompi_proc_local_proc);
    *procs = ompi_proc_local_proc;
    *size = 1;
    return procs;
}

ompi_proc_t * ompi_proc_find ( const ompi_process_name_t * name )
{
    ompi_proc_t *proc, *rproc=NULL;
    ompi_rte_cmp_bitmask_t mask;

    /* return the proc-struct which matches this jobid+process id */
    mask = OMPI_RTE_CMP_JOBID | OMPI_RTE_CMP_VPID;
    opal_mutex_lock (&ompi_proc_lock);
    OPAL_LIST_FOREACH(proc, &ompi_proc_list, ompi_proc_t) {
        if (OPAL_EQUAL == ompi_rte_compare_name_fields(mask, &proc->super.proc_name, name)) {
            rproc = proc;
            break;
        }
    }
    opal_mutex_unlock (&ompi_proc_lock);

    return rproc;
}


int ompi_proc_refresh(void)
{
    ompi_proc_t *proc = NULL;
    ompi_vpid_t i = 0;
    int ret=OMPI_SUCCESS;

    opal_mutex_lock (&ompi_proc_lock);

    OPAL_LIST_FOREACH(proc, &ompi_proc_list, ompi_proc_t) {
        /* Does not change: proc->super.proc_name.vpid */
        OMPI_CAST_RTE_NAME(&proc->super.proc_name)->jobid = OMPI_PROC_MY_NAME->jobid;

        /* Make sure to clear the local flag before we set it below */
        proc->super.proc_flags = 0;

        if (i == OMPI_PROC_MY_NAME->vpid) {
            ompi_proc_local_proc = proc;
            proc->super.proc_flags = OPAL_PROC_ALL_LOCAL;
            proc->super.proc_hostname = ompi_process_info.nodename;
            proc->super.proc_arch = opal_local_arch;
            opal_proc_local_set(&proc->super);
        } else {
            ret = ompi_proc_complete_init_single (proc);
            if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
                break;
            }
        }
    }

    opal_mutex_unlock (&ompi_proc_lock);

    return ret;
}

int
ompi_proc_pack(ompi_proc_t **proclist, int proclistsize,
               opal_buffer_t* buf)
{
    int rc;
    char *nspace;

    opal_mutex_lock (&ompi_proc_lock);

    /* cycle through the provided array, packing the OMPI level
     * data for each proc. This data may or may not be included
     * in any subsequent modex operation, so we include it here
     * to ensure completion of a connect/accept handshake. See
     * the ompi/mca/dpm framework for an example of where and how
     * this info is used.
     *
     * Eventually, we will review the procedures that call this
     * function to see if duplication of communication can be
     * reduced. For now, just go ahead and pack the info so it
     * can be sent.
     */
    for (int i = 0 ; i < proclistsize ; ++i) {
        ompi_proc_t *proc = proclist[i];

        if (ompi_proc_is_sentinel (proc)) {
            proc = ompi_proc_for_name_nolock (ompi_proc_sentinel_to_name ((uintptr_t) proc));
        }

        /* send proc name */
        rc = opal_dss.pack(buf, &(proc->super.proc_name), 1, OMPI_NAME);
        if(rc != OPAL_SUCCESS) {
            OMPI_ERROR_LOG(rc);
            opal_mutex_unlock (&ompi_proc_lock);
            return rc;
        }
        /* retrieve and send the corresponding nspace for this job
         * as the remote side may not know the translation */
        nspace = (char*)opal_pmix.get_nspace(proc->super.proc_name.jobid);
        rc = opal_dss.pack(buf, &nspace, 1, OPAL_STRING);
        if(rc != OPAL_SUCCESS) {
            OMPI_ERROR_LOG(rc);
            opal_mutex_unlock (&ompi_proc_lock);
            return rc;
        }
        /* pack architecture flag */
        rc = opal_dss.pack(buf, &(proc->super.proc_arch), 1, OPAL_UINT32);
        if(rc != OPAL_SUCCESS) {
            OMPI_ERROR_LOG(rc);
            opal_mutex_unlock (&ompi_proc_lock);
            return rc;
        }
        /* pass the name of the host this proc is on */
        rc = opal_dss.pack(buf, &(proc->super.proc_hostname), 1, OPAL_STRING);
        if(rc != OPAL_SUCCESS) {
            OMPI_ERROR_LOG(rc);
            opal_mutex_unlock (&ompi_proc_lock);
            return rc;
        }
    }
    opal_mutex_unlock (&ompi_proc_lock);
    return OMPI_SUCCESS;
}

ompi_proc_t *
ompi_proc_find_and_add(const ompi_process_name_t * name, bool* isnew)
{
    ompi_proc_t *proc, *rproc = NULL;
    ompi_rte_cmp_bitmask_t mask;

    /* return the proc-struct which matches this jobid+process id */
    mask = OMPI_RTE_CMP_JOBID | OMPI_RTE_CMP_VPID;
    opal_mutex_lock (&ompi_proc_lock);
    OPAL_LIST_FOREACH(proc, &ompi_proc_list, ompi_proc_t) {
        if (OPAL_EQUAL == ompi_rte_compare_name_fields(mask, &proc->super.proc_name, name)) {
            rproc = proc;
            *isnew = false;
            break;
        }
    }

    /* if we didn't find this proc in the list, create a new
     * proc_t and append it to the list
     */
    if (NULL == rproc) {
        *isnew = true;
        ompi_proc_allocate (name->jobid, name->vpid, &rproc);
    }

    opal_mutex_unlock (&ompi_proc_lock);

    return rproc;
}


int
ompi_proc_unpack(opal_buffer_t* buf,
                 int proclistsize, ompi_proc_t ***proclist,
                 int *newproclistsize, ompi_proc_t ***newproclist)
{
    size_t newprocs_len = 0;
    ompi_proc_t **plist=NULL, **newprocs = NULL;

    /* do not free plist *ever*, since it is used in the remote group
       structure of a communicator */
    plist = (ompi_proc_t **) calloc (proclistsize, sizeof (ompi_proc_t *));
    if ( NULL == plist ) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    /* free this on the way out */
    newprocs = (ompi_proc_t **) calloc (proclistsize, sizeof (ompi_proc_t *));
    if (NULL == newprocs) {
        free(plist);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    /* cycle through the array of provided procs and unpack
     * their info - as packed by ompi_proc_pack
     */
    for (int i = 0; i < proclistsize ; ++i){
        int32_t count=1;
        ompi_process_name_t new_name;
        uint32_t new_arch;
        char *new_hostname;
        bool isnew = false;
        int rc;
        char *nspace;

        rc = opal_dss.unpack(buf, &new_name, &count, OMPI_NAME);
        if (rc != OPAL_SUCCESS) {
            OMPI_ERROR_LOG(rc);
            free(plist);
            free(newprocs);
            return rc;
        }
        rc = opal_dss.unpack(buf, &nspace, &count, OPAL_STRING);
        if (rc != OPAL_SUCCESS) {
            OMPI_ERROR_LOG(rc);
            free(plist);
            free(newprocs);
            return rc;
        }
        opal_pmix.register_jobid(new_name.jobid, nspace);
        free(nspace);
        rc = opal_dss.unpack(buf, &new_arch, &count, OPAL_UINT32);
        if (rc != OPAL_SUCCESS) {
            OMPI_ERROR_LOG(rc);
            free(plist);
            free(newprocs);
            return rc;
        }
        rc = opal_dss.unpack(buf, &new_hostname, &count, OPAL_STRING);
        if (rc != OPAL_SUCCESS) {
            OMPI_ERROR_LOG(rc);
            free(plist);
            free(newprocs);
            return rc;
        }
        /* see if this proc is already on our ompi_proc_list */
        plist[i] = ompi_proc_find_and_add(&new_name, &isnew);
        if (isnew) {
            /* if not, then it was added, so update the values
             * in the proc_t struct with the info that was passed
             * to us
             */
            newprocs[newprocs_len++] = plist[i];

            /* update all the values */
            plist[i]->super.proc_arch = new_arch;
            /* if arch is different than mine, create a new convertor for this proc */
            if (plist[i]->super.proc_arch != opal_local_arch) {
#if OPAL_ENABLE_HETEROGENEOUS_SUPPORT
                OBJ_RELEASE(plist[i]->super.proc_convertor);
                plist[i]->super.proc_convertor = opal_convertor_create(plist[i]->super.proc_arch, 0);
#else
                opal_show_help("help-mpi-runtime.txt",
                               "heterogeneous-support-unavailable",
                               true, ompi_process_info.nodename,
                               new_hostname == NULL ? "<hostname unavailable>" :
                               new_hostname);
                free(plist);
                free(newprocs);
                return OMPI_ERR_NOT_SUPPORTED;
#endif
            }

            if (NULL != new_hostname) {
                if (0 == strcmp(ompi_proc_local_proc->super.proc_hostname, new_hostname)) {
                    plist[i]->super.proc_flags |= (OPAL_PROC_ON_NODE | OPAL_PROC_ON_CU | OPAL_PROC_ON_CLUSTER);
                }

                /* Save the hostname */
                plist[i]->super.proc_hostname = new_hostname;
            }
        } else if (NULL != new_hostname) {
            free(new_hostname);
        }
    }

    if (NULL != newproclistsize) *newproclistsize = newprocs_len;
    if (NULL != newproclist) {
        *newproclist = newprocs;
    } else if (newprocs != NULL) {
        free(newprocs);
    }

    *proclist = plist;
    return OMPI_SUCCESS;
}
