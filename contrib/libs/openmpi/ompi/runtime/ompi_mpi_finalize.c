/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2018 Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2006-2014 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2006      University of Houston. All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2011      Sandia National Laboratories. All rights reserved.
 * Copyright (c) 2014-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2016      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 *
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif

#include "opal/mca/event/event.h"
#include "opal/util/output.h"
#include "opal/runtime/opal_progress.h"
#include "opal/mca/base/base.h"
#include "opal/sys/atomic.h"
#include "opal/runtime/opal.h"
#include "opal/util/show_help.h"
#include "opal/mca/mpool/base/base.h"
#include "opal/mca/mpool/base/mpool_base_tree.h"
#include "opal/mca/rcache/base/base.h"
#include "opal/mca/allocator/base/base.h"
#include "opal/mca/pmix/pmix.h"
#include "opal/util/timings.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/errhandler/errcode.h"
#include "ompi/communicator/communicator.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/message/message.h"
#include "ompi/op/op.h"
#include "ompi/file/file.h"
#include "ompi/info/info.h"
#include "ompi/runtime/mpiruntime.h"
#include "ompi/attribute/attribute.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/bml/bml.h"
#include "ompi/mca/pml/base/base.h"
#include "ompi/mca/bml/base/base.h"
#include "ompi/mca/osc/base/base.h"
#include "ompi/mca/coll/base/base.h"
#include "ompi/mca/rte/rte.h"
#include "ompi/mca/rte/base/base.h"
#include "ompi/mca/topo/base/base.h"
#include "ompi/mca/io/io.h"
#include "ompi/mca/io/base/base.h"
#include "ompi/mca/pml/base/pml_base_bsend.h"
#include "ompi/runtime/params.h"
#include "ompi/dpm/dpm.h"
#include "ompi/mpiext/mpiext.h"
#include "ompi/mca/hook/base/base.h"

#if OPAL_ENABLE_FT_CR == 1
#include "ompi/mca/crcp/crcp.h"
#include "ompi/mca/crcp/base/base.h"
#endif
#include "ompi/runtime/ompi_cr.h"

extern bool ompi_enable_timing;

static void fence_cbfunc(int status, void *cbdata)
{
    volatile bool *active = (volatile bool*)cbdata;
    OPAL_ACQUIRE_OBJECT(active);
    *active = false;
    OPAL_POST_OBJECT(active);
}

int ompi_mpi_finalize(void)
{
    int ret = MPI_SUCCESS;
    opal_list_item_t *item;
    ompi_proc_t** procs;
    size_t nprocs;
    volatile bool active;
    uint32_t key;
    ompi_datatype_t * datatype;

    ompi_hook_base_mpi_finalize_top();

    int32_t state = ompi_mpi_state;
    if (state < OMPI_MPI_STATE_INIT_COMPLETED ||
        state >= OMPI_MPI_STATE_FINALIZE_STARTED) {
        /* Note that if we're not initialized or already finalized, we
           cannot raise an MPI exception.  The best that we can do is
           write something to stderr. */
        char hostname[OPAL_MAXHOSTNAMELEN];
        pid_t pid = getpid();
        gethostname(hostname, sizeof(hostname));

        if (state < OMPI_MPI_STATE_INIT_COMPLETED) {
            opal_show_help("help-mpi-runtime.txt",
                           "mpi_finalize: not initialized",
                           true, hostname, pid);
        } else if (state >= OMPI_MPI_STATE_FINALIZE_STARTED) {
            opal_show_help("help-mpi-runtime.txt",
                           "mpi_finalize:invoked_multiple_times",
                           true, hostname, pid);
        }
        return MPI_ERR_OTHER;
    }
    opal_atomic_wmb();
    opal_atomic_swap_32(&ompi_mpi_state, OMPI_MPI_STATE_FINALIZE_STARTED);

    ompi_mpiext_fini();

    /* Per MPI-2:4.8, we have to free MPI_COMM_SELF before doing
       anything else in MPI_FINALIZE (to include setting up such that
       MPI_FINALIZED will return true). */

    if (NULL != ompi_mpi_comm_self.comm.c_keyhash) {
        ompi_attr_delete_all(COMM_ATTR, &ompi_mpi_comm_self,
                             ompi_mpi_comm_self.comm.c_keyhash);
        OBJ_RELEASE(ompi_mpi_comm_self.comm.c_keyhash);
        ompi_mpi_comm_self.comm.c_keyhash = NULL;
    }

    /* Mark that we are past COMM_SELF destruction so that
       MPI_FINALIZED can return an accurate value (per MPI-3.1,
       FINALIZED needs to return FALSE to MPI_FINALIZED until after
       COMM_SELF is destroyed / all the attribute callbacks have been
       invoked) */
    opal_atomic_wmb();
    opal_atomic_swap_32(&ompi_mpi_state,
                        OMPI_MPI_STATE_FINALIZE_PAST_COMM_SELF_DESTRUCT);

    /* As finalize is the last legal MPI call, we are allowed to force the release
     * of the user buffer used for bsend, before going anywhere further.
     */
    (void)mca_pml_base_bsend_detach(NULL, NULL);

#if OPAL_ENABLE_PROGRESS_THREADS == 0
    opal_progress_set_event_flag(OPAL_EVLOOP_ONCE | OPAL_EVLOOP_NONBLOCK);
#endif

    /* Redo ORTE calling opal_progress_event_users_increment() during
       MPI lifetime, to get better latency when not using TCP */
    opal_progress_event_users_increment();

    /* NOTE: MPI-2.1 requires that MPI_FINALIZE is "collective" across
       *all* connected processes.  This only means that all processes
       have to call it.  It does *not* mean that all connected
       processes need to synchronize (either directly or indirectly).

       For example, it is quite easy to construct complicated
       scenarios where one job is "connected" to another job via
       transitivity, but have no direct knowledge of each other.
       Consider the following case: job A spawns job B, and job B
       later spawns job C.  A "connectedness" graph looks something
       like this:

           A <--> B <--> C

       So what are we *supposed* to do in this case?  If job A is
       still connected to B when it calls FINALIZE, should it block
       until jobs B and C also call FINALIZE?

       After lengthy discussions many times over the course of this
       project, the issue was finally decided at the Louisville Feb
       2009 meeting: no.

       Rationale:

       - "Collective" does not mean synchronizing.  It only means that
         every process call it.  Hence, in this scenario, every
         process in A, B, and C must call FINALIZE.

       - KEY POINT: if A calls FINALIZE, then it is erroneous for B or
         C to try to communicate with A again.

       - Hence, OMPI is *correct* to only effect a barrier across each
         jobs' MPI_COMM_WORLD before exiting.  Specifically, if A
         calls FINALIZE long before B or C, it's *correct* if A exits
         at any time (and doesn't notify B or C that it is exiting).

       - Arguably, if B or C do try to communicate with the now-gone
         A, OMPI should try to print a nice error ("you tried to
         communicate with a job that is already gone...") instead of
         segv or other Badness.  However, that is an *extremely*
         difficult problem -- sure, it's easy for A to tell B that it
         is finalizing, but how can A tell C?  A doesn't even know
         about C.  You'd need to construct a "connected" graph in a
         distributed fashion, which is fraught with race conditions,
         etc.

      Hence, our conclusion is: OMPI is *correct* in its current
      behavior (of only doing a barrier across its own COMM_WORLD)
      before exiting.  Any problems that occur are as a result of
      erroneous MPI applications.  We *could* tighten up the erroneous
      cases and ensure that we print nice error messages / don't
      crash, but that is such a difficult problem that we decided we
      have many other, much higher priority issues to handle that deal
      with non-erroneous cases. */

    /* Wait for everyone to reach this point.  This is a PMIx
       barrier instead of an MPI barrier for (at least) two reasons:

       1. An MPI barrier doesn't ensure that all messages have been
          transmitted before exiting (e.g., a BTL can lie and buffer a
          message without actually injecting it to the network, and
          therefore require further calls to that BTL's progress), so
          the possibility of a stranded message exists.

       2. If the MPI communication is using an unreliable transport,
          there's a problem of knowing that everyone has *left* the
          barrier.  E.g., one proc can send its ACK to the barrier
          message to a peer and then leave the barrier, but the ACK
          can get lost and therefore the peer is left in the barrier.

       Point #1 has been known for a long time; point #2 emerged after
       we added the first unreliable BTL to Open MPI and fixed the
       del_procs behavior around May of 2014 (see
       https://svn.open-mpi.org/trac/ompi/ticket/4669#comment:4 for
       more details). */
    if (!ompi_async_mpi_finalize) {
        if (NULL != opal_pmix.fence_nb) {
            active = true;
            OPAL_POST_OBJECT(&active);
            /* Note that use of the non-blocking PMIx fence will
             * allow us to lazily cycle calling
             * opal_progress(), which will allow any other pending
             * communications/actions to complete.  See
             * https://github.com/open-mpi/ompi/issues/1576 for the
             * original bug report. */
            if (OMPI_SUCCESS != (ret = opal_pmix.fence_nb(NULL, 0, fence_cbfunc,
                                                          (void*)&active))) {
                OMPI_ERROR_LOG(ret);
                /* Reset the active flag to false, to avoid waiting for
                 * completion when the fence was failed. */
                active = false;
            }
            OMPI_LAZY_WAIT_FOR_COMPLETION(active);
        } else {
            /* However, we cannot guarantee that the provided PMIx has
             * fence_nb.  If it doesn't, then do the best we can: an MPI
             * barrier on COMM_WORLD (which isn't the best because of the
             * reasons cited above), followed by a blocking PMIx fence
             * (which does not call opal_progress()). */
            ompi_communicator_t *comm = &ompi_mpi_comm_world.comm;
            comm->c_coll->coll_barrier(comm, comm->c_coll->coll_barrier_module);

            if (OMPI_SUCCESS != (ret = opal_pmix.fence(NULL, 0))) {
                OMPI_ERROR_LOG(ret);
            }
        }
    }

    /*
     * Shutdown the Checkpoint/Restart Mech.
     */
    if (OMPI_SUCCESS != (ret = ompi_cr_finalize())) {
        OMPI_ERROR_LOG(ret);
    }

    /* Shut down any bindings-specific issues: C++, F77, F90 */

    /* Remove all memory associated by MPI_REGISTER_DATAREP (per
       MPI-2:9.5.3, there is no way for an MPI application to
       *un*register datareps, but we don't want the OMPI layer causing
       memory leaks). */
    while (NULL != (item = opal_list_remove_first(&ompi_registered_datareps))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&ompi_registered_datareps);

    /* Remove all F90 types from the hash tables */
    OPAL_HASH_TABLE_FOREACH(key, uint32, datatype, &ompi_mpi_f90_integer_hashtable)
        OBJ_RELEASE(datatype);
    OBJ_DESTRUCT(&ompi_mpi_f90_integer_hashtable);
    OPAL_HASH_TABLE_FOREACH(key, uint32, datatype, &ompi_mpi_f90_real_hashtable)
        OBJ_RELEASE(datatype);
    OBJ_DESTRUCT(&ompi_mpi_f90_real_hashtable);
    OPAL_HASH_TABLE_FOREACH(key, uint32, datatype, &ompi_mpi_f90_complex_hashtable)
        OBJ_RELEASE(datatype);
    OBJ_DESTRUCT(&ompi_mpi_f90_complex_hashtable);

    /* Free communication objects */

    /* free file resources */
    if (OMPI_SUCCESS != (ret = ompi_file_finalize())) {
        goto done;
    }

    /* free window resources */
    if (OMPI_SUCCESS != (ret = ompi_win_finalize())) {
        goto done;
    }
    if (OMPI_SUCCESS != (ret = ompi_osc_base_finalize())) {
        goto done;
    }

    /* free communicator resources. this MUST come before finalizing the PML
     * as this will call into the pml */
    if (OMPI_SUCCESS != (ret = ompi_comm_finalize())) {
        goto done;
    }

    /* call del_procs on all allocated procs even though some may not be known
     * to the pml layer. the pml layer is expected to be resilient and ignore
     * any unknown procs. */
    nprocs = 0;
    procs = ompi_proc_get_allocated (&nprocs);
    MCA_PML_CALL(del_procs(procs, nprocs));
    free(procs);

    /* free pml resource */
    if(OMPI_SUCCESS != (ret = mca_pml_base_finalize())) {
        goto done;
    }

    /* free requests */
    if (OMPI_SUCCESS != (ret = ompi_request_finalize())) {
        goto done;
    }

    if (OMPI_SUCCESS != (ret = ompi_message_finalize())) {
        goto done;
    }

    /* If requested, print out a list of memory allocated by ALLOC_MEM
       but not freed by FREE_MEM */
    if (0 != ompi_debug_show_mpi_alloc_mem_leaks) {
        mca_mpool_base_tree_print(ompi_debug_show_mpi_alloc_mem_leaks);
    }

    /* Now that all MPI objects dealing with communications are gone,
       shut down MCA types having to do with communications */
    if (OMPI_SUCCESS != (ret = mca_base_framework_close(&ompi_pml_base_framework) ) ) {
        OMPI_ERROR_LOG(ret);
        goto done;
    }

    /* shut down buffered send code */
    mca_pml_base_bsend_fini();

#if OPAL_ENABLE_FT_CR == 1
    /*
     * Shutdown the CRCP Framework, must happen after PML shutdown
     */
    if (OMPI_SUCCESS != (ret = mca_base_framework_close(&ompi_crcp_base_framework) ) ) {
        OMPI_ERROR_LOG(ret);
        goto done;
    }
#endif

    /* Free secondary resources */

    /* free attr resources */
    if (OMPI_SUCCESS != (ret = ompi_attr_finalize())) {
        goto done;
    }

    /* free group resources */
    if (OMPI_SUCCESS != (ret = ompi_group_finalize())) {
        goto done;
    }

    /* finalize the DPM subsystem */
    if ( OMPI_SUCCESS != (ret = ompi_dpm_finalize())) {
        goto done;
    }

    /* free internal error resources */
    if (OMPI_SUCCESS != (ret = ompi_errcode_intern_finalize())) {
        goto done;
    }

    /* free error code resources */
    if (OMPI_SUCCESS != (ret = ompi_mpi_errcode_finalize())) {
        goto done;
    }

    /* free errhandler resources */
    if (OMPI_SUCCESS != (ret = ompi_errhandler_finalize())) {
        goto done;
    }

    /* Free all other resources */

    /* free op resources */
    if (OMPI_SUCCESS != (ret = ompi_op_finalize())) {
        goto done;
    }

    /* free ddt resources */
    if (OMPI_SUCCESS != (ret = ompi_datatype_finalize())) {
        goto done;
    }

    /* free info resources */
    if (OMPI_SUCCESS != (ret = ompi_mpiinfo_finalize())) {
        goto done;
    }

    /* Close down MCA modules */

    /* io is opened lazily, so it's only necessary to close it if it
       was actually opened */
    if (0 < ompi_io_base_framework.framework_refcnt) {
        /* May have been "opened" multiple times. We want it closed now */
        ompi_io_base_framework.framework_refcnt = 1;

        if (OMPI_SUCCESS != mca_base_framework_close(&ompi_io_base_framework)) {
            goto done;
        }
    }
    (void) mca_base_framework_close(&ompi_topo_base_framework);
    if (OMPI_SUCCESS != (ret = mca_base_framework_close(&ompi_osc_base_framework))) {
        goto done;
    }
    if (OMPI_SUCCESS != (ret = mca_base_framework_close(&ompi_coll_base_framework))) {
        goto done;
    }
    if (OMPI_SUCCESS != (ret = mca_base_framework_close(&ompi_bml_base_framework))) {
        goto done;
    }
    if (OMPI_SUCCESS != (ret = mca_base_framework_close(&opal_mpool_base_framework))) {
        goto done;
    }
    if (OMPI_SUCCESS != (ret = mca_base_framework_close(&opal_rcache_base_framework))) {
        goto done;
    }
    if (OMPI_SUCCESS != (ret = mca_base_framework_close(&opal_allocator_base_framework))) {
        goto done;
    }

    /* free proc resources */
    if ( OMPI_SUCCESS != (ret = ompi_proc_finalize())) {
        goto done;
    }

    if (NULL != ompi_mpi_main_thread) {
        OBJ_RELEASE(ompi_mpi_main_thread);
        ompi_mpi_main_thread = NULL;
    }

    /* Clean up memory/resources from the MPI dynamic process
       functionality checker */
    ompi_mpi_dynamics_finalize();

    /* Leave the RTE */

    if (OMPI_SUCCESS != (ret = ompi_rte_finalize())) {
        goto done;
    }
    ompi_rte_initialized = false;

    /* now close the rte framework */
    if (OMPI_SUCCESS != (ret = mca_base_framework_close(&ompi_rte_base_framework) ) ) {
        OMPI_ERROR_LOG(ret);
        goto done;
    }

    /* Now close the hook framework */
    if (OMPI_SUCCESS != (ret = mca_base_framework_close(&ompi_hook_base_framework) ) ) {
        OMPI_ERROR_LOG(ret);
        goto done;
    }

    if (OPAL_SUCCESS != (ret = opal_finalize_util())) {
        goto done;
    }

    if (0 == opal_initialized) {
        /* if there is no MPI_T_init_thread that has been MPI_T_finalize'd,
         * then be gentle to the app and release all the memory now (instead
         * of the opal library destructor */
        opal_class_finalize();
    }

    /* cleanup environment */
    opal_unsetenv("OMPI_COMMAND", &environ);
    opal_unsetenv("OMPI_ARGV", &environ);

    /* All done */

  done:
    opal_atomic_wmb();
    opal_atomic_swap_32(&ompi_mpi_state, OMPI_MPI_STATE_FINALIZE_COMPLETED);

    ompi_hook_base_mpi_finalize_bottom();

    return ret;
}
