/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2017 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2009-2013 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2012 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2014-2015 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2015      Intel, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Warning: this is not for the faint of heart -- don't even bother
 * reading this source code if you don't have a strong understanding
 * of nested data structures and pointer math (remember that
 * associativity and order of C operations is *critical* in terms of
 * pointer math!).
 */

#include "ompi_config.h"

#include <stdio.h>
#include <string.h>
#ifdef HAVE_SCHED_H
#include <sched.h>
#endif
#include <sys/types.h>
#ifdef HAVE_SYS_MMAN_H
#include <sys/mman.h>
#endif  /* HAVE_SYS_MMAN_H */
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */

#include "mpi.h"
#include "opal_stdint.h"
#include "opal/mca/hwloc/base/base.h"
#include "opal/util/os_path.h"

#include "ompi/communicator/communicator.h"
#include "ompi/group/group.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/base.h"
#include "ompi/mca/rte/rte.h"
#include "ompi/proc/proc.h"
#include "coll_sm.h"

#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/pml.h"

/*
 * Global variables
 */
uint32_t mca_coll_sm_one = 1;


/*
 * Local functions
 */
static int sm_module_enable(mca_coll_base_module_t *module,
                          struct ompi_communicator_t *comm);
static int bootstrap_comm(ompi_communicator_t *comm,
                          mca_coll_sm_module_t *module);
static int mca_coll_sm_module_disable(mca_coll_base_module_t *module,
                          struct ompi_communicator_t *comm);

/*
 * Module constructor
 */
static void mca_coll_sm_module_construct(mca_coll_sm_module_t *module)
{
    module->enabled = false;
    module->sm_comm_data = NULL;
    module->previous_reduce = NULL;
    module->previous_reduce_module = NULL;
    module->super.coll_module_disable = mca_coll_sm_module_disable;
}

/*
 * Module destructor
 */
static void mca_coll_sm_module_destruct(mca_coll_sm_module_t *module)
{
    mca_coll_sm_comm_t *c = module->sm_comm_data;

    if (NULL != c) {
        /* Munmap the per-communicator shmem data segment */
        if (NULL != c->sm_bootstrap_meta) {
            /* Ignore any errors -- what are we going to do about
               them? */
            mca_common_sm_fini(c->sm_bootstrap_meta);
            OBJ_RELEASE(c->sm_bootstrap_meta);
        }
        free(c);
    }

    /* It should always be non-NULL, but just in case */
    if (NULL != module->previous_reduce_module) {
        OBJ_RELEASE(module->previous_reduce_module);
    }

    module->enabled = false;
}

/*
 * Module disable
 */
static int mca_coll_sm_module_disable(mca_coll_base_module_t *module, struct ompi_communicator_t *comm)
{
    mca_coll_sm_module_t *sm_module = (mca_coll_sm_module_t*) module;
    if (NULL != sm_module->previous_reduce_module) {
	sm_module->previous_reduce = NULL;
        OBJ_RELEASE(sm_module->previous_reduce_module);
	sm_module->previous_reduce_module = NULL;
    }
    return OMPI_SUCCESS;
}


OBJ_CLASS_INSTANCE(mca_coll_sm_module_t,
                   mca_coll_base_module_t,
                   mca_coll_sm_module_construct,
                   mca_coll_sm_module_destruct);

/*
 * Initial query function that is invoked during MPI_INIT, allowing
 * this component to disqualify itself if it doesn't support the
 * required level of thread support.  This function is invoked exactly
 * once.
 */
int mca_coll_sm_init_query(bool enable_progress_threads,
                           bool enable_mpi_threads)
{
    /* if no session directory was created, then we cannot be used */
    if (NULL == ompi_process_info.job_session_dir) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    /* Don't do much here because we don't really want to allocate any
       shared memory until this component is selected to be used. */
    opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                        "coll:sm:init_query: pick me! pick me!");
    return OMPI_SUCCESS;
}


/*
 * Invoked when there's a new communicator that has been created.
 * Look at the communicator and decide which set of functions and
 * priority we want to return.
 */
mca_coll_base_module_t *
mca_coll_sm_comm_query(struct ompi_communicator_t *comm, int *priority)
{
    mca_coll_sm_module_t *sm_module;

    /* If we're intercomm, or if there's only one process in the
       communicator, or if not all the processes in the communicator
       are not on this node, then we don't want to run */
    if (OMPI_COMM_IS_INTER(comm) || 1 == ompi_comm_size(comm) || ompi_group_have_remote_peers (comm->c_local_group)) {
        opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                            "coll:sm:comm_query (%d/%s): intercomm, comm is too small, or not all peers local; disqualifying myself", comm->c_contextid, comm->c_name);
	return NULL;
    }

    /* Get the priority level attached to this module. If priority is less
     * than or equal to 0, then the module is unavailable. */
    *priority = mca_coll_sm_component.sm_priority;
    if (mca_coll_sm_component.sm_priority <= 0) {
        opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                            "coll:sm:comm_query (%d/%s): priority too low; disqualifying myself", comm->c_contextid, comm->c_name);
	return NULL;
    }

    sm_module = OBJ_NEW(mca_coll_sm_module_t);
    if (NULL == sm_module) {
        return NULL;
    }

    /* All is good -- return a module */
    sm_module->super.coll_module_enable = sm_module_enable;
    sm_module->super.ft_event        = mca_coll_sm_ft_event;
    sm_module->super.coll_allgather  = NULL;
    sm_module->super.coll_allgatherv = NULL;
    sm_module->super.coll_allreduce  = mca_coll_sm_allreduce_intra;
    sm_module->super.coll_alltoall   = NULL;
    sm_module->super.coll_alltoallv  = NULL;
    sm_module->super.coll_alltoallw  = NULL;
    sm_module->super.coll_barrier    = mca_coll_sm_barrier_intra;
    sm_module->super.coll_bcast      = mca_coll_sm_bcast_intra;
    sm_module->super.coll_exscan     = NULL;
    sm_module->super.coll_gather     = NULL;
    sm_module->super.coll_gatherv    = NULL;
    sm_module->super.coll_reduce     = mca_coll_sm_reduce_intra;
    sm_module->super.coll_reduce_scatter = NULL;
    sm_module->super.coll_scan       = NULL;
    sm_module->super.coll_scatter    = NULL;
    sm_module->super.coll_scatterv   = NULL;

    opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                        "coll:sm:comm_query (%d/%s): pick me! pick me!",
                        comm->c_contextid, comm->c_name);
    return &(sm_module->super);
}


/*
 * Init module on the communicator
 */
static int sm_module_enable(mca_coll_base_module_t *module,
                            struct ompi_communicator_t *comm)
{
    if (NULL == comm->c_coll->coll_reduce ||
        NULL == comm->c_coll->coll_reduce_module) {
        opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                            "coll:sm:enable (%d/%s): no underlying reduce; disqualifying myself",
                            comm->c_contextid, comm->c_name);
        return OMPI_ERROR;
    }

    /* We do everything lazily in ompi_coll_sm_enable() */
    return OMPI_SUCCESS;
}

int ompi_coll_sm_lazy_enable(mca_coll_base_module_t *module,
                             struct ompi_communicator_t *comm)
{
    int i, j, root, ret;
    int rank = ompi_comm_rank(comm);
    int size = ompi_comm_size(comm);
    mca_coll_sm_module_t *sm_module = (mca_coll_sm_module_t*) module;
    mca_coll_sm_comm_t *data = NULL;
    size_t control_size, frag_size;
    mca_coll_sm_component_t *c = &mca_coll_sm_component;
    opal_hwloc_base_memory_segment_t *maffinity;
    int parent, min_child, num_children;
    unsigned char *base = NULL;
    const int num_barrier_buffers = 2;

    /* Just make sure we haven't been here already */
    if (sm_module->enabled) {
        return OMPI_SUCCESS;
    }
    sm_module->enabled = true;

    /* Get some space to setup memory affinity (just easier to try to
       alloc here to handle the error case) */
    maffinity = (opal_hwloc_base_memory_segment_t*)
        malloc(sizeof(opal_hwloc_base_memory_segment_t) *
               c->sm_comm_num_segments * 3);
    if (NULL == maffinity) {
        opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                            "coll:sm:enable (%d/%s): malloc failed (1)",
                            comm->c_contextid, comm->c_name);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    /* Allocate data to hang off the communicator.  The memory we
       alloc will be laid out as follows:

       1. mca_coll_base_comm_t
       2. array of num_segments mca_coll_base_mpool_index_t instances
          (pointed to by the array in 2)
       3. array of ompi_comm_size(comm) mca_coll_sm_tree_node_t
          instances
       4. array of sm_tree_degree pointers to other tree nodes (i.e.,
          this nodes' children) for each instance of
          mca_coll_sm_tree_node_t
    */
    sm_module->sm_comm_data = data = (mca_coll_sm_comm_t*)
        malloc(sizeof(mca_coll_sm_comm_t) +
               (c->sm_comm_num_segments *
                sizeof(mca_coll_sm_data_index_t)) +
               (size *
                (sizeof(mca_coll_sm_tree_node_t) +
                 (sizeof(mca_coll_sm_tree_node_t*) * c->sm_tree_degree))));
    if (NULL == data) {
        free(maffinity);
        opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                            "coll:sm:enable (%d/%s): malloc failed (2)",
                            comm->c_contextid, comm->c_name);
        return OMPI_ERR_TEMP_OUT_OF_RESOURCE;
    }
    data->mcb_operation_count = 0;

    /* Setup #2: set the array to point immediately beyond the
       mca_coll_base_comm_t */
    data->mcb_data_index = (mca_coll_sm_data_index_t*) (data + 1);
    /* Setup array of pointers for #3 */
    data->mcb_tree = (mca_coll_sm_tree_node_t*)
        (data->mcb_data_index + c->sm_comm_num_segments);
    /* Finally, setup the array of children pointers in the instances
       in #5 to point to their corresponding arrays in #6 */
    data->mcb_tree[0].mcstn_children = (mca_coll_sm_tree_node_t**)
        (data->mcb_tree + size);
    for (i = 1; i < size; ++i) {
        data->mcb_tree[i].mcstn_children =
            data->mcb_tree[i - 1].mcstn_children + c->sm_tree_degree;
    }

    /* Pre-compute a tree for a given number of processes and degree.
       We'll re-use this tree for all possible values of root (i.e.,
       shift everyone's process to be the "0"/root in this tree. */
    for (root = 0; root < size; ++root) {
        parent = (root - 1) / mca_coll_sm_component.sm_tree_degree;
        num_children = mca_coll_sm_component.sm_tree_degree;

        /* Do we have children?  If so, how many? */

        if ((root * num_children) + 1 >= size) {
            /* Leaves */
            min_child = -1;
            num_children = 0;
        } else {
            /* Interior nodes */
            int max_child;
            min_child = root * num_children + 1;
            max_child = root * num_children + num_children;
            if (max_child >= size) {
                max_child = size - 1;
            }
            num_children = max_child - min_child + 1;
        }

        /* Save the values */
        data->mcb_tree[root].mcstn_id = root;
        if (root == 0 && parent == 0) {
            data->mcb_tree[root].mcstn_parent = NULL;
        } else {
            data->mcb_tree[root].mcstn_parent = &data->mcb_tree[parent];
        }
        data->mcb_tree[root].mcstn_num_children = num_children;
        for (i = 0; i < c->sm_tree_degree; ++i) {
            data->mcb_tree[root].mcstn_children[i] =
                (i < num_children) ?
                &data->mcb_tree[min_child + i] : NULL;
        }
    }

    /* Attach to this communicator's shmem data segment */
    if (OMPI_SUCCESS != (ret = bootstrap_comm(comm, sm_module))) {
        free(data);
        free(maffinity);
        sm_module->sm_comm_data = NULL;
        return ret;
    }

    /* Once the communicator is bootstrapped, setup the pointers into
       the per-communicator shmem data segment.  First, setup the
       barrier buffers.  There are 2 sets of barrier buffers (because
       there can never be more than one outstanding barrier occuring
       at any timie).  Setup pointers to my control buffers, my
       parents, and [the beginning of] my children (note that the
       children are contiguous, so having the first pointer and the
       num_children from the mcb_tree data is sufficient). */
    control_size = c->sm_control_size;
    base = data->sm_bootstrap_meta->module_data_addr;
    data->mcb_barrier_control_me = (uint32_t*)
        (base + (rank * control_size * num_barrier_buffers * 2));
    if (data->mcb_tree[rank].mcstn_parent) {
        data->mcb_barrier_control_parent = (uint32_t*)
            (base +
             (data->mcb_tree[rank].mcstn_parent->mcstn_id * control_size *
              num_barrier_buffers * 2));
    } else {
        data->mcb_barrier_control_parent = NULL;
    }
    if (data->mcb_tree[rank].mcstn_num_children > 0) {
        data->mcb_barrier_control_children = (uint32_t*)
            (base +
             (data->mcb_tree[rank].mcstn_children[0]->mcstn_id * control_size *
              num_barrier_buffers * 2));
    } else {
        data->mcb_barrier_control_children = NULL;
    }
    data->mcb_barrier_count = 0;

    /* Next, setup the pointer to the in-use flags.  The number of
       segments will be an even multiple of the number of in-use
       flags. */
    base += (c->sm_control_size * size * num_barrier_buffers * 2);
    data->mcb_in_use_flags = (mca_coll_sm_in_use_flag_t*) base;

    /* All things being equal, if we're rank 0, then make the in-use
       flags be local (memory affinity).  Then zero them all out so
       that they're marked as unused. */
    j = 0;
    if (0 == rank) {
        maffinity[j].mbs_start_addr = base;
        maffinity[j].mbs_len = c->sm_control_size *
            c->sm_comm_num_in_use_flags;
        /* Set the op counts to 1 (actually any nonzero value will do)
           so that the first time children/leaf processes come
           through, they don't see a value of 0 and think that the
           root/parent has already set the count to their op number
           (i.e., 0 is the first op count value). */
        for (i = 0; i < mca_coll_sm_component.sm_comm_num_in_use_flags; ++i) {
            ((mca_coll_sm_in_use_flag_t *)base)[i].mcsiuf_operation_count = 1;
            ((mca_coll_sm_in_use_flag_t *)base)[i].mcsiuf_num_procs_using = 0;
        }
        ++j;
    }

    /* Next, setup pointers to the control and data portions of the
       segments, as well as to the relevant in-use flags. */
    base += (c->sm_comm_num_in_use_flags * c->sm_control_size);
    control_size = size * c->sm_control_size;
    frag_size = size * c->sm_fragment_size;
    for (i = 0; i < c->sm_comm_num_segments; ++i) {
        data->mcb_data_index[i].mcbmi_control = (uint32_t*)
            (base + (i * (control_size + frag_size)));
        data->mcb_data_index[i].mcbmi_data =
            (((char*) data->mcb_data_index[i].mcbmi_control) +
             control_size);

        /* Memory affinity: control */

        maffinity[j].mbs_len = c->sm_control_size;
        maffinity[j].mbs_start_addr = (void *)
            (((char*) data->mcb_data_index[i].mcbmi_control) +
             (rank * c->sm_control_size));
        ++j;

        /* Memory affinity: data */

        maffinity[j].mbs_len = c->sm_fragment_size;
        maffinity[j].mbs_start_addr =
            ((char*) data->mcb_data_index[i].mcbmi_data) +
            (rank * c->sm_control_size);
        ++j;
    }

    /* Setup memory affinity so that the pages that belong to this
       process are local to this process */
    opal_hwloc_base_memory_set(maffinity, j);
    free(maffinity);

    /* Zero out the control structures that belong to this process */
    memset(data->mcb_barrier_control_me, 0,
           num_barrier_buffers * 2 * c->sm_control_size);
    for (i = 0; i < c->sm_comm_num_segments; ++i) {
        memset((void *) data->mcb_data_index[i].mcbmi_control, 0,
               c->sm_control_size);
    }

    /* Save previous component's reduce information */
    sm_module->previous_reduce = comm->c_coll->coll_reduce;
    sm_module->previous_reduce_module = comm->c_coll->coll_reduce_module;
    OBJ_RETAIN(sm_module->previous_reduce_module);

    /* Indicate that we have successfully attached and setup */
    opal_atomic_add (&(data->sm_bootstrap_meta->module_seg->seg_inited), 1);

    /* Wait for everyone in this communicator to attach and setup */
    opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                        "coll:sm:enable (%d/%s): waiting for peers to attach",
                        comm->c_contextid, comm->c_name);
    SPIN_CONDITION(size == data->sm_bootstrap_meta->module_seg->seg_inited, seg_init_exit);

    /* Once we're all here, remove the mmap file; it's not needed anymore */
    if (0 == rank) {
        unlink(data->sm_bootstrap_meta->shmem_ds.seg_name);
        opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                            "coll:sm:enable (%d/%s): removed mmap file %s",
                            comm->c_contextid, comm->c_name,
                            data->sm_bootstrap_meta->shmem_ds.seg_name);
    }

    /* All done */

    opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                        "coll:sm:enable (%d/%s): success!",
                        comm->c_contextid, comm->c_name);
    return OMPI_SUCCESS;
}

static int bootstrap_comm(ompi_communicator_t *comm,
                          mca_coll_sm_module_t *module)
{
    int i;
    char *shortpath, *fullpath;
    mca_coll_sm_component_t *c = &mca_coll_sm_component;
    mca_coll_sm_comm_t *data = module->sm_comm_data;
    int comm_size = ompi_comm_size(comm);
    int num_segments = c->sm_comm_num_segments;
    int num_in_use = c->sm_comm_num_in_use_flags;
    int frag_size = c->sm_fragment_size;
    int control_size = c->sm_control_size;
    ompi_process_name_t *lowest_name = NULL;
    size_t size;
    ompi_proc_t *proc;

    /* Make the rendezvous filename for this communicators shmem data
       segment.  The CID is not guaranteed to be unique among all
       procs on this node, so also pair it with the PID of the proc
       with the lowest ORTE name to form a unique filename. */
    proc = ompi_group_peer_lookup(comm->c_local_group, 0);
    lowest_name = OMPI_CAST_RTE_NAME(&proc->super.proc_name);
    for (i = 1; i < comm_size; ++i) {
        proc = ompi_group_peer_lookup(comm->c_local_group, i);
        if (ompi_rte_compare_name_fields(OMPI_RTE_CMP_ALL,
                                          OMPI_CAST_RTE_NAME(&proc->super.proc_name),
                                          lowest_name) < 0) {
            lowest_name = OMPI_CAST_RTE_NAME(&proc->super.proc_name);
        }
    }
    asprintf(&shortpath, "coll-sm-cid-%d-name-%s.mmap", comm->c_contextid,
             OMPI_NAME_PRINT(lowest_name));
    if (NULL == shortpath) {
        opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                            "coll:sm:enable:bootstrap comm (%d/%s): asprintf failed",
                            comm->c_contextid, comm->c_name);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    fullpath = opal_os_path(false, ompi_process_info.job_session_dir,
                            shortpath, NULL);
    free(shortpath);
    if (NULL == fullpath) {
        opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                            "coll:sm:enable:bootstrap comm (%d/%s): opal_os_path failed",
                            comm->c_contextid, comm->c_name);
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    /* Calculate how much space we need in the per-communicator shmem
       data segment.  There are several values to add:

       - size of the barrier data (2 of these):
           - fan-in data (num_procs * control_size)
           - fan-out data (num_procs * control_size)
       - size of the "in use" buffers:
           - num_in_use_buffers * control_size
       - size of the message fragment area (one for each segment):
           - control (num_procs * control_size)
           - fragment data (num_procs * (frag_size))

       So it's:

           barrier: 2 * control_size + 2 * control_size
           in use:  num_in_use * control_size
           control: num_segments * (num_procs * control_size * 2 +
                                    num_procs * control_size)
           message: num_segments * (num_procs * frag_size)
     */

    size = 4 * control_size +
        (num_in_use * control_size) +
        (num_segments * (comm_size * control_size * 2)) +
        (num_segments * (comm_size * frag_size));
    opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                        "coll:sm:enable:bootstrap comm (%d/%s): attaching to %" PRIsize_t " byte mmap: %s",
                        comm->c_contextid, comm->c_name, size, fullpath);
    if (0 == ompi_comm_rank (comm)) {
        data->sm_bootstrap_meta = mca_common_sm_module_create_and_attach (size, fullpath, sizeof(mca_common_sm_seg_header_t), 8);
        if (NULL == data->sm_bootstrap_meta) {
            opal_output_verbose(10, ompi_coll_base_framework.framework_output,
                "coll:sm:enable:bootstrap comm (%d/%s): mca_common_sm_init_group failed",
                comm->c_contextid, comm->c_name);
            free(fullpath);
            return OMPI_ERR_OUT_OF_RESOURCE;
        }

        for (int i = 1 ; i < ompi_comm_size (comm) ; ++i) {
            MCA_PML_CALL(send(&data->sm_bootstrap_meta->shmem_ds, sizeof (data->sm_bootstrap_meta->shmem_ds), MPI_BYTE,
                         i, MCA_COLL_BASE_TAG_BCAST, MCA_PML_BASE_SEND_STANDARD, comm));
        }
    } else {
        opal_shmem_ds_t shmem_ds;
        MCA_PML_CALL(recv(&shmem_ds, sizeof (shmem_ds), MPI_BYTE, 0, MCA_COLL_BASE_TAG_BCAST, comm, MPI_STATUS_IGNORE));
        data->sm_bootstrap_meta = mca_common_sm_module_attach (&shmem_ds, sizeof(mca_common_sm_seg_header_t), 8);
    }

    /* All done */
    free(fullpath);
    return OMPI_SUCCESS;
}


int mca_coll_sm_ft_event(int state) {
    if(OPAL_CRS_CHECKPOINT == state) {
        ;
    }
    else if(OPAL_CRS_CONTINUE == state) {
        ;
    }
    else if(OPAL_CRS_RESTART == state) {
        ;
    }
    else if(OPAL_CRS_TERM == state ) {
        ;
    }
    else {
        ;
    }

    return OMPI_SUCCESS;
}
