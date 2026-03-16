/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2009 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file */

#ifndef MCA_COLL_SM_EXPORT_H
#define MCA_COLL_SM_EXPORT_H

#include "ompi_config.h"

#include "mpi.h"
#include "ompi/mca/mca.h"
#include "opal/datatype/opal_convertor.h"
#include "opal/mca/common/sm/common_sm.h"
#include "ompi/mca/coll/coll.h"

BEGIN_C_DECLS

/* Attempt to give some sort of progress / fairness if we're blocked
   in an sm collective for a long time: call opal_progress once in a
   great while.  Use a "goto" label for expdiency to exit loops. */
#define SPIN_CONDITION_MAX 100000
#define SPIN_CONDITION(cond, exit_label) \
  do { int i; \
       if (cond) goto exit_label; \
       for (i = 0; i < SPIN_CONDITION_MAX; ++i) { \
           if (cond) { goto exit_label; } \
       } \
       opal_progress(); \
  } while (1); \
  exit_label:

    /**
     * Structure to hold the sm coll component.  First it holds the
     * base coll component, and then holds a bunch of
     * sm-coll-component-specific stuff (e.g., current MCA param
     * values).
     */
    typedef struct mca_coll_sm_component_t {
        /** Base coll component */
        mca_coll_base_component_2_0_0_t super;

        /** MCA parameter: Priority of this component */
        int sm_priority;

        /** MCA parameter: Length of a cache line or page (in bytes) */
        int sm_control_size;

        /** MCA parameter: Number of "in use" flags in each
            communicator's area in the data mpool */
        int sm_comm_num_in_use_flags;

        /** MCA parameter: Number of segments for each communicator in
            the data mpool */
        int sm_comm_num_segments;

        /** MCA parameter: Fragment size for data */
        int sm_fragment_size;

        /** MCA parameter: Degree of tree for tree-based collectives */
        int sm_tree_degree;

        /** MCA parameter: Number of processes to use in the
            calculation of the "info" MCA parameter */
        int sm_info_comm_size;

        /******* end of MCA params ********/

        /** How many fragment segments are protected by a single
            in-use flags.  This is solely so that we can only perform
            the division once and then just use the value without
            having to re-calculate. */
        int sm_segs_per_inuse_flag;
    } mca_coll_sm_component_t;

    /**
     * Structure for representing a node in the tree
     */
    typedef struct mca_coll_sm_tree_node_t {
        /** Arbitrary ID number, starting from 0 */
        int mcstn_id;
        /** Pointer to parent, or NULL if root */
        struct mca_coll_sm_tree_node_t *mcstn_parent;
        /** Number of children, or 0 if a leaf */
        int mcstn_num_children;
        /** Pointer to an array of children, or NULL if 0 ==
            mcstn_num_children */
        struct mca_coll_sm_tree_node_t **mcstn_children;
    } mca_coll_sm_tree_node_t;

    /**
     * Simple structure comprising the "in use" flags.  Contains two
     * members: the number of processes that are currently using this
     * set of segments and the operation number of the current
     * operation.
     */
    typedef struct mca_coll_sm_in_use_flag_t {
        /** Number of processes currently using this set of
            segments */
        volatile uint32_t mcsiuf_num_procs_using;
        /** Must match data->mcb_count */
        volatile uint32_t mcsiuf_operation_count;
    } mca_coll_sm_in_use_flag_t;

    /**
     * Structure containing pointers to various arrays of data in the
     * per-communicator shmem data segment (one of these indexes a
     * single segment in the per-communicator shmem data segment).
     * Nothing is hard-coded because all the array lengths and
     * displacements of the pointers all depend on how many processes
     * are in the communicator.
     */
    typedef struct mca_coll_sm_data_index_t {
        /** Pointer to beginning of control data */
        uint32_t volatile *mcbmi_control;
        /** Pointer to beginning of message fragment data */
        char *mcbmi_data;
    } mca_coll_sm_data_index_t;

    /**
     * Structure for the sm coll module to hang off the communicator.
     * Contains communicator-specific information, including pointers
     * into the per-communicator shmem data data segment for this
     * comm's sm collective operations area.
     */
    typedef struct mca_coll_sm_comm_t {
        /* Meta data that we get back from the common mmap allocation
           function */
        mca_common_sm_module_t *sm_bootstrap_meta;

        /** Pointer to my barrier control pages (odd index pages are
            "in", even index pages are "out") */
        uint32_t *mcb_barrier_control_me;

        /** Pointer to my parent's barrier control pages (will be NULL
            for communicator rank 0; odd index pages are "in", even
            index pages are "out") */
        uint32_t *mcb_barrier_control_parent;

        /** Pointers to my childrens' barrier control pages (they're
            contiguous in memory, so we only point to the base -- the
            number of children is in my entry in the mcb_tree); will
            be NULL if this process has no children (odd index pages
            are "in", even index pages are "out") */
        uint32_t *mcb_barrier_control_children;

        /** Number of barriers that we have executed (i.e., which set
            of barrier buffers to use). */
        int mcb_barrier_count;

        /** "In use" flags indicating which segments are available */
        mca_coll_sm_in_use_flag_t *mcb_in_use_flags;

        /** Array of indexes into the per-communicator shmem data
            segment for control and data fragment passing (containing
            pointers to each segments control and data areas). */
        mca_coll_sm_data_index_t *mcb_data_index;

        /** Array of graph nodes representing the tree used for
            communications */
        mca_coll_sm_tree_node_t *mcb_tree;

        /** Operation number (i.e., which segment number to use) */
        uint32_t mcb_operation_count;
    } mca_coll_sm_comm_t;

    /** Coll sm module */
    typedef struct mca_coll_sm_module_t {
        /** Base module */
	mca_coll_base_module_t super;

        /* Whether this module has been lazily initialized or not yet */
        bool enabled;

        /* Data that hangs off the communicator */
	mca_coll_sm_comm_t *sm_comm_data;

        /* Underlying reduce function and module */
	mca_coll_base_module_reduce_fn_t previous_reduce;
	mca_coll_base_module_t *previous_reduce_module;
    } mca_coll_sm_module_t;
    OBJ_CLASS_DECLARATION(mca_coll_sm_module_t);

    /**
     * Global component instance
     */
    OMPI_MODULE_DECLSPEC extern mca_coll_sm_component_t mca_coll_sm_component;

    /*
     * coll module functions
     */
    int mca_coll_sm_init_query(bool enable_progress_threads,
			       bool enable_mpi_threads);

    mca_coll_base_module_t *
    mca_coll_sm_comm_query(struct ompi_communicator_t *comm, int *priority);

    /* Lazily enable a module (since it involves expensive/slow mmap
       allocation, etc.) */
    int ompi_coll_sm_lazy_enable(mca_coll_base_module_t *module,
                                 struct ompi_communicator_t *comm);

    int mca_coll_sm_allgather_intra(const void *sbuf, int scount,
				    struct ompi_datatype_t *sdtype,
				    void *rbuf, int rcount,
				    struct ompi_datatype_t *rdtype,
				    struct ompi_communicator_t *comm,
				    mca_coll_base_module_t *module);

    int mca_coll_sm_allgatherv_intra(const void *sbuf, int scount,
				     struct ompi_datatype_t *sdtype,
				     void * rbuf, const int *rcounts, const int *disps,
				     struct ompi_datatype_t *rdtype,
				     struct ompi_communicator_t *comm,
				     mca_coll_base_module_t *module);
    int mca_coll_sm_allreduce_intra(const void *sbuf, void *rbuf, int count,
				    struct ompi_datatype_t *dtype,
				    struct ompi_op_t *op,
				    struct ompi_communicator_t *comm,
				    mca_coll_base_module_t *module);
    int mca_coll_sm_alltoall_intra(const void *sbuf, int scount,
				   struct ompi_datatype_t *sdtype,
				   void* rbuf, int rcount,
				   struct ompi_datatype_t *rdtype,
				   struct ompi_communicator_t *comm,
				   mca_coll_base_module_t *module);
    int mca_coll_sm_alltoallv_intra(const void *sbuf, const int *scounts, const int *sdisps,
				    struct ompi_datatype_t *sdtype,
				    void *rbuf, const int *rcounts, const int *rdisps,
				    struct ompi_datatype_t *rdtype,
				    struct ompi_communicator_t *comm,
				    mca_coll_base_module_t *module);
    int mca_coll_sm_alltoallw_intra(const void *sbuf, const int *scounts, const int *sdisps,
				    struct ompi_datatype_t * const *sdtypes,
				    void *rbuf, const int *rcounts, const int *rdisps,
				    struct ompi_datatype_t * const *rdtypes,
				    struct ompi_communicator_t *comm,
				    mca_coll_base_module_t *module);
    int mca_coll_sm_barrier_intra(struct ompi_communicator_t *comm,
				  mca_coll_base_module_t *module);
    int mca_coll_sm_bcast_intra(void *buff, int count,
				struct ompi_datatype_t *datatype,
				int root,
				struct ompi_communicator_t *comm,
				mca_coll_base_module_t *module);
    int mca_coll_sm_bcast_log_intra(void *buff, int count,
				    struct ompi_datatype_t *datatype,
				    int root,
				    struct ompi_communicator_t *comm,
				    mca_coll_base_module_t *module);
    int mca_coll_sm_exscan_intra(const void *sbuf, void *rbuf, int count,
				 struct ompi_datatype_t *dtype,
				 struct ompi_op_t *op,
				 struct ompi_communicator_t *comm,
				 mca_coll_base_module_t *module);
    int mca_coll_sm_gather_intra(void *sbuf, int scount,
				 struct ompi_datatype_t *sdtype, void *rbuf,
				 int rcount, struct ompi_datatype_t *rdtype,
				 int root, struct ompi_communicator_t *comm,
				 mca_coll_base_module_t *module);
    int mca_coll_sm_gatherv_intra(void *sbuf, int scount,
				  struct ompi_datatype_t *sdtype, void *rbuf,
				  int *rcounts, int *disps,
				  struct ompi_datatype_t *rdtype, int root,
				  struct ompi_communicator_t *comm,
				  mca_coll_base_module_t *module);
    int mca_coll_sm_reduce_intra(const void *sbuf, void* rbuf, int count,
				 struct ompi_datatype_t *dtype,
				 struct ompi_op_t *op,
				 int root,
				 struct ompi_communicator_t *comm,
				 mca_coll_base_module_t *module);
    int mca_coll_sm_reduce_log_intra(const void *sbuf, void* rbuf, int count,
				     struct ompi_datatype_t *dtype,
				     struct ompi_op_t *op,
				     int root,
				     struct ompi_communicator_t *comm,
				     mca_coll_base_module_t *module);
    int mca_coll_sm_reduce_scatter_intra(const void *sbuf, void *rbuf,
					 int *rcounts,
					 struct ompi_datatype_t *dtype,
					 struct ompi_op_t *op,
					 struct ompi_communicator_t *comm,
					 mca_coll_base_module_t *module);
    int mca_coll_sm_scan_intra(const void *sbuf, void *rbuf, int count,
			       struct ompi_datatype_t *dtype,
			       struct ompi_op_t *op,
			       struct ompi_communicator_t *comm,
			       mca_coll_base_module_t *module);
    int mca_coll_sm_scatter_intra(const void *sbuf, int scount,
				  struct ompi_datatype_t *sdtype, void *rbuf,
				  int rcount, struct ompi_datatype_t *rdtype,
				  int root, struct ompi_communicator_t *comm,
				  mca_coll_base_module_t *module);
    int mca_coll_sm_scatterv_intra(const void *sbuf, const int *scounts, const int *disps,
				   struct ompi_datatype_t *sdtype,
				   void* rbuf, int rcount,
				   struct ompi_datatype_t *rdtype, int root,
				   struct ompi_communicator_t *comm,
				   mca_coll_base_module_t *module);

    int mca_coll_sm_ft_event(int state);

/**
 * Global variables used in the macros (essentially constants, so
 * these are thread safe)
 */
extern uint32_t mca_coll_sm_one;


/**
 * Macro to setup flag usage
 */
#define FLAG_SETUP(flag_num, flag, data) \
    (flag) = (mca_coll_sm_in_use_flag_t*) \
        (((char *) (data)->mcb_in_use_flags) + \
        ((flag_num) * mca_coll_sm_component.sm_control_size))

/**
 * Macro to wait for the in-use flag to become idle (used by the root)
 */
#define FLAG_WAIT_FOR_IDLE(flag, label) \
    SPIN_CONDITION(0 == (flag)->mcsiuf_num_procs_using, label)

/**
 * Macro to wait for a flag to indicate that it's ready for this
 * operation (used by non-root processes to know when FLAG_SET() has
 * been called)
 */
#define FLAG_WAIT_FOR_OP(flag, op, label) \
    SPIN_CONDITION((op) == flag->mcsiuf_operation_count, label)

/**
 * Macro to set an in-use flag with relevant data to claim it
 */
#define FLAG_RETAIN(flag, num_procs, op_count) \
    (flag)->mcsiuf_num_procs_using = (num_procs); \
    (flag)->mcsiuf_operation_count = (op_count)

/**
 * Macro to release an in-use flag from this process
 */
#define FLAG_RELEASE(flag) \
    opal_atomic_add(&(flag)->mcsiuf_num_procs_using, -1)

/**
 * Macro to copy a single segment in from a user buffer to a shared
 * segment
 */
#define COPY_FRAGMENT_IN(convertor, index, rank, iov, max_data) \
    (iov).iov_base = \
        (index)->mcbmi_data + \
        ((rank) * mca_coll_sm_component.sm_fragment_size); \
    (iov).iov_len = (max_data); \
    opal_convertor_pack(&(convertor), &(iov), &mca_coll_sm_one, \
                        &(max_data) )

/**
 * Macro to copy a single segment out from a shared segment to a user
 * buffer
 */
#define COPY_FRAGMENT_OUT(convertor, src_rank, index, iov, max_data) \
    (iov).iov_base = (((char*) (index)->mcbmi_data) + \
                       ((src_rank) * (mca_coll_sm_component.sm_fragment_size))); \
    (iov).iov_len = (max_data); \
    opal_convertor_unpack(&(convertor), &(iov), &mca_coll_sm_one, \
                          &(max_data) )

/**
 * Macro to memcpy a fragment between one shared segment and another
 */
#define COPY_FRAGMENT_BETWEEN(src_rank, dest_rank, index, len) \
    memcpy(((index)->mcbmi_data + \
            ((dest_rank) * mca_coll_sm_component.sm_fragment_size)), \
           ((index)->mcbmi_data + \
            ((src_rank) * \
             mca_coll_sm_component.sm_fragment_size)), \
           (len))

/**
 * Macro to tell children that a segment is ready (normalize
 * the child's ID based on the shift used to calculate the "me" node
 * in the tree).  Used in fan out opertations.
 */
#define PARENT_NOTIFY_CHILDREN(children, num_children, index, value) \
    do { \
        for (i = 0; i < (num_children); ++i) { \
            *((size_t*) \
              (((char*) index->mcbmi_control) + \
               (mca_coll_sm_component.sm_control_size * \
                (((children)[i]->mcstn_id + root) % size)))) = (value); \
        } \
    } while (0)

/**
 * Macro for childen to wait for parent notification (use real rank).
 * Save the value passed and then reset it when done.  Used in fan out
 * operations.
 */
#define CHILD_WAIT_FOR_NOTIFY(rank, index, value, label) \
    do { \
        uint32_t volatile *ptr = ((uint32_t*) \
                                  (((char*) index->mcbmi_control) + \
                                   ((rank) * mca_coll_sm_component.sm_control_size))); \
        SPIN_CONDITION(0 != *ptr, label); \
        (value) = *ptr; \
        *ptr = 0; \
    } while (0)

/**
 * Macro for children to tell parent that the data is ready in their
 * segment.  Used for fan in operations.
 */
#define CHILD_NOTIFY_PARENT(child_rank, parent_rank, index, value) \
    ((size_t volatile *) \
     (((char*) (index)->mcbmi_control) + \
      (mca_coll_sm_component.sm_control_size * \
       (parent_rank))))[(child_rank)] = (value)

/**
 * Macro for parent to wait for a specific child to tell it that the
 * data is in the child's segment.  Save the value when done.  Used
 * for fan in operations.
 */
#define PARENT_WAIT_FOR_NOTIFY_SPECIFIC(child_rank, parent_rank, index, value, label) \
    do { \
        size_t volatile *ptr = ((size_t volatile *) \
                                (((char*) index->mcbmi_control) + \
                                 (mca_coll_sm_component.sm_control_size * \
                                  (parent_rank)))) + child_rank; \
        SPIN_CONDITION(0 != *ptr, label); \
        (value) = *ptr; \
        *ptr = 0; \
    } while (0)

END_C_DECLS

#endif /* MCA_COLL_SM_EXPORT_H */
