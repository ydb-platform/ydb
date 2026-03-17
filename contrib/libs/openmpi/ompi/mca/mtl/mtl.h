/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2012      Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2017      Intel, Inc. All rights reserved
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Matching Transport Layer
 *
 * The Matching Transport Layer (MTL) provides device-layer support
 * for transfer of MPI point-to-point messages over devices that
 * support hardware / library message matching.  This layer is used
 * with the MTL PML component to provide lowest latency and highest
 * bandwidth on given architectures.  Features found in other PML
 * interfaces, such as message fragmenting, multi-device support, and
 * NIC failover are not provided by the upper layers.
 *
 * In general, this interface should not be used for transport layer
 * support.  Instead, the BTL interface should be used.  The BTL
 * interface allows for multiplexing between multiple users
 * (point-to-point, one-sided, etc.) and provides many features not
 * found in this interface (RDMA from arbitrary buffers, active
 * messaging, reasonable pinned memory caching, etc.)
 */

#ifndef OMPI_MTL_H
#define OMPI_MTL_H

#include "ompi_config.h"
#include "mpi.h" /* needed for MPI_ANY_TAG */
#include "ompi/mca/mca.h"
#include "ompi/mca/pml/pml_constants.h" /* for send_mode enum */
#include "ompi/request/request.h"

BEGIN_C_DECLS

struct ompi_request_t;
struct opal_convertor_t;

struct mca_mtl_base_module_t;

struct mca_mtl_request_t {
    /** pointer to associated ompi_request_t */
    struct ompi_request_t *ompi_req;
    void (*completion_callback)(struct mca_mtl_request_t* mtl_request);
};
typedef struct mca_mtl_request_t mca_mtl_request_t;


/**
 * MTL module flags
 */
#define MCA_MTL_BASE_FLAG_REQUIRE_WORLD 0x00000001
#if OPAL_CUDA_SUPPORT
#define MCA_MTL_BASE_FLAG_CUDA_INIT_DISABLE 0x00000002
#endif

/**
 * Initialization routine for MTL component
 *
 * Initialization routine for MTL component.  This function should
 * allocate resources for communication and try to do all local setup.
 * It should not attempt to contact it's peers, as that should be
 * done at add_procs time.  Contact information should be published
 * during this initialization function.  It will be made available
 * during add_procs().
 *
 * @param enable_progress_threads (IN) Progress threads have been
 *                  enabled by the user and the component must be
 *                  capable of making asycnhronous progress (either
 *                  with its own thread, with the kernel, or with
 *                  the event library.
 * @param enable_mpi_threads (IN) MPI threads have been enabled by the
 *                  user and the component must be capable of coping
 *                  with threads.  If the component can cope with
 *                  MPI_THREAD_MULTIPLE, enable_mpi_thread_multiple
 *                  should be set to true.  Otherwise, it is assumed
 *                  that only THREAD_FUNNELLED and THREAD_SERIALIZED
 *                  can be used.
 * @param enable_mpi_thread_multiple (OUT) Component does / does not
 *                  support MPI_THREAD_MULTIPLE.  This variable only
 *                  needs to be set if enable_mpi_threads is true.
 *                  Otherwise, the return value will be ignored.
 *
 * @retval NULL     component can not operate on the current machine
 * @retval non-NULL component interface function
 */
typedef struct mca_mtl_base_module_t*
(*mca_mtl_base_component_init_fn_t)(bool enable_progress_threads,
                                    bool enable_mpi_threads);


struct mca_mtl_base_component_2_0_0_t {
  mca_base_component_t mtl_version;
  mca_base_component_data_t mtl_data;
  mca_mtl_base_component_init_fn_t mtl_init;
};
typedef struct mca_mtl_base_component_2_0_0_t mca_mtl_base_component_2_0_0_t;
typedef struct mca_mtl_base_component_2_0_0_t mca_mtl_base_component_t;


/**
 * MCA->MTL Clean up any resources held by MTL module
 *
 * Opposite of module_init.  Called when communication will no longer
 * be necessary.  ussually this is during MPI_FINALIZE, but it can be
 * earlier if the component was not selected to run.  Assuming
 * module_init was called, finalize will always be called before the
 * component_close function is called.
 *
 * @param mtl (IN)   MTL module returned from call to initialize
 *
 * @retval OMPI_SUCCESS cleanup finished successfully
 * @retval other        failure during cleanup
 *
 */
typedef int (*mca_mtl_base_module_finalize_fn_t)(struct mca_mtl_base_module_t* mtl);


/**
 * PML->MTL notification of change in the process list.
 *
 * The mca_mtl_base_module_add_procs_fn_t() is used by the PML to
 * notify the MTL that new processes are connected to the current
 * process.  Any addressing information exported by the peer via the
 * ompi_modex_send() function should be available during this
 * call via the corresponding ompi_modex_recv() function. The
 * MTL may utilize this information to determine reachability of each
 * peer process.
 *
 * It is an error for a proc to not be reachable by the given MTL, and
 * an error should be returned if that case is detected.  If a MTL
 * requires per-endpoint data, it must handle storage, either using a
 * static endpoint tag (MTL is the default tag that should generally
 * be used) or a dynamic endpoint tag (although it should be noted
 * that OMPI can be built without dynamic endpoint tag support).
 *
 * @param mtl (IN)            MTL module
 * @param nprocs (IN)         Number of processes
 * @param procs (IN)          Set of processes
 *
 * @retval OMPI_SUCCESS successfully connected to processes
 * @retval other failure during setup
 */
typedef int (*mca_mtl_base_module_add_procs_fn_t)(
                            struct mca_mtl_base_module_t* mtl,
                            size_t nprocs,
                            struct ompi_proc_t** procs);


/**
 * Notification of change to the process list.
 *
 * When the process list changes, the PML notifies the MTL of the
 * change, to provide the opportunity to cleanup or release any
 * resources associated with the peer.  The MTL is responsible for
 * releasing any memory associated with the endpoint data it may have
 * stored during add_procs().
 *
 * @param mtl (IN)     MTL module
 * @param nprocs (IN)  Number of processes
 * @param proc (IN)    Set of processes
 * @param peer (IN)    Set of peer addressing information.
 *
 * @return             Status indicating if cleanup was successful
 */
typedef int (*mca_mtl_base_module_del_procs_fn_t)(
                            struct mca_mtl_base_module_t* mtl,
                            size_t nprocs,
                            struct ompi_proc_t** procs);


/**
 * Blocking send to peer
 *
 * Blocking send (Call should not return until the user buffer may be
 * used again).  Standard MPI semantics must be met by this call, as
 * mandated in the mode argument.  There is one special mode argument,
 * MCA_PML_BASE_SEND_COMPLETE, which requires local completion before
 * the function can return.  This is an optimization for coillective
 * routines that can otherwise lead to degenerate performance for
 * broadcast-based collectives.
 *
 * @param comm (IN)      Communicator used for operation
 * @param dest (IN)      Destination rank for send (relative to comm)
 * @param tag (IN)       MPI tag used for sending.  See note below.
 * @param convertor (IN) Datatype convertor describing send datatype.
 *                       Already prepared for send.
 * @param mode (IN)      Mode for send operation
 *
 * @return               OMPI_SUCCESS or error value
 *
 * \note Open MPI is built around non-blocking operations.  This
 * function is provided for networks where progressing events outside
 * of point-to-point (for example, collectives, I/O, one-sided) can
 * occur without a progress function regularily being triggered.
 *
 * \note While MPI does not allow users to specify negative tags, they
 * are used internally in Open MPI to provide a unique channel for
 * collective operations.  Therefore, the MTL can *not* cause an error
 * if a negative tag is used.
 */
typedef int (*mca_mtl_base_module_send_fn_t)(
                          struct mca_mtl_base_module_t* mtl,
                          struct ompi_communicator_t *comm,
                          int dest,
                          int tag,
                          struct opal_convertor_t *convertor,
                          mca_pml_base_send_mode_t mode);


/**
 * Non-blocking send to peer
 *
 * Non-blocking send to peer.  Standard MPI semantics must be met by
 * this call, as mandated in the mode argument.  There is one special
 * mode argument, MCA_PML_BASE_SEND_COMPLETE, which requires local
 * completion before the request is marked as complete.
 *
 * The PML will handle creation of the request, leaving the number of
 * bytes requested in the module structure available for the MTL
 * directly after the ompi_request_t structure.  The PML will handle
 * proper destruction of the request once it can safely be destructed
 * (it has been completed and freeed by a call to REQUEST_FReE or
 * TEST/WAIT).  The MTL should remove all resources associated with
 * the request when it is marked as completed.
 *
 * @param comm (IN)      Communicator used for operation
 * @param dest (IN)      Destination rank for send (relative to comm)
 * @param tag (IN)       MPI tag used for sending.  See note below.
 * @param convertor (IN) Datatype convertor describing send datatype.
 *                       Already prepared for send.
 * @param mode (IN)      Mode for send operation (see pml.h)
 * @param blocking (IN)  True if the call originated from a blocking
 *                       call, but the PML decided to use a
 *                       non-blocking operation, likely for
 *                       internal performance decisions This is an
 *                       optimization flag and is not needed for
 *                       correctness.
 * @param mtl_request (IN) Pointer to mtl_request.  The ompi_req field
 *                       will be populated with an initialized
 *                       ompi_request_t before calling.
 *
 * @return               OMPI_SUCCESS or error value
 *
 * \note While MPI does not allow users to specify negative tags, they
 * are used internally in Open MPI to provide a unique channel for
 * collective operations.  Therefore, the MTL can *not* cause an error
 * if a negative tag is used.
 */
typedef int (*mca_mtl_base_module_isend_fn_t)(
                          struct mca_mtl_base_module_t* mtl,
                          struct ompi_communicator_t *comm,
                          int dest,
                          int tag,
                          struct opal_convertor_t *convertor,
                          mca_pml_base_send_mode_t mode,
                          bool blocking,
                          mca_mtl_request_t *mtl_request);


/**
 * Non-blocking receive
 *
 * Non-blocking receive function.  Standard MPI semantics for
 * MPI_Irecv must be implemented by this call.
 *
 * The PML will handle creation of the request, leaving the number of
 * bytes requested in teh module structure available for the MTL,
 * directly after the ompi_request_t structure.  The PML will handle
 * proper destruction of the request once it can safely be destroyed
 * (it has been completed and free'ed by a call to REQUEST_FREE or
 * TEST/WAIT).  The MTL should remove all resources associated with
 * the request when it is marked as completed.
 *
 * @param comm (IN)      Communicator used for operation
 * @param src (IN)       Source rank for send (relative to comm)
 * @param tag (IN)       MPI tag used for sending.  See note below.
 * @param convertor (IN) Datatype convertor describing receive datatype.
 *                       Already prepared for receive.
 * @param mtl_request (IN) Pointer to mtl_request.  The ompi_req field
 *                       will be populated with an initialized
 *                       ompi_request_t before calling.
 *
 * @return              OMPI_SUCCESS or error value
 *
 * \note While MPI does not allow users to specify negative tags, they
 * are used internally in Open MPI to provide a unique channel for
 * collective operations.  Therefore, the MTL can *not* cause an error
 * if a negative tag is used.  Further, MPI_ANY_TAG should *not* match
 * against negative tags.
 */
typedef int (*mca_mtl_base_module_irecv_fn_t)(
                          struct mca_mtl_base_module_t* mtl,
                          struct ompi_communicator_t *comm,
                          int src,
                          int tag,
                          struct opal_convertor_t *convertor,
                          struct mca_mtl_request_t *mtl_request);


/**
 * Non-blocking probe
 *
 * Non-blocking probe function.  Standard MPI semantics for MPI_IPROBE
 * must be implemented by this call.
 *
 * @param comm (IN)      Communicator used for operation
 * @param src (IN)       Source rank for send (relative to comm)
 * @param tag (IN)       MPI tag used for sending.  See note below.
 * @param flag (OUT)     true if message available, false otherwise
 * @param status (OUT)   Status structure for information on
 *                       available message
 *
 * \note While MPI does not allow users to specify negative tags, they
 * are used internally in Open MPI to provide a unique channel for
 * collective operations.  Therefore, the MTL can *not* cause an error
 * if a negative tag is used.  Further, MPI_ANY_TAG should *not* match
 * against negative tags.
 */
typedef int (*mca_mtl_base_module_iprobe_fn_t)(
                          struct mca_mtl_base_module_t* mtl,
                          struct ompi_communicator_t *comm,
                          int src,
                          int tag,
                          int *flag,
                          struct ompi_status_public_t *status);


typedef int (*mca_mtl_base_module_imrecv_fn_t)(struct mca_mtl_base_module_t* mtl,
                                               struct opal_convertor_t *convertor,
                                               struct ompi_message_t **message,
                                               struct mca_mtl_request_t *mtl_request);

typedef int (*mca_mtl_base_module_improbe_fn_t)(struct mca_mtl_base_module_t *mtl,
                                                struct ompi_communicator_t *comm,
                                                int src,
                                                int tag,
                                                int *matched,
                                                struct ompi_message_t **message,
                                                struct ompi_status_public_t *status);

/**
 * Cancel an existing request
 *
 * Attempt to cancel an existing request.  The (poorly defined)
 * semantics for MPI_CANCEL must be implemented by this call.  This,
 * of course, allows the MTL module to do nothing at all.
 * Implementations of the MTL should make a good faith effort to
 * cancel receive requests that have not been started, as the "post a
 * receive for control messages" paradigm is a common one in loosely
 * coupled MPI applications.
 *
 * @param request(IN)     Request that should be cancelled
 * @param flag            Unknown exactly what this does.
 *
 */
typedef int (*mca_mtl_base_module_cancel_fn_t)(
                          struct mca_mtl_base_module_t* mtl,
                          mca_mtl_request_t *mtl_request,
                          int flag);


/**
 * Downcall from PML layer when a new communicator is created.
 *
 * @param comm  Communicator
 * @return      OMPI_SUCCESS or failure status.
 *
 * Provides the MTL the opportunity to initialize/cache a data structure
 * on the communicator.
 */
typedef int (*mca_mtl_base_module_add_comm_fn_t)(
                          struct mca_mtl_base_module_t* mtl,
                          struct ompi_communicator_t* comm);


/**
 * Downcall from PML layer when a communicator is destroyed.
 *
 * @param comm  Communicator
 * @return      OMPI_SUCCESS or failure status.
 *
 * Provides the MTL the opportunity to cleanup any datastructures
 * associated with the communicator.
 */
typedef int (*mca_mtl_base_module_del_comm_fn_t)(
                          struct mca_mtl_base_module_t* mtl,
                          struct ompi_communicator_t* comm);


/**
 * MTL module interface functions and attributes.
 */
struct mca_mtl_base_module_t {
    int      mtl_max_contextid;   /**< maximum allowable contextid */
    int      mtl_max_tag;         /**< maximum tag value.  note that negative tags must be allowed */
    size_t   mtl_request_size;    /**< number of bytes to reserve with request structure */

    uint32_t mtl_flags;           /**< flags (put/get...) */

    /* MTL function table */
    mca_mtl_base_module_add_procs_fn_t   mtl_add_procs;
    mca_mtl_base_module_del_procs_fn_t   mtl_del_procs;
    mca_mtl_base_module_finalize_fn_t    mtl_finalize;

    mca_mtl_base_module_send_fn_t        mtl_send;
    mca_mtl_base_module_isend_fn_t       mtl_isend;
    mca_mtl_base_module_irecv_fn_t       mtl_irecv;
    mca_mtl_base_module_iprobe_fn_t      mtl_iprobe;
    mca_mtl_base_module_imrecv_fn_t      mtl_imrecv;
    mca_mtl_base_module_improbe_fn_t     mtl_improbe;

    /* Optional MTL functions */
    mca_mtl_base_module_cancel_fn_t      mtl_cancel;
    mca_mtl_base_module_add_comm_fn_t    mtl_add_comm;
    mca_mtl_base_module_del_comm_fn_t    mtl_del_comm;
};
typedef struct mca_mtl_base_module_t mca_mtl_base_module_t;

/*
 * Macro for use in modules that are of type mtl
 */
#define MCA_MTL_BASE_VERSION_2_0_0 \
    OMPI_MCA_BASE_VERSION_2_1_0("mtl", 2, 0, 0)

OMPI_DECLSPEC extern mca_mtl_base_module_t *ompi_mtl;

/*
 * macro for doing direct call / call through struct
 */
#if MCA_ompi_mtl_DIRECT_CALL


#define OMPI_MTL_CALL_STAMP(a, b) ompi_mtl_ ## a ## _ ## b
#define OMPI_MTL_CALL_EXPANDER(a, b) OMPI_MTL_CALL_STAMP(a,b)
#define OMPI_MTL_CALL(a) OMPI_MTL_CALL_EXPANDER(MCA_ompi_mtl_DIRECT_CALL_COMPONENT, a)

#error #include MCA_ompi_mtl_DIRECT_CALL_HEADER

#else
#define OMPI_MTL_CALL(a) ompi_mtl->mtl_ ## a
#endif


END_C_DECLS
#endif
