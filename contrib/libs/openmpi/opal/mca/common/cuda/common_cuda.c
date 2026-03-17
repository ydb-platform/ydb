/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2011-2015 NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2015      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * This file contains various support functions for doing CUDA
 * operations.
 */
#include "opal_config.h"

#include <errno.h>
#include <unistd.h>
#include <cuda.h>

#include "opal/align.h"
#include "opal/datatype/opal_convertor.h"
#include "opal/datatype/opal_datatype_cuda.h"
#include "opal/util/output.h"
#include "opal/util/show_help.h"
#include "opal/util/proc.h"
#include "opal/util/argv.h"

#include "opal/mca/rcache/base/base.h"
#include "opal/runtime/opal_params.h"
#include "opal/mca/timer/base/base.h"
#include "opal/mca/dl/base/base.h"

#include "common_cuda.h"

/**
 * Since function names can get redefined in cuda.h file, we need to do this
 * stringifying to get the latest function name from the header file.  For
 * example, cuda.h may have something like this:
 * #define cuMemFree cuMemFree_v2
 * We want to make sure we find cuMemFree_v2, not cuMemFree.
 */
#define STRINGIFY2(x) #x
#define STRINGIFY(x) STRINGIFY2(x)

#define OPAL_CUDA_DLSYM(libhandle, funcName)                                         \
do {                                                                                 \
 char *err_msg;                                                                      \
 void *ptr;                                                                          \
 if (OPAL_SUCCESS !=                                                                 \
     opal_dl_lookup(libhandle, STRINGIFY(funcName), &ptr, &err_msg)) {               \
        opal_show_help("help-mpi-common-cuda.txt", "dlsym failed", true,             \
                       STRINGIFY(funcName), err_msg);                                \
        return 1;                                                                    \
    } else {                                                                         \
        *(void **)(&cuFunc.funcName) = ptr;                                          \
        opal_output_verbose(15, mca_common_cuda_output,                              \
                            "CUDA: successful dlsym of %s",                          \
                            STRINGIFY(funcName));                                    \
    }                                                                                \
} while (0)

/* Structure to hold CUDA function pointers that get dynamically loaded. */
struct cudaFunctionTable {
    int (*cuPointerGetAttribute)(void *, CUpointer_attribute, CUdeviceptr);
    int (*cuMemcpyAsync)(CUdeviceptr, CUdeviceptr, size_t, CUstream);
    int (*cuMemcpy)(CUdeviceptr, CUdeviceptr, size_t);
    int (*cuMemAlloc)(CUdeviceptr *, unsigned int);
    int (*cuMemFree)(CUdeviceptr buf);
    int (*cuCtxGetCurrent)(void *cuContext);
    int (*cuStreamCreate)(CUstream *, int);
    int (*cuEventCreate)(CUevent *, int);
    int (*cuEventRecord)(CUevent, CUstream);
    int (*cuMemHostRegister)(void *, size_t, unsigned int);
    int (*cuMemHostUnregister)(void *);
    int (*cuEventQuery)(CUevent);
    int (*cuEventDestroy)(CUevent);
    int (*cuStreamWaitEvent)(CUstream, CUevent, unsigned int);
    int (*cuMemGetAddressRange)(CUdeviceptr*, size_t*, CUdeviceptr);
    int (*cuIpcGetEventHandle)(CUipcEventHandle*, CUevent);
    int (*cuIpcOpenEventHandle)(CUevent*, CUipcEventHandle);
    int (*cuIpcOpenMemHandle)(CUdeviceptr*, CUipcMemHandle, unsigned int);
    int (*cuIpcCloseMemHandle)(CUdeviceptr);
    int (*cuIpcGetMemHandle)(CUipcMemHandle*, CUdeviceptr);
    int (*cuCtxGetDevice)(CUdevice *);
    int (*cuDeviceCanAccessPeer)(int *, CUdevice, CUdevice);
    int (*cuDeviceGet)(CUdevice *, int);
#if OPAL_CUDA_GDR_SUPPORT
    int (*cuPointerSetAttribute)(const void *, CUpointer_attribute, CUdeviceptr);
#endif /* OPAL_CUDA_GDR_SUPPORT */
    int (*cuCtxSetCurrent)(CUcontext);
    int (*cuEventSynchronize)(CUevent);
    int (*cuStreamSynchronize)(CUstream);
    int (*cuStreamDestroy)(CUstream);
#if OPAL_CUDA_GET_ATTRIBUTES
    int (*cuPointerGetAttributes)(unsigned int, CUpointer_attribute *, void **, CUdeviceptr);
#endif /* OPAL_CUDA_GET_ATTRIBUTES */
};
typedef struct cudaFunctionTable cudaFunctionTable_t;
static cudaFunctionTable_t cuFunc;

static int stage_one_init_ref_count = 0;
static bool stage_three_init_complete = false;
static bool common_cuda_initialized = false;
static bool common_cuda_mca_parames_registered = false;
static int mca_common_cuda_verbose;
static int mca_common_cuda_output = 0;
bool mca_common_cuda_enabled = false;
static bool mca_common_cuda_register_memory = true;
static bool mca_common_cuda_warning = false;
static opal_list_t common_cuda_memory_registrations;
static CUstream ipcStream = NULL;
static CUstream dtohStream = NULL;
static CUstream htodStream = NULL;
static CUstream memcpyStream = NULL;
static int mca_common_cuda_gpu_mem_check_workaround = (CUDA_VERSION > 7000) ? 0 : 1;
static opal_mutex_t common_cuda_init_lock;
static opal_mutex_t common_cuda_htod_lock;
static opal_mutex_t common_cuda_dtoh_lock;
static opal_mutex_t common_cuda_ipc_lock;

/* Functions called by opal layer - plugged into opal function table */
static int mca_common_cuda_is_gpu_buffer(const void*, opal_convertor_t*);
static int mca_common_cuda_memmove(void*, void*, size_t);
static int mca_common_cuda_cu_memcpy_async(void*, const void*, size_t, opal_convertor_t*);
static int mca_common_cuda_cu_memcpy(void*, const void*, size_t);

/* Function that gets plugged into opal layer */
static int mca_common_cuda_stage_two_init(opal_common_cuda_function_table_t *);

/* Structure to hold memory registrations that are delayed until first
 * call to send or receive a GPU pointer */
struct common_cuda_mem_regs_t {
    opal_list_item_t super;
    void *ptr;
    size_t amount;
    char *msg;
};
typedef struct common_cuda_mem_regs_t common_cuda_mem_regs_t;
OBJ_CLASS_DECLARATION(common_cuda_mem_regs_t);
OBJ_CLASS_INSTANCE(common_cuda_mem_regs_t,
                   opal_list_item_t,
                   NULL,
                   NULL);

static int mca_common_cuda_async = 1;
static int mca_common_cuda_cumemcpy_async;
#if OPAL_ENABLE_DEBUG
static int mca_common_cuda_cumemcpy_timing;
#endif /* OPAL_ENABLE_DEBUG */

/* Array of CUDA events to be queried for IPC stream, sending side and
 * receiving side. */
CUevent *cuda_event_ipc_array = NULL;
CUevent *cuda_event_dtoh_array = NULL;
CUevent *cuda_event_htod_array = NULL;

/* Array of fragments currently being moved by cuda async non-blocking
 * operations */
struct mca_btl_base_descriptor_t **cuda_event_ipc_frag_array = NULL;
struct mca_btl_base_descriptor_t **cuda_event_dtoh_frag_array = NULL;
struct mca_btl_base_descriptor_t **cuda_event_htod_frag_array = NULL;

/* First free/available location in cuda_event_status_array */
static int cuda_event_ipc_first_avail, cuda_event_dtoh_first_avail, cuda_event_htod_first_avail;

/* First currently-being used location in the cuda_event_status_array */
static int cuda_event_ipc_first_used, cuda_event_dtoh_first_used, cuda_event_htod_first_used;

/* Number of status items currently in use */
static int cuda_event_ipc_num_used, cuda_event_dtoh_num_used, cuda_event_htod_num_used;

/* Size of array holding events */
int cuda_event_max = 400;
static int cuda_event_ipc_most = 0;
static int cuda_event_dtoh_most = 0;
static int cuda_event_htod_most = 0;

/* Handle to libcuda.so */
opal_dl_handle_t *libcuda_handle = NULL;

/* Unused variable that we register at init time and unregister at fini time.
 * This is used to detect if user has done a device reset prior to MPI_Finalize.
 * This is a workaround to avoid SEGVs.
 */
static int checkmem;
static int ctx_ok = 1;

#define CUDA_COMMON_TIMING 0
#if OPAL_ENABLE_DEBUG
/* Some timing support structures.  Enable this to help analyze
 * internal performance issues. */
static opal_timer_t ts_start;
static opal_timer_t ts_end;
static double accum;
#define THOUSAND  1000L
#define MILLION   1000000L
static float mydifftime(opal_timer_t ts_start, opal_timer_t ts_end);
#endif /* OPAL_ENABLE_DEBUG */

/* These functions are typically unused in the optimized builds. */
static void cuda_dump_evthandle(int, void *, char *) __opal_attribute_unused__ ;
static void cuda_dump_memhandle(int, void *, char *) __opal_attribute_unused__ ;
#if OPAL_ENABLE_DEBUG
#define CUDA_DUMP_MEMHANDLE(a) cuda_dump_memhandle a
#define CUDA_DUMP_EVTHANDLE(a) cuda_dump_evthandle a
#else
#define CUDA_DUMP_MEMHANDLE(a)
#define CUDA_DUMP_EVTHANDLE(a)
#endif /* OPAL_ENABLE_DEBUG */

/* This is a seperate function so we can see these variables with ompi_info and
 * also set them with the tools interface */
void mca_common_cuda_register_mca_variables(void)
{

    if (false == common_cuda_mca_parames_registered) {
        common_cuda_mca_parames_registered = true;
    }
    /* Set different levels of verbosity in the cuda related code. */
    mca_common_cuda_verbose = 0;
    (void) mca_base_var_register("ompi", "mpi", "common_cuda", "verbose",
                                 "Set level of common cuda verbosity",
                                 MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &mca_common_cuda_verbose);

    /* Control whether system buffers get CUDA pinned or not.  Allows for
     * performance analysis. */
    mca_common_cuda_register_memory = true;
    (void) mca_base_var_register("ompi", "mpi", "common_cuda", "register_memory",
                                 "Whether to cuMemHostRegister preallocated BTL buffers",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &mca_common_cuda_register_memory);

    /* Control whether we see warnings when CUDA memory registration fails.  This is
     * useful when CUDA support is configured in, but we are running a regular MPI
     * application without CUDA. */
    mca_common_cuda_warning = true;
    (void) mca_base_var_register("ompi", "mpi", "common_cuda", "warning",
                                 "Whether to print warnings when CUDA registration fails",
                                 MCA_BASE_VAR_TYPE_BOOL, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &mca_common_cuda_warning);

    /* Use this flag to test async vs sync copies */
    mca_common_cuda_async = 1;
    (void) mca_base_var_register("ompi", "mpi", "common_cuda", "memcpy_async",
                                 "Set to 0 to force CUDA sync copy instead of async",
                                 MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &mca_common_cuda_async);

    /* Use this parameter to increase the number of outstanding events allows */
    (void) mca_base_var_register("ompi", "mpi", "common_cuda", "event_max",
                                 "Set number of oustanding CUDA events",
                                 MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &cuda_event_max);

    /* Use this flag to test cuMemcpyAsync vs cuMemcpy */
    mca_common_cuda_cumemcpy_async = 1;
    (void) mca_base_var_register("ompi", "mpi", "common_cuda", "cumemcpy_async",
                                 "Set to 0 to force CUDA cuMemcpy instead of cuMemcpyAsync/cuStreamSynchronize",
                                 MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                 OPAL_INFO_LVL_5,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &mca_common_cuda_cumemcpy_async);

#if OPAL_ENABLE_DEBUG
    /* Use this flag to dump out timing of cumempcy sync and async */
    mca_common_cuda_cumemcpy_timing = 0;
    (void) mca_base_var_register("ompi", "mpi", "common_cuda", "cumemcpy_timing",
                                 "Set to 1 to dump timing of eager copies",
                                 MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                 OPAL_INFO_LVL_5,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &mca_common_cuda_cumemcpy_timing);
#endif /* OPAL_ENABLE_DEBUG */

    (void) mca_base_var_register("ompi", "mpi", "common_cuda", "gpu_mem_check_workaround",
                                 "Set to 0 to disable GPU memory check workaround. A user would rarely have to do this.",
                                 MCA_BASE_VAR_TYPE_INT, NULL, 0, 0,
                                 OPAL_INFO_LVL_9,
                                 MCA_BASE_VAR_SCOPE_READONLY,
                                 &mca_common_cuda_gpu_mem_check_workaround);
}

/**
 * This is the first stage of initialization.  This function is called
 * explicitly by any BTLs that can support CUDA-aware. It is called during
 * the component open phase of initialization. This fuction will look for
 * the SONAME of the library which is libcuda.so.1. In most cases, this will
 * result in the library found.  However, there are some setups that require
 * the extra steps for searching. This function will then load the symbols
 * needed from the CUDA driver library. Any failure will result in this
 * initialization failing and status will be set showing that.
 */
int mca_common_cuda_stage_one_init(void)
{
    int retval, i, j;
    char *cudalibs[] = {"libcuda.so.1", "libcuda.dylib", NULL};
    char *searchpaths[] = {"", "/usr/lib64", NULL};
    char **errmsgs = NULL;
    char *errmsg = NULL;
    int errsize;
    bool stage_one_init_passed = false;

    stage_one_init_ref_count++;
    if (stage_one_init_ref_count > 1) {
        opal_output_verbose(10, mca_common_cuda_output,
                            "CUDA: stage_one_init_ref_count is now %d, no need to init",
                            stage_one_init_ref_count);
        return OPAL_SUCCESS;
    }

    /* This is a no-op in most cases as the parameters were registered earlier */
    mca_common_cuda_register_mca_variables();

    OBJ_CONSTRUCT(&common_cuda_init_lock, opal_mutex_t);
    OBJ_CONSTRUCT(&common_cuda_htod_lock, opal_mutex_t);
    OBJ_CONSTRUCT(&common_cuda_dtoh_lock, opal_mutex_t);
    OBJ_CONSTRUCT(&common_cuda_ipc_lock, opal_mutex_t);

    mca_common_cuda_output = opal_output_open(NULL);
    opal_output_set_verbosity(mca_common_cuda_output, mca_common_cuda_verbose);

    opal_output_verbose(10, mca_common_cuda_output,
                        "CUDA: stage_one_init_ref_count is now %d, initializing",
                        stage_one_init_ref_count);

    /* First check if the support is enabled.  In the case that the user has
     * turned it off, we do not need to continue with any CUDA specific
     * initialization.  Do this after MCA parameter registration. */
    if (!opal_cuda_support) {
        return 1;
    }

    if (!OPAL_HAVE_DL_SUPPORT) {
        opal_show_help("help-mpi-common-cuda.txt", "dlopen disabled", true);
        return 1;
    }

    /* Now walk through all the potential names libcuda and find one
     * that works.  If it does, all is good.  If not, print out all
     * the messages about why things failed.  This code was careful
     * to try and save away all error messages if the loading ultimately
     * failed to help with debugging.
     *
     * NOTE: On the first loop we just utilize the default loading
     * paths from the system.  For the second loop, set /usr/lib64 to
     * the search path and try again.  This is done to handle the case
     * where we have both 32 and 64 bit libcuda.so libraries
     * installed.  Even when running in 64-bit mode, the /usr/lib
     * directory is searched first and we may find a 32-bit
     * libcuda.so.1 library.  Loading of this library will fail as the
     * OPAL DL framework does not handle having the wrong ABI in the
     * search path (unlike ld or ld.so).  Note that we only set this
     * search path after the original search.  This is so that
     * LD_LIBRARY_PATH and run path settings are respected.  Setting
     * this search path overrides them (rather then being
     * appended). */
    j = 0;
    while (searchpaths[j] != NULL) {
        i = 0;
        while (cudalibs[i] != NULL) {
            char *filename = NULL;
            char *str = NULL;

            /* If there's a non-empty search path, prepend it
               to the library filename */
            if (strlen(searchpaths[j]) > 0) {
                asprintf(&filename, "%s/%s", searchpaths[j], cudalibs[i]);
            } else {
                filename = strdup(cudalibs[i]);
            }
            if (NULL == filename) {
                opal_show_help("help-mpi-common-cuda.txt", "No memory",
                               true, OPAL_PROC_MY_HOSTNAME);
                return 1;
            }

            retval = opal_dl_open(filename, false, false,
                                  &libcuda_handle, &str);
            if (OPAL_SUCCESS != retval || NULL == libcuda_handle) {
                if (NULL != str) {
                    opal_argv_append(&errsize, &errmsgs, str);
                } else {
                    opal_argv_append(&errsize, &errmsgs,
                                     "opal_dl_open() returned NULL.");
                }
                opal_output_verbose(10, mca_common_cuda_output,
                                    "CUDA: Library open error: %s",
                                    errmsgs[errsize-1]);
            } else {
                opal_output_verbose(10, mca_common_cuda_output,
                                    "CUDA: Library successfully opened %s",
                                    cudalibs[i]);
                stage_one_init_passed = true;
                break;
            }
            i++;

            free(filename);
        }
        if (true == stage_one_init_passed) {
            break; /* Break out of outer loop */
        }
        j++;
    }

    if (true != stage_one_init_passed) {
        errmsg = opal_argv_join(errmsgs, '\n');
        if (opal_warn_on_missing_libcuda) {
            opal_show_help("help-mpi-common-cuda.txt", "dlopen failed", true,
                           errmsg);
        }
        opal_cuda_support = 0;
    }
    opal_argv_free(errmsgs);
    free(errmsg);

    if (true != stage_one_init_passed) {
        return 1;
    }
    opal_cuda_add_initialization_function(&mca_common_cuda_stage_two_init);
    OBJ_CONSTRUCT(&common_cuda_memory_registrations, opal_list_t);

    /* Map in the functions that we need.  Note that if there is an error
     * the macro OPAL_CUDA_DLSYM will print an error and call return.  */
    OPAL_CUDA_DLSYM(libcuda_handle, cuStreamCreate);
    OPAL_CUDA_DLSYM(libcuda_handle, cuCtxGetCurrent);
    OPAL_CUDA_DLSYM(libcuda_handle, cuEventCreate);
    OPAL_CUDA_DLSYM(libcuda_handle, cuEventRecord);
    OPAL_CUDA_DLSYM(libcuda_handle, cuMemHostRegister);
    OPAL_CUDA_DLSYM(libcuda_handle, cuMemHostUnregister);
    OPAL_CUDA_DLSYM(libcuda_handle, cuPointerGetAttribute);
    OPAL_CUDA_DLSYM(libcuda_handle, cuEventQuery);
    OPAL_CUDA_DLSYM(libcuda_handle, cuEventDestroy);
    OPAL_CUDA_DLSYM(libcuda_handle, cuStreamWaitEvent);
    OPAL_CUDA_DLSYM(libcuda_handle, cuMemcpyAsync);
    OPAL_CUDA_DLSYM(libcuda_handle, cuMemcpy);
    OPAL_CUDA_DLSYM(libcuda_handle, cuMemFree);
    OPAL_CUDA_DLSYM(libcuda_handle, cuMemAlloc);
    OPAL_CUDA_DLSYM(libcuda_handle, cuMemGetAddressRange);
    OPAL_CUDA_DLSYM(libcuda_handle, cuIpcGetEventHandle);
    OPAL_CUDA_DLSYM(libcuda_handle, cuIpcOpenEventHandle);
    OPAL_CUDA_DLSYM(libcuda_handle, cuIpcOpenMemHandle);
    OPAL_CUDA_DLSYM(libcuda_handle, cuIpcCloseMemHandle);
    OPAL_CUDA_DLSYM(libcuda_handle, cuIpcGetMemHandle);
    OPAL_CUDA_DLSYM(libcuda_handle, cuCtxGetDevice);
    OPAL_CUDA_DLSYM(libcuda_handle, cuDeviceCanAccessPeer);
    OPAL_CUDA_DLSYM(libcuda_handle, cuDeviceGet);
#if OPAL_CUDA_GDR_SUPPORT
    OPAL_CUDA_DLSYM(libcuda_handle, cuPointerSetAttribute);
#endif /* OPAL_CUDA_GDR_SUPPORT */
    OPAL_CUDA_DLSYM(libcuda_handle, cuCtxSetCurrent);
    OPAL_CUDA_DLSYM(libcuda_handle, cuEventSynchronize);
    OPAL_CUDA_DLSYM(libcuda_handle, cuStreamSynchronize);
    OPAL_CUDA_DLSYM(libcuda_handle, cuStreamDestroy);
#if OPAL_CUDA_GET_ATTRIBUTES
    OPAL_CUDA_DLSYM(libcuda_handle, cuPointerGetAttributes);
#endif /* OPAL_CUDA_GET_ATTRIBUTES */
    return 0;
}

/**
 * This function is registered with the OPAL CUDA support.  In that way,
 * these function pointers will be loaded into the OPAL CUDA code when
 * the first convertor is initialized.  This does not trigger any CUDA
 * specific initialization as this may just be a host buffer that is
 * triggering this call.
 */
static int mca_common_cuda_stage_two_init(opal_common_cuda_function_table_t *ftable)
{
    if (OPAL_UNLIKELY(!opal_cuda_support)) {
        return OPAL_ERROR;
    }

    ftable->gpu_is_gpu_buffer = &mca_common_cuda_is_gpu_buffer;
    ftable->gpu_cu_memcpy_async = &mca_common_cuda_cu_memcpy_async;
    ftable->gpu_cu_memcpy = &mca_common_cuda_cu_memcpy;
    ftable->gpu_memmove = &mca_common_cuda_memmove;

    opal_output_verbose(30, mca_common_cuda_output,
                        "CUDA: support functions initialized");
    return OPAL_SUCCESS;
}

/**
 * This is the last phase of initialization.  This is triggered when we examine
 * a buffer pointer and determine it is a GPU buffer.  We then assume the user
 * has selected their GPU and we can go ahead with all the CUDA related
 * initializations.  If we get an error, just return.  Cleanup of resources
 * will happen when fini is called.
 */
static int mca_common_cuda_stage_three_init(void)
{
    int i, s, rc;
    CUresult res;
    CUcontext cuContext;
    common_cuda_mem_regs_t *mem_reg;

    OPAL_THREAD_LOCK(&common_cuda_init_lock);
    opal_output_verbose(20, mca_common_cuda_output,
                        "CUDA: entering stage three init");

/* Compiled without support or user disabled support */
    if (OPAL_UNLIKELY(!opal_cuda_support)) {
        opal_output_verbose(20, mca_common_cuda_output,
                            "CUDA: No mpi cuda support, exiting stage three init");
        stage_three_init_complete = true;
        OPAL_THREAD_UNLOCK(&common_cuda_init_lock);
        return OPAL_ERROR;
    }

    /* In case another thread snuck in and completed the initialization */
    if (true == stage_three_init_complete) {
        if (common_cuda_initialized) {
            opal_output_verbose(20, mca_common_cuda_output,
                                "CUDA: Stage three already complete, exiting stage three init");
            OPAL_THREAD_UNLOCK(&common_cuda_init_lock);
            return OPAL_SUCCESS;
        } else {
            opal_output_verbose(20, mca_common_cuda_output,
                                "CUDA: Stage three already complete, failed during the init");
            OPAL_THREAD_UNLOCK(&common_cuda_init_lock);
            return OPAL_ERROR;
        }
    }

    /* Check to see if this process is running in a CUDA context.  If
     * so, all is good.  If not, then disable registration of memory. */
    res = cuFunc.cuCtxGetCurrent(&cuContext);
    if (CUDA_SUCCESS != res) {
        if (mca_common_cuda_warning) {
            /* Check for the not initialized error since we can make suggestions to
             * user for this error. */
            if (CUDA_ERROR_NOT_INITIALIZED == res) {
                opal_show_help("help-mpi-common-cuda.txt", "cuCtxGetCurrent failed not initialized",
                               true);
            } else {
                opal_show_help("help-mpi-common-cuda.txt", "cuCtxGetCurrent failed",
                               true, res);
            }
        }
        mca_common_cuda_enabled = false;
        mca_common_cuda_register_memory = false;
    } else if ((CUDA_SUCCESS == res) && (NULL == cuContext)) {
        if (mca_common_cuda_warning) {
            opal_show_help("help-mpi-common-cuda.txt", "cuCtxGetCurrent returned NULL",
                           true);
        }
        mca_common_cuda_enabled = false;
        mca_common_cuda_register_memory = false;
    } else {
        /* All is good.  mca_common_cuda_register_memory will retain its original
         * value.  Normally, that is 1, but the user can override it to disable
         * registration of the internal buffers. */
        mca_common_cuda_enabled = true;
        opal_output_verbose(20, mca_common_cuda_output,
                            "CUDA: cuCtxGetCurrent succeeded");
    }

    /* No need to go on at this point.  If we cannot create a context and we are at
     * the point where we are making MPI calls, it is time to fully disable
     * CUDA support.
     */
    if (false == mca_common_cuda_enabled) {
        OPAL_THREAD_UNLOCK(&common_cuda_init_lock);
        return OPAL_ERROR;
    }

    if (true == mca_common_cuda_enabled) {
        /* Set up an array to store outstanding IPC async copy events */
        cuda_event_ipc_num_used = 0;
        cuda_event_ipc_first_avail = 0;
        cuda_event_ipc_first_used = 0;

        cuda_event_ipc_array = (CUevent *) calloc(cuda_event_max, sizeof(CUevent *));
        if (NULL == cuda_event_ipc_array) {
            opal_show_help("help-mpi-common-cuda.txt", "No memory",
                           true, OPAL_PROC_MY_HOSTNAME);
            rc = OPAL_ERROR;
            goto cleanup_and_error;
        }

        /* Create the events since they can be reused. */
        for (i = 0; i < cuda_event_max; i++) {
            res = cuFunc.cuEventCreate(&cuda_event_ipc_array[i], CU_EVENT_DISABLE_TIMING);
            if (CUDA_SUCCESS != res) {
                opal_show_help("help-mpi-common-cuda.txt", "cuEventCreate failed",
                               true, OPAL_PROC_MY_HOSTNAME, res);
                rc = OPAL_ERROR;
                goto cleanup_and_error;
            }
        }

        /* The first available status index is 0.  Make an empty frag
           array. */
        cuda_event_ipc_frag_array = (struct mca_btl_base_descriptor_t **)
            malloc(sizeof(struct mca_btl_base_descriptor_t *) * cuda_event_max);
        if (NULL == cuda_event_ipc_frag_array) {
            opal_show_help("help-mpi-common-cuda.txt", "No memory",
                           true, OPAL_PROC_MY_HOSTNAME);
            rc = OPAL_ERROR;
            goto cleanup_and_error;
        }
    }

    if (true == mca_common_cuda_enabled) {
        /* Set up an array to store outstanding async dtoh events.  Used on the
         * sending side for asynchronous copies. */
        cuda_event_dtoh_num_used = 0;
        cuda_event_dtoh_first_avail = 0;
        cuda_event_dtoh_first_used = 0;

        cuda_event_dtoh_array = (CUevent *) calloc(cuda_event_max, sizeof(CUevent *));
        if (NULL == cuda_event_dtoh_array) {
            opal_show_help("help-mpi-common-cuda.txt", "No memory",
                           true, OPAL_PROC_MY_HOSTNAME);
            rc = OPAL_ERROR;
            goto cleanup_and_error;
        }

        /* Create the events since they can be reused. */
        for (i = 0; i < cuda_event_max; i++) {
            res = cuFunc.cuEventCreate(&cuda_event_dtoh_array[i], CU_EVENT_DISABLE_TIMING);
            if (CUDA_SUCCESS != res) {
                opal_show_help("help-mpi-common-cuda.txt", "cuEventCreate failed",
                               true, OPAL_PROC_MY_HOSTNAME, res);
                rc = OPAL_ERROR;
                goto cleanup_and_error;
            }
        }

        /* The first available status index is 0.  Make an empty frag
           array. */
        cuda_event_dtoh_frag_array = (struct mca_btl_base_descriptor_t **)
            malloc(sizeof(struct mca_btl_base_descriptor_t *) * cuda_event_max);
        if (NULL == cuda_event_dtoh_frag_array) {
            opal_show_help("help-mpi-common-cuda.txt", "No memory",
                           true, OPAL_PROC_MY_HOSTNAME);
            rc = OPAL_ERROR;
            goto cleanup_and_error;
        }

        /* Set up an array to store outstanding async htod events.  Used on the
         * receiving side for asynchronous copies. */
        cuda_event_htod_num_used = 0;
        cuda_event_htod_first_avail = 0;
        cuda_event_htod_first_used = 0;

        cuda_event_htod_array = (CUevent *) calloc(cuda_event_max, sizeof(CUevent *));
        if (NULL == cuda_event_htod_array) {
            opal_show_help("help-mpi-common-cuda.txt", "No memory",
                           true, OPAL_PROC_MY_HOSTNAME);
           rc = OPAL_ERROR;
           goto cleanup_and_error;
        }

        /* Create the events since they can be reused. */
        for (i = 0; i < cuda_event_max; i++) {
            res = cuFunc.cuEventCreate(&cuda_event_htod_array[i], CU_EVENT_DISABLE_TIMING);
            if (CUDA_SUCCESS != res) {
                opal_show_help("help-mpi-common-cuda.txt", "cuEventCreate failed",
                               true, OPAL_PROC_MY_HOSTNAME, res);
               rc = OPAL_ERROR;
               goto cleanup_and_error;
            }
        }

        /* The first available status index is 0.  Make an empty frag
           array. */
        cuda_event_htod_frag_array = (struct mca_btl_base_descriptor_t **)
            malloc(sizeof(struct mca_btl_base_descriptor_t *) * cuda_event_max);
        if (NULL == cuda_event_htod_frag_array) {
            opal_show_help("help-mpi-common-cuda.txt", "No memory",
                           true, OPAL_PROC_MY_HOSTNAME);
           rc = OPAL_ERROR;
           goto cleanup_and_error;
        }
    }

    s = opal_list_get_size(&common_cuda_memory_registrations);
    for(i = 0; i < s; i++) {
        mem_reg = (common_cuda_mem_regs_t *)
            opal_list_remove_first(&common_cuda_memory_registrations);
        if (mca_common_cuda_enabled && mca_common_cuda_register_memory) {
            res = cuFunc.cuMemHostRegister(mem_reg->ptr, mem_reg->amount, 0);
            if (res != CUDA_SUCCESS) {
                /* If registering the memory fails, print a message and continue.
                 * This is not a fatal error. */
                opal_show_help("help-mpi-common-cuda.txt", "cuMemHostRegister during init failed",
                               true, mem_reg->ptr, mem_reg->amount,
                               OPAL_PROC_MY_HOSTNAME, res, mem_reg->msg);
            } else {
                opal_output_verbose(20, mca_common_cuda_output,
                                    "CUDA: cuMemHostRegister OK on rcache %s: "
                                    "address=%p, bufsize=%d",
                                    mem_reg->msg, mem_reg->ptr, (int)mem_reg->amount);
            }
        }
        free(mem_reg->msg);
        OBJ_RELEASE(mem_reg);
    }

    /* Create stream for use in ipc asynchronous copies */
    res = cuFunc.cuStreamCreate(&ipcStream, 0);
    if (OPAL_UNLIKELY(res != CUDA_SUCCESS)) {
        opal_show_help("help-mpi-common-cuda.txt", "cuStreamCreate failed",
                       true, OPAL_PROC_MY_HOSTNAME, res);
        rc = OPAL_ERROR;
        goto cleanup_and_error;
    }

    /* Create stream for use in dtoh asynchronous copies */
    res = cuFunc.cuStreamCreate(&dtohStream, 0);
    if (OPAL_UNLIKELY(res != CUDA_SUCCESS)) {
        opal_show_help("help-mpi-common-cuda.txt", "cuStreamCreate failed",
                       true, OPAL_PROC_MY_HOSTNAME, res);
        rc = OPAL_ERROR;
        goto cleanup_and_error;
    }

    /* Create stream for use in htod asynchronous copies */
    res = cuFunc.cuStreamCreate(&htodStream, 0);
    if (OPAL_UNLIKELY(res != CUDA_SUCCESS)) {
        opal_show_help("help-mpi-common-cuda.txt", "cuStreamCreate failed",
                       true, OPAL_PROC_MY_HOSTNAME, res);
        rc = OPAL_ERROR;
        goto cleanup_and_error;
    }

    if (mca_common_cuda_cumemcpy_async) {
        /* Create stream for use in cuMemcpyAsync synchronous copies */
        res = cuFunc.cuStreamCreate(&memcpyStream, 0);
        if (OPAL_UNLIKELY(res != CUDA_SUCCESS)) {
            opal_show_help("help-mpi-common-cuda.txt", "cuStreamCreate failed",
                           true, OPAL_PROC_MY_HOSTNAME, res);
            rc = OPAL_ERROR;
            goto cleanup_and_error;
        }
    }

    res = cuFunc.cuMemHostRegister(&checkmem, sizeof(int), 0);
    if (res != CUDA_SUCCESS) {
        /* If registering the memory fails, print a message and continue.
         * This is not a fatal error. */
        opal_show_help("help-mpi-common-cuda.txt", "cuMemHostRegister during init failed",
                       true, &checkmem, sizeof(int),
                       OPAL_PROC_MY_HOSTNAME, res, "checkmem");

    } else {
        opal_output_verbose(20, mca_common_cuda_output,
                            "CUDA: cuMemHostRegister OK on test region");
    }

    opal_output_verbose(20, mca_common_cuda_output,
                        "CUDA: the extra gpu memory check is %s", (mca_common_cuda_gpu_mem_check_workaround == 1) ? "on":"off");

    opal_output_verbose(30, mca_common_cuda_output,
                        "CUDA: initialized");
    opal_atomic_mb();  /* Make sure next statement does not get reordered */
    common_cuda_initialized = true;
    stage_three_init_complete = true;
    OPAL_THREAD_UNLOCK(&common_cuda_init_lock);
    return OPAL_SUCCESS;

    /* If we are here, something went wrong.  Cleanup and return an error. */
 cleanup_and_error:
    opal_atomic_mb(); /* Make sure next statement does not get reordered */
    stage_three_init_complete = true;
    OPAL_THREAD_UNLOCK(&common_cuda_init_lock);
    return rc;
}

/**
 * Cleanup all CUDA resources.
 *
 * Note: Still figuring out how to get cuMemHostUnregister called from the smcuda sm
 * rcache.  Looks like with the memory pool from openib (grdma), the unregistering is
 * called as the free list is destructed.  Not true for the sm mpool.  This means we
 * are currently still leaking some host memory we registered with CUDA.
 */
void mca_common_cuda_fini(void)
{
    int i;
    CUresult res;

    if (false == common_cuda_initialized) {
        stage_one_init_ref_count--;
        opal_output_verbose(20, mca_common_cuda_output,
                            "CUDA: mca_common_cuda_fini, never completed initialization so "
                            "skipping fini, ref_count is now %d", stage_one_init_ref_count);
        return;
    }

    if (0 == stage_one_init_ref_count) {
        opal_output_verbose(20, mca_common_cuda_output,
                            "CUDA: mca_common_cuda_fini, ref_count=%d, fini is already complete",
                            stage_one_init_ref_count);
        return;
    }

    if (1 == stage_one_init_ref_count) {
        opal_output_verbose(20, mca_common_cuda_output,
                            "CUDA: mca_common_cuda_fini, ref_count=%d, cleaning up started",
                            stage_one_init_ref_count);

        /* This call is in here to make sure the context is still valid.
         * This was the one way of checking which did not cause problems
         * while calling into the CUDA library.  This check will detect if
         * a user has called cudaDeviceReset prior to MPI_Finalize. If so,
         * then this call will fail and we skip cleaning up CUDA resources. */
        res = cuFunc.cuMemHostUnregister(&checkmem);
        if (CUDA_SUCCESS != res) {
            ctx_ok = 0;
        }
        opal_output_verbose(20, mca_common_cuda_output,
                            "CUDA: mca_common_cuda_fini, cuMemHostUnregister returned %d, ctx_ok=%d",
                            res, ctx_ok);

        if (NULL != cuda_event_ipc_array) {
            if (ctx_ok) {
                for (i = 0; i < cuda_event_max; i++) {
                    if (NULL != cuda_event_ipc_array[i]) {
                        cuFunc.cuEventDestroy(cuda_event_ipc_array[i]);
                    }
                }
            }
            free(cuda_event_ipc_array);
        }
        if (NULL != cuda_event_htod_array) {
            if (ctx_ok) {
                for (i = 0; i < cuda_event_max; i++) {
                    if (NULL != cuda_event_htod_array[i]) {
                        cuFunc.cuEventDestroy(cuda_event_htod_array[i]);
                    }
                }
            }
            free(cuda_event_htod_array);
        }

        if (NULL != cuda_event_dtoh_array) {
            if (ctx_ok) {
                for (i = 0; i < cuda_event_max; i++) {
                    if (NULL != cuda_event_dtoh_array[i]) {
                        cuFunc.cuEventDestroy(cuda_event_dtoh_array[i]);
                    }
                }
            }
            free(cuda_event_dtoh_array);
        }

        if (NULL != cuda_event_ipc_frag_array) {
            free(cuda_event_ipc_frag_array);
        }
        if (NULL != cuda_event_htod_frag_array) {
            free(cuda_event_htod_frag_array);
        }
        if (NULL != cuda_event_dtoh_frag_array) {
            free(cuda_event_dtoh_frag_array);
        }
        if ((NULL != ipcStream) && ctx_ok) {
            cuFunc.cuStreamDestroy(ipcStream);
        }
        if ((NULL != dtohStream) && ctx_ok) {
            cuFunc.cuStreamDestroy(dtohStream);
        }
        if ((NULL != htodStream) && ctx_ok) {
            cuFunc.cuStreamDestroy(htodStream);
        }
        if ((NULL != memcpyStream) && ctx_ok) {
            cuFunc.cuStreamDestroy(memcpyStream);
        }
        OBJ_DESTRUCT(&common_cuda_init_lock);
        OBJ_DESTRUCT(&common_cuda_htod_lock);
        OBJ_DESTRUCT(&common_cuda_dtoh_lock);
        OBJ_DESTRUCT(&common_cuda_ipc_lock);
        if (NULL != libcuda_handle) {
            opal_dl_close(libcuda_handle);
        }

        opal_output_verbose(20, mca_common_cuda_output,
                            "CUDA: mca_common_cuda_fini, ref_count=%d, cleaning up all done",
                            stage_one_init_ref_count);

        opal_output_close(mca_common_cuda_output);

    } else {
        opal_output_verbose(20, mca_common_cuda_output,
                            "CUDA: mca_common_cuda_fini, ref_count=%d, cuda still in use",
                            stage_one_init_ref_count);
    }
    stage_one_init_ref_count--;
}

/**
 * Call the CUDA register function so we pin the memory in the CUDA
 * space.
 */
void mca_common_cuda_register(void *ptr, size_t amount, char *msg) {
    int res;

    /* Always first check if the support is enabled.  If not, just return */
    if (!opal_cuda_support)
        return;

    if (!common_cuda_initialized) {
        OPAL_THREAD_LOCK(&common_cuda_init_lock);
        if (!common_cuda_initialized) {
            common_cuda_mem_regs_t *regptr;
            regptr = OBJ_NEW(common_cuda_mem_regs_t);
            regptr->ptr = ptr;
            regptr->amount = amount;
            regptr->msg = strdup(msg);
            opal_list_append(&common_cuda_memory_registrations,
                             (opal_list_item_t*)regptr);
            OPAL_THREAD_UNLOCK(&common_cuda_init_lock);
            return;
        }
        OPAL_THREAD_UNLOCK(&common_cuda_init_lock);
    }

    if (mca_common_cuda_enabled && mca_common_cuda_register_memory) {
        res = cuFunc.cuMemHostRegister(ptr, amount, 0);
        if (OPAL_UNLIKELY(res != CUDA_SUCCESS)) {
            /* If registering the memory fails, print a message and continue.
             * This is not a fatal error. */
            opal_show_help("help-mpi-common-cuda.txt", "cuMemHostRegister failed",
                           true, ptr, amount,
                           OPAL_PROC_MY_HOSTNAME, res, msg);
        } else {
            opal_output_verbose(20, mca_common_cuda_output,
                                "CUDA: cuMemHostRegister OK on rcache %s: "
                                "address=%p, bufsize=%d",
                                msg, ptr, (int)amount);
        }
    }
}

/**
 * Call the CUDA unregister function so we unpin the memory in the CUDA
 * space.
 */
void mca_common_cuda_unregister(void *ptr, char *msg) {
    int res, i, s;
    common_cuda_mem_regs_t *mem_reg;

    /* This can happen if memory was queued up to be registered, but
     * no CUDA operations happened, so it never was registered.
     * Therefore, just release any of the resources. */
    if (!common_cuda_initialized) {
        s = opal_list_get_size(&common_cuda_memory_registrations);
        for(i = 0; i < s; i++) {
            mem_reg = (common_cuda_mem_regs_t *)
                opal_list_remove_first(&common_cuda_memory_registrations);
            free(mem_reg->msg);
            OBJ_RELEASE(mem_reg);
        }
        return;
    }

    if (mca_common_cuda_enabled && mca_common_cuda_register_memory) {
        res = cuFunc.cuMemHostUnregister(ptr);
        if (OPAL_UNLIKELY(res != CUDA_SUCCESS)) {
            /* If unregistering the memory fails, just continue.  This is during
             * shutdown.  Only print when running in verbose mode. */
            opal_output_verbose(20, mca_common_cuda_output,
                                "CUDA: cuMemHostUnregister failed: ptr=%p, res=%d, rcache=%s",
                                ptr, res, msg);

        } else {
            opal_output_verbose(20, mca_common_cuda_output,
                                "CUDA: cuMemHostUnregister OK on rcache %s: "
                                "address=%p",
                                msg, ptr);
        }
    }
}

/*
 * Get the memory handle of a local section of memory that can be sent
 * to the remote size so it can access the memory.  This is the
 * registration function for the sending side of a message transfer.
 */
int cuda_getmemhandle(void *base, size_t size, mca_rcache_base_registration_t *newreg,
                      mca_rcache_base_registration_t *hdrreg)

{
    CUmemorytype memType;
    CUresult result;
    CUipcMemHandle *memHandle;
    CUdeviceptr pbase;
    size_t psize;

    mca_rcache_common_cuda_reg_t *cuda_reg = (mca_rcache_common_cuda_reg_t*)newreg;
    memHandle = (CUipcMemHandle *)cuda_reg->data.memHandle;

    /* We should only be there if this is a CUDA device pointer */
    result = cuFunc.cuPointerGetAttribute(&memType,
                                          CU_POINTER_ATTRIBUTE_MEMORY_TYPE, (CUdeviceptr)base);
    assert(CUDA_SUCCESS == result);
    assert(CU_MEMORYTYPE_DEVICE == memType);

    /* Get the memory handle so we can send it to the remote process. */
    result = cuFunc.cuIpcGetMemHandle(memHandle, (CUdeviceptr)base);
    CUDA_DUMP_MEMHANDLE((100, memHandle, "GetMemHandle-After"));

    if (CUDA_SUCCESS != result) {
        opal_show_help("help-mpi-common-cuda.txt", "cuIpcGetMemHandle failed",
                       true, result, base);
        return OPAL_ERROR;
    } else {
        opal_output_verbose(20, mca_common_cuda_output,
                            "CUDA: cuIpcGetMemHandle passed: base=%p size=%d",
                            base, (int)size);
    }

    /* Need to get the real base and size of the memory handle.  This is
     * how the remote side saves the handles in a cache. */
    result = cuFunc.cuMemGetAddressRange(&pbase, &psize, (CUdeviceptr)base);
    if (CUDA_SUCCESS != result) {
        opal_show_help("help-mpi-common-cuda.txt", "cuMemGetAddressRange failed",
                       true, result, base);
        return OPAL_ERROR;
    } else {
        opal_output_verbose(10, mca_common_cuda_output,
                            "CUDA: cuMemGetAddressRange passed: addr=%p, size=%d, pbase=%p, psize=%d ",
                            base, (int)size, (void *)pbase, (int)psize);
    }

    /* Store all the information in the registration */
    cuda_reg->base.base = (void *)pbase;
    cuda_reg->base.bound = (unsigned char *)pbase + psize - 1;
    cuda_reg->data.memh_seg_addr.pval = (void *) pbase;
    cuda_reg->data.memh_seg_len = psize;

#if OPAL_CUDA_SYNC_MEMOPS
    /* With CUDA 6.0, we can set an attribute on the memory pointer that will
     * ensure any synchronous copies are completed prior to any other access
     * of the memory region.  This means we do not need to record an event
     * and send to the remote side.
     */
    memType = 1; /* Just use this variable since we already have it */
    result = cuFunc.cuPointerSetAttribute(&memType, CU_POINTER_ATTRIBUTE_SYNC_MEMOPS,
                                          (CUdeviceptr)base);
    if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
        opal_show_help("help-mpi-common-cuda.txt", "cuPointerSetAttribute failed",
                       true, OPAL_PROC_MY_HOSTNAME, result, base);
        return OPAL_ERROR;
    }
#else
    /* Need to record the event to ensure that any memcopies into the
     * device memory have completed.  The event handle associated with
     * this event is sent to the remote process so that it will wait
     * on this event prior to copying data out of the device memory.
     * Note that this needs to be the NULL stream to make since it is
     * unknown what stream any copies into the device memory were done
     * with. */
    result = cuFunc.cuEventRecord((CUevent)cuda_reg->data.event, 0);
    if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
        opal_show_help("help-mpi-common-cuda.txt", "cuEventRecord failed",
                       true, result, base);
        return OPAL_ERROR;
    }
#endif /* OPAL_CUDA_SYNC_MEMOPS */

    return OPAL_SUCCESS;
}

/*
 * This function is called by the local side that called the cuda_getmemhandle.
 * There is nothing to be done so just return.
 */
int cuda_ungetmemhandle(void *reg_data, mca_rcache_base_registration_t *reg)
{
    opal_output_verbose(10, mca_common_cuda_output,
                        "CUDA: cuda_ungetmemhandle (no-op): base=%p", reg->base);
    CUDA_DUMP_MEMHANDLE((100, ((mca_rcache_common_cuda_reg_t *)reg)->data.memHandle, "cuda_ungetmemhandle"));

    return OPAL_SUCCESS;
}

/*
 * Open a memory handle that refers to remote memory so we can get an address
 * that works on the local side.  This is the registration function for the
 * remote side of a transfer.  newreg contains the new handle.  hddrreg contains
 * the memory handle that was received from the remote side.
 */
int cuda_openmemhandle(void *base, size_t size, mca_rcache_base_registration_t *newreg,
                       mca_rcache_base_registration_t *hdrreg)
{
    CUresult result;
    CUipcMemHandle *memHandle;
    mca_rcache_common_cuda_reg_t *cuda_newreg = (mca_rcache_common_cuda_reg_t*)newreg;

    /* Save in local variable to avoid ugly casting */
    memHandle = (CUipcMemHandle *)cuda_newreg->data.memHandle;
    CUDA_DUMP_MEMHANDLE((100, memHandle, "Before call to cuIpcOpenMemHandle"));

    /* Open the memory handle and store it into the registration structure. */
    result = cuFunc.cuIpcOpenMemHandle((CUdeviceptr *)&newreg->alloc_base, *memHandle,
                                       CU_IPC_MEM_LAZY_ENABLE_PEER_ACCESS);

    /* If there are some stale entries in the cache, they can cause other
     * registrations to fail.  Let the caller know that so that can attempt
     * to clear them out. */
    if (CUDA_ERROR_ALREADY_MAPPED == result) {
        opal_output_verbose(10, mca_common_cuda_output,
                            "CUDA: cuIpcOpenMemHandle returned CUDA_ERROR_ALREADY_MAPPED for "
                            "p=%p,size=%d: notify memory pool\n", base, (int)size);
        return OPAL_ERR_WOULD_BLOCK;
    }
    if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
        opal_show_help("help-mpi-common-cuda.txt", "cuIpcOpenMemHandle failed",
                       true, OPAL_PROC_MY_HOSTNAME, result, base);
        /* Currently, this is a non-recoverable error */
        return OPAL_ERROR;
    } else {
        opal_output_verbose(10, mca_common_cuda_output,
                            "CUDA: cuIpcOpenMemHandle passed: base=%p (remote base=%p,size=%d)",
                            newreg->alloc_base, base, (int)size);
        CUDA_DUMP_MEMHANDLE((200, memHandle, "cuIpcOpenMemHandle"));
    }

    return OPAL_SUCCESS;
}

/*
 * Close a memory handle that refers to remote memory.
 */
int cuda_closememhandle(void *reg_data, mca_rcache_base_registration_t *reg)
{
    CUresult result;
    mca_rcache_common_cuda_reg_t *cuda_reg = (mca_rcache_common_cuda_reg_t*)reg;

    /* Only attempt to close if we have valid context.  This can change if a call
     * to the fini function is made and we discover context is gone. */
    if (ctx_ok) {
        result = cuFunc.cuIpcCloseMemHandle((CUdeviceptr)cuda_reg->base.alloc_base);
        if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
            if (CUDA_ERROR_DEINITIALIZED != result) {
                opal_show_help("help-mpi-common-cuda.txt", "cuIpcCloseMemHandle failed",
                true, result, cuda_reg->base.alloc_base);
            }
            /* We will just continue on and hope things continue to work. */
        } else {
            opal_output_verbose(10, mca_common_cuda_output,
                                "CUDA: cuIpcCloseMemHandle passed: base=%p",
                                cuda_reg->base.alloc_base);
            CUDA_DUMP_MEMHANDLE((100, cuda_reg->data.memHandle, "cuIpcCloseMemHandle"));
        }
    }

    return OPAL_SUCCESS;
}

void mca_common_cuda_construct_event_and_handle(uintptr_t *event, void *handle)
{
    CUresult result;

    result = cuFunc.cuEventCreate((CUevent *)event, CU_EVENT_INTERPROCESS | CU_EVENT_DISABLE_TIMING);
    if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
        opal_show_help("help-mpi-common-cuda.txt", "cuEventCreate failed",
                       true, OPAL_PROC_MY_HOSTNAME, result);
    }

    result = cuFunc.cuIpcGetEventHandle((CUipcEventHandle *)handle, (CUevent)*event);
    if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
        opal_show_help("help-mpi-common-cuda.txt", "cuIpcGetEventHandle failed",
                       true, result);
    }

    CUDA_DUMP_EVTHANDLE((10, handle, "construct_event_and_handle"));

}

void mca_common_cuda_destruct_event(uintptr_t event)
{
    CUresult result;

    /* Only attempt to destroy if we have valid context.  This can change if a call
     * to the fini function is made and we discover context is gone. */
    if (ctx_ok) {
        result = cuFunc.cuEventDestroy((CUevent)event);
        if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
            opal_show_help("help-mpi-common-cuda.txt", "cuEventDestroy failed",
                           true, result);
        }
    }
}


/*
 * Put remote event on stream to ensure that the the start of the
 * copy does not start until the completion of the event.
 */
void mca_common_wait_stream_synchronize(mca_rcache_common_cuda_reg_t *rget_reg)
{
#if OPAL_CUDA_SYNC_MEMOPS
    /* No need for any of this with SYNC_MEMOPS feature */
    return;
#else /* OPAL_CUDA_SYNC_MEMOPS */
    CUipcEventHandle evtHandle;
    CUevent event;
    CUresult result;

    memcpy(&evtHandle, rget_reg->data.evtHandle, sizeof(evtHandle));
    CUDA_DUMP_EVTHANDLE((100, &evtHandle, "stream_synchronize"));

    result = cuFunc.cuIpcOpenEventHandle(&event, evtHandle);
    if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
        opal_show_help("help-mpi-common-cuda.txt", "cuIpcOpenEventHandle failed",
                       true, result);
    }

    /* BEGIN of Workaround - There is a bug in CUDA 4.1 RC2 and earlier
     * versions.  Need to record an event on the stream, even though
     * it is not used, to make sure we do not short circuit our way
     * out of the cuStreamWaitEvent test.
     */
    result = cuFunc.cuEventRecord(event, 0);
    if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
        opal_show_help("help-mpi-common-cuda.txt", "cuEventRecord failed",
                       true, OPAL_PROC_MY_HOSTNAME, result);
    }
    /* END of Workaround */

    result = cuFunc.cuStreamWaitEvent(0, event, 0);
    if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
        opal_show_help("help-mpi-common-cuda.txt", "cuStreamWaitEvent failed",
                       true, result);
    }

    /* All done with this event. */
    result = cuFunc.cuEventDestroy(event);
    if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
        opal_show_help("help-mpi-common-cuda.txt", "cuEventDestroy failed",
                       true, result);
    }
#endif /* OPAL_CUDA_SYNC_MEMOPS */
}

/*
 * Start the asynchronous copy.  Then record and save away an event that will
 * be queried to indicate the copy has completed.
 */
int mca_common_cuda_memcpy(void *dst, void *src, size_t amount, char *msg,
                           struct mca_btl_base_descriptor_t *frag, int *done)
{
    CUresult result;
    int iter;

    OPAL_THREAD_LOCK(&common_cuda_ipc_lock);
    /* First make sure there is room to store the event.  If not, then
     * return an error.  The error message will tell the user to try and
     * run again, but with a larger array for storing events. */
    if (cuda_event_ipc_num_used == cuda_event_max) {
        opal_show_help("help-mpi-common-cuda.txt", "Out of cuEvent handles",
                       true, cuda_event_max, cuda_event_max+100, cuda_event_max+100);
        OPAL_THREAD_UNLOCK(&common_cuda_ipc_lock);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    if (cuda_event_ipc_num_used > cuda_event_ipc_most) {
        cuda_event_ipc_most = cuda_event_ipc_num_used;
        /* Just print multiples of 10 */
        if (0 == (cuda_event_ipc_most % 10)) {
            opal_output_verbose(20, mca_common_cuda_output,
                                "Maximum ipc events used is now %d", cuda_event_ipc_most);
        }
    }

    /* This is the standard way to run.  Running with synchronous copies is available
     * to measure the advantages of asynchronous copies. */
    if (OPAL_LIKELY(mca_common_cuda_async)) {
        result = cuFunc.cuMemcpyAsync((CUdeviceptr)dst, (CUdeviceptr)src, amount, ipcStream);
        if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
            opal_show_help("help-mpi-common-cuda.txt", "cuMemcpyAsync failed",
                           true, dst, src, amount, result);
            OPAL_THREAD_UNLOCK(&common_cuda_ipc_lock);
            return OPAL_ERROR;
        } else {
            opal_output_verbose(20, mca_common_cuda_output,
                                "CUDA: cuMemcpyAsync passed: dst=%p, src=%p, size=%d",
                                dst, src, (int)amount);
        }
        result = cuFunc.cuEventRecord(cuda_event_ipc_array[cuda_event_ipc_first_avail], ipcStream);
        if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
            opal_show_help("help-mpi-common-cuda.txt", "cuEventRecord failed",
                           true, OPAL_PROC_MY_HOSTNAME, result);
            OPAL_THREAD_UNLOCK(&common_cuda_ipc_lock);
            return OPAL_ERROR;
        }
        cuda_event_ipc_frag_array[cuda_event_ipc_first_avail] = frag;

        /* Bump up the first available slot and number used by 1 */
        cuda_event_ipc_first_avail++;
        if (cuda_event_ipc_first_avail >= cuda_event_max) {
            cuda_event_ipc_first_avail = 0;
        }
        cuda_event_ipc_num_used++;

        *done = 0;
    } else {
        /* Mimic the async function so they use the same memcpy call. */
        result = cuFunc.cuMemcpyAsync((CUdeviceptr)dst, (CUdeviceptr)src, amount, ipcStream);
        if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
            opal_show_help("help-mpi-common-cuda.txt", "cuMemcpyAsync failed",
                           true, dst, src, amount, result);
            OPAL_THREAD_UNLOCK(&common_cuda_ipc_lock);
            return OPAL_ERROR;
        } else {
            opal_output_verbose(20, mca_common_cuda_output,
                                "CUDA: cuMemcpyAsync passed: dst=%p, src=%p, size=%d",
                                dst, src, (int)amount);
        }

        /* Record an event, then wait for it to complete with calls to cuEventQuery */
        result = cuFunc.cuEventRecord(cuda_event_ipc_array[cuda_event_ipc_first_avail], ipcStream);
        if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
            opal_show_help("help-mpi-common-cuda.txt", "cuEventRecord failed",
                           true, OPAL_PROC_MY_HOSTNAME, result);
            OPAL_THREAD_UNLOCK(&common_cuda_ipc_lock);
            return OPAL_ERROR;
        }

        cuda_event_ipc_frag_array[cuda_event_ipc_first_avail] = frag;

        /* Bump up the first available slot and number used by 1 */
        cuda_event_ipc_first_avail++;
        if (cuda_event_ipc_first_avail >= cuda_event_max) {
            cuda_event_ipc_first_avail = 0;
        }
        cuda_event_ipc_num_used++;

        result = cuFunc.cuEventQuery(cuda_event_ipc_array[cuda_event_ipc_first_used]);
        if ((CUDA_SUCCESS != result) && (CUDA_ERROR_NOT_READY != result)) {
            opal_show_help("help-mpi-common-cuda.txt", "cuEventQuery failed",
                           true, result);
            OPAL_THREAD_UNLOCK(&common_cuda_ipc_lock);
            return OPAL_ERROR;
        }

        iter = 0;
        while (CUDA_ERROR_NOT_READY == result) {
            if (0 == (iter % 10)) {
                opal_output(-1, "EVENT NOT DONE (iter=%d)", iter);
            }
            result = cuFunc.cuEventQuery(cuda_event_ipc_array[cuda_event_ipc_first_used]);
            if ((CUDA_SUCCESS != result) && (CUDA_ERROR_NOT_READY != result)) {
                opal_show_help("help-mpi-common-cuda.txt", "cuEventQuery failed",
                               true, result);
            OPAL_THREAD_UNLOCK(&common_cuda_ipc_lock);
                return OPAL_ERROR;
            }
            iter++;
        }

        --cuda_event_ipc_num_used;
        ++cuda_event_ipc_first_used;
        if (cuda_event_ipc_first_used >= cuda_event_max) {
            cuda_event_ipc_first_used = 0;
        }
        *done = 1;
    }
    OPAL_THREAD_UNLOCK(&common_cuda_ipc_lock);
    return OPAL_SUCCESS;
}

/*
 * Record an event and save the frag.  This is called by the sending side and
 * is used to queue an event when a htod copy has been initiated.
 */
int mca_common_cuda_record_dtoh_event(char *msg, struct mca_btl_base_descriptor_t *frag)
{
    CUresult result;

    /* First make sure there is room to store the event.  If not, then
     * return an error.  The error message will tell the user to try and
     * run again, but with a larger array for storing events. */
    OPAL_THREAD_LOCK(&common_cuda_dtoh_lock);
    if (cuda_event_dtoh_num_used == cuda_event_max) {
        opal_show_help("help-mpi-common-cuda.txt", "Out of cuEvent handles",
                       true, cuda_event_max, cuda_event_max+100, cuda_event_max+100);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    if (cuda_event_dtoh_num_used > cuda_event_dtoh_most) {
        cuda_event_dtoh_most = cuda_event_dtoh_num_used;
        /* Just print multiples of 10 */
        if (0 == (cuda_event_dtoh_most % 10)) {
            opal_output_verbose(20, mca_common_cuda_output,
                                "Maximum DtoH events used is now %d", cuda_event_dtoh_most);
        }
    }

    result = cuFunc.cuEventRecord(cuda_event_dtoh_array[cuda_event_dtoh_first_avail], dtohStream);
    if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
        opal_show_help("help-mpi-common-cuda.txt", "cuEventRecord failed",
                       true, OPAL_PROC_MY_HOSTNAME, result);
        OPAL_THREAD_UNLOCK(&common_cuda_dtoh_lock);
        return OPAL_ERROR;
    }
    cuda_event_dtoh_frag_array[cuda_event_dtoh_first_avail] = frag;

    /* Bump up the first available slot and number used by 1 */
    cuda_event_dtoh_first_avail++;
    if (cuda_event_dtoh_first_avail >= cuda_event_max) {
        cuda_event_dtoh_first_avail = 0;
    }
    cuda_event_dtoh_num_used++;

    OPAL_THREAD_UNLOCK(&common_cuda_dtoh_lock);
    return OPAL_SUCCESS;
}

/*
 * Record an event and save the frag.  This is called by the receiving side and
 * is used to queue an event when a dtoh copy has been initiated.
 */
int mca_common_cuda_record_htod_event(char *msg, struct mca_btl_base_descriptor_t *frag)
{
    CUresult result;

    OPAL_THREAD_LOCK(&common_cuda_htod_lock);
    /* First make sure there is room to store the event.  If not, then
     * return an error.  The error message will tell the user to try and
     * run again, but with a larger array for storing events. */
    if (cuda_event_htod_num_used == cuda_event_max) {
        opal_show_help("help-mpi-common-cuda.txt", "Out of cuEvent handles",
                       true, cuda_event_max, cuda_event_max+100, cuda_event_max+100);
        OPAL_THREAD_UNLOCK(&common_cuda_htod_lock);
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    if (cuda_event_htod_num_used > cuda_event_htod_most) {
        cuda_event_htod_most = cuda_event_htod_num_used;
        /* Just print multiples of 10 */
        if (0 == (cuda_event_htod_most % 10)) {
            opal_output_verbose(20, mca_common_cuda_output,
                                "Maximum HtoD events used is now %d", cuda_event_htod_most);
        }
    }

    result = cuFunc.cuEventRecord(cuda_event_htod_array[cuda_event_htod_first_avail], htodStream);
    if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
        opal_show_help("help-mpi-common-cuda.txt", "cuEventRecord failed",
                       true, OPAL_PROC_MY_HOSTNAME, result);
        OPAL_THREAD_UNLOCK(&common_cuda_htod_lock);
        return OPAL_ERROR;
    }
    cuda_event_htod_frag_array[cuda_event_htod_first_avail] = frag;

   /* Bump up the first available slot and number used by 1 */
    cuda_event_htod_first_avail++;
    if (cuda_event_htod_first_avail >= cuda_event_max) {
        cuda_event_htod_first_avail = 0;
    }
    cuda_event_htod_num_used++;

    OPAL_THREAD_UNLOCK(&common_cuda_htod_lock);
    return OPAL_SUCCESS;
}

/**
 * Used to get the dtoh stream for initiating asynchronous copies.
 */
void *mca_common_cuda_get_dtoh_stream(void) {
    return (void *)dtohStream;
}

/**
 * Used to get the htod stream for initiating asynchronous copies.
 */
void *mca_common_cuda_get_htod_stream(void) {
    return (void *)htodStream;
}

/*
 * Function is called every time progress is called with the sm BTL.  If there
 * are outstanding events, check to see if one has completed.  If so, hand
 * back the fragment for further processing.
 */
int progress_one_cuda_ipc_event(struct mca_btl_base_descriptor_t **frag) {
    CUresult result;

    OPAL_THREAD_LOCK(&common_cuda_ipc_lock);
    if (cuda_event_ipc_num_used > 0) {
        opal_output_verbose(20, mca_common_cuda_output,
                           "CUDA: progress_one_cuda_ipc_event, outstanding_events=%d",
                            cuda_event_ipc_num_used);

        result = cuFunc.cuEventQuery(cuda_event_ipc_array[cuda_event_ipc_first_used]);

        /* We found an event that is not ready, so return. */
        if (CUDA_ERROR_NOT_READY == result) {
            opal_output_verbose(20, mca_common_cuda_output,
                                "CUDA: cuEventQuery returned CUDA_ERROR_NOT_READY");
            *frag = NULL;
            OPAL_THREAD_UNLOCK(&common_cuda_ipc_lock);
            return 0;
        } else if (CUDA_SUCCESS != result) {
            opal_show_help("help-mpi-common-cuda.txt", "cuEventQuery failed",
                           true, result);
            *frag = NULL;
            OPAL_THREAD_UNLOCK(&common_cuda_ipc_lock);
            return OPAL_ERROR;
        }

        *frag = cuda_event_ipc_frag_array[cuda_event_ipc_first_used];
        opal_output_verbose(10, mca_common_cuda_output,
                            "CUDA: cuEventQuery returned %d", result);

        /* Bump counters, loop around the circular buffer if necessary */
        --cuda_event_ipc_num_used;
        ++cuda_event_ipc_first_used;
        if (cuda_event_ipc_first_used >= cuda_event_max) {
            cuda_event_ipc_first_used = 0;
        }
        /* A return value of 1 indicates an event completed and a frag was returned */
        OPAL_THREAD_UNLOCK(&common_cuda_ipc_lock);
        return 1;
    }
    OPAL_THREAD_UNLOCK(&common_cuda_ipc_lock);
    return 0;
}

/**
 * Progress any dtoh event completions.
 */
int progress_one_cuda_dtoh_event(struct mca_btl_base_descriptor_t **frag) {
    CUresult result;

    OPAL_THREAD_LOCK(&common_cuda_dtoh_lock);
    if (cuda_event_dtoh_num_used > 0) {
        opal_output_verbose(30, mca_common_cuda_output,
                           "CUDA: progress_one_cuda_dtoh_event, outstanding_events=%d",
                            cuda_event_dtoh_num_used);

        result = cuFunc.cuEventQuery(cuda_event_dtoh_array[cuda_event_dtoh_first_used]);

        /* We found an event that is not ready, so return. */
        if (CUDA_ERROR_NOT_READY == result) {
            opal_output_verbose(30, mca_common_cuda_output,
                                "CUDA: cuEventQuery returned CUDA_ERROR_NOT_READY");
            *frag = NULL;
            OPAL_THREAD_UNLOCK(&common_cuda_dtoh_lock);
            return 0;
        } else if (CUDA_SUCCESS != result) {
            opal_show_help("help-mpi-common-cuda.txt", "cuEventQuery failed",
                           true, result);
            *frag = NULL;
            OPAL_THREAD_UNLOCK(&common_cuda_dtoh_lock);
            return OPAL_ERROR;
        }

        *frag = cuda_event_dtoh_frag_array[cuda_event_dtoh_first_used];
        opal_output_verbose(30, mca_common_cuda_output,
                            "CUDA: cuEventQuery returned %d", result);

        /* Bump counters, loop around the circular buffer if necessary */
        --cuda_event_dtoh_num_used;
        ++cuda_event_dtoh_first_used;
        if (cuda_event_dtoh_first_used >= cuda_event_max) {
            cuda_event_dtoh_first_used = 0;
        }
        /* A return value of 1 indicates an event completed and a frag was returned */
        OPAL_THREAD_UNLOCK(&common_cuda_dtoh_lock);
        return 1;
    }
    OPAL_THREAD_UNLOCK(&common_cuda_dtoh_lock);
    return 0;
}

/**
 * Progress any dtoh event completions.
 */
int progress_one_cuda_htod_event(struct mca_btl_base_descriptor_t **frag) {
    CUresult result;

    OPAL_THREAD_LOCK(&common_cuda_htod_lock);
    if (cuda_event_htod_num_used > 0) {
        opal_output_verbose(30, mca_common_cuda_output,
                           "CUDA: progress_one_cuda_htod_event, outstanding_events=%d",
                            cuda_event_htod_num_used);

        result = cuFunc.cuEventQuery(cuda_event_htod_array[cuda_event_htod_first_used]);

        /* We found an event that is not ready, so return. */
        if (CUDA_ERROR_NOT_READY == result) {
            opal_output_verbose(30, mca_common_cuda_output,
                                "CUDA: cuEventQuery returned CUDA_ERROR_NOT_READY");
            *frag = NULL;
            OPAL_THREAD_UNLOCK(&common_cuda_htod_lock);
            return 0;
        } else if (CUDA_SUCCESS != result) {
            opal_show_help("help-mpi-common-cuda.txt", "cuEventQuery failed",
                           true, result);
            *frag = NULL;
            OPAL_THREAD_UNLOCK(&common_cuda_htod_lock);
            return OPAL_ERROR;
        }

        *frag = cuda_event_htod_frag_array[cuda_event_htod_first_used];
        opal_output_verbose(30, mca_common_cuda_output,
                            "CUDA: cuEventQuery returned %d", result);

        /* Bump counters, loop around the circular buffer if necessary */
        --cuda_event_htod_num_used;
        ++cuda_event_htod_first_used;
        if (cuda_event_htod_first_used >= cuda_event_max) {
            cuda_event_htod_first_used = 0;
        }
        /* A return value of 1 indicates an event completed and a frag was returned */
        OPAL_THREAD_UNLOCK(&common_cuda_htod_lock);
        return 1;
    }
    OPAL_THREAD_UNLOCK(&common_cuda_htod_lock);
    return OPAL_ERR_RESOURCE_BUSY;
}


/**
 * Need to make sure the handle we are retrieving from the cache is still
 * valid.  Compare the cached handle to the one received.
 */
int mca_common_cuda_memhandle_matches(mca_rcache_common_cuda_reg_t *new_reg,
                                      mca_rcache_common_cuda_reg_t *old_reg)
{

    if (0 == memcmp(new_reg->data.memHandle, old_reg->data.memHandle, sizeof(new_reg->data.memHandle))) {
        return 1;
    } else {
        return 0;
    }

}

/*
 * Function to dump memory handle information.  This is based on
 * definitions from cuiinterprocess_private.h.
 */
static void cuda_dump_memhandle(int verbose, void *memHandle, char *str) {

    struct InterprocessMemHandleInternal
    {
        /* The first two entries are the CUinterprocessCtxHandle */
        int64_t ctxId; /* unique (within a process) id of the sharing context */
        int     pid;   /* pid of sharing context */

        int64_t size;
        int64_t blocksize;
        int64_t offset;
        int     gpuId;
        int     subDeviceIndex;
        int64_t serial;
    } memH;

    if (NULL == str) {
        str = "CUDA";
    }
    memcpy(&memH, memHandle, sizeof(memH));
    opal_output_verbose(verbose, mca_common_cuda_output,
                        "%s:ctxId=0x%" PRIx64 ", pid=%d, size=%" PRIu64 ", blocksize=%" PRIu64 ", offset=%"
                        PRIu64 ", gpuId=%d, subDeviceIndex=%d, serial=%" PRIu64,
                        str, memH.ctxId, memH.pid, memH.size, memH.blocksize, memH.offset,
                        memH.gpuId, memH.subDeviceIndex, memH.serial);
}

/*
 * Function to dump memory handle information.  This is based on
 * definitions from cuiinterprocess_private.h.
 */
static void cuda_dump_evthandle(int verbose, void *evtHandle, char *str) {

    struct InterprocessEventHandleInternal
    {
        unsigned long pid;
        unsigned long serial;
        int index;
    } evtH;

    if (NULL == str) {
        str = "CUDA";
    }
    memcpy(&evtH, evtHandle, sizeof(evtH));
    opal_output_verbose(verbose, mca_common_cuda_output,
                        "CUDA: %s:pid=%lu, serial=%lu, index=%d",
                        str, evtH.pid, evtH.serial, evtH.index);
}


/* Return microseconds of elapsed time. Microseconds are relevant when
 * trying to understand the fixed overhead of the communication. Used
 * when trying to time various functions.
 *
 * Cut and past the following to get timings where wanted.
 *
 *   clock_gettime(CLOCK_MONOTONIC, &ts_start);
 *   FUNCTION OF INTEREST
 *   clock_gettime(CLOCK_MONOTONIC, &ts_end);
 *   accum = mydifftime(ts_start, ts_end);
 *   opal_output(0, "Function took   %7.2f usecs\n", accum);
 *
 */
#if OPAL_ENABLE_DEBUG
static float mydifftime(opal_timer_t ts_start, opal_timer_t ts_end) {
    return (ts_end - ts_start);
}
#endif /* OPAL_ENABLE_DEBUG */

/* Routines that get plugged into the opal datatype code */
static int mca_common_cuda_is_gpu_buffer(const void *pUserBuf, opal_convertor_t *convertor)
{
    int res;
    CUmemorytype memType = 0;
    CUdeviceptr dbuf = (CUdeviceptr)pUserBuf;
    CUcontext ctx = NULL, memCtx = NULL;
#if OPAL_CUDA_GET_ATTRIBUTES
    uint32_t isManaged = 0;
    /* With CUDA 7.0, we can get multiple attributes with a single call */
    CUpointer_attribute attributes[3] = {CU_POINTER_ATTRIBUTE_MEMORY_TYPE,
                                         CU_POINTER_ATTRIBUTE_CONTEXT,
                                         CU_POINTER_ATTRIBUTE_IS_MANAGED};
    void *attrdata[] = {(void *)&memType, (void *)&memCtx, (void *)&isManaged};

    res = cuFunc.cuPointerGetAttributes(3, attributes, attrdata, dbuf);
    OPAL_OUTPUT_VERBOSE((101, mca_common_cuda_output,
                        "dbuf=%p, memType=%d, memCtx=%p, isManaged=%d, res=%d",
                         (void *)dbuf, (int)memType, (void *)memCtx, isManaged, res));

    /* Mark unified memory buffers with a flag.  This will allow all unified
     * memory to be forced through host buffers.  Note that this memory can
     * be either host or device so we need to set this flag prior to that check. */
    if (1 == isManaged) {
        if (NULL != convertor) {
            convertor->flags |= CONVERTOR_CUDA_UNIFIED;
        }
    }
    if (res != CUDA_SUCCESS) {
        /* If we cannot determine it is device pointer,
         * just assume it is not. */
        return 0;
    } else if (memType == CU_MEMORYTYPE_HOST) {
        /* Host memory, nothing to do here */
        return 0;
    } else if (memType == 0) {
        /* This can happen when CUDA is initialized but dbuf is not valid CUDA pointer */
        return 0;
    }
    /* Must be a device pointer */
    assert(memType == CU_MEMORYTYPE_DEVICE);
#else /* OPAL_CUDA_GET_ATTRIBUTES */
    res = cuFunc.cuPointerGetAttribute(&memType,
                                       CU_POINTER_ATTRIBUTE_MEMORY_TYPE, dbuf);
    if (res != CUDA_SUCCESS) {
        /* If we cannot determine it is device pointer,
         * just assume it is not. */
        return 0;
    } else if (memType == CU_MEMORYTYPE_HOST) {
        /* Host memory, nothing to do here */
        return 0;
    }
    /* Must be a device pointer */
    assert(memType == CU_MEMORYTYPE_DEVICE);
#endif /* OPAL_CUDA_GET_ATTRIBUTES */

    /* This piece of code was added in to handle in a case involving
     * OMP threads.  The user had initialized CUDA and then spawned
     * two threads.  The first thread had the CUDA context, but the
     * second thread did not.  We therefore had no context to act upon
     * and future CUDA driver calls would fail.  Therefore, if we have
     * GPU memory, but no context, get the context from the GPU memory
     * and set the current context to that.  It is rare that we will not
     * have a context. */
    res = cuFunc.cuCtxGetCurrent(&ctx);
    if (OPAL_UNLIKELY(NULL == ctx)) {
        if (CUDA_SUCCESS == res) {
#if !OPAL_CUDA_GET_ATTRIBUTES
            res = cuFunc.cuPointerGetAttribute(&memCtx,
                                               CU_POINTER_ATTRIBUTE_CONTEXT, dbuf);
            if (OPAL_UNLIKELY(res != CUDA_SUCCESS)) {
                opal_output(0, "CUDA: error calling cuPointerGetAttribute: "
                            "res=%d, ptr=%p aborting...", res, pUserBuf);
                return OPAL_ERROR;
            }
#endif /* OPAL_CUDA_GET_ATTRIBUTES */
            res = cuFunc.cuCtxSetCurrent(memCtx);
            if (OPAL_UNLIKELY(res != CUDA_SUCCESS)) {
                opal_output(0, "CUDA: error calling cuCtxSetCurrent: "
                            "res=%d, ptr=%p aborting...", res, pUserBuf);
                return OPAL_ERROR;
            } else {
                OPAL_OUTPUT_VERBOSE((10, mca_common_cuda_output,
                                     "CUDA: cuCtxSetCurrent passed: ptr=%p", pUserBuf));
            }
        } else {
            /* Print error and proceed */
            opal_output(0, "CUDA: error calling cuCtxGetCurrent: "
                        "res=%d, ptr=%p aborting...", res, pUserBuf);
            return OPAL_ERROR;
        }
    }

    /* WORKAROUND - They are times when the above code determines a pice of memory
     * is GPU memory, but it actually is not.  That has been seen on multi-GPU systems
     * with 6 or 8 GPUs on them. Therefore, we will do this extra check.  Note if we
     * made it this far, then the assumption at this point is we have GPU memory.
     * Unfotunately, this extra call is costing us another 100 ns almost doubling
     * the cost of this entire function. */
    if (OPAL_LIKELY(mca_common_cuda_gpu_mem_check_workaround)) {
        CUdeviceptr pbase;
        size_t psize;
        res = cuFunc.cuMemGetAddressRange(&pbase, &psize, dbuf);
        if (CUDA_SUCCESS != res) {
            opal_output_verbose(5, mca_common_cuda_output,
                                "CUDA: cuMemGetAddressRange failed on this pointer: res=%d, buf=%p "
                                "Overriding check and setting to host pointer. ",
                              res, (void *)dbuf);
            /* This cannot be GPU memory if the previous call failed */
            return 0;
        }
    }

    /* First access on a device pointer finalizes CUDA support initialization.
     * If initialization fails, disable support. */
    if (!stage_three_init_complete) {
        if (0 != mca_common_cuda_stage_three_init()) {
            opal_cuda_support = 0;
        }
    }

    return 1;
}

static int mca_common_cuda_cu_memcpy_async(void *dest, const void *src, size_t size,
                                         opal_convertor_t* convertor)
{
    return cuFunc.cuMemcpyAsync((CUdeviceptr)dest, (CUdeviceptr)src, size,
                                (CUstream)convertor->stream);
}

/**
 * This function is plugged into various areas where a cuMemcpy would be called.
 * This is a synchronous operation that will not return until the copy is complete.
 */
static int mca_common_cuda_cu_memcpy(void *dest, const void *src, size_t size)
{
    CUresult result;
#if OPAL_ENABLE_DEBUG
    CUmemorytype memTypeSrc, memTypeDst;
    if (OPAL_UNLIKELY(mca_common_cuda_cumemcpy_timing)) {
        /* Nice to know type of source and destination for timing output. Do
         * not care about return code as memory type will just be set to 0 */
        result = cuFunc.cuPointerGetAttribute(&memTypeDst,
                                              CU_POINTER_ATTRIBUTE_MEMORY_TYPE, (CUdeviceptr)dest);
        result = cuFunc.cuPointerGetAttribute(&memTypeSrc,
                                              CU_POINTER_ATTRIBUTE_MEMORY_TYPE, (CUdeviceptr)src);
        ts_start = opal_timer_base_get_usec();
    }
#endif
    if (mca_common_cuda_cumemcpy_async) {
        result = cuFunc.cuMemcpyAsync((CUdeviceptr)dest, (CUdeviceptr)src, size, memcpyStream);
        if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
            opal_show_help("help-mpi-common-cuda.txt", "cuMemcpyAsync failed",
                           true, dest, src, size, result);
            return OPAL_ERROR;
        }
        result = cuFunc.cuStreamSynchronize(memcpyStream);
        if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
            opal_show_help("help-mpi-common-cuda.txt", "cuStreamSynchronize failed",
                           true, OPAL_PROC_MY_HOSTNAME, result);
            return OPAL_ERROR;
        }
    } else {
         result = cuFunc.cuMemcpy((CUdeviceptr)dest, (CUdeviceptr)src, size);
         if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
             opal_show_help("help-mpi-common-cuda.txt", "cuMemcpy failed",
                            true, OPAL_PROC_MY_HOSTNAME, result);
             return OPAL_ERROR;
         }
    }
#if OPAL_ENABLE_DEBUG
    if (OPAL_UNLIKELY(mca_common_cuda_cumemcpy_timing)) {
        ts_end = opal_timer_base_get_usec();
        accum = mydifftime(ts_start, ts_end);
        if (mca_common_cuda_cumemcpy_async) {
            opal_output(0, "cuMemcpyAsync took   %7.2f usecs, size=%d, (src=%p (%d), dst=%p (%d))\n",
                        accum, (int)size, src, memTypeSrc, dest, memTypeDst);
        } else {
            opal_output(0, "cuMemcpy took   %7.2f usecs, size=%d,  (src=%p (%d), dst=%p (%d))\n",
                        accum, (int)size, src, memTypeSrc, dest, memTypeDst);
        }
    }
#endif
    return OPAL_SUCCESS;
}

static int mca_common_cuda_memmove(void *dest, void *src, size_t size)
{
    CUdeviceptr tmp;
    int result;

    result = cuFunc.cuMemAlloc(&tmp,size);
    if (mca_common_cuda_cumemcpy_async) {
        result = cuFunc.cuMemcpyAsync(tmp, (CUdeviceptr)src, size, memcpyStream);
        if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
            opal_show_help("help-mpi-common-cuda.txt", "cuMemcpyAsync failed",
                           true, tmp, src, size, result);
            return OPAL_ERROR;
        }
        result = cuFunc.cuMemcpyAsync((CUdeviceptr)dest, tmp, size, memcpyStream);
        if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
            opal_show_help("help-mpi-common-cuda.txt", "cuMemcpyAsync failed",
                           true, dest, tmp, size, result);
            return OPAL_ERROR;
        }
        result = cuFunc.cuStreamSynchronize(memcpyStream);
        if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
            opal_show_help("help-mpi-common-cuda.txt", "cuStreamSynchronize failed",
                           true, OPAL_PROC_MY_HOSTNAME, result);
            return OPAL_ERROR;
        }
    } else {
        result = cuFunc.cuMemcpy(tmp, (CUdeviceptr)src, size);
        if (OPAL_UNLIKELY(result != CUDA_SUCCESS)) {
            opal_output(0, "CUDA: memmove-Error in cuMemcpy: res=%d, dest=%p, src=%p, size=%d",
                        result, (void *)tmp, src, (int)size);
            return OPAL_ERROR;
        }
        result = cuFunc.cuMemcpy((CUdeviceptr)dest, tmp, size);
        if (OPAL_UNLIKELY(result != CUDA_SUCCESS)) {
            opal_output(0, "CUDA: memmove-Error in cuMemcpy: res=%d, dest=%p, src=%p, size=%d",
                        result, dest, (void *)tmp, (int)size);
            return OPAL_ERROR;
        }
    }
    cuFunc.cuMemFree(tmp);
    return OPAL_SUCCESS;
}

int mca_common_cuda_get_device(int *devicenum)
{
    CUdevice cuDev;
    int res;

    res = cuFunc.cuCtxGetDevice(&cuDev);
    if (OPAL_UNLIKELY(res != CUDA_SUCCESS)) {
        opal_output(0, "CUDA: cuCtxGetDevice failed: res=%d",
                    res);
        return res;
    }
    *devicenum = cuDev;
    return 0;
}

int mca_common_cuda_device_can_access_peer(int *access, int dev1, int dev2)
{
    int res;
    res = cuFunc.cuDeviceCanAccessPeer(access, (CUdevice)dev1, (CUdevice)dev2);
    if (OPAL_UNLIKELY(res != CUDA_SUCCESS)) {
        opal_output(0, "CUDA: cuDeviceCanAccessPeer failed: res=%d",
                    res);
        return res;
    }
    return 0;
}

int mca_common_cuda_get_address_range(void *pbase, size_t *psize, void *base)
{
    CUresult result;
    result = cuFunc.cuMemGetAddressRange((CUdeviceptr *)pbase, psize, (CUdeviceptr)base);
    if (OPAL_UNLIKELY(CUDA_SUCCESS != result)) {
        opal_show_help("help-mpi-common-cuda.txt", "cuMemGetAddressRange failed 2",
                       true, OPAL_PROC_MY_HOSTNAME, result, base);
        return OPAL_ERROR;
    } else {
        opal_output_verbose(50, mca_common_cuda_output,
                            "CUDA: cuMemGetAddressRange passed: addr=%p, pbase=%p, psize=%lu ",
                            base, *(char **)pbase, *psize);
    }
    return 0;
}

#if OPAL_CUDA_GDR_SUPPORT
/* Check to see if the memory was freed between the time it was stored in
 * the registration cache and now.  Return true if the memory was previously
 * freed.  This is indicated by the BUFFER_ID value in the registration cache
 * not matching the BUFFER_ID of the buffer we are checking.  Return false
 * if the registration is still good.
 */
bool mca_common_cuda_previously_freed_memory(mca_rcache_base_registration_t *reg)
{
    int res;
    unsigned long long bufID;
    unsigned char *dbuf = reg->base;

    res = cuFunc.cuPointerGetAttribute(&bufID, CU_POINTER_ATTRIBUTE_BUFFER_ID,
                                       (CUdeviceptr)dbuf);
    /* If we cannot determine the BUFFER_ID, then print a message and default
     * to forcing the registration to be kicked out. */
    if (OPAL_UNLIKELY(res != CUDA_SUCCESS)) {
        opal_show_help("help-mpi-common-cuda.txt", "bufferID failed",
                       true, OPAL_PROC_MY_HOSTNAME, res);
        return true;
    }
    opal_output_verbose(50, mca_common_cuda_output,
                        "CUDA: base=%p, bufID=%llu, reg->gpu_bufID=%llu, %s", dbuf, bufID, reg->gpu_bufID,
                        (reg->gpu_bufID == bufID ? "BUFFER_ID match":"BUFFER_ID do not match"));
    if (bufID != reg->gpu_bufID) {
        return true;
    } else {
        return false;
    }
}

/*
 * Get the buffer ID from the memory and store it in the registration.
 * This is needed to ensure the cached registration is not stale.  If
 * we fail to get buffer ID, print an error and set buffer ID to 0.
 * Also set SYNC_MEMOPS on any GPU registration to ensure that
 * synchronous copies complete before the buffer is accessed.
 */
void mca_common_cuda_get_buffer_id(mca_rcache_base_registration_t *reg)
{
    int res;
    unsigned long long bufID = 0;
    unsigned char *dbuf = reg->base;
    int enable = 1;

    res = cuFunc.cuPointerGetAttribute(&bufID, CU_POINTER_ATTRIBUTE_BUFFER_ID,
                                       (CUdeviceptr)dbuf);
    if (OPAL_UNLIKELY(res != CUDA_SUCCESS)) {
        opal_show_help("help-mpi-common-cuda.txt", "bufferID failed",
                       true, OPAL_PROC_MY_HOSTNAME, res);
    }
    reg->gpu_bufID = bufID;

    res = cuFunc.cuPointerSetAttribute(&enable, CU_POINTER_ATTRIBUTE_SYNC_MEMOPS,
                                       (CUdeviceptr)dbuf);
    if (OPAL_UNLIKELY(CUDA_SUCCESS != res)) {
        opal_show_help("help-mpi-common-cuda.txt", "cuPointerSetAttribute failed",
                       true, OPAL_PROC_MY_HOSTNAME, res, dbuf);
    }
}
#endif /* OPAL_CUDA_GDR_SUPPORT */
