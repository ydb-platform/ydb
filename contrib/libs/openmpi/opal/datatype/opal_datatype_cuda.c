/*
 * Copyright (c) 2011-2014 NVIDIA Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"

#include <errno.h>
#include <string.h>
#include <unistd.h>

#include "opal/align.h"
#include "opal/util/output.h"
#include "opal/datatype/opal_convertor.h"
#include "opal/datatype/opal_datatype_cuda.h"

static bool initialized = false;
int opal_cuda_verbose = 0;
static int opal_cuda_enabled = 0; /* Starts out disabled */
static int opal_cuda_output = 0;
static void opal_cuda_support_init(void);
static int (*common_cuda_initialization_function)(opal_common_cuda_function_table_t *) = NULL;
static opal_common_cuda_function_table_t ftable;

/* This function allows the common cuda code to register an
 * initialization function that gets called the first time an attempt
 * is made to send or receive a GPU pointer.  This allows us to delay
 * some CUDA initialization until after MPI_Init().
 */
void opal_cuda_add_initialization_function(int (*fptr)(opal_common_cuda_function_table_t *)) {
    common_cuda_initialization_function = fptr;
}

/**
 * This function is called when a convertor is instantiated.  It has to call
 * the opal_cuda_support_init() function once to figure out if CUDA support
 * is enabled or not.  If CUDA is not enabled, then short circuit out
 * for all future calls.
 */
void mca_cuda_convertor_init(opal_convertor_t* convertor, const void *pUserBuf)
{
    /* Only do the initialization on the first GPU access */
    if (!initialized) {
        opal_cuda_support_init();
    }

    /* This is needed to handle case where convertor is not fully initialized
     * like when trying to do a sendi with convertor on the statck */
    convertor->cbmemcpy = (memcpy_fct_t)&opal_cuda_memcpy;

    /* If not enabled, then nothing else to do */
    if (!opal_cuda_enabled) {
        return;
    }

    if (ftable.gpu_is_gpu_buffer(pUserBuf, convertor)) {
        convertor->flags |= CONVERTOR_CUDA;
    }
}

/* Checks the type of pointer
 *
 * @param dest   One pointer to check
 * @param source Another pointer to check
 */
bool opal_cuda_check_bufs(char *dest, char *src)
{
    /* Only do the initialization on the first GPU access */
    if (!initialized) {
        opal_cuda_support_init();
    }

    if (!opal_cuda_enabled) {
        return false;
    }

    if (ftable.gpu_is_gpu_buffer(dest, NULL) || ftable.gpu_is_gpu_buffer(src, NULL)) {
        return true;
    } else {
        return false;
    }
}

/*
 * With CUDA enabled, all contiguous copies will pass through this function.
 * Therefore, the first check is to see if the convertor is a GPU buffer.
 * Note that if there is an error with any of the CUDA calls, the program
 * aborts as there is no recovering.
 */

/* Checks the type of pointer
 *
 * @param buf   check one pointer providing a convertor.
 *  Provides aditional information, e.g. managed vs. unmanaged GPU buffer
 */
bool  opal_cuda_check_one_buf(char *buf, opal_convertor_t *convertor )
{
    /* Only do the initialization on the first GPU access */
    if (!initialized) {
        opal_cuda_support_init();
    }

    if (!opal_cuda_enabled) {
        return false;
    }

    return ( ftable.gpu_is_gpu_buffer(buf, convertor));
}

/*
 * With CUDA enabled, all contiguous copies will pass through this function.
 * Therefore, the first check is to see if the convertor is a GPU buffer.
 * Note that if there is an error with any of the CUDA calls, the program
 * aborts as there is no recovering.
 */

void *opal_cuda_memcpy(void *dest, const void *src, size_t size, opal_convertor_t* convertor)
{
    int res;

    if (!(convertor->flags & CONVERTOR_CUDA)) {
        return memcpy(dest, src, size);
    }

    if (convertor->flags & CONVERTOR_CUDA_ASYNC) {
        res = ftable.gpu_cu_memcpy_async(dest, (void *)src, size, convertor);
    } else {
        res = ftable.gpu_cu_memcpy(dest, (void *)src, size);
    }

    if (res != 0) {
        opal_output(0, "CUDA: Error in cuMemcpy: res=%d, dest=%p, src=%p, size=%d",
                    res, dest, src, (int)size);
        abort();
    } else {
        return dest;
    }
}

/*
 * This function is needed in cases where we do not have contiguous
 * datatypes.  The current code has macros that cannot handle a convertor
 * argument to the memcpy call.
 */
void *opal_cuda_memcpy_sync(void *dest, const void *src, size_t size)
{
    int res;
    res = ftable.gpu_cu_memcpy(dest, src, size);
    if (res != 0) {
        opal_output(0, "CUDA: Error in cuMemcpy: res=%d, dest=%p, src=%p, size=%d",
                    res, dest, src, (int)size);
        abort();
    } else {
        return dest;
    }
}

/*
 * In some cases, need an implementation of memmove.  This is not fast, but
 * it is not often needed.
 */
void *opal_cuda_memmove(void *dest, void *src, size_t size)
{
    int res;

    res = ftable.gpu_memmove(dest, src, size);
    if(res != 0){
        opal_output(0, "CUDA: Error in gpu memmove: res=%d, dest=%p, src=%p, size=%d",
                    res, dest, src, (int)size);
        abort();
    }
    return dest;
}

/**
 * This function gets called once to check if the program is running in a cuda
 * environment.
 */
static void opal_cuda_support_init(void)
{
    if (initialized) {
        return;
    }

    /* Set different levels of verbosity in the cuda related code. */
    opal_cuda_output = opal_output_open(NULL);
    opal_output_set_verbosity(opal_cuda_output, opal_cuda_verbose);

    /* Callback into the common cuda initialization routine. This is only
     * set if some work had been done already in the common cuda code.*/
    if (NULL != common_cuda_initialization_function) {
        if (0 == common_cuda_initialization_function(&ftable)) {
            opal_cuda_enabled = 1;
        }
    }

    if (1 == opal_cuda_enabled) {
        opal_output_verbose(10, opal_cuda_output,
                            "CUDA: enabled successfully, CUDA device pointers will work");
    } else {
        opal_output_verbose(10, opal_cuda_output,
                            "CUDA: not enabled, CUDA device pointers will not work");
    }

    initialized = true;
}

/**
 * Tell the convertor that copies will be asynchronous CUDA copies.  The
 * flags are cleared when the convertor is reinitialized.
 */
void opal_cuda_set_copy_function_async(opal_convertor_t* convertor, void *stream)
{
    convertor->flags |= CONVERTOR_CUDA_ASYNC;
    convertor->stream = stream;
}
