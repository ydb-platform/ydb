/*
 * Copyright (c) 2011-2014 NVIDIA Corporation.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef _OPAL_DATATYPE_CUDA_H
#define _OPAL_DATATYPE_CUDA_H

/* Structure to hold CUDA support functions that gets filled in when the
 * common cuda code is initialized.  This removes any dependency on <cuda.h>
 * in the opal cuda datatype code. */
struct opal_common_cuda_function_table {
    int (*gpu_is_gpu_buffer)(const void*, opal_convertor_t*);
    int (*gpu_cu_memcpy_async)(void*, const void*, size_t, opal_convertor_t*);
    int (*gpu_cu_memcpy)(void*, const void*, size_t);
    int (*gpu_memmove)(void*, void*, size_t);
};
typedef struct opal_common_cuda_function_table opal_common_cuda_function_table_t;

void mca_cuda_convertor_init(opal_convertor_t* convertor, const void *pUserBuf);
bool opal_cuda_check_bufs(char *dest, char *src);
bool opal_cuda_check_one_buf(char *buf, opal_convertor_t *convertor );
void* opal_cuda_memcpy(void * dest, const void * src, size_t size, opal_convertor_t* convertor);
void* opal_cuda_memcpy_sync(void * dest, const void * src, size_t size);
void* opal_cuda_memmove(void * dest, void * src, size_t size);
void opal_cuda_add_initialization_function(int (*fptr)(opal_common_cuda_function_table_t *));
void opal_cuda_set_copy_function_async(opal_convertor_t* convertor, void *stream);

#endif
