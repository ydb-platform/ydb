/*
 * Copyright 1993-2015 NVIDIA Corporation.  All rights reserved.
 *
 * NOTICE TO LICENSEE:
 *
 * This source code and/or documentation ("Licensed Deliverables") are
 * subject to NVIDIA intellectual property rights under U.S. and
 * international Copyright laws.
 *
 * These Licensed Deliverables contained herein is PROPRIETARY and
 * CONFIDENTIAL to NVIDIA and is being provided under the terms and
 * conditions of a form of NVIDIA software license agreement by and
 * between NVIDIA and Licensee ("License Agreement") or electronically
 * accepted by Licensee.  Notwithstanding any terms or conditions to
 * the contrary in the License Agreement, reproduction or disclosure
 * of the Licensed Deliverables to any third party without the express
 * written consent of NVIDIA is prohibited.
 *
 * NOTWITHSTANDING ANY TERMS OR CONDITIONS TO THE CONTRARY IN THE
 * LICENSE AGREEMENT, NVIDIA MAKES NO REPRESENTATION ABOUT THE
 * SUITABILITY OF THESE LICENSED DELIVERABLES FOR ANY PURPOSE.  IT IS
 * PROVIDED "AS IS" WITHOUT EXPRESS OR IMPLIED WARRANTY OF ANY KIND.
 * NVIDIA DISCLAIMS ALL WARRANTIES WITH REGARD TO THESE LICENSED
 * DELIVERABLES, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY,
 * NONINFRINGEMENT, AND FITNESS FOR A PARTICULAR PURPOSE.
 * NOTWITHSTANDING ANY TERMS OR CONDITIONS TO THE CONTRARY IN THE
 * LICENSE AGREEMENT, IN NO EVENT SHALL NVIDIA BE LIABLE FOR ANY
 * SPECIAL, INDIRECT, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, OR ANY
 * DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS,
 * WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
 * ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE
 * OF THESE LICENSED DELIVERABLES.
 *
 * U.S. Government End Users.  These Licensed Deliverables are a
 * "commercial item" as that term is defined at 48 C.F.R. 2.101 (OCT
 * 1995), consisting of "commercial computer software" and "commercial
 * computer software documentation" as such terms are used in 48
 * C.F.R. 12.212 (SEPT 1995) and is provided to the U.S. Government
 * only as a commercial end item.  Consistent with 48 C.F.R.12.212 and
 * 48 C.F.R. 227.7202-1 through 227.7202-4 (JUNE 1995), all
 * U.S. Government End Users acquire the Licensed Deliverables with
 * only those rights set forth herein.
 *
 * Any use of the Licensed Deliverables in individual and commercial
 * software must include, in the user documentation and internal
 * comments to the code, the above Disclaimer and U.S. Government End
 * Users Notice.
 */

/*   cudnn : Neural Networks Library

*/

#if !defined(CUDNN_H_)
#define CUDNN_H_

#define CUDNN_MAJOR 7
#define CUDNN_MINOR 6
#define CUDNN_PATCHLEVEL 5

#define CUDNN_VERSION (CUDNN_MAJOR * 1000 + CUDNN_MINOR * 100 + CUDNN_PATCHLEVEL)

#include "driver_types.h"
#include <cuda_runtime.h>
#include <stdint.h>

#ifndef CUDNNWINAPI
#ifdef _WIN32
#define CUDNNWINAPI __stdcall
#else
#define CUDNNWINAPI
#endif
#endif

#if defined(__cplusplus)
extern "C" {
#endif

struct cudnnContext;
typedef struct cudnnContext *cudnnHandle_t;

size_t CUDNNWINAPI
cudnnGetVersion(void);

/* Returns CUDA Runtime version statically linked against cudnn */
size_t CUDNNWINAPI
cudnnGetCudartVersion(void);

/*
 * CUDNN return codes
 */
typedef enum {
    CUDNN_STATUS_SUCCESS                      = 0,
    CUDNN_STATUS_NOT_INITIALIZED              = 1,
    CUDNN_STATUS_ALLOC_FAILED                 = 2,
    CUDNN_STATUS_BAD_PARAM                    = 3,
    CUDNN_STATUS_INTERNAL_ERROR               = 4,
    CUDNN_STATUS_INVALID_VALUE                = 5,
    CUDNN_STATUS_ARCH_MISMATCH                = 6,
    CUDNN_STATUS_MAPPING_ERROR                = 7,
    CUDNN_STATUS_EXECUTION_FAILED             = 8,
    CUDNN_STATUS_NOT_SUPPORTED                = 9,
    CUDNN_STATUS_LICENSE_ERROR                = 10,
    CUDNN_STATUS_RUNTIME_PREREQUISITE_MISSING = 11,
    CUDNN_STATUS_RUNTIME_IN_PROGRESS          = 12,
    CUDNN_STATUS_RUNTIME_FP_OVERFLOW          = 13,
} cudnnStatus_t;

/* human-readable error messages */
const char *CUDNNWINAPI
cudnnGetErrorString(cudnnStatus_t status);

/* Forward definition in this version only */
typedef struct cudnnRuntimeTag_t cudnnRuntimeTag_t;

typedef enum {
    CUDNN_ERRQUERY_RAWCODE     = 0,
    CUDNN_ERRQUERY_NONBLOCKING = 1,
    CUDNN_ERRQUERY_BLOCKING    = 2,
} cudnnErrQueryMode_t;

cudnnStatus_t CUDNNWINAPI
cudnnQueryRuntimeError(cudnnHandle_t handle, cudnnStatus_t *rstatus, cudnnErrQueryMode_t mode, cudnnRuntimeTag_t *tag);

#ifndef __LIBRARY_TYPES_H__

typedef enum libraryPropertyType_t { MAJOR_VERSION, MINOR_VERSION, PATCH_LEVEL } libraryPropertyType;

#endif

cudnnStatus_t CUDNNWINAPI
cudnnGetProperty(libraryPropertyType type, int *value);

cudnnStatus_t CUDNNWINAPI
cudnnCreate(cudnnHandle_t *handle);
cudnnStatus_t CUDNNWINAPI
cudnnDestroy(cudnnHandle_t handle);
cudnnStatus_t CUDNNWINAPI
cudnnSetStream(cudnnHandle_t handle, cudaStream_t streamId);
cudnnStatus_t CUDNNWINAPI
cudnnGetStream(cudnnHandle_t handle, cudaStream_t *streamId);

/* Data structures to represent Image/Filter and the Neural Network Layer */
typedef struct cudnnTensorStruct *cudnnTensorDescriptor_t;
typedef struct cudnnConvolutionStruct *cudnnConvolutionDescriptor_t;
typedef struct cudnnPoolingStruct *cudnnPoolingDescriptor_t;
typedef struct cudnnFilterStruct *cudnnFilterDescriptor_t;
typedef struct cudnnLRNStruct *cudnnLRNDescriptor_t;
typedef struct cudnnActivationStruct *cudnnActivationDescriptor_t;
typedef struct cudnnSpatialTransformerStruct *cudnnSpatialTransformerDescriptor_t;
typedef struct cudnnOpTensorStruct *cudnnOpTensorDescriptor_t;
typedef struct cudnnReduceTensorStruct *cudnnReduceTensorDescriptor_t;
typedef struct cudnnCTCLossStruct *cudnnCTCLossDescriptor_t;
typedef struct cudnnTensorTransformStruct *cudnnTensorTransformDescriptor_t;
/*
* CUDNN data type
*/
typedef enum {
    CUDNN_DATA_FLOAT   = 0,
    CUDNN_DATA_DOUBLE  = 1,
    CUDNN_DATA_HALF    = 2,
    CUDNN_DATA_INT8    = 3,
    CUDNN_DATA_INT32   = 4,
    CUDNN_DATA_INT8x4  = 5,
    CUDNN_DATA_UINT8   = 6,
    CUDNN_DATA_UINT8x4 = 7,
    CUDNN_DATA_INT8x32 = 8,
} cudnnDataType_t;

/*
* CUDNN math type
*/
typedef enum {
    CUDNN_DEFAULT_MATH                    = 0,
    CUDNN_TENSOR_OP_MATH                  = 1,
    CUDNN_TENSOR_OP_MATH_ALLOW_CONVERSION = 2,
} cudnnMathType_t;

/*
 * CUDNN propagate Nan
 */
typedef enum {
    CUDNN_NOT_PROPAGATE_NAN = 0,
    CUDNN_PROPAGATE_NAN     = 1,
} cudnnNanPropagation_t;

/*
 * CUDNN Determinism
 */
typedef enum {
    CUDNN_NON_DETERMINISTIC = 0,
    CUDNN_DETERMINISTIC     = 1,
} cudnnDeterminism_t;

/*
 * CUDNN Reorder
 */
typedef enum {
    CUDNN_DEFAULT_REORDER = 0,
    CUDNN_NO_REORDER      = 1,
} cudnnReorderType_t;

/* Maximum supported number of tensor dimensions */
#define CUDNN_DIM_MAX 8

/* Create an instance of a generic Tensor descriptor */
cudnnStatus_t CUDNNWINAPI
cudnnCreateTensorDescriptor(cudnnTensorDescriptor_t *tensorDesc);

typedef enum {
    CUDNN_TENSOR_NCHW        = 0, /* row major (wStride = 1, hStride = w) */
    CUDNN_TENSOR_NHWC        = 1, /* feature maps interleaved ( cStride = 1 )*/
    CUDNN_TENSOR_NCHW_VECT_C = 2, /* each image point is vector of element of C, vector length in data type */
} cudnnTensorFormat_t;

cudnnStatus_t CUDNNWINAPI
cudnnSetTensor4dDescriptor(cudnnTensorDescriptor_t tensorDesc,
                           cudnnTensorFormat_t format,
                           cudnnDataType_t dataType, /* image data type */
                           int n,                    /* number of inputs (batch size) */
                           int c,                    /* number of input feature maps */
                           int h,                    /* height of input section */
                           int w);                   /* width of input section */

cudnnStatus_t CUDNNWINAPI
cudnnSetTensor4dDescriptorEx(cudnnTensorDescriptor_t tensorDesc,
                             cudnnDataType_t dataType, /* image data type */
                             int n,                    /* number of inputs (batch size) */
                             int c,                    /* number of input feature maps */
                             int h,                    /* height of input section */
                             int w,                    /* width of input section */
                             int nStride,
                             int cStride,
                             int hStride,
                             int wStride);

cudnnStatus_t CUDNNWINAPI
cudnnGetTensor4dDescriptor(const cudnnTensorDescriptor_t tensorDesc,
                           cudnnDataType_t *dataType, /* image data type */
                           int *n,                    /* number of inputs (batch size) */
                           int *c,                    /* number of input feature maps  */
                           int *h,                    /* height of input section */
                           int *w,                    /* width of input section */
                           int *nStride,
                           int *cStride,
                           int *hStride,
                           int *wStride);

cudnnStatus_t CUDNNWINAPI
cudnnSetTensorNdDescriptor(cudnnTensorDescriptor_t tensorDesc,
                           cudnnDataType_t dataType,
                           int nbDims,
                           const int dimA[],
                           const int strideA[]);

cudnnStatus_t CUDNNWINAPI
cudnnSetTensorNdDescriptorEx(cudnnTensorDescriptor_t tensorDesc,
                             cudnnTensorFormat_t format,
                             cudnnDataType_t dataType,
                             int nbDims,
                             const int dimA[]);

cudnnStatus_t CUDNNWINAPI
cudnnGetTensorNdDescriptor(const cudnnTensorDescriptor_t tensorDesc,
                           int nbDimsRequested,
                           cudnnDataType_t *dataType,
                           int *nbDims,
                           int dimA[],
                           int strideA[]);

cudnnStatus_t CUDNNWINAPI
cudnnGetTensorSizeInBytes(const cudnnTensorDescriptor_t tensorDesc, size_t *size);

/* PixelOffset( n, c, h, w ) = n *input_stride + c * feature_stride + h * h_stride + w * w_stride

   1)Example of all images in row major order one batch of features after the other (with an optional padding on row)
   input_stride :  c x h x h_stride
   feature_stride : h x h_stride
   h_stride  :  >= w  ( h_stride = w if no padding)
   w_stride  : 1


   2)Example of all images in row major with features maps interleaved
   input_stride :  c x h x h_stride
   feature_stride : 1
   h_stride  :  w x c
   w_stride  : c

   3)Example of all images in column major order one batch of features after the other (with optional padding on column)
   input_stride :  c x w x w_stride
   feature_stride : w x w_stride
   h_stride  :  1
   w_stride  :  >= h

*/

/* Destroy an instance of Tensor4d descriptor */
cudnnStatus_t CUDNNWINAPI
cudnnDestroyTensorDescriptor(cudnnTensorDescriptor_t tensorDesc);

/* Fold/unfold transforms */
typedef enum {
    CUDNN_TRANSFORM_FOLD   = 0U,
    CUDNN_TRANSFORM_UNFOLD = 1U,
} cudnnFoldingDirection_t;

/** Create a destination descriptor for cudnnTransformTensor */
cudnnStatus_t CUDNNWINAPI
cudnnInitTransformDest(const cudnnTensorTransformDescriptor_t transformDesc,
                       const cudnnTensorDescriptor_t srcDesc,
                       cudnnTensorDescriptor_t destDesc,
                       size_t *destSizeInBytes);

/** Create an empty tensor transform descriptor */
cudnnStatus_t CUDNNWINAPI
cudnnCreateTensorTransformDescriptor(cudnnTensorTransformDescriptor_t *transformDesc);

/** Initialize a previously created tensor transform descriptor. */
cudnnStatus_t CUDNNWINAPI
cudnnSetTensorTransformDescriptor(cudnnTensorTransformDescriptor_t transformDesc,
                                  const uint32_t nbDims,
                                  const cudnnTensorFormat_t destFormat,
                                  const int32_t padBeforeA[],
                                  const int32_t padAfterA[],
                                  const uint32_t foldA[],
                                  const cudnnFoldingDirection_t direction);

/**
 * Retrieves the values stored in a previously initialized tensor transform
 * descriptor.
 */
cudnnStatus_t CUDNNWINAPI
cudnnGetTensorTransformDescriptor(cudnnTensorTransformDescriptor_t transformDesc,
                                  uint32_t nbDimsRequested,
                                  cudnnTensorFormat_t *destFormat,
                                  int32_t padBeforeA[],
                                  int32_t padAfterA[],
                                  uint32_t foldA[],
                                  cudnnFoldingDirection_t *direction);

/**
 * Destroys a previously created tensor transform descriptor.
 */
cudnnStatus_t CUDNNWINAPI
cudnnDestroyTensorTransformDescriptor(cudnnTensorTransformDescriptor_t transformDesc);

/* Tensor layout conversion helper (y = alpha * x + beta * y) */
cudnnStatus_t CUDNNWINAPI
cudnnTransformTensor(cudnnHandle_t handle,
                     const void *alpha,
                     const cudnnTensorDescriptor_t xDesc,
                     const void *x,
                     const void *beta,
                     const cudnnTensorDescriptor_t yDesc,
                     void *y);

cudnnStatus_t CUDNNWINAPI
cudnnTransformTensorEx(cudnnHandle_t handle,
                       const cudnnTensorTransformDescriptor_t transDesc,
                       const void *alpha,
                       const cudnnTensorDescriptor_t srcDesc,
                       const void *srcData,
                       const void *beta,
                       const cudnnTensorDescriptor_t destDesc,
                       void *destData);

/* Helper function to calculate folding descriptors  for dgrad */
cudnnStatus_t CUDNNWINAPI
cudnnGetFoldedConvBackwardDataDescriptors(const cudnnHandle_t handle,
                                          const cudnnFilterDescriptor_t filterDesc,
                                          const cudnnTensorDescriptor_t diffDesc,
                                          const cudnnConvolutionDescriptor_t convDesc,
                                          const cudnnTensorDescriptor_t gradDesc,
                                          const cudnnTensorFormat_t transformFormat,
                                          cudnnFilterDescriptor_t foldedFilterDesc,
                                          cudnnTensorDescriptor_t paddedDiffDesc,
                                          cudnnConvolutionDescriptor_t foldedConvDesc,
                                          cudnnTensorDescriptor_t foldedGradDesc,
                                          cudnnTensorTransformDescriptor_t filterFoldTransDesc,
                                          cudnnTensorTransformDescriptor_t diffPadTransDesc,
                                          cudnnTensorTransformDescriptor_t gradFoldTransDesc,
                                          cudnnTensorTransformDescriptor_t gradUnfoldTransDesc);

/* Tensor Bias addition : C = alpha * A + beta * C  */
cudnnStatus_t CUDNNWINAPI
cudnnAddTensor(cudnnHandle_t handle,
               const void *alpha,
               const cudnnTensorDescriptor_t aDesc,
               const void *A,
               const void *beta,
               const cudnnTensorDescriptor_t cDesc,
               void *C);

/*
* CUDNN OpTensor op type
*/
typedef enum {
    CUDNN_OP_TENSOR_ADD  = 0,
    CUDNN_OP_TENSOR_MUL  = 1,
    CUDNN_OP_TENSOR_MIN  = 2,
    CUDNN_OP_TENSOR_MAX  = 3,
    CUDNN_OP_TENSOR_SQRT = 4,
    CUDNN_OP_TENSOR_NOT  = 5,
} cudnnOpTensorOp_t;

cudnnStatus_t CUDNNWINAPI
cudnnCreateOpTensorDescriptor(cudnnOpTensorDescriptor_t *opTensorDesc);

cudnnStatus_t CUDNNWINAPI
cudnnSetOpTensorDescriptor(cudnnOpTensorDescriptor_t opTensorDesc,
                           cudnnOpTensorOp_t opTensorOp,
                           cudnnDataType_t opTensorCompType,
                           cudnnNanPropagation_t opTensorNanOpt);

cudnnStatus_t CUDNNWINAPI
cudnnGetOpTensorDescriptor(const cudnnOpTensorDescriptor_t opTensorDesc,
                           cudnnOpTensorOp_t *opTensorOp,
                           cudnnDataType_t *opTensorCompType,
                           cudnnNanPropagation_t *opTensorNanOpt);

cudnnStatus_t CUDNNWINAPI
cudnnDestroyOpTensorDescriptor(cudnnOpTensorDescriptor_t opTensorDesc);

/* Tensor operation : C = op( alpha1 * A, alpha2 * B ) + beta * C */
/* B tensor is ignored for CUDNN_OP_TENSOR_SQRT, CUDNN_OP_TENSOR_NOT. */
cudnnStatus_t CUDNNWINAPI
cudnnOpTensor(cudnnHandle_t handle,
              const cudnnOpTensorDescriptor_t opTensorDesc,
              const void *alpha1,
              const cudnnTensorDescriptor_t aDesc,
              const void *A,
              const void *alpha2,
              const cudnnTensorDescriptor_t bDesc,
              const void *B,
              const void *beta,
              const cudnnTensorDescriptor_t cDesc,
              void *C);

/*
* CUDNN ReduceTensor op type
*/
typedef enum {
    CUDNN_REDUCE_TENSOR_ADD          = 0,
    CUDNN_REDUCE_TENSOR_MUL          = 1,
    CUDNN_REDUCE_TENSOR_MIN          = 2,
    CUDNN_REDUCE_TENSOR_MAX          = 3,
    CUDNN_REDUCE_TENSOR_AMAX         = 4,
    CUDNN_REDUCE_TENSOR_AVG          = 5,
    CUDNN_REDUCE_TENSOR_NORM1        = 6,
    CUDNN_REDUCE_TENSOR_NORM2        = 7,
    CUDNN_REDUCE_TENSOR_MUL_NO_ZEROS = 8,
} cudnnReduceTensorOp_t;

/*
* CUDNN ReduceTensor indices type
*/
typedef enum {
    CUDNN_REDUCE_TENSOR_NO_INDICES        = 0,
    CUDNN_REDUCE_TENSOR_FLATTENED_INDICES = 1,
} cudnnReduceTensorIndices_t;

/*
* CUDNN tensor indices type size (all unsigned)
* Currently not supported, default is 32 bit unsigned.
*/
typedef enum {
    CUDNN_32BIT_INDICES = 0,
    CUDNN_64BIT_INDICES = 1,
    CUDNN_16BIT_INDICES = 2,
    CUDNN_8BIT_INDICES  = 3,
} cudnnIndicesType_t;

cudnnStatus_t CUDNNWINAPI
cudnnCreateReduceTensorDescriptor(cudnnReduceTensorDescriptor_t *reduceTensorDesc);

cudnnStatus_t CUDNNWINAPI
cudnnSetReduceTensorDescriptor(cudnnReduceTensorDescriptor_t reduceTensorDesc,
                               cudnnReduceTensorOp_t reduceTensorOp,
                               cudnnDataType_t reduceTensorCompType,
                               cudnnNanPropagation_t reduceTensorNanOpt,
                               cudnnReduceTensorIndices_t reduceTensorIndices,
                               cudnnIndicesType_t reduceTensorIndicesType);

cudnnStatus_t CUDNNWINAPI
cudnnGetReduceTensorDescriptor(const cudnnReduceTensorDescriptor_t reduceTensorDesc,
                               cudnnReduceTensorOp_t *reduceTensorOp,
                               cudnnDataType_t *reduceTensorCompType,
                               cudnnNanPropagation_t *reduceTensorNanOpt,
                               cudnnReduceTensorIndices_t *reduceTensorIndices,
                               cudnnIndicesType_t *reduceTensorIndicesType);

cudnnStatus_t CUDNNWINAPI
cudnnDestroyReduceTensorDescriptor(cudnnReduceTensorDescriptor_t reduceTensorDesc);

/* Helper function to return the minimum size of the index space to be passed to the reduction given the input and
 * output tensors */
cudnnStatus_t CUDNNWINAPI
cudnnGetReductionIndicesSize(cudnnHandle_t handle,
                             const cudnnReduceTensorDescriptor_t reduceTensorDesc,
                             const cudnnTensorDescriptor_t aDesc,
                             const cudnnTensorDescriptor_t cDesc,
                             size_t *sizeInBytes);

/* Helper function to return the minimum size of the workspace to be passed to the reduction given the input and output
 * tensors */
cudnnStatus_t CUDNNWINAPI
cudnnGetReductionWorkspaceSize(cudnnHandle_t handle,
                               const cudnnReduceTensorDescriptor_t reduceTensorDesc,
                               const cudnnTensorDescriptor_t aDesc,
                               const cudnnTensorDescriptor_t cDesc,
                               size_t *sizeInBytes);

/* Tensor operation : C = reduce op( alpha * A ) + beta * C */
/* The NaN propagation enum applies to only the min and max reduce ops; the other reduce ops propagate NaN as usual. */
/* The indices space is ignored for reduce ops other than min or max. */
cudnnStatus_t CUDNNWINAPI
cudnnReduceTensor(cudnnHandle_t handle,
                  const cudnnReduceTensorDescriptor_t reduceTensorDesc,
                  void *indices,
                  size_t indicesSizeInBytes,
                  void *workspace,
                  size_t workspaceSizeInBytes,
                  const void *alpha,
                  const cudnnTensorDescriptor_t aDesc,
                  const void *A,
                  const void *beta,
                  const cudnnTensorDescriptor_t cDesc,
                  void *C);

/* Set all values of a tensor to a given value : y[i] = value[0] */
cudnnStatus_t CUDNNWINAPI
cudnnSetTensor(cudnnHandle_t handle, const cudnnTensorDescriptor_t yDesc, void *y, const void *valuePtr);

/* Scale all values of a tensor by a given factor : y[i] = alpha * y[i] */
cudnnStatus_t CUDNNWINAPI
cudnnScaleTensor(cudnnHandle_t handle, const cudnnTensorDescriptor_t yDesc, void *y, const void *alpha);

/*
 *  convolution mode
 */
typedef enum { CUDNN_CONVOLUTION = 0, CUDNN_CROSS_CORRELATION = 1 } cudnnConvolutionMode_t;

/* Create an instance of FilterStruct */
cudnnStatus_t CUDNNWINAPI
cudnnCreateFilterDescriptor(cudnnFilterDescriptor_t *filterDesc);

cudnnStatus_t CUDNNWINAPI
cudnnSetFilter4dDescriptor(cudnnFilterDescriptor_t filterDesc,
                           cudnnDataType_t dataType, /* image data type */
                           cudnnTensorFormat_t format,
                           int k,  /* number of output feature maps */
                           int c,  /* number of input feature maps */
                           int h,  /* height of each input filter */
                           int w); /* width of  each input filter */

cudnnStatus_t CUDNNWINAPI
cudnnGetFilter4dDescriptor(const cudnnFilterDescriptor_t filterDesc,
                           cudnnDataType_t *dataType, /* image data type */
                           cudnnTensorFormat_t *format,
                           int *k,  /* number of output feature maps */
                           int *c,  /* number of input feature maps */
                           int *h,  /* height of each input filter */
                           int *w); /* width of  each input filter */

cudnnStatus_t CUDNNWINAPI
cudnnSetFilterNdDescriptor(cudnnFilterDescriptor_t filterDesc,
                           cudnnDataType_t dataType, /* image data type */
                           cudnnTensorFormat_t format,
                           int nbDims,
                           const int filterDimA[]);

cudnnStatus_t CUDNNWINAPI
cudnnGetFilterNdDescriptor(const cudnnFilterDescriptor_t filterDesc,
                           int nbDimsRequested,
                           cudnnDataType_t *dataType, /* image data type */
                           cudnnTensorFormat_t *format,
                           int *nbDims,
                           int filterDimA[]);
cudnnStatus_t CUDNNWINAPI
cudnnGetFilterSizeInBytes(const cudnnFilterDescriptor_t filterDesc, size_t *size);

cudnnStatus_t CUDNNWINAPI
cudnnTransformFilter(cudnnHandle_t handle,
                     const cudnnTensorTransformDescriptor_t transDesc,
                     const void *alpha,
                     const cudnnFilterDescriptor_t srcDesc,
                     const void *srcData,
                     const void *beta,
                     const cudnnFilterDescriptor_t destDesc,
                     void *destData);

cudnnStatus_t CUDNNWINAPI
cudnnDestroyFilterDescriptor(cudnnFilterDescriptor_t filterDesc);

cudnnStatus_t CUDNNWINAPI
cudnnReorderFilterAndBias(cudnnHandle_t handle,
                          const cudnnFilterDescriptor_t filterDesc,
                          cudnnReorderType_t reorderType,
                          const void *filterData,
                          void *reorderedFilterData,
                          int reorderBias,
                          const void *biasData,
                          void *reorderedBiasData);

/* Create an instance of convolution descriptor */
cudnnStatus_t CUDNNWINAPI
cudnnCreateConvolutionDescriptor(cudnnConvolutionDescriptor_t *convDesc);

cudnnStatus_t CUDNNWINAPI
cudnnSetConvolutionMathType(cudnnConvolutionDescriptor_t convDesc, cudnnMathType_t mathType);

cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionMathType(cudnnConvolutionDescriptor_t convDesc, cudnnMathType_t *mathType);

cudnnStatus_t CUDNNWINAPI
cudnnSetConvolutionGroupCount(cudnnConvolutionDescriptor_t convDesc, int groupCount);

cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionGroupCount(cudnnConvolutionDescriptor_t convDesc, int *groupCount);

cudnnStatus_t CUDNNWINAPI
cudnnSetConvolutionReorderType(cudnnConvolutionDescriptor_t convDesc, cudnnReorderType_t reorderType);

cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionReorderType(cudnnConvolutionDescriptor_t convDesc, cudnnReorderType_t *reorderType);

cudnnStatus_t CUDNNWINAPI
cudnnSetConvolution2dDescriptor(cudnnConvolutionDescriptor_t convDesc,
                                int pad_h,      /* zero-padding height */
                                int pad_w,      /* zero-padding width */
                                int u,          /* vertical filter stride */
                                int v,          /* horizontal filter stride */
                                int dilation_h, /* filter dilation in the vertical dimension */
                                int dilation_w, /* filter dilation in the horizontal dimension */
                                cudnnConvolutionMode_t mode,
                                cudnnDataType_t computeType);

cudnnStatus_t CUDNNWINAPI
cudnnGetConvolution2dDescriptor(const cudnnConvolutionDescriptor_t convDesc,
                                int *pad_h,      /* zero-padding height */
                                int *pad_w,      /* zero-padding width */
                                int *u,          /* vertical filter stride */
                                int *v,          /* horizontal filter stride */
                                int *dilation_h, /* filter dilation in the vertical dimension */
                                int *dilation_w, /* filter dilation in the horizontal dimension */
                                cudnnConvolutionMode_t *mode,
                                cudnnDataType_t *computeType);

/* Helper function to return the dimensions of the output tensor given a convolution descriptor */
cudnnStatus_t CUDNNWINAPI
cudnnGetConvolution2dForwardOutputDim(const cudnnConvolutionDescriptor_t convDesc,
                                      const cudnnTensorDescriptor_t inputTensorDesc,
                                      const cudnnFilterDescriptor_t filterDesc,
                                      int *n,
                                      int *c,
                                      int *h,
                                      int *w);

cudnnStatus_t CUDNNWINAPI
cudnnSetConvolutionNdDescriptor(cudnnConvolutionDescriptor_t convDesc,
                                int arrayLength, /* nbDims-2 size */
                                const int padA[],
                                const int filterStrideA[],
                                const int dilationA[],
                                cudnnConvolutionMode_t mode,
                                cudnnDataType_t computeType); /* convolution data type */

cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionNdDescriptor(const cudnnConvolutionDescriptor_t convDesc,
                                int arrayLengthRequested,
                                int *arrayLength,
                                int padA[],
                                int strideA[],
                                int dilationA[],
                                cudnnConvolutionMode_t *mode,
                                cudnnDataType_t *computeType); /* convolution data type */

/* Helper function to return the dimensions of the output tensor given a convolution descriptor */
cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionNdForwardOutputDim(const cudnnConvolutionDescriptor_t convDesc,
                                      const cudnnTensorDescriptor_t inputTensorDesc,
                                      const cudnnFilterDescriptor_t filterDesc,
                                      int nbDims,
                                      int tensorOuputDimA[]);

/* Destroy an instance of convolution descriptor */
cudnnStatus_t CUDNNWINAPI
cudnnDestroyConvolutionDescriptor(cudnnConvolutionDescriptor_t convDesc);

/* helper function to provide the convolution algo that fit best the requirement */
typedef enum {
    CUDNN_CONVOLUTION_FWD_NO_WORKSPACE            = 0,
    CUDNN_CONVOLUTION_FWD_PREFER_FASTEST          = 1,
    CUDNN_CONVOLUTION_FWD_SPECIFY_WORKSPACE_LIMIT = 2,
} cudnnConvolutionFwdPreference_t;

typedef enum {
    CUDNN_CONVOLUTION_FWD_ALGO_IMPLICIT_GEMM         = 0,
    CUDNN_CONVOLUTION_FWD_ALGO_IMPLICIT_PRECOMP_GEMM = 1,
    CUDNN_CONVOLUTION_FWD_ALGO_GEMM                  = 2,
    CUDNN_CONVOLUTION_FWD_ALGO_DIRECT                = 3,
    CUDNN_CONVOLUTION_FWD_ALGO_FFT                   = 4,
    CUDNN_CONVOLUTION_FWD_ALGO_FFT_TILING            = 5,
    CUDNN_CONVOLUTION_FWD_ALGO_WINOGRAD              = 6,
    CUDNN_CONVOLUTION_FWD_ALGO_WINOGRAD_NONFUSED     = 7,
    CUDNN_CONVOLUTION_FWD_ALGO_COUNT                 = 8
} cudnnConvolutionFwdAlgo_t;

typedef struct {
    cudnnConvolutionFwdAlgo_t algo;
    cudnnStatus_t status;
    float time;
    size_t memory;
    cudnnDeterminism_t determinism;
    cudnnMathType_t mathType;
    int reserved[3];
} cudnnConvolutionFwdAlgoPerf_t;

cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionForwardAlgorithmMaxCount(cudnnHandle_t handle, int *count);

cudnnStatus_t CUDNNWINAPI
cudnnFindConvolutionForwardAlgorithm(cudnnHandle_t handle,
                                     const cudnnTensorDescriptor_t xDesc,
                                     const cudnnFilterDescriptor_t wDesc,
                                     const cudnnConvolutionDescriptor_t convDesc,
                                     const cudnnTensorDescriptor_t yDesc,
                                     const int requestedAlgoCount,
                                     int *returnedAlgoCount,
                                     cudnnConvolutionFwdAlgoPerf_t *perfResults);

cudnnStatus_t CUDNNWINAPI
cudnnFindConvolutionForwardAlgorithmEx(cudnnHandle_t handle,
                                       const cudnnTensorDescriptor_t xDesc,
                                       const void *x,
                                       const cudnnFilterDescriptor_t wDesc,
                                       const void *w,
                                       const cudnnConvolutionDescriptor_t convDesc,
                                       const cudnnTensorDescriptor_t yDesc,
                                       void *y,
                                       const int requestedAlgoCount,
                                       int *returnedAlgoCount,
                                       cudnnConvolutionFwdAlgoPerf_t *perfResults,
                                       void *workSpace,
                                       size_t workSpaceSizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionForwardAlgorithm(cudnnHandle_t handle,
                                    const cudnnTensorDescriptor_t xDesc,
                                    const cudnnFilterDescriptor_t wDesc,
                                    const cudnnConvolutionDescriptor_t convDesc,
                                    const cudnnTensorDescriptor_t yDesc,
                                    cudnnConvolutionFwdPreference_t preference,
                                    size_t memoryLimitInBytes,
                                    cudnnConvolutionFwdAlgo_t *algo);

cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionForwardAlgorithm_v7(cudnnHandle_t handle,
                                       const cudnnTensorDescriptor_t srcDesc,
                                       const cudnnFilterDescriptor_t filterDesc,
                                       const cudnnConvolutionDescriptor_t convDesc,
                                       const cudnnTensorDescriptor_t destDesc,
                                       const int requestedAlgoCount,
                                       int *returnedAlgoCount,
                                       cudnnConvolutionFwdAlgoPerf_t *perfResults);

/*
 *  convolution algorithm (which requires potentially some workspace)
 */

/* Helper function to return the minimum size of the workspace to be passed to the convolution given an algo*/
cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionForwardWorkspaceSize(cudnnHandle_t handle,
                                        const cudnnTensorDescriptor_t xDesc,
                                        const cudnnFilterDescriptor_t wDesc,
                                        const cudnnConvolutionDescriptor_t convDesc,
                                        const cudnnTensorDescriptor_t yDesc,
                                        cudnnConvolutionFwdAlgo_t algo,
                                        size_t *sizeInBytes);

/* Convolution functions: All of the form "output = alpha * Op(inputs) + beta * output" */

/* Function to perform the forward pass for batch convolution */
cudnnStatus_t CUDNNWINAPI
cudnnConvolutionForward(cudnnHandle_t handle,
                        const void *alpha,
                        const cudnnTensorDescriptor_t xDesc,
                        const void *x,
                        const cudnnFilterDescriptor_t wDesc,
                        const void *w,
                        const cudnnConvolutionDescriptor_t convDesc,
                        cudnnConvolutionFwdAlgo_t algo,
                        void *workSpace,
                        size_t workSpaceSizeInBytes,
                        const void *beta,
                        const cudnnTensorDescriptor_t yDesc,
                        void *y);

/* Fused conv/bias/activation operation : y = Act( alpha1 * conv(x) + alpha2 * z + bias ) */
cudnnStatus_t CUDNNWINAPI
cudnnConvolutionBiasActivationForward(cudnnHandle_t handle,
                                      const void *alpha1,
                                      const cudnnTensorDescriptor_t xDesc,
                                      const void *x,
                                      const cudnnFilterDescriptor_t wDesc,
                                      const void *w,
                                      const cudnnConvolutionDescriptor_t convDesc,
                                      cudnnConvolutionFwdAlgo_t algo,
                                      void *workSpace,
                                      size_t workSpaceSizeInBytes,
                                      const void *alpha2,
                                      const cudnnTensorDescriptor_t zDesc,
                                      const void *z,
                                      const cudnnTensorDescriptor_t biasDesc,
                                      const void *bias,
                                      const cudnnActivationDescriptor_t activationDesc,
                                      const cudnnTensorDescriptor_t yDesc,
                                      void *y);

/* Function to compute the bias gradient for batch convolution */
cudnnStatus_t CUDNNWINAPI
cudnnConvolutionBackwardBias(cudnnHandle_t handle,
                             const void *alpha,
                             const cudnnTensorDescriptor_t dyDesc,
                             const void *dy,
                             const void *beta,
                             const cudnnTensorDescriptor_t dbDesc,
                             void *db);

/* helper function to provide the convolution algo that fit best the requirement */
typedef enum {
    CUDNN_CONVOLUTION_BWD_FILTER_NO_WORKSPACE            = 0,
    CUDNN_CONVOLUTION_BWD_FILTER_PREFER_FASTEST          = 1,
    CUDNN_CONVOLUTION_BWD_FILTER_SPECIFY_WORKSPACE_LIMIT = 2,
} cudnnConvolutionBwdFilterPreference_t;

typedef enum {
    CUDNN_CONVOLUTION_BWD_FILTER_ALGO_0                 = 0, /* non-deterministic */
    CUDNN_CONVOLUTION_BWD_FILTER_ALGO_1                 = 1,
    CUDNN_CONVOLUTION_BWD_FILTER_ALGO_FFT               = 2,
    CUDNN_CONVOLUTION_BWD_FILTER_ALGO_3                 = 3, /* non-deterministic */
    CUDNN_CONVOLUTION_BWD_FILTER_ALGO_WINOGRAD          = 4, /* not implemented */
    CUDNN_CONVOLUTION_BWD_FILTER_ALGO_WINOGRAD_NONFUSED = 5,
    CUDNN_CONVOLUTION_BWD_FILTER_ALGO_FFT_TILING        = 6,
    CUDNN_CONVOLUTION_BWD_FILTER_ALGO_COUNT             = 7
} cudnnConvolutionBwdFilterAlgo_t;

typedef struct {
    cudnnConvolutionBwdFilterAlgo_t algo;
    cudnnStatus_t status;
    float time;
    size_t memory;
    cudnnDeterminism_t determinism;
    cudnnMathType_t mathType;
    int reserved[3];
} cudnnConvolutionBwdFilterAlgoPerf_t;

cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionBackwardFilterAlgorithmMaxCount(cudnnHandle_t handle, int *count);

cudnnStatus_t CUDNNWINAPI
cudnnFindConvolutionBackwardFilterAlgorithm(cudnnHandle_t handle,
                                            const cudnnTensorDescriptor_t xDesc,
                                            const cudnnTensorDescriptor_t dyDesc,
                                            const cudnnConvolutionDescriptor_t convDesc,
                                            const cudnnFilterDescriptor_t dwDesc,
                                            const int requestedAlgoCount,
                                            int *returnedAlgoCount,
                                            cudnnConvolutionBwdFilterAlgoPerf_t *perfResults);

cudnnStatus_t CUDNNWINAPI
cudnnFindConvolutionBackwardFilterAlgorithmEx(cudnnHandle_t handle,
                                              const cudnnTensorDescriptor_t xDesc,
                                              const void *x,
                                              const cudnnTensorDescriptor_t dyDesc,
                                              const void *y,
                                              const cudnnConvolutionDescriptor_t convDesc,
                                              const cudnnFilterDescriptor_t dwDesc,
                                              void *dw,
                                              const int requestedAlgoCount,
                                              int *returnedAlgoCount,
                                              cudnnConvolutionBwdFilterAlgoPerf_t *perfResults,
                                              void *workSpace,
                                              size_t workSpaceSizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionBackwardFilterAlgorithm(cudnnHandle_t handle,
                                           const cudnnTensorDescriptor_t xDesc,
                                           const cudnnTensorDescriptor_t dyDesc,
                                           const cudnnConvolutionDescriptor_t convDesc,
                                           const cudnnFilterDescriptor_t dwDesc,
                                           cudnnConvolutionBwdFilterPreference_t preference,
                                           size_t memoryLimitInBytes,
                                           cudnnConvolutionBwdFilterAlgo_t *algo);

cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionBackwardFilterAlgorithm_v7(cudnnHandle_t handle,
                                              const cudnnTensorDescriptor_t srcDesc,
                                              const cudnnTensorDescriptor_t diffDesc,
                                              const cudnnConvolutionDescriptor_t convDesc,
                                              const cudnnFilterDescriptor_t gradDesc,
                                              const int requestedAlgoCount,
                                              int *returnedAlgoCount,
                                              cudnnConvolutionBwdFilterAlgoPerf_t *perfResults);

/*
 *  convolution algorithm (which requires potentially some workspace)
 */

/* Helper function to return the minimum size of the workspace to be passed to the convolution given an algo*/
cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionBackwardFilterWorkspaceSize(cudnnHandle_t handle,
                                               const cudnnTensorDescriptor_t xDesc,
                                               const cudnnTensorDescriptor_t dyDesc,
                                               const cudnnConvolutionDescriptor_t convDesc,
                                               const cudnnFilterDescriptor_t gradDesc,
                                               cudnnConvolutionBwdFilterAlgo_t algo,
                                               size_t *sizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnConvolutionBackwardFilter(cudnnHandle_t handle,
                               const void *alpha,
                               const cudnnTensorDescriptor_t xDesc,
                               const void *x,
                               const cudnnTensorDescriptor_t dyDesc,
                               const void *dy,
                               const cudnnConvolutionDescriptor_t convDesc,
                               cudnnConvolutionBwdFilterAlgo_t algo,
                               void *workSpace,
                               size_t workSpaceSizeInBytes,
                               const void *beta,
                               const cudnnFilterDescriptor_t dwDesc,
                               void *dw);

/*********************************************************/
/* helper function to provide the convolution algo that fit best the requirement */
typedef enum {
    CUDNN_CONVOLUTION_BWD_DATA_NO_WORKSPACE            = 0,
    CUDNN_CONVOLUTION_BWD_DATA_PREFER_FASTEST          = 1,
    CUDNN_CONVOLUTION_BWD_DATA_SPECIFY_WORKSPACE_LIMIT = 2,
} cudnnConvolutionBwdDataPreference_t;

typedef enum {
    CUDNN_CONVOLUTION_BWD_DATA_ALGO_0                 = 0, /* non-deterministic */
    CUDNN_CONVOLUTION_BWD_DATA_ALGO_1                 = 1,
    CUDNN_CONVOLUTION_BWD_DATA_ALGO_FFT               = 2,
    CUDNN_CONVOLUTION_BWD_DATA_ALGO_FFT_TILING        = 3,
    CUDNN_CONVOLUTION_BWD_DATA_ALGO_WINOGRAD          = 4,
    CUDNN_CONVOLUTION_BWD_DATA_ALGO_WINOGRAD_NONFUSED = 5,
    CUDNN_CONVOLUTION_BWD_DATA_ALGO_COUNT             = 6
} cudnnConvolutionBwdDataAlgo_t;

typedef struct {
    cudnnConvolutionBwdDataAlgo_t algo;
    cudnnStatus_t status;
    float time;
    size_t memory;
    cudnnDeterminism_t determinism;
    cudnnMathType_t mathType;
    int reserved[3];
} cudnnConvolutionBwdDataAlgoPerf_t;

cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionBackwardDataAlgorithmMaxCount(cudnnHandle_t handle, int *count);

cudnnStatus_t CUDNNWINAPI
cudnnFindConvolutionBackwardDataAlgorithm(cudnnHandle_t handle,
                                          const cudnnFilterDescriptor_t wDesc,
                                          const cudnnTensorDescriptor_t dyDesc,
                                          const cudnnConvolutionDescriptor_t convDesc,
                                          const cudnnTensorDescriptor_t dxDesc,
                                          const int requestedAlgoCount,
                                          int *returnedAlgoCount,
                                          cudnnConvolutionBwdDataAlgoPerf_t *perfResults);

cudnnStatus_t CUDNNWINAPI
cudnnFindConvolutionBackwardDataAlgorithmEx(cudnnHandle_t handle,
                                            const cudnnFilterDescriptor_t wDesc,
                                            const void *w,
                                            const cudnnTensorDescriptor_t dyDesc,
                                            const void *dy,
                                            const cudnnConvolutionDescriptor_t convDesc,
                                            const cudnnTensorDescriptor_t dxDesc,
                                            void *dx,
                                            const int requestedAlgoCount,
                                            int *returnedAlgoCount,
                                            cudnnConvolutionBwdDataAlgoPerf_t *perfResults,
                                            void *workSpace,
                                            size_t workSpaceSizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionBackwardDataAlgorithm(cudnnHandle_t handle,
                                         const cudnnFilterDescriptor_t wDesc,
                                         const cudnnTensorDescriptor_t dyDesc,
                                         const cudnnConvolutionDescriptor_t convDesc,
                                         const cudnnTensorDescriptor_t dxDesc,
                                         cudnnConvolutionBwdDataPreference_t preference,
                                         size_t memoryLimitInBytes,
                                         cudnnConvolutionBwdDataAlgo_t *algo);

cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionBackwardDataAlgorithm_v7(cudnnHandle_t handle,
                                            const cudnnFilterDescriptor_t filterDesc,
                                            const cudnnTensorDescriptor_t diffDesc,
                                            const cudnnConvolutionDescriptor_t convDesc,
                                            const cudnnTensorDescriptor_t gradDesc,
                                            const int requestedAlgoCount,
                                            int *returnedAlgoCount,
                                            cudnnConvolutionBwdDataAlgoPerf_t *perfResults);

/* Helper function to return the minimum size of the workspace to be passed to the convolution given an algo*/
cudnnStatus_t CUDNNWINAPI
cudnnGetConvolutionBackwardDataWorkspaceSize(cudnnHandle_t handle,
                                             const cudnnFilterDescriptor_t wDesc,
                                             const cudnnTensorDescriptor_t dyDesc,
                                             const cudnnConvolutionDescriptor_t convDesc,
                                             const cudnnTensorDescriptor_t dxDesc,
                                             cudnnConvolutionBwdDataAlgo_t algo,
                                             size_t *sizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnConvolutionBackwardData(cudnnHandle_t handle,
                             const void *alpha,
                             const cudnnFilterDescriptor_t wDesc,
                             const void *w,
                             const cudnnTensorDescriptor_t dyDesc,
                             const void *dy,
                             const cudnnConvolutionDescriptor_t convDesc,
                             cudnnConvolutionBwdDataAlgo_t algo,
                             void *workSpace,
                             size_t workSpaceSizeInBytes,
                             const void *beta,
                             const cudnnTensorDescriptor_t dxDesc,
                             void *dx);

cudnnStatus_t CUDNNWINAPI
cudnnIm2Col(cudnnHandle_t handle,
            const cudnnTensorDescriptor_t xDesc,
            const void *x,
            const cudnnFilterDescriptor_t wDesc,
            const cudnnConvolutionDescriptor_t convDesc,
            void *colBuffer);

/*
 *  softmax algorithm
 */
typedef enum {
    CUDNN_SOFTMAX_FAST     = 0, /* straightforward implementation */
    CUDNN_SOFTMAX_ACCURATE = 1, /* subtract max from every point to avoid overflow */
    CUDNN_SOFTMAX_LOG      = 2
} cudnnSoftmaxAlgorithm_t;

typedef enum {
    CUDNN_SOFTMAX_MODE_INSTANCE = 0, /* compute the softmax over all C, H, W for each N */
    CUDNN_SOFTMAX_MODE_CHANNEL  = 1  /* compute the softmax over all C for each H, W, N */
} cudnnSoftmaxMode_t;

/* Softmax functions: All of the form "output = alpha * Op(inputs) + beta * output" */

/* Function to perform forward softmax */
cudnnStatus_t CUDNNWINAPI
cudnnSoftmaxForward(cudnnHandle_t handle,
                    cudnnSoftmaxAlgorithm_t algo,
                    cudnnSoftmaxMode_t mode,
                    const void *alpha,
                    const cudnnTensorDescriptor_t xDesc,
                    const void *x,
                    const void *beta,
                    const cudnnTensorDescriptor_t yDesc,
                    void *y);

/* Function to perform backward softmax */
cudnnStatus_t CUDNNWINAPI
cudnnSoftmaxBackward(cudnnHandle_t handle,
                     cudnnSoftmaxAlgorithm_t algo,
                     cudnnSoftmaxMode_t mode,
                     const void *alpha,
                     const cudnnTensorDescriptor_t yDesc,
                     const void *y,
                     const cudnnTensorDescriptor_t dyDesc,
                     const void *dy,
                     const void *beta,
                     const cudnnTensorDescriptor_t dxDesc,
                     void *dx);

/*
 *  pooling mode
 */
typedef enum {
    CUDNN_POOLING_MAX                           = 0,
    CUDNN_POOLING_AVERAGE_COUNT_INCLUDE_PADDING = 1, /* count for average includes padded values */
    CUDNN_POOLING_AVERAGE_COUNT_EXCLUDE_PADDING = 2, /* count for average does not include padded values */
    CUDNN_POOLING_MAX_DETERMINISTIC             = 3
} cudnnPoolingMode_t;

/* Create an instance of pooling descriptor */
cudnnStatus_t CUDNNWINAPI
cudnnCreatePoolingDescriptor(cudnnPoolingDescriptor_t *poolingDesc);

cudnnStatus_t CUDNNWINAPI
cudnnSetPooling2dDescriptor(cudnnPoolingDescriptor_t poolingDesc,
                            cudnnPoolingMode_t mode,
                            cudnnNanPropagation_t maxpoolingNanOpt,
                            int windowHeight,
                            int windowWidth,
                            int verticalPadding,
                            int horizontalPadding,
                            int verticalStride,
                            int horizontalStride);

cudnnStatus_t CUDNNWINAPI
cudnnGetPooling2dDescriptor(const cudnnPoolingDescriptor_t poolingDesc,
                            cudnnPoolingMode_t *mode,
                            cudnnNanPropagation_t *maxpoolingNanOpt,
                            int *windowHeight,
                            int *windowWidth,
                            int *verticalPadding,
                            int *horizontalPadding,
                            int *verticalStride,
                            int *horizontalStride);

cudnnStatus_t CUDNNWINAPI
cudnnSetPoolingNdDescriptor(cudnnPoolingDescriptor_t poolingDesc,
                            const cudnnPoolingMode_t mode,
                            const cudnnNanPropagation_t maxpoolingNanOpt,
                            int nbDims,
                            const int windowDimA[],
                            const int paddingA[],
                            const int strideA[]);

cudnnStatus_t CUDNNWINAPI
cudnnGetPoolingNdDescriptor(const cudnnPoolingDescriptor_t poolingDesc,
                            int nbDimsRequested,
                            cudnnPoolingMode_t *mode,
                            cudnnNanPropagation_t *maxpoolingNanOpt,
                            int *nbDims,
                            int windowDimA[],
                            int paddingA[],
                            int strideA[]);

cudnnStatus_t CUDNNWINAPI
cudnnGetPoolingNdForwardOutputDim(const cudnnPoolingDescriptor_t poolingDesc,
                                  const cudnnTensorDescriptor_t inputTensorDesc,
                                  int nbDims,
                                  int outputTensorDimA[]);

cudnnStatus_t CUDNNWINAPI
cudnnGetPooling2dForwardOutputDim(const cudnnPoolingDescriptor_t poolingDesc,
                                  const cudnnTensorDescriptor_t inputTensorDesc,
                                  int *n,
                                  int *c,
                                  int *h,
                                  int *w);

/* Destroy an instance of pooling descriptor */
cudnnStatus_t CUDNNWINAPI
cudnnDestroyPoolingDescriptor(cudnnPoolingDescriptor_t poolingDesc);

/* Pooling functions: All of the form "output = alpha * Op(inputs) + beta * output" */

/* Function to perform forward pooling */
cudnnStatus_t CUDNNWINAPI
cudnnPoolingForward(cudnnHandle_t handle,
                    const cudnnPoolingDescriptor_t poolingDesc,
                    const void *alpha,
                    const cudnnTensorDescriptor_t xDesc,
                    const void *x,
                    const void *beta,
                    const cudnnTensorDescriptor_t yDesc,
                    void *y);

/* Function to perform backward pooling */
cudnnStatus_t CUDNNWINAPI
cudnnPoolingBackward(cudnnHandle_t handle,
                     const cudnnPoolingDescriptor_t poolingDesc,
                     const void *alpha,
                     const cudnnTensorDescriptor_t yDesc,
                     const void *y,
                     const cudnnTensorDescriptor_t dyDesc,
                     const void *dy,
                     const cudnnTensorDescriptor_t xDesc,
                     const void *x,
                     const void *beta,
                     const cudnnTensorDescriptor_t dxDesc,
                     void *dx);

/*
 * activation mode
 */
typedef enum {
    CUDNN_ACTIVATION_SIGMOID      = 0,
    CUDNN_ACTIVATION_RELU         = 1,
    CUDNN_ACTIVATION_TANH         = 2,
    CUDNN_ACTIVATION_CLIPPED_RELU = 3,
    CUDNN_ACTIVATION_ELU          = 4,
    CUDNN_ACTIVATION_IDENTITY     = 5
} cudnnActivationMode_t;

/* Activation functions: All of the form "output = alpha * Op(inputs) + beta * output" */
cudnnStatus_t CUDNNWINAPI
cudnnCreateActivationDescriptor(cudnnActivationDescriptor_t *activationDesc);

cudnnStatus_t CUDNNWINAPI
cudnnSetActivationDescriptor(cudnnActivationDescriptor_t activationDesc,
                             cudnnActivationMode_t mode,
                             cudnnNanPropagation_t reluNanOpt,
                             double coef); /* ceiling for clipped RELU, alpha for ELU */

cudnnStatus_t CUDNNWINAPI
cudnnGetActivationDescriptor(const cudnnActivationDescriptor_t activationDesc,
                             cudnnActivationMode_t *mode,
                             cudnnNanPropagation_t *reluNanOpt,
                             double *coef); /* ceiling for clipped RELU, alpha for ELU */

cudnnStatus_t CUDNNWINAPI
cudnnDestroyActivationDescriptor(cudnnActivationDescriptor_t activationDesc);

/* Function to perform forward activation  */
cudnnStatus_t CUDNNWINAPI
cudnnActivationForward(cudnnHandle_t handle,
                       cudnnActivationDescriptor_t activationDesc,
                       const void *alpha,
                       const cudnnTensorDescriptor_t xDesc,
                       const void *x,
                       const void *beta,
                       const cudnnTensorDescriptor_t yDesc,
                       void *y);

/* Function to perform backward activation  */
cudnnStatus_t CUDNNWINAPI
cudnnActivationBackward(cudnnHandle_t handle,
                        cudnnActivationDescriptor_t activationDesc,
                        const void *alpha,
                        const cudnnTensorDescriptor_t yDesc,
                        const void *y,
                        const cudnnTensorDescriptor_t dyDesc,
                        const void *dy,
                        const cudnnTensorDescriptor_t xDesc,
                        const void *x,
                        const void *beta,
                        const cudnnTensorDescriptor_t dxDesc,
                        void *dx);

/*
* Create an instance of LRN (Local Response Normalization) descriptor
* Uses lrnN=5, lrnAlpha=1e-4, lrnBeta=0.75, lrnK=2.0 as defaults from Krizhevsky'12 ImageNet paper
*/
cudnnStatus_t CUDNNWINAPI
cudnnCreateLRNDescriptor(cudnnLRNDescriptor_t *normDesc);

#define CUDNN_LRN_MIN_N 1       /* minimum allowed lrnN */
#define CUDNN_LRN_MAX_N 16      /* maximum allowed lrnN */
#define CUDNN_LRN_MIN_K 1e-5    /* minimum allowed lrnK */
#define CUDNN_LRN_MIN_BETA 0.01 /* minimum allowed lrnBeta */

/* LRN layer mode */
typedef enum {
    CUDNN_LRN_CROSS_CHANNEL_DIM1 = 0, /* Normalize across tensor's dimA[1] dimension */
} cudnnLRNMode_t;

/*
* Uses a window [center-lookBehind, center+lookAhead], where
* lookBehind = floor( (lrnN-1)/2 ), lookAhead = lrnN-lookBehind-1.
* Values of double parameters cast to tensor data type.
*/
cudnnStatus_t CUDNNWINAPI
cudnnSetLRNDescriptor(cudnnLRNDescriptor_t normDesc, unsigned lrnN, double lrnAlpha, double lrnBeta, double lrnK);
/*
* Retrieve the settings currently stored in an LRN layer descriptor
* Any of the provided pointers can be NULL (no corresponding value will be returned)
*/
cudnnStatus_t CUDNNWINAPI
cudnnGetLRNDescriptor(cudnnLRNDescriptor_t normDesc, unsigned *lrnN, double *lrnAlpha, double *lrnBeta, double *lrnK);

/* Destroy an instance of LRN descriptor */
cudnnStatus_t CUDNNWINAPI
cudnnDestroyLRNDescriptor(cudnnLRNDescriptor_t lrnDesc);

/* LRN functions: output = alpha * normalize(x) + beta * old_y */

/* LRN cross-channel forward computation. Double parameters cast to tensor data type */
cudnnStatus_t CUDNNWINAPI
cudnnLRNCrossChannelForward(cudnnHandle_t handle,
                            cudnnLRNDescriptor_t normDesc,
                            cudnnLRNMode_t lrnMode,
                            const void *alpha,
                            const cudnnTensorDescriptor_t xDesc,
                            const void *x,
                            const void *beta,
                            const cudnnTensorDescriptor_t yDesc,
                            void *y);

/* LRN cross-channel backward computation. Double parameters cast to tensor data type */
cudnnStatus_t CUDNNWINAPI
cudnnLRNCrossChannelBackward(cudnnHandle_t handle,
                             cudnnLRNDescriptor_t normDesc,
                             cudnnLRNMode_t lrnMode,
                             const void *alpha,
                             const cudnnTensorDescriptor_t yDesc,
                             const void *y,
                             const cudnnTensorDescriptor_t dyDesc,
                             const void *dy,
                             const cudnnTensorDescriptor_t xDesc,
                             const void *x,
                             const void *beta,
                             const cudnnTensorDescriptor_t dxDesc,
                             void *dx);

typedef enum {
    CUDNN_DIVNORM_PRECOMPUTED_MEANS = 0,
} cudnnDivNormMode_t;

/* LCN/divisive normalization functions: y = alpha * normalize(x) + beta * y */
cudnnStatus_t CUDNNWINAPI
cudnnDivisiveNormalizationForward(cudnnHandle_t handle,
                                  cudnnLRNDescriptor_t normDesc,
                                  cudnnDivNormMode_t mode,
                                  const void *alpha,
                                  const cudnnTensorDescriptor_t xDesc, /* same desc for means, temp, temp2 */
                                  const void *x,
                                  const void *means, /* if NULL, means are assumed to be zero */
                                  void *temp,
                                  void *temp2,
                                  const void *beta,
                                  const cudnnTensorDescriptor_t yDesc,
                                  void *y);

cudnnStatus_t CUDNNWINAPI
cudnnDivisiveNormalizationBackward(cudnnHandle_t handle,
                                   cudnnLRNDescriptor_t normDesc,
                                   cudnnDivNormMode_t mode,
                                   const void *alpha,
                                   const cudnnTensorDescriptor_t xDesc, /* same desc for x, means, dy, temp, temp2 */
                                   const void *x,
                                   const void *means, /* if NULL, means are assumed to be zero */
                                   const void *dy,
                                   void *temp,
                                   void *temp2,
                                   const void *beta,
                                   const cudnnTensorDescriptor_t dXdMeansDesc, /* same desc for dx, dMeans */
                                   void *dx,                                   /* output x differential */
                                   void *dMeans); /* output means differential, can be NULL */

typedef enum {
    /* bnScale, bnBias tensor dims are 1xCxHxWx.. (one value per CHW...-slice, normalized over N slice) */
    CUDNN_BATCHNORM_PER_ACTIVATION = 0,

    /* bnScale, bnBias tensor dims are 1xCx1x1 (one value per C-dim normalized over Nx1xHxW subtensors) */
    CUDNN_BATCHNORM_SPATIAL = 1,

    /*
     * bnScale, bnBias tensor dims are 1xCx1x1 (one value per C-dim normalized over Nx1xHxW subtensors).
     * May be faster than CUDNN_BATCHNORM_SPATIAL but imposes some limits on the range of values
     */
    CUDNN_BATCHNORM_SPATIAL_PERSISTENT = 2,
} cudnnBatchNormMode_t;

#define CUDNN_BN_MIN_EPSILON 0.0 /* Minimum epsilon allowed to be used in the Batch Normalization formula */

/*
* Derives a tensor descriptor from layer data descriptor for BatchNormalization
* scale, invVariance, bnBias, bnScale tensors. Use this tensor desc for
* bnScaleBiasMeanVarDesc and bnScaleBiasDiffDesc in Batch Normalization forward and backward functions.
*/
cudnnStatus_t CUDNNWINAPI
cudnnDeriveBNTensorDescriptor(cudnnTensorDescriptor_t derivedBnDesc,
                              const cudnnTensorDescriptor_t xDesc,
                              cudnnBatchNormMode_t mode);

typedef enum {
    CUDNN_BATCHNORM_OPS_BN                = 0, /* do batch normalization only */
    CUDNN_BATCHNORM_OPS_BN_ACTIVATION     = 1, /* do batchNorm, then activation */
    CUDNN_BATCHNORM_OPS_BN_ADD_ACTIVATION = 2, /* do batchNorm, then elemWiseAdd, then activation */
} cudnnBatchNormOps_t;

cudnnStatus_t CUDNNWINAPI
cudnnGetBatchNormalizationForwardTrainingExWorkspaceSize(cudnnHandle_t handle,
                                                         cudnnBatchNormMode_t mode,
                                                         cudnnBatchNormOps_t bnOps,
                                                         const cudnnTensorDescriptor_t xDesc,
                                                         const cudnnTensorDescriptor_t zDesc,
                                                         const cudnnTensorDescriptor_t yDesc,
                                                         const cudnnTensorDescriptor_t bnScaleBiasMeanVarDesc,
                                                         const cudnnActivationDescriptor_t activationDesc,
                                                         size_t *sizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnGetBatchNormalizationBackwardExWorkspaceSize(cudnnHandle_t handle,
                                                  cudnnBatchNormMode_t mode,
                                                  cudnnBatchNormOps_t bnOps,
                                                  const cudnnTensorDescriptor_t xDesc,
                                                  const cudnnTensorDescriptor_t yDesc,
                                                  const cudnnTensorDescriptor_t dyDesc,
                                                  const cudnnTensorDescriptor_t dzDesc,
                                                  const cudnnTensorDescriptor_t dxDesc,
                                                  const cudnnTensorDescriptor_t dBnScaleBiasDesc,
                                                  const cudnnActivationDescriptor_t activationDesc,
                                                  size_t *sizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnGetBatchNormalizationTrainingExReserveSpaceSize(cudnnHandle_t handle,
                                                     cudnnBatchNormMode_t mode,
                                                     cudnnBatchNormOps_t bnOps,
                                                     const cudnnActivationDescriptor_t activationDesc,
                                                     const cudnnTensorDescriptor_t xDesc,
                                                     size_t *sizeInBytes);

/* Computes y = BN(x). Also accumulates moving averages of mean and inverse variances */
cudnnStatus_t CUDNNWINAPI
cudnnBatchNormalizationForwardTraining(
    cudnnHandle_t handle,
    cudnnBatchNormMode_t mode,

    const void *alpha, /* alpha[0] = result blend factor */
    const void *beta,  /* beta[0] = dest layer blend factor */

    const cudnnTensorDescriptor_t xDesc,
    const void *x, /* NxCxHxW */
    const cudnnTensorDescriptor_t yDesc,
    void *y, /* NxCxHxW */

    /* Shared desc for the next 6 tensors in the argument list.
       Data type to be set as follows:
       type = (typeOf(x) == double) ? double : float
       Dimensions for this descriptor depend on normalization mode
       - Spatial Normalization : tensors are expected to have dims 1xCx1x1
        (normalization is performed across NxHxW)
       - Per-Activation Normalization : tensors are expected to have dims of 1xCxHxW
        (normalization is performed across N) */
    const cudnnTensorDescriptor_t bnScaleBiasMeanVarDesc,

    /* 'Gamma' and 'Beta' respectively in Ioffe and Szegedy's paper's notation */
    const void *bnScale,
    const void *bnBias,

    /* MUST use factor=1 in the very first call of a complete training cycle.
       Use a factor=1/(1+n) at N-th call to the function to get
       Cumulative Moving Average (CMA) behavior
       CMA[n] = (x[1]+...+x[n])/n
       Since CMA[n+1] = (n*CMA[n]+x[n+1])/(n+1) =
       ((n+1)*CMA[n]-CMA[n])/(n+1) + x[n+1]/(n+1) =
       CMA[n]*(1-1/(n+1)) + x[n+1]*1/(n+1) */
    double exponentialAverageFactor,

    /* Used in Training phase only.
       runningMean = newMean*factor + runningMean*(1-factor) */
    void *resultRunningMean,
    /* Output in training mode, input in inference. Is the moving average
       of  variance[x] (factor is applied in the same way as for runningMean) */
    void *resultRunningVariance,

    /* Has to be >= CUDNN_BN_MIN_EPSILON. Should be the same in forward and backward functions. */
    double epsilon,

    /* Optionally save intermediate results from the forward pass here
       - can be reused to speed up backward pass. NULL if unused */
    void *resultSaveMean,
    void *resultSaveInvVariance);

/* Computes y = relu(BN(x) + z). Also accumulates moving averages of mean and inverse variances */
cudnnStatus_t CUDNNWINAPI
cudnnBatchNormalizationForwardTrainingEx(
    cudnnHandle_t handle,
    cudnnBatchNormMode_t mode,
    cudnnBatchNormOps_t bnOps,

    const void *alpha, /* alpha[0] = result blend factor */
    const void *beta,  /* beta[0] = dest layer blend factor */

    const cudnnTensorDescriptor_t xDesc,
    const void *xData,
    const cudnnTensorDescriptor_t zDesc,
    const void *zData,
    const cudnnTensorDescriptor_t yDesc,
    void *yData,

    const cudnnTensorDescriptor_t bnScaleBiasMeanVarDesc,
    const void *bnScale,
    const void *bnBias,

    double exponentialAverageFactor,
    void *resultRunningMean,
    void *resultRunningVariance,

    /* Has to be >= CUDNN_BN_MIN_EPSILON. Should be the same in forward and backward functions. */
    double epsilon,

    /* Optionally save intermediate results from the forward pass here
       - can be reused to speed up backward pass. NULL if unused */
    void *resultSaveMean,
    void *resultSaveInvVariance,

    cudnnActivationDescriptor_t activationDesc,
    void *workspace,
    size_t workSpaceSizeInBytes,
    void *reserveSpace,
    size_t reserveSpaceSizeInBytes);

/*
* Performs Batch Normalization during Inference:
* y[i] = bnScale[k]*(x[i]-estimatedMean[k])/sqrt(epsilon+estimatedVariance[k]) + bnBias[k]
* with bnScale, bnBias, runningMean, runningInvVariance tensors indexed
* according to spatial or per-activation mode. Refer to cudnnBatchNormalizationForwardTraining
* above for notes on function arguments.
*/
cudnnStatus_t CUDNNWINAPI
cudnnBatchNormalizationForwardInference(cudnnHandle_t handle,
                                        cudnnBatchNormMode_t mode,
                                        const void *alpha, /* alpha[0] = result blend factor */
                                        const void *beta,  /* beta[0] = dest layer blend factor */
                                        const cudnnTensorDescriptor_t xDesc,
                                        const void *x, /* NxCxHxW */
                                        const cudnnTensorDescriptor_t yDesc,
                                        void *y, /* NxCxHxW */
                                        const cudnnTensorDescriptor_t bnScaleBiasMeanVarDesc,
                                        const void *bnScale,
                                        const void *bnBias,
                                        const void *estimatedMean,
                                        const void *estimatedVariance,
                                        double epsilon);

/* Performs backward pass of Batch Normalization layer. Returns x gradient,
* bnScale gradient and bnBias gradient */
cudnnStatus_t CUDNNWINAPI
cudnnBatchNormalizationBackward(cudnnHandle_t handle,
                                cudnnBatchNormMode_t mode,
                                const void *alphaDataDiff,
                                const void *betaDataDiff,
                                const void *alphaParamDiff,
                                const void *betaParamDiff,
                                const cudnnTensorDescriptor_t xDesc, /* same desc for x, dx, dy */
                                const void *x,
                                const cudnnTensorDescriptor_t dyDesc,
                                const void *dy,
                                const cudnnTensorDescriptor_t dxDesc,
                                void *dx,
                                /* Shared tensor desc for the 4 tensors below */
                                const cudnnTensorDescriptor_t dBnScaleBiasDesc,
                                const void *bnScale, /* bnBias doesn't affect backpropagation */
                                /* scale and bias diff are not backpropagated below this layer */
                                void *dBnScaleResult,
                                void *dBnBiasResult,
                                /* Same epsilon as forward pass */
                                double epsilon,

                                /* Optionally cached intermediate results from
                                   forward pass */
                                const void *savedMean,
                                const void *savedInvVariance);

cudnnStatus_t CUDNNWINAPI
cudnnBatchNormalizationBackwardEx(cudnnHandle_t handle,
                                  cudnnBatchNormMode_t mode,
                                  cudnnBatchNormOps_t bnOps,

                                  const void *alphaDataDiff,
                                  const void *betaDataDiff,
                                  const void *alphaParamDiff,
                                  const void *betaParamDiff,
                                  const cudnnTensorDescriptor_t xDesc,
                                  const void *xData,
                                  const cudnnTensorDescriptor_t yDesc,
                                  const void *yData,
                                  const cudnnTensorDescriptor_t dyDesc,
                                  const void *dyData,
                                  const cudnnTensorDescriptor_t dzDesc,
                                  void *dzData,
                                  const cudnnTensorDescriptor_t dxDesc,
                                  void *dxData,

                                  /* Shared tensor desc for the 4 tensors below */
                                  const cudnnTensorDescriptor_t dBnScaleBiasDesc,
                                  const void *bnScaleData,
                                  const void *bnBiasData, /* needed if there is activation */
                                  void *dBnScaleData,
                                  void *dBnBiasData,
                                  double epsilon, /* Same epsilon as forward pass */

                                  /* Optionally cached intermediate results from
                                     forward pass */
                                  const void *savedMean,
                                  const void *savedInvVariance,
                                  cudnnActivationDescriptor_t activationDesc,
                                  void *workSpace,
                                  size_t workSpaceSizeInBytes,
                                  void *reserveSpace,
                                  size_t reserveSpaceSizeInBytes);

/* APIs for spatial transformer network*/
typedef enum {
    CUDNN_SAMPLER_BILINEAR = 0,
} cudnnSamplerType_t;

cudnnStatus_t CUDNNWINAPI
cudnnCreateSpatialTransformerDescriptor(cudnnSpatialTransformerDescriptor_t *stDesc);

cudnnStatus_t CUDNNWINAPI
cudnnSetSpatialTransformerNdDescriptor(cudnnSpatialTransformerDescriptor_t stDesc,
                                       cudnnSamplerType_t samplerType,
                                       cudnnDataType_t dataType,
                                       const int nbDims,
                                       const int dimA[]);

cudnnStatus_t CUDNNWINAPI
cudnnDestroySpatialTransformerDescriptor(cudnnSpatialTransformerDescriptor_t stDesc);

cudnnStatus_t CUDNNWINAPI
cudnnSpatialTfGridGeneratorForward(cudnnHandle_t handle,
                                   const cudnnSpatialTransformerDescriptor_t stDesc,
                                   const void *theta,
                                   void *grid);

cudnnStatus_t CUDNNWINAPI
cudnnSpatialTfGridGeneratorBackward(cudnnHandle_t handle,
                                    const cudnnSpatialTransformerDescriptor_t stDesc,
                                    const void *dgrid,
                                    void *dtheta);

cudnnStatus_t CUDNNWINAPI
cudnnSpatialTfSamplerForward(cudnnHandle_t handle,
                             cudnnSpatialTransformerDescriptor_t stDesc,
                             const void *alpha,
                             const cudnnTensorDescriptor_t xDesc,
                             const void *x,
                             const void *grid,
                             const void *beta,
                             cudnnTensorDescriptor_t yDesc,
                             void *y);

cudnnStatus_t CUDNNWINAPI
cudnnSpatialTfSamplerBackward(cudnnHandle_t handle,
                              cudnnSpatialTransformerDescriptor_t stDesc,
                              const void *alpha,
                              const cudnnTensorDescriptor_t xDesc,
                              const void *x,
                              const void *beta,
                              const cudnnTensorDescriptor_t dxDesc,
                              void *dx,
                              const void *alphaDgrid,
                              const cudnnTensorDescriptor_t dyDesc,
                              const void *dy,
                              const void *grid,
                              const void *betaDgrid,
                              void *dgrid);

typedef struct cudnnDropoutStruct *cudnnDropoutDescriptor_t;

cudnnStatus_t CUDNNWINAPI
cudnnCreateDropoutDescriptor(cudnnDropoutDescriptor_t *dropoutDesc);

cudnnStatus_t CUDNNWINAPI
cudnnDestroyDropoutDescriptor(cudnnDropoutDescriptor_t dropoutDesc);

/*helper function to determine size of the states to be passed to cudnnSetDropoutDescriptor */
cudnnStatus_t CUDNNWINAPI
cudnnDropoutGetStatesSize(cudnnHandle_t handle, size_t *sizeInBytes);

/*helper function to determine size of the reserve space to be passed to dropout forward/backward calls */
cudnnStatus_t CUDNNWINAPI
cudnnDropoutGetReserveSpaceSize(cudnnTensorDescriptor_t xdesc, size_t *sizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnSetDropoutDescriptor(cudnnDropoutDescriptor_t dropoutDesc,
                          cudnnHandle_t handle,
                          float dropout,
                          void *states,
                          size_t stateSizeInBytes,
                          unsigned long long seed);

/* Restores the dropout descriptor to a previously saved-off state */
cudnnStatus_t CUDNNWINAPI
cudnnRestoreDropoutDescriptor(cudnnDropoutDescriptor_t dropoutDesc,
                              cudnnHandle_t handle,
                              float dropout,
                              void *states,
                              size_t stateSizeInBytes,
                              unsigned long long seed);

cudnnStatus_t CUDNNWINAPI
cudnnGetDropoutDescriptor(cudnnDropoutDescriptor_t dropoutDesc,
                          cudnnHandle_t handle,
                          float *dropout,
                          void **states,
                          unsigned long long *seed);

cudnnStatus_t CUDNNWINAPI
cudnnDropoutForward(cudnnHandle_t handle,
                    const cudnnDropoutDescriptor_t dropoutDesc,
                    const cudnnTensorDescriptor_t xdesc,
                    const void *x,
                    const cudnnTensorDescriptor_t ydesc,
                    void *y,
                    void *reserveSpace,
                    size_t reserveSpaceSizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnDropoutBackward(cudnnHandle_t handle,
                     const cudnnDropoutDescriptor_t dropoutDesc,
                     const cudnnTensorDescriptor_t dydesc,
                     const void *dy,
                     const cudnnTensorDescriptor_t dxdesc,
                     void *dx,
                     void *reserveSpace,
                     size_t reserveSpaceSizeInBytes);

/* BASIC RNN API */

typedef enum {
    CUDNN_RNN_ALGO_STANDARD        = 0,
    CUDNN_RNN_ALGO_PERSIST_STATIC  = 1,
    CUDNN_RNN_ALGO_PERSIST_DYNAMIC = 2,
    CUDNN_RNN_ALGO_COUNT           = 3,
} cudnnRNNAlgo_t;

typedef enum {
    CUDNN_RNN_RELU = 0, /* basic RNN cell type with ReLu activation */
    CUDNN_RNN_TANH = 1, /* basic RNN cell type with tanh activation */
    CUDNN_LSTM     = 2, /* LSTM with no peephole connections */
    CUDNN_GRU      = 3, /* Using h' = tanh(r * Uh(t-1) + Wx) and h = (1 - z) * h' + z * h(t-1); */
} cudnnRNNMode_t;

typedef enum {
    CUDNN_RNN_NO_BIAS         = 0, /* rnn cell formulas do not use biases */
    CUDNN_RNN_SINGLE_INP_BIAS = 1, /* rnn cell formulas use one input bias in input GEMM */
    CUDNN_RNN_DOUBLE_BIAS     = 2, /* default, rnn cell formulas use two bias vectors */
    CUDNN_RNN_SINGLE_REC_BIAS = 3  /* rnn cell formulas use one recurrent bias in recurrent GEMM */
} cudnnRNNBiasMode_t;

typedef enum {
    CUDNN_UNIDIRECTIONAL = 0, /* single direction network */
    CUDNN_BIDIRECTIONAL  = 1, /* output concatination at each layer */
} cudnnDirectionMode_t;

typedef enum {
    CUDNN_LINEAR_INPUT = 0, /* adjustable weight matrix in first layer input GEMM */
    CUDNN_SKIP_INPUT   = 1, /* fixed identity matrix in the first layer input GEMM */
} cudnnRNNInputMode_t;

typedef enum {
    CUDNN_RNN_CLIP_NONE   = 0, /* disables LSTM cell clipping */
    CUDNN_RNN_CLIP_MINMAX = 1, /* enables LSTM cell clipping */
} cudnnRNNClipMode_t;

typedef enum {
    CUDNN_RNN_DATA_LAYOUT_SEQ_MAJOR_UNPACKED   = 0, /* padded, outer stride from one time-step to the next */
    CUDNN_RNN_DATA_LAYOUT_SEQ_MAJOR_PACKED     = 1, /* sequence length sorted and packed as in basic RNN api */
    CUDNN_RNN_DATA_LAYOUT_BATCH_MAJOR_UNPACKED = 2, /* padded, outer stride from one batch to the next */
} cudnnRNNDataLayout_t;

typedef enum {
    CUDNN_RNN_PADDED_IO_DISABLED = 0,
    CUDNN_RNN_PADDED_IO_ENABLED  = 1,
} cudnnRNNPaddingMode_t;

struct cudnnRNNStruct;
typedef struct cudnnRNNStruct *cudnnRNNDescriptor_t;

struct cudnnPersistentRNNPlan;
typedef struct cudnnPersistentRNNPlan *cudnnPersistentRNNPlan_t;

struct cudnnRNNDataStruct;
typedef struct cudnnRNNDataStruct *cudnnRNNDataDescriptor_t;

cudnnStatus_t CUDNNWINAPI
cudnnCreateRNNDescriptor(cudnnRNNDescriptor_t *rnnDesc);

cudnnStatus_t CUDNNWINAPI
cudnnDestroyRNNDescriptor(cudnnRNNDescriptor_t rnnDesc);

/* mathPrec in the RNN descriptor is determines compute math precision, modified by cudnnMathType_t */
/* dataType in weight descriptors and input descriptors is used to describe data/parameter storage */
/* dropout is between RNN layers, not between recurrent steps */
cudnnStatus_t CUDNNWINAPI
cudnnSetRNNDescriptor(cudnnHandle_t handle,
                      cudnnRNNDescriptor_t rnnDesc,
                      const int hiddenSize,
                      const int numLayers,
                      cudnnDropoutDescriptor_t dropoutDesc,
                      cudnnRNNInputMode_t inputMode,
                      cudnnDirectionMode_t direction,
                      cudnnRNNMode_t mode,
                      cudnnRNNAlgo_t algo,
                      cudnnDataType_t mathPrec);

cudnnStatus_t CUDNNWINAPI
cudnnGetRNNDescriptor(cudnnHandle_t handle,
                      cudnnRNNDescriptor_t rnnDesc,
                      int *hiddenSize,
                      int *numLayers,
                      cudnnDropoutDescriptor_t *dropoutDesc,
                      cudnnRNNInputMode_t *inputMode,
                      cudnnDirectionMode_t *direction,
                      cudnnRNNMode_t *mode,
                      cudnnRNNAlgo_t *algo,
                      cudnnDataType_t *mathPrec);

cudnnStatus_t CUDNNWINAPI
cudnnSetRNNMatrixMathType(cudnnRNNDescriptor_t rnnDesc, cudnnMathType_t mType);

cudnnStatus_t CUDNNWINAPI
cudnnGetRNNMatrixMathType(cudnnRNNDescriptor_t rnnDesc, cudnnMathType_t *mType);

cudnnStatus_t CUDNNWINAPI
cudnnSetRNNBiasMode(cudnnRNNDescriptor_t rnnDesc, cudnnRNNBiasMode_t biasMode);

cudnnStatus_t CUDNNWINAPI
cudnnGetRNNBiasMode(cudnnRNNDescriptor_t rnnDesc, cudnnRNNBiasMode_t *biasMode);

cudnnStatus_t CUDNNWINAPI
cudnnRNNSetClip(cudnnHandle_t handle,
                cudnnRNNDescriptor_t rnnDesc,
                cudnnRNNClipMode_t clipMode,
                cudnnNanPropagation_t clipNanOpt,
                double lclip,
                double rclip);

cudnnStatus_t CUDNNWINAPI
cudnnRNNGetClip(cudnnHandle_t handle,
                cudnnRNNDescriptor_t rnnDesc,
                cudnnRNNClipMode_t *clipMode,
                cudnnNanPropagation_t *clipNanOpt,
                double *lclip,
                double *rclip);

cudnnStatus_t CUDNNWINAPI
cudnnSetRNNProjectionLayers(cudnnHandle_t handle,
                            cudnnRNNDescriptor_t rnnDesc,
                            const int recProjSize,
                            const int outProjSize);

cudnnStatus_t CUDNNWINAPI
cudnnGetRNNProjectionLayers(cudnnHandle_t handle,
                            const cudnnRNNDescriptor_t rnnDesc,
                            int *recProjSize,
                            int *outProjSize);

/* Expensive. Creates the plan for the specific settings. */
cudnnStatus_t CUDNNWINAPI
cudnnCreatePersistentRNNPlan(cudnnRNNDescriptor_t rnnDesc,
                             const int minibatch,
                             const cudnnDataType_t dataType,
                             cudnnPersistentRNNPlan_t *plan);

cudnnStatus_t CUDNNWINAPI
cudnnDestroyPersistentRNNPlan(cudnnPersistentRNNPlan_t plan);

cudnnStatus_t CUDNNWINAPI
cudnnSetPersistentRNNPlan(cudnnRNNDescriptor_t rnnDesc, cudnnPersistentRNNPlan_t plan);

/* dataType in weight descriptors and input descriptors is used to describe storage */
cudnnStatus_t CUDNNWINAPI
cudnnGetRNNWorkspaceSize(cudnnHandle_t handle,
                         const cudnnRNNDescriptor_t rnnDesc,
                         const int seqLength,
                         const cudnnTensorDescriptor_t *xDesc,
                         size_t *sizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnGetRNNTrainingReserveSize(cudnnHandle_t handle,
                               const cudnnRNNDescriptor_t rnnDesc,
                               const int seqLength,
                               const cudnnTensorDescriptor_t *xDesc,
                               size_t *sizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnGetRNNParamsSize(cudnnHandle_t handle,
                      const cudnnRNNDescriptor_t rnnDesc,
                      const cudnnTensorDescriptor_t xDesc,
                      size_t *sizeInBytes,
                      cudnnDataType_t dataType);

cudnnStatus_t CUDNNWINAPI
cudnnGetRNNLinLayerMatrixParams(cudnnHandle_t handle,
                                const cudnnRNNDescriptor_t rnnDesc,
                                const int pseudoLayer,
                                const cudnnTensorDescriptor_t xDesc,
                                const cudnnFilterDescriptor_t wDesc,
                                const void *w,
                                const int linLayerID,
                                cudnnFilterDescriptor_t linLayerMatDesc,
                                void **linLayerMat);

cudnnStatus_t CUDNNWINAPI
cudnnGetRNNLinLayerBiasParams(cudnnHandle_t handle,
                              const cudnnRNNDescriptor_t rnnDesc,
                              const int pseudoLayer,
                              const cudnnTensorDescriptor_t xDesc,
                              const cudnnFilterDescriptor_t wDesc,
                              const void *w,
                              const int linLayerID,
                              cudnnFilterDescriptor_t linLayerBiasDesc,
                              void **linLayerBias);

cudnnStatus_t CUDNNWINAPI
cudnnRNNForwardInference(cudnnHandle_t handle,
                         const cudnnRNNDescriptor_t rnnDesc,
                         const int seqLength,
                         const cudnnTensorDescriptor_t *xDesc,
                         const void *x,
                         const cudnnTensorDescriptor_t hxDesc,
                         const void *hx,
                         const cudnnTensorDescriptor_t cxDesc,
                         const void *cx,
                         const cudnnFilterDescriptor_t wDesc,
                         const void *w,
                         const cudnnTensorDescriptor_t *yDesc,
                         void *y,
                         const cudnnTensorDescriptor_t hyDesc,
                         void *hy,
                         const cudnnTensorDescriptor_t cyDesc,
                         void *cy,
                         void *workspace,
                         size_t workSpaceSizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnRNNForwardTraining(cudnnHandle_t handle,
                        const cudnnRNNDescriptor_t rnnDesc,
                        const int seqLength,
                        const cudnnTensorDescriptor_t *xDesc,
                        const void *x,
                        const cudnnTensorDescriptor_t hxDesc,
                        const void *hx,
                        const cudnnTensorDescriptor_t cxDesc,
                        const void *cx,
                        const cudnnFilterDescriptor_t wDesc,
                        const void *w,
                        const cudnnTensorDescriptor_t *yDesc,
                        void *y,
                        const cudnnTensorDescriptor_t hyDesc,
                        void *hy,
                        const cudnnTensorDescriptor_t cyDesc,
                        void *cy,
                        void *workspace,
                        size_t workSpaceSizeInBytes,
                        void *reserveSpace,
                        size_t reserveSpaceSizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnRNNBackwardData(cudnnHandle_t handle,
                     const cudnnRNNDescriptor_t rnnDesc,
                     const int seqLength,
                     const cudnnTensorDescriptor_t *yDesc,
                     const void *y,
                     const cudnnTensorDescriptor_t *dyDesc,
                     const void *dy,
                     const cudnnTensorDescriptor_t dhyDesc,
                     const void *dhy,
                     const cudnnTensorDescriptor_t dcyDesc,
                     const void *dcy,
                     const cudnnFilterDescriptor_t wDesc,
                     const void *w,
                     const cudnnTensorDescriptor_t hxDesc,
                     const void *hx,
                     const cudnnTensorDescriptor_t cxDesc,
                     const void *cx,
                     const cudnnTensorDescriptor_t *dxDesc,
                     void *dx,
                     const cudnnTensorDescriptor_t dhxDesc,
                     void *dhx,
                     const cudnnTensorDescriptor_t dcxDesc,
                     void *dcx,
                     void *workspace,
                     size_t workSpaceSizeInBytes,
                     void *reserveSpace,
                     size_t reserveSpaceSizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnRNNBackwardWeights(cudnnHandle_t handle,
                        const cudnnRNNDescriptor_t rnnDesc,
                        const int seqLength,
                        const cudnnTensorDescriptor_t *xDesc,
                        const void *x,
                        const cudnnTensorDescriptor_t hxDesc,
                        const void *hx,
                        const cudnnTensorDescriptor_t *yDesc,
                        const void *y,
                        const void *workspace,
                        size_t workSpaceSizeInBytes,
                        const cudnnFilterDescriptor_t dwDesc,
                        void *dw,
                        const void *reserveSpace,
                        size_t reserveSpaceSizeInBytes);

/* RNN EX API */

cudnnStatus_t CUDNNWINAPI
cudnnSetRNNPaddingMode(cudnnRNNDescriptor_t rnnDesc, cudnnRNNPaddingMode_t paddingMode);

cudnnStatus_t CUDNNWINAPI
cudnnGetRNNPaddingMode(cudnnRNNDescriptor_t rnnDesc, cudnnRNNPaddingMode_t *paddingMode);

cudnnStatus_t CUDNNWINAPI
cudnnCreateRNNDataDescriptor(cudnnRNNDataDescriptor_t *rnnDataDesc);

cudnnStatus_t CUDNNWINAPI
cudnnDestroyRNNDataDescriptor(cudnnRNNDataDescriptor_t rnnDataDesc);

cudnnStatus_t CUDNNWINAPI
cudnnSetRNNDataDescriptor(cudnnRNNDataDescriptor_t rnnDataDesc,
                          cudnnDataType_t dataType,
                          cudnnRNNDataLayout_t layout,
                          int maxSeqLength,
                          int batchSize,
                          int vectorSize,
                          const int seqLengthArray[], /* length of each sequence in the batch */
                          void *paddingFill);         /* symbol for filling padding position in output */

cudnnStatus_t CUDNNWINAPI
cudnnGetRNNDataDescriptor(cudnnRNNDataDescriptor_t rnnDataDesc,
                          cudnnDataType_t *dataType,
                          cudnnRNNDataLayout_t *layout,
                          int *maxSeqLength,
                          int *batchSize,
                          int *vectorSize,
                          int arrayLengthRequested,
                          int seqLengthArray[],
                          void *paddingFill);

cudnnStatus_t CUDNNWINAPI
cudnnRNNForwardTrainingEx(cudnnHandle_t handle,
                          const cudnnRNNDescriptor_t rnnDesc,
                          const cudnnRNNDataDescriptor_t xDesc,
                          const void *x,
                          const cudnnTensorDescriptor_t hxDesc,
                          const void *hx,
                          const cudnnTensorDescriptor_t cxDesc,
                          const void *cx,
                          const cudnnFilterDescriptor_t wDesc,
                          const void *w,
                          const cudnnRNNDataDescriptor_t yDesc,
                          void *y,
                          const cudnnTensorDescriptor_t hyDesc,
                          void *hy,
                          const cudnnTensorDescriptor_t cyDesc,
                          void *cy,
                          const cudnnRNNDataDescriptor_t kDesc, /* reserved, should pass NULL */
                          const void *keys,                     /* reserved, should pass NULL */
                          const cudnnRNNDataDescriptor_t cDesc, /* reserved, should pass NULL */
                          void *cAttn,                          /* reserved, should pass NULL */
                          const cudnnRNNDataDescriptor_t iDesc, /* reserved, should pass NULL */
                          void *iAttn,                          /* reserved, should pass NULL */
                          const cudnnRNNDataDescriptor_t qDesc, /* reserved, should pass NULL */
                          void *queries,                        /* reserved, should pass NULL */
                          void *workSpace,
                          size_t workSpaceSizeInBytes,
                          void *reserveSpace,
                          size_t reserveSpaceSizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnRNNForwardInferenceEx(cudnnHandle_t handle,
                           const cudnnRNNDescriptor_t rnnDesc,
                           const cudnnRNNDataDescriptor_t xDesc,
                           const void *x,
                           const cudnnTensorDescriptor_t hxDesc,
                           const void *hx,
                           const cudnnTensorDescriptor_t cxDesc,
                           const void *cx,
                           const cudnnFilterDescriptor_t wDesc,
                           const void *w,
                           const cudnnRNNDataDescriptor_t yDesc,
                           void *y,
                           const cudnnTensorDescriptor_t hyDesc,
                           void *hy,
                           const cudnnTensorDescriptor_t cyDesc,
                           void *cy,
                           const cudnnRNNDataDescriptor_t kDesc, /* reserved, should pass NULL */
                           const void *keys,                     /* reserved, should pass NULL */
                           const cudnnRNNDataDescriptor_t cDesc, /* reserved, should pass NULL */
                           void *cAttn,                          /* reserved, should pass NULL */
                           const cudnnRNNDataDescriptor_t iDesc, /* reserved, should pass NULL */
                           void *iAttn,                          /* reserved, should pass NULL */
                           const cudnnRNNDataDescriptor_t qDesc, /* reserved, should pass NULL */
                           void *queries,                        /* reserved, should pass NULL */
                           void *workSpace,
                           size_t workSpaceSizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnRNNBackwardDataEx(cudnnHandle_t handle,
                       const cudnnRNNDescriptor_t rnnDesc,
                       const cudnnRNNDataDescriptor_t yDesc,
                       const void *y,
                       const cudnnRNNDataDescriptor_t dyDesc,
                       const void *dy,
                       const cudnnRNNDataDescriptor_t dcDesc, /* reserved, should pass NULL */
                       const void *dcAttn,                    /* reserved, should pass NULL */
                       const cudnnTensorDescriptor_t dhyDesc,
                       const void *dhy,
                       const cudnnTensorDescriptor_t dcyDesc,
                       const void *dcy,
                       const cudnnFilterDescriptor_t wDesc,
                       const void *w,
                       const cudnnTensorDescriptor_t hxDesc,
                       const void *hx,
                       const cudnnTensorDescriptor_t cxDesc,
                       const void *cx,
                       const cudnnRNNDataDescriptor_t dxDesc,
                       void *dx,
                       const cudnnTensorDescriptor_t dhxDesc,
                       void *dhx,
                       const cudnnTensorDescriptor_t dcxDesc,
                       void *dcx,
                       const cudnnRNNDataDescriptor_t dkDesc, /* reserved, should pass NULL */
                       void *dkeys,                           /* reserved, should pass NULL */
                       void *workSpace,
                       size_t workSpaceSizeInBytes,
                       void *reserveSpace,
                       size_t reserveSpaceSizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnRNNBackwardWeightsEx(cudnnHandle_t handle,
                          const cudnnRNNDescriptor_t rnnDesc,
                          const cudnnRNNDataDescriptor_t xDesc,
                          const void *x,
                          const cudnnTensorDescriptor_t hxDesc,
                          const void *hx,
                          const cudnnRNNDataDescriptor_t yDesc,
                          const void *y,
                          void *workSpace,
                          size_t workSpaceSizeInBytes,
                          const cudnnFilterDescriptor_t dwDesc,
                          void *dw,
                          void *reserveSpace,
                          size_t reserveSpaceSizeInBytes);

/* RNN FIND API */

typedef struct cudnnAlgorithmStruct *cudnnAlgorithmDescriptor_t;

typedef struct cudnnAlgorithmPerformanceStruct *cudnnAlgorithmPerformance_t;

cudnnStatus_t CUDNNWINAPI
cudnnSetRNNAlgorithmDescriptor(cudnnHandle_t handle, cudnnRNNDescriptor_t rnnDesc, cudnnAlgorithmDescriptor_t algoDesc);

cudnnStatus_t CUDNNWINAPI
cudnnGetRNNForwardInferenceAlgorithmMaxCount(cudnnHandle_t handle, const cudnnRNNDescriptor_t rnnDesc, int *count);

cudnnStatus_t CUDNNWINAPI
cudnnFindRNNForwardInferenceAlgorithmEx(cudnnHandle_t handle,
                                        const cudnnRNNDescriptor_t rnnDesc,
                                        const int seqLength,
                                        const cudnnTensorDescriptor_t *xDesc,
                                        const void *x,
                                        const cudnnTensorDescriptor_t hxDesc,
                                        const void *hx,
                                        const cudnnTensorDescriptor_t cxDesc,
                                        const void *cx,
                                        const cudnnFilterDescriptor_t wDesc,
                                        const void *w,
                                        const cudnnTensorDescriptor_t *yDesc,
                                        void *y,
                                        const cudnnTensorDescriptor_t hyDesc,
                                        void *hy,
                                        const cudnnTensorDescriptor_t cyDesc,
                                        void *cy,
                                        const float findIntensity,
                                        const int requestedAlgoCount,
                                        int *returnedAlgoCount,
                                        cudnnAlgorithmPerformance_t *perfResults,
                                        void *workspace,
                                        size_t workSpaceSizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnGetRNNForwardTrainingAlgorithmMaxCount(cudnnHandle_t handle, const cudnnRNNDescriptor_t rnnDesc, int *count);

cudnnStatus_t CUDNNWINAPI
cudnnFindRNNForwardTrainingAlgorithmEx(cudnnHandle_t handle,
                                       const cudnnRNNDescriptor_t rnnDesc,
                                       const int seqLength,
                                       const cudnnTensorDescriptor_t *xDesc,
                                       const void *x,
                                       const cudnnTensorDescriptor_t hxDesc,
                                       const void *hx,
                                       const cudnnTensorDescriptor_t cxDesc,
                                       const void *cx,
                                       const cudnnFilterDescriptor_t wDesc,
                                       const void *w,
                                       const cudnnTensorDescriptor_t *yDesc,
                                       void *y,
                                       const cudnnTensorDescriptor_t hyDesc,
                                       void *hy,
                                       const cudnnTensorDescriptor_t cyDesc,
                                       void *cy,
                                       const float findIntensity,
                                       const int requestedAlgoCount,
                                       int *returnedAlgoCount,
                                       cudnnAlgorithmPerformance_t *perfResults,
                                       void *workspace,
                                       size_t workSpaceSizeInBytes,
                                       void *reserveSpace,
                                       size_t reserveSpaceSizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnGetRNNBackwardDataAlgorithmMaxCount(cudnnHandle_t handle, const cudnnRNNDescriptor_t rnnDesc, int *count);

cudnnStatus_t CUDNNWINAPI
cudnnFindRNNBackwardDataAlgorithmEx(cudnnHandle_t handle,
                                    const cudnnRNNDescriptor_t rnnDesc,
                                    const int seqLength,
                                    const cudnnTensorDescriptor_t *yDesc,
                                    const void *y,
                                    const cudnnTensorDescriptor_t *dyDesc,
                                    const void *dy,
                                    const cudnnTensorDescriptor_t dhyDesc,
                                    const void *dhy,
                                    const cudnnTensorDescriptor_t dcyDesc,
                                    const void *dcy,
                                    const cudnnFilterDescriptor_t wDesc,
                                    const void *w,
                                    const cudnnTensorDescriptor_t hxDesc,
                                    const void *hx,
                                    const cudnnTensorDescriptor_t cxDesc,
                                    const void *cx,
                                    const cudnnTensorDescriptor_t *dxDesc,
                                    void *dx,
                                    const cudnnTensorDescriptor_t dhxDesc,
                                    void *dhx,
                                    const cudnnTensorDescriptor_t dcxDesc,
                                    void *dcx,
                                    const float findIntensity,
                                    const int requestedAlgoCount,
                                    int *returnedAlgoCount,
                                    cudnnAlgorithmPerformance_t *perfResults,
                                    void *workspace,
                                    size_t workSpaceSizeInBytes,
                                    void *reserveSpace,
                                    size_t reserveSpaceSizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnGetRNNBackwardWeightsAlgorithmMaxCount(cudnnHandle_t handle, const cudnnRNNDescriptor_t rnnDesc, int *count);

cudnnStatus_t CUDNNWINAPI
cudnnFindRNNBackwardWeightsAlgorithmEx(cudnnHandle_t handle,
                                       const cudnnRNNDescriptor_t rnnDesc,
                                       const int seqLength,
                                       const cudnnTensorDescriptor_t *xDesc,
                                       const void *x,
                                       const cudnnTensorDescriptor_t hxDesc,
                                       const void *hx,
                                       const cudnnTensorDescriptor_t *yDesc,
                                       const void *y,
                                       const float findIntensity,
                                       const int requestedAlgoCount,
                                       int *returnedAlgoCount,
                                       cudnnAlgorithmPerformance_t *perfResults,
                                       const void *workspace,
                                       size_t workSpaceSizeInBytes,
                                       const cudnnFilterDescriptor_t dwDesc,
                                       void *dw,
                                       const void *reserveSpace,
                                       size_t reserveSpaceSizeInBytes);

/* Sequence data descriptor */

typedef enum {
    CUDNN_SEQDATA_TIME_DIM  = 0, /* index in time */
    CUDNN_SEQDATA_BATCH_DIM = 1, /* index in batch */
    CUDNN_SEQDATA_BEAM_DIM  = 2, /* index in beam */
    CUDNN_SEQDATA_VECT_DIM  = 3  /* index in vector */
} cudnnSeqDataAxis_t;

#define CUDNN_SEQDATA_DIM_COUNT 4 /* dimension count */

struct cudnnSeqDataStruct;
typedef struct cudnnSeqDataStruct *cudnnSeqDataDescriptor_t;

cudnnStatus_t CUDNNWINAPI
cudnnCreateSeqDataDescriptor(cudnnSeqDataDescriptor_t *seqDataDesc);

cudnnStatus_t CUDNNWINAPI
cudnnDestroySeqDataDescriptor(cudnnSeqDataDescriptor_t seqDataDesc);

cudnnStatus_t CUDNNWINAPI
cudnnSetSeqDataDescriptor(cudnnSeqDataDescriptor_t seqDataDesc,
                          cudnnDataType_t dataType,
                          int nbDims,
                          const int dimA[],
                          const cudnnSeqDataAxis_t axes[],
                          size_t seqLengthArraySize,
                          const int seqLengthArray[],
                          void *paddingFill);

cudnnStatus_t CUDNNWINAPI
cudnnGetSeqDataDescriptor(const cudnnSeqDataDescriptor_t seqDataDesc,
                          cudnnDataType_t *dataType,
                          int *nbDims,
                          int nbDimsRequested,
                          int dimA[],
                          cudnnSeqDataAxis_t axes[],
                          size_t *seqLengthArraySize,
                          size_t seqLengthSizeRequested,
                          int seqLengthArray[],
                          void *paddingFill);

/* Multihead Attention */

/* Legacy type for backward compatibility */
typedef unsigned cudnnAttnQueryMap_t;

/* Multi-head attention modes set in attention descriptor */
#define CUDNN_ATTN_QUERYMAP_ALL_TO_ONE 0         /* multiple Q-s map to a single (K,V) set when beam size > 1 */
#define CUDNN_ATTN_QUERYMAP_ONE_TO_ONE (1U << 0) /* multiple Q-s map to multiple (K,V) sets when beam size > 1 */
#define CUDNN_ATTN_DISABLE_PROJ_BIASES 0         /* no biases in attention input and output projections */
#define CUDNN_ATTN_ENABLE_PROJ_BIASES (1U << 1)  /* use biases in attention input and output projections */

struct cudnnAttnStruct;
typedef struct cudnnAttnStruct *cudnnAttnDescriptor_t;

cudnnStatus_t CUDNNWINAPI
cudnnCreateAttnDescriptor(cudnnAttnDescriptor_t *attnDesc);

cudnnStatus_t CUDNNWINAPI
cudnnDestroyAttnDescriptor(cudnnAttnDescriptor_t attnDesc);

cudnnStatus_t CUDNNWINAPI
cudnnSetAttnDescriptor(cudnnAttnDescriptor_t attnDesc,
                       unsigned attnMode,
                       int nHeads,
                       double smScaler,
                       cudnnDataType_t dataType,
                       cudnnDataType_t computePrec,
                       cudnnMathType_t mathType,
                       cudnnDropoutDescriptor_t attnDropoutDesc,
                       cudnnDropoutDescriptor_t postDropoutDesc,
                       int qSize,
                       int kSize,
                       int vSize,
                       int qProjSize,
                       int kProjSize,
                       int vProjSize,
                       int oProjSize,
                       int qoMaxSeqLength,
                       int kvMaxSeqLength,
                       int maxBatchSize,
                       int maxBeamSize);

cudnnStatus_t CUDNNWINAPI
cudnnGetAttnDescriptor(cudnnAttnDescriptor_t attnDesc,
                       unsigned *attnMode,
                       int *nHeads,
                       double *smScaler,
                       cudnnDataType_t *dataType,
                       cudnnDataType_t *computePrec,
                       cudnnMathType_t *mathType,
                       cudnnDropoutDescriptor_t *attnDropoutDesc,
                       cudnnDropoutDescriptor_t *postDropoutDesc,
                       int *qSize,
                       int *kSize,
                       int *vSize,
                       int *qProjSize,
                       int *kProjSize,
                       int *vProjSize,
                       int *oProjSize,
                       int *qoMaxSeqLength,
                       int *kvMaxSeqLength,
                       int *maxBatchSize,
                       int *maxBeamSize);

cudnnStatus_t CUDNNWINAPI
cudnnGetMultiHeadAttnBuffers(cudnnHandle_t handle,
                             const cudnnAttnDescriptor_t attnDesc,
                             size_t *weightSizeInBytes,
                             size_t *workSpaceSizeInBytes,
                             size_t *reserveSpaceSizeInBytes);

typedef enum {
    CUDNN_MH_ATTN_Q_WEIGHTS = 0, /* input projection weights for 'queries' */
    CUDNN_MH_ATTN_K_WEIGHTS = 1, /* input projection weights for 'keys' */
    CUDNN_MH_ATTN_V_WEIGHTS = 2, /* input projection weights for 'values' */
    CUDNN_MH_ATTN_O_WEIGHTS = 3, /* output projection weights */
    CUDNN_MH_ATTN_Q_BIASES  = 4, /* input projection bias tensor for 'queries' */
    CUDNN_MH_ATTN_K_BIASES  = 5, /* input projection bias for 'keys' */
    CUDNN_MH_ATTN_V_BIASES  = 6, /* input projection bias for 'values' */
    CUDNN_MH_ATTN_O_BIASES  = 7, /* output projection biases */
} cudnnMultiHeadAttnWeightKind_t;

#define CUDNN_ATTN_WKIND_COUNT 8 /* Number of attention weight/bias tensors */

cudnnStatus_t CUDNNWINAPI
cudnnGetMultiHeadAttnWeights(cudnnHandle_t handle,
                             const cudnnAttnDescriptor_t attnDesc,
                             cudnnMultiHeadAttnWeightKind_t wKind,
                             size_t weightSizeInBytes,
                             const void *weights,
                             cudnnTensorDescriptor_t wDesc,
                             void **wAddr);

cudnnStatus_t CUDNNWINAPI
cudnnMultiHeadAttnForward(cudnnHandle_t handle,
                          const cudnnAttnDescriptor_t attnDesc,
                          int currIdx,
                          const int loWinIdx[],
                          const int hiWinIdx[],
                          const int devSeqLengthsQO[],
                          const int devSeqLengthsKV[],
                          const cudnnSeqDataDescriptor_t qDesc,
                          const void *queries,
                          const void *residuals,
                          const cudnnSeqDataDescriptor_t kDesc,
                          const void *keys,
                          const cudnnSeqDataDescriptor_t vDesc,
                          const void *values,
                          const cudnnSeqDataDescriptor_t oDesc,
                          void *out,
                          size_t weightSizeInBytes,
                          const void *weights,
                          size_t workSpaceSizeInBytes,
                          void *workSpace,
                          size_t reserveSpaceSizeInBytes,
                          void *reserveSpace);

cudnnStatus_t CUDNNWINAPI
cudnnMultiHeadAttnBackwardData(cudnnHandle_t handle,
                               const cudnnAttnDescriptor_t attnDesc,
                               const int loWinIdx[],
                               const int hiWinIdx[],
                               const int devSeqLengthsDQDO[],
                               const int devSeqLengthsDKDV[],
                               const cudnnSeqDataDescriptor_t doDesc,
                               const void *dout,
                               const cudnnSeqDataDescriptor_t dqDesc,
                               void *dqueries,
                               const void *queries,
                               const cudnnSeqDataDescriptor_t dkDesc,
                               void *dkeys,
                               const void *keys,
                               const cudnnSeqDataDescriptor_t dvDesc,
                               void *dvalues,
                               const void *values,
                               size_t weightSizeInBytes,
                               const void *weights,
                               size_t workSpaceSizeInBytes,
                               void *workSpace,
                               size_t reserveSpaceSizeInBytes,
                               void *reserveSpace);

typedef enum {
    CUDNN_WGRAD_MODE_ADD = 0, /* add partial gradients to wgrad output buffers */
    CUDNN_WGRAD_MODE_SET = 1, /* write partial gradients to wgrad output buffers */
} cudnnWgradMode_t;

cudnnStatus_t CUDNNWINAPI
cudnnMultiHeadAttnBackwardWeights(cudnnHandle_t handle,
                                  const cudnnAttnDescriptor_t attnDesc,
                                  cudnnWgradMode_t addGrad,
                                  const cudnnSeqDataDescriptor_t qDesc,
                                  const void *queries,
                                  const cudnnSeqDataDescriptor_t kDesc,
                                  const void *keys,
                                  const cudnnSeqDataDescriptor_t vDesc,
                                  const void *values,
                                  const cudnnSeqDataDescriptor_t doDesc,
                                  const void *dout,
                                  size_t weightSizeInBytes,
                                  const void *weights,
                                  void *dweights,
                                  size_t workSpaceSizeInBytes,
                                  void *workSpace,
                                  size_t reserveSpaceSizeInBytes,
                                  void *reserveSpace);

/* CTC LOSS */
typedef enum { CUDNN_CTC_LOSS_ALGO_DETERMINISTIC = 0, CUDNN_CTC_LOSS_ALGO_NON_DETERMINISTIC = 1 } cudnnCTCLossAlgo_t;

/* Input normalization mode for loss function */
typedef enum { CUDNN_LOSS_NORMALIZATION_NONE = 0, CUDNN_LOSS_NORMALIZATION_SOFTMAX = 1 } cudnnLossNormalizationMode_t;

/*
* CTC (Connectionist Temporal Classification) loss descriptor create/destory/set/get functions
*/
cudnnStatus_t CUDNNWINAPI
cudnnCreateCTCLossDescriptor(cudnnCTCLossDescriptor_t *ctcLossDesc);

cudnnStatus_t CUDNNWINAPI
cudnnSetCTCLossDescriptor(cudnnCTCLossDescriptor_t ctcLossDesc, cudnnDataType_t compType);

cudnnStatus_t CUDNNWINAPI
cudnnSetCTCLossDescriptorEx(cudnnCTCLossDescriptor_t ctcLossDesc,
                            cudnnDataType_t compType,
                            cudnnLossNormalizationMode_t normMode,
                            cudnnNanPropagation_t gradMode);

cudnnStatus_t CUDNNWINAPI
cudnnGetCTCLossDescriptor(cudnnCTCLossDescriptor_t ctcLossDesc, cudnnDataType_t *compType);

cudnnStatus_t CUDNNWINAPI
cudnnGetCTCLossDescriptorEx(cudnnCTCLossDescriptor_t ctcLossDesc,
                            cudnnDataType_t *compType,
                            cudnnLossNormalizationMode_t *normMode,
                            cudnnNanPropagation_t *gradMode);

cudnnStatus_t CUDNNWINAPI
cudnnDestroyCTCLossDescriptor(cudnnCTCLossDescriptor_t ctcLossDesc);

/* return the ctc costs and gradients, given the probabilities and labels */
cudnnStatus_t CUDNNWINAPI
cudnnCTCLoss(
    cudnnHandle_t handle,
    const cudnnTensorDescriptor_t
        probsDesc,     /* Tensor descriptor for probabilities, the dimensions are T,N,A (T is the timing steps, N is the
                          mini batch size, A is the alphabet size)  */
    const void *probs, /* probabilities after softmax, in GPU memory */
    const int *labels, /* labels, in CPU memory */
    const int *labelLengths,                     /* the length of each label, in CPU memory */
    const int *inputLengths,                     /* the lengths of timing steps in each batch, in CPU memory */
    void *costs,                                 /* the returned costs of CTC, in GPU memory */
    const cudnnTensorDescriptor_t gradientsDesc, /* Tensor descriptor for gradients, the dimensions are T,N,A */
    const void *gradients,   /* the returned CTC gradients, in GPU memory, to compute costs only, set it to NULL */
    cudnnCTCLossAlgo_t algo, /* algorithm selected, supported now 0 and 1 */
    cudnnCTCLossDescriptor_t ctcLossDesc,
    void *workspace,              /* pointer to the workspace, in GPU memory */
    size_t workSpaceSizeInBytes); /* size of the workspace */

/* return the workspace size needed for ctc */
cudnnStatus_t CUDNNWINAPI
cudnnGetCTCLossWorkspaceSize(
    cudnnHandle_t handle,
    const cudnnTensorDescriptor_t probsDesc, /* Tensor descriptor for probabilities, the dimensions are T,N,A (T is the
                                                timing steps, N is the mini batch size, A is the alphabet size) */
    const cudnnTensorDescriptor_t gradientsDesc, /* Tensor descriptor for gradients, the
                                                    dimensions are T,N,A. To compute costs
                                                    only, set it to NULL */
    const int *labels,                           /* labels, in CPU memory */
    const int *labelLengths,                     /* the length of each label, in CPU memory */
    const int *inputLengths,                     /* the lengths of timing steps in each batch, in CPU memory */
    cudnnCTCLossAlgo_t algo,                     /* algorithm selected, supported now 0 and 1 */
    cudnnCTCLossDescriptor_t ctcLossDesc,
    size_t *sizeInBytes); /* pointer to the returned workspace size */

typedef struct {
    union Algorithm {
        cudnnConvolutionFwdAlgo_t convFwdAlgo;
        cudnnConvolutionBwdFilterAlgo_t convBwdFilterAlgo;
        cudnnConvolutionBwdDataAlgo_t convBwdDataAlgo;
        cudnnRNNAlgo_t RNNAlgo;
        cudnnCTCLossAlgo_t CTCLossAlgo;
    } algo;
} cudnnAlgorithm_t;

cudnnStatus_t CUDNNWINAPI
cudnnCreateAlgorithmDescriptor(cudnnAlgorithmDescriptor_t *algoDesc);

cudnnStatus_t CUDNNWINAPI
cudnnSetAlgorithmDescriptor(cudnnAlgorithmDescriptor_t algoDesc, cudnnAlgorithm_t algorithm);

cudnnStatus_t CUDNNWINAPI
cudnnGetAlgorithmDescriptor(const cudnnAlgorithmDescriptor_t algoDesc, cudnnAlgorithm_t *algorithm);

cudnnStatus_t CUDNNWINAPI
cudnnCopyAlgorithmDescriptor(const cudnnAlgorithmDescriptor_t src, cudnnAlgorithmDescriptor_t dest);

cudnnStatus_t CUDNNWINAPI
cudnnDestroyAlgorithmDescriptor(cudnnAlgorithmDescriptor_t algoDesc);

cudnnStatus_t CUDNNWINAPI
cudnnCreateAlgorithmPerformance(cudnnAlgorithmPerformance_t *algoPerf, int numberToCreate);

cudnnStatus_t CUDNNWINAPI
cudnnSetAlgorithmPerformance(cudnnAlgorithmPerformance_t algoPerf,
                             cudnnAlgorithmDescriptor_t algoDesc,
                             cudnnStatus_t status,
                             float time,
                             size_t memory);

cudnnStatus_t CUDNNWINAPI
cudnnGetAlgorithmPerformance(const cudnnAlgorithmPerformance_t algoPerf,
                             cudnnAlgorithmDescriptor_t *algoDesc,
                             cudnnStatus_t *status,
                             float *time,
                             size_t *memory);

cudnnStatus_t CUDNNWINAPI
cudnnDestroyAlgorithmPerformance(cudnnAlgorithmPerformance_t *algoPerf, int numberToDestroy);

cudnnStatus_t CUDNNWINAPI
cudnnGetAlgorithmSpaceSize(cudnnHandle_t handle, cudnnAlgorithmDescriptor_t algoDesc, size_t *algoSpaceSizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnSaveAlgorithm(cudnnHandle_t handle,
                   cudnnAlgorithmDescriptor_t algoDesc,
                   void *algoSpace,
                   size_t algoSpaceSizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnRestoreAlgorithm(cudnnHandle_t handle,
                      void *algoSpace,
                      size_t algoSpaceSizeInBytes,
                      cudnnAlgorithmDescriptor_t algoDesc);

typedef enum {
    CUDNN_SEV_FATAL   = 0,
    CUDNN_SEV_ERROR   = 1,
    CUDNN_SEV_WARNING = 2,
    CUDNN_SEV_INFO    = 3,
} cudnnSeverity_t;

/* Message masks to be used with cudnnSetCallback() */
#define CUDNN_SEV_ERROR_EN (1U << CUDNN_SEV_ERROR)
#define CUDNN_SEV_WARNING_EN (1U << CUDNN_SEV_WARNING)
#define CUDNN_SEV_INFO_EN (1U << CUDNN_SEV_INFO)

/* struct containing useful informaiton for each API call */
typedef struct {
    unsigned cudnn_version;
    cudnnStatus_t cudnnStatus;
    unsigned time_sec;      /* epoch time in seconds */
    unsigned time_usec;     /* microseconds part of epoch time */
    unsigned time_delta;    /* time since start in seconds */
    cudnnHandle_t handle;   /* cudnn handle */
    cudaStream_t stream;    /* cuda stream ID */
    unsigned long long pid; /* process ID */
    unsigned long long tid; /* thread ID */
    int cudaDeviceId;       /* CUDA device ID */
    int reserved[15];       /* reserved for future use */
} cudnnDebug_t;

typedef void (*cudnnCallback_t)(cudnnSeverity_t sev, void *udata, const cudnnDebug_t *dbg, const char *msg);

cudnnStatus_t CUDNNWINAPI
cudnnSetCallback(unsigned mask, void *udata, cudnnCallback_t fptr);

cudnnStatus_t CUDNNWINAPI
cudnnGetCallback(unsigned *mask, void **udata, cudnnCallback_t *fptr);

struct cudnnFusedOpsConstParamStruct;
typedef struct cudnnFusedOpsConstParamStruct *cudnnFusedOpsConstParamPack_t;

struct cudnnFusedOpsVariantParamStruct;
typedef struct cudnnFusedOpsVariantParamStruct *cudnnFusedOpsVariantParamPack_t;

struct cudnnFusedOpsPlanStruct;
typedef struct cudnnFusedOpsPlanStruct *cudnnFusedOpsPlan_t;

typedef enum {
    /* each op in [ ] can be disabled by passing NULL ptr */
    /* [per channel scale], [per channel bias], [activation], convolution, [generate BN stats] */
    CUDNN_FUSED_SCALE_BIAS_ACTIVATION_CONV_BNSTATS = 0,
    /* [per channel scale], [per channel bias], [activation], convolutionBackwardWeights */
    CUDNN_FUSED_SCALE_BIAS_ACTIVATION_WGRAD = 1,
    /* utility for BN training in BN-conv fusion */
    /* computes the equivalent scale and bias from ySum ySqSum and learned scale, bias */
    /* optionally update running stats and generate saved stats */
    CUDNN_FUSED_BN_FINALIZE_STATISTICS_TRAINING = 2,
    /* utility for BN inference in BN-conv fusion */
    /* computes the equivalent scale and bias from learned running stats and learned scale, bias */
    CUDNN_FUSED_BN_FINALIZE_STATISTICS_INFERENCE = 3,
    /* reserved for future use: convolution, [per channel scale], [per channel bias], [residual add], [activation] */
    CUDNN_FUSED_CONV_SCALE_BIAS_ADD_ACTIVATION = 4,
    /* reserved for future use: [per channel scale], [per channel bias], [residual add],  activation, bitmask */
    CUDNN_FUSED_SCALE_BIAS_ADD_ACTIVATION_GEN_BITMASK = 5,
    /* reserved for future use */
    CUDNN_FUSED_DACTIVATION_FORK_DBATCHNORM = 6,
} cudnnFusedOps_t;

typedef enum {
    /* set XDESC: pass previously initialized cudnnTensorDescriptor_t */
    /* get XDESC: pass previously created cudnnTensorDescriptor_t */
    CUDNN_PARAM_XDESC = 0,
    /* set/get XDATA_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_XDATA_PLACEHOLDER = 1,
    /* set/get BN_MODE: pass cudnnBatchNormMode_t* */
    CUDNN_PARAM_BN_MODE = 2,
    /* set CUDNN_PARAM_BN_EQSCALEBIAS_DESC: pass previously initialized cudnnTensorDescriptor_t */
    /* get CUDNN_PARAM_BN_EQSCALEBIAS_DESC: pass previously created cudnnTensorDescriptor_t */
    CUDNN_PARAM_BN_EQSCALEBIAS_DESC = 3,
    /* set/get BN_EQSCALE_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_BN_EQSCALE_PLACEHOLDER = 4,
    /* set/get BN_EQBIAS_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_BN_EQBIAS_PLACEHOLDER = 5,
    /* set ACTIVATION_DESC: pass previously initialized cudnnActivationDescriptor_t */
    /* get ACTIVATION_DESC: pass previously created cudnnActivationDescriptor_t */
    CUDNN_PARAM_ACTIVATION_DESC = 6,
    /* set CONV_DESC: pass previously initialized cudnnConvolutionDescriptor_t */
    /* get CONV_DESC: pass previously created cudnnConvolutionDescriptor_t */
    CUDNN_PARAM_CONV_DESC = 7,
    /* set WDESC: pass previously initialized cudnnFilterDescriptor_t */
    /* get WDESC: pass previously created cudnnFilterDescriptor_t */
    CUDNN_PARAM_WDESC = 8,
    /* set/get WDATA_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_WDATA_PLACEHOLDER = 9,
    /* set DWDESC: pass previously initialized cudnnFilterDescriptor_t */
    /* get DWDESC: pass previously created cudnnFilterDescriptor_t */
    CUDNN_PARAM_DWDESC = 10,
    /* set/get DWDATA_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_DWDATA_PLACEHOLDER = 11,
    /* set YDESC: pass previously initialized cudnnTensorDescriptor_t */
    /* get YDESC: pass previously created cudnnTensorDescriptor_t */
    CUDNN_PARAM_YDESC = 12,
    /* set/get YDATA_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_YDATA_PLACEHOLDER = 13,
    /* set DYDESC: pass previously initialized cudnnTensorDescriptor_t */
    /* get DYDESC: pass previously created cudnnTensorDescriptor_t */
    CUDNN_PARAM_DYDESC = 14,
    /* set/get DYDATA_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_DYDATA_PLACEHOLDER = 15,
    /* set YSTATS_DESC: pass previously initialized cudnnTensorDescriptor_t */
    /* get YSTATS_DESC: pass previously created cudnnTensorDescriptor_t */
    CUDNN_PARAM_YSTATS_DESC = 16,
    /* set/get YSUM_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_YSUM_PLACEHOLDER = 17,
    /* set/get YSQSUM_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_YSQSUM_PLACEHOLDER = 18,
    /* set CUDNN_PARAM_BN_SCALEBIAS_MEANVAR_DESC: pass previously initialized cudnnTensorDescriptor_t */
    /* get CUDNN_PARAM_BN_SCALEBIAS_MEANVAR_DESC: pass previously created cudnnTensorDescriptor_t */
    CUDNN_PARAM_BN_SCALEBIAS_MEANVAR_DESC = 19,
    /* set/get CUDNN_PARAM_BN_SCALE_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_BN_SCALE_PLACEHOLDER = 20,
    /* set/get CUDNN_PARAM_BN_BIAS_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_BN_BIAS_PLACEHOLDER = 21,
    /* set/get CUDNN_PARAM_BN_SAVED_MEAN_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_BN_SAVED_MEAN_PLACEHOLDER = 22,
    /* set/get CUDNN_PARAM_BN_SAVED_INVSTD_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_BN_SAVED_INVSTD_PLACEHOLDER = 23,
    /* set/get CUDNN_PARAM_BN_RUNNING_MEAN_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_BN_RUNNING_MEAN_PLACEHOLDER = 24,
    /* set/get CUDNN_PARAM_BN_RUNNING_VAR_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_BN_RUNNING_VAR_PLACEHOLDER = 25,

    /* set ZDESC: pass previously initialized cudnnTensorDescriptor_t */
    /* get ZDESC: pass previously created cudnnTensorDescriptor_t */
    CUDNN_PARAM_ZDESC = 26,
    /* set/get ZDATA_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_ZDATA_PLACEHOLDER = 27,
    /* set BN_Z_EQSCALEBIAS_DESC: pass previously initialized cudnnTensorDescriptor_t */
    /* get BN_Z_EQSCALEBIAS_DESC: pass previously created cudnnTensorDescriptor_t */
    CUDNN_PARAM_BN_Z_EQSCALEBIAS_DESC = 28,
    /* set/get BN_Z_EQSCALE_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_BN_Z_EQSCALE_PLACEHOLDER = 29,
    /* set/get BN_Z_EQBIAS_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_BN_Z_EQBIAS_PLACEHOLDER = 30,

    /* set ACTIVATION_BITMASK_DESC: pass previously initialized cudnnTensorDescriptor_t */
    /* get ACTIVATION_BITMASK_DESC: pass previously created cudnnTensorDescriptor_t */
    CUDNN_PARAM_ACTIVATION_BITMASK_DESC = 31,
    /* set/get ACTIVATION_BITMASK_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_ACTIVATION_BITMASK_PLACEHOLDER = 32,

    /* set DXDESC: pass previously initialized cudnnTensorDescriptor_t */
    /* get DXDESC: pass previously created cudnnTensorDescriptor_t */
    CUDNN_PARAM_DXDESC = 33,
    /* set/get DXDATA_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_DXDATA_PLACEHOLDER = 34,
    /* set DZDESC: pass previously initialized cudnnTensorDescriptor_t */
    /* get DZDESC: pass previously created cudnnTensorDescriptor_t */
    CUDNN_PARAM_DZDESC = 35,
    /* set/get DZDATA_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_DZDATA_PLACEHOLDER = 36,
    /* set/get CUDNN_PARAM_BN_DSCALE_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_BN_DSCALE_PLACEHOLDER = 37,
    /* set/get CUDNN_PARAM_BN_DBIAS_PLACEHOLDER: pass cudnnFusedOpsPointerPlaceHolder_t* */
    CUDNN_PARAM_BN_DBIAS_PLACEHOLDER = 38,
} cudnnFusedOpsConstParamLabel_t;

typedef enum {
    CUDNN_PTR_NULL         = 0,
    CUDNN_PTR_ELEM_ALIGNED = 1,
    CUDNN_PTR_16B_ALIGNED  = 2,
} cudnnFusedOpsPointerPlaceHolder_t;

typedef enum {
    /* set: pass void* pointing to dev memory */
    /* get: pass void** pointing to host memory */
    CUDNN_PTR_XDATA              = 0,
    CUDNN_PTR_BN_EQSCALE         = 1,
    CUDNN_PTR_BN_EQBIAS          = 2,
    CUDNN_PTR_WDATA              = 3,
    CUDNN_PTR_DWDATA             = 4,
    CUDNN_PTR_YDATA              = 5,
    CUDNN_PTR_DYDATA             = 6,
    CUDNN_PTR_YSUM               = 7,
    CUDNN_PTR_YSQSUM             = 8,
    CUDNN_PTR_WORKSPACE          = 9,
    CUDNN_PTR_BN_SCALE           = 10,
    CUDNN_PTR_BN_BIAS            = 11,
    CUDNN_PTR_BN_SAVED_MEAN      = 12,
    CUDNN_PTR_BN_SAVED_INVSTD    = 13,
    CUDNN_PTR_BN_RUNNING_MEAN    = 14,
    CUDNN_PTR_BN_RUNNING_VAR     = 15,
    CUDNN_PTR_ZDATA              = 16,
    CUDNN_PTR_BN_Z_EQSCALE       = 17,
    CUDNN_PTR_BN_Z_EQBIAS        = 18,
    CUDNN_PTR_ACTIVATION_BITMASK = 19,
    CUDNN_PTR_DXDATA             = 20,
    CUDNN_PTR_DZDATA             = 21,
    CUDNN_PTR_BN_DSCALE          = 22,
    CUDNN_PTR_BN_DBIAS           = 23,

    /* set/get: pass size_t* pointing to host memory */
    CUDNN_SCALAR_SIZE_T_WORKSPACE_SIZE_IN_BYTES = 100,
    /* set/get: pass int64_t* pointing to host memory */
    CUDNN_SCALAR_INT64_T_BN_ACCUMULATION_COUNT = 101,
    /* set/get: pass double* pointing to host memory */
    CUDNN_SCALAR_DOUBLE_BN_EXP_AVG_FACTOR = 102,
    /* set/get: pass double* pointing to host memory */
    CUDNN_SCALAR_DOUBLE_BN_EPSILON = 103,
} cudnnFusedOpsVariantParamLabel_t;

cudnnStatus_t CUDNNWINAPI
cudnnCreateFusedOpsConstParamPack(cudnnFusedOpsConstParamPack_t *constPack, cudnnFusedOps_t ops);

cudnnStatus_t CUDNNWINAPI
cudnnDestroyFusedOpsConstParamPack(cudnnFusedOpsConstParamPack_t constPack);

cudnnStatus_t CUDNNWINAPI
cudnnSetFusedOpsConstParamPackAttribute(cudnnFusedOpsConstParamPack_t constPack,
                                        cudnnFusedOpsConstParamLabel_t paramLabel,
                                        const void *param);

cudnnStatus_t CUDNNWINAPI
cudnnGetFusedOpsConstParamPackAttribute(const cudnnFusedOpsConstParamPack_t constPack,
                                        cudnnFusedOpsConstParamLabel_t paramLabel,
                                        void *param,
                                        int *isNULL);

cudnnStatus_t CUDNNWINAPI
cudnnCreateFusedOpsVariantParamPack(cudnnFusedOpsVariantParamPack_t *varPack, cudnnFusedOps_t ops);

cudnnStatus_t CUDNNWINAPI
cudnnDestroyFusedOpsVariantParamPack(cudnnFusedOpsVariantParamPack_t varPack);

cudnnStatus_t CUDNNWINAPI
cudnnSetFusedOpsVariantParamPackAttribute(cudnnFusedOpsVariantParamPack_t varPack,
                                          cudnnFusedOpsVariantParamLabel_t paramLabel,
                                          void *ptr);

cudnnStatus_t CUDNNWINAPI
cudnnGetFusedOpsVariantParamPackAttribute(const cudnnFusedOpsVariantParamPack_t varPack,
                                          cudnnFusedOpsVariantParamLabel_t paramLabel,
                                          void *ptr);

cudnnStatus_t CUDNNWINAPI
cudnnCreateFusedOpsPlan(cudnnFusedOpsPlan_t *plan, cudnnFusedOps_t ops);

cudnnStatus_t CUDNNWINAPI
cudnnDestroyFusedOpsPlan(cudnnFusedOpsPlan_t plan);

cudnnStatus_t CUDNNWINAPI
cudnnMakeFusedOpsPlan(cudnnHandle_t handle,
                      cudnnFusedOpsPlan_t plan,
                      const cudnnFusedOpsConstParamPack_t constPack,
                      size_t *workspaceSizeInBytes);

cudnnStatus_t CUDNNWINAPI
cudnnFusedOpsExecute(cudnnHandle_t handle, const cudnnFusedOpsPlan_t plan, cudnnFusedOpsVariantParamPack_t varPack);

/* DEPRECATED routines to be removed next release :
   User should use the non-suffixed version (which has the API and functionality of _v6 version)
   Routines with _v5 suffix has the functionality of the non-suffixed routines in the CUDNN V6
 */

cudnnStatus_t CUDNNWINAPI
cudnnSetRNNDescriptor_v6(cudnnHandle_t handle,
                         cudnnRNNDescriptor_t rnnDesc,
                         const int hiddenSize,
                         const int numLayers,
                         cudnnDropoutDescriptor_t dropoutDesc,
                         cudnnRNNInputMode_t inputMode,
                         cudnnDirectionMode_t direction,
                         cudnnRNNMode_t mode,
                         cudnnRNNAlgo_t algo,
                         cudnnDataType_t mathPrec);

cudnnStatus_t CUDNNWINAPI
cudnnSetRNNDescriptor_v5(cudnnRNNDescriptor_t rnnDesc,
                         int hiddenSize,
                         int numLayers,
                         cudnnDropoutDescriptor_t dropoutDesc,
                         cudnnRNNInputMode_t inputMode,
                         cudnnDirectionMode_t direction,
                         cudnnRNNMode_t mode,
                         cudnnDataType_t mathPrec);

#if defined(__cplusplus)
}
#endif

#endif /* CUDNN_H_ */
