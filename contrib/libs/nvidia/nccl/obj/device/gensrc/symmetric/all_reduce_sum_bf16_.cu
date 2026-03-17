#include "cuda_runtime.h"
#include "symmetric.h"
#include "symmetric/kernel.cuh"
#include "symmetric/all_reduce.cuh"
#if CUDART_VERSION >= 11000
  __global__ void ncclSymDevKernel_AllReduce_AGxLL_R_sum_bf16(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
    #if __CUDA_ARCH__ >= 0
      ncclSymRun_AllReduce_AGxLL_R<FuncSum, __nv_bfloat16>(&args);
    #endif
  }
#endif
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_bf16(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
    #if __CUDA_ARCH__ >= 900
      ncclSymRun_AllReduce_AGxLLMC_R<FuncSum, __nv_bfloat16>(&args);
    #endif
  }
#endif
#if CUDART_VERSION >= 11000
  __global__ void ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_bf16(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
    #if __CUDA_ARCH__ >= 0
      ncclSymRun_AllReduce_RSxLD_AGxST<FuncSum, __nv_bfloat16>(&args);
    #endif
  }
#endif
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_bf16(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
    #if __CUDA_ARCH__ >= 900
      ncclSymRun_AllReduce_RSxLDMC_AGxSTMC<FuncSum, __nv_bfloat16>(&args);
    #endif
  }
#endif
