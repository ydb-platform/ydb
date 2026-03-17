#include "cuda_runtime.h"
#include "symmetric.h"
#include "symmetric/kernel.cuh"
#include "symmetric/all_reduce.cuh"
#if CUDART_VERSION >= 11080
  __global__ void ncclSymDevKernel_AllReduce_AGxLL_R_sum_f8e4m3(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
    #if __CUDA_ARCH__ >= 900
      ncclSymRun_AllReduce_AGxLL_R<FuncSum, __nv_fp8_e4m3>(&args);
    #endif
  }
#endif
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f8e4m3(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
    #if __CUDA_ARCH__ >= 900
      ncclSymRun_AllReduce_AGxLLMC_R<FuncSum, __nv_fp8_e4m3>(&args);
    #endif
  }
#endif
#if CUDART_VERSION >= 11080
  __global__ void ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_f8e4m3(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
    #if __CUDA_ARCH__ >= 900
      ncclSymRun_AllReduce_RSxLD_AGxST<FuncSum, __nv_fp8_e4m3>(&args);
    #endif
  }
#endif
