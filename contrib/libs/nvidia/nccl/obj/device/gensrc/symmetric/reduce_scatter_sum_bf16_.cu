#include "cuda_runtime.h"
#include "symmetric.h"
#include "symmetric/kernel.cuh"
#include "symmetric/reduce_scatter.cuh"
#if CUDART_VERSION >= 11000
  __global__ void ncclSymDevKernel_ReduceScatter_LL_sum_bf16(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
    #if __CUDA_ARCH__ >= 0
      ncclSymRun_ReduceScatter_LL<FuncSum, __nv_bfloat16>(&args);
    #endif
  }
#endif
#if CUDART_VERSION >= 11000
  __global__ void ncclSymDevKernel_ReduceScatter_LD_sum_bf16(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
    #if __CUDA_ARCH__ >= 0
      ncclSymRun_ReduceScatter_LD<FuncSum, __nv_bfloat16>(&args);
    #endif
  }
#endif
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_ReduceScatter_LDMC_sum_bf16(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
    #if __CUDA_ARCH__ >= 900
      ncclSymRun_ReduceScatter_LDMC<FuncSum, __nv_bfloat16>(&args);
    #endif
  }
#endif
