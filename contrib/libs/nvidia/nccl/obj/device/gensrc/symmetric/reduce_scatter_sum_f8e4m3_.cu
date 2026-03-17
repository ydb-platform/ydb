#include "cuda_runtime.h"
#include "symmetric.h"
#include "symmetric/kernel.cuh"
#include "symmetric/reduce_scatter.cuh"
#if CUDART_VERSION >= 11080
  __global__ void ncclSymDevKernel_ReduceScatter_LL_sum_f8e4m3(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
    #if __CUDA_ARCH__ >= 900
      ncclSymRun_ReduceScatter_LL<FuncSum, __nv_fp8_e4m3>(&args);
    #endif
  }
#endif
#if CUDART_VERSION >= 11080
  __global__ void ncclSymDevKernel_ReduceScatter_LD_sum_f8e4m3(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
    #if __CUDA_ARCH__ >= 900
      ncclSymRun_ReduceScatter_LD<FuncSum, __nv_fp8_e4m3>(&args);
    #endif
  }
#endif
