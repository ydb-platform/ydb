#include "cuda_runtime.h"
#include "symmetric.h"
#include "symmetric/kernel.cuh"
#include "symmetric/reduce_scatter.cuh"
__global__ void ncclSymDevKernel_ReduceScatter_LL_sum_f16(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
  ncclSymRun_ReduceScatter_LL<FuncSum, half>(&args);
}
__global__ void ncclSymDevKernel_ReduceScatter_LD_sum_f16(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
  ncclSymRun_ReduceScatter_LD<FuncSum, half>(&args);
}
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_ReduceScatter_LDMC_sum_f16(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
    #if __CUDA_ARCH__ >= 900
      ncclSymRun_ReduceScatter_LDMC<FuncSum, half>(&args);
    #endif
  }
#endif
