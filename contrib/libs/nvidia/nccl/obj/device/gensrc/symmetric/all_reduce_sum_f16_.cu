#include "cuda_runtime.h"
#include "symmetric.h"
#include "symmetric/kernel.cuh"
#include "symmetric/all_reduce.cuh"
__global__ void ncclSymDevKernel_AllReduce_AGxLL_R_sum_f16(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
  ncclSymRun_AllReduce_AGxLL_R<FuncSum, half>(&args);
}
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f16(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
    #if __CUDA_ARCH__ >= 900
      ncclSymRun_AllReduce_AGxLLMC_R<FuncSum, half>(&args);
    #endif
  }
#endif
__global__ void ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_f16(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
  ncclSymRun_AllReduce_RSxLD_AGxST<FuncSum, half>(&args);
}
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f16(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
    #if __CUDA_ARCH__ >= 900
      ncclSymRun_AllReduce_RSxLDMC_AGxSTMC<FuncSum, half>(&args);
    #endif
  }
#endif
