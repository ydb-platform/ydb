#include "cuda_runtime.h"
#include "symmetric.h"
#include "symmetric/kernel.cuh"
#include "symmetric/all_reduce.cuh"
#if CUDART_VERSION >= 12070
  __global__ void ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f8e5m2(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
    #if 0 || NCCL_CUDA_ARCH_SPECIFIC==1000 || NCCL_CUDA_ARCH_SPECIFIC==1010 || NCCL_CUDA_ARCH_FAMILY_SPECIFIC==1000 || NCCL_CUDA_ARCH_FAMILY_SPECIFIC==1010 || NCCL_CUDA_ARCH_SPECIFIC==1200 || NCCL_CUDA_ARCH_SPECIFIC==1210
      ncclSymRun_AllReduce_RSxLDMC_AGxSTMC<FuncSum, __nv_fp8_e5m2>(&args);
    #endif
  }
#endif
