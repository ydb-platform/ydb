#include "cuda_runtime.h"
#include "symmetric.h"
#include "symmetric/kernel.cuh"
#include "symmetric/all_gather.cuh"
__global__ void ncclSymDevKernel_AllGather_LL(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
  ncclSymRun_AllGather_LL(&args);
}
__global__ void ncclSymDevKernel_AllGather_LLMC(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
  ncclSymRun_AllGather_LLMC(&args);
}
__global__ void ncclSymDevKernel_AllGather_ST(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
  ncclSymRun_AllGather_ST(&args);
}
__global__ void ncclSymDevKernel_AllGather_STMC(ncclSymDevArgs NCCL_GRID_CONSTANT const args) {
  ncclSymRun_AllGather_STMC(&args);
}
