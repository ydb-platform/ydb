#include "cuda_runtime.h"
#include "common.h"
#include "reduce.h"
#if CUDART_VERSION >= 11000
  #if __CUDA_ARCH__ < 0
    DEFINE_ncclDevKernel_nop(Reduce_Sum_bf16_RING_LL, ncclFuncReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_RING, NCCL_PROTO_LL, 426)
  #else
    DEFINE_ncclDevKernel(Reduce_Sum_bf16_RING_LL, ncclFuncReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_RING, NCCL_PROTO_LL, 426)
  #endif
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
DEFINE_ncclDevFunc(Reduce_Sum_bf16_RING_LL, ncclFuncReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_RING, NCCL_PROTO_LL)
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
DEFINE_ncclDevFunc(Reduce_Sum_bf16_RING_LL128, ncclFuncReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_RING, NCCL_PROTO_LL128)
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
DEFINE_ncclDevFunc(Reduce_Sum_bf16_RING_SIMPLE, ncclFuncReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
#endif
