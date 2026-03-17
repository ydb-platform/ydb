#include "cuda_runtime.h"
#include "common.h"
#include "reduce.h"
#if CUDART_VERSION >= 11080
  #if __CUDA_ARCH__ < 900
    DEFINE_ncclDevKernel_nop(Reduce_Sum_f8e4m3_RING_LL, ncclFuncReduce, FuncSum, __nv_fp8_e4m3, NCCL_ALGO_RING, NCCL_PROTO_LL, 438)
  #else
    DEFINE_ncclDevKernel(Reduce_Sum_f8e4m3_RING_LL, ncclFuncReduce, FuncSum, __nv_fp8_e4m3, NCCL_ALGO_RING, NCCL_PROTO_LL, 438)
  #endif
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(Reduce_Sum_f8e4m3_RING_LL, ncclFuncReduce, FuncSum, __nv_fp8_e4m3, NCCL_ALGO_RING, NCCL_PROTO_LL)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(Reduce_Sum_f8e4m3_RING_LL128, ncclFuncReduce, FuncSum, __nv_fp8_e4m3, NCCL_ALGO_RING, NCCL_PROTO_LL128)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(Reduce_Sum_f8e4m3_RING_SIMPLE, ncclFuncReduce, FuncSum, __nv_fp8_e4m3, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
#endif
