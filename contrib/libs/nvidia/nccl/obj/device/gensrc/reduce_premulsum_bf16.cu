#include "cuda_runtime.h"
#include "common.h"
#include "reduce.h"
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
DEFINE_ncclDevFunc(Reduce_PreMulSum_bf16_RING_LL, ncclFuncReduce, FuncPreMulSum, __nv_bfloat16, NCCL_ALGO_RING, NCCL_PROTO_LL)
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
DEFINE_ncclDevFunc(Reduce_PreMulSum_bf16_RING_LL128, ncclFuncReduce, FuncPreMulSum, __nv_bfloat16, NCCL_ALGO_RING, NCCL_PROTO_LL128)
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
DEFINE_ncclDevFunc(Reduce_PreMulSum_bf16_RING_SIMPLE, ncclFuncReduce, FuncPreMulSum, __nv_bfloat16, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
#endif
