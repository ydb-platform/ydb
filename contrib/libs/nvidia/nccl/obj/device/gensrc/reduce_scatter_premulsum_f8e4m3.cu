#include "cuda_runtime.h"
#include "common.h"
#include "reduce_scatter.h"
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(ReduceScatter_PreMulSum_f8e4m3_COLLNET_DIRECT_SIMPLE, ncclFuncReduceScatter, FuncPreMulSum, __nv_fp8_e4m3, NCCL_ALGO_COLLNET_DIRECT, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(ReduceScatter_PreMulSum_f8e4m3_PAT_SIMPLE, ncclFuncReduceScatter, FuncPreMulSum, __nv_fp8_e4m3, NCCL_ALGO_PAT, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(ReduceScatter_PreMulSum_f8e4m3_RING_LL, ncclFuncReduceScatter, FuncPreMulSum, __nv_fp8_e4m3, NCCL_ALGO_RING, NCCL_PROTO_LL)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(ReduceScatter_PreMulSum_f8e4m3_RING_LL128, ncclFuncReduceScatter, FuncPreMulSum, __nv_fp8_e4m3, NCCL_ALGO_RING, NCCL_PROTO_LL128)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(ReduceScatter_PreMulSum_f8e4m3_RING_SIMPLE, ncclFuncReduceScatter, FuncPreMulSum, __nv_fp8_e4m3, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
#endif
