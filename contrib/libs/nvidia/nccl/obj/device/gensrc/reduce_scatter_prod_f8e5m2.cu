#include "cuda_runtime.h"
#include "common.h"
#include "reduce_scatter.h"
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(ReduceScatter_Prod_f8e5m2_COLLNET_DIRECT_SIMPLE, ncclFuncReduceScatter, FuncProd, __nv_fp8_e5m2, NCCL_ALGO_COLLNET_DIRECT, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(ReduceScatter_Prod_f8e5m2_PAT_SIMPLE, ncclFuncReduceScatter, FuncProd, __nv_fp8_e5m2, NCCL_ALGO_PAT, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(ReduceScatter_Prod_f8e5m2_RING_LL, ncclFuncReduceScatter, FuncProd, __nv_fp8_e5m2, NCCL_ALGO_RING, NCCL_PROTO_LL)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(ReduceScatter_Prod_f8e5m2_RING_LL128, ncclFuncReduceScatter, FuncProd, __nv_fp8_e5m2, NCCL_ALGO_RING, NCCL_PROTO_LL128)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(ReduceScatter_Prod_f8e5m2_RING_SIMPLE, ncclFuncReduceScatter, FuncProd, __nv_fp8_e5m2, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
#endif
