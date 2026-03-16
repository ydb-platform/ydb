#include "cuda_runtime.h"
#include "common.h"
#include "all_reduce.h"
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_PreMulSum_f8e5m2_COLLNET_CHAIN_SIMPLE, ncclFuncAllReduce, FuncPreMulSum, __nv_fp8_e5m2, NCCL_ALGO_COLLNET_CHAIN, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_PreMulSum_f8e5m2_COLLNET_DIRECT_SIMPLE, ncclFuncAllReduce, FuncPreMulSum, __nv_fp8_e5m2, NCCL_ALGO_COLLNET_DIRECT, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_PreMulSum_f8e5m2_RING_LL, ncclFuncAllReduce, FuncPreMulSum, __nv_fp8_e5m2, NCCL_ALGO_RING, NCCL_PROTO_LL)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_PreMulSum_f8e5m2_RING_LL128, ncclFuncAllReduce, FuncPreMulSum, __nv_fp8_e5m2, NCCL_ALGO_RING, NCCL_PROTO_LL128)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_PreMulSum_f8e5m2_RING_SIMPLE, ncclFuncAllReduce, FuncPreMulSum, __nv_fp8_e5m2, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_PreMulSum_f8e5m2_TREE_LL, ncclFuncAllReduce, FuncPreMulSum, __nv_fp8_e5m2, NCCL_ALGO_TREE, NCCL_PROTO_LL)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_PreMulSum_f8e5m2_TREE_LL128, ncclFuncAllReduce, FuncPreMulSum, __nv_fp8_e5m2, NCCL_ALGO_TREE, NCCL_PROTO_LL128)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_PreMulSum_f8e5m2_TREE_SIMPLE, ncclFuncAllReduce, FuncPreMulSum, __nv_fp8_e5m2, NCCL_ALGO_TREE, NCCL_PROTO_SIMPLE)
#endif
