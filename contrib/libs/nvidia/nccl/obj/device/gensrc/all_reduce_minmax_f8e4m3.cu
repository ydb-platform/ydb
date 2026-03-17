#include "cuda_runtime.h"
#include "common.h"
#include "all_reduce.h"
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_MinMax_f8e4m3_COLLNET_CHAIN_SIMPLE, ncclFuncAllReduce, FuncMinMax, __nv_fp8_e4m3, NCCL_ALGO_COLLNET_CHAIN, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_MinMax_f8e4m3_COLLNET_DIRECT_SIMPLE, ncclFuncAllReduce, FuncMinMax, __nv_fp8_e4m3, NCCL_ALGO_COLLNET_DIRECT, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_MinMax_f8e4m3_RING_LL, ncclFuncAllReduce, FuncMinMax, __nv_fp8_e4m3, NCCL_ALGO_RING, NCCL_PROTO_LL)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_MinMax_f8e4m3_RING_LL128, ncclFuncAllReduce, FuncMinMax, __nv_fp8_e4m3, NCCL_ALGO_RING, NCCL_PROTO_LL128)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_MinMax_f8e4m3_RING_SIMPLE, ncclFuncAllReduce, FuncMinMax, __nv_fp8_e4m3, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_MinMax_f8e4m3_TREE_LL, ncclFuncAllReduce, FuncMinMax, __nv_fp8_e4m3, NCCL_ALGO_TREE, NCCL_PROTO_LL)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_MinMax_f8e4m3_TREE_LL128, ncclFuncAllReduce, FuncMinMax, __nv_fp8_e4m3, NCCL_ALGO_TREE, NCCL_PROTO_LL128)
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_MinMax_f8e4m3_TREE_SIMPLE, ncclFuncAllReduce, FuncMinMax, __nv_fp8_e4m3, NCCL_ALGO_TREE, NCCL_PROTO_SIMPLE)
#endif
