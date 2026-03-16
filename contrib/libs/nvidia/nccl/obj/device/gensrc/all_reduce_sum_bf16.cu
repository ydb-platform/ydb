#include "cuda_runtime.h"
#include "common.h"
#include "all_reduce.h"
#if CUDART_VERSION >= 11000
  #if __CUDA_ARCH__ < 0
    DEFINE_ncclDevKernel_nop(AllReduce_Sum_bf16_RING_LL, ncclFuncAllReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_RING, NCCL_PROTO_LL, 238)
  #else
    DEFINE_ncclDevKernel(AllReduce_Sum_bf16_RING_LL, ncclFuncAllReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_RING, NCCL_PROTO_LL, 238)
  #endif
#endif
#if CUDART_VERSION >= 11000
  #if __CUDA_ARCH__ < 0
    DEFINE_ncclDevKernel_nop(AllReduce_Sum_bf16_TREE_LL, ncclFuncAllReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_TREE, NCCL_PROTO_LL, 241)
  #else
    DEFINE_ncclDevKernel(AllReduce_Sum_bf16_TREE_LL, ncclFuncAllReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_TREE, NCCL_PROTO_LL, 241)
  #endif
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
DEFINE_ncclDevFunc(AllReduce_Sum_bf16_COLLNET_CHAIN_SIMPLE, ncclFuncAllReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_COLLNET_CHAIN, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
DEFINE_ncclDevFunc(AllReduce_Sum_bf16_COLLNET_DIRECT_SIMPLE, ncclFuncAllReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_COLLNET_DIRECT, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_Sum_bf16_NVLS_SIMPLE, ncclFuncAllReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_NVLS, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_Sum_bf16_NVLS_TREE_SIMPLE, ncclFuncAllReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_NVLS_TREE, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
DEFINE_ncclDevFunc(AllReduce_Sum_bf16_RING_LL, ncclFuncAllReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_RING, NCCL_PROTO_LL)
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
DEFINE_ncclDevFunc(AllReduce_Sum_bf16_RING_LL128, ncclFuncAllReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_RING, NCCL_PROTO_LL128)
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
DEFINE_ncclDevFunc(AllReduce_Sum_bf16_RING_SIMPLE, ncclFuncAllReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
DEFINE_ncclDevFunc(AllReduce_Sum_bf16_TREE_LL, ncclFuncAllReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_TREE, NCCL_PROTO_LL)
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
DEFINE_ncclDevFunc(AllReduce_Sum_bf16_TREE_LL128, ncclFuncAllReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_TREE, NCCL_PROTO_LL128)
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
DEFINE_ncclDevFunc(AllReduce_Sum_bf16_TREE_SIMPLE, ncclFuncAllReduce, FuncSum, __nv_bfloat16, NCCL_ALGO_TREE, NCCL_PROTO_SIMPLE)
#endif
