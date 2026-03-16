#include "cuda_runtime.h"
#include "common.h"
#include "all_reduce.h"
DEFINE_ncclDevKernel(AllReduce_Sum_u32_RING_LL, ncclFuncAllReduce, FuncSum, uint32_t, NCCL_ALGO_RING, NCCL_PROTO_LL, 294)
DEFINE_ncclDevKernel(AllReduce_Sum_u32_TREE_LL, ncclFuncAllReduce, FuncSum, uint32_t, NCCL_ALGO_TREE, NCCL_PROTO_LL, 297)
DEFINE_ncclDevFunc(AllReduce_Sum_u32_COLLNET_CHAIN_SIMPLE, ncclFuncAllReduce, FuncSum, uint32_t, NCCL_ALGO_COLLNET_CHAIN, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(AllReduce_Sum_u32_COLLNET_DIRECT_SIMPLE, ncclFuncAllReduce, FuncSum, uint32_t, NCCL_ALGO_COLLNET_DIRECT, NCCL_PROTO_SIMPLE)
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_Sum_u32_NVLS_SIMPLE, ncclFuncAllReduce, FuncSum, uint32_t, NCCL_ALGO_NVLS, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_Sum_u32_NVLS_TREE_SIMPLE, ncclFuncAllReduce, FuncSum, uint32_t, NCCL_ALGO_NVLS_TREE, NCCL_PROTO_SIMPLE)
#endif
DEFINE_ncclDevFunc(AllReduce_Sum_u32_RING_LL, ncclFuncAllReduce, FuncSum, uint32_t, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(AllReduce_Sum_u32_RING_LL128, ncclFuncAllReduce, FuncSum, uint32_t, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(AllReduce_Sum_u32_RING_SIMPLE, ncclFuncAllReduce, FuncSum, uint32_t, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(AllReduce_Sum_u32_TREE_LL, ncclFuncAllReduce, FuncSum, uint32_t, NCCL_ALGO_TREE, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(AllReduce_Sum_u32_TREE_LL128, ncclFuncAllReduce, FuncSum, uint32_t, NCCL_ALGO_TREE, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(AllReduce_Sum_u32_TREE_SIMPLE, ncclFuncAllReduce, FuncSum, uint32_t, NCCL_ALGO_TREE, NCCL_PROTO_SIMPLE)
