#include "cuda_runtime.h"
#include "common.h"
#include "all_reduce.h"
DEFINE_ncclDevKernel(AllReduce_Sum_f16_RING_LL, ncclFuncAllReduce, FuncSum, half, NCCL_ALGO_RING, NCCL_PROTO_LL, 248)
DEFINE_ncclDevKernel(AllReduce_Sum_f16_TREE_LL, ncclFuncAllReduce, FuncSum, half, NCCL_ALGO_TREE, NCCL_PROTO_LL, 251)
DEFINE_ncclDevFunc(AllReduce_Sum_f16_COLLNET_CHAIN_SIMPLE, ncclFuncAllReduce, FuncSum, half, NCCL_ALGO_COLLNET_CHAIN, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(AllReduce_Sum_f16_COLLNET_DIRECT_SIMPLE, ncclFuncAllReduce, FuncSum, half, NCCL_ALGO_COLLNET_DIRECT, NCCL_PROTO_SIMPLE)
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_Sum_f16_NVLS_SIMPLE, ncclFuncAllReduce, FuncSum, half, NCCL_ALGO_NVLS, NCCL_PROTO_SIMPLE)
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllReduce_Sum_f16_NVLS_TREE_SIMPLE, ncclFuncAllReduce, FuncSum, half, NCCL_ALGO_NVLS_TREE, NCCL_PROTO_SIMPLE)
#endif
DEFINE_ncclDevFunc(AllReduce_Sum_f16_RING_LL, ncclFuncAllReduce, FuncSum, half, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(AllReduce_Sum_f16_RING_LL128, ncclFuncAllReduce, FuncSum, half, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(AllReduce_Sum_f16_RING_SIMPLE, ncclFuncAllReduce, FuncSum, half, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(AllReduce_Sum_f16_TREE_LL, ncclFuncAllReduce, FuncSum, half, NCCL_ALGO_TREE, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(AllReduce_Sum_f16_TREE_LL128, ncclFuncAllReduce, FuncSum, half, NCCL_ALGO_TREE, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(AllReduce_Sum_f16_TREE_SIMPLE, ncclFuncAllReduce, FuncSum, half, NCCL_ALGO_TREE, NCCL_PROTO_SIMPLE)
