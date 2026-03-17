#include "cuda_runtime.h"
#include "common.h"
#include "all_reduce.h"
DEFINE_ncclDevKernel(AllReduce_Sum_u8_RING_LL, ncclFuncAllReduce, FuncSum, uint8_t, NCCL_ALGO_RING, NCCL_PROTO_LL, 312)
DEFINE_ncclDevKernel(AllReduce_Sum_u8_TREE_LL, ncclFuncAllReduce, FuncSum, uint8_t, NCCL_ALGO_TREE, NCCL_PROTO_LL, 315)
DEFINE_ncclDevFunc(AllReduce_Sum_u8_COLLNET_CHAIN_SIMPLE, ncclFuncAllReduce, FuncSum, uint8_t, NCCL_ALGO_COLLNET_CHAIN, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(AllReduce_Sum_u8_COLLNET_DIRECT_SIMPLE, ncclFuncAllReduce, FuncSum, uint8_t, NCCL_ALGO_COLLNET_DIRECT, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(AllReduce_Sum_u8_RING_LL, ncclFuncAllReduce, FuncSum, uint8_t, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(AllReduce_Sum_u8_RING_LL128, ncclFuncAllReduce, FuncSum, uint8_t, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(AllReduce_Sum_u8_RING_SIMPLE, ncclFuncAllReduce, FuncSum, uint8_t, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(AllReduce_Sum_u8_TREE_LL, ncclFuncAllReduce, FuncSum, uint8_t, NCCL_ALGO_TREE, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(AllReduce_Sum_u8_TREE_LL128, ncclFuncAllReduce, FuncSum, uint8_t, NCCL_ALGO_TREE, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(AllReduce_Sum_u8_TREE_SIMPLE, ncclFuncAllReduce, FuncSum, uint8_t, NCCL_ALGO_TREE, NCCL_PROTO_SIMPLE)
