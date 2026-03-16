#include "cuda_runtime.h"
#include "common.h"
#include "all_reduce.h"
DEFINE_ncclDevFunc(AllReduce_Prod_u32_COLLNET_CHAIN_SIMPLE, ncclFuncAllReduce, FuncProd, uint32_t, NCCL_ALGO_COLLNET_CHAIN, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(AllReduce_Prod_u32_COLLNET_DIRECT_SIMPLE, ncclFuncAllReduce, FuncProd, uint32_t, NCCL_ALGO_COLLNET_DIRECT, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(AllReduce_Prod_u32_RING_LL, ncclFuncAllReduce, FuncProd, uint32_t, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(AllReduce_Prod_u32_RING_LL128, ncclFuncAllReduce, FuncProd, uint32_t, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(AllReduce_Prod_u32_RING_SIMPLE, ncclFuncAllReduce, FuncProd, uint32_t, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(AllReduce_Prod_u32_TREE_LL, ncclFuncAllReduce, FuncProd, uint32_t, NCCL_ALGO_TREE, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(AllReduce_Prod_u32_TREE_LL128, ncclFuncAllReduce, FuncProd, uint32_t, NCCL_ALGO_TREE, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(AllReduce_Prod_u32_TREE_SIMPLE, ncclFuncAllReduce, FuncProd, uint32_t, NCCL_ALGO_TREE, NCCL_PROTO_SIMPLE)
