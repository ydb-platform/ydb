#include "cuda_runtime.h"
#include "common.h"
#include "reduce_scatter.h"
DEFINE_ncclDevFunc(ReduceScatter_SumPostDiv_u32_COLLNET_DIRECT_SIMPLE, ncclFuncReduceScatter, FuncSumPostDiv, uint32_t, NCCL_ALGO_COLLNET_DIRECT, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(ReduceScatter_SumPostDiv_u32_PAT_SIMPLE, ncclFuncReduceScatter, FuncSumPostDiv, uint32_t, NCCL_ALGO_PAT, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(ReduceScatter_SumPostDiv_u32_RING_LL, ncclFuncReduceScatter, FuncSumPostDiv, uint32_t, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(ReduceScatter_SumPostDiv_u32_RING_LL128, ncclFuncReduceScatter, FuncSumPostDiv, uint32_t, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(ReduceScatter_SumPostDiv_u32_RING_SIMPLE, ncclFuncReduceScatter, FuncSumPostDiv, uint32_t, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
