#include "cuda_runtime.h"
#include "common.h"
#include "reduce_scatter.h"
DEFINE_ncclDevKernel(ReduceScatter_Sum_u8_RING_LL, ncclFuncReduceScatter, FuncSum, uint8_t, NCCL_ALGO_RING, NCCL_PROTO_LL, 651)
DEFINE_ncclDevFunc(ReduceScatter_Sum_u8_COLLNET_DIRECT_SIMPLE, ncclFuncReduceScatter, FuncSum, uint8_t, NCCL_ALGO_COLLNET_DIRECT, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(ReduceScatter_Sum_u8_PAT_SIMPLE, ncclFuncReduceScatter, FuncSum, uint8_t, NCCL_ALGO_PAT, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(ReduceScatter_Sum_u8_RING_LL, ncclFuncReduceScatter, FuncSum, uint8_t, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(ReduceScatter_Sum_u8_RING_LL128, ncclFuncReduceScatter, FuncSum, uint8_t, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(ReduceScatter_Sum_u8_RING_SIMPLE, ncclFuncReduceScatter, FuncSum, uint8_t, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
