#include "cuda_runtime.h"
#include "common.h"
#include "reduce.h"
DEFINE_ncclDevFunc(Reduce_Prod_u32_RING_LL, ncclFuncReduce, FuncProd, uint32_t, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(Reduce_Prod_u32_RING_LL128, ncclFuncReduce, FuncProd, uint32_t, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(Reduce_Prod_u32_RING_SIMPLE, ncclFuncReduce, FuncProd, uint32_t, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
