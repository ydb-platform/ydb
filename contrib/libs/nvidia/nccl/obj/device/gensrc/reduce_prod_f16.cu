#include "cuda_runtime.h"
#include "common.h"
#include "reduce.h"
DEFINE_ncclDevFunc(Reduce_Prod_f16_RING_LL, ncclFuncReduce, FuncProd, half, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(Reduce_Prod_f16_RING_LL128, ncclFuncReduce, FuncProd, half, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(Reduce_Prod_f16_RING_SIMPLE, ncclFuncReduce, FuncProd, half, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
