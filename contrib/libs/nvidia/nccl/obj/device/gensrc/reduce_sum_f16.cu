#include "cuda_runtime.h"
#include "common.h"
#include "reduce.h"
DEFINE_ncclDevKernel(Reduce_Sum_f16_RING_LL, ncclFuncReduce, FuncSum, half, NCCL_ALGO_RING, NCCL_PROTO_LL, 429)
DEFINE_ncclDevFunc(Reduce_Sum_f16_RING_LL, ncclFuncReduce, FuncSum, half, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(Reduce_Sum_f16_RING_LL128, ncclFuncReduce, FuncSum, half, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(Reduce_Sum_f16_RING_SIMPLE, ncclFuncReduce, FuncSum, half, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
