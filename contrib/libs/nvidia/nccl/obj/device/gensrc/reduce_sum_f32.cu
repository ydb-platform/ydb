#include "cuda_runtime.h"
#include "common.h"
#include "reduce.h"
DEFINE_ncclDevKernel(Reduce_Sum_f32_RING_LL, ncclFuncReduce, FuncSum, float, NCCL_ALGO_RING, NCCL_PROTO_LL, 432)
DEFINE_ncclDevFunc(Reduce_Sum_f32_RING_LL, ncclFuncReduce, FuncSum, float, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(Reduce_Sum_f32_RING_LL128, ncclFuncReduce, FuncSum, float, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(Reduce_Sum_f32_RING_SIMPLE, ncclFuncReduce, FuncSum, float, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
