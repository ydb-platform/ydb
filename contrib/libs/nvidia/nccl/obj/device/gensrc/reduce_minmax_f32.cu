#include "cuda_runtime.h"
#include "common.h"
#include "reduce.h"
DEFINE_ncclDevFunc(Reduce_MinMax_f32_RING_LL, ncclFuncReduce, FuncMinMax, float, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(Reduce_MinMax_f32_RING_LL128, ncclFuncReduce, FuncMinMax, float, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(Reduce_MinMax_f32_RING_SIMPLE, ncclFuncReduce, FuncMinMax, float, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
