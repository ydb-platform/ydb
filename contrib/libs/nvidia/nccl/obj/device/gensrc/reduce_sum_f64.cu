#include "cuda_runtime.h"
#include "common.h"
#include "reduce.h"
DEFINE_ncclDevKernel(Reduce_Sum_f64_RING_LL, ncclFuncReduce, FuncSum, double, NCCL_ALGO_RING, NCCL_PROTO_LL, 435)
DEFINE_ncclDevFunc(Reduce_Sum_f64_RING_LL, ncclFuncReduce, FuncSum, double, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(Reduce_Sum_f64_RING_LL128, ncclFuncReduce, FuncSum, double, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(Reduce_Sum_f64_RING_SIMPLE, ncclFuncReduce, FuncSum, double, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
