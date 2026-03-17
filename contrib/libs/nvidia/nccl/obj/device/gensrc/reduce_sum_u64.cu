#include "cuda_runtime.h"
#include "common.h"
#include "reduce.h"
DEFINE_ncclDevKernel(Reduce_Sum_u64_RING_LL, ncclFuncReduce, FuncSum, uint64_t, NCCL_ALGO_RING, NCCL_PROTO_LL, 447)
DEFINE_ncclDevFunc(Reduce_Sum_u64_RING_LL, ncclFuncReduce, FuncSum, uint64_t, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(Reduce_Sum_u64_RING_LL128, ncclFuncReduce, FuncSum, uint64_t, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(Reduce_Sum_u64_RING_SIMPLE, ncclFuncReduce, FuncSum, uint64_t, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
