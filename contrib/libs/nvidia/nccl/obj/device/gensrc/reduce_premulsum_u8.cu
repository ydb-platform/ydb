#include "cuda_runtime.h"
#include "common.h"
#include "reduce.h"
DEFINE_ncclDevFunc(Reduce_PreMulSum_u8_RING_LL, ncclFuncReduce, FuncPreMulSum, uint8_t, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(Reduce_PreMulSum_u8_RING_LL128, ncclFuncReduce, FuncPreMulSum, uint8_t, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(Reduce_PreMulSum_u8_RING_SIMPLE, ncclFuncReduce, FuncPreMulSum, uint8_t, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
