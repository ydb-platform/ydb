#include "cuda_runtime.h"
#include "common.h"
#include "broadcast.h"
DEFINE_ncclDevKernel(Broadcast_RING_LL, ncclFuncBroadcast, FuncCopy, int8_t, NCCL_ALGO_RING, NCCL_PROTO_LL, 342)
DEFINE_ncclDevFunc(Broadcast_RING_LL, ncclFuncBroadcast, FuncCopy, int8_t, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(Broadcast_RING_LL128, ncclFuncBroadcast, FuncCopy, int8_t, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(Broadcast_RING_SIMPLE, ncclFuncBroadcast, FuncCopy, int8_t, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
