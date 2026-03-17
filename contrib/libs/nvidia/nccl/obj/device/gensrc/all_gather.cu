#include "cuda_runtime.h"
#include "common.h"
#include "all_gather.h"
DEFINE_ncclDevKernel(AllGather_RING_LL, ncclFuncAllGather, FuncCopy, int8_t, NCCL_ALGO_RING, NCCL_PROTO_LL, 3)
DEFINE_ncclDevFunc(AllGather_COLLNET_DIRECT_SIMPLE, ncclFuncAllGather, FuncCopy, int8_t, NCCL_ALGO_COLLNET_DIRECT, NCCL_PROTO_SIMPLE)
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
DEFINE_ncclDevFunc(AllGather_NVLS_SIMPLE, ncclFuncAllGather, FuncCopy, int8_t, NCCL_ALGO_NVLS, NCCL_PROTO_SIMPLE)
#endif
DEFINE_ncclDevFunc(AllGather_PAT_SIMPLE, ncclFuncAllGather, FuncCopy, int8_t, NCCL_ALGO_PAT, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(AllGather_RING_LL, ncclFuncAllGather, FuncCopy, int8_t, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(AllGather_RING_LL128, ncclFuncAllGather, FuncCopy, int8_t, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(AllGather_RING_SIMPLE, ncclFuncAllGather, FuncCopy, int8_t, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
