#include "cuda_runtime.h"
#include "common.h"
#include "reduce_scatter.h"
DEFINE_ncclDevFunc(ReduceScatter_Prod_f64_COLLNET_DIRECT_SIMPLE, ncclFuncReduceScatter, FuncProd, double, NCCL_ALGO_COLLNET_DIRECT, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(ReduceScatter_Prod_f64_PAT_SIMPLE, ncclFuncReduceScatter, FuncProd, double, NCCL_ALGO_PAT, NCCL_PROTO_SIMPLE)
DEFINE_ncclDevFunc(ReduceScatter_Prod_f64_RING_LL, ncclFuncReduceScatter, FuncProd, double, NCCL_ALGO_RING, NCCL_PROTO_LL)
DEFINE_ncclDevFunc(ReduceScatter_Prod_f64_RING_LL128, ncclFuncReduceScatter, FuncProd, double, NCCL_ALGO_RING, NCCL_PROTO_LL128)
DEFINE_ncclDevFunc(ReduceScatter_Prod_f64_RING_SIMPLE, ncclFuncReduceScatter, FuncProd, double, NCCL_ALGO_RING, NCCL_PROTO_SIMPLE)
