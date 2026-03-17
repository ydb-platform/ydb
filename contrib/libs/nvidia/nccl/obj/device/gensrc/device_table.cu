#include "cuda_runtime.h"
#include "common.h"

__device__ void ncclDevFunc_AllGather_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllGather_NVLS_SIMPLE();
#endif
__device__ void ncclDevFunc_AllGather_PAT_SIMPLE();
__device__ void ncclDevFunc_AllGather_RING_LL();
__device__ void ncclDevFunc_AllGather_RING_LL128();
__device__ void ncclDevFunc_AllGather_RING_SIMPLE();
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_MinMax_bf16_COLLNET_CHAIN_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_MinMax_bf16_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_bf16_NVLS_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_bf16_NVLS_TREE_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_MinMax_bf16_RING_LL();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_MinMax_bf16_RING_LL128();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_MinMax_bf16_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_MinMax_bf16_TREE_LL();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_MinMax_bf16_TREE_LL128();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_MinMax_bf16_TREE_SIMPLE();
#endif
__device__ void ncclDevFunc_AllReduce_MinMax_f16_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_f16_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f16_NVLS_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f16_NVLS_TREE_SIMPLE();
#endif
__device__ void ncclDevFunc_AllReduce_MinMax_f16_RING_LL();
__device__ void ncclDevFunc_AllReduce_MinMax_f16_RING_LL128();
__device__ void ncclDevFunc_AllReduce_MinMax_f16_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_f16_TREE_LL();
__device__ void ncclDevFunc_AllReduce_MinMax_f16_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_MinMax_f16_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_f32_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_f32_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_f32_RING_LL();
__device__ void ncclDevFunc_AllReduce_MinMax_f32_RING_LL128();
__device__ void ncclDevFunc_AllReduce_MinMax_f32_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_f32_TREE_LL();
__device__ void ncclDevFunc_AllReduce_MinMax_f32_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_MinMax_f32_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_f64_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_f64_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_f64_RING_LL();
__device__ void ncclDevFunc_AllReduce_MinMax_f64_RING_LL128();
__device__ void ncclDevFunc_AllReduce_MinMax_f64_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_f64_TREE_LL();
__device__ void ncclDevFunc_AllReduce_MinMax_f64_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_MinMax_f64_TREE_SIMPLE();
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f8e4m3_COLLNET_CHAIN_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f8e4m3_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f8e4m3_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f8e4m3_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f8e4m3_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f8e4m3_TREE_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f8e4m3_TREE_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f8e4m3_TREE_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f8e5m2_COLLNET_CHAIN_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f8e5m2_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f8e5m2_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f8e5m2_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f8e5m2_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f8e5m2_TREE_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f8e5m2_TREE_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_f8e5m2_TREE_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_i32_NVLS_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_i32_NVLS_TREE_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_i64_NVLS_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_i64_NVLS_TREE_SIMPLE();
#endif
__device__ void ncclDevFunc_AllReduce_MinMax_u32_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_u32_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_u32_NVLS_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_u32_NVLS_TREE_SIMPLE();
#endif
__device__ void ncclDevFunc_AllReduce_MinMax_u32_RING_LL();
__device__ void ncclDevFunc_AllReduce_MinMax_u32_RING_LL128();
__device__ void ncclDevFunc_AllReduce_MinMax_u32_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_u32_TREE_LL();
__device__ void ncclDevFunc_AllReduce_MinMax_u32_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_MinMax_u32_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_u64_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_u64_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_u64_NVLS_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_MinMax_u64_NVLS_TREE_SIMPLE();
#endif
__device__ void ncclDevFunc_AllReduce_MinMax_u64_RING_LL();
__device__ void ncclDevFunc_AllReduce_MinMax_u64_RING_LL128();
__device__ void ncclDevFunc_AllReduce_MinMax_u64_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_u64_TREE_LL();
__device__ void ncclDevFunc_AllReduce_MinMax_u64_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_MinMax_u64_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_u8_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_u8_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_u8_RING_LL();
__device__ void ncclDevFunc_AllReduce_MinMax_u8_RING_LL128();
__device__ void ncclDevFunc_AllReduce_MinMax_u8_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_MinMax_u8_TREE_LL();
__device__ void ncclDevFunc_AllReduce_MinMax_u8_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_MinMax_u8_TREE_SIMPLE();
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_PreMulSum_bf16_COLLNET_CHAIN_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_PreMulSum_bf16_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_PreMulSum_bf16_RING_LL();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_PreMulSum_bf16_RING_LL128();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_PreMulSum_bf16_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_PreMulSum_bf16_TREE_LL();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_PreMulSum_bf16_TREE_LL128();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_PreMulSum_bf16_TREE_SIMPLE();
#endif
__device__ void ncclDevFunc_AllReduce_PreMulSum_f16_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f16_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f16_RING_LL();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f16_RING_LL128();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f16_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f16_TREE_LL();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f16_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f16_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f32_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f32_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f32_RING_LL();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f32_RING_LL128();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f32_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f32_TREE_LL();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f32_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f32_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f64_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f64_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f64_RING_LL();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f64_RING_LL128();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f64_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f64_TREE_LL();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f64_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_PreMulSum_f64_TREE_SIMPLE();
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_PreMulSum_f8e4m3_COLLNET_CHAIN_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_PreMulSum_f8e4m3_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_PreMulSum_f8e4m3_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_PreMulSum_f8e4m3_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_PreMulSum_f8e4m3_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_PreMulSum_f8e4m3_TREE_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_PreMulSum_f8e4m3_TREE_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_PreMulSum_f8e4m3_TREE_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_PreMulSum_f8e5m2_COLLNET_CHAIN_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_PreMulSum_f8e5m2_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_PreMulSum_f8e5m2_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_PreMulSum_f8e5m2_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_PreMulSum_f8e5m2_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_PreMulSum_f8e5m2_TREE_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_PreMulSum_f8e5m2_TREE_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_PreMulSum_f8e5m2_TREE_SIMPLE();
#endif
__device__ void ncclDevFunc_AllReduce_PreMulSum_u32_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u32_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u32_RING_LL();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u32_RING_LL128();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u32_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u32_TREE_LL();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u32_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u32_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u64_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u64_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u64_RING_LL();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u64_RING_LL128();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u64_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u64_TREE_LL();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u64_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u64_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u8_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u8_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u8_RING_LL();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u8_RING_LL128();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u8_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u8_TREE_LL();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u8_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_PreMulSum_u8_TREE_SIMPLE();
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_Prod_bf16_COLLNET_CHAIN_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_Prod_bf16_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_Prod_bf16_RING_LL();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_Prod_bf16_RING_LL128();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_Prod_bf16_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_Prod_bf16_TREE_LL();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_Prod_bf16_TREE_LL128();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_Prod_bf16_TREE_SIMPLE();
#endif
__device__ void ncclDevFunc_AllReduce_Prod_f16_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_f16_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_f16_RING_LL();
__device__ void ncclDevFunc_AllReduce_Prod_f16_RING_LL128();
__device__ void ncclDevFunc_AllReduce_Prod_f16_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_f16_TREE_LL();
__device__ void ncclDevFunc_AllReduce_Prod_f16_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_Prod_f16_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_f32_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_f32_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_f32_RING_LL();
__device__ void ncclDevFunc_AllReduce_Prod_f32_RING_LL128();
__device__ void ncclDevFunc_AllReduce_Prod_f32_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_f32_TREE_LL();
__device__ void ncclDevFunc_AllReduce_Prod_f32_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_Prod_f32_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_f64_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_f64_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_f64_RING_LL();
__device__ void ncclDevFunc_AllReduce_Prod_f64_RING_LL128();
__device__ void ncclDevFunc_AllReduce_Prod_f64_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_f64_TREE_LL();
__device__ void ncclDevFunc_AllReduce_Prod_f64_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_Prod_f64_TREE_SIMPLE();
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Prod_f8e4m3_COLLNET_CHAIN_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Prod_f8e4m3_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Prod_f8e4m3_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Prod_f8e4m3_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Prod_f8e4m3_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Prod_f8e4m3_TREE_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Prod_f8e4m3_TREE_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Prod_f8e4m3_TREE_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Prod_f8e5m2_COLLNET_CHAIN_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Prod_f8e5m2_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Prod_f8e5m2_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Prod_f8e5m2_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Prod_f8e5m2_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Prod_f8e5m2_TREE_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Prod_f8e5m2_TREE_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Prod_f8e5m2_TREE_SIMPLE();
#endif
__device__ void ncclDevFunc_AllReduce_Prod_u32_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_u32_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_u32_RING_LL();
__device__ void ncclDevFunc_AllReduce_Prod_u32_RING_LL128();
__device__ void ncclDevFunc_AllReduce_Prod_u32_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_u32_TREE_LL();
__device__ void ncclDevFunc_AllReduce_Prod_u32_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_Prod_u32_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_u64_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_u64_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_u64_RING_LL();
__device__ void ncclDevFunc_AllReduce_Prod_u64_RING_LL128();
__device__ void ncclDevFunc_AllReduce_Prod_u64_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_u64_TREE_LL();
__device__ void ncclDevFunc_AllReduce_Prod_u64_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_Prod_u64_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_u8_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_u8_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_u8_RING_LL();
__device__ void ncclDevFunc_AllReduce_Prod_u8_RING_LL128();
__device__ void ncclDevFunc_AllReduce_Prod_u8_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Prod_u8_TREE_LL();
__device__ void ncclDevFunc_AllReduce_Prod_u8_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_Prod_u8_TREE_SIMPLE();
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_Sum_bf16_COLLNET_CHAIN_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_Sum_bf16_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_bf16_NVLS_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_bf16_NVLS_TREE_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_Sum_bf16_RING_LL();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_Sum_bf16_RING_LL128();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_Sum_bf16_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_Sum_bf16_TREE_LL();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_Sum_bf16_TREE_LL128();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_AllReduce_Sum_bf16_TREE_SIMPLE();
#endif
__device__ void ncclDevFunc_AllReduce_Sum_f16_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_f16_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f16_NVLS_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f16_NVLS_TREE_SIMPLE();
#endif
__device__ void ncclDevFunc_AllReduce_Sum_f16_RING_LL();
__device__ void ncclDevFunc_AllReduce_Sum_f16_RING_LL128();
__device__ void ncclDevFunc_AllReduce_Sum_f16_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_f16_TREE_LL();
__device__ void ncclDevFunc_AllReduce_Sum_f16_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_Sum_f16_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_f32_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_f32_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f32_NVLS_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f32_NVLS_TREE_SIMPLE();
#endif
__device__ void ncclDevFunc_AllReduce_Sum_f32_RING_LL();
__device__ void ncclDevFunc_AllReduce_Sum_f32_RING_LL128();
__device__ void ncclDevFunc_AllReduce_Sum_f32_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_f32_TREE_LL();
__device__ void ncclDevFunc_AllReduce_Sum_f32_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_Sum_f32_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_f64_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_f64_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f64_NVLS_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f64_NVLS_TREE_SIMPLE();
#endif
__device__ void ncclDevFunc_AllReduce_Sum_f64_RING_LL();
__device__ void ncclDevFunc_AllReduce_Sum_f64_RING_LL128();
__device__ void ncclDevFunc_AllReduce_Sum_f64_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_f64_TREE_LL();
__device__ void ncclDevFunc_AllReduce_Sum_f64_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_Sum_f64_TREE_SIMPLE();
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f8e4m3_COLLNET_CHAIN_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f8e4m3_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f8e4m3_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f8e4m3_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f8e4m3_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f8e4m3_TREE_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f8e4m3_TREE_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f8e4m3_TREE_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f8e5m2_COLLNET_CHAIN_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f8e5m2_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f8e5m2_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f8e5m2_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f8e5m2_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f8e5m2_TREE_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f8e5m2_TREE_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_f8e5m2_TREE_SIMPLE();
#endif
__device__ void ncclDevFunc_AllReduce_Sum_u32_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_u32_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_u32_NVLS_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_u32_NVLS_TREE_SIMPLE();
#endif
__device__ void ncclDevFunc_AllReduce_Sum_u32_RING_LL();
__device__ void ncclDevFunc_AllReduce_Sum_u32_RING_LL128();
__device__ void ncclDevFunc_AllReduce_Sum_u32_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_u32_TREE_LL();
__device__ void ncclDevFunc_AllReduce_Sum_u32_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_Sum_u32_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_u64_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_u64_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_u64_NVLS_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_AllReduce_Sum_u64_NVLS_TREE_SIMPLE();
#endif
__device__ void ncclDevFunc_AllReduce_Sum_u64_RING_LL();
__device__ void ncclDevFunc_AllReduce_Sum_u64_RING_LL128();
__device__ void ncclDevFunc_AllReduce_Sum_u64_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_u64_TREE_LL();
__device__ void ncclDevFunc_AllReduce_Sum_u64_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_Sum_u64_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_u8_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_u8_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_u8_RING_LL();
__device__ void ncclDevFunc_AllReduce_Sum_u8_RING_LL128();
__device__ void ncclDevFunc_AllReduce_Sum_u8_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_Sum_u8_TREE_LL();
__device__ void ncclDevFunc_AllReduce_Sum_u8_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_Sum_u8_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u32_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u32_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u32_RING_LL();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u32_RING_LL128();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u32_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u32_TREE_LL();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u32_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u32_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u64_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u64_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u64_RING_LL();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u64_RING_LL128();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u64_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u64_TREE_LL();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u64_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u64_TREE_SIMPLE();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u8_COLLNET_CHAIN_SIMPLE();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u8_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u8_RING_LL();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u8_RING_LL128();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u8_RING_SIMPLE();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u8_TREE_LL();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u8_TREE_LL128();
__device__ void ncclDevFunc_AllReduce_SumPostDiv_u8_TREE_SIMPLE();
__device__ void ncclDevFunc_Broadcast_RING_LL();
__device__ void ncclDevFunc_Broadcast_RING_LL128();
__device__ void ncclDevFunc_Broadcast_RING_SIMPLE();
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_Reduce_MinMax_bf16_RING_LL();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_Reduce_MinMax_bf16_RING_LL128();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_Reduce_MinMax_bf16_RING_SIMPLE();
#endif
__device__ void ncclDevFunc_Reduce_MinMax_f16_RING_LL();
__device__ void ncclDevFunc_Reduce_MinMax_f16_RING_LL128();
__device__ void ncclDevFunc_Reduce_MinMax_f16_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_MinMax_f32_RING_LL();
__device__ void ncclDevFunc_Reduce_MinMax_f32_RING_LL128();
__device__ void ncclDevFunc_Reduce_MinMax_f32_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_MinMax_f64_RING_LL();
__device__ void ncclDevFunc_Reduce_MinMax_f64_RING_LL128();
__device__ void ncclDevFunc_Reduce_MinMax_f64_RING_SIMPLE();
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_MinMax_f8e4m3_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_MinMax_f8e4m3_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_MinMax_f8e4m3_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_MinMax_f8e5m2_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_MinMax_f8e5m2_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_MinMax_f8e5m2_RING_SIMPLE();
#endif
__device__ void ncclDevFunc_Reduce_MinMax_u32_RING_LL();
__device__ void ncclDevFunc_Reduce_MinMax_u32_RING_LL128();
__device__ void ncclDevFunc_Reduce_MinMax_u32_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_MinMax_u64_RING_LL();
__device__ void ncclDevFunc_Reduce_MinMax_u64_RING_LL128();
__device__ void ncclDevFunc_Reduce_MinMax_u64_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_MinMax_u8_RING_LL();
__device__ void ncclDevFunc_Reduce_MinMax_u8_RING_LL128();
__device__ void ncclDevFunc_Reduce_MinMax_u8_RING_SIMPLE();
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_Reduce_PreMulSum_bf16_RING_LL();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_Reduce_PreMulSum_bf16_RING_LL128();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_Reduce_PreMulSum_bf16_RING_SIMPLE();
#endif
__device__ void ncclDevFunc_Reduce_PreMulSum_f16_RING_LL();
__device__ void ncclDevFunc_Reduce_PreMulSum_f16_RING_LL128();
__device__ void ncclDevFunc_Reduce_PreMulSum_f16_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_PreMulSum_f32_RING_LL();
__device__ void ncclDevFunc_Reduce_PreMulSum_f32_RING_LL128();
__device__ void ncclDevFunc_Reduce_PreMulSum_f32_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_PreMulSum_f64_RING_LL();
__device__ void ncclDevFunc_Reduce_PreMulSum_f64_RING_LL128();
__device__ void ncclDevFunc_Reduce_PreMulSum_f64_RING_SIMPLE();
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_PreMulSum_f8e4m3_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_PreMulSum_f8e4m3_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_PreMulSum_f8e4m3_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_PreMulSum_f8e5m2_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_PreMulSum_f8e5m2_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_PreMulSum_f8e5m2_RING_SIMPLE();
#endif
__device__ void ncclDevFunc_Reduce_PreMulSum_u32_RING_LL();
__device__ void ncclDevFunc_Reduce_PreMulSum_u32_RING_LL128();
__device__ void ncclDevFunc_Reduce_PreMulSum_u32_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_PreMulSum_u64_RING_LL();
__device__ void ncclDevFunc_Reduce_PreMulSum_u64_RING_LL128();
__device__ void ncclDevFunc_Reduce_PreMulSum_u64_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_PreMulSum_u8_RING_LL();
__device__ void ncclDevFunc_Reduce_PreMulSum_u8_RING_LL128();
__device__ void ncclDevFunc_Reduce_PreMulSum_u8_RING_SIMPLE();
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_Reduce_Prod_bf16_RING_LL();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_Reduce_Prod_bf16_RING_LL128();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_Reduce_Prod_bf16_RING_SIMPLE();
#endif
__device__ void ncclDevFunc_Reduce_Prod_f16_RING_LL();
__device__ void ncclDevFunc_Reduce_Prod_f16_RING_LL128();
__device__ void ncclDevFunc_Reduce_Prod_f16_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_Prod_f32_RING_LL();
__device__ void ncclDevFunc_Reduce_Prod_f32_RING_LL128();
__device__ void ncclDevFunc_Reduce_Prod_f32_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_Prod_f64_RING_LL();
__device__ void ncclDevFunc_Reduce_Prod_f64_RING_LL128();
__device__ void ncclDevFunc_Reduce_Prod_f64_RING_SIMPLE();
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_Prod_f8e4m3_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_Prod_f8e4m3_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_Prod_f8e4m3_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_Prod_f8e5m2_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_Prod_f8e5m2_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_Prod_f8e5m2_RING_SIMPLE();
#endif
__device__ void ncclDevFunc_Reduce_Prod_u32_RING_LL();
__device__ void ncclDevFunc_Reduce_Prod_u32_RING_LL128();
__device__ void ncclDevFunc_Reduce_Prod_u32_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_Prod_u64_RING_LL();
__device__ void ncclDevFunc_Reduce_Prod_u64_RING_LL128();
__device__ void ncclDevFunc_Reduce_Prod_u64_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_Prod_u8_RING_LL();
__device__ void ncclDevFunc_Reduce_Prod_u8_RING_LL128();
__device__ void ncclDevFunc_Reduce_Prod_u8_RING_SIMPLE();
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_Reduce_Sum_bf16_RING_LL();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_Reduce_Sum_bf16_RING_LL128();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_Reduce_Sum_bf16_RING_SIMPLE();
#endif
__device__ void ncclDevFunc_Reduce_Sum_f16_RING_LL();
__device__ void ncclDevFunc_Reduce_Sum_f16_RING_LL128();
__device__ void ncclDevFunc_Reduce_Sum_f16_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_Sum_f32_RING_LL();
__device__ void ncclDevFunc_Reduce_Sum_f32_RING_LL128();
__device__ void ncclDevFunc_Reduce_Sum_f32_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_Sum_f64_RING_LL();
__device__ void ncclDevFunc_Reduce_Sum_f64_RING_LL128();
__device__ void ncclDevFunc_Reduce_Sum_f64_RING_SIMPLE();
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_Sum_f8e4m3_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_Sum_f8e4m3_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_Sum_f8e4m3_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_Sum_f8e5m2_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_Sum_f8e5m2_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_Reduce_Sum_f8e5m2_RING_SIMPLE();
#endif
__device__ void ncclDevFunc_Reduce_Sum_u32_RING_LL();
__device__ void ncclDevFunc_Reduce_Sum_u32_RING_LL128();
__device__ void ncclDevFunc_Reduce_Sum_u32_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_Sum_u64_RING_LL();
__device__ void ncclDevFunc_Reduce_Sum_u64_RING_LL128();
__device__ void ncclDevFunc_Reduce_Sum_u64_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_Sum_u8_RING_LL();
__device__ void ncclDevFunc_Reduce_Sum_u8_RING_LL128();
__device__ void ncclDevFunc_Reduce_Sum_u8_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_SumPostDiv_u32_RING_LL();
__device__ void ncclDevFunc_Reduce_SumPostDiv_u32_RING_LL128();
__device__ void ncclDevFunc_Reduce_SumPostDiv_u32_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_SumPostDiv_u64_RING_LL();
__device__ void ncclDevFunc_Reduce_SumPostDiv_u64_RING_LL128();
__device__ void ncclDevFunc_Reduce_SumPostDiv_u64_RING_SIMPLE();
__device__ void ncclDevFunc_Reduce_SumPostDiv_u8_RING_LL();
__device__ void ncclDevFunc_Reduce_SumPostDiv_u8_RING_LL128();
__device__ void ncclDevFunc_Reduce_SumPostDiv_u8_RING_SIMPLE();
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_MinMax_bf16_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_MinMax_bf16_NVLS_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_MinMax_bf16_PAT_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_MinMax_bf16_RING_LL();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_MinMax_bf16_RING_LL128();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_MinMax_bf16_RING_SIMPLE();
#endif
__device__ void ncclDevFunc_ReduceScatter_MinMax_f16_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_MinMax_f16_NVLS_SIMPLE();
#endif
__device__ void ncclDevFunc_ReduceScatter_MinMax_f16_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_MinMax_f16_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_MinMax_f16_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_MinMax_f16_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_MinMax_f32_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_MinMax_f32_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_MinMax_f32_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_MinMax_f32_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_MinMax_f32_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_MinMax_f64_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_MinMax_f64_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_MinMax_f64_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_MinMax_f64_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_MinMax_f64_RING_SIMPLE();
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_MinMax_f8e4m3_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_MinMax_f8e4m3_PAT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_MinMax_f8e4m3_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_MinMax_f8e4m3_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_MinMax_f8e4m3_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_MinMax_f8e5m2_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_MinMax_f8e5m2_PAT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_MinMax_f8e5m2_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_MinMax_f8e5m2_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_MinMax_f8e5m2_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_MinMax_i32_NVLS_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_MinMax_i64_NVLS_SIMPLE();
#endif
__device__ void ncclDevFunc_ReduceScatter_MinMax_u32_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_MinMax_u32_NVLS_SIMPLE();
#endif
__device__ void ncclDevFunc_ReduceScatter_MinMax_u32_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_MinMax_u32_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_MinMax_u32_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_MinMax_u32_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_MinMax_u64_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_MinMax_u64_NVLS_SIMPLE();
#endif
__device__ void ncclDevFunc_ReduceScatter_MinMax_u64_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_MinMax_u64_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_MinMax_u64_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_MinMax_u64_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_MinMax_u8_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_MinMax_u8_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_MinMax_u8_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_MinMax_u8_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_MinMax_u8_RING_SIMPLE();
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_bf16_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_bf16_PAT_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_bf16_RING_LL();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_bf16_RING_LL128();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_bf16_RING_SIMPLE();
#endif
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f16_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f16_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f16_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f16_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f16_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f32_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f32_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f32_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f32_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f32_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f64_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f64_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f64_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f64_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f64_RING_SIMPLE();
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f8e4m3_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f8e4m3_PAT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f8e4m3_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f8e4m3_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f8e4m3_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f8e5m2_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f8e5m2_PAT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f8e5m2_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f8e5m2_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_f8e5m2_RING_SIMPLE();
#endif
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_u32_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_u32_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_u32_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_u32_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_u32_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_u64_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_u64_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_u64_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_u64_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_u64_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_u8_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_u8_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_u8_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_u8_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_PreMulSum_u8_RING_SIMPLE();
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_Prod_bf16_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_Prod_bf16_PAT_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_Prod_bf16_RING_LL();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_Prod_bf16_RING_LL128();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_Prod_bf16_RING_SIMPLE();
#endif
__device__ void ncclDevFunc_ReduceScatter_Prod_f16_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Prod_f16_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Prod_f16_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_Prod_f16_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_Prod_f16_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Prod_f32_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Prod_f32_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Prod_f32_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_Prod_f32_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_Prod_f32_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Prod_f64_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Prod_f64_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Prod_f64_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_Prod_f64_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_Prod_f64_RING_SIMPLE();
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Prod_f8e4m3_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Prod_f8e4m3_PAT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Prod_f8e4m3_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Prod_f8e4m3_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Prod_f8e4m3_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Prod_f8e5m2_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Prod_f8e5m2_PAT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Prod_f8e5m2_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Prod_f8e5m2_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Prod_f8e5m2_RING_SIMPLE();
#endif
__device__ void ncclDevFunc_ReduceScatter_Prod_u32_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Prod_u32_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Prod_u32_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_Prod_u32_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_Prod_u32_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Prod_u64_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Prod_u64_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Prod_u64_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_Prod_u64_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_Prod_u64_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Prod_u8_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Prod_u8_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Prod_u8_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_Prod_u8_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_Prod_u8_RING_SIMPLE();
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_Sum_bf16_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Sum_bf16_NVLS_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_Sum_bf16_PAT_SIMPLE();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_Sum_bf16_RING_LL();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_Sum_bf16_RING_LL128();
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
__device__ void ncclDevFunc_ReduceScatter_Sum_bf16_RING_SIMPLE();
#endif
__device__ void ncclDevFunc_ReduceScatter_Sum_f16_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Sum_f16_NVLS_SIMPLE();
#endif
__device__ void ncclDevFunc_ReduceScatter_Sum_f16_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Sum_f16_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_Sum_f16_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_Sum_f16_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Sum_f32_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Sum_f32_NVLS_SIMPLE();
#endif
__device__ void ncclDevFunc_ReduceScatter_Sum_f32_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Sum_f32_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_Sum_f32_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_Sum_f32_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Sum_f64_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Sum_f64_NVLS_SIMPLE();
#endif
__device__ void ncclDevFunc_ReduceScatter_Sum_f64_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Sum_f64_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_Sum_f64_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_Sum_f64_RING_SIMPLE();
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Sum_f8e4m3_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Sum_f8e4m3_PAT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Sum_f8e4m3_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Sum_f8e4m3_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Sum_f8e4m3_RING_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Sum_f8e5m2_COLLNET_DIRECT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Sum_f8e5m2_PAT_SIMPLE();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Sum_f8e5m2_RING_LL();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Sum_f8e5m2_RING_LL128();
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Sum_f8e5m2_RING_SIMPLE();
#endif
__device__ void ncclDevFunc_ReduceScatter_Sum_u32_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Sum_u32_NVLS_SIMPLE();
#endif
__device__ void ncclDevFunc_ReduceScatter_Sum_u32_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Sum_u32_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_Sum_u32_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_Sum_u32_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Sum_u64_COLLNET_DIRECT_SIMPLE();
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
__device__ void ncclDevFunc_ReduceScatter_Sum_u64_NVLS_SIMPLE();
#endif
__device__ void ncclDevFunc_ReduceScatter_Sum_u64_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Sum_u64_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_Sum_u64_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_Sum_u64_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Sum_u8_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Sum_u8_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_Sum_u8_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_Sum_u8_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_Sum_u8_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_SumPostDiv_u32_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_SumPostDiv_u32_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_SumPostDiv_u32_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_SumPostDiv_u32_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_SumPostDiv_u32_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_SumPostDiv_u64_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_SumPostDiv_u64_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_SumPostDiv_u64_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_SumPostDiv_u64_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_SumPostDiv_u64_RING_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_SumPostDiv_u8_COLLNET_DIRECT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_SumPostDiv_u8_PAT_SIMPLE();
__device__ void ncclDevFunc_ReduceScatter_SumPostDiv_u8_RING_LL();
__device__ void ncclDevFunc_ReduceScatter_SumPostDiv_u8_RING_LL128();
__device__ void ncclDevFunc_ReduceScatter_SumPostDiv_u8_RING_SIMPLE();
__device__ void ncclDevFunc_SendRecv();

__device__ ncclDevFuncPtr_t const ncclDevFuncTable[] = {
/*   0*/ ncclDevFunc_AllGather_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/*   1*/ ncclDevFunc_AllGather_NVLS_SIMPLE,
#else
/*   1*/ nullptr,
#endif
/*   2*/ ncclDevFunc_AllGather_PAT_SIMPLE,
/*   3*/ ncclDevFunc_AllGather_RING_LL,
/*   4*/ ncclDevFunc_AllGather_RING_LL128,
/*   5*/ ncclDevFunc_AllGather_RING_SIMPLE,
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/*   6*/ ncclDevFunc_AllReduce_MinMax_bf16_COLLNET_CHAIN_SIMPLE,
#else
/*   6*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/*   7*/ ncclDevFunc_AllReduce_MinMax_bf16_COLLNET_DIRECT_SIMPLE,
#else
/*   7*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/*   8*/ ncclDevFunc_AllReduce_MinMax_bf16_NVLS_SIMPLE,
#else
/*   8*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/*   9*/ ncclDevFunc_AllReduce_MinMax_bf16_NVLS_TREE_SIMPLE,
#else
/*   9*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/*  10*/ ncclDevFunc_AllReduce_MinMax_bf16_RING_LL,
#else
/*  10*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/*  11*/ ncclDevFunc_AllReduce_MinMax_bf16_RING_LL128,
#else
/*  11*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/*  12*/ ncclDevFunc_AllReduce_MinMax_bf16_RING_SIMPLE,
#else
/*  12*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/*  13*/ ncclDevFunc_AllReduce_MinMax_bf16_TREE_LL,
#else
/*  13*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/*  14*/ ncclDevFunc_AllReduce_MinMax_bf16_TREE_LL128,
#else
/*  14*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/*  15*/ ncclDevFunc_AllReduce_MinMax_bf16_TREE_SIMPLE,
#else
/*  15*/ nullptr,
#endif
/*  16*/ ncclDevFunc_AllReduce_MinMax_f16_COLLNET_CHAIN_SIMPLE,
/*  17*/ ncclDevFunc_AllReduce_MinMax_f16_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/*  18*/ ncclDevFunc_AllReduce_MinMax_f16_NVLS_SIMPLE,
#else
/*  18*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/*  19*/ ncclDevFunc_AllReduce_MinMax_f16_NVLS_TREE_SIMPLE,
#else
/*  19*/ nullptr,
#endif
/*  20*/ ncclDevFunc_AllReduce_MinMax_f16_RING_LL,
/*  21*/ ncclDevFunc_AllReduce_MinMax_f16_RING_LL128,
/*  22*/ ncclDevFunc_AllReduce_MinMax_f16_RING_SIMPLE,
/*  23*/ ncclDevFunc_AllReduce_MinMax_f16_TREE_LL,
/*  24*/ ncclDevFunc_AllReduce_MinMax_f16_TREE_LL128,
/*  25*/ ncclDevFunc_AllReduce_MinMax_f16_TREE_SIMPLE,
/*  26*/ ncclDevFunc_AllReduce_MinMax_f32_COLLNET_CHAIN_SIMPLE,
/*  27*/ ncclDevFunc_AllReduce_MinMax_f32_COLLNET_DIRECT_SIMPLE,
/*  28*/ ncclDevFunc_AllReduce_MinMax_f32_RING_LL,
/*  29*/ ncclDevFunc_AllReduce_MinMax_f32_RING_LL128,
/*  30*/ ncclDevFunc_AllReduce_MinMax_f32_RING_SIMPLE,
/*  31*/ ncclDevFunc_AllReduce_MinMax_f32_TREE_LL,
/*  32*/ ncclDevFunc_AllReduce_MinMax_f32_TREE_LL128,
/*  33*/ ncclDevFunc_AllReduce_MinMax_f32_TREE_SIMPLE,
/*  34*/ ncclDevFunc_AllReduce_MinMax_f64_COLLNET_CHAIN_SIMPLE,
/*  35*/ ncclDevFunc_AllReduce_MinMax_f64_COLLNET_DIRECT_SIMPLE,
/*  36*/ ncclDevFunc_AllReduce_MinMax_f64_RING_LL,
/*  37*/ ncclDevFunc_AllReduce_MinMax_f64_RING_LL128,
/*  38*/ ncclDevFunc_AllReduce_MinMax_f64_RING_SIMPLE,
/*  39*/ ncclDevFunc_AllReduce_MinMax_f64_TREE_LL,
/*  40*/ ncclDevFunc_AllReduce_MinMax_f64_TREE_LL128,
/*  41*/ ncclDevFunc_AllReduce_MinMax_f64_TREE_SIMPLE,
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/*  42*/ ncclDevFunc_AllReduce_MinMax_f8e4m3_COLLNET_CHAIN_SIMPLE,
#else
/*  42*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/*  43*/ ncclDevFunc_AllReduce_MinMax_f8e4m3_COLLNET_DIRECT_SIMPLE,
#else
/*  43*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/*  44*/ ncclDevFunc_AllReduce_MinMax_f8e4m3_RING_LL,
#else
/*  44*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/*  45*/ ncclDevFunc_AllReduce_MinMax_f8e4m3_RING_LL128,
#else
/*  45*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/*  46*/ ncclDevFunc_AllReduce_MinMax_f8e4m3_RING_SIMPLE,
#else
/*  46*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/*  47*/ ncclDevFunc_AllReduce_MinMax_f8e4m3_TREE_LL,
#else
/*  47*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/*  48*/ ncclDevFunc_AllReduce_MinMax_f8e4m3_TREE_LL128,
#else
/*  48*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/*  49*/ ncclDevFunc_AllReduce_MinMax_f8e4m3_TREE_SIMPLE,
#else
/*  49*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/*  50*/ ncclDevFunc_AllReduce_MinMax_f8e5m2_COLLNET_CHAIN_SIMPLE,
#else
/*  50*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/*  51*/ ncclDevFunc_AllReduce_MinMax_f8e5m2_COLLNET_DIRECT_SIMPLE,
#else
/*  51*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/*  52*/ ncclDevFunc_AllReduce_MinMax_f8e5m2_RING_LL,
#else
/*  52*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/*  53*/ ncclDevFunc_AllReduce_MinMax_f8e5m2_RING_LL128,
#else
/*  53*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/*  54*/ ncclDevFunc_AllReduce_MinMax_f8e5m2_RING_SIMPLE,
#else
/*  54*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/*  55*/ ncclDevFunc_AllReduce_MinMax_f8e5m2_TREE_LL,
#else
/*  55*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/*  56*/ ncclDevFunc_AllReduce_MinMax_f8e5m2_TREE_LL128,
#else
/*  56*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/*  57*/ ncclDevFunc_AllReduce_MinMax_f8e5m2_TREE_SIMPLE,
#else
/*  57*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/*  58*/ ncclDevFunc_AllReduce_MinMax_i32_NVLS_SIMPLE,
#else
/*  58*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/*  59*/ ncclDevFunc_AllReduce_MinMax_i32_NVLS_TREE_SIMPLE,
#else
/*  59*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/*  60*/ ncclDevFunc_AllReduce_MinMax_i64_NVLS_SIMPLE,
#else
/*  60*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/*  61*/ ncclDevFunc_AllReduce_MinMax_i64_NVLS_TREE_SIMPLE,
#else
/*  61*/ nullptr,
#endif
/*  62*/ ncclDevFunc_AllReduce_MinMax_u32_COLLNET_CHAIN_SIMPLE,
/*  63*/ ncclDevFunc_AllReduce_MinMax_u32_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/*  64*/ ncclDevFunc_AllReduce_MinMax_u32_NVLS_SIMPLE,
#else
/*  64*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/*  65*/ ncclDevFunc_AllReduce_MinMax_u32_NVLS_TREE_SIMPLE,
#else
/*  65*/ nullptr,
#endif
/*  66*/ ncclDevFunc_AllReduce_MinMax_u32_RING_LL,
/*  67*/ ncclDevFunc_AllReduce_MinMax_u32_RING_LL128,
/*  68*/ ncclDevFunc_AllReduce_MinMax_u32_RING_SIMPLE,
/*  69*/ ncclDevFunc_AllReduce_MinMax_u32_TREE_LL,
/*  70*/ ncclDevFunc_AllReduce_MinMax_u32_TREE_LL128,
/*  71*/ ncclDevFunc_AllReduce_MinMax_u32_TREE_SIMPLE,
/*  72*/ ncclDevFunc_AllReduce_MinMax_u64_COLLNET_CHAIN_SIMPLE,
/*  73*/ ncclDevFunc_AllReduce_MinMax_u64_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/*  74*/ ncclDevFunc_AllReduce_MinMax_u64_NVLS_SIMPLE,
#else
/*  74*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/*  75*/ ncclDevFunc_AllReduce_MinMax_u64_NVLS_TREE_SIMPLE,
#else
/*  75*/ nullptr,
#endif
/*  76*/ ncclDevFunc_AllReduce_MinMax_u64_RING_LL,
/*  77*/ ncclDevFunc_AllReduce_MinMax_u64_RING_LL128,
/*  78*/ ncclDevFunc_AllReduce_MinMax_u64_RING_SIMPLE,
/*  79*/ ncclDevFunc_AllReduce_MinMax_u64_TREE_LL,
/*  80*/ ncclDevFunc_AllReduce_MinMax_u64_TREE_LL128,
/*  81*/ ncclDevFunc_AllReduce_MinMax_u64_TREE_SIMPLE,
/*  82*/ ncclDevFunc_AllReduce_MinMax_u8_COLLNET_CHAIN_SIMPLE,
/*  83*/ ncclDevFunc_AllReduce_MinMax_u8_COLLNET_DIRECT_SIMPLE,
/*  84*/ ncclDevFunc_AllReduce_MinMax_u8_RING_LL,
/*  85*/ ncclDevFunc_AllReduce_MinMax_u8_RING_LL128,
/*  86*/ ncclDevFunc_AllReduce_MinMax_u8_RING_SIMPLE,
/*  87*/ ncclDevFunc_AllReduce_MinMax_u8_TREE_LL,
/*  88*/ ncclDevFunc_AllReduce_MinMax_u8_TREE_LL128,
/*  89*/ ncclDevFunc_AllReduce_MinMax_u8_TREE_SIMPLE,
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/*  90*/ ncclDevFunc_AllReduce_PreMulSum_bf16_COLLNET_CHAIN_SIMPLE,
#else
/*  90*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/*  91*/ ncclDevFunc_AllReduce_PreMulSum_bf16_COLLNET_DIRECT_SIMPLE,
#else
/*  91*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/*  92*/ ncclDevFunc_AllReduce_PreMulSum_bf16_RING_LL,
#else
/*  92*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/*  93*/ ncclDevFunc_AllReduce_PreMulSum_bf16_RING_LL128,
#else
/*  93*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/*  94*/ ncclDevFunc_AllReduce_PreMulSum_bf16_RING_SIMPLE,
#else
/*  94*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/*  95*/ ncclDevFunc_AllReduce_PreMulSum_bf16_TREE_LL,
#else
/*  95*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/*  96*/ ncclDevFunc_AllReduce_PreMulSum_bf16_TREE_LL128,
#else
/*  96*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/*  97*/ ncclDevFunc_AllReduce_PreMulSum_bf16_TREE_SIMPLE,
#else
/*  97*/ nullptr,
#endif
/*  98*/ ncclDevFunc_AllReduce_PreMulSum_f16_COLLNET_CHAIN_SIMPLE,
/*  99*/ ncclDevFunc_AllReduce_PreMulSum_f16_COLLNET_DIRECT_SIMPLE,
/* 100*/ ncclDevFunc_AllReduce_PreMulSum_f16_RING_LL,
/* 101*/ ncclDevFunc_AllReduce_PreMulSum_f16_RING_LL128,
/* 102*/ ncclDevFunc_AllReduce_PreMulSum_f16_RING_SIMPLE,
/* 103*/ ncclDevFunc_AllReduce_PreMulSum_f16_TREE_LL,
/* 104*/ ncclDevFunc_AllReduce_PreMulSum_f16_TREE_LL128,
/* 105*/ ncclDevFunc_AllReduce_PreMulSum_f16_TREE_SIMPLE,
/* 106*/ ncclDevFunc_AllReduce_PreMulSum_f32_COLLNET_CHAIN_SIMPLE,
/* 107*/ ncclDevFunc_AllReduce_PreMulSum_f32_COLLNET_DIRECT_SIMPLE,
/* 108*/ ncclDevFunc_AllReduce_PreMulSum_f32_RING_LL,
/* 109*/ ncclDevFunc_AllReduce_PreMulSum_f32_RING_LL128,
/* 110*/ ncclDevFunc_AllReduce_PreMulSum_f32_RING_SIMPLE,
/* 111*/ ncclDevFunc_AllReduce_PreMulSum_f32_TREE_LL,
/* 112*/ ncclDevFunc_AllReduce_PreMulSum_f32_TREE_LL128,
/* 113*/ ncclDevFunc_AllReduce_PreMulSum_f32_TREE_SIMPLE,
/* 114*/ ncclDevFunc_AllReduce_PreMulSum_f64_COLLNET_CHAIN_SIMPLE,
/* 115*/ ncclDevFunc_AllReduce_PreMulSum_f64_COLLNET_DIRECT_SIMPLE,
/* 116*/ ncclDevFunc_AllReduce_PreMulSum_f64_RING_LL,
/* 117*/ ncclDevFunc_AllReduce_PreMulSum_f64_RING_LL128,
/* 118*/ ncclDevFunc_AllReduce_PreMulSum_f64_RING_SIMPLE,
/* 119*/ ncclDevFunc_AllReduce_PreMulSum_f64_TREE_LL,
/* 120*/ ncclDevFunc_AllReduce_PreMulSum_f64_TREE_LL128,
/* 121*/ ncclDevFunc_AllReduce_PreMulSum_f64_TREE_SIMPLE,
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 122*/ ncclDevFunc_AllReduce_PreMulSum_f8e4m3_COLLNET_CHAIN_SIMPLE,
#else
/* 122*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 123*/ ncclDevFunc_AllReduce_PreMulSum_f8e4m3_COLLNET_DIRECT_SIMPLE,
#else
/* 123*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 124*/ ncclDevFunc_AllReduce_PreMulSum_f8e4m3_RING_LL,
#else
/* 124*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 125*/ ncclDevFunc_AllReduce_PreMulSum_f8e4m3_RING_LL128,
#else
/* 125*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 126*/ ncclDevFunc_AllReduce_PreMulSum_f8e4m3_RING_SIMPLE,
#else
/* 126*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 127*/ ncclDevFunc_AllReduce_PreMulSum_f8e4m3_TREE_LL,
#else
/* 127*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 128*/ ncclDevFunc_AllReduce_PreMulSum_f8e4m3_TREE_LL128,
#else
/* 128*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 129*/ ncclDevFunc_AllReduce_PreMulSum_f8e4m3_TREE_SIMPLE,
#else
/* 129*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 130*/ ncclDevFunc_AllReduce_PreMulSum_f8e5m2_COLLNET_CHAIN_SIMPLE,
#else
/* 130*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 131*/ ncclDevFunc_AllReduce_PreMulSum_f8e5m2_COLLNET_DIRECT_SIMPLE,
#else
/* 131*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 132*/ ncclDevFunc_AllReduce_PreMulSum_f8e5m2_RING_LL,
#else
/* 132*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 133*/ ncclDevFunc_AllReduce_PreMulSum_f8e5m2_RING_LL128,
#else
/* 133*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 134*/ ncclDevFunc_AllReduce_PreMulSum_f8e5m2_RING_SIMPLE,
#else
/* 134*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 135*/ ncclDevFunc_AllReduce_PreMulSum_f8e5m2_TREE_LL,
#else
/* 135*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 136*/ ncclDevFunc_AllReduce_PreMulSum_f8e5m2_TREE_LL128,
#else
/* 136*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 137*/ ncclDevFunc_AllReduce_PreMulSum_f8e5m2_TREE_SIMPLE,
#else
/* 137*/ nullptr,
#endif
/* 138*/ ncclDevFunc_AllReduce_PreMulSum_u32_COLLNET_CHAIN_SIMPLE,
/* 139*/ ncclDevFunc_AllReduce_PreMulSum_u32_COLLNET_DIRECT_SIMPLE,
/* 140*/ ncclDevFunc_AllReduce_PreMulSum_u32_RING_LL,
/* 141*/ ncclDevFunc_AllReduce_PreMulSum_u32_RING_LL128,
/* 142*/ ncclDevFunc_AllReduce_PreMulSum_u32_RING_SIMPLE,
/* 143*/ ncclDevFunc_AllReduce_PreMulSum_u32_TREE_LL,
/* 144*/ ncclDevFunc_AllReduce_PreMulSum_u32_TREE_LL128,
/* 145*/ ncclDevFunc_AllReduce_PreMulSum_u32_TREE_SIMPLE,
/* 146*/ ncclDevFunc_AllReduce_PreMulSum_u64_COLLNET_CHAIN_SIMPLE,
/* 147*/ ncclDevFunc_AllReduce_PreMulSum_u64_COLLNET_DIRECT_SIMPLE,
/* 148*/ ncclDevFunc_AllReduce_PreMulSum_u64_RING_LL,
/* 149*/ ncclDevFunc_AllReduce_PreMulSum_u64_RING_LL128,
/* 150*/ ncclDevFunc_AllReduce_PreMulSum_u64_RING_SIMPLE,
/* 151*/ ncclDevFunc_AllReduce_PreMulSum_u64_TREE_LL,
/* 152*/ ncclDevFunc_AllReduce_PreMulSum_u64_TREE_LL128,
/* 153*/ ncclDevFunc_AllReduce_PreMulSum_u64_TREE_SIMPLE,
/* 154*/ ncclDevFunc_AllReduce_PreMulSum_u8_COLLNET_CHAIN_SIMPLE,
/* 155*/ ncclDevFunc_AllReduce_PreMulSum_u8_COLLNET_DIRECT_SIMPLE,
/* 156*/ ncclDevFunc_AllReduce_PreMulSum_u8_RING_LL,
/* 157*/ ncclDevFunc_AllReduce_PreMulSum_u8_RING_LL128,
/* 158*/ ncclDevFunc_AllReduce_PreMulSum_u8_RING_SIMPLE,
/* 159*/ ncclDevFunc_AllReduce_PreMulSum_u8_TREE_LL,
/* 160*/ ncclDevFunc_AllReduce_PreMulSum_u8_TREE_LL128,
/* 161*/ ncclDevFunc_AllReduce_PreMulSum_u8_TREE_SIMPLE,
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 162*/ ncclDevFunc_AllReduce_Prod_bf16_COLLNET_CHAIN_SIMPLE,
#else
/* 162*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 163*/ ncclDevFunc_AllReduce_Prod_bf16_COLLNET_DIRECT_SIMPLE,
#else
/* 163*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 164*/ ncclDevFunc_AllReduce_Prod_bf16_RING_LL,
#else
/* 164*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 165*/ ncclDevFunc_AllReduce_Prod_bf16_RING_LL128,
#else
/* 165*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 166*/ ncclDevFunc_AllReduce_Prod_bf16_RING_SIMPLE,
#else
/* 166*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 167*/ ncclDevFunc_AllReduce_Prod_bf16_TREE_LL,
#else
/* 167*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 168*/ ncclDevFunc_AllReduce_Prod_bf16_TREE_LL128,
#else
/* 168*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 169*/ ncclDevFunc_AllReduce_Prod_bf16_TREE_SIMPLE,
#else
/* 169*/ nullptr,
#endif
/* 170*/ ncclDevFunc_AllReduce_Prod_f16_COLLNET_CHAIN_SIMPLE,
/* 171*/ ncclDevFunc_AllReduce_Prod_f16_COLLNET_DIRECT_SIMPLE,
/* 172*/ ncclDevFunc_AllReduce_Prod_f16_RING_LL,
/* 173*/ ncclDevFunc_AllReduce_Prod_f16_RING_LL128,
/* 174*/ ncclDevFunc_AllReduce_Prod_f16_RING_SIMPLE,
/* 175*/ ncclDevFunc_AllReduce_Prod_f16_TREE_LL,
/* 176*/ ncclDevFunc_AllReduce_Prod_f16_TREE_LL128,
/* 177*/ ncclDevFunc_AllReduce_Prod_f16_TREE_SIMPLE,
/* 178*/ ncclDevFunc_AllReduce_Prod_f32_COLLNET_CHAIN_SIMPLE,
/* 179*/ ncclDevFunc_AllReduce_Prod_f32_COLLNET_DIRECT_SIMPLE,
/* 180*/ ncclDevFunc_AllReduce_Prod_f32_RING_LL,
/* 181*/ ncclDevFunc_AllReduce_Prod_f32_RING_LL128,
/* 182*/ ncclDevFunc_AllReduce_Prod_f32_RING_SIMPLE,
/* 183*/ ncclDevFunc_AllReduce_Prod_f32_TREE_LL,
/* 184*/ ncclDevFunc_AllReduce_Prod_f32_TREE_LL128,
/* 185*/ ncclDevFunc_AllReduce_Prod_f32_TREE_SIMPLE,
/* 186*/ ncclDevFunc_AllReduce_Prod_f64_COLLNET_CHAIN_SIMPLE,
/* 187*/ ncclDevFunc_AllReduce_Prod_f64_COLLNET_DIRECT_SIMPLE,
/* 188*/ ncclDevFunc_AllReduce_Prod_f64_RING_LL,
/* 189*/ ncclDevFunc_AllReduce_Prod_f64_RING_LL128,
/* 190*/ ncclDevFunc_AllReduce_Prod_f64_RING_SIMPLE,
/* 191*/ ncclDevFunc_AllReduce_Prod_f64_TREE_LL,
/* 192*/ ncclDevFunc_AllReduce_Prod_f64_TREE_LL128,
/* 193*/ ncclDevFunc_AllReduce_Prod_f64_TREE_SIMPLE,
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 194*/ ncclDevFunc_AllReduce_Prod_f8e4m3_COLLNET_CHAIN_SIMPLE,
#else
/* 194*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 195*/ ncclDevFunc_AllReduce_Prod_f8e4m3_COLLNET_DIRECT_SIMPLE,
#else
/* 195*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 196*/ ncclDevFunc_AllReduce_Prod_f8e4m3_RING_LL,
#else
/* 196*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 197*/ ncclDevFunc_AllReduce_Prod_f8e4m3_RING_LL128,
#else
/* 197*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 198*/ ncclDevFunc_AllReduce_Prod_f8e4m3_RING_SIMPLE,
#else
/* 198*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 199*/ ncclDevFunc_AllReduce_Prod_f8e4m3_TREE_LL,
#else
/* 199*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 200*/ ncclDevFunc_AllReduce_Prod_f8e4m3_TREE_LL128,
#else
/* 200*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 201*/ ncclDevFunc_AllReduce_Prod_f8e4m3_TREE_SIMPLE,
#else
/* 201*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 202*/ ncclDevFunc_AllReduce_Prod_f8e5m2_COLLNET_CHAIN_SIMPLE,
#else
/* 202*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 203*/ ncclDevFunc_AllReduce_Prod_f8e5m2_COLLNET_DIRECT_SIMPLE,
#else
/* 203*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 204*/ ncclDevFunc_AllReduce_Prod_f8e5m2_RING_LL,
#else
/* 204*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 205*/ ncclDevFunc_AllReduce_Prod_f8e5m2_RING_LL128,
#else
/* 205*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 206*/ ncclDevFunc_AllReduce_Prod_f8e5m2_RING_SIMPLE,
#else
/* 206*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 207*/ ncclDevFunc_AllReduce_Prod_f8e5m2_TREE_LL,
#else
/* 207*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 208*/ ncclDevFunc_AllReduce_Prod_f8e5m2_TREE_LL128,
#else
/* 208*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 209*/ ncclDevFunc_AllReduce_Prod_f8e5m2_TREE_SIMPLE,
#else
/* 209*/ nullptr,
#endif
/* 210*/ ncclDevFunc_AllReduce_Prod_u32_COLLNET_CHAIN_SIMPLE,
/* 211*/ ncclDevFunc_AllReduce_Prod_u32_COLLNET_DIRECT_SIMPLE,
/* 212*/ ncclDevFunc_AllReduce_Prod_u32_RING_LL,
/* 213*/ ncclDevFunc_AllReduce_Prod_u32_RING_LL128,
/* 214*/ ncclDevFunc_AllReduce_Prod_u32_RING_SIMPLE,
/* 215*/ ncclDevFunc_AllReduce_Prod_u32_TREE_LL,
/* 216*/ ncclDevFunc_AllReduce_Prod_u32_TREE_LL128,
/* 217*/ ncclDevFunc_AllReduce_Prod_u32_TREE_SIMPLE,
/* 218*/ ncclDevFunc_AllReduce_Prod_u64_COLLNET_CHAIN_SIMPLE,
/* 219*/ ncclDevFunc_AllReduce_Prod_u64_COLLNET_DIRECT_SIMPLE,
/* 220*/ ncclDevFunc_AllReduce_Prod_u64_RING_LL,
/* 221*/ ncclDevFunc_AllReduce_Prod_u64_RING_LL128,
/* 222*/ ncclDevFunc_AllReduce_Prod_u64_RING_SIMPLE,
/* 223*/ ncclDevFunc_AllReduce_Prod_u64_TREE_LL,
/* 224*/ ncclDevFunc_AllReduce_Prod_u64_TREE_LL128,
/* 225*/ ncclDevFunc_AllReduce_Prod_u64_TREE_SIMPLE,
/* 226*/ ncclDevFunc_AllReduce_Prod_u8_COLLNET_CHAIN_SIMPLE,
/* 227*/ ncclDevFunc_AllReduce_Prod_u8_COLLNET_DIRECT_SIMPLE,
/* 228*/ ncclDevFunc_AllReduce_Prod_u8_RING_LL,
/* 229*/ ncclDevFunc_AllReduce_Prod_u8_RING_LL128,
/* 230*/ ncclDevFunc_AllReduce_Prod_u8_RING_SIMPLE,
/* 231*/ ncclDevFunc_AllReduce_Prod_u8_TREE_LL,
/* 232*/ ncclDevFunc_AllReduce_Prod_u8_TREE_LL128,
/* 233*/ ncclDevFunc_AllReduce_Prod_u8_TREE_SIMPLE,
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 234*/ ncclDevFunc_AllReduce_Sum_bf16_COLLNET_CHAIN_SIMPLE,
#else
/* 234*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 235*/ ncclDevFunc_AllReduce_Sum_bf16_COLLNET_DIRECT_SIMPLE,
#else
/* 235*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 236*/ ncclDevFunc_AllReduce_Sum_bf16_NVLS_SIMPLE,
#else
/* 236*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 237*/ ncclDevFunc_AllReduce_Sum_bf16_NVLS_TREE_SIMPLE,
#else
/* 237*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 238*/ ncclDevFunc_AllReduce_Sum_bf16_RING_LL,
#else
/* 238*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 239*/ ncclDevFunc_AllReduce_Sum_bf16_RING_LL128,
#else
/* 239*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 240*/ ncclDevFunc_AllReduce_Sum_bf16_RING_SIMPLE,
#else
/* 240*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 241*/ ncclDevFunc_AllReduce_Sum_bf16_TREE_LL,
#else
/* 241*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 242*/ ncclDevFunc_AllReduce_Sum_bf16_TREE_LL128,
#else
/* 242*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 243*/ ncclDevFunc_AllReduce_Sum_bf16_TREE_SIMPLE,
#else
/* 243*/ nullptr,
#endif
/* 244*/ ncclDevFunc_AllReduce_Sum_f16_COLLNET_CHAIN_SIMPLE,
/* 245*/ ncclDevFunc_AllReduce_Sum_f16_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 246*/ ncclDevFunc_AllReduce_Sum_f16_NVLS_SIMPLE,
#else
/* 246*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 247*/ ncclDevFunc_AllReduce_Sum_f16_NVLS_TREE_SIMPLE,
#else
/* 247*/ nullptr,
#endif
/* 248*/ ncclDevFunc_AllReduce_Sum_f16_RING_LL,
/* 249*/ ncclDevFunc_AllReduce_Sum_f16_RING_LL128,
/* 250*/ ncclDevFunc_AllReduce_Sum_f16_RING_SIMPLE,
/* 251*/ ncclDevFunc_AllReduce_Sum_f16_TREE_LL,
/* 252*/ ncclDevFunc_AllReduce_Sum_f16_TREE_LL128,
/* 253*/ ncclDevFunc_AllReduce_Sum_f16_TREE_SIMPLE,
/* 254*/ ncclDevFunc_AllReduce_Sum_f32_COLLNET_CHAIN_SIMPLE,
/* 255*/ ncclDevFunc_AllReduce_Sum_f32_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 256*/ ncclDevFunc_AllReduce_Sum_f32_NVLS_SIMPLE,
#else
/* 256*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 257*/ ncclDevFunc_AllReduce_Sum_f32_NVLS_TREE_SIMPLE,
#else
/* 257*/ nullptr,
#endif
/* 258*/ ncclDevFunc_AllReduce_Sum_f32_RING_LL,
/* 259*/ ncclDevFunc_AllReduce_Sum_f32_RING_LL128,
/* 260*/ ncclDevFunc_AllReduce_Sum_f32_RING_SIMPLE,
/* 261*/ ncclDevFunc_AllReduce_Sum_f32_TREE_LL,
/* 262*/ ncclDevFunc_AllReduce_Sum_f32_TREE_LL128,
/* 263*/ ncclDevFunc_AllReduce_Sum_f32_TREE_SIMPLE,
/* 264*/ ncclDevFunc_AllReduce_Sum_f64_COLLNET_CHAIN_SIMPLE,
/* 265*/ ncclDevFunc_AllReduce_Sum_f64_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 266*/ ncclDevFunc_AllReduce_Sum_f64_NVLS_SIMPLE,
#else
/* 266*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 267*/ ncclDevFunc_AllReduce_Sum_f64_NVLS_TREE_SIMPLE,
#else
/* 267*/ nullptr,
#endif
/* 268*/ ncclDevFunc_AllReduce_Sum_f64_RING_LL,
/* 269*/ ncclDevFunc_AllReduce_Sum_f64_RING_LL128,
/* 270*/ ncclDevFunc_AllReduce_Sum_f64_RING_SIMPLE,
/* 271*/ ncclDevFunc_AllReduce_Sum_f64_TREE_LL,
/* 272*/ ncclDevFunc_AllReduce_Sum_f64_TREE_LL128,
/* 273*/ ncclDevFunc_AllReduce_Sum_f64_TREE_SIMPLE,
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 274*/ ncclDevFunc_AllReduce_Sum_f8e4m3_COLLNET_CHAIN_SIMPLE,
#else
/* 274*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 275*/ ncclDevFunc_AllReduce_Sum_f8e4m3_COLLNET_DIRECT_SIMPLE,
#else
/* 275*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 276*/ ncclDevFunc_AllReduce_Sum_f8e4m3_RING_LL,
#else
/* 276*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 277*/ ncclDevFunc_AllReduce_Sum_f8e4m3_RING_LL128,
#else
/* 277*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 278*/ ncclDevFunc_AllReduce_Sum_f8e4m3_RING_SIMPLE,
#else
/* 278*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 279*/ ncclDevFunc_AllReduce_Sum_f8e4m3_TREE_LL,
#else
/* 279*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 280*/ ncclDevFunc_AllReduce_Sum_f8e4m3_TREE_LL128,
#else
/* 280*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 281*/ ncclDevFunc_AllReduce_Sum_f8e4m3_TREE_SIMPLE,
#else
/* 281*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 282*/ ncclDevFunc_AllReduce_Sum_f8e5m2_COLLNET_CHAIN_SIMPLE,
#else
/* 282*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 283*/ ncclDevFunc_AllReduce_Sum_f8e5m2_COLLNET_DIRECT_SIMPLE,
#else
/* 283*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 284*/ ncclDevFunc_AllReduce_Sum_f8e5m2_RING_LL,
#else
/* 284*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 285*/ ncclDevFunc_AllReduce_Sum_f8e5m2_RING_LL128,
#else
/* 285*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 286*/ ncclDevFunc_AllReduce_Sum_f8e5m2_RING_SIMPLE,
#else
/* 286*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 287*/ ncclDevFunc_AllReduce_Sum_f8e5m2_TREE_LL,
#else
/* 287*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 288*/ ncclDevFunc_AllReduce_Sum_f8e5m2_TREE_LL128,
#else
/* 288*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 289*/ ncclDevFunc_AllReduce_Sum_f8e5m2_TREE_SIMPLE,
#else
/* 289*/ nullptr,
#endif
/* 290*/ ncclDevFunc_AllReduce_Sum_u32_COLLNET_CHAIN_SIMPLE,
/* 291*/ ncclDevFunc_AllReduce_Sum_u32_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 292*/ ncclDevFunc_AllReduce_Sum_u32_NVLS_SIMPLE,
#else
/* 292*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 293*/ ncclDevFunc_AllReduce_Sum_u32_NVLS_TREE_SIMPLE,
#else
/* 293*/ nullptr,
#endif
/* 294*/ ncclDevFunc_AllReduce_Sum_u32_RING_LL,
/* 295*/ ncclDevFunc_AllReduce_Sum_u32_RING_LL128,
/* 296*/ ncclDevFunc_AllReduce_Sum_u32_RING_SIMPLE,
/* 297*/ ncclDevFunc_AllReduce_Sum_u32_TREE_LL,
/* 298*/ ncclDevFunc_AllReduce_Sum_u32_TREE_LL128,
/* 299*/ ncclDevFunc_AllReduce_Sum_u32_TREE_SIMPLE,
/* 300*/ ncclDevFunc_AllReduce_Sum_u64_COLLNET_CHAIN_SIMPLE,
/* 301*/ ncclDevFunc_AllReduce_Sum_u64_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 302*/ ncclDevFunc_AllReduce_Sum_u64_NVLS_SIMPLE,
#else
/* 302*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 303*/ ncclDevFunc_AllReduce_Sum_u64_NVLS_TREE_SIMPLE,
#else
/* 303*/ nullptr,
#endif
/* 304*/ ncclDevFunc_AllReduce_Sum_u64_RING_LL,
/* 305*/ ncclDevFunc_AllReduce_Sum_u64_RING_LL128,
/* 306*/ ncclDevFunc_AllReduce_Sum_u64_RING_SIMPLE,
/* 307*/ ncclDevFunc_AllReduce_Sum_u64_TREE_LL,
/* 308*/ ncclDevFunc_AllReduce_Sum_u64_TREE_LL128,
/* 309*/ ncclDevFunc_AllReduce_Sum_u64_TREE_SIMPLE,
/* 310*/ ncclDevFunc_AllReduce_Sum_u8_COLLNET_CHAIN_SIMPLE,
/* 311*/ ncclDevFunc_AllReduce_Sum_u8_COLLNET_DIRECT_SIMPLE,
/* 312*/ ncclDevFunc_AllReduce_Sum_u8_RING_LL,
/* 313*/ ncclDevFunc_AllReduce_Sum_u8_RING_LL128,
/* 314*/ ncclDevFunc_AllReduce_Sum_u8_RING_SIMPLE,
/* 315*/ ncclDevFunc_AllReduce_Sum_u8_TREE_LL,
/* 316*/ ncclDevFunc_AllReduce_Sum_u8_TREE_LL128,
/* 317*/ ncclDevFunc_AllReduce_Sum_u8_TREE_SIMPLE,
/* 318*/ ncclDevFunc_AllReduce_SumPostDiv_u32_COLLNET_CHAIN_SIMPLE,
/* 319*/ ncclDevFunc_AllReduce_SumPostDiv_u32_COLLNET_DIRECT_SIMPLE,
/* 320*/ ncclDevFunc_AllReduce_SumPostDiv_u32_RING_LL,
/* 321*/ ncclDevFunc_AllReduce_SumPostDiv_u32_RING_LL128,
/* 322*/ ncclDevFunc_AllReduce_SumPostDiv_u32_RING_SIMPLE,
/* 323*/ ncclDevFunc_AllReduce_SumPostDiv_u32_TREE_LL,
/* 324*/ ncclDevFunc_AllReduce_SumPostDiv_u32_TREE_LL128,
/* 325*/ ncclDevFunc_AllReduce_SumPostDiv_u32_TREE_SIMPLE,
/* 326*/ ncclDevFunc_AllReduce_SumPostDiv_u64_COLLNET_CHAIN_SIMPLE,
/* 327*/ ncclDevFunc_AllReduce_SumPostDiv_u64_COLLNET_DIRECT_SIMPLE,
/* 328*/ ncclDevFunc_AllReduce_SumPostDiv_u64_RING_LL,
/* 329*/ ncclDevFunc_AllReduce_SumPostDiv_u64_RING_LL128,
/* 330*/ ncclDevFunc_AllReduce_SumPostDiv_u64_RING_SIMPLE,
/* 331*/ ncclDevFunc_AllReduce_SumPostDiv_u64_TREE_LL,
/* 332*/ ncclDevFunc_AllReduce_SumPostDiv_u64_TREE_LL128,
/* 333*/ ncclDevFunc_AllReduce_SumPostDiv_u64_TREE_SIMPLE,
/* 334*/ ncclDevFunc_AllReduce_SumPostDiv_u8_COLLNET_CHAIN_SIMPLE,
/* 335*/ ncclDevFunc_AllReduce_SumPostDiv_u8_COLLNET_DIRECT_SIMPLE,
/* 336*/ ncclDevFunc_AllReduce_SumPostDiv_u8_RING_LL,
/* 337*/ ncclDevFunc_AllReduce_SumPostDiv_u8_RING_LL128,
/* 338*/ ncclDevFunc_AllReduce_SumPostDiv_u8_RING_SIMPLE,
/* 339*/ ncclDevFunc_AllReduce_SumPostDiv_u8_TREE_LL,
/* 340*/ ncclDevFunc_AllReduce_SumPostDiv_u8_TREE_LL128,
/* 341*/ ncclDevFunc_AllReduce_SumPostDiv_u8_TREE_SIMPLE,
/* 342*/ ncclDevFunc_Broadcast_RING_LL,
/* 343*/ ncclDevFunc_Broadcast_RING_LL128,
/* 344*/ ncclDevFunc_Broadcast_RING_SIMPLE,
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 345*/ ncclDevFunc_Reduce_MinMax_bf16_RING_LL,
#else
/* 345*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 346*/ ncclDevFunc_Reduce_MinMax_bf16_RING_LL128,
#else
/* 346*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 347*/ ncclDevFunc_Reduce_MinMax_bf16_RING_SIMPLE,
#else
/* 347*/ nullptr,
#endif
/* 348*/ ncclDevFunc_Reduce_MinMax_f16_RING_LL,
/* 349*/ ncclDevFunc_Reduce_MinMax_f16_RING_LL128,
/* 350*/ ncclDevFunc_Reduce_MinMax_f16_RING_SIMPLE,
/* 351*/ ncclDevFunc_Reduce_MinMax_f32_RING_LL,
/* 352*/ ncclDevFunc_Reduce_MinMax_f32_RING_LL128,
/* 353*/ ncclDevFunc_Reduce_MinMax_f32_RING_SIMPLE,
/* 354*/ ncclDevFunc_Reduce_MinMax_f64_RING_LL,
/* 355*/ ncclDevFunc_Reduce_MinMax_f64_RING_LL128,
/* 356*/ ncclDevFunc_Reduce_MinMax_f64_RING_SIMPLE,
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 357*/ ncclDevFunc_Reduce_MinMax_f8e4m3_RING_LL,
#else
/* 357*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 358*/ ncclDevFunc_Reduce_MinMax_f8e4m3_RING_LL128,
#else
/* 358*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 359*/ ncclDevFunc_Reduce_MinMax_f8e4m3_RING_SIMPLE,
#else
/* 359*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 360*/ ncclDevFunc_Reduce_MinMax_f8e5m2_RING_LL,
#else
/* 360*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 361*/ ncclDevFunc_Reduce_MinMax_f8e5m2_RING_LL128,
#else
/* 361*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 362*/ ncclDevFunc_Reduce_MinMax_f8e5m2_RING_SIMPLE,
#else
/* 362*/ nullptr,
#endif
/* 363*/ ncclDevFunc_Reduce_MinMax_u32_RING_LL,
/* 364*/ ncclDevFunc_Reduce_MinMax_u32_RING_LL128,
/* 365*/ ncclDevFunc_Reduce_MinMax_u32_RING_SIMPLE,
/* 366*/ ncclDevFunc_Reduce_MinMax_u64_RING_LL,
/* 367*/ ncclDevFunc_Reduce_MinMax_u64_RING_LL128,
/* 368*/ ncclDevFunc_Reduce_MinMax_u64_RING_SIMPLE,
/* 369*/ ncclDevFunc_Reduce_MinMax_u8_RING_LL,
/* 370*/ ncclDevFunc_Reduce_MinMax_u8_RING_LL128,
/* 371*/ ncclDevFunc_Reduce_MinMax_u8_RING_SIMPLE,
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 372*/ ncclDevFunc_Reduce_PreMulSum_bf16_RING_LL,
#else
/* 372*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 373*/ ncclDevFunc_Reduce_PreMulSum_bf16_RING_LL128,
#else
/* 373*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 374*/ ncclDevFunc_Reduce_PreMulSum_bf16_RING_SIMPLE,
#else
/* 374*/ nullptr,
#endif
/* 375*/ ncclDevFunc_Reduce_PreMulSum_f16_RING_LL,
/* 376*/ ncclDevFunc_Reduce_PreMulSum_f16_RING_LL128,
/* 377*/ ncclDevFunc_Reduce_PreMulSum_f16_RING_SIMPLE,
/* 378*/ ncclDevFunc_Reduce_PreMulSum_f32_RING_LL,
/* 379*/ ncclDevFunc_Reduce_PreMulSum_f32_RING_LL128,
/* 380*/ ncclDevFunc_Reduce_PreMulSum_f32_RING_SIMPLE,
/* 381*/ ncclDevFunc_Reduce_PreMulSum_f64_RING_LL,
/* 382*/ ncclDevFunc_Reduce_PreMulSum_f64_RING_LL128,
/* 383*/ ncclDevFunc_Reduce_PreMulSum_f64_RING_SIMPLE,
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 384*/ ncclDevFunc_Reduce_PreMulSum_f8e4m3_RING_LL,
#else
/* 384*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 385*/ ncclDevFunc_Reduce_PreMulSum_f8e4m3_RING_LL128,
#else
/* 385*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 386*/ ncclDevFunc_Reduce_PreMulSum_f8e4m3_RING_SIMPLE,
#else
/* 386*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 387*/ ncclDevFunc_Reduce_PreMulSum_f8e5m2_RING_LL,
#else
/* 387*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 388*/ ncclDevFunc_Reduce_PreMulSum_f8e5m2_RING_LL128,
#else
/* 388*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 389*/ ncclDevFunc_Reduce_PreMulSum_f8e5m2_RING_SIMPLE,
#else
/* 389*/ nullptr,
#endif
/* 390*/ ncclDevFunc_Reduce_PreMulSum_u32_RING_LL,
/* 391*/ ncclDevFunc_Reduce_PreMulSum_u32_RING_LL128,
/* 392*/ ncclDevFunc_Reduce_PreMulSum_u32_RING_SIMPLE,
/* 393*/ ncclDevFunc_Reduce_PreMulSum_u64_RING_LL,
/* 394*/ ncclDevFunc_Reduce_PreMulSum_u64_RING_LL128,
/* 395*/ ncclDevFunc_Reduce_PreMulSum_u64_RING_SIMPLE,
/* 396*/ ncclDevFunc_Reduce_PreMulSum_u8_RING_LL,
/* 397*/ ncclDevFunc_Reduce_PreMulSum_u8_RING_LL128,
/* 398*/ ncclDevFunc_Reduce_PreMulSum_u8_RING_SIMPLE,
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 399*/ ncclDevFunc_Reduce_Prod_bf16_RING_LL,
#else
/* 399*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 400*/ ncclDevFunc_Reduce_Prod_bf16_RING_LL128,
#else
/* 400*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 401*/ ncclDevFunc_Reduce_Prod_bf16_RING_SIMPLE,
#else
/* 401*/ nullptr,
#endif
/* 402*/ ncclDevFunc_Reduce_Prod_f16_RING_LL,
/* 403*/ ncclDevFunc_Reduce_Prod_f16_RING_LL128,
/* 404*/ ncclDevFunc_Reduce_Prod_f16_RING_SIMPLE,
/* 405*/ ncclDevFunc_Reduce_Prod_f32_RING_LL,
/* 406*/ ncclDevFunc_Reduce_Prod_f32_RING_LL128,
/* 407*/ ncclDevFunc_Reduce_Prod_f32_RING_SIMPLE,
/* 408*/ ncclDevFunc_Reduce_Prod_f64_RING_LL,
/* 409*/ ncclDevFunc_Reduce_Prod_f64_RING_LL128,
/* 410*/ ncclDevFunc_Reduce_Prod_f64_RING_SIMPLE,
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 411*/ ncclDevFunc_Reduce_Prod_f8e4m3_RING_LL,
#else
/* 411*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 412*/ ncclDevFunc_Reduce_Prod_f8e4m3_RING_LL128,
#else
/* 412*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 413*/ ncclDevFunc_Reduce_Prod_f8e4m3_RING_SIMPLE,
#else
/* 413*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 414*/ ncclDevFunc_Reduce_Prod_f8e5m2_RING_LL,
#else
/* 414*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 415*/ ncclDevFunc_Reduce_Prod_f8e5m2_RING_LL128,
#else
/* 415*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 416*/ ncclDevFunc_Reduce_Prod_f8e5m2_RING_SIMPLE,
#else
/* 416*/ nullptr,
#endif
/* 417*/ ncclDevFunc_Reduce_Prod_u32_RING_LL,
/* 418*/ ncclDevFunc_Reduce_Prod_u32_RING_LL128,
/* 419*/ ncclDevFunc_Reduce_Prod_u32_RING_SIMPLE,
/* 420*/ ncclDevFunc_Reduce_Prod_u64_RING_LL,
/* 421*/ ncclDevFunc_Reduce_Prod_u64_RING_LL128,
/* 422*/ ncclDevFunc_Reduce_Prod_u64_RING_SIMPLE,
/* 423*/ ncclDevFunc_Reduce_Prod_u8_RING_LL,
/* 424*/ ncclDevFunc_Reduce_Prod_u8_RING_LL128,
/* 425*/ ncclDevFunc_Reduce_Prod_u8_RING_SIMPLE,
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 426*/ ncclDevFunc_Reduce_Sum_bf16_RING_LL,
#else
/* 426*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 427*/ ncclDevFunc_Reduce_Sum_bf16_RING_LL128,
#else
/* 427*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 428*/ ncclDevFunc_Reduce_Sum_bf16_RING_SIMPLE,
#else
/* 428*/ nullptr,
#endif
/* 429*/ ncclDevFunc_Reduce_Sum_f16_RING_LL,
/* 430*/ ncclDevFunc_Reduce_Sum_f16_RING_LL128,
/* 431*/ ncclDevFunc_Reduce_Sum_f16_RING_SIMPLE,
/* 432*/ ncclDevFunc_Reduce_Sum_f32_RING_LL,
/* 433*/ ncclDevFunc_Reduce_Sum_f32_RING_LL128,
/* 434*/ ncclDevFunc_Reduce_Sum_f32_RING_SIMPLE,
/* 435*/ ncclDevFunc_Reduce_Sum_f64_RING_LL,
/* 436*/ ncclDevFunc_Reduce_Sum_f64_RING_LL128,
/* 437*/ ncclDevFunc_Reduce_Sum_f64_RING_SIMPLE,
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 438*/ ncclDevFunc_Reduce_Sum_f8e4m3_RING_LL,
#else
/* 438*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 439*/ ncclDevFunc_Reduce_Sum_f8e4m3_RING_LL128,
#else
/* 439*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 440*/ ncclDevFunc_Reduce_Sum_f8e4m3_RING_SIMPLE,
#else
/* 440*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 441*/ ncclDevFunc_Reduce_Sum_f8e5m2_RING_LL,
#else
/* 441*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 442*/ ncclDevFunc_Reduce_Sum_f8e5m2_RING_LL128,
#else
/* 442*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 443*/ ncclDevFunc_Reduce_Sum_f8e5m2_RING_SIMPLE,
#else
/* 443*/ nullptr,
#endif
/* 444*/ ncclDevFunc_Reduce_Sum_u32_RING_LL,
/* 445*/ ncclDevFunc_Reduce_Sum_u32_RING_LL128,
/* 446*/ ncclDevFunc_Reduce_Sum_u32_RING_SIMPLE,
/* 447*/ ncclDevFunc_Reduce_Sum_u64_RING_LL,
/* 448*/ ncclDevFunc_Reduce_Sum_u64_RING_LL128,
/* 449*/ ncclDevFunc_Reduce_Sum_u64_RING_SIMPLE,
/* 450*/ ncclDevFunc_Reduce_Sum_u8_RING_LL,
/* 451*/ ncclDevFunc_Reduce_Sum_u8_RING_LL128,
/* 452*/ ncclDevFunc_Reduce_Sum_u8_RING_SIMPLE,
/* 453*/ ncclDevFunc_Reduce_SumPostDiv_u32_RING_LL,
/* 454*/ ncclDevFunc_Reduce_SumPostDiv_u32_RING_LL128,
/* 455*/ ncclDevFunc_Reduce_SumPostDiv_u32_RING_SIMPLE,
/* 456*/ ncclDevFunc_Reduce_SumPostDiv_u64_RING_LL,
/* 457*/ ncclDevFunc_Reduce_SumPostDiv_u64_RING_LL128,
/* 458*/ ncclDevFunc_Reduce_SumPostDiv_u64_RING_SIMPLE,
/* 459*/ ncclDevFunc_Reduce_SumPostDiv_u8_RING_LL,
/* 460*/ ncclDevFunc_Reduce_SumPostDiv_u8_RING_LL128,
/* 461*/ ncclDevFunc_Reduce_SumPostDiv_u8_RING_SIMPLE,
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 462*/ ncclDevFunc_ReduceScatter_MinMax_bf16_COLLNET_DIRECT_SIMPLE,
#else
/* 462*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 463*/ ncclDevFunc_ReduceScatter_MinMax_bf16_NVLS_SIMPLE,
#else
/* 463*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 464*/ ncclDevFunc_ReduceScatter_MinMax_bf16_PAT_SIMPLE,
#else
/* 464*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 465*/ ncclDevFunc_ReduceScatter_MinMax_bf16_RING_LL,
#else
/* 465*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 466*/ ncclDevFunc_ReduceScatter_MinMax_bf16_RING_LL128,
#else
/* 466*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 467*/ ncclDevFunc_ReduceScatter_MinMax_bf16_RING_SIMPLE,
#else
/* 467*/ nullptr,
#endif
/* 468*/ ncclDevFunc_ReduceScatter_MinMax_f16_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 469*/ ncclDevFunc_ReduceScatter_MinMax_f16_NVLS_SIMPLE,
#else
/* 469*/ nullptr,
#endif
/* 470*/ ncclDevFunc_ReduceScatter_MinMax_f16_PAT_SIMPLE,
/* 471*/ ncclDevFunc_ReduceScatter_MinMax_f16_RING_LL,
/* 472*/ ncclDevFunc_ReduceScatter_MinMax_f16_RING_LL128,
/* 473*/ ncclDevFunc_ReduceScatter_MinMax_f16_RING_SIMPLE,
/* 474*/ ncclDevFunc_ReduceScatter_MinMax_f32_COLLNET_DIRECT_SIMPLE,
/* 475*/ ncclDevFunc_ReduceScatter_MinMax_f32_PAT_SIMPLE,
/* 476*/ ncclDevFunc_ReduceScatter_MinMax_f32_RING_LL,
/* 477*/ ncclDevFunc_ReduceScatter_MinMax_f32_RING_LL128,
/* 478*/ ncclDevFunc_ReduceScatter_MinMax_f32_RING_SIMPLE,
/* 479*/ ncclDevFunc_ReduceScatter_MinMax_f64_COLLNET_DIRECT_SIMPLE,
/* 480*/ ncclDevFunc_ReduceScatter_MinMax_f64_PAT_SIMPLE,
/* 481*/ ncclDevFunc_ReduceScatter_MinMax_f64_RING_LL,
/* 482*/ ncclDevFunc_ReduceScatter_MinMax_f64_RING_LL128,
/* 483*/ ncclDevFunc_ReduceScatter_MinMax_f64_RING_SIMPLE,
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 484*/ ncclDevFunc_ReduceScatter_MinMax_f8e4m3_COLLNET_DIRECT_SIMPLE,
#else
/* 484*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 485*/ ncclDevFunc_ReduceScatter_MinMax_f8e4m3_PAT_SIMPLE,
#else
/* 485*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 486*/ ncclDevFunc_ReduceScatter_MinMax_f8e4m3_RING_LL,
#else
/* 486*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 487*/ ncclDevFunc_ReduceScatter_MinMax_f8e4m3_RING_LL128,
#else
/* 487*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 488*/ ncclDevFunc_ReduceScatter_MinMax_f8e4m3_RING_SIMPLE,
#else
/* 488*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 489*/ ncclDevFunc_ReduceScatter_MinMax_f8e5m2_COLLNET_DIRECT_SIMPLE,
#else
/* 489*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 490*/ ncclDevFunc_ReduceScatter_MinMax_f8e5m2_PAT_SIMPLE,
#else
/* 490*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 491*/ ncclDevFunc_ReduceScatter_MinMax_f8e5m2_RING_LL,
#else
/* 491*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 492*/ ncclDevFunc_ReduceScatter_MinMax_f8e5m2_RING_LL128,
#else
/* 492*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 493*/ ncclDevFunc_ReduceScatter_MinMax_f8e5m2_RING_SIMPLE,
#else
/* 493*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 494*/ ncclDevFunc_ReduceScatter_MinMax_i32_NVLS_SIMPLE,
#else
/* 494*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 495*/ ncclDevFunc_ReduceScatter_MinMax_i64_NVLS_SIMPLE,
#else
/* 495*/ nullptr,
#endif
/* 496*/ ncclDevFunc_ReduceScatter_MinMax_u32_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 497*/ ncclDevFunc_ReduceScatter_MinMax_u32_NVLS_SIMPLE,
#else
/* 497*/ nullptr,
#endif
/* 498*/ ncclDevFunc_ReduceScatter_MinMax_u32_PAT_SIMPLE,
/* 499*/ ncclDevFunc_ReduceScatter_MinMax_u32_RING_LL,
/* 500*/ ncclDevFunc_ReduceScatter_MinMax_u32_RING_LL128,
/* 501*/ ncclDevFunc_ReduceScatter_MinMax_u32_RING_SIMPLE,
/* 502*/ ncclDevFunc_ReduceScatter_MinMax_u64_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 503*/ ncclDevFunc_ReduceScatter_MinMax_u64_NVLS_SIMPLE,
#else
/* 503*/ nullptr,
#endif
/* 504*/ ncclDevFunc_ReduceScatter_MinMax_u64_PAT_SIMPLE,
/* 505*/ ncclDevFunc_ReduceScatter_MinMax_u64_RING_LL,
/* 506*/ ncclDevFunc_ReduceScatter_MinMax_u64_RING_LL128,
/* 507*/ ncclDevFunc_ReduceScatter_MinMax_u64_RING_SIMPLE,
/* 508*/ ncclDevFunc_ReduceScatter_MinMax_u8_COLLNET_DIRECT_SIMPLE,
/* 509*/ ncclDevFunc_ReduceScatter_MinMax_u8_PAT_SIMPLE,
/* 510*/ ncclDevFunc_ReduceScatter_MinMax_u8_RING_LL,
/* 511*/ ncclDevFunc_ReduceScatter_MinMax_u8_RING_LL128,
/* 512*/ ncclDevFunc_ReduceScatter_MinMax_u8_RING_SIMPLE,
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 513*/ ncclDevFunc_ReduceScatter_PreMulSum_bf16_COLLNET_DIRECT_SIMPLE,
#else
/* 513*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 514*/ ncclDevFunc_ReduceScatter_PreMulSum_bf16_PAT_SIMPLE,
#else
/* 514*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 515*/ ncclDevFunc_ReduceScatter_PreMulSum_bf16_RING_LL,
#else
/* 515*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 516*/ ncclDevFunc_ReduceScatter_PreMulSum_bf16_RING_LL128,
#else
/* 516*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 517*/ ncclDevFunc_ReduceScatter_PreMulSum_bf16_RING_SIMPLE,
#else
/* 517*/ nullptr,
#endif
/* 518*/ ncclDevFunc_ReduceScatter_PreMulSum_f16_COLLNET_DIRECT_SIMPLE,
/* 519*/ ncclDevFunc_ReduceScatter_PreMulSum_f16_PAT_SIMPLE,
/* 520*/ ncclDevFunc_ReduceScatter_PreMulSum_f16_RING_LL,
/* 521*/ ncclDevFunc_ReduceScatter_PreMulSum_f16_RING_LL128,
/* 522*/ ncclDevFunc_ReduceScatter_PreMulSum_f16_RING_SIMPLE,
/* 523*/ ncclDevFunc_ReduceScatter_PreMulSum_f32_COLLNET_DIRECT_SIMPLE,
/* 524*/ ncclDevFunc_ReduceScatter_PreMulSum_f32_PAT_SIMPLE,
/* 525*/ ncclDevFunc_ReduceScatter_PreMulSum_f32_RING_LL,
/* 526*/ ncclDevFunc_ReduceScatter_PreMulSum_f32_RING_LL128,
/* 527*/ ncclDevFunc_ReduceScatter_PreMulSum_f32_RING_SIMPLE,
/* 528*/ ncclDevFunc_ReduceScatter_PreMulSum_f64_COLLNET_DIRECT_SIMPLE,
/* 529*/ ncclDevFunc_ReduceScatter_PreMulSum_f64_PAT_SIMPLE,
/* 530*/ ncclDevFunc_ReduceScatter_PreMulSum_f64_RING_LL,
/* 531*/ ncclDevFunc_ReduceScatter_PreMulSum_f64_RING_LL128,
/* 532*/ ncclDevFunc_ReduceScatter_PreMulSum_f64_RING_SIMPLE,
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 533*/ ncclDevFunc_ReduceScatter_PreMulSum_f8e4m3_COLLNET_DIRECT_SIMPLE,
#else
/* 533*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 534*/ ncclDevFunc_ReduceScatter_PreMulSum_f8e4m3_PAT_SIMPLE,
#else
/* 534*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 535*/ ncclDevFunc_ReduceScatter_PreMulSum_f8e4m3_RING_LL,
#else
/* 535*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 536*/ ncclDevFunc_ReduceScatter_PreMulSum_f8e4m3_RING_LL128,
#else
/* 536*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 537*/ ncclDevFunc_ReduceScatter_PreMulSum_f8e4m3_RING_SIMPLE,
#else
/* 537*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 538*/ ncclDevFunc_ReduceScatter_PreMulSum_f8e5m2_COLLNET_DIRECT_SIMPLE,
#else
/* 538*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 539*/ ncclDevFunc_ReduceScatter_PreMulSum_f8e5m2_PAT_SIMPLE,
#else
/* 539*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 540*/ ncclDevFunc_ReduceScatter_PreMulSum_f8e5m2_RING_LL,
#else
/* 540*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 541*/ ncclDevFunc_ReduceScatter_PreMulSum_f8e5m2_RING_LL128,
#else
/* 541*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 542*/ ncclDevFunc_ReduceScatter_PreMulSum_f8e5m2_RING_SIMPLE,
#else
/* 542*/ nullptr,
#endif
/* 543*/ ncclDevFunc_ReduceScatter_PreMulSum_u32_COLLNET_DIRECT_SIMPLE,
/* 544*/ ncclDevFunc_ReduceScatter_PreMulSum_u32_PAT_SIMPLE,
/* 545*/ ncclDevFunc_ReduceScatter_PreMulSum_u32_RING_LL,
/* 546*/ ncclDevFunc_ReduceScatter_PreMulSum_u32_RING_LL128,
/* 547*/ ncclDevFunc_ReduceScatter_PreMulSum_u32_RING_SIMPLE,
/* 548*/ ncclDevFunc_ReduceScatter_PreMulSum_u64_COLLNET_DIRECT_SIMPLE,
/* 549*/ ncclDevFunc_ReduceScatter_PreMulSum_u64_PAT_SIMPLE,
/* 550*/ ncclDevFunc_ReduceScatter_PreMulSum_u64_RING_LL,
/* 551*/ ncclDevFunc_ReduceScatter_PreMulSum_u64_RING_LL128,
/* 552*/ ncclDevFunc_ReduceScatter_PreMulSum_u64_RING_SIMPLE,
/* 553*/ ncclDevFunc_ReduceScatter_PreMulSum_u8_COLLNET_DIRECT_SIMPLE,
/* 554*/ ncclDevFunc_ReduceScatter_PreMulSum_u8_PAT_SIMPLE,
/* 555*/ ncclDevFunc_ReduceScatter_PreMulSum_u8_RING_LL,
/* 556*/ ncclDevFunc_ReduceScatter_PreMulSum_u8_RING_LL128,
/* 557*/ ncclDevFunc_ReduceScatter_PreMulSum_u8_RING_SIMPLE,
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 558*/ ncclDevFunc_ReduceScatter_Prod_bf16_COLLNET_DIRECT_SIMPLE,
#else
/* 558*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 559*/ ncclDevFunc_ReduceScatter_Prod_bf16_PAT_SIMPLE,
#else
/* 559*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 560*/ ncclDevFunc_ReduceScatter_Prod_bf16_RING_LL,
#else
/* 560*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 561*/ ncclDevFunc_ReduceScatter_Prod_bf16_RING_LL128,
#else
/* 561*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 562*/ ncclDevFunc_ReduceScatter_Prod_bf16_RING_SIMPLE,
#else
/* 562*/ nullptr,
#endif
/* 563*/ ncclDevFunc_ReduceScatter_Prod_f16_COLLNET_DIRECT_SIMPLE,
/* 564*/ ncclDevFunc_ReduceScatter_Prod_f16_PAT_SIMPLE,
/* 565*/ ncclDevFunc_ReduceScatter_Prod_f16_RING_LL,
/* 566*/ ncclDevFunc_ReduceScatter_Prod_f16_RING_LL128,
/* 567*/ ncclDevFunc_ReduceScatter_Prod_f16_RING_SIMPLE,
/* 568*/ ncclDevFunc_ReduceScatter_Prod_f32_COLLNET_DIRECT_SIMPLE,
/* 569*/ ncclDevFunc_ReduceScatter_Prod_f32_PAT_SIMPLE,
/* 570*/ ncclDevFunc_ReduceScatter_Prod_f32_RING_LL,
/* 571*/ ncclDevFunc_ReduceScatter_Prod_f32_RING_LL128,
/* 572*/ ncclDevFunc_ReduceScatter_Prod_f32_RING_SIMPLE,
/* 573*/ ncclDevFunc_ReduceScatter_Prod_f64_COLLNET_DIRECT_SIMPLE,
/* 574*/ ncclDevFunc_ReduceScatter_Prod_f64_PAT_SIMPLE,
/* 575*/ ncclDevFunc_ReduceScatter_Prod_f64_RING_LL,
/* 576*/ ncclDevFunc_ReduceScatter_Prod_f64_RING_LL128,
/* 577*/ ncclDevFunc_ReduceScatter_Prod_f64_RING_SIMPLE,
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 578*/ ncclDevFunc_ReduceScatter_Prod_f8e4m3_COLLNET_DIRECT_SIMPLE,
#else
/* 578*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 579*/ ncclDevFunc_ReduceScatter_Prod_f8e4m3_PAT_SIMPLE,
#else
/* 579*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 580*/ ncclDevFunc_ReduceScatter_Prod_f8e4m3_RING_LL,
#else
/* 580*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 581*/ ncclDevFunc_ReduceScatter_Prod_f8e4m3_RING_LL128,
#else
/* 581*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 582*/ ncclDevFunc_ReduceScatter_Prod_f8e4m3_RING_SIMPLE,
#else
/* 582*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 583*/ ncclDevFunc_ReduceScatter_Prod_f8e5m2_COLLNET_DIRECT_SIMPLE,
#else
/* 583*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 584*/ ncclDevFunc_ReduceScatter_Prod_f8e5m2_PAT_SIMPLE,
#else
/* 584*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 585*/ ncclDevFunc_ReduceScatter_Prod_f8e5m2_RING_LL,
#else
/* 585*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 586*/ ncclDevFunc_ReduceScatter_Prod_f8e5m2_RING_LL128,
#else
/* 586*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 587*/ ncclDevFunc_ReduceScatter_Prod_f8e5m2_RING_SIMPLE,
#else
/* 587*/ nullptr,
#endif
/* 588*/ ncclDevFunc_ReduceScatter_Prod_u32_COLLNET_DIRECT_SIMPLE,
/* 589*/ ncclDevFunc_ReduceScatter_Prod_u32_PAT_SIMPLE,
/* 590*/ ncclDevFunc_ReduceScatter_Prod_u32_RING_LL,
/* 591*/ ncclDevFunc_ReduceScatter_Prod_u32_RING_LL128,
/* 592*/ ncclDevFunc_ReduceScatter_Prod_u32_RING_SIMPLE,
/* 593*/ ncclDevFunc_ReduceScatter_Prod_u64_COLLNET_DIRECT_SIMPLE,
/* 594*/ ncclDevFunc_ReduceScatter_Prod_u64_PAT_SIMPLE,
/* 595*/ ncclDevFunc_ReduceScatter_Prod_u64_RING_LL,
/* 596*/ ncclDevFunc_ReduceScatter_Prod_u64_RING_LL128,
/* 597*/ ncclDevFunc_ReduceScatter_Prod_u64_RING_SIMPLE,
/* 598*/ ncclDevFunc_ReduceScatter_Prod_u8_COLLNET_DIRECT_SIMPLE,
/* 599*/ ncclDevFunc_ReduceScatter_Prod_u8_PAT_SIMPLE,
/* 600*/ ncclDevFunc_ReduceScatter_Prod_u8_RING_LL,
/* 601*/ ncclDevFunc_ReduceScatter_Prod_u8_RING_LL128,
/* 602*/ ncclDevFunc_ReduceScatter_Prod_u8_RING_SIMPLE,
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 603*/ ncclDevFunc_ReduceScatter_Sum_bf16_COLLNET_DIRECT_SIMPLE,
#else
/* 603*/ nullptr,
#endif
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 604*/ ncclDevFunc_ReduceScatter_Sum_bf16_NVLS_SIMPLE,
#else
/* 604*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 605*/ ncclDevFunc_ReduceScatter_Sum_bf16_PAT_SIMPLE,
#else
/* 605*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 606*/ ncclDevFunc_ReduceScatter_Sum_bf16_RING_LL,
#else
/* 606*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 607*/ ncclDevFunc_ReduceScatter_Sum_bf16_RING_LL128,
#else
/* 607*/ nullptr,
#endif
#if CUDART_VERSION >= 11000 && __CUDA_ARCH__ >= 0
/* 608*/ ncclDevFunc_ReduceScatter_Sum_bf16_RING_SIMPLE,
#else
/* 608*/ nullptr,
#endif
/* 609*/ ncclDevFunc_ReduceScatter_Sum_f16_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 610*/ ncclDevFunc_ReduceScatter_Sum_f16_NVLS_SIMPLE,
#else
/* 610*/ nullptr,
#endif
/* 611*/ ncclDevFunc_ReduceScatter_Sum_f16_PAT_SIMPLE,
/* 612*/ ncclDevFunc_ReduceScatter_Sum_f16_RING_LL,
/* 613*/ ncclDevFunc_ReduceScatter_Sum_f16_RING_LL128,
/* 614*/ ncclDevFunc_ReduceScatter_Sum_f16_RING_SIMPLE,
/* 615*/ ncclDevFunc_ReduceScatter_Sum_f32_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 616*/ ncclDevFunc_ReduceScatter_Sum_f32_NVLS_SIMPLE,
#else
/* 616*/ nullptr,
#endif
/* 617*/ ncclDevFunc_ReduceScatter_Sum_f32_PAT_SIMPLE,
/* 618*/ ncclDevFunc_ReduceScatter_Sum_f32_RING_LL,
/* 619*/ ncclDevFunc_ReduceScatter_Sum_f32_RING_LL128,
/* 620*/ ncclDevFunc_ReduceScatter_Sum_f32_RING_SIMPLE,
/* 621*/ ncclDevFunc_ReduceScatter_Sum_f64_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 622*/ ncclDevFunc_ReduceScatter_Sum_f64_NVLS_SIMPLE,
#else
/* 622*/ nullptr,
#endif
/* 623*/ ncclDevFunc_ReduceScatter_Sum_f64_PAT_SIMPLE,
/* 624*/ ncclDevFunc_ReduceScatter_Sum_f64_RING_LL,
/* 625*/ ncclDevFunc_ReduceScatter_Sum_f64_RING_LL128,
/* 626*/ ncclDevFunc_ReduceScatter_Sum_f64_RING_SIMPLE,
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 627*/ ncclDevFunc_ReduceScatter_Sum_f8e4m3_COLLNET_DIRECT_SIMPLE,
#else
/* 627*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 628*/ ncclDevFunc_ReduceScatter_Sum_f8e4m3_PAT_SIMPLE,
#else
/* 628*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 629*/ ncclDevFunc_ReduceScatter_Sum_f8e4m3_RING_LL,
#else
/* 629*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 630*/ ncclDevFunc_ReduceScatter_Sum_f8e4m3_RING_LL128,
#else
/* 630*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 631*/ ncclDevFunc_ReduceScatter_Sum_f8e4m3_RING_SIMPLE,
#else
/* 631*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 632*/ ncclDevFunc_ReduceScatter_Sum_f8e5m2_COLLNET_DIRECT_SIMPLE,
#else
/* 632*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 633*/ ncclDevFunc_ReduceScatter_Sum_f8e5m2_PAT_SIMPLE,
#else
/* 633*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 634*/ ncclDevFunc_ReduceScatter_Sum_f8e5m2_RING_LL,
#else
/* 634*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 635*/ ncclDevFunc_ReduceScatter_Sum_f8e5m2_RING_LL128,
#else
/* 635*/ nullptr,
#endif
#if CUDART_VERSION >= 11080 && __CUDA_ARCH__ >= 900
/* 636*/ ncclDevFunc_ReduceScatter_Sum_f8e5m2_RING_SIMPLE,
#else
/* 636*/ nullptr,
#endif
/* 637*/ ncclDevFunc_ReduceScatter_Sum_u32_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 638*/ ncclDevFunc_ReduceScatter_Sum_u32_NVLS_SIMPLE,
#else
/* 638*/ nullptr,
#endif
/* 639*/ ncclDevFunc_ReduceScatter_Sum_u32_PAT_SIMPLE,
/* 640*/ ncclDevFunc_ReduceScatter_Sum_u32_RING_LL,
/* 641*/ ncclDevFunc_ReduceScatter_Sum_u32_RING_LL128,
/* 642*/ ncclDevFunc_ReduceScatter_Sum_u32_RING_SIMPLE,
/* 643*/ ncclDevFunc_ReduceScatter_Sum_u64_COLLNET_DIRECT_SIMPLE,
#if CUDART_VERSION >= 12010 && __CUDA_ARCH__ >= 900
/* 644*/ ncclDevFunc_ReduceScatter_Sum_u64_NVLS_SIMPLE,
#else
/* 644*/ nullptr,
#endif
/* 645*/ ncclDevFunc_ReduceScatter_Sum_u64_PAT_SIMPLE,
/* 646*/ ncclDevFunc_ReduceScatter_Sum_u64_RING_LL,
/* 647*/ ncclDevFunc_ReduceScatter_Sum_u64_RING_LL128,
/* 648*/ ncclDevFunc_ReduceScatter_Sum_u64_RING_SIMPLE,
/* 649*/ ncclDevFunc_ReduceScatter_Sum_u8_COLLNET_DIRECT_SIMPLE,
/* 650*/ ncclDevFunc_ReduceScatter_Sum_u8_PAT_SIMPLE,
/* 651*/ ncclDevFunc_ReduceScatter_Sum_u8_RING_LL,
/* 652*/ ncclDevFunc_ReduceScatter_Sum_u8_RING_LL128,
/* 653*/ ncclDevFunc_ReduceScatter_Sum_u8_RING_SIMPLE,
/* 654*/ ncclDevFunc_ReduceScatter_SumPostDiv_u32_COLLNET_DIRECT_SIMPLE,
/* 655*/ ncclDevFunc_ReduceScatter_SumPostDiv_u32_PAT_SIMPLE,
/* 656*/ ncclDevFunc_ReduceScatter_SumPostDiv_u32_RING_LL,
/* 657*/ ncclDevFunc_ReduceScatter_SumPostDiv_u32_RING_LL128,
/* 658*/ ncclDevFunc_ReduceScatter_SumPostDiv_u32_RING_SIMPLE,
/* 659*/ ncclDevFunc_ReduceScatter_SumPostDiv_u64_COLLNET_DIRECT_SIMPLE,
/* 660*/ ncclDevFunc_ReduceScatter_SumPostDiv_u64_PAT_SIMPLE,
/* 661*/ ncclDevFunc_ReduceScatter_SumPostDiv_u64_RING_LL,
/* 662*/ ncclDevFunc_ReduceScatter_SumPostDiv_u64_RING_LL128,
/* 663*/ ncclDevFunc_ReduceScatter_SumPostDiv_u64_RING_SIMPLE,
/* 664*/ ncclDevFunc_ReduceScatter_SumPostDiv_u8_COLLNET_DIRECT_SIMPLE,
/* 665*/ ncclDevFunc_ReduceScatter_SumPostDiv_u8_PAT_SIMPLE,
/* 666*/ ncclDevFunc_ReduceScatter_SumPostDiv_u8_RING_LL,
/* 667*/ ncclDevFunc_ReduceScatter_SumPostDiv_u8_RING_LL128,
/* 668*/ ncclDevFunc_ReduceScatter_SumPostDiv_u8_RING_SIMPLE,
/* 669*/ ncclDevFunc_SendRecv,
nullptr};

// Workaround for https://reviews.llvm.org/D55580
__device__ void ncclWorkaroundClangD55580() {}
