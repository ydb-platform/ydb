#include "symmetric.h"
#include "device.h"

__global__ void ncclSymDevKernel_AllGather_LL(ncclSymDevArgs const);
__global__ void ncclSymDevKernel_AllGather_LLMC(ncclSymDevArgs const);
__global__ void ncclSymDevKernel_AllGather_ST(ncclSymDevArgs const);
__global__ void ncclSymDevKernel_AllGather_STMC(ncclSymDevArgs const);
__global__ void ncclSymDevKernel_AllReduce_AGxLL_R_sum_f32(ncclSymDevArgs const);
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f32(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f32 = nullptr;
#endif
__global__ void ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_f32(ncclSymDevArgs const);
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f32(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f32 = nullptr;
#endif
__global__ void ncclSymDevKernel_ReduceScatter_LL_sum_f32(ncclSymDevArgs const);
__global__ void ncclSymDevKernel_ReduceScatter_LD_sum_f32(ncclSymDevArgs const);
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_ReduceScatter_LDMC_sum_f32(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_ReduceScatter_LDMC_sum_f32 = nullptr;
#endif
__global__ void ncclSymDevKernel_AllReduce_AGxLL_R_sum_f16(ncclSymDevArgs const);
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f16(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f16 = nullptr;
#endif
__global__ void ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_f16(ncclSymDevArgs const);
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f16(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f16 = nullptr;
#endif
__global__ void ncclSymDevKernel_ReduceScatter_LL_sum_f16(ncclSymDevArgs const);
__global__ void ncclSymDevKernel_ReduceScatter_LD_sum_f16(ncclSymDevArgs const);
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_ReduceScatter_LDMC_sum_f16(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_ReduceScatter_LDMC_sum_f16 = nullptr;
#endif
#if CUDART_VERSION >= 11000
  __global__ void ncclSymDevKernel_AllReduce_AGxLL_R_sum_bf16(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_AllReduce_AGxLL_R_sum_bf16 = nullptr;
#endif
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_bf16(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_bf16 = nullptr;
#endif
#if CUDART_VERSION >= 11000
  __global__ void ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_bf16(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_bf16 = nullptr;
#endif
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_bf16(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_bf16 = nullptr;
#endif
#if CUDART_VERSION >= 11000
  __global__ void ncclSymDevKernel_ReduceScatter_LL_sum_bf16(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_ReduceScatter_LL_sum_bf16 = nullptr;
#endif
#if CUDART_VERSION >= 11000
  __global__ void ncclSymDevKernel_ReduceScatter_LD_sum_bf16(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_ReduceScatter_LD_sum_bf16 = nullptr;
#endif
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_ReduceScatter_LDMC_sum_bf16(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_ReduceScatter_LDMC_sum_bf16 = nullptr;
#endif
#if CUDART_VERSION >= 11080
  __global__ void ncclSymDevKernel_AllReduce_AGxLL_R_sum_f8e4m3(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_AllReduce_AGxLL_R_sum_f8e4m3 = nullptr;
#endif
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f8e4m3(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f8e4m3 = nullptr;
#endif
#if CUDART_VERSION >= 11080
  __global__ void ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_f8e4m3(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_f8e4m3 = nullptr;
#endif
#if CUDART_VERSION >= 12070
  __global__ void ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f8e4m3(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f8e4m3 = nullptr;
#endif
#if CUDART_VERSION >= 11080
  __global__ void ncclSymDevKernel_ReduceScatter_LL_sum_f8e4m3(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_ReduceScatter_LL_sum_f8e4m3 = nullptr;
#endif
#if CUDART_VERSION >= 11080
  __global__ void ncclSymDevKernel_ReduceScatter_LD_sum_f8e4m3(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_ReduceScatter_LD_sum_f8e4m3 = nullptr;
#endif
#if CUDART_VERSION >= 12070
  __global__ void ncclSymDevKernel_ReduceScatter_LDMC_sum_f8e4m3(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_ReduceScatter_LDMC_sum_f8e4m3 = nullptr;
#endif
#if CUDART_VERSION >= 11080
  __global__ void ncclSymDevKernel_AllReduce_AGxLL_R_sum_f8e5m2(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_AllReduce_AGxLL_R_sum_f8e5m2 = nullptr;
#endif
#if CUDART_VERSION >= 12010
  __global__ void ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f8e5m2(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f8e5m2 = nullptr;
#endif
#if CUDART_VERSION >= 11080
  __global__ void ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_f8e5m2(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_f8e5m2 = nullptr;
#endif
#if CUDART_VERSION >= 12070
  __global__ void ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f8e5m2(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f8e5m2 = nullptr;
#endif
#if CUDART_VERSION >= 11080
  __global__ void ncclSymDevKernel_ReduceScatter_LL_sum_f8e5m2(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_ReduceScatter_LL_sum_f8e5m2 = nullptr;
#endif
#if CUDART_VERSION >= 11080
  __global__ void ncclSymDevKernel_ReduceScatter_LD_sum_f8e5m2(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_ReduceScatter_LD_sum_f8e5m2 = nullptr;
#endif
#if CUDART_VERSION >= 12070
  __global__ void ncclSymDevKernel_ReduceScatter_LDMC_sum_f8e5m2(ncclSymDevArgs const);
#else
  constexpr void* ncclSymDevKernel_ReduceScatter_LDMC_sum_f8e5m2 = nullptr;
#endif

extern int const ncclSymKernelCount = 39;
extern void* const ncclSymKernelList[] = {
(void*)ncclSymDevKernel_AllGather_LL,
(void*)ncclSymDevKernel_AllGather_LLMC,
(void*)ncclSymDevKernel_AllGather_ST,
(void*)ncclSymDevKernel_AllGather_STMC,
(void*)ncclSymDevKernel_AllReduce_AGxLL_R_sum_f32,
(void*)ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f32,
(void*)ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_f32,
(void*)ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f32,
(void*)ncclSymDevKernel_ReduceScatter_LL_sum_f32,
(void*)ncclSymDevKernel_ReduceScatter_LD_sum_f32,
(void*)ncclSymDevKernel_ReduceScatter_LDMC_sum_f32,
(void*)ncclSymDevKernel_AllReduce_AGxLL_R_sum_f16,
(void*)ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f16,
(void*)ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_f16,
(void*)ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f16,
(void*)ncclSymDevKernel_ReduceScatter_LL_sum_f16,
(void*)ncclSymDevKernel_ReduceScatter_LD_sum_f16,
(void*)ncclSymDevKernel_ReduceScatter_LDMC_sum_f16,
(void*)ncclSymDevKernel_AllReduce_AGxLL_R_sum_bf16,
(void*)ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_bf16,
(void*)ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_bf16,
(void*)ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_bf16,
(void*)ncclSymDevKernel_ReduceScatter_LL_sum_bf16,
(void*)ncclSymDevKernel_ReduceScatter_LD_sum_bf16,
(void*)ncclSymDevKernel_ReduceScatter_LDMC_sum_bf16,
(void*)ncclSymDevKernel_AllReduce_AGxLL_R_sum_f8e4m3,
(void*)ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f8e4m3,
(void*)ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_f8e4m3,
(void*)ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f8e4m3,
(void*)ncclSymDevKernel_ReduceScatter_LL_sum_f8e4m3,
(void*)ncclSymDevKernel_ReduceScatter_LD_sum_f8e4m3,
(void*)ncclSymDevKernel_ReduceScatter_LDMC_sum_f8e4m3,
(void*)ncclSymDevKernel_AllReduce_AGxLL_R_sum_f8e5m2,
(void*)ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f8e5m2,
(void*)ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_f8e5m2,
(void*)ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f8e5m2,
(void*)ncclSymDevKernel_ReduceScatter_LL_sum_f8e5m2,
(void*)ncclSymDevKernel_ReduceScatter_LD_sum_f8e5m2,
(void*)ncclSymDevKernel_ReduceScatter_LDMC_sum_f8e5m2,
nullptr};

void* ncclSymGetKernelPtr(ncclSymKernelId id, int red, ncclDataType_t ty) {
  switch (id) {
  default: return nullptr;
  case ncclSymKernelId_AllGather_LL:
    return (void*)&ncclSymDevKernel_AllGather_LL;
  case ncclSymKernelId_AllGather_LLMC:
    return (void*)&ncclSymDevKernel_AllGather_LLMC;
  case ncclSymKernelId_AllGather_ST:
    return (void*)&ncclSymDevKernel_AllGather_ST;
  case ncclSymKernelId_AllGather_STMC:
    return (void*)&ncclSymDevKernel_AllGather_STMC;
  case ncclSymKernelId_AllReduce_AGxLL_R:
    switch ((ncclDevRedOp_t)red) {
    default: return nullptr;
    case ncclDevSum:
      switch (ty) {
      default: return nullptr;
      case ncclFloat32: return (void*)ncclSymDevKernel_AllReduce_AGxLL_R_sum_f32;
      case ncclFloat16: return (void*)ncclSymDevKernel_AllReduce_AGxLL_R_sum_f16;
      case ncclBfloat16: return (void*)ncclSymDevKernel_AllReduce_AGxLL_R_sum_bf16;
      case ncclFloat8e4m3: return (void*)ncclSymDevKernel_AllReduce_AGxLL_R_sum_f8e4m3;
      case ncclFloat8e5m2: return (void*)ncclSymDevKernel_AllReduce_AGxLL_R_sum_f8e5m2;
      }
    }
  case ncclSymKernelId_AllReduce_AGxLLMC_R:
    switch ((ncclDevRedOp_t)red) {
    default: return nullptr;
    case ncclDevSum:
      switch (ty) {
      default: return nullptr;
      case ncclFloat32: return (void*)ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f32;
      case ncclFloat16: return (void*)ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f16;
      case ncclBfloat16: return (void*)ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_bf16;
      case ncclFloat8e4m3: return (void*)ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f8e4m3;
      case ncclFloat8e5m2: return (void*)ncclSymDevKernel_AllReduce_AGxLLMC_R_sum_f8e5m2;
      }
    }
  case ncclSymKernelId_AllReduce_RSxLD_AGxST:
    switch ((ncclDevRedOp_t)red) {
    default: return nullptr;
    case ncclDevSum:
      switch (ty) {
      default: return nullptr;
      case ncclFloat32: return (void*)ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_f32;
      case ncclFloat16: return (void*)ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_f16;
      case ncclBfloat16: return (void*)ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_bf16;
      case ncclFloat8e4m3: return (void*)ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_f8e4m3;
      case ncclFloat8e5m2: return (void*)ncclSymDevKernel_AllReduce_RSxLD_AGxST_sum_f8e5m2;
      }
    }
  case ncclSymKernelId_AllReduce_RSxLDMC_AGxSTMC:
    switch ((ncclDevRedOp_t)red) {
    default: return nullptr;
    case ncclDevSum:
      switch (ty) {
      default: return nullptr;
      case ncclFloat32: return (void*)ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f32;
      case ncclFloat16: return (void*)ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f16;
      case ncclBfloat16: return (void*)ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_bf16;
      case ncclFloat8e4m3: return (void*)ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f8e4m3;
      case ncclFloat8e5m2: return (void*)ncclSymDevKernel_AllReduce_RSxLDMC_AGxSTMC_sum_f8e5m2;
      }
    }
  case ncclSymKernelId_ReduceScatter_LL:
    switch ((ncclDevRedOp_t)red) {
    default: return nullptr;
    case ncclDevSum:
      switch (ty) {
      default: return nullptr;
      case ncclFloat32: return (void*)ncclSymDevKernel_ReduceScatter_LL_sum_f32;
      case ncclFloat16: return (void*)ncclSymDevKernel_ReduceScatter_LL_sum_f16;
      case ncclBfloat16: return (void*)ncclSymDevKernel_ReduceScatter_LL_sum_bf16;
      case ncclFloat8e4m3: return (void*)ncclSymDevKernel_ReduceScatter_LL_sum_f8e4m3;
      case ncclFloat8e5m2: return (void*)ncclSymDevKernel_ReduceScatter_LL_sum_f8e5m2;
      }
    }
  case ncclSymKernelId_ReduceScatter_LD:
    switch ((ncclDevRedOp_t)red) {
    default: return nullptr;
    case ncclDevSum:
      switch (ty) {
      default: return nullptr;
      case ncclFloat32: return (void*)ncclSymDevKernel_ReduceScatter_LD_sum_f32;
      case ncclFloat16: return (void*)ncclSymDevKernel_ReduceScatter_LD_sum_f16;
      case ncclBfloat16: return (void*)ncclSymDevKernel_ReduceScatter_LD_sum_bf16;
      case ncclFloat8e4m3: return (void*)ncclSymDevKernel_ReduceScatter_LD_sum_f8e4m3;
      case ncclFloat8e5m2: return (void*)ncclSymDevKernel_ReduceScatter_LD_sum_f8e5m2;
      }
    }
  case ncclSymKernelId_ReduceScatter_LDMC:
    switch ((ncclDevRedOp_t)red) {
    default: return nullptr;
    case ncclDevSum:
      switch (ty) {
      default: return nullptr;
      case ncclFloat32: return (void*)ncclSymDevKernel_ReduceScatter_LDMC_sum_f32;
      case ncclFloat16: return (void*)ncclSymDevKernel_ReduceScatter_LDMC_sum_f16;
      case ncclBfloat16: return (void*)ncclSymDevKernel_ReduceScatter_LDMC_sum_bf16;
      case ncclFloat8e4m3: return (void*)ncclSymDevKernel_ReduceScatter_LDMC_sum_f8e4m3;
      case ncclFloat8e5m2: return (void*)ncclSymDevKernel_ReduceScatter_LDMC_sum_f8e5m2;
      }
    }
  }
}
