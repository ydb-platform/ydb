// Copyright 2017 The PDFium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

// Original code copyright 2014 Foxit Software Inc. http://www.foxitsoftware.com

#include "core/fxcrt/fx_random.h"

#include <array>

#include "build/build_config.h"
#include "core/fxcrt/fx_memory.h"
#include "core/fxcrt/fx_string.h"
#include "core/fxcrt/fx_system.h"

#define MT_N 848
#define MT_M 456
#define MT_Matrix_A 0x9908b0df
#define MT_Upper_Mask 0x80000000
#define MT_Lower_Mask 0x7fffffff

#if BUILDFLAG(IS_WIN)
#include <wincrypt.h>
#else
#include <sys/time.h>
#include <unistd.h>
#endif

namespace {

struct MTContext {
  uint32_t mti;
  std::array<uint32_t, MT_N> mt;
};

bool g_bHaveGlobalSeed = false;
uint32_t g_nGlobalSeed = 0;

#if BUILDFLAG(IS_WIN)
bool GenerateSeedFromCryptoRandom(uint32_t* pSeed) {
  HCRYPTPROV hCP = 0;
  if (!::CryptAcquireContext(&hCP, nullptr, nullptr, PROV_RSA_FULL, 0) ||
      !hCP) {
    return false;
  }
  ::CryptGenRandom(hCP, sizeof(uint32_t), reinterpret_cast<uint8_t*>(pSeed));
  ::CryptReleaseContext(hCP, 0);
  return true;
}
#endif

uint32_t GenerateSeedFromEnvironment() {
  char c;
  uintptr_t p = reinterpret_cast<uintptr_t>(&c);
  uint32_t seed = ~static_cast<uint32_t>(p >> 3);
#if BUILDFLAG(IS_WIN)
  SYSTEMTIME st;
  GetSystemTime(&st);
  seed ^= static_cast<uint32_t>(st.wSecond) * 1000000;
  seed ^= static_cast<uint32_t>(st.wMilliseconds) * 1000;
  seed ^= GetCurrentProcessId();
#else
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  seed ^= static_cast<uint32_t>(tv.tv_sec) * 1000000;
  seed ^= static_cast<uint32_t>(tv.tv_usec);
  seed ^= static_cast<uint32_t>(getpid());
#endif
  return seed;
}

void* ContextFromNextGlobalSeed() {
  if (!g_bHaveGlobalSeed) {
#if BUILDFLAG(IS_WIN)
    if (!GenerateSeedFromCryptoRandom(&g_nGlobalSeed))
      g_nGlobalSeed = GenerateSeedFromEnvironment();
#else
    g_nGlobalSeed = GenerateSeedFromEnvironment();
#endif
    g_bHaveGlobalSeed = true;
  }
  return FX_Random_MT_Start(++g_nGlobalSeed);
}

}  // namespace

void* FX_Random_MT_Start(uint32_t dwSeed) {
  MTContext* pContext = FX_Alloc(MTContext, 1);
  pContext->mt[0] = dwSeed;
  for (uint32_t i = 1; i < MT_N; i++) {
    const uint32_t prev = pContext->mt[i - 1];
    pContext->mt[i] = (1812433253UL * (prev ^ (prev >> 30)) + i);
  }
  pContext->mti = MT_N;
  return pContext;
}

uint32_t FX_Random_MT_Generate(void* pContext) {
  MTContext* pMTC = static_cast<MTContext*>(pContext);
  uint32_t v;
  if (pMTC->mti >= MT_N) {
    static constexpr std::array<uint32_t, 2> mag = {{0, MT_Matrix_A}};
    uint32_t kk;
    for (kk = 0; kk < MT_N - MT_M; kk++) {
      v = (pMTC->mt[kk] & MT_Upper_Mask) | (pMTC->mt[kk + 1] & MT_Lower_Mask);
      pMTC->mt[kk] = pMTC->mt[kk + MT_M] ^ (v >> 1) ^ mag[v & 1];
    }
    for (; kk < MT_N - 1; kk++) {
      v = (pMTC->mt[kk] & MT_Upper_Mask) | (pMTC->mt[kk + 1] & MT_Lower_Mask);
      pMTC->mt[kk] = pMTC->mt[kk + (MT_M - MT_N)] ^ (v >> 1) ^ mag[v & 1];
    }
    v = (pMTC->mt[MT_N - 1] & MT_Upper_Mask) | (pMTC->mt[0] & MT_Lower_Mask);
    pMTC->mt[MT_N - 1] = pMTC->mt[MT_M - 1] ^ (v >> 1) ^ mag[v & 1];
    pMTC->mti = 0;
  }
  v = pMTC->mt[pMTC->mti++];
  v ^= (v >> 11);
  v ^= (v << 7) & 0x9d2c5680UL;
  v ^= (v << 15) & 0xefc60000UL;
  v ^= (v >> 18);
  return v;
}

void FX_Random_MT_Close(void* pContext) {
  FX_Free(pContext);
}

void FX_Random_GenerateMT(pdfium::span<uint32_t> pBuffer) {
  void* pContext = ContextFromNextGlobalSeed();
  for (size_t i = 0; i < pBuffer.size(); ++i) {
    pBuffer[i] = FX_Random_MT_Generate(pContext);
  }
  FX_Random_MT_Close(pContext);
}
