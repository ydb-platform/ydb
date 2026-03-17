#pragma once

#include <stdint.h>

#define CPU_FEATURE_SSE2 (1 << 0)
#define CPU_FEATURE_SSE3 (1 << 1)
#define CPU_FEATURE_SSSE3 (1 << 2)
#define CPU_FEATURE_SSE4_1 (1 << 3)
#define CPU_FEATURE_SSE4_2 (1 << 4)
#define CPU_FEATURE_AVX (1 << 5)
#define CPU_FEATURE_AVX2 (1 << 7)

struct CpuInfo {
private:
    CpuInfo();

    static const CpuInfo& instance()
    {
        static CpuInfo instance;
        return instance;
    }

public:
    static bool supports(uint32_t features)
    {
        return (instance().supportedFeatures & features) == features;
    }

private:
    static bool detect_OS_AVX();
    uint32_t supportedFeatures;
};
