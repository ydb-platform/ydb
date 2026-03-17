//  Dependencies
#include "CpuInfo.hpp"
#include <string.h>

#if defined(_WIN32) && (defined(_MSC_VER) || defined(__clang__))
#include <intrin.h>

__forceinline void cpuid(int32_t out[4], int32_t eax, int32_t ecx)
{
    __cpuidex(out, eax, ecx);
}

__forceinline uint64_t xgetbv(unsigned int x)
{
    return _xgetbv(x);
}
#elif defined(__GNUC__) || defined(__clang__)
#include <cpuid.h>

inline void cpuid(int32_t out[4], int32_t eax, int32_t ecx)
{
    __cpuid_count(eax, ecx, out[0], out[1], out[2], out[3]);
}

inline uint64_t xgetbv(unsigned int index)
{
    uint32_t eax, edx;
    __asm__ __volatile__("xgetbv" : "=a"(eax), "=d"(edx) : "c"(index));
    return ((uint64_t)edx << 32) | eax;
}
#endif

bool CpuInfo::detect_OS_AVX()
{
    bool avxSupported = false;
    int cpuInfo[4];
    cpuid(cpuInfo, 1, 0);

    bool osUsesXSAVE_XRSTORE = (cpuInfo[2] & (1 << 27)) != 0;
    bool cpuAVXSuport = (cpuInfo[2] & (1 << 28)) != 0;
    if (osUsesXSAVE_XRSTORE && cpuAVXSuport) {
        uint64_t xcrFeatureMask = xgetbv(0);
        avxSupported = (xcrFeatureMask & 0x6) == 0x6;
    }

    return avxSupported;
}

CpuInfo::CpuInfo() : supportedFeatures(0)
{
    int regs[4];
    cpuid(regs, 0, 0);

    int cpuIdCount = regs[0];
    if (cpuIdCount < 1) return;

    cpuid(regs, 1, 0);

    if ((regs[3] & ((int)1 << 26))) supportedFeatures |= CPU_FEATURE_SSE2;

    if ((regs[2] & ((int)1 << 0))) supportedFeatures |= CPU_FEATURE_SSE3;

    if ((regs[2] & ((int)1 << 9))) supportedFeatures |= CPU_FEATURE_SSSE3;

    if ((regs[2] & ((int)1 << 19))) supportedFeatures |= CPU_FEATURE_SSE4_1;

    if ((regs[2] & ((int)1 << 20))) supportedFeatures |= CPU_FEATURE_SSE4_2;

    if (detect_OS_AVX()) {
        if ((regs[2] & ((int)1 << 28))) supportedFeatures |= CPU_FEATURE_AVX;

        if (cpuIdCount >= 7) {
            cpuid(regs, 0x00000007, 0);

            if ((regs[1] & ((int)1 << 5))) supportedFeatures |= CPU_FEATURE_AVX2;
        }
    }
}
