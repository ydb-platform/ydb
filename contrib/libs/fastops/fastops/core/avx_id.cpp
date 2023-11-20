#include "avx_id.h"

#if defined(__x86_64__) || defined(_M_X64) || defined(_M_AMD64) || defined(__i386__) || defined(_M_IX86)

#include <stdint.h>

#if defined(_WIN32)
#include <intrin.h>
#else
#include <cpuid.h>
#endif

namespace NFastOps {
    namespace NDetail {
        // YMM XSAVE/XRESTORE OS support should be checked in order to use AVX
        // http://www.intel.com/content/dam/www/public/us/en/documents/manuals/64-ia-32-architectures-optimization-manual.pdf
        static bool IsYMMSaveEnabled() noexcept {
#if !defined(_WIN32)
            int32_t eax, edx;
            __asm__ volatile(
                "xgetbv"
                : "=a"(eax), "=d"(edx)
                : "c"(0));
            return (eax & 6u) == 6u;
#else
            return (_xgetbv(0) & 6u) == 6u;
#endif
        }

        enum { EAX = 0, EBX, ECX, EDX, NREGS };

        // These are in ECX of cpuid(1)
        static constexpr uint32_t OSXSAVE_BIT = 0x04000000;
        static constexpr uint32_t AVX_BIT     = 0x10000000;

        // This in EBX of cpuid(7)
        static constexpr uint32_t AVX2_BIT    = 0x00000020;

        static void CpuId(int32_t op, int32_t* res) noexcept {
#if defined(_MSC_VER)
            __cpuidex((int*)res, op, 0);
#else
            __cpuid_count(op, 0, res[EAX], res[EBX], res[ECX], res[EDX]);
#endif
        }

        bool IsAVXEnabled() noexcept {
            int32_t info[NREGS];
            CpuId(1, info);
            return (info[ECX] & OSXSAVE_BIT) && IsYMMSaveEnabled() && (info[ECX] & AVX_BIT);
        }

        bool IsAVX2Enabled() noexcept {
            int32_t info[NREGS];
            CpuId(7, info);
            return info[EBX] & AVX2_BIT;
        }
    }
}
#else
namespace NFastOps {
    namespace NDetail {
        bool IsAVXEnabled() noexcept {
            return false;
        }

        bool IsAVX2Enabled() noexcept {
            return false;
        }
    }
}
#endif
