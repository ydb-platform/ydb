#include "ops_avx.h"

#include <fastops/core/FastIntrinsics.h>

namespace NFastOps {
    template <bool I_Exact, bool I_OutAligned>
    void ExpAvx(const float* from, size_t size, float* to) {
        NFastOps::AVXExp<I_Exact, I_OutAligned>(from, size, to);
    }

    template <bool I_Exact, bool I_OutAligned>
    void ExpAvx(const double* from, size_t size, double* to) {
        NFastOps::AVXExp<I_Exact, I_OutAligned>(from, size, to);
    }

    template <bool I_Exact, bool I_OutAligned>
    void LogAvx(const float* from, size_t size, float* to) {
        NFastOps::AVXLn<I_Exact, I_OutAligned>(from, size, to);
    }

    template <bool I_Exact, bool I_OutAligned>
    void LogAvx(const double* from, size_t size, double* to) {
        NFastOps::AVXLn<I_Exact, I_OutAligned>(from, size, to);
    }

    template <bool I_Exact, bool I_OutAligned>
    void SigmoidAvx(const float* from, size_t size, float* to) {
        NFastOps::AVXSigmoid<I_Exact, I_OutAligned>(from, size, to);
    }

    template <bool I_Exact, bool I_OutAligned>
    void SigmoidAvx(const double* from, size_t size, double* to) {
        NFastOps::AVXSigmoid<I_Exact, I_OutAligned>(from, size, to);
    }

    template <bool I_Exact, bool I_OutAligned>
    void TanhAvx(const float* from, size_t size, float* to) {
        NFastOps::AVXTanh<I_Exact, I_OutAligned>(from, size, to);
    }

    template <bool I_Exact, bool I_OutAligned>
    void TanhAvx(const double* from, size_t size, double* to) {
        NFastOps::AVXTanh<I_Exact, I_OutAligned>(from, size, to);
    }

    template void ExpAvx<false, false>(const float* from, size_t size, float* to);
    template void ExpAvx<false, true>(const float* from, size_t size, float* to);
    template void ExpAvx<true, false>(const float* from, size_t size, float* to);
    template void ExpAvx<true, true>(const float* from, size_t size, float* to);

    template void ExpAvx<false, false>(const double* from, size_t size, double* to);
    template void ExpAvx<false, true>(const double* from, size_t size, double* to);
    template void ExpAvx<true, false>(const double* from, size_t size, double* to);
    template void ExpAvx<true, true>(const double* from, size_t size, double* to);

    template void LogAvx<false, false>(const float* from, size_t size, float* to);
    template void LogAvx<false, true>(const float* from, size_t size, float* to);
    template void LogAvx<true, false>(const float* from, size_t size, float* to);
    template void LogAvx<true, true>(const float* from, size_t size, float* to);

    template void LogAvx<false, false>(const double* from, size_t size, double* to);
    template void LogAvx<false, true>(const double* from, size_t size, double* to);
    template void LogAvx<true, false>(const double* from, size_t size, double* to);
    template void LogAvx<true, true>(const double* from, size_t size, double* to);

    template void SigmoidAvx<false, false>(const float* from, size_t size, float* to);
    template void SigmoidAvx<false, true>(const float* from, size_t size, float* to);
    template void SigmoidAvx<true, false>(const float* from, size_t size, float* to);
    template void SigmoidAvx<true, true>(const float* from, size_t size, float* to);

    template void SigmoidAvx<false, false>(const double* from, size_t size, double* to);
    template void SigmoidAvx<false, true>(const double* from, size_t size, double* to);
    template void SigmoidAvx<true, false>(const double* from, size_t size, double* to);
    template void SigmoidAvx<true, true>(const double* from, size_t size, double* to);

    template void TanhAvx<false, false>(const double* from, size_t size, double* to);
    template void TanhAvx<false, true>(const double* from, size_t size, double* to);
    template void TanhAvx<true, false>(const double* from, size_t size, double* to);
    template void TanhAvx<true, true>(const double* from, size_t size, double* to);

    template void TanhAvx<false, false>(const float* from, size_t size, float* to);
    template void TanhAvx<false, true>(const float* from, size_t size, float* to);
    template void TanhAvx<true, false>(const float* from, size_t size, float* to);
    template void TanhAvx<true, true>(const float* from, size_t size, float* to);
}
