#include "ops_avx2.h"

#include <fastops/core/FastIntrinsics.h>

namespace NFastOps {
    template <bool I_Exact, bool I_OutAligned>
    void ExpAvx2(const float* from, size_t size, float* to) {
        NFastOps::AVXExp<I_Exact, I_OutAligned>(from, size, to);
    }

    template <bool I_Exact, bool I_OutAligned>
    void ExpAvx2(const double* from, size_t size, double* to) {
        NFastOps::AVXExp<I_Exact, I_OutAligned>(from, size, to);
    }

    template <bool I_Exact, bool I_OutAligned>
    void LogAvx2(const float* from, size_t size, float* to) {
        NFastOps::AVXLn<I_Exact, I_OutAligned>(from, size, to);
    }

    template <bool I_Exact, bool I_OutAligned>
    void LogAvx2(const double* from, size_t size, double* to) {
        NFastOps::AVXLn<I_Exact, I_OutAligned>(from, size, to);
    }

    template <bool I_Exact, bool I_OutAligned>
    void SigmoidAvx2(const float* from, size_t size, float* to) {
        NFastOps::AVXSigmoid<I_Exact, I_OutAligned>(from, size, to);
    }

    template <bool I_Exact, bool I_OutAligned>
    void SigmoidAvx2(const double* from, size_t size, double* to) {
        NFastOps::AVXSigmoid<I_Exact, I_OutAligned>(from, size, to);
    }

    template <bool I_Exact, bool I_OutAligned>
    void TanhAvx2(const float* from, size_t size, float* to) {
        NFastOps::AVXTanh<I_Exact, I_OutAligned>(from, size, to);
    }

    template <bool I_Exact, bool I_OutAligned>
    void TanhAvx2(const double* from, size_t size, double* to) {
        NFastOps::AVXTanh<I_Exact, I_OutAligned>(from, size, to);
    }

    template void ExpAvx2<false, false>(const float* from, size_t size, float* to);
    template void ExpAvx2<false, true>(const float* from, size_t size, float* to);
    template void ExpAvx2<true, false>(const float* from, size_t size, float* to);
    template void ExpAvx2<true, true>(const float* from, size_t size, float* to);

    template void ExpAvx2<false, false>(const double* from, size_t size, double* to);
    template void ExpAvx2<false, true>(const double* from, size_t size, double* to);
    template void ExpAvx2<true, false>(const double* from, size_t size, double* to);
    template void ExpAvx2<true, true>(const double* from, size_t size, double* to);

    template void LogAvx2<false, false>(const float* from, size_t size, float* to);
    template void LogAvx2<false, true>(const float* from, size_t size, float* to);
    template void LogAvx2<true, false>(const float* from, size_t size, float* to);
    template void LogAvx2<true, true>(const float* from, size_t size, float* to);

    template void LogAvx2<false, false>(const double* from, size_t size, double* to);
    template void LogAvx2<false, true>(const double* from, size_t size, double* to);
    template void LogAvx2<true, false>(const double* from, size_t size, double* to);
    template void LogAvx2<true, true>(const double* from, size_t size, double* to);

    template void SigmoidAvx2<false, false>(const float* from, size_t size, float* to);
    template void SigmoidAvx2<false, true>(const float* from, size_t size, float* to);
    template void SigmoidAvx2<true, false>(const float* from, size_t size, float* to);
    template void SigmoidAvx2<true, true>(const float* from, size_t size, float* to);

    template void SigmoidAvx2<false, false>(const double* from, size_t size, double* to);
    template void SigmoidAvx2<false, true>(const double* from, size_t size, double* to);
    template void SigmoidAvx2<true, false>(const double* from, size_t size, double* to);
    template void SigmoidAvx2<true, true>(const double* from, size_t size, double* to);

    template void TanhAvx2<false, false>(const float* from, size_t size, float* to);
    template void TanhAvx2<false, true>(const float* from, size_t size, float* to);
    template void TanhAvx2<true, false>(const float* from, size_t size, float* to);
    template void TanhAvx2<true, true>(const float* from, size_t size, float* to);

    template void TanhAvx2<false, false>(const double* from, size_t size, double* to);
    template void TanhAvx2<false, true>(const double* from, size_t size, double* to);
    template void TanhAvx2<true, false>(const double* from, size_t size, double* to);
    template void TanhAvx2<true, true>(const double* from, size_t size, double* to);
}
