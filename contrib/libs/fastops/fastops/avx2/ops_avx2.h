#pragma once

#include <cstddef>

namespace NFastOps {
    template <bool I_Exact = false, bool I_OutAligned = false>
    void ExpAvx2(const float* from, size_t size, float* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void ExpAvx2(const double* from, size_t size, double* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void LogAvx2(const float* from, size_t size, float* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void LogAvx2(const double* from, size_t size, double* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void SigmoidAvx2(const float* from, size_t size, float* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void SigmoidAvx2(const double* from, size_t size, double* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void TanhAvx2(const float* from, size_t size, float* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void TanhAvx2(const double* from, size_t size, double* to);
}
