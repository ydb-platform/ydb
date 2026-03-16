#pragma once

#include <cstddef>

namespace NFastOps {
    template <bool I_Exact = false, bool I_OutAligned = false>
    void ExpAvx(const float* from, size_t size, float* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void ExpAvx(const double* from, size_t size, double* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void LogAvx(const float* from, size_t size, float* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void LogAvx(const double* from, size_t size, double* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void SigmoidAvx(const float* from, size_t size, float* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void SigmoidAvx(const double* from, size_t size, double* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void TanhAvx(const float* from, size_t size, float* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void TanhAvx(const double* from, size_t size, double* to);
}
