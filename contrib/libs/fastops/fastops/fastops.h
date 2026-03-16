#pragma once

#include <cstddef>

namespace NFastOps {
    template <bool I_Exact = false, bool I_OutAligned = false>
    void Exp(const float* from, size_t size, float* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void Exp(const double* from, size_t size, double* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void Log(const float* from, size_t size, float* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void Log(const double* from, size_t size, double* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void Sigmoid(const float* from, size_t size, float* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void Sigmoid(const double* from, size_t size, double* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void Tanh(const float* from, size_t size, float* to);

    template <bool I_Exact = false, bool I_OutAligned = false>
    void Tanh(const double* from, size_t size, double* to);
}
