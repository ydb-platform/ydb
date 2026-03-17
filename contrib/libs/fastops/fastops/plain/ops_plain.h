#pragma once

#include <cstddef>

namespace NFastOps {
    void ExpPlain(const float* from, size_t size, float* to);
    void ExpPlain(const double* from, size_t size, double* to);

    void LogPlain(const float* from, size_t size, float* to);
    void LogPlain(const double* from, size_t size, double* to);

    void SigmoidPlain(const float* from, size_t size, float* to);
    void SigmoidPlain(const double* from, size_t size, double* to);

    void TanhPlain(const float* from, size_t size, float* to);
    void TanhPlain(const double* from, size_t size, double* to);
}
