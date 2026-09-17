#pragma once

#include "float16.h"

#include <library/cpp/dot_product/common.h>

#include <util/system/compiler.h>

#include <cmath>

namespace NVectorDistance::NSimple {
    template <typename T>
    Y_FORCE_INLINE float L1(const T* lhs, const T* rhs, size_t n) noexcept {
        float sum = 0.f;
        for (size_t i = 0; i < n; ++i) {
            sum += std::abs(static_cast<float>(lhs[i]) - static_cast<float>(rhs[i]));
        }
        return sum;
    }

    template <typename T>
    Y_FORCE_INLINE float L2Sqr(const T* lhs, const T* rhs, size_t n) noexcept {
        float sum = 0.f;
        for (size_t i = 0; i < n; ++i) {
            const float d = static_cast<float>(lhs[i]) - static_cast<float>(rhs[i]);
            sum += d * d;
        }
        return sum;
    }

    template <typename T>
    Y_FORCE_INLINE float Dot(const T* lhs, const T* rhs, size_t n) noexcept {
        float sum = 0.f;
        for (size_t i = 0; i < n; ++i) {
            sum += static_cast<float>(lhs[i]) * static_cast<float>(rhs[i]);
        }
        return sum;
    }

    template <typename T>
    Y_FORCE_INLINE TTriWayDotProduct<float> TriWay(const T* lhs, const T* rhs, size_t n) noexcept {
        TTriWayDotProduct<float> res;
        res.LL = 0.f;
        res.LR = 0.f;
        res.RR = 0.f;
        for (size_t i = 0; i < n; ++i) {
            const float a = static_cast<float>(lhs[i]);
            const float b = static_cast<float>(rhs[i]);
            res.LL += a * a;
            res.LR += a * b;
            res.RR += b * b;
        }
        return res;
    }
}
