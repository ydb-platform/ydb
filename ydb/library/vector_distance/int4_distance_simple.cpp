#include "int4_distance_simple.h"

#include <cmath>

namespace NVectorDistance::NInt4::NSimple {
namespace {
    template <bool Signed>
    TTriWayDotProduct<float> Compute(const ui8* lhs, const ui8* rhs, size_t length, float& l1, float& l2) {
        TTriWayDotProduct<float> result{0, 0, 0};
        for (size_t i = 0; i < length; ++i) {
            const int a = NPrivate::TInt4<Signed>::FromBits(lhs[i / 2] >> (4 * (i % 2)));
            const int b = NPrivate::TInt4<Signed>::FromBits(rhs[i / 2] >> (4 * (i % 2)));
            l1 += std::abs(a - b);
            l2 += (a - b) * (a - b);
            result.LL += a * a;
            result.LR += a * b;
            result.RR += b * b;
        }
        return result;
    }
}

float L1Distance(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<false>) noexcept {
    float l1 = 0, l2 = 0;
    [[maybe_unused]] const auto dots = Compute<false>(lhs, rhs, length, l1, l2);
    return l1;
}

float L2SqrDistance(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<false>) noexcept {
    float l1 = 0, l2 = 0;
    [[maybe_unused]] const auto dots = Compute<false>(lhs, rhs, length, l1, l2);
    return l2;
}

float DotProduct(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<false>) noexcept {
    float l1 = 0, l2 = 0;
    [[maybe_unused]] const auto dots = Compute<false>(lhs, rhs, length, l1, l2);
    return dots.LR;
}

TTriWayDotProduct<float> TriWayDotProduct(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<false>) noexcept {
    float l1 = 0, l2 = 0;
    [[maybe_unused]] const auto dots = Compute<false>(lhs, rhs, length, l1, l2);
    return dots;
}

float L1Distance(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<true>) noexcept {
    float l1 = 0, l2 = 0;
    [[maybe_unused]] const auto dots = Compute<true>(lhs, rhs, length, l1, l2);
    return l1;
}

float L2SqrDistance(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<true>) noexcept {
    float l1 = 0, l2 = 0;
    [[maybe_unused]] const auto dots = Compute<true>(lhs, rhs, length, l1, l2);
    return l2;
}

float DotProduct(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<true>) noexcept {
    float l1 = 0, l2 = 0;
    [[maybe_unused]] const auto dots = Compute<true>(lhs, rhs, length, l1, l2);
    return dots.LR;
}

TTriWayDotProduct<float> TriWayDotProduct(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<true>) noexcept {
    float l1 = 0, l2 = 0;
    [[maybe_unused]] const auto dots = Compute<true>(lhs, rhs, length, l1, l2);
    return dots;
}

} // namespace NVectorDistance::NInt4::NSimple
