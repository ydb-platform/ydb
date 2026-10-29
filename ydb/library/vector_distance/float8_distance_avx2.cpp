#include "float8_distance_avx2.h"
#include "float8_distance_avx2_impl.h"
#include "float8_distance_simd.h"

namespace NVectorDistance::NFloat8::NAvx2 {

#if defined(_avx2_)
using NPrivate::TOps;

float L1Distance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return Distance<EMetric::L1, TFloat8E4M3, TOps>(lhs, rhs, length);
}

float L2SqrDistance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return Distance<EMetric::L2Sqr, TFloat8E4M3, TOps>(lhs, rhs, length);
}

float DotProduct(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return Distance<EMetric::Dot, TFloat8E4M3, TOps>(lhs, rhs, length);
}

TTriWayDotProduct<float> TriWayDotProduct(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return TriWay<TFloat8E4M3, TOps>(lhs, rhs, length);
}

float L1Distance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return Distance<EMetric::L1, TFloat8E5M2, TOps>(lhs, rhs, length);
}

float L2SqrDistance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return Distance<EMetric::L2Sqr, TFloat8E5M2, TOps>(lhs, rhs, length);
}

float DotProduct(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return Distance<EMetric::Dot, TFloat8E5M2, TOps>(lhs, rhs, length);
}

TTriWayDotProduct<float> TriWayDotProduct(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return TriWay<TFloat8E5M2, TOps>(lhs, rhs, length);
}

#else

float L1Distance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return NSimple::L1(lhs, rhs, length);
}

float L2SqrDistance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return NSimple::L2Sqr(lhs, rhs, length);
}

float DotProduct(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return NSimple::Dot(lhs, rhs, length);
}

TTriWayDotProduct<float> TriWayDotProduct(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return NSimple::TriWay(lhs, rhs, length);
}

float L1Distance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return NSimple::L1(lhs, rhs, length);
}

float L2SqrDistance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return NSimple::L2Sqr(lhs, rhs, length);
}

float DotProduct(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return NSimple::Dot(lhs, rhs, length);
}

TTriWayDotProduct<float> TriWayDotProduct(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return NSimple::TriWay(lhs, rhs, length);
}

#endif

} // namespace NVectorDistance::NFloat8::NAvx2
