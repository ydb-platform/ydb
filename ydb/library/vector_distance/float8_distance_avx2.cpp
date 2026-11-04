#include "float8_distance_avx2.h"
#include "float8_distance_avx2_impl.h"
#include "float8_distance_simd.h"
#include "float8_distance_sse.h"

namespace NVectorDistance::NFloat8::NAvx2 {

#if defined(_avx2_)
using NPrivate::TOps;

float L1Distance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return Distance<EMetric::L1, TFloat8E4M3, TOps>(lhs, rhs, length, &NSse::L1Distance);
}

float L2SqrDistance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return Distance<EMetric::L2Sqr, TFloat8E4M3, TOps>(lhs, rhs, length, &NSse::L2SqrDistance);
}

float DotProduct(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return Distance<EMetric::Dot, TFloat8E4M3, TOps>(lhs, rhs, length, &NSse::DotProduct);
}

TTriWayDotProduct<float> TriWayDotProduct(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return TriWay<TFloat8E4M3, TOps>(lhs, rhs, length, &NSse::TriWayDotProduct);
}

float L1Distance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return Distance<EMetric::L1, TFloat8E5M2, TOps>(lhs, rhs, length, &NSse::L1Distance);
}

float L2SqrDistance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return Distance<EMetric::L2Sqr, TFloat8E5M2, TOps>(lhs, rhs, length, &NSse::L2SqrDistance);
}

float DotProduct(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return Distance<EMetric::Dot, TFloat8E5M2, TOps>(lhs, rhs, length, &NSse::DotProduct);
}

TTriWayDotProduct<float> TriWayDotProduct(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return TriWay<TFloat8E5M2, TOps>(lhs, rhs, length, &NSse::TriWayDotProduct);
}

#else

float L1Distance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return NSse::L1Distance(lhs, rhs, length);
}

float L2SqrDistance(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return NSse::L2SqrDistance(lhs, rhs, length);
}

float DotProduct(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return NSse::DotProduct(lhs, rhs, length);
}

TTriWayDotProduct<float> TriWayDotProduct(const TFloat8E4M3* lhs, const TFloat8E4M3* rhs, size_t length) noexcept {
    return NSse::TriWayDotProduct(lhs, rhs, length);
}

float L1Distance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return NSse::L1Distance(lhs, rhs, length);
}

float L2SqrDistance(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return NSse::L2SqrDistance(lhs, rhs, length);
}

float DotProduct(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return NSse::DotProduct(lhs, rhs, length);
}

TTriWayDotProduct<float> TriWayDotProduct(const TFloat8E5M2* lhs, const TFloat8E5M2* rhs, size_t length) noexcept {
    return NSse::TriWayDotProduct(lhs, rhs, length);
}

#endif

} // namespace NVectorDistance::NFloat8::NAvx2
