#include "int4_distance_avx2.h"
#include "int4_distance_sse.h"
#include "simd_distance.h"
#include "int4_distance_avx2_impl.h"

#include <cstring>

namespace NVectorDistance::NInt4::NAvx2 {
#ifdef _avx2_
using NPrivate::TOps;
#endif

float L1Distance(const ui8* lhs, const ui8* rhs, size_t length, [[maybe_unused]] std::bool_constant<false> sign) noexcept {
#ifdef _avx2_
    const auto tail = [](const ui8* a, const ui8* b, size_t n) noexcept { return NSse::L1Distance(a, b, n, std::bool_constant<false>{}); };
    return NSimd::Distance<NSimd::EMetric::L1, ui8, TOps<false>>(lhs, rhs, length, tail);
#else
    return NSse::L1Distance(lhs, rhs, length, sign);
#endif
}

float L2SqrDistance(const ui8* lhs, const ui8* rhs, size_t length, [[maybe_unused]] std::bool_constant<false> sign) noexcept {
#ifdef _avx2_
    const auto tail = [](const ui8* a, const ui8* b, size_t n) noexcept { return NSse::L2SqrDistance(a, b, n, std::bool_constant<false>{}); };
    return NSimd::Distance<NSimd::EMetric::L2Sqr, ui8, TOps<false>>(lhs, rhs, length, tail);
#else
    return NSse::L2SqrDistance(lhs, rhs, length, sign);
#endif
}

float DotProduct(const ui8* lhs, const ui8* rhs, size_t length, [[maybe_unused]] std::bool_constant<false> sign) noexcept {
#ifdef _avx2_
    const auto tail = [](const ui8* a, const ui8* b, size_t n) noexcept { return NSse::DotProduct(a, b, n, std::bool_constant<false>{}); };
    return NSimd::Distance<NSimd::EMetric::Dot, ui8, TOps<false>>(lhs, rhs, length, tail);
#else
    return NSse::DotProduct(lhs, rhs, length, sign);
#endif
}

TTriWayDotProduct<float> TriWayDotProduct(const ui8* lhs, const ui8* rhs, size_t length, [[maybe_unused]] std::bool_constant<false> sign) noexcept {
#ifdef _avx2_
    const auto tail = [](const ui8* a, const ui8* b, size_t n) noexcept { return NSse::TriWayDotProduct(a, b, n, std::bool_constant<false>{}); };
    return NSimd::TriWay<ui8, TOps<false>>(lhs, rhs, length, tail);
#else
    return NSse::TriWayDotProduct(lhs, rhs, length, sign);
#endif
}

float L1Distance(const ui8* lhs, const ui8* rhs, size_t length, [[maybe_unused]] std::bool_constant<true> sign) noexcept {
#ifdef _avx2_
    const auto tail = [](const ui8* a, const ui8* b, size_t n) noexcept { return NSse::L1Distance(a, b, n, std::bool_constant<true>{}); };
    return NSimd::Distance<NSimd::EMetric::L1, ui8, TOps<true>>(lhs, rhs, length, tail);
#else
    return NSse::L1Distance(lhs, rhs, length, sign);
#endif
}

float L2SqrDistance(const ui8* lhs, const ui8* rhs, size_t length, [[maybe_unused]] std::bool_constant<true> sign) noexcept {
#ifdef _avx2_
    const auto tail = [](const ui8* a, const ui8* b, size_t n) noexcept { return NSse::L2SqrDistance(a, b, n, std::bool_constant<true>{}); };
    return NSimd::Distance<NSimd::EMetric::L2Sqr, ui8, TOps<true>>(lhs, rhs, length, tail);
#else
    return NSse::L2SqrDistance(lhs, rhs, length, sign);
#endif
}

float DotProduct(const ui8* lhs, const ui8* rhs, size_t length, [[maybe_unused]] std::bool_constant<true> sign) noexcept {
#ifdef _avx2_
    const auto tail = [](const ui8* a, const ui8* b, size_t n) noexcept { return NSse::DotProduct(a, b, n, std::bool_constant<true>{}); };
    return NSimd::Distance<NSimd::EMetric::Dot, ui8, TOps<true>>(lhs, rhs, length, tail);
#else
    return NSse::DotProduct(lhs, rhs, length, sign);
#endif
}

TTriWayDotProduct<float> TriWayDotProduct(const ui8* lhs, const ui8* rhs, size_t length, [[maybe_unused]] std::bool_constant<true> sign) noexcept {
#ifdef _avx2_
    const auto tail = [](const ui8* a, const ui8* b, size_t n) noexcept { return NSse::TriWayDotProduct(a, b, n, std::bool_constant<true>{}); };
    return NSimd::TriWay<ui8, TOps<true>>(lhs, rhs, length, tail);
#else
    return NSse::TriWayDotProduct(lhs, rhs, length, sign);
#endif
}

} // namespace NVectorDistance::NInt4::NAvx2
