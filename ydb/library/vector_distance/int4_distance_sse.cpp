#include "int4_distance_sse.h"
#include "int4_distance_simple.h"
#include "simd_distance.h"
#include "int4_distance_sse_impl.h"

#include <cstring>

namespace NVectorDistance::NInt4::NSse {
#ifdef ARCADIA_SSE
using NPrivate::TOps;
#endif

float L1Distance(const ui8* lhs, const ui8* rhs, size_t length, [[maybe_unused]] std::bool_constant<false> sign) noexcept {
#ifdef ARCADIA_SSE
    const auto tail = [](const ui8* a, const ui8* b, size_t n) noexcept { return NSimple::L1Distance(a, b, n, std::bool_constant<false>{}); };
    return NSimd::Distance<NSimd::EMetric::L1, ui8, TOps<false>>(lhs, rhs, length, tail);
#else
    return NSimple::L1Distance(lhs, rhs, length, sign);
#endif
}

float L2SqrDistance(const ui8* lhs, const ui8* rhs, size_t length, [[maybe_unused]] std::bool_constant<false> sign) noexcept {
#ifdef ARCADIA_SSE
    const auto tail = [](const ui8* a, const ui8* b, size_t n) noexcept { return NSimple::L2SqrDistance(a, b, n, std::bool_constant<false>{}); };
    return NSimd::Distance<NSimd::EMetric::L2Sqr, ui8, TOps<false>>(lhs, rhs, length, tail);
#else
    return NSimple::L2SqrDistance(lhs, rhs, length, sign);
#endif
}

float DotProduct(const ui8* lhs, const ui8* rhs, size_t length, [[maybe_unused]] std::bool_constant<false> sign) noexcept {
#ifdef ARCADIA_SSE
    const auto tail = [](const ui8* a, const ui8* b, size_t n) noexcept { return NSimple::DotProduct(a, b, n, std::bool_constant<false>{}); };
    return NSimd::Distance<NSimd::EMetric::Dot, ui8, TOps<false>>(lhs, rhs, length, tail);
#else
    return NSimple::DotProduct(lhs, rhs, length, sign);
#endif
}

TTriWayDotProduct<float> TriWayDotProduct(const ui8* lhs, const ui8* rhs, size_t length, [[maybe_unused]] std::bool_constant<false> sign) noexcept {
#ifdef ARCADIA_SSE
    const auto tail = [](const ui8* a, const ui8* b, size_t n) noexcept { return NSimple::TriWayDotProduct(a, b, n, std::bool_constant<false>{}); };
    return NSimd::TriWay<ui8, TOps<false>>(lhs, rhs, length, tail);
#else
    return NSimple::TriWayDotProduct(lhs, rhs, length, sign);
#endif
}

float L1Distance(const ui8* lhs, const ui8* rhs, size_t length, [[maybe_unused]] std::bool_constant<true> sign) noexcept {
#ifdef ARCADIA_SSE
    const auto tail = [](const ui8* a, const ui8* b, size_t n) noexcept { return NSimple::L1Distance(a, b, n, std::bool_constant<true>{}); };
    return NSimd::Distance<NSimd::EMetric::L1, ui8, TOps<true>>(lhs, rhs, length, tail);
#else
    return NSimple::L1Distance(lhs, rhs, length, sign);
#endif
}

float L2SqrDistance(const ui8* lhs, const ui8* rhs, size_t length, [[maybe_unused]] std::bool_constant<true> sign) noexcept {
#ifdef ARCADIA_SSE
    const auto tail = [](const ui8* a, const ui8* b, size_t n) noexcept { return NSimple::L2SqrDistance(a, b, n, std::bool_constant<true>{}); };
    return NSimd::Distance<NSimd::EMetric::L2Sqr, ui8, TOps<true>>(lhs, rhs, length, tail);
#else
    return NSimple::L2SqrDistance(lhs, rhs, length, sign);
#endif
}

float DotProduct(const ui8* lhs, const ui8* rhs, size_t length, [[maybe_unused]] std::bool_constant<true> sign) noexcept {
#ifdef ARCADIA_SSE
    const auto tail = [](const ui8* a, const ui8* b, size_t n) noexcept { return NSimple::DotProduct(a, b, n, std::bool_constant<true>{}); };
    return NSimd::Distance<NSimd::EMetric::Dot, ui8, TOps<true>>(lhs, rhs, length, tail);
#else
    return NSimple::DotProduct(lhs, rhs, length, sign);
#endif
}

TTriWayDotProduct<float> TriWayDotProduct(const ui8* lhs, const ui8* rhs, size_t length, [[maybe_unused]] std::bool_constant<true> sign) noexcept {
#ifdef ARCADIA_SSE
    const auto tail = [](const ui8* a, const ui8* b, size_t n) noexcept { return NSimple::TriWayDotProduct(a, b, n, std::bool_constant<true>{}); };
    return NSimd::TriWay<ui8, TOps<true>>(lhs, rhs, length, tail);
#else
    return NSimple::TriWayDotProduct(lhs, rhs, length, sign);
#endif
}

} // namespace NVectorDistance::NInt4::NSse
