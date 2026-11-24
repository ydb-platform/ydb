#include "int4_distance.h"
#include "int4_distance_sse.h"
#include "int4_distance_avx2.h"

#include <util/system/cpu_id.h>
#include <cmath>

namespace NVectorDistance::NInt4 {

float L1Distance(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<false> sign) noexcept {
    static const auto kernel = NX86::HaveAVX2()
        ? static_cast<float (*)(const ui8*, const ui8*, size_t, std::bool_constant<false>) noexcept>(&NAvx2::L1Distance)
        : static_cast<float (*)(const ui8*, const ui8*, size_t, std::bool_constant<false>) noexcept>(&NSse::L1Distance);
    return kernel(lhs, rhs, length, sign);
}

float L2SqrDistance(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<false> sign) noexcept {
    static const auto kernel = NX86::HaveAVX2()
        ? static_cast<float (*)(const ui8*, const ui8*, size_t, std::bool_constant<false>) noexcept>(&NAvx2::L2SqrDistance)
        : static_cast<float (*)(const ui8*, const ui8*, size_t, std::bool_constant<false>) noexcept>(&NSse::L2SqrDistance);
    return kernel(lhs, rhs, length, sign);
}

float DotProduct(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<false> sign) noexcept {
    static const auto kernel = NX86::HaveAVX2()
        ? static_cast<float (*)(const ui8*, const ui8*, size_t, std::bool_constant<false>) noexcept>(&NAvx2::DotProduct)
        : static_cast<float (*)(const ui8*, const ui8*, size_t, std::bool_constant<false>) noexcept>(&NSse::DotProduct);
    return kernel(lhs, rhs, length, sign);
}

TTriWayDotProduct<float> TriWayDotProduct(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<false> sign) noexcept {
    static const auto kernel = NX86::HaveAVX2()
        ? static_cast<TTriWayDotProduct<float> (*)(const ui8*, const ui8*, size_t, std::bool_constant<false>) noexcept>(&NAvx2::TriWayDotProduct)
        : static_cast<TTriWayDotProduct<float> (*)(const ui8*, const ui8*, size_t, std::bool_constant<false>) noexcept>(&NSse::TriWayDotProduct);
    return kernel(lhs, rhs, length, sign);
}

float L2Distance(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<false> sign) noexcept {
    return std::sqrt(L2SqrDistance(lhs, rhs, length, sign));
}

float L1Distance(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<true> sign) noexcept {
    static const auto kernel = NX86::HaveAVX2()
        ? static_cast<float (*)(const ui8*, const ui8*, size_t, std::bool_constant<true>) noexcept>(&NAvx2::L1Distance)
        : static_cast<float (*)(const ui8*, const ui8*, size_t, std::bool_constant<true>) noexcept>(&NSse::L1Distance);
    return kernel(lhs, rhs, length, sign);
}

float L2SqrDistance(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<true> sign) noexcept {
    static const auto kernel = NX86::HaveAVX2()
        ? static_cast<float (*)(const ui8*, const ui8*, size_t, std::bool_constant<true>) noexcept>(&NAvx2::L2SqrDistance)
        : static_cast<float (*)(const ui8*, const ui8*, size_t, std::bool_constant<true>) noexcept>(&NSse::L2SqrDistance);
    return kernel(lhs, rhs, length, sign);
}

float DotProduct(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<true> sign) noexcept {
    static const auto kernel = NX86::HaveAVX2()
        ? static_cast<float (*)(const ui8*, const ui8*, size_t, std::bool_constant<true>) noexcept>(&NAvx2::DotProduct)
        : static_cast<float (*)(const ui8*, const ui8*, size_t, std::bool_constant<true>) noexcept>(&NSse::DotProduct);
    return kernel(lhs, rhs, length, sign);
}

TTriWayDotProduct<float> TriWayDotProduct(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<true> sign) noexcept {
    static const auto kernel = NX86::HaveAVX2()
        ? static_cast<TTriWayDotProduct<float> (*)(const ui8*, const ui8*, size_t, std::bool_constant<true>) noexcept>(&NAvx2::TriWayDotProduct)
        : static_cast<TTriWayDotProduct<float> (*)(const ui8*, const ui8*, size_t, std::bool_constant<true>) noexcept>(&NSse::TriWayDotProduct);
    return kernel(lhs, rhs, length, sign);
}

float L2Distance(const ui8* lhs, const ui8* rhs, size_t length, std::bool_constant<true> sign) noexcept {
    return std::sqrt(L2SqrDistance(lhs, rhs, length, sign));
}

} // namespace NVectorDistance::NInt4
