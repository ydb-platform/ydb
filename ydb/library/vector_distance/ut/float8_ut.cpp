#include <ydb/library/vector_distance/float8.h>
#include <ydb/library/vector_distance/float8_distance.h>
#include <ydb/library/vector_distance/float8_distance_avx2.h>
#include <ydb/library/vector_distance/float8_distance_sse.h>
#include <ydb/library/vector_distance/half_distance_simple.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>
#include <util/system/cpu_id.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>
#include <random>

namespace {
    // Independent arithmetic reference, rather than reusing the bit converter.
    template <bool E5M2>
    float Decode(ui8 bits) {
        constexpr unsigned fractionBits = E5M2 ? 2 : 3;
        constexpr int bias = E5M2 ? 15 : 7;
        const unsigned magnitude = bits & 127;
        const int exponent = magnitude >> fractionBits;
        const unsigned fraction = magnitude & ((1 << fractionBits) - 1);
        float value;
        if (E5M2 && exponent == 31) {
            value = fraction == 0 ? std::numeric_limits<float>::infinity() : std::numeric_limits<float>::quiet_NaN();
        } else if (!E5M2 && magnitude == 127) {
            value = std::numeric_limits<float>::quiet_NaN();
        } else if (exponent == 0) {
            value = std::ldexp(static_cast<float>(fraction), 1 - bias - fractionBits);
        } else {
            value = std::ldexp(1.0f + std::ldexp(static_cast<float>(fraction), -static_cast<int>(fractionBits)), exponent - bias);
        }
        return std::copysign(value, bits & 128 ? -1.0f : 1.0f);
    }

    template <bool E5M2>
    void TestEncoding() {
        using T = std::conditional_t<E5M2, TFloat8E5M2, TFloat8E4M3>;
        constexpr unsigned maxBits = E5M2 ? 123 : 126;
        for (unsigned bits = 0; bits < 256; ++bits) {
            const float expected = Decode<E5M2>(bits);
            const float actual = static_cast<float>(T::FromBits(bits));
            if (std::isnan(expected)) {
                UNIT_ASSERT(std::isnan(actual));
                UNIT_ASSERT(std::isnan(static_cast<float>(T(expected))));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(actual, expected);
                UNIT_ASSERT_VALUES_EQUAL(std::signbit(actual), std::signbit(expected));
                const unsigned encoded = std::isinf(expected) ? ((bits & 128) | maxBits) : bits;
                UNIT_ASSERT_VALUES_EQUAL(T(actual).Bits, encoded);
            }
        }

        // Exercise both sides of every midpoint, including subnormal/normal and
        // exponent boundaries. FP32 represents all these midpoints exactly.
        for (unsigned bits = 0; bits < maxBits; ++bits) {
            const float midpoint = (Decode<E5M2>(bits) + Decode<E5M2>(bits + 1)) / 2;
            const unsigned even = bits + (bits & 1);
            for (unsigned sign : {0u, 128u}) {
                const float factor = sign ? -1.0f : 1.0f;
                UNIT_ASSERT_VALUES_EQUAL(T(midpoint * factor).Bits, even | sign);
                UNIT_ASSERT_VALUES_EQUAL(T(std::nextafter(midpoint, 0.0f) * factor).Bits, bits | sign);
                UNIT_ASSERT_VALUES_EQUAL(T(std::nextafter(midpoint, std::numeric_limits<float>::infinity()) * factor).Bits, (bits + 1) | sign);
            }
        }

        for (float value : {std::numeric_limits<float>::max(), std::numeric_limits<float>::infinity()}) {
            UNIT_ASSERT_VALUES_EQUAL(T(value).Bits, maxBits);
            UNIT_ASSERT_VALUES_EQUAL(T(-value).Bits, maxBits | 128);
        }
        UNIT_ASSERT_VALUES_EQUAL(T(std::numeric_limits<float>::denorm_min()).Bits, 0);
        UNIT_ASSERT_VALUES_EQUAL(T(-std::numeric_limits<float>::denorm_min()).Bits, 128);

        // A float->half->FP8 converter would double-round this value downward.
        const float midpoint = E5M2 ? 1.125f : 1.0625f;
        UNIT_ASSERT_VALUES_EQUAL(static_cast<float>(T(std::nextafter(midpoint, 2.0f))), E5M2 ? 1.25f : 1.125f);

        // Compare random FP32 bit patterns against a search over all positive
        // finite values, independently checking rounding and saturation.
        std::mt19937 random(42);
        for (unsigned i = 0; i < 10000; ++i) {
            const float input = std::bit_cast<float>(static_cast<ui32>(random()));
            if (std::isnan(input)) {
                UNIT_ASSERT(std::isnan(static_cast<float>(T(input))));
                continue;
            }
            const double magnitude = std::abs(static_cast<double>(input));
            unsigned nearest = maxBits;
            for (unsigned bits = 0; bits < maxBits; ++bits) {
                const double midpoint = (static_cast<double>(Decode<E5M2>(bits)) + Decode<E5M2>(bits + 1)) / 2;
                if (magnitude < midpoint || (magnitude == midpoint && (bits & 1) == 0)) {
                    nearest = bits;
                    break;
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(T(input).Bits, nearest | (std::signbit(input) ? 128 : 0));
        }
    }

    void CheckFloat(float actual, double expected) {
        if (std::isnan(expected)) {
            UNIT_ASSERT(std::isnan(actual));
        } else if (std::isinf(expected)) {
            UNIT_ASSERT_VALUES_EQUAL(actual, expected);
        } else {
            UNIT_ASSERT_DOUBLES_EQUAL(actual, expected, 2e-5 * std::max(1.0, std::abs(expected)));
        }
    }

    template <typename T>
    struct TKernels {
        float (*L1)(const T*, const T*, size_t) noexcept;
        float (*L2Sqr)(const T*, const T*, size_t) noexcept;
        float (*Dot)(const T*, const T*, size_t) noexcept;
        TTriWayDotProduct<float> (*TriWay)(const T*, const T*, size_t) noexcept;
    };

    template <bool E5M2>
    void TestDistances() {
        using T = std::conditional_t<E5M2, TFloat8E5M2, TFloat8E4M3>;
        using namespace NVectorDistance;
        TVector<TKernels<T>> kernels = {
            {&NSimple::L1<T>, &NSimple::L2Sqr<T>, &NSimple::Dot<T>, &NSimple::TriWay<T>},
            {&L1Distance, &L2SqrDistance, &DotProduct, &TriWayDotProduct},
            {&NFloat8::NSse::L1Distance, &NFloat8::NSse::L2SqrDistance, &NFloat8::NSse::DotProduct, &NFloat8::NSse::TriWayDotProduct},
        };
        if (NX86::HaveAVX2()) {
            kernels.push_back({&NFloat8::NAvx2::L1Distance, &NFloat8::NAvx2::L2SqrDistance,
                &NFloat8::NAvx2::DotProduct, &NFloat8::NAvx2::TriWayDotProduct});
        }

        // Exact-sized allocations and offset starts detect accidental reads of
        // padding or the format byte under sanitizers, including short tails.
        for (size_t length : {0u, 1u, 2u, 3u, 4u, 5u, 7u, 8u, 9u, 15u, 16u, 17u, 31u, 32u, 33u, 67u, 257u, 1536u}) {
            for (size_t offset : {0u, 1u, 3u}) {
                TVector<T> lhs(length + offset), rhs(length + offset);
                double l1 = 0, l2 = 0, dot = 0, ll = 0, rr = 0;
                for (size_t i = 0; i < length; ++i) {
                    lhs[offset + i] = T(static_cast<float>(static_cast<int>(i * 17 % 97) - 48) / 16);
                    rhs[offset + i] = T(static_cast<float>(static_cast<int>(i * 7 % 67) - 33) / 16);
                    const double a = Decode<E5M2>(lhs[offset + i].Bits);
                    const double b = Decode<E5M2>(rhs[offset + i].Bits);
                    l1 += std::abs(a - b);
                    l2 += (a - b) * (a - b);
                    dot += a * b;
                    ll += a * a;
                    rr += b * b;
                }
                const T* a = length + offset ? lhs.data() + offset : nullptr;
                const T* b = length + offset ? rhs.data() + offset : nullptr;
                for (const auto& kernel : kernels) {
                    CheckFloat(kernel.L1(a, b, length), l1);
                    CheckFloat(kernel.L2Sqr(a, b, length), l2);
                    CheckFloat(kernel.Dot(a, b, length), dot);
                    const auto tri = kernel.TriWay(a, b, length);
                    CheckFloat(tri.LL, ll);
                    CheckFloat(tri.LR, dot);
                    CheckFloat(tri.RR, rr);
                }
                CheckFloat(L2Distance(a, b, length), std::sqrt(l2));
            }
        }

        // Run every FP8 code through every SIMD lane and tail, including NaN,
        // infinity, negative zero, and the E4M3 finite all-ones exponent.
        for (unsigned bits = 0; bits < 256; ++bits) {
            for (size_t position = 0; position < 17; ++position) {
                TVector<T> lhs(17, T(0.0f)), rhs(17, T(1.0f));
                lhs[position] = T::FromBits(bits);
                const double value = Decode<E5M2>(bits);
                for (const auto& kernel : kernels) {
                    CheckFloat(kernel.Dot(lhs.data(), rhs.data(), lhs.size()), value);
                    CheckFloat(kernel.L1(lhs.data(), rhs.data(), lhs.size()), 16 + std::abs(value - 1));
                    CheckFloat(kernel.L2Sqr(lhs.data(), rhs.data(), lhs.size()), 16 + (value - 1) * (value - 1));
                    const auto tri = kernel.TriWay(lhs.data(), rhs.data(), lhs.size());
                    CheckFloat(tri.LL, value * value);
                    CheckFloat(tri.LR, value);
                    CheckFloat(tri.RR, 17);
                }
            }
        }
    }
} // namespace

Y_UNIT_TEST_SUITE(Float8) {
    Y_UNIT_TEST_TWIN(Encoding, E5M2) {
        TestEncoding<E5M2>();
    }

    Y_UNIT_TEST_TWIN(Distances, E5M2) {
        TestDistances<E5M2>();
    }
}
