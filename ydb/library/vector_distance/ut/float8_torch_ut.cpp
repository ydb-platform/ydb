#include "float8_torch_data.h"
#include "float8_load_ut.h"

#include <ydb/library/vector_distance/float8_distance.h>
#include <ydb/library/vector_distance/float8_distance_avx2.h>
#include <ydb/library/vector_distance/float8_distance_sse.h>
#include <ydb/library/vector_distance/float8_distance_sse_impl.h>
#include <ydb/library/vector_distance/half_distance_simple.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/cpu_id.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>

namespace {
    using namespace NVectorDistance::NFloat8::NTorchReference;

    void CheckDecoded(float actual, ui32 expectedBits, const TString& context) {
        const ui32 actualBits = std::bit_cast<ui32>(actual);
        if ((expectedBits & 0x7f800000u) == 0x7f800000u && (expectedBits & 0x007fffffu)) {
            // PyTorch can quiet signaling NaNs and choose a different payload.
            // Classification and sign must agree; finite values, infinities
            // and signed zeros must agree bit for bit.
            UNIT_ASSERT_C((actualBits & 0x7f800000u) == 0x7f800000u && (actualBits & 0x007fffffu), context);
            UNIT_ASSERT_VALUES_EQUAL_C(actualBits & 0x80000000u, expectedBits & 0x80000000u, context);
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(actualBits, expectedBits, context);
        }
    }

    template <typename T, size_t Width, typename TLoad>
    void CheckLoad(const std::array<ui32, 256>& reference, TLoad load) {
        for (size_t offset = 0; offset < Width; ++offset) {
            TVector<T> input(offset + Width);
            for (unsigned bits = 0; bits < 256; ++bits) {
                for (size_t lane = 0; lane < Width; ++lane) {
                    input[offset + lane] = T::FromBits(static_cast<ui8>(bits + 37 * lane));
                }
                const auto result = load(input.data() + offset);
                for (size_t lane = 0; lane < Width; ++lane) {
                    const ui8 code = input[offset + lane].Bits;
                    CheckDecoded(result[lane], reference[code], TStringBuilder() << "FP8=" << static_cast<unsigned>(code) << ", lane=" << lane << ", offset=" << offset);
                }
            }
        }
    }

    void CheckDistance(float actual, double expected, const TString& context) {
        UNIT_ASSERT_DOUBLES_EQUAL_C(actual, expected, 2e-5 * std::max(1.0, std::abs(expected)), context);
    }

    template <typename T>
    struct TKernels {
        float (*L1)(const T*, const T*, size_t) noexcept;
        float (*L2Sqr)(const T*, const T*, size_t) noexcept;
        float (*Dot)(const T*, const T*, size_t) noexcept;
        TTriWayDotProduct<float> (*TriWay)(const T*, const T*, size_t) noexcept;
    };
} // namespace

Y_UNIT_TEST_SUITE(Float8PyTorch) {
    Y_UNIT_TEST_TWIN(ScalarDecode, E5M2) {
        using T = std::conditional_t<E5M2, TFloat8E5M2, TFloat8E4M3>;
        const auto& reference = E5M2 ? E5M2Decoded : E4M3Decoded;
        for (unsigned bits = 0; bits < 256; ++bits) {
            CheckDecoded(static_cast<float>(T::FromBits(bits)), reference[bits],
                         TStringBuilder() << "FP8=" << bits);
        }
    }

    Y_UNIT_TEST_TWIN(ScalarEncode, E5M2) {
        using T = std::conditional_t<E5M2, TFloat8E5M2, TFloat8E4M3>;
        const auto& decoded = E5M2 ? E5M2Decoded : E4M3Decoded;
        const auto& saturated = E5M2 ? E5M2Encoded : E4M3Encoded;
        const auto& plain = E5M2 ? E5M2PlainCast : E4M3PlainCast;
        const float maximum = static_cast<float>(T::FromBits(T::MaxFiniteBits));
        for (size_t index = 0; index < Float32Inputs.size(); ++index) {
            const float input = std::bit_cast<float>(Float32Inputs[index]);
            const T actual(input);
            const TString context = TStringBuilder() << "FP32 bits=" << Float32Inputs[index];
            if (std::isnan(input)) {
                CheckDecoded(static_cast<float>(actual), decoded[saturated[index]], context);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(actual.Bits, saturated[index], context);
                // In-range conversion also agrees with the unmodified PyTorch
                // cast. Overflow follows the documented saturating contract.
                if (std::abs(input) <= maximum) {
                    UNIT_ASSERT_VALUES_EQUAL_C(actual.Bits, plain[index], context);
                }
            }
        }
    }

    Y_UNIT_TEST_TWIN(SimdLoads, E5M2) {
        using T [[maybe_unused]] = std::conditional_t<E5M2, TFloat8E5M2, TFloat8E4M3>;
        const auto& reference = E5M2 ? E5M2Decoded : E4M3Decoded;
#ifdef ARCADIA_SSE
        if (NX86::HaveSSE2()) {
            using TOps = NVectorDistance::NFloat8::NSse::NPrivate::TOps;
            CheckLoad<T, TOps::Width>(reference, [](const T* input) {
                std::array<float, TOps::Width> result;
                _mm_storeu_ps(result.data(), TOps::Load(input));
                return result;
            });
        }
#endif
#ifdef TEST_FLOAT8_AVX2
        if (NX86::HaveAVX2() && NX86::HaveF16C()) {
            CheckLoad<T, 8>(reference, [](const T* input) {
                return NVectorDistance::NFloat8::NTesting::LoadAvx2(input);
            });
        }
#endif
        // Keep this test valid on platforms without either SIMD implementation.
        Y_UNUSED(reference);
    }

    Y_UNIT_TEST_TWIN(Distances, E5M2) {
        using T = std::conditional_t<E5M2, TFloat8E5M2, TFloat8E4M3>;
        using namespace NVectorDistance;
        TVector<TKernels<T>> kernels = {
            {&NSimple::L1<T>, &NSimple::L2Sqr<T>, &NSimple::Dot<T>, &NSimple::TriWay<T>},
            {&L1Distance, &L2SqrDistance, &DotProduct, &TriWayDotProduct},
            {&NFloat8::NSse::L1Distance, &NFloat8::NSse::L2SqrDistance, &NFloat8::NSse::DotProduct, &NFloat8::NSse::TriWayDotProduct},
        };
        if (NX86::HaveAVX2() && NX86::HaveF16C()) {
            kernels.push_back({&NFloat8::NAvx2::L1Distance, &NFloat8::NAvx2::L2SqrDistance,
                               &NFloat8::NAvx2::DotProduct, &NFloat8::NAvx2::TriWayDotProduct});
        }
        const auto& left = E5M2 ? E5M2Lhs : E4M3Lhs;
        const auto& right = E5M2 ? E5M2Rhs : E4M3Rhs;
        const auto& reference = E5M2 ? E5M2Distances : E4M3Distances;
        for (const auto& test : reference) {
            for (size_t offset : {0u, 1u, 3u}) {
                TVector<T> lhs(offset + test.Length), rhs(offset + test.Length);
                for (size_t index = 0; index < test.Length; ++index) {
                    lhs[offset + index] = T::FromBits(left[test.Offset + index]);
                    rhs[offset + index] = T::FromBits(right[test.Offset + index]);
                }
                const T* a = lhs.empty() ? nullptr : lhs.data() + offset;
                const T* b = rhs.empty() ? nullptr : rhs.data() + offset;
                const TString context = TStringBuilder() << "length=" << test.Length << ", offset=" << offset;
                for (const auto& kernel : kernels) {
                    CheckDistance(kernel.L1(a, b, test.Length), test.L1, context);
                    CheckDistance(kernel.L2Sqr(a, b, test.Length), test.L2Sqr, context);
                    CheckDistance(kernel.Dot(a, b, test.Length), test.LR, context);
                    const auto tri = kernel.TriWay(a, b, test.Length);
                    CheckDistance(tri.LL, test.LL, context);
                    CheckDistance(tri.LR, test.LR, context);
                    CheckDistance(tri.RR, test.RR, context);
                }
                CheckDistance(L2Distance(a, b, test.Length), test.L2, context);
            }
        }
    }
} // Y_UNIT_TEST_SUITE(Float8PyTorch)
