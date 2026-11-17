#include "int4_load_ut.h"

#include <ydb/library/vector_distance/int4_distance.h>
#include <ydb/library/vector_distance/int4_distance_sse.h>
#include <ydb/library/vector_distance/int4_distance_sse_impl.h>
#include <ydb/library/vector_distance/int4_distance_avx2.h>
#include <ydb/library/vector_distance/int4_distance_simple.h>
#include <ydb/library/yql/udfs/common/knn/knn-distance.h>
#include <ydb/library/yql/udfs/common/knn/knn-serializer-shared.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/stream/str.h>
#include <util/system/cpu_id.h>

#include <array>
#include <cmath>
#include <limits>

namespace {
    template <typename T>
    TString Encode(const TVector<int>& values) {
        TString bytes;
        TStringOutput output(bytes);
        NKnnVectorSerialization::TSerializer<T> encoder(&output);
        for (int value : values) {
            encoder.HandleElement(value);
        }
        encoder.Finish();
        return bytes;
    }

    template <bool Signed>
    void CheckDistances(size_t length, size_t offset, unsigned seed) {
        using T = NVectorDistance::NPrivate::TInt4<Signed>;
        using TSign = std::bool_constant<Signed>;
        TVector<ui8> a(offset + (length + 1) / 2, 0), b(a.size(), 0);
        float l1 = 0, l2 = 0, dot = 0, ll = 0, rr = 0;
        for (size_t i = 0; i < length; ++i) {
            const T av = T::FromBits(seed + i * 7);
            const T bv = T::FromBits(seed / 16 + i * 11);
            a[offset + i / 2] |= av.Bits << (4 * (i % 2));
            b[offset + i / 2] |= bv.Bits << (4 * (i % 2));
            const int x = av, y = bv;
            l1 += std::abs(x - y);
            l2 += (x - y) * (x - y);
            dot += x * y;
            ll += x * x;
            rr += y * y;
        }
        const auto* lhs = a.empty() ? nullptr : a.data() + offset;
        const auto* rhs = b.empty() ? nullptr : b.data() + offset;
        struct TKernels {
            float (*L1)(const ui8*, const ui8*, size_t, TSign) noexcept;
            float (*L2)(const ui8*, const ui8*, size_t, TSign) noexcept;
            float (*Dot)(const ui8*, const ui8*, size_t, TSign) noexcept;
            TTriWayDotProduct<float> (*Tri)(const ui8*, const ui8*, size_t, TSign) noexcept;
        };
        using namespace NVectorDistance::NInt4;
        TVector<TKernels> kernels = {
            {&L1Distance, &L2SqrDistance, &DotProduct, &TriWayDotProduct},
            {&NSimple::L1Distance, &NSimple::L2SqrDistance, &NSimple::DotProduct, &NSimple::TriWayDotProduct},
            {&NSse::L1Distance, &NSse::L2SqrDistance, &NSse::DotProduct, &NSse::TriWayDotProduct},
        };
        if (NX86::HaveAVX2()) {
            kernels.push_back({&NAvx2::L1Distance, &NAvx2::L2SqrDistance, &NAvx2::DotProduct, &NAvx2::TriWayDotProduct});
        }
        for (const auto& kernel : kernels) {
            UNIT_ASSERT_VALUES_EQUAL(kernel.L1(lhs, rhs, length, TSign{}), l1);
            UNIT_ASSERT_VALUES_EQUAL(kernel.L2(lhs, rhs, length, TSign{}), l2);
            UNIT_ASSERT_VALUES_EQUAL(kernel.Dot(lhs, rhs, length, TSign{}), dot);
            const auto tri = kernel.Tri(lhs, rhs, length, TSign{});
            UNIT_ASSERT_VALUES_EQUAL(tri.LL, ll);
            UNIT_ASSERT_VALUES_EQUAL(tri.LR, dot);
            UNIT_ASSERT_VALUES_EQUAL(tri.RR, rr);
        }
    }

    template <bool Signed, size_t Width, typename TLoad>
    void CheckLoad(TLoad load) {
        using T = NVectorDistance::NPrivate::TInt4<Signed>;
        for (size_t offset = 0; offset < 8; ++offset) {
            TVector<ui8> bytes(offset + Width / 2);
            for (unsigned bits = 0; bits < 256; ++bits) {
                for (size_t i = 0; i < Width / 2; ++i) {
                    bytes[offset + i] = static_cast<ui8>(bits + 37 * i);
                }
                const auto result = load(bytes.data() + offset);
                for (size_t lane = 0; lane < Width; ++lane) {
                    const int expected = T::FromBits(bytes[offset + lane / 2] >> (4 * (lane % 2)));
                    UNIT_ASSERT_VALUES_EQUAL(result[lane], expected);
                }
            }
        }
    }
}

Y_UNIT_TEST_SUITE(Int4) {
    Y_UNIT_TEST_TWIN(ConversionAndSerialization, Signed) {
        using T = NVectorDistance::NPrivate::TInt4<Signed>;
        for (int value = -256; value <= 256; ++value) {
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(T(value)), std::clamp(value, T::MinValue, T::MaxValue));
        }
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(T(std::numeric_limits<float>::infinity())), T::MaxValue);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(T(-std::numeric_limits<float>::infinity())), T::MinValue);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(T(std::numeric_limits<float>::quiet_NaN())), 0);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(T(3.9f)), 3);
        for (size_t count : {0u, 1u, 2u, 3u, 4u, 7u, 8u, 15u, 16u, 17u, 67u, 256u}) {
            TVector<int> values;
            for (size_t i = 0; i < count; ++i) {
                values.push_back(T::MinValue + i % 16);
            }
            const auto bytes = Encode<T>(values);
            UNIT_ASSERT_VALUES_EQUAL(bytes.size(), (count + 1) / 2 + 2);
            UNIT_ASSERT_VALUES_EQUAL(bytes.size(), NKnnVectorSerialization::GetBufferSize<T>(count));
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(bytes.back()), static_cast<ui8>(Format<T>));
            UNIT_ASSERT_VALUES_EQUAL(static_cast<ui8>(bytes[bytes.size() - 2]), count % 2);
            for (size_t i = 0; i < count; ++i) {
                UNIT_ASSERT_VALUES_EQUAL((static_cast<ui8>(bytes[i / 2]) >> (4 * (i % 2))) & 15, values[i] & 15);
            }
            NKnnVectorSerialization::TDeserializer<T> decoder(bytes);
            UNIT_ASSERT_VALUES_EQUAL(decoder.GetElementCount(), count);
            size_t seen = 0;
            decoder.DoDeserialize([&](const T& value) { UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(value), values.at(seen++)); });
            UNIT_ASSERT_VALUES_EQUAL(seen, count);
        }
    }

    Y_UNIT_TEST_TWIN(SerializerWriteFailure, Signed) {
        using T = NVectorDistance::NPrivate::TInt4<Signed>;
        struct TFailingOutput : IOutputStream {
            size_t Writes = 0;
            void DoWrite(const void*, size_t) override {
                ++Writes;
                ythrow yexception() << "write failed";
            }
        } output;
        {
            NKnnVectorSerialization::TSerializer<T> serializer(&output);
            serializer.HandleElement(1);
            UNIT_ASSERT_EXCEPTION_CONTAINS(serializer.Finish(), yexception, "write failed");
        }
        UNIT_ASSERT_VALUES_EQUAL(output.Writes, 1);
    }

    Y_UNIT_TEST_TWIN(Distances, Signed) {
        for (size_t length : {0u, 1u, 2u, 3u, 4u, 5u, 7u, 8u, 9u, 15u, 16u, 17u, 31u, 32u, 33u, 67u, 257u, 65535u, 65536u}) {
            for (size_t offset : {0u, 1u, 3u}) {
                CheckDistances<Signed>(length, offset, 42);
            }
        }
        for (unsigned pair = 0; pair < 256; ++pair) {
            CheckDistances<Signed>(17, 1, pair);
        }
    }

    Y_UNIT_TEST_TWIN(SerializedDistancesAndValidation, Signed) {
        using T = NVectorDistance::NPrivate::TInt4<Signed>;
        const auto a = Encode<T>({T::MinValue, 0, T::MaxValue});
        const auto b = Encode<T>({T::MaxValue, 1, T::MinValue});
        const float l1 = 2 * (T::MaxValue - T::MinValue) + 1;
        const float l2 = 2 * (T::MaxValue - T::MinValue) * (T::MaxValue - T::MinValue) + 1;
        UNIT_ASSERT_VALUES_EQUAL(*KnnDistance<>::ManhattanDistance(a, b), l1);
        UNIT_ASSERT_DOUBLES_EQUAL(*KnnDistance<>::EuclideanDistance(a, b), std::sqrt(l2), 1e-5);
        const float dot = 2 * T::MinValue * T::MaxValue;
        UNIT_ASSERT_VALUES_EQUAL(*KnnDistance<>::DotProduct(a, b), dot);
        const float ll = T::MinValue * T::MinValue + T::MaxValue * T::MaxValue;
        UNIT_ASSERT_DOUBLES_EQUAL(*KnnDistance<>::CosineSimilarity(a, b), dot / std::sqrt(ll * (ll + 1)), 1e-6);
        const auto empty = Encode<T>({});
        UNIT_ASSERT_VALUES_EQUAL(*KnnDistance<>::DotProduct(empty, empty), 0);
        UNIT_ASSERT_VALUES_EQUAL(*KnnDistance<>::CosineSimilarity(empty, empty), 1);
        UNIT_ASSERT(!KnnDistance<>::DotProduct(a, empty));
        auto badPadding = a;
        badPadding[badPadding.size() - 2] = 2;
        auto badNibble = a;
        badNibble[badNibble.size() - 3] = static_cast<char>(static_cast<ui8>(badNibble[badNibble.size() - 3]) | 0xf0);
        auto missingPayload = empty;
        missingPayload[0] = 1;
        auto wrongType = a;
        wrongType.back() = Signed ? Format<TUint4> : Format<TInt4>;
        for (const auto& invalid : {TString(), TString(1, Format<T>), badPadding, badNibble, missingPayload, wrongType}) {
            UNIT_ASSERT(!KnnDistance<>::ManhattanDistance(a, invalid));
            UNIT_ASSERT(!KnnDistance<>::EuclideanDistance(a, invalid));
            UNIT_ASSERT(!KnnDistance<>::DotProduct(a, invalid));
            UNIT_ASSERT(!KnnDistance<>::CosineSimilarity(a, invalid));
        }
    }

#ifdef ARCADIA_SSE
    Y_UNIT_TEST_TWIN(SseLoad, Signed) {
        using TOps = NVectorDistance::NInt4::NSse::NPrivate::TOps<Signed>;
        CheckLoad<Signed, TOps::Width>([](const ui8* ptr) {
            std::array<float, TOps::Width> result;
            _mm_storeu_ps(result.data(), TOps::Load(ptr));
            return result;
        });
    }
#endif
#ifdef TEST_INT4_AVX2
    Y_UNIT_TEST_TWIN(Avx2Load, Signed) {
        if (!NX86::HaveAVX2()) {
            return;
        }
        CheckLoad<Signed, 8>([](const ui8* ptr) {
            return NVectorDistance::NInt4::NTesting::LoadAvx2(ptr, std::bool_constant<Signed>{});
        });
    }
#endif
}
