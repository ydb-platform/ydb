#pragma once

#include <ydb/library/vector_distance/float8.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <array>
#include <bit>

namespace NVectorDistance::NFloat8::NTesting {

    template <typename T, size_t Width, typename TLoad>
    void TestLoad(TLoad load) {
        // Every encoding visits every lane, with different values in adjacent
        // lanes. Exact-sized inputs also expose overreads under sanitizers.
        for (size_t offset = 0; offset < Width; ++offset) {
            TVector<T> values(Width + offset);
            for (unsigned bits = 0; bits < 256; ++bits) {
                for (size_t lane = 0; lane < Width; ++lane) {
                    values[offset + lane] = T::FromBits(static_cast<ui8>(bits + 37 * lane));
                }
                const auto actual = load(values.data() + offset);
                for (size_t lane = 0; lane < Width; ++lane) {
                    const float expected = static_cast<float>(values[offset + lane]);
                    // Compare bits to preserve signed zero and NaN signs and
                    // payloads, including the E5M2 signaling NaN encodings.
                    UNIT_ASSERT_VALUES_EQUAL_C(std::bit_cast<ui32>(actual[lane]), std::bit_cast<ui32>(expected),
                        TStringBuilder() << "bits=" << static_cast<unsigned>(values[offset + lane].Bits)
                                         << ", lane=" << lane << ", offset=" << offset);
                }
            }
        }
    }

    std::array<float, 8> LoadAvx2(const TFloat8E4M3* ptr);
    std::array<float, 8> LoadAvx2(const TFloat8E5M2* ptr);

} // namespace NVectorDistance::NFloat8::NTesting
