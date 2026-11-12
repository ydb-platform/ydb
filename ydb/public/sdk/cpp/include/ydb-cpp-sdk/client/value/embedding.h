#pragma once

#include "value.h"

#include <bit>
#include <concepts>
#include <cstdint>
#include <limits>
#include <ranges>
#include <string>

namespace NYdb::inline Dev {
namespace NValueHelpers {

namespace NPrivate {

template <typename T>
concept TEmbeddingNumber = (std::integral<T> && sizeof(T) > 1) || std::same_as<T, float> || std::same_as<T, double>;

} // namespace NPrivate

//! Builds a Bytes value in YDB FloatVector format. Elements are converted to Float32.
//! An empty range produces a single format byte. Declare the query parameter as Bytes.
template <typename TRange>
    requires std::ranges::sized_range<const TRange&> && NPrivate::TEmbeddingNumber<std::ranges::range_value_t<TRange>>
TValue Embedding(const TRange& values) {
    static_assert(sizeof(float) == sizeof(std::uint32_t));
    static_assert(std::numeric_limits<float>::is_iec559);

    std::string bytes;
    bytes.reserve(std::ranges::size(values) * sizeof(float) + 1);
    for (auto value : values) {
        const std::uint32_t bits = std::bit_cast<std::uint32_t>(static_cast<float>(value));
        for (unsigned shift = 0; shift < 32; shift += 8) {
            bytes.push_back(static_cast<char>(bits >> shift));
        }
    }
    bytes.push_back('\x01');

    return TValueBuilder().Bytes(bytes).Build();
}

} // namespace NValueHelpers
} // namespace NYdb::inline Dev
