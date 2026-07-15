#pragma once

#include <cstdint>

namespace NKikimr::NOlap::NIndexes::NDefaults {

// Bloom filter
constexpr double FalsePositiveProbability = 0.1;
constexpr bool CaseSensitive = true;

// Bloom ngram filter
constexpr std::uint32_t NGrammSize = 3;
constexpr std::uint32_t HashesCount = 2;
constexpr std::uint32_t RecordsCountZero = 0;

}   // namespace NKikimr::NOlap::NIndexes::NDefaults
