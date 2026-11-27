#pragma once

namespace NKikimr::NTable {

// Default false positive probability for bloom filters when not explicitly specified
inline constexpr double DefaultBloomFilterFpp = 0.0001;

} // namespace NKikimr::NTable
