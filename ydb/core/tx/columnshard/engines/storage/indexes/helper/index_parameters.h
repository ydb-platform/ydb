#pragma once

namespace NKikimr::NOlap::NIndexes::NIndexParameters {

// Common
inline constexpr const char* ColumnName = "column_name";

// Bloom filter
inline constexpr const char* FalsePositiveProbability = "false_positive_probability";
inline constexpr const char* CaseSensitive = "case_sensitive";

// Bloom ngram filter
inline constexpr const char* NGrammSize = "ngramm_size";
inline constexpr const char* HashesCount = "hashes_count";
inline constexpr const char* FilterSizeBytes = "filter_size_bytes";
inline constexpr const char* RecordsCount = "records_count";

}   // namespace NKikimr::NOlap::NIndexes::NIndexParameters
