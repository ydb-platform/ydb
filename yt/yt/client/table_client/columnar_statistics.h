#pragma once

#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TLightweightColumnarStatistics
{
    //! Sum of per-column data weight for chunks whose meta contains columnar statistics.
    i64 ColumnDataWeightsSum = 0;
    //! Total weight of all write and delete timestamps.
    std::optional<i64> TimestampTotalWeight;
    //! Total data weight of legacy chunks whose meta misses columnar statistics.
    i64 LegacyChunkDataWeight = 0;
};

struct TNamedColumnarStatistics
{
    //! Per-column total data weight for chunks whose meta contains columnar statistics.
    THashMap<TString, i64> ColumnDataWeights;
    //! Total weight of all write and delete timestamps.
    std::optional<i64> TimestampTotalWeight;
    //! Total data weight of legacy chunks whose meta misses columnar statistics.
    i64 LegacyChunkDataWeight = 0;

    TNamedColumnarStatistics& operator +=(const TNamedColumnarStatistics& other);
};

//! TColumnarStatistics stores per-column statistics of data stored in a chunk/table.
struct TColumnarStatistics
{
    //! Per-column total data weight for chunks whose meta contains columnar statistics.
    std::vector<i64> ColumnDataWeights;
    //! Total weight of all write and delete timestamps.
    std::optional<i64> TimestampTotalWeight;
    //! Total data weight of legacy chunks whose meta misses columnar statistics.
    i64 LegacyChunkDataWeight = 0;

    //! Per-column approximate minimum non-null values.
    //! Can be missing for data produced before the introduction of value statistics.
    //! Stored value is guaranteed to be less than or equal to any value in the corresponding column,
    //! but it may not correspond to any real value in the column (e.g. strings may be approximated
    //! with shorter strings to reduce statistics size).
    //! If it is impossible to determine the minimum value, `EValueType::Min` is used.
    //! If there aren't non-null values in the column, it contains `EValueType::Null`.
    std::vector<TUnversionedOwningValue> ColumnMinValues;
    //! Per-column approximate maximum non-null values.
    //! Can be missing for data produced before the introduction of value statistics.
    //! Stored value is guaranteed to be greater than or equal to any value in the corresponding column,
    //! but it may not correspond to any real value in the column (e.g. strings may be approximated
    //! with shorter strings to reduce statistics size).
    //! If it is impossible to determine the maximum value, `EValueType::Max` is used.
    //! If there aren't non-null values in the column, it contains `EValueType::Null`.
    std::vector<TUnversionedOwningValue> ColumnMaxValues;
    //! Number of non-null values in each column.
    //! Can be missing for data produced before the introduction of value statistics.
    std::vector<i64> ColumnNonNullValueCounts;

    //! Total number of rows in all chunks whose meta contains columnar statistics.
    //! Can be missing only if the cluster version is 23.1 or older.
    std::optional<i64> ChunkRowCount = 0;
    //! Total number of rows in legacy chunks whose meta misses columnar statistics.
    //! Can be missing only if the cluster version is 23.1 or older.
    std::optional<i64> LegacyChunkRowCount = 0;

    TColumnarStatistics& operator+=(const TColumnarStatistics& other);
    bool operator==(const TColumnarStatistics& other) const = default;

    static TColumnarStatistics MakeEmpty(int columnCount, bool hasValueStatistics = true);
    static TColumnarStatistics MakeLegacy(int columnCount, i64 legacyChunkDataWeight, i64 legacyChunkRowCount);

    TLightweightColumnarStatistics MakeLightweightStatistics() const;

    TNamedColumnarStatistics MakeNamedStatistics(const std::vector<TString>& names) const;

    //! Checks if there are minimum, maximum, and non-null value statistics.
    bool HasValueStatistics() const;
    //! Clears minimum, maximum, and non-null value statistics.
    void ClearValueStatistics();

    int GetColumnCount() const;

    void Resize(int columnCount, bool keepValueStatistics = true);

    void Update(TRange<TUnversionedRow> rows);
    void Update(TRange<TVersionedRow> rows);

    TColumnarStatistics SelectByColumnNames(
        const TNameTablePtr& nameTable,
        const std::vector<TColumnStableName>& columnStableNames) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
