#include "columnar_statistics.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <library/cpp/iterator/functools.h>

#include <numeric>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TNamedColumnarStatistics& TNamedColumnarStatistics::operator+=(const TNamedColumnarStatistics& other)
{
    for (const auto& [columnName, dataWeight] : other.ColumnDataWeights) {
        ColumnDataWeights[columnName] += dataWeight;
    }
    if (other.TimestampTotalWeight) {
        TimestampTotalWeight = TimestampTotalWeight.value_or(0) + *other.TimestampTotalWeight;
    }
    LegacyChunkDataWeight += other.LegacyChunkDataWeight;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr size_t MaxStringValueLength = 100;
constexpr auto NullUnversionedValue = MakeNullValue<TUnversionedValue>();

//! Approximates long string values with shorter but lexicographically less ones. Other values are intact.
TUnversionedOwningValue ApproximateMinValue(TUnversionedValue value)
{
    if (value.Type != EValueType::String || value.Length <= MaxStringValueLength) {
        value.Flags = EValueFlags::None;
        value.Id = 0;
        return value;
    }
    return MakeUnversionedStringValue(value.AsStringBuf().SubString(0, MaxStringValueLength));
}

//! Approximates long string values with shorter but lexicographically greater ones. Other values are intact.
TUnversionedOwningValue ApproximateMaxValue(TUnversionedValue value)
{
    if (value.Type != EValueType::String || value.Length <= MaxStringValueLength) {
        value.Flags = EValueFlags::None;
        value.Id = 0;
        return value;
    }

    const char MaxChar = std::numeric_limits<unsigned char>::max();
    auto truncatedStringBuf = value.AsStringBuf().SubString(0, MaxStringValueLength);

    while (!truncatedStringBuf.empty() && truncatedStringBuf.back() == MaxChar) {
        truncatedStringBuf.remove_suffix(1);
    }

    if (truncatedStringBuf.empty()) {
        // String was larger than any possible approximation.
        return MakeSentinelValue<TUnversionedValue>(EValueType::Max);
    } else {
        TUnversionedOwningValue result = MakeUnversionedStringValue(truncatedStringBuf);
        char* mutableString = result.GetMutableString();
        int lastIndex = truncatedStringBuf.size() - 1;
        mutableString[lastIndex] = static_cast<unsigned char>(mutableString[lastIndex]) + 1;
        return result;
    }
}

template <typename TRow>
void UpdateColumnarStatistics(TColumnarStatistics& statistics, TRange<TRow> rows)
{
    int maxId = -1;
    for (const auto& values : rows) {
        for (const auto& value : values) {
            maxId = std::max<int>(maxId, value.Id);
        }
    }
    if (maxId >= statistics.GetColumnCount()) {
        statistics.Resize(maxId + 1);
    }

    for (const auto& values : rows) {
        for (const auto& value : values) {
            statistics.ColumnDataWeights[value.Id] += GetDataWeight(value);
        }
    }

    if (!statistics.HasValueStatistics()) {
        return;
    }

    // Vectors for precalculation of minimum and maximum values from rows that we are adding.
    std::vector<TUnversionedValue> minValues(maxId + 1, NullUnversionedValue);
    std::vector<TUnversionedValue> maxValues(maxId + 1, NullUnversionedValue);

    for (const auto& row : rows) {
        for (const auto& value : row) {
            if (value.Type == EValueType::Composite || value.Type == EValueType::Any) {
                // Composite and YSON values are not comparable.
                minValues[value.Id] = MakeSentinelValue<TUnversionedValue>(EValueType::Min);
                maxValues[value.Id] = MakeSentinelValue<TUnversionedValue>(EValueType::Max);
            } else if (value.Type != EValueType::Null) {
                if (minValues[value.Id] == NullUnversionedValue || value < minValues[value.Id]) {
                    minValues[value.Id] = value;
                }
                if (maxValues[value.Id] == NullUnversionedValue || value > maxValues[value.Id]) {
                    maxValues[value.Id] = value;
                }
            }
        }
    }

    for (int index = 0; index <= maxId; ++index) {

        if (minValues[index] != NullUnversionedValue &&
            (statistics.ColumnMinValues[index] == NullUnversionedValue || statistics.ColumnMinValues[index] > minValues[index]))
        {
            statistics.ColumnMinValues[index] = ApproximateMinValue(minValues[index]);
        }

        if (maxValues[index] != NullUnversionedValue &&
            (statistics.ColumnMaxValues[index] == NullUnversionedValue || statistics.ColumnMaxValues[index] < maxValues[index]))
        {
            statistics.ColumnMaxValues[index] = ApproximateMaxValue(maxValues[index]);
        }
    }

    for (const auto& row : rows) {
        for (const auto& value : row) {
            statistics.ColumnNonNullValueCounts[value.Id] += (value.Type != EValueType::Null);
        }
    }
}

} // namespace

TColumnarStatistics& TColumnarStatistics::operator+=(const TColumnarStatistics& other)
{
    if (GetColumnCount() == 0) {
        Resize(other.GetColumnCount(), other.HasValueStatistics());
    }

    YT_VERIFY(GetColumnCount() == other.GetColumnCount());

    for (int index = 0; index < GetColumnCount(); ++index) {
        ColumnDataWeights[index] += other.ColumnDataWeights[index];
    }
    if (other.TimestampTotalWeight) {
        TimestampTotalWeight = TimestampTotalWeight.value_or(0) + *other.TimestampTotalWeight;
    }
    LegacyChunkDataWeight += other.LegacyChunkDataWeight;

    if (ChunkRowCount.has_value() && other.ChunkRowCount.has_value()) {
        ChunkRowCount = *ChunkRowCount + *other.ChunkRowCount;
    } else {
        ChunkRowCount.reset();
    }
    if (LegacyChunkRowCount.has_value() && other.LegacyChunkRowCount.has_value()) {
        LegacyChunkRowCount = *LegacyChunkRowCount + *other.LegacyChunkRowCount;
    } else {
        LegacyChunkRowCount.reset();
    }

    if (!other.HasValueStatistics()) {
        ClearValueStatistics();
    } else if (HasValueStatistics()) {
        for (int index = 0; index < GetColumnCount(); ++index) {

            if (other.ColumnMinValues[index] != NullUnversionedValue &&
                (ColumnMinValues[index] == NullUnversionedValue || ColumnMinValues[index] > other.ColumnMinValues[index]))
            {
                ColumnMinValues[index] = other.ColumnMinValues[index];
            }

            if (other.ColumnMaxValues[index] != NullUnversionedValue &&
                (ColumnMaxValues[index] == NullUnversionedValue || ColumnMaxValues[index] < other.ColumnMaxValues[index]))
            {
                ColumnMaxValues[index] = other.ColumnMaxValues[index];
            }

            ColumnNonNullValueCounts[index] += other.ColumnNonNullValueCounts[index];
        }
    }
    return *this;
}

TColumnarStatistics TColumnarStatistics::MakeEmpty(int columnCount, bool hasValueStatistics)
{
    TColumnarStatistics result;
    result.Resize(columnCount, hasValueStatistics);
    return result;
}

TColumnarStatistics TColumnarStatistics::MakeLegacy(int columnCount, i64 legacyChunkDataWeight, i64 legacyChunkRowCount)
{
    TColumnarStatistics result = MakeEmpty(columnCount, /*hasValueStatistics*/ false);
    result.LegacyChunkDataWeight = legacyChunkDataWeight;
    result.LegacyChunkRowCount = legacyChunkRowCount;
    return result;
}

TLightweightColumnarStatistics TColumnarStatistics::MakeLightweightStatistics() const
{
    return TLightweightColumnarStatistics{
        .ColumnDataWeightsSum = std::accumulate(ColumnDataWeights.begin(), ColumnDataWeights.end(), (i64)0),
        .TimestampTotalWeight = TimestampTotalWeight,
        .LegacyChunkDataWeight = LegacyChunkDataWeight
    };
}

TNamedColumnarStatistics TColumnarStatistics::MakeNamedStatistics(const std::vector<TString>& names) const
{
    TNamedColumnarStatistics result;
    result.TimestampTotalWeight = TimestampTotalWeight;
    result.LegacyChunkDataWeight = LegacyChunkDataWeight;

    for (const auto& [name, columnDataWeight] : Zip(names, ColumnDataWeights)) {
        result.ColumnDataWeights[name] = columnDataWeight;
    }

    return result;
}

bool TColumnarStatistics::HasValueStatistics() const
{
    YT_VERIFY(ColumnMinValues.size() == ColumnMaxValues.size());
    YT_VERIFY(ColumnMinValues.size() == ColumnNonNullValueCounts.size());

    return GetColumnCount() == 0 || !ColumnMinValues.empty();
}

void TColumnarStatistics::ClearValueStatistics()
{
    ColumnMinValues.clear();
    ColumnMaxValues.clear();
    ColumnNonNullValueCounts.clear();
}

int TColumnarStatistics::GetColumnCount() const
{
    return ColumnDataWeights.size();
}

void TColumnarStatistics::Resize(int columnCount, bool keepValueStatistics)
{
    keepValueStatistics &= HasValueStatistics();

    ColumnDataWeights.resize(columnCount, 0);

    if (keepValueStatistics) {
        ColumnMinValues.resize(columnCount, NullUnversionedValue);
        ColumnMaxValues.resize(columnCount, NullUnversionedValue);
        ColumnNonNullValueCounts.resize(columnCount, 0);
    } else {
        ClearValueStatistics();
    }
}

void TColumnarStatistics::Update(TRange<TUnversionedRow> rows)
{
    UpdateColumnarStatistics(*this, rows);

    if (ChunkRowCount) {
        ChunkRowCount = *ChunkRowCount + rows.Size();
    }
}

void TColumnarStatistics::Update(TRange<TVersionedRow> rows)
{
    std::vector<TRange<TUnversionedValue>> keyColumnRows;
    keyColumnRows.reserve(rows.Size());
    for (const auto& row : rows) {
        keyColumnRows.emplace_back(row.Keys());
    }
    UpdateColumnarStatistics(*this, TRange(keyColumnRows));

    std::vector<TRange<TVersionedValue>> valueColumnRows;
    valueColumnRows.reserve(rows.Size());
    for (const auto& row : rows) {
        valueColumnRows.emplace_back(row.Values());
    }
    UpdateColumnarStatistics(*this, TRange(valueColumnRows));

    for (const auto& row : rows) {
        TimestampTotalWeight = TimestampTotalWeight.value_or(0) +
            (row.GetWriteTimestampCount() + row.GetDeleteTimestampCount()) * sizeof(TTimestamp);
    }

    if (ChunkRowCount) {
        ChunkRowCount = *ChunkRowCount + rows.Size();
    }
}

TColumnarStatistics TColumnarStatistics::SelectByColumnNames(const TNameTablePtr& nameTable, const std::vector<TColumnStableName>& columnStableNames) const
{
    auto result = MakeEmpty(columnStableNames.size(), HasValueStatistics());

    for (const auto& [columnIndex, columnName] : Enumerate(columnStableNames)) {
        if (auto id = nameTable->FindId(columnName.Underlying()); id && *id < GetColumnCount()) {
            result.ColumnDataWeights[columnIndex] = ColumnDataWeights[*id];

            if (HasValueStatistics()) {
                result.ColumnMinValues[columnIndex] = ColumnMinValues[*id];
                result.ColumnMaxValues[columnIndex] = ColumnMaxValues[*id];
                result.ColumnNonNullValueCounts[columnIndex] = ColumnNonNullValueCounts[*id];
            }
        }
    }
    result.TimestampTotalWeight = TimestampTotalWeight;
    result.LegacyChunkDataWeight = LegacyChunkDataWeight;

    result.ChunkRowCount = ChunkRowCount;
    result.LegacyChunkRowCount = LegacyChunkRowCount;

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
