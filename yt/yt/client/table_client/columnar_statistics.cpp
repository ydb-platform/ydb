#include "columnar_statistics.h"

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

TColumnarStatistics& TColumnarStatistics::operator +=(const TColumnarStatistics& other)
{
    if (ColumnDataWeights.empty()) {
        ColumnDataWeights = other.ColumnDataWeights;
    } else if (!other.ColumnDataWeights.empty()) {
        YT_VERIFY(ColumnDataWeights.size() == other.ColumnDataWeights.size());
        for (int index = 0; index < std::ssize(ColumnDataWeights); ++index) {
            ColumnDataWeights[index] += other.ColumnDataWeights[index];
        }
    }
    if (other.TimestampTotalWeight) {
        TimestampTotalWeight = TimestampTotalWeight.value_or(0) + *other.TimestampTotalWeight;
    }
    LegacyChunkDataWeight += other.LegacyChunkDataWeight;
    return *this;
}

TColumnarStatistics TColumnarStatistics::MakeEmpty(int columnCount)
{
    return TColumnarStatistics{std::vector<i64>(columnCount, 0), std::nullopt, 0};
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
