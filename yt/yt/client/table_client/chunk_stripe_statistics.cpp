#include "chunk_stripe_statistics.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TChunkStripeStatistics operator + (
    const TChunkStripeStatistics& lhs,
    const TChunkStripeStatistics& rhs)
{
    TChunkStripeStatistics result;
    result.ChunkCount = lhs.ChunkCount + rhs.ChunkCount;
    result.DataWeight = lhs.DataWeight + rhs.DataWeight;
    result.RowCount = lhs.RowCount + rhs.RowCount;
    result.ValueCount = lhs.ValueCount + rhs.ValueCount;
    result.MaxBlockSize = std::max(lhs.MaxBlockSize, rhs.MaxBlockSize);
    result.CompressedDataSize = lhs.CompressedDataSize + rhs.CompressedDataSize;
    return result;
}

TChunkStripeStatistics& operator += (
    TChunkStripeStatistics& lhs,
    const TChunkStripeStatistics& rhs)
{
    lhs.ChunkCount += rhs.ChunkCount;
    lhs.DataWeight += rhs.DataWeight;
    lhs.RowCount += rhs.RowCount;
    lhs.ValueCount += rhs.ValueCount;
    lhs.MaxBlockSize = std::max(lhs.MaxBlockSize, rhs.MaxBlockSize);
    lhs.CompressedDataSize += rhs.CompressedDataSize;
    return lhs;
}

TChunkStripeStatisticsVector AggregateStatistics(
    const TChunkStripeStatisticsVector& statistics)
{
    TChunkStripeStatistics sum;
    for (const auto& stat : statistics) {
        sum += stat;
    }
    return TChunkStripeStatisticsVector(1, sum);
}

void Serialize(const TChunkStripeStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("chunk_count").Value(statistics.ChunkCount)
            .Item("data_weight").Value(statistics.DataWeight)
            .Item("row_count").Value(statistics.RowCount)
            .OptionalItem("value_count", statistics.ValueCount)
            .OptionalItem("max_block_size", statistics.MaxBlockSize)
            .OptionalItem("compressed_data_size", statistics.CompressedDataSize)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
