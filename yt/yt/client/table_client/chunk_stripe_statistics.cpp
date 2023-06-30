#include "chunk_stripe_statistics.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

void TChunkStripeStatistics::Persist(const NTableClient::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ChunkCount);
    Persist(context, DataWeight);
    Persist(context, RowCount);
    Persist(context, ValueCount);
    Persist(context, MaxBlockSize);
}

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
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
