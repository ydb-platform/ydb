#pragma once

#include "public.h"
#include "serialize.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripeStatistics
{
    int ChunkCount = 0;
    i64 DataWeight = 0;
    i64 RowCount = 0;
    i64 ValueCount = 0;
    i64 MaxBlockSize = 0;

    void Persist(const TPersistenceContext& context);
};

TChunkStripeStatistics operator + (
    const TChunkStripeStatistics& lhs,
    const TChunkStripeStatistics& rhs);

TChunkStripeStatistics& operator += (
    TChunkStripeStatistics& lhs,
    const TChunkStripeStatistics& rhs);

using TChunkStripeStatisticsVector = TCompactVector<TChunkStripeStatistics, 1>;

//! Adds up input statistics and returns a single-item vector with the sum.
TChunkStripeStatisticsVector AggregateStatistics(
    const TChunkStripeStatisticsVector& statistics);

void Serialize(const TChunkStripeStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
