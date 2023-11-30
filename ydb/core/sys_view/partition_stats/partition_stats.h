#pragma once

#include <ydb/core/kqp/runtime/kqp_compute.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr {
namespace NSysView {

constexpr size_t STATS_COLLECTOR_BATCH_SIZE = 5000;
constexpr size_t STATS_COLLECTOR_QUEUE_SIZE_LIMIT = 10;

THolder<NActors::IActor> CreatePartitionStatsCollector(
    size_t batchSize = STATS_COLLECTOR_BATCH_SIZE,
    size_t pendingRequestsCount = STATS_COLLECTOR_QUEUE_SIZE_LIMIT);

THolder<NActors::IActor> CreatePartitionStatsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns);

} // NSysView
} // NKikimr
