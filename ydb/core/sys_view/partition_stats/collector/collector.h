#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr {
namespace NSysView {

constexpr size_t STATS_COLLECTOR_BATCH_SIZE = 5000;
constexpr size_t STATS_COLLECTOR_QUEUE_SIZE_LIMIT = 10;

THolder<NActors::IActor> CreatePartitionStatsCollector(
    size_t batchSize = STATS_COLLECTOR_BATCH_SIZE,
    size_t pendingRequestsCount = STATS_COLLECTOR_QUEUE_SIZE_LIMIT);

} // NSysView
} // NKikimr
