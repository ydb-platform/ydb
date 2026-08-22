#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <util/generic/fwd.h>

namespace NKikimrPQ {
class TMirrorPartitionConfig;
}

namespace NPersQueue {
class TTopicNameConverter;
using TTopicConverterPtr = std::shared_ptr<TTopicNameConverter>;
}

namespace NKikimr {

class TTabletCountersBase;

namespace NPQ {

class TPartitionId;

NActors::IActor* CreateMirrorer(const ui64 tabletId,
                                const NActors::TActorId& tabletActor,
                                const NActors::TActorId& partitionActor,
                                const NPersQueue::TTopicConverterPtr& topicConverter,
                                const ui32 partition,
                                const bool localDC,
                                const ui64 endOffset,
                                const NKikimrPQ::TMirrorPartitionConfig& config,
                                const TTabletCountersBase& counters);

} // namespace NPQ
} // namespace NKikimr
