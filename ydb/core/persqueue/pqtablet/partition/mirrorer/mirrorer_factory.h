#pragma once

#include <ydb/core/persqueue/public/nameresolver/nameresolver.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <util/generic/fwd.h>

namespace NKikimrPQ {
class TMirrorPartitionConfig;
}

namespace NKikimr {

class TTabletCountersBase;

namespace NPQ {

class TPartitionId;

NActors::IActor* CreateMirrorer(const ui64 tabletId,
                                const NActors::TActorId& tabletActor,
                                const NActors::TActorId& partitionActor,
                                const NKikimr::NPQ::NNameResolver::TTopicNamesPtr& topicConverter,
                                const ui32 partition,
                                const bool localDC,
                                const ui64 endOffset,
                                const NKikimrPQ::TMirrorPartitionConfig& config,
                                const TTabletCountersBase& counters);

} // namespace NPQ
} // namespace NKikimr
