#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <util/generic/fwd.h>

namespace NKikimrPQ {
class TPQTabletConfig;
}

namespace NPersQueue {
class TTopicConverterPtr;
}

namespace NKikimr {

class TTabletCountersBase;

namespace NPQ {

class TPartitionId;

namespace NJaegerTracing {
class TSamplingThrottlingControl;
}

NActors::IActor* CreatePartitionActor(ui64 tabletId,
                                      const TPartitionId& partition,
                                      const NActors::TActorId& tablet,
                                      ui32 tabletGeneration,
                                      const NActors::TActorId& blobCache,
                                      const NPersQueue::TTopicConverterPtr& topicConverter,
                                      TString dcId,
                                      bool isServerless,
                                      const NKikimrPQ::TPQTabletConfig& config,
                                      const TTabletCountersBase& counters,
                                      bool SubDomainOutOfSpace,
                                      ui32 numChannels,
                                      const NActors::TActorId& writeQuoterActorId,
                                      TIntrusivePtr<NJaegerTracing::TSamplingThrottlingControl> samplingControl,
                                      bool newPartition = false);

} // namespace NPQ
} // namespace NKikimr
