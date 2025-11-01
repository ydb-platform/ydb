#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <util/generic/fwd.h>

namespace NKikimrPQ {
class TPQTabletConfig;
}

namespace NPersQueue {
class TTopicNameConverter;
using TTopicConverterPtr = std::shared_ptr<TTopicNameConverter>;
}

namespace NKikimr {

class TTabletCountersBase;

namespace NJaegerTracing {
class TSamplingThrottlingControl;
}


namespace NPQ {

class TPartitionId;

NActors::IActor* CreatePartitionActor(ui64 tabletId,
                                      const TPartitionId& partition,
                                      const NActors::TActorId& tablet,
                                      ui32 tabletGeneration,
                                      const NActors::TActorId& blobCache,
                                      const NPersQueue::TTopicConverterPtr& topicConverter,
                                      TString dcId,
                                      bool isServerless,
                                      const NKikimrPQ::TPQTabletConfig& config,
                                      const std::shared_ptr<TTabletCountersBase>& counters,
                                      bool SubDomainOutOfSpace,
                                      ui32 numChannels,
                                      const NActors::TActorId& writeQuoterActorId,
                                      TIntrusivePtr<NJaegerTracing::TSamplingThrottlingControl> samplingControl,
                                      bool newPartition = false);

} // namespace NPQ
} // namespace NKikimr
