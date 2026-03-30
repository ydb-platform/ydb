#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <util/generic/string.h>

namespace NKikimr::NPQ {

NActors::IActor* CreateDeduplicationWriteQueueActor(
        NActors::TActorId partitionActorId,
        TString topicName,
        ui32 partitionId,
        TVector<NKikimrPQ::TPQTabletConfig::TPartition> parentPartitions);

} // namespace NKikimr::NPQ
