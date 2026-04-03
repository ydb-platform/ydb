#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <util/generic/string.h>

namespace NKikimr::NPQ {

    NActors::IActor* CreateDeduplicationWriteQueueActor(
        ui64 tabletId,
        NActors::TActorId tabletActorId,
        NActors::TActorId partitionActorId,
        TString topicName,
        ui32 partitionId,
        TVector<NKikimrPQ::TPQTabletConfig::TPartition> parentPartitions);

    namespace NPrivate {
        enum class EBypassMode {
            Disabled,
            Pending, // wait for queue
            Enabled,
        };
    }
} // namespace NKikimr::NPQ
