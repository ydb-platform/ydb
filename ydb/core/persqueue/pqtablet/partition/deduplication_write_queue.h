#pragma once

#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/actors/core/actor.h>
#include <util/generic/string.h>

namespace NKikimr::NPQ {

    struct TCreateDeduplicationWriteQueueActorResult {
        size_t RecentPartitionsCount = 0;
        TInstant DisableTimestamp;
        THolder<NActors::IActor> Actor;
    };

    TCreateDeduplicationWriteQueueActorResult CreateDeduplicationWriteQueueActor(
        ui64 tabletId,
        NActors::TActorId tabletActorId,
        NActors::TActorId partitionActorId,
        TString topicName,
        ui32 partitionId,
        const TPartitionGraph& partitionGraph);

    namespace NPrivate {
        enum class EBypassMode {
            Disabled,
            Pending, // wait for queue
            Enabled,
        };
    }
} // namespace NKikimr::NPQ
