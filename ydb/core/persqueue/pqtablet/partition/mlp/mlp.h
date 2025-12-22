#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/core/persqueue/public/config.h>
#include <ydb/core/tablet/tablet_counters.h>

#include <util/system/types.h>

#include <optional>


class TDuration;

namespace NKikimr::NPQ::NMLP {

struct TMetrics {
    size_t InflightMessageCount = 0;
    size_t UnprocessedMessageCount = 0;
    size_t LockedMessageCount = 0;
    size_t LockedMessageGroupCount = 0;
    size_t DelayedMessageCount = 0;
    size_t CommittedMessageCount = 0;
    size_t DeadlineExpiredMessageCount = 0;
    size_t DLQMessageCount = 0;

    size_t TotalCommittedMessageCount = 0;
    size_t TotalMovedToDLQMessageCount = 0;
    size_t TotalScheduledToDLQMessageCount = 0;
    size_t TotalPurgedMessageCount = 0; // TODO MLP
    size_t TotalDeletedByDeadlinePolicyMessageCount = 0;
    size_t TotalDeletedByRetentionMessageCount = 0;

    // stores how many times messages were locked
    TTabletPercentileCounter MessageLocks;
    // stores the duration of message locking
    TTabletPercentileCounter MessageLockingDuration;
};

// MLP не работает если включена компактифкация по ключу!!! (иначе не понятно как прореживать скомпакченные значения)
NActors::IActor* CreateConsumerActor(
    const TString& database,
    ui64 tabletId,
    const NActors::TActorId& tabletActorId,
    ui32 partitionId,
    const NActors::TActorId& partitionActorId,
    const NKikimrPQ::TPQTabletConfig_TConsumer& config,
    const std::optional<TDuration> retentionPeriod,
    ui64 partitionEndOffset
);

TString MakeSnapshotKey(ui32 partitionId, const TString& consumerName);
TString MinWALKey(ui32 partitionId, const TString& consumerName);
TString MaxWALKey(ui32 partitionId, const TString& consumerName);

} // namespace NKikimr::NPQ::NMLP
