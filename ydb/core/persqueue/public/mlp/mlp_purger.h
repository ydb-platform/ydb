#pragma once

#include "mlp.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/util/backoff.h>

#include <ydb/library/actors/core/events.h>

namespace NKikimr::NPQ::NMLP {

class TPurgerActor : public TBaseActor<TPurgerActor>
                   , public TConstantLogPrefix {

public:
    TPurgerActor(const TActorId& parentId, const TPurgerSettings& settings);

    void Bootstrap();
    void PassAway() override;

private:
    enum class EPartitionStatus {
        NotStarted,
        InProgress,
        Success,
        Error,
    };
    struct TPartitionStatus {
        ui64 TabletId;
        EPartitionStatus Status = EPartitionStatus::NotStarted;
        ui64 Cookie = 0;
        bool WaitRetry = false;
        TBackoff Backoff = TBackoff(5, TDuration::MilliSeconds(25));
    };

    void DoDescribe();
    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr&);
    STFUNC(DescribeState);

    void DoPurge();
    void Handle(TEvPQ::TEvMLPPurgeResponse::TPtr&);
    void Handle(TEvPQ::TEvMLPErrorResponse::TPtr&);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&);
    void Handle(TEvents::TEvWakeup::TPtr&);
    STFUNC(PurgeState);

    void RequestPartitionIfNeeded(ui32 partitionId, TPartitionStatus& status);
    void RetryIfPossible(ui32 partitionId, TPartitionStatus& status);
    void ReplyIfPossible();

    void SendToTablet(ui64 tabletId, IEventBase *ev, ui64 cookie);
    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage);

    bool OnUnhandledException(const std::exception&) override;

private:
    const TActorId ParentId;
    const TPurgerSettings Settings;

    TActorId ChildActorId;

    NDescriber::TTopicInfo TopicInfo;
    size_t PendingPartitions = 0;
    size_t PendingRetries = 0;
    size_t NextCookie = 0;
    absl::flat_hash_map<ui32, TPartitionStatus> Partitions;
    absl::flat_hash_map<ui64, ui64> TabletCookies;
};

} // namespace NKikimr::NPQ::NMLP
