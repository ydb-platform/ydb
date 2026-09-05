#pragma once

#include "set_offsets.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/common/actor.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/actors/core/events.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

namespace NKikimr::NPQ::NSetOffsets {

class TSetOffsetsActor : public TBaseActor<TSetOffsetsActor>
                        , public TConstantLogPrefix {
public:
    TSetOffsetsActor(const TActorId& parentId, const TSetOffsetsSettings& settings);

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
        ui64 TabletId = 0;
        EPartitionStatus Status = EPartitionStatus::NotStarted;
        ui64 Cookie = 0;
        bool WaitRetry = false;
        TBackoff Backoff = TBackoff(5, TDuration::MilliSeconds(25));
        Ydb::StatusIds::StatusCode ErrorStatus = Ydb::StatusIds::GENERIC_ERROR;
        TString Error;
    };

    void DoDescribe();
    void Handle(NDescriber::TEvDescribeTopicsResponse::TPtr&);
    STFUNC(DescribeState);

    void DoSet();
    void Handle(TEvPQ::TEvSetOffsetsResponse::TPtr&);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr&);
    void Handle(TEvents::TEvWakeup::TPtr&);
    STFUNC(SetState);

    void RequestPartitionIfNeeded(ui32 partitionId, TPartitionStatus& status);
    void RetryIfPossible(ui32 partitionId, TPartitionStatus& status);
    void MarkPartitionSuccess(TPartitionStatus& status);
    void ReplyIfPossible();

    void SendToTablet(ui64 tabletId, IEventBase* ev, ui64 cookie);
    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode errorCode, TString&& errorMessage);
    void ReplyResultAndDie();

    bool OnUnhandledException(const std::exception&) override;

private:
    const TActorId ParentId;
    const TSetOffsetsSettings Settings;

    TActorId ChildActorId;
    TString ResolvedConsumer;

    NDescriber::TTopicInfo TopicInfo;
    size_t PendingPartitions = 0;
    size_t PendingRetries = 0;
    size_t NextCookie = 0;
    absl::flat_hash_map<ui32, TPartitionStatus> Partitions;
    absl::flat_hash_map<ui64, ui64> TabletCookies;
};

} // namespace NKikimr::NPQ::NSetOffsets
