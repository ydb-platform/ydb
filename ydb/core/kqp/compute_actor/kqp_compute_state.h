#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

namespace NKikimr::NKqp::NComputeActor {

enum class EShardState: ui32 {
    Initial,
    Starting,
    Running,
    PostRunning, //We already receive all data, we has not processed it yet.
    Resolving,
};

bool FindSchemeErrorInIssues(const Ydb::StatusIds::StatusCode& status, const NYql::TIssues& issues);

class TCommonRetriesState {
public:
    ui32 RetryAttempt = 0;
    ui32 TotalRetries = 0;
    bool AllowInstantRetry = true;
    TDuration LastRetryDelay;
    TActorId RetryTimer;
    ui32 ResolveAttempt = 0;
    TDuration CalcRetryDelay();
    void ResetRetry();
};

struct TShardState: public TCommonRetriesState {
    using TPtr = std::shared_ptr<TShardState>;
    const ui64 TabletId;
    TSmallVec<TSerializedTableRange> Ranges;
    EShardState State = EShardState::Initial;
    ui32 Generation = 0;
    bool SubscribedOnTablet = false;
    TActorId ActorId;
    TOwnedCellVec LastKey;
    std::optional<ui32> AvailablePacks;

    TString PrintLastKey(TConstArrayRef<NScheme::TTypeInfo> keyTypes) const;

    TShardState(const ui64 tabletId);

    TString ToString(TConstArrayRef<NScheme::TTypeInfo> keyTypes) const;
    const TSmallVec<TSerializedTableRange> GetScanRanges(TConstArrayRef<NScheme::TTypeInfo> keyTypes) const;
    TString GetAddress() const;
};
}
