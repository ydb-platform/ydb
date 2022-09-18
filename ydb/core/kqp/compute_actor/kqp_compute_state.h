#pragma once

#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/kqp_compute.h>
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

class TShardCostsState: public TCommonRetriesState {
private:
    const ui32 ScanId;
    const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::TReadOpMeta* ReadData;
public:
    using TPtr = std::shared_ptr<TShardCostsState>;
    const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::TReadOpMeta& GetReadData() const {
        return *ReadData;
    }

    ui32 GetScanId() const {
        return ScanId;
    }

    ui64 GetShardId() const {
        return ReadData->GetShardId();
    }

    TShardCostsState(const ui32 scanId, const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta::TReadOpMeta* readData)
        : ScanId(scanId)
        , ReadData(readData)
    {

    }
};

struct TShardState: public TCommonRetriesState {
    using TPtr = std::shared_ptr<TShardState>;
    const ui64 TabletId;
    const ui32 ScannerIdx = 0;
    TSmallVec<TSerializedTableRange> Ranges;
    EShardState State = EShardState::Initial;
    ui32 Generation = 0;
    bool SubscribedOnTablet = false;
    TActorId ActorId;
    TOwnedCellVec LastKey;

    TString PrintLastKey(TConstArrayRef<NScheme::TTypeId> keyTypes) const;

    TShardState(const ui64 tabletId, const ui32 scanIdx)
        : TabletId(tabletId)
        , ScannerIdx(scanIdx) {
    }

    TString ToString(TConstArrayRef<NScheme::TTypeId> keyTypes) const;
    const TSmallVec<TSerializedTableRange> GetScanRanges(TConstArrayRef<NScheme::TTypeId> keyTypes) const;
    TString GetAddress() const;
};
}
