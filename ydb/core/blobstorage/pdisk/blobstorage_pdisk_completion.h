#pragma once

#include <ydb/library/pdisk_io/aio.h>

#include <util/system/hp_timer.h>
#include <util/generic/string.h>
#include <library/cpp/lwtrace/shuttle.h>
#include <ydb/library/actors/wilson/wilson_span.h>

namespace NKikimr::NPDisk {

struct TCompletionAction {
    ui64 OperationIdx;
    NHPTimer::STime SubmitTime;
    NHPTimer::STime GetTime;
    TCompletionAction *FlushAction = nullptr;
    ui64 CostNs = 0;
    NWilson::TTraceId TraceId;
    EIoResult Result = EIoResult::Unknown;
    TString ErrorReason;
    // Only reads should be executed in a separate thread since their completions consist of
    // time-consuming deciphering of read data. But currently some completion actions can write
    // to BlockDevice from Exec() and it's more safe to use WhiteList to allow only
    // LogWrite and ChunkWrite to be executed from GetThread
    bool ShouldBeExecutedInCompletionThread = true;
    bool CanBeExecutedInAdditionalCompletionThread = false;

    mutable NLWTrace::TOrbit Orbit;
protected:
    TVector<ui64> BadOffsets;

public:
    void SetResult(const EIoResult result) {
        Result = result;
        if (FlushAction) {
            FlushAction->SetResult(result);
        }
    }

    void SetErrorReason(const TString& errorReason) {
        ErrorReason = errorReason;
        if (FlushAction) {
            FlushAction->SetErrorReason(errorReason);
        }
    }

    void RegisterBadOffset(ui64 offset) {
        BadOffsets.push_back(offset);
    }

    virtual bool CanHandleResult() const {
        return Result == EIoResult::Ok;
    }
    virtual void Exec(TActorSystem *actorSystem) = 0;
    virtual void Release(TActorSystem *) = 0;
    virtual ~TCompletionAction() {}
};

}
