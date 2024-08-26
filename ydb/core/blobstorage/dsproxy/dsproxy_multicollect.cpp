#include "dsproxy.h"
#include "dsproxy_mon.h"
#include <ydb/core/blobstorage/base/utility.h>
#include <util/generic/set.h>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MULTI COLLECT request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TBlobStorageGroupMultiCollectRequest : public TBlobStorageGroupRequestActor {
    struct TRequestInfo {
        bool IsReplied;
    };

    const ui64 Iterations;

    const ui64 TabletId;
    const ui32 RecordGeneration;
    const ui32 PerGenerationCounter; // monotone increasing cmd counter for RecordGeneration
    const ui32 Channel;

    const std::unique_ptr<TVector<TLogoBlobID> > Keep;
    const std::unique_ptr<TVector<TLogoBlobID> > DoNotKeep;
    const TInstant Deadline;

    const ui32 CollectGeneration;
    const ui32 CollectStep;
    const bool Hard;
    const bool Collect;
    const bool Decommission;

    ui64 FlagRequestsInFlight;
    ui64 CollectRequestsInFlight;

    TInstant StartTime;

    TStackVec<TRequestInfo, TypicalDisksInGroup> RequestInfos;

    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr &ev) {
        const TEvBlobStorage::TEvCollectGarbageResult &res = *ev->Get();
        A_LOG_LOG_S(PriorityForStatusResult(res.Status), "BPMC1", "Handle TEvCollectGarbageResult"
            << " status# " << NKikimrProto::EReplyStatus_Name(res.Status)
            << " FlagRequestsInFlight# " << FlagRequestsInFlight
            << " CollectRequestsInFlight " << CollectRequestsInFlight);

        if (res.Status != NKikimrProto::OK) {
            ReplyAndDie(res.Status);
            return;
        }

        Y_ABORT_UNLESS(ev->Cookie < RequestInfos.size());
        TRequestInfo &info = RequestInfos[ev->Cookie];
        Y_ABORT_UNLESS(!info.IsReplied);
        info.IsReplied = true;

        if (FlagRequestsInFlight) {
            FlagRequestsInFlight--;
            if (FlagRequestsInFlight == 0) {
                if (Collect) {
                    SendRequest(Iterations - 1, true);
                } else {
                    ReplyAndDie(NKikimrProto::OK);
                }
            }
        } else {
            CollectRequestsInFlight--;
            Y_ABORT_UNLESS(CollectRequestsInFlight == 0);
            ReplyAndDie(NKikimrProto::OK);
        }
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status) override {
        std::unique_ptr<TEvBlobStorage::TEvCollectGarbageResult> ev(new TEvBlobStorage::TEvCollectGarbageResult(
            status, TabletId, RecordGeneration, PerGenerationCounter, Channel));
        ev->ErrorReason = ErrorReason;
        SendResponseAndDie(std::move(ev));
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32) override {
        Y_ABORT();
    }

public:
    ::NMonitoring::TDynamicCounters::TCounterPtr& GetActiveCounter() const override {
        return Mon->ActiveMultiCollect;
    }

    ERequestType GetRequestType() const override {
        return ERequestType::CollectGarbage;
    }

    TBlobStorageGroupMultiCollectRequest(TBlobStorageGroupMultiCollectParameters& params)
        : TBlobStorageGroupRequestActor(params)
        , Iterations(params.Common.Event->PerGenerationCounterStepSize())
        , TabletId(params.Common.Event->TabletId)
        , RecordGeneration(params.Common.Event->RecordGeneration)
        , PerGenerationCounter(params.Common.Event->PerGenerationCounter)
        , Channel(params.Common.Event->Channel)
        , Keep(params.Common.Event->Keep.Release())
        , DoNotKeep(params.Common.Event->DoNotKeep.Release())
        , Deadline(params.Common.Event->Deadline)
        , CollectGeneration(params.Common.Event->CollectGeneration)
        , CollectStep(params.Common.Event->CollectStep)
        , Hard(params.Common.Event->Hard)
        , Collect(params.Common.Event->Collect)
        , Decommission(params.Common.Event->Decommission)
        , FlagRequestsInFlight(0)
        , CollectRequestsInFlight(0)
        , StartTime(params.Common.Now)
    {
        Y_ABORT_UNLESS(Iterations > 1);
    }

    void SendRequest(ui64 idx, bool withCollect) {
        ui64 cookie = RequestInfos.size();
        RequestInfos.push_back({false});

        bool isCollect = withCollect && Collect;
        std::unique_ptr<TVector<TLogoBlobID>> keepPart;
        std::unique_ptr<TVector<TLogoBlobID>> doNotKeepPart;
        ui64 keepCount = Keep ? Keep->size() : 0;
        ui64 doNotKeepCount = DoNotKeep ? DoNotKeep->size() : 0;
        ui64 keepPartCount = 0;
        ui64 doNotKeepPartCount = 0;
        ui64 begin = idx * MaxCollectGarbageFlagsPerMessage;
        ui64 end = begin + MaxCollectGarbageFlagsPerMessage;
        if (begin < keepCount) {
            keepPartCount = Min(MaxCollectGarbageFlagsPerMessage, keepCount - begin);
            keepPart.reset(new TVector<TLogoBlobID>(keepPartCount));
            for (ui64 keepIdx = 0; keepIdx < keepPartCount; ++keepIdx) {
                (*keepPart)[keepIdx] = (*Keep)[begin + keepIdx];
            }
            if (end > keepCount) {
                doNotKeepPartCount = Min(doNotKeepCount, MaxCollectGarbageFlagsPerMessage - keepPartCount);
                doNotKeepPart.reset(new TVector<TLogoBlobID>(doNotKeepPartCount));
                for (ui64 doNotKeepIdx = 0; doNotKeepIdx < doNotKeepPartCount; ++doNotKeepIdx) {
                    (*doNotKeepPart)[doNotKeepIdx] = (*DoNotKeep)[doNotKeepIdx];
                }
            }
        } else {
            ui64 doNotKeepBegin = begin - keepCount;
            doNotKeepPartCount = Min(MaxCollectGarbageFlagsPerMessage, doNotKeepCount - doNotKeepBegin);
            doNotKeepPart.reset(new TVector<TLogoBlobID>(doNotKeepPartCount));
            for (ui64 doNotKeepIdx = 0; doNotKeepIdx < doNotKeepPartCount; ++doNotKeepIdx) {
                (*doNotKeepPart)[doNotKeepIdx] = (*DoNotKeep)[doNotKeepBegin + doNotKeepIdx];
            }
        }

        std::unique_ptr<TEvBlobStorage::TEvCollectGarbage> ev(new TEvBlobStorage::TEvCollectGarbage(
            TabletId, RecordGeneration, PerGenerationCounter, Channel,
            isCollect, CollectGeneration, CollectStep, keepPart.release(), doNotKeepPart.release(), Deadline, false,
            Hard));
        ev->Decommission = Decommission; // retain decommission flag
        R_LOG_DEBUG_S("BPMC3", "SendRequest idx# " << idx
            << " withCollect# " << withCollect
            << " isCollect# " << isCollect
            << " ev# " << ev->ToString());
        SendToProxy(std::move(ev), cookie, Span.GetTraceId());

        Y_ABORT_UNLESS(idx < Iterations - 1 ? !isCollect : isCollect || !Collect);

        if (isCollect) {
            Y_ABORT_UNLESS(!FlagRequestsInFlight);
            CollectRequestsInFlight++;
        } else {
            FlagRequestsInFlight++;
        }
    }

    void Bootstrap() override {
        A_LOG_INFO_S("BPMC4", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " TabletId# " << TabletId
            << " Channel# " << Channel
            << " RecordGeneration# " << RecordGeneration
            << " PerGenerationCounter# " << PerGenerationCounter
            << " Deadline# " << Deadline
            << " CollectGeneration# " << CollectGeneration
            << " CollectStep# " << CollectStep
            << " Collect# " << (Collect ? "true" : "false")
            << " Hard# " << (Hard ? "true" : "false"));

        for (const auto& item : Keep ? *Keep : TVector<TLogoBlobID>()) {
            A_LOG_INFO_S("BPMC5", "Keep# " << item);
        }

        for (const auto& item : DoNotKeep ? *DoNotKeep : TVector<TLogoBlobID>()) {
            A_LOG_INFO_S("BPMC6", "DoNotKeep# " << item);
        }

        for (ui64 idx = 0; idx < Iterations - (Collect ? 1 : 0); ++idx) {
            SendRequest(idx, false);
        }
        Become(&TBlobStorageGroupMultiCollectRequest::StateWait);
    }

    STATEFN(StateWait) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
        }
    }
};

IActor* CreateBlobStorageGroupMultiCollectRequest(TBlobStorageGroupMultiCollectParameters params) {
    return new TBlobStorageGroupMultiCollectRequest(params);
}

}//NKikimr
