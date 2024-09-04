#include "dsproxy.h"
#include "dsproxy_mon.h"

#include <ydb/core/blobstorage/vdisk/query/query_spacetracker.h>

#include <util/generic/set.h>

#include <optional>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// GET request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TBlobStorageGroupMultiGetRequest : public TBlobStorageGroupRequestActor {
    struct TRequestInfo {
        ui64 BeginIdx;
        ui64 EndIdx;
        bool IsReplied;
    };

    const ui64 QuerySize;
    const TArrayHolder<TEvBlobStorage::TEvGet::TQuery> Queries;
    const TInstant Deadline;
    const bool IsInternal;
    const bool PhantomCheck;
    const bool Decommission;
    TArrayHolder<TEvBlobStorage::TEvGetResult::TResponse> Responses;

    const TInstant StartTime;
    const bool MustRestoreFirst;
    const NKikimrBlobStorage::EGetHandleClass GetHandleClass;
    const std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> ForceBlockTabletData;

    static constexpr ui64 MaxRequestsInFlight = 3;
    ui64 RequestsInFlight = 0;
    std::deque<std::pair<std::unique_ptr<TEvBlobStorage::TEvGet>, ui64>> PendingGets;

    TStackVec<TRequestInfo, TypicalDisksInGroup> RequestInfos;

    std::optional<TEvBlobStorage::TEvGet::TReaderTabletData> ReaderTabletData;

    void Handle(TEvBlobStorage::TEvGetResult::TPtr &ev) {
        RequestsInFlight--;

        const TEvBlobStorage::TEvGetResult &res = *ev->Get();
        if (res.Status != NKikimrProto::OK) {
            DSP_LOG_ERROR_S("BPMG1", "Handle TEvGetResult status# " << NKikimrProto::EReplyStatus_Name(res.Status));
            ReplyAndDie(res.Status);
            return;
        }

        Y_ABORT_UNLESS(ev->Cookie < RequestInfos.size());
        TRequestInfo &info = RequestInfos[ev->Cookie];
        Y_ABORT_UNLESS(!info.IsReplied);
        info.IsReplied = true;
        Y_ABORT_UNLESS(res.ResponseSz == info.EndIdx - info.BeginIdx);

        for (ui64 offset = 0; offset < res.ResponseSz; ++offset) {
            Y_DEBUG_ABORT_UNLESS(!PhantomCheck || res.Responses[offset].LooksLikePhantom.has_value());
            Responses[info.BeginIdx + offset] = res.Responses[offset];
        }

        SendRequests();
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status) override {
        std::unique_ptr<TEvBlobStorage::TEvGetResult> ev(new TEvBlobStorage::TEvGetResult(status, QuerySize, Info->GroupID));
        Y_ABORT_UNLESS(status != NKikimrProto::NODATA);
        for (ui32 i = 0, e = QuerySize; i != e; ++i) {
            const TEvBlobStorage::TEvGet::TQuery &query = Queries[i];
            TEvBlobStorage::TEvGetResult::TResponse &x = ev->Responses[i];
            x.Status = status;
            x.Id = query.Id;
            x.LooksLikePhantom = PhantomCheck ? std::make_optional(false) : std::nullopt;
        }
        ev->ErrorReason = ErrorReason;
        Mon->CountGetResponseTime(Info->GetDeviceType(), GetHandleClass, ev->PayloadSizeBytes(), TActivationContext::Now() - StartTime);
        Y_ABORT_UNLESS(status != NKikimrProto::OK);
        SendResponseAndDie(std::move(ev));
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32) override {
        Y_ABORT();
    }

public:
    ::NMonitoring::TDynamicCounters::TCounterPtr& GetActiveCounter() const override {
        return Mon->ActiveMultiGet;
    }

    ERequestType GetRequestType() const override {
        return ERequestType::Get;
    }

    TBlobStorageGroupMultiGetRequest(TBlobStorageGroupMultiGetParameters& params)
        : TBlobStorageGroupRequestActor(params)
        , QuerySize(params.Common.Event->QuerySize)
        , Queries(params.Common.Event->Queries.Release())
        , Deadline(params.Common.Event->Deadline)
        , IsInternal(params.Common.Event->IsInternal)
        , PhantomCheck(params.Common.Event->PhantomCheck)
        , Decommission(params.Common.Event->Decommission)
        , Responses(new TEvBlobStorage::TEvGetResult::TResponse[QuerySize])
        , StartTime(params.Common.Now)
        , MustRestoreFirst(params.Common.Event->MustRestoreFirst)
        , GetHandleClass(params.Common.Event->GetHandleClass)
        , ForceBlockTabletData(params.Common.Event->ForceBlockTabletData)
    {}

    void PrepareRequest(ui32 beginIdx, ui32 endIdx) {
        Y_ABORT_UNLESS(endIdx > beginIdx);
        ui64 cookie = RequestInfos.size();
        RequestInfos.push_back({beginIdx, endIdx, false});
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queries(new TEvBlobStorage::TEvGet::TQuery[endIdx - beginIdx]);
        for (ui32 idx = beginIdx; idx < endIdx; ++idx) {
            queries[idx - beginIdx] = Queries[idx];
        }
        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(queries, endIdx - beginIdx, Deadline, GetHandleClass,
            MustRestoreFirst, false, ForceBlockTabletData);
        ev->IsInternal = IsInternal;
        ev->ReaderTabletData = ReaderTabletData;
        ev->PhantomCheck = PhantomCheck;
        ev->Decommission = Decommission;
        PendingGets.emplace_back(std::move(ev), cookie);
    }

    void SendRequests() {
        for (; RequestsInFlight < MaxRequestsInFlight && !PendingGets.empty(); ++RequestsInFlight, PendingGets.pop_front()) {
            auto& [ev, cookie] = PendingGets.front();
            SendToProxy(std::move(ev), cookie, Span.GetTraceId());
        }
        if (!RequestsInFlight && PendingGets.empty()) {
            for (size_t i = 0; PhantomCheck && i < QuerySize; ++i) {
                Y_DEBUG_ABORT_UNLESS(Responses[i].LooksLikePhantom.has_value());
            }
            auto ev = std::make_unique<TEvBlobStorage::TEvGetResult>(NKikimrProto::OK, 0, Info->GroupID);
            ev->ResponseSz = QuerySize;
            ev->Responses = std::move(Responses);
            Mon->CountGetResponseTime(Info->GetDeviceType(), GetHandleClass, ev->PayloadSizeBytes(), TActivationContext::Now() - StartTime);
            SendResponseAndDie(std::move(ev));
        }
    }

    void Bootstrap() override {
        auto dumpQuery = [this] {
            TStringStream str;
            str << "{";
            for (ui32 i = 0; i < QuerySize; ++i) {
                str << (i ? " " : "")
                    << Queries[i].Id
                    << "@" << Queries[i].Shift
                    << ":" << Queries[i].Size;
            }
            str << "}";
            return str.Str();
        };
        DSP_LOG_INFO_S("BPMG3", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " Query# " << dumpQuery()
            << " Deadline# " << Deadline);

        Y_ABORT_UNLESS(QuerySize != 0); // reply with error?
        ui32 beginIdx = 0;
        TLogoBlobID lastBlobId;
        TQueryResultSizeTracker resultSize;
        resultSize.Init();

        for (ui32 queryIdx = 0; queryIdx < QuerySize; ++queryIdx) {
            const TEvBlobStorage::TEvGet::TQuery &query = Queries[queryIdx];
            if (lastBlobId == query.Id && queryIdx != 0) {
                continue;
            }
            resultSize.AddAllPartsOfLogoBlob(Info->Type, query.Id);

            if (queryIdx != beginIdx) {
                if (resultSize.IsOverflow() || queryIdx - beginIdx == 10000) {
                    PrepareRequest(beginIdx, queryIdx);
                    beginIdx = queryIdx;
                    resultSize.Init();
                    resultSize.AddAllPartsOfLogoBlob(Info->Type, query.Id);
                }
            }
        }
        PrepareRequest(beginIdx, QuerySize);

        SendRequests();

        Become(&TBlobStorageGroupMultiGetRequest::StateWait);
    }

    STATEFN(StateWait) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvGetResult, Handle);
        }
    }
};

IActor* CreateBlobStorageGroupMultiGetRequest(TBlobStorageGroupMultiGetParameters params) {
    return new TBlobStorageGroupMultiGetRequest(params);
}

}//NKikimr
