#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "dsproxy_quorum_tracker.h"
#include "dsproxy_blob_tracker.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>

#include <library/cpp/pop_count/popcount.h>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RANGE request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TBlobStorageGroupRangeRequest : public TBlobStorageGroupRequestActor {
    static constexpr ui32 MaxBlobsToQueryAtOnce = 8096;

    const ui64 TabletId;
    const TLogoBlobID From;
    const TLogoBlobID To;
    const TInstant Deadline;
    const bool MustRestoreFirst;
    const bool IsIndexOnly;
    const ui32 ForceBlockedGeneration;
    const bool Decommission;
    TInstant StartTime;

    TMap<TLogoBlobID, TBlobStatusTracker> BlobStatus;
    TBlobStorageGroupInfo::TGroupVDisks FailedDisks;

    ui32 NumVGetsPending = 0;

    struct TBlobQueryItem {
        TLogoBlobID BlobId;
        bool RequiredToBePresent;

        TBlobQueryItem(const TLogoBlobID& blobId, bool requiredToBePresent)
            : BlobId(blobId)
            , RequiredToBePresent(requiredToBePresent)
        {}
    };
    TVector<TBlobQueryItem> BlobsToGet;

    template<typename TPtr>
    void SendReply(TPtr& reply) {
        /*ui32 size = 0;
        for (const TEvBlobStorage::TEvRangeResult::TResponse& resp : reply->Responses) {
            size += resp.Buffer.size();
        }*/
        Mon->CountRangeResponseTime(TActivationContext::Now() - StartTime);
        SendResponseAndDie(std::move(reply));
    }

    void SendQueryToVDisk(const TVDiskID &vdisk, const TLogoBlobID &from, const TLogoBlobID &to) {
        // prepare new index query message
        auto msg = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(vdisk, Deadline, NKikimrBlobStorage::EGetHandleClass::FastRead,
                TEvBlobStorage::TEvVGet::EFlags::ShowInternals, {}, from, to, MaxBlobsToQueryAtOnce, nullptr,
                TEvBlobStorage::TEvVGet::TForceBlockTabletData(TabletId, ForceBlockedGeneration));

        // disable barrier checking
        msg->Record.SetSuppressBarrierCheck(true);

        // trace message and send it to queue
        SendToQueue(std::move(msg), 0);

        // add pending count
        ++NumVGetsPending;
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev) {
        ProcessReplyFromQueue(ev->Get());

        const auto& record = ev->Get()->Record;
        Y_ABORT_UNLESS(record.HasStatus());
        NKikimrProto::EReplyStatus status = record.GetStatus();

        Y_ABORT_UNLESS(record.HasVDiskID());
        const TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());

        R_LOG_DEBUG_S("DSR01", "received"
            << " VDiskId# " << vdisk
            << " TEvVGetResult# " << ev->Get()->ToString());

        Y_ABORT_UNLESS(NumVGetsPending > 0);
        --NumVGetsPending;

        bool isOk = status == NKikimrProto::OK;
        switch (status) {
            case NKikimrProto::ERROR:
            case NKikimrProto::VDISK_ERROR_STATE:
                FailedDisks |= TBlobStorageGroupInfo::TGroupVDisks(&Info->GetTopology(), vdisk);
                if (!Info->GetQuorumChecker().CheckFailModelForGroup(FailedDisks)) {
                    ErrorReason = "Failed disks check fails on non-OK event status";
                    return ReplyAndDie(NKikimrProto::ERROR);
                }
                break;

            case NKikimrProto::OK:
                if (record.ResultSize() == 0 && record.GetIsRangeOverflow()) {
                    isOk = false;
                    R_LOG_CRIT_S("DSR09", "Don't know how to interpret an empty range with IsRangeOverflow set." <<
                            " TEvVGetResult# " << ev->Get()->ToString());
                    FailedDisks |= TBlobStorageGroupInfo::TGroupVDisks(&Info->GetTopology(), vdisk);
                    if (!Info->GetQuorumChecker().CheckFailModelForGroup(FailedDisks)) {
                        ErrorReason = "Failed disks check fails on OK event status";
                        return ReplyAndDie(NKikimrProto::ERROR);
                    }
                }
                break;

            default:
                Y_ABORT("unexpected queryStatus# %s", NKikimrProto::EReplyStatus_Name(status).data());
        }

        if (isOk) {
            TLogoBlobID lastBlobId = From;
            for (const NKikimrBlobStorage::TQueryResult& blob : record.GetResult()) {
                Y_ABORT_UNLESS(blob.HasBlobID());
                const TLogoBlobID blobId(LogoBlobIDFromLogoBlobID(blob.GetBlobID()));
                const TLogoBlobID fullId(blobId.FullID());

                // as this is index only query, we operate only with full blob ids, not using parts
                Y_ABORT_UNLESS(blobId == fullId);

                auto it = BlobStatus.find(fullId);
                if (it == BlobStatus.end()) {
                    it = BlobStatus.emplace(fullId, TBlobStatusTracker(fullId, Info.Get())).first;
                }
                it->second.UpdateFromResponseData(blob, vdisk, Info.Get());

                Y_ABORT_UNLESS(From <= To ? lastBlobId <= fullId : lastBlobId >= fullId,
                        "Blob IDs are out of order in TEvVGetResult");
                // remember last processed blob id to resume query
                lastBlobId = fullId;
            }

            // check if the response was full -- this means that we (may) have to continue querying
            if (record.ResultSize() >= MaxBlobsToQueryAtOnce || record.GetIsRangeOverflow()) {
                TLogoBlobID from(From), to(To);
                bool send = true;

                ui32 cookie = lastBlobId.Cookie();
                ui32 step = lastBlobId.Step();
                ui32 generation = lastBlobId.Generation();
                ui8 channel = lastBlobId.Channel();

                if (from <= to) {
                    // forward query; we have to patch 'from' with successor of lastBlobId
                    if (cookie++ == TLogoBlobID::MaxCookie) {
                        cookie = 0; // overflow -- reset to zero
                        if (step++ == Max<ui32>()) { // if the step was maximum possible, it is reset to zero
                            if (generation++ == Max<ui32>()) {
                                if (channel++ == Max<ui8>()) {
                                    send = false;
                                }
                            }
                        }
                    }

                    from = TLogoBlobID(TabletId, generation, step, channel, 0 /* blobSize */, cookie);
                    send = send && from <= to;
                } else {
                    // backward query; we have to patch 'to' with predecessor of lastBlobId
                    if (!cookie--) {
                        cookie = TLogoBlobID::MaxCookie;
                        if (!step--) { // if the step was zero, it is set to Max<ui32> automatically and so for generation
                            if (!generation--) {
                                if (!channel--) {
                                    send = false;
                                }
                            }
                        }
                    }

                    from = TLogoBlobID(TabletId, generation, step, channel, TLogoBlobID::MaxBlobSize, cookie);
                    send = send && to < from;
                }

                if (send) {
                    SendQueryToVDisk(vdisk, from, to);
                }
            }
        }

        if (!NumVGetsPending) {
            for (const auto& item : BlobStatus) {
                const TBlobStatusTracker& tracker = item.second;
                bool lostByIngress = false;
                bool requiredToBePresent = false;
                switch (tracker.GetBlobState(Info.Get(), &lostByIngress)) {
                    case TBlobStorageGroupInfo::EBS_DISINTEGRATED:
                        R_LOG_ERROR_S("DSR02", "disintegrated");
                        ErrorReason = "BS disintegrated";
                        return ReplyAndDie(NKikimrProto::ERROR);

                    case TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY:
                        // try to recover, but this try will fail; in case when we see that blob was confirmed, we
                        // issue an error to tablet
                        requiredToBePresent = lostByIngress && IngressAsAReasonForErrorEnabled;
                        break;

                    case TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY:
                        // blob is recoverable so we recover it
                        requiredToBePresent = true;
                        break;

                    case TBlobStorageGroupInfo::EBS_RECOVERABLE_DOUBTED:
                        // there are doubts about blob's origin, we assume that it haven't been confirmed to writer
                        break;

                    case TBlobStorageGroupInfo::EBS_FULL:
                        // the blob is full so it should be successfully read
                        requiredToBePresent = true;
                        break;
                }

                BlobsToGet.emplace_back(item.first, requiredToBePresent);
            }

            // send request
            if (BlobsToGet) {
                SendGetRequest();
            } else {
                // reply with empty set
                ReplyAndDie(NKikimrProto::OK);
            }
        }
    }

    void SendGetRequest() {
        // build array of queries to send to DS proxy
        const ui32 queryCount = BlobsToGet.size();
        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queries(new TEvBlobStorage::TEvGet::TQuery[queryCount]);
        TEvBlobStorage::TEvGet::TQuery *query = queries.Get();
        for (const TBlobQueryItem& item : BlobsToGet) {
            query->Set(item.BlobId, 0, 0);
            ++query;
        }
        Y_ABORT_UNLESS(query == queries.Get() + queryCount);

        // register query in wilson and send it to DS proxy; issue non-index query when MustRestoreFirst is false to
        // prevent IndexRestoreGet invocation
        auto get = std::make_unique<TEvBlobStorage::TEvGet>(queries, queryCount, Deadline,
                NKikimrBlobStorage::EGetHandleClass::FastRead, MustRestoreFirst, MustRestoreFirst ? IsIndexOnly : false,
                TEvBlobStorage::TEvGet::TForceBlockTabletData(TabletId, ForceBlockedGeneration));
        get->IsInternal = true;
        get->Decommission = Decommission;

        R_LOG_DEBUG_S("DSR08", "sending TEvGet# " << get->ToString());

        SendToProxy(std::move(get), 0, Span.GetTraceId());

        // switch state
        Become(&TBlobStorageGroupRangeRequest::StateGet);
    }

    TString DumpBlobsToGet() const {
        TStringStream str("[");
        for (ui32 i = 0; i < BlobsToGet.size(); ++i) {
            str << (i ? " " : "")
                << BlobsToGet[i].BlobId.ToString() << "/"
                << (BlobsToGet[i].RequiredToBePresent ? "true" : "false");
        }
        str << "]";
        return str.Str();
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr &ev) {
        TEvBlobStorage::TEvGetResult &getResult = *ev->Get();
        NKikimrProto::EReplyStatus status = getResult.Status;
        if (status != NKikimrProto::OK) {
            R_LOG_ERROR_S("DSR03", "Handle TEvGetResult status# " << NKikimrProto::EReplyStatus_Name(status).data());
            ErrorReason = getResult.ErrorReason;
            ReplyAndDie(status);
            return;
        }

        std::unique_ptr<TEvBlobStorage::TEvRangeResult> result(new TEvBlobStorage::TEvRangeResult(status, From, To, Info->GroupID));
        result->Responses.reserve(getResult.ResponseSz);
        Y_ABORT_UNLESS(getResult.ResponseSz == BlobsToGet.size());
        for (ui32 i = 0; i < getResult.ResponseSz; ++i) {
            TEvBlobStorage::TEvGetResult::TResponse &response = getResult.Responses[i];
            Y_ABORT_UNLESS(response.Id == BlobsToGet[i].BlobId);

            if (getResult.Responses[i].Status == NKikimrProto::OK) {
                result->Responses.emplace_back(response.Id, IsIndexOnly ? TString() : response.Buffer.ConvertToString(),
                    response.Keep, response.DoNotKeep);
            } else if (getResult.Responses[i].Status != NKikimrProto::NODATA || BlobsToGet[i].RequiredToBePresent) {
                // it's okay to get NODATA if blob wasn't confirmed -- this blob is simply thrown out of resulting
                // set; otherwise we return error about lost data
                R_LOG_ERROR_S("DSR04", "Handle TEvGetResult status# " << NKikimrProto::EReplyStatus_Name(status)
                    << " Response[" << i << "]# " << NKikimrProto::EReplyStatus_Name(response.Status)
                    << " BlobsToGet# " << DumpBlobsToGet()
                    << " TEvGetResult# " << ev->Get()->ToString());
                ErrorReason = getResult.ErrorReason;
                ReplyAndDie(NKikimrProto::ERROR);
                return;
            }
        }
        if (To < From) {
            std::reverse(result->Responses.begin(), result->Responses.end());
        }
        DSP_LOG_LOG_S(NLog::PRI_INFO, "DSR05", "Result# " << result->Print(false));
        SendReply(result);
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status) override {
        std::unique_ptr<TEvBlobStorage::TEvRangeResult> result(new TEvBlobStorage::TEvRangeResult(
                    status, From, To, Info->GroupID));
        result->ErrorReason = ErrorReason;
        DSP_LOG_LOG_S(NLog::PRI_NOTICE, "DSR06", "Result# " << result->Print(false));
        SendReply(result);
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) override {
        ++*Mon->NodeMon->RestartRange;
        auto ev = std::make_unique<TEvBlobStorage::TEvRange>(TabletId, From, To, MustRestoreFirst, Deadline, IsIndexOnly,
            ForceBlockedGeneration);
        ev->RestartCounter = counter;
        ev->Decommission = Decommission;
        return ev;
    }

public:
    ::NMonitoring::TDynamicCounters::TCounterPtr& GetActiveCounter() const override {
        return Mon->ActiveRange;
    }

    ERequestType GetRequestType() const override {
        return ERequestType::Range;
    }

    TBlobStorageGroupRangeRequest(TBlobStorageGroupRangeParameters& params)
        : TBlobStorageGroupRequestActor(params)
        , TabletId(params.Common.Event->TabletId)
        , From(params.Common.Event->From)
        , To(params.Common.Event->To)
        , Deadline(params.Common.Event->Deadline)
        , MustRestoreFirst(params.Common.Event->MustRestoreFirst)
        , IsIndexOnly(params.Common.Event->IsIndexOnly)
        , ForceBlockedGeneration(params.Common.Event->ForceBlockedGeneration)
        , Decommission(params.Common.Event->Decommission)
        , StartTime(params.Common.Now)
        , FailedDisks(&Info->GetTopology())
    {}

    void Bootstrap() override {
        R_LOG_INFO_S("DSR07", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " From# " << From.ToString()
            << " To# " << To.ToString()
            << " Deadline# " << Deadline.ToString()
            << " MustRestoreFirst# " << (MustRestoreFirst ? "true" : "false")
            << " IsIndexOnly# " << (IsIndexOnly ? "true" : "false")
            << " ForceBlockedGeneration# " << ForceBlockedGeneration
            << " RestartCounter# " << RestartCounter);

        // ensure we are querying ranges for the same tablet
        Y_ABORT_UNLESS(TabletId == From.TabletID());
        Y_ABORT_UNLESS(TabletId == To.TabletID());

        // issue queries to all VDisks
        for (const auto& vdisk : Info->GetVDisks()) {
            SendQueryToVDisk(Info->GetVDiskId(vdisk.OrderNumber), From, To);
        }

        Become(&TBlobStorageGroupRangeRequest::StateWait);
    }

    STATEFN(StateWait) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVGetResult, Handle);
        }
    }

    STATEFN(StateGet) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvGetResult, Handle);
        }
    }
};

IActor* CreateBlobStorageGroupRangeRequest(TBlobStorageGroupRangeParameters params) {
    return new TBlobStorageGroupRangeRequest(params);
}

};//NKikimr
