#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "dsproxy_quorum_tracker.h"
#include "dsproxy_blob_tracker.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <util/generic/set.h>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// GET request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TBlobStorageGroupIndexRestoreGetRequest : public TBlobStorageGroupRequestActor {
    const ui32 QuerySize;
    TArrayHolder<TEvBlobStorage::TEvGet::TQuery> Queries;
    const TInstant Deadline;
    const bool IsInternal;
    const bool Decommission;
    const bool DoNotReportIndexRestoreGetMissingBlobs;
    const std::optional<TEvBlobStorage::TEvGet::TForceBlockTabletData> ForceBlockTabletData;

    THashMap<ui64, TGroupQuorumTracker> QuorumTracker;
    TVector<TBlobStatusTracker> BlobStatus;
    struct TKeepFlagVotes {
        ui32 Expected = 0;
        ui32 Keep = 0;
        ui32 DoNotKeep = 0;
        ui32 ReleasedOrEmpty = 0;

        void Observe(ui32 bit, const NKikimrBlobStorage::TQueryResult& r, const TBlobStorageGroupInfo& info,
                bool gcRecipient) {
            if (r.GetStatus() != NKikimrProto::OK && r.GetStatus() != NKikimrProto::NODATA &&
                    r.GetStatus() != NKikimrProto::NOT_YET) {
                return;
            }
            const ui32 vote = gcRecipient ? bit : 0;
            if (r.GetDoNotKeep()) {
                DoNotKeep |= vote;
                ReleasedOrEmpty |= bit;
            } else if (r.GetKeep()) {
                Keep |= vote;
            } else if (r.GetStatus() == NKikimrProto::NODATA && TIngress(r.GetIngress()).LocalParts(info.Type).Empty()) {
                ReleasedOrEmpty |= bit;
            }
        }

        void Finish(TEvBlobStorage::TEvGetResult::TResponse& r, const TBlobStorageGroupInfo& info) const {
            const auto mirror = TMirror3dcWriteQuorumChecker::TryCreate(info, r.Id.Hash());
            using TDisks = TBlobStorageGroupInfo::TSubgroupVDisks;
            const auto canCompleteGCWithout = [&](const TDisks& missing) {
                return mirror ? mirror->CheckFailModelForSubgroup(missing)
                    : info.GetQuorumChecker().CheckFailModelForSubgroup(missing);
            };
            if (canCompleteGCWithout(~TDisks::CreateFromMask(&info.GetTopology(), DoNotKeep))) {
                r.Status = NKikimrProto::OK;
                r.DoNotKeep = true;
            } else if (Expected && ReleasedOrEmpty == Expected) {
                r.Status = NKikimrProto::NODATA;
            } else if (Keep && !canCompleteGCWithout(TDisks::CreateFromMask(&info.GetTopology(), Keep))) {
                // Every GC quorum intersects these Keep replies.
                r.Status = NKikimrProto::OK;
                r.Keep = true;
            } else {
                // A lone Keep among empty replicas can be either an arriving Keep or a compacted release.
                r.Status = NKikimrProto::UNKNOWN;
            }
        }
    };
    TVector<TKeepFlagVotes> FlagVotes;
    ui32 VGetsInFlight;

    NKikimrBlobStorage::EGetHandleClass GetHandleClass;

    ui64 TabletId;
    ui64 RestoreQueriesStarted;
    ui64 RestoreQueriesFinished;

    std::unique_ptr<TEvBlobStorage::TEvGetResult> PendingResult;

    THashMap<TLogoBlobID, std::pair<bool, bool>> KeepFlags;

    void ReplyAndDie(NKikimrProto::EReplyStatus status) override {
        DSP_LOG_INFO_S("DSPI14", "ReplyAndDie"
            << " Reply with status# " << NKikimrProto::EReplyStatus_Name(status)
            << " PendingResult# " << (PendingResult ? PendingResult->ToString().data() : "nullptr"));
        if (status != NKikimrProto::OK) {
            PendingResult.reset(new TEvBlobStorage::TEvGetResult(status, QuerySize, Info->GroupID));
            for (ui32 i = 0; i < QuerySize; ++i) {
                auto& item = PendingResult->Responses[i];
                item.Status = status;
                item.Id = Queries[i].Id;
                item.Shift = Queries[i].Shift;
                item.RequestedSize = Queries[i].Size;
            }
            PendingResult->ErrorReason = ErrorReason;
        } else if (FlagVotes.empty()) {
            for (ui32 i = 0; i < PendingResult->ResponseSz; ++i) {
                auto& response = PendingResult->Responses[i];
                const auto& [keep, doNotKeep] = KeepFlags.at(response.Id);
                response.MergeKeepFlags(keep, doNotKeep);
            }
        }
        Y_ABORT_UNLESS(PendingResult);
        Mon->CountIndexRestoreGetResponseTime(TActivationContext::Monotonic() - RequestStartTime);
        SendResponseAndDie(std::move(PendingResult));
    }

    TString DumpBlobStatus() const {
        TStringStream str("{");

        for (ui32 i = 0; i < BlobStatus.size(); ++i) {
            if (i) {
                str << " ";
            }
            BlobStatus[i].Output(str, Info.Get());
        }

        str << "}";
        return str.Str();
    }

    TString DumpBlobStatus(ui32 i) const {
        TStringStream str;
        BlobStatus[i].Output(str, Info.Get());
        return str.Str();
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr &ev) {
        ProcessReplyFromQueue(ev->Get());

        const NKikimrBlobStorage::TEvVGetResult &record = ev->Get()->Record;

        Y_ABORT_UNLESS(record.HasStatus());
        NKikimrProto::EReplyStatus status = record.GetStatus();

        Y_ABORT_UNLESS(record.HasVDiskID());
        const TVDiskID vdisk = VDiskIDFromVDiskID(record.GetVDiskID());

        Y_ABORT_UNLESS(VGetsInFlight > 0);
        --VGetsInFlight;

        DSP_LOG_DEBUG_S("DSPI10", "Handle TEvVGetResult"
            << " status# " << NKikimrProto::EReplyStatus_Name(status).data()
            << " VDiskId# " << vdisk
            << " ev# " << ev->Get()->ToString());

        Y_ABORT_UNLESS(record.HasCookie());
        const ui64 queueId = record.GetCookie();
        Y_ABORT_UNLESS(queueId > 0);

        if (FlagVotes.empty() && QuorumTracker.at(queueId).ProcessReply(vdisk, status) == NKikimrProto::ERROR) {
            return ReplyAndDie(NKikimrProto::ERROR);
        }

        for (const auto& result : record.GetResult()) {
            const TLogoBlobID& blobId = LogoBlobIDFromLogoBlobID(result.GetBlobID());
            const ui32 queryIdx = result.GetCookie();
            Y_ABORT_UNLESS(queryIdx < QuerySize && blobId == Queries[queryIdx].Id);
            if (!FlagVotes.empty()) {
                if (status == NKikimrProto::OK) {
                    const ui32 bit = 1u << Info->GetIdxInSubgroup(vdisk, blobId.Hash());
                    FlagVotes[queryIdx].Observe(bit, result, *Info, Info->IsWriteTargetRealm(vdisk.FailRealm));
                }
                continue;
            }
            BlobStatus[queryIdx].UpdateFromResponseData(result, vdisk, Info.Get());
            auto& [keep, doNotKeep] = KeepFlags[blobId];
            keep |= result.GetKeep();
            doNotKeep |= result.GetDoNotKeep();
        }

        if (!VGetsInFlight) {
            OnEnoughVGetResults();
        }
    }

    void OnEnoughVGetResults() {
        PendingResult.reset(new TEvBlobStorage::TEvGetResult(NKikimrProto::OK, QuerySize, Info->GroupID));
        for (ui32 idx = 0; idx < QuerySize; ++idx) {
            if (!FlagVotes.empty()) {
                auto& response = PendingResult->Responses[idx];
                response.Id = Queries[idx].Id;
                FlagVotes[idx].Finish(response, *Info);
                continue;
            }
            const TBlobStatusTracker& blobTracker = BlobStatus[idx];
            bool lostByIngress;
            TBlobStorageGroupInfo::EBlobState blobState = blobTracker.GetBlobState(Info.Get(), &lostByIngress);

            auto &q = Queries[idx];
            auto &a = PendingResult->Responses[idx];
            a.Id = q.Id;

            DSP_LOG_DEBUG_S("DSPI11", "OnEnoughVGetResults Id# " << q.Id << " BlobStatus# " << DumpBlobStatus(idx));

            if (blobState == TBlobStorageGroupInfo::EBS_DISINTEGRATED) {
                DSP_LOG_ERROR_S("DSPI04", "OnEnoughVGetResults"
                    << " disintegrated, id# " << Queries[idx].Id.ToString()
                    << " BlobStatus# " << DumpBlobStatus(idx));
                ReplyAndDie(NKikimrProto::ERROR);
                return;
            } else if (blobState == TBlobStorageGroupInfo::EBS_UNRECOVERABLE_FRAGMENTARY) {
                // The blob must have never been written.
                a.Status = NKikimrProto::NODATA;
                // Proove it using part bits from VDisks
                if (lostByIngress) {
                    // The blob might actually be written! Report the error!
                    DSP_LOG_ERROR_S("DSPI05", "OnEnoughVGetResults for tablet# " << TabletId
                        << " unrecoverable blob, id# " << Queries[idx].Id.ToString()
                        << " BlobStatus# " << DumpBlobStatus(idx));
                    if (IngressAsAReasonForErrorEnabled) {
                        ReplyAndDie(NKikimrProto::ERROR);
                        return;
                    }
                }
            } else if (blobState == TBlobStorageGroupInfo::EBS_RECOVERABLE_DOUBTED ||
                    blobState == TBlobStorageGroupInfo::EBS_RECOVERABLE_FRAGMENTARY) {
                // Blob is not full, send EvGet to recover it
                a.Status = NKikimrProto::OK;
                std::unique_ptr<TEvBlobStorage::TEvGet> get(new TEvBlobStorage::TEvGet(
                    Queries[idx].Id, 0, 0, Deadline,
                    GetHandleClass, true));
                get->Decommission = Decommission;
                DSP_LOG_DEBUG_S("DSPI12", "OnEnoughVGetResults"
                        << " recoverable blob, id# " << Queries[idx].Id.ToString()
                        << " BlobStatus# " << DumpBlobStatus(idx)
                        << " sending EvGet");
                SendToProxy(std::move(get));
                RestoreQueriesStarted++;
            } else if (blobState == TBlobStorageGroupInfo::EBS_FULL) {
                a.Status = NKikimrProto::OK;
            }
        }

        if (RestoreQueriesStarted == 0) {
            ReplyAndDie(NKikimrProto::OK);
            return;
        }

        Become(&TBlobStorageGroupIndexRestoreGetRequest::StateRestore);

        DSP_LOG_DEBUG_S("DSPI13", "OnEnoughVGetResults"
            << " Become StateRestore RestoreQueriesStarted# " << RestoreQueriesStarted);

        return;
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr &ev) {
        TEvBlobStorage::TEvGetResult &getResult = *ev->Get();
        NKikimrProto::EReplyStatus status = getResult.Status;
        if (status != NKikimrProto::OK) {
            DSP_LOG_ERROR_S("DSPI06", "Handle TEvGetResult status# " << NKikimrProto::EReplyStatus_Name(status).data()
                << " for tablet# " << TabletId
                << " BlobStatus# " << DumpBlobStatus());
            ReplyAndDie(status);
            return;
        }

        Y_ABORT_UNLESS(PendingResult);
        for (ui32 i = 0; i < getResult.ResponseSz; ++i) {
            TEvBlobStorage::TEvGetResult::TResponse &response = getResult.Responses[i];
            auto& [keep, doNotKeep] = KeepFlags.at(response.Id);
            keep |= response.Keep;
            doNotKeep |= response.DoNotKeep;
            if (response.Status != NKikimrProto::OK) {
                if (!DoNotReportIndexRestoreGetMissingBlobs) {
                    DSP_LOG_ERROR_S("DSPI07", "Handle TEvGetResult status# " << NKikimrProto::EReplyStatus_Name(status)
                        << " Response[" << i << "]# " << NKikimrProto::EReplyStatus_Name(response.Status)
                        << " for tablet# " << TabletId
                        << " BlobStatus# " << DumpBlobStatus());
                }
                SetPendingResultResponseStatus(response.Id, response.Status);
            }
        }
        DSP_LOG_LOG_S(PriorityForStatusInbound(status), "DSPI08", "Result# " << getResult.Print(false)
            << " RestoreQueriesStarted# " << RestoreQueriesStarted
            << " RestoreQueriesFinished# " << RestoreQueriesFinished);

        RestoreQueriesFinished++;
        if (RestoreQueriesFinished == RestoreQueriesStarted) {
            ReplyAndDie(NKikimrProto::OK);
            return;
        }
        return;
    }

    void SetPendingResultResponseStatus(TLogoBlobID id, NKikimrProto::EReplyStatus status) {
        Y_ABORT_UNLESS(PendingResult);
        for (ui64 idx = 0; idx < PendingResult->ResponseSz; ++idx) {
            TEvBlobStorage::TEvGetResult::TResponse &response = PendingResult->Responses[idx];
            if (response.Id.IsSameBlob(id)) {
                response.Status = status;
            }
        }
    }

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) override {
        ++*Mon->NodeMon->RestartIndexRestoreGet;
        auto ev = std::make_unique<TEvBlobStorage::TEvGet>(Queries, QuerySize, Deadline, GetHandleClass,
            true /*mustRestoreFirst*/, true /*isIndexOnly*/, ForceBlockTabletData, IsInternal);
        ev->VerifyKeepFlags = !FlagVotes.empty();
        ev->RestartCounter = counter;
        ev->Decommission = Decommission;
        ev->DoNotReportIndexRestoreGetMissingBlobs = DoNotReportIndexRestoreGetMissingBlobs;
        return ev;
    }

public:
    ::NMonitoring::TDynamicCounters::TCounterPtr& GetActiveCounter() const override {
        return Mon->ActiveIndexRestoreGet;
    }

    ERequestType GetRequestType() const override {
        return ERequestType::Get;
    }

    TBlobStorageGroupIndexRestoreGetRequest(TBlobStorageGroupRestoreGetParameters& params)
        : TBlobStorageGroupRequestActor(params)
        , QuerySize(params.Common.Event->QuerySize)
        , Queries(params.Common.Event->Queries.Release())
        , Deadline(params.Common.Event->Deadline)
        , IsInternal(params.Common.Event->IsInternal)
        , Decommission(params.Common.Event->Decommission)
        , DoNotReportIndexRestoreGetMissingBlobs(params.Common.Event->DoNotReportIndexRestoreGetMissingBlobs)
        , ForceBlockTabletData(params.Common.Event->ForceBlockTabletData)
        , VGetsInFlight(0)
        , GetHandleClass(params.Common.Event->GetHandleClass)
        , RestoreQueriesStarted(0)
        , RestoreQueriesFinished(0)
    {
        if (QuerySize) {
            TabletId = Queries[0].Id.TabletID();
        } else {
            TabletId = 0;
        }

        if (params.Common.Event->VerifyKeepFlags) {
            FlagVotes.resize(QuerySize);
        } else {
            BlobStatus.reserve(QuerySize);
            for (ui32 idx = 0; idx < QuerySize; ++idx) {
                BlobStatus.emplace_back(Queries[idx].Id, Info.Get());
            }
        }

        // phantom checks are for non-index queries only
        Y_ABORT_UNLESS(!params.Common.Event->PhantomCheck);
    }

    void Bootstrap() override {
        auto makeQueriesList = [this] {
            TStringStream str;
            str << "{";
            for (ui32 i = 0; i < QuerySize; ++i) {
                str << (i ? " " : "")
                    << Queries[i].Id.ToString();
            }
            str << "}";
            return str.Str();
        };
        DSP_LOG_INFO_S("DSPI09", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " QuerySize# " << QuerySize
            << " Queries# " << makeQueriesList()
            << " Deadline# " << Deadline
            << " RestartCounter# " << RestartCounter
            << " ForceBlockTabletId# " << (ForceBlockTabletData ? ToString(ForceBlockTabletData->Id) : "")
            << " ForceBlockTabletGeneration# " << (ForceBlockTabletData ? ToString(ForceBlockTabletData->Generation) : ""));

        using EQuorum = TGroupQuorumTracker::EQuorum;
        const EQuorum quorum = ForceBlockTabletData && ForceBlockTabletData->Generation
            ? EQuorum::Startup : EQuorum::Read;
        for (const auto& vdisk : Info->GetVDisks()) {
            auto vd = Info->GetVDiskId(vdisk.OrderNumber);
            if (!Info->IsFailRealmReadable(vd.FailRealm) && (FlagVotes.empty() || !Info->IsWriteTargetRealm(vd.FailRealm))) {
                continue;
            }
            std::unique_ptr<TEvBlobStorage::TEvVGet> vget;
            ui64 queueId = 1;

            auto sendQuery = [&] {
                if (vget) {
                    const ui64 cookie = TVDiskIdShort(vd).GetRaw();
                    SendToQueue(std::move(vget), cookie);
                    vget.reset();
                    ++VGetsInFlight;
                }
            };

            //   Loop through the queries
            for (ui32 queryIdx = 0; queryIdx < QuerySize; ++queryIdx) {
                const TLogoBlobID &id = Queries[queryIdx].Id;
                const ui32 disk = Info->GetIdxInSubgroup(vd, id.Hash());
                if (disk < Info->Type.BlobSubgroupSize()) {
                    if (!FlagVotes.empty()) {
                        FlagVotes[queryIdx].Expected |= 1u << disk;
                    }
                    if (!vget) {
                        const ui64 cookie = queueId++;
                        if (FlagVotes.empty()) {
                            QuorumTracker.try_emplace(cookie, Info.Get(), quorum);
                        }

                        vget = TEvBlobStorage::TEvVGet::CreateExtremeIndexQuery(
                                    vd,
                                    Deadline,
                                    NKikimrBlobStorage::EGetHandleClass::FastRead,
                                    TEvBlobStorage::TEvVGet::EFlags::ShowInternals,
                                    cookie,
                                    {},
                                    ForceBlockTabletData
                                );

                        vget->Record.SetSuppressBarrierCheck(IsInternal);
                        vget->Record.SetTabletId(TabletId);
                    }
                    const ui64 cookie = queryIdx;
                    vget->AddExtremeQuery(id, 0, 0, &cookie);
                }
                if ((queryIdx + 1) % 65536 == 0) {
                    sendQuery();
                }
            }
            // If vqeurry array is not empty, send the query to the disk, else mark request replied
            sendQuery();
        }
        Y_ABORT_UNLESS(VGetsInFlight);
        Become(&TBlobStorageGroupIndexRestoreGetRequest::StateWait);
    }

    STATEFN(StateWait) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVGetResult, Handle);
        }
    }

    STATEFN(StateRestore) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvGetResult, Handle);
        }
    }
};

IActor* CreateBlobStorageGroupIndexRestoreGetRequest(TBlobStorageGroupRestoreGetParameters params) {
    return new TBlobStorageGroupIndexRestoreGetRequest(params);
}

}//NKikimr
