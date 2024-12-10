#include "dsproxy.h"
#include "dsproxy_mon.h"
#include "dsproxy_quorum_tracker.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <ydb/core/blobstorage/lwtrace_probes/blobstorage_probes.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>

namespace NKikimr {

LWTRACE_USING(BLOBSTORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DISCOVER request
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TDiscoverVDiskWorker {
    static constexpr ui32 BlobsAtOnce = 100;

    const TVDiskID VDiskId;

    const ui64 TabletId;

    // is there an get unanswered get request?
    bool GetQueryInFlight = false;

    // has this VDisk finished?
    bool Finished = false;

    // is this VDisk erroneous?
    bool Erroneous = false;

    // is there possibly more blobs
    bool MoreBlobs = true;

    // first and last searched blob ids
    TLogoBlobID FirstBlob;
    const TLogoBlobID LastBlob;

    const ui32 ForceBlockedGeneration;

    struct TBlobQueueItem {
        TLogoBlobID                Id;      // identifier of this blob
        TIngress                   Ingress; // returned ingress
        NKikimrProto::EReplyStatus Status;  // status for this blob

        TBlobQueueItem(const TBlobQueueItem& other) = default;

        TBlobQueueItem(const NKikimrBlobStorage::TQueryResult& item)
            : Id(LogoBlobIDFromLogoBlobID(item.GetBlobID()))
            , Ingress(item.GetIngress())
            , Status(item.GetStatus())
        {
            Y_DEBUG_ABORT_UNLESS(item.HasBlobID() && item.HasIngress() && item.HasStatus());
        }
    };

    // queue of blobs in descending order
    TDeque<TBlobQueueItem> BlobQueue;

public:
    TDiscoverVDiskWorker(const TVDiskID& vdiskId, ui64 tabletId, ui32 minGeneration, ui32 forceBlockedGeneration)
        : VDiskId(vdiskId)
        , TabletId(tabletId)
        , FirstBlob(tabletId, Max<ui32>(), Max<ui32>(), 0, TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie)
        , LastBlob(tabletId, minGeneration, 0, 0, 0, 0)
        , ForceBlockedGeneration(forceBlockedGeneration)
    {}

    // this function creates TEvVGet query to VDisk to be sent or returns nullptr if there is no query to send right now
    std::unique_ptr<TEvBlobStorage::TEvVGet> GenerateGetRequest(TInstant deadline) {
        if (!IsReady() && !GetQueryInFlight) {
            // create a query
            auto query = TEvBlobStorage::TEvVGet::CreateRangeIndexQuery(
                VDiskId,                                // vdisk
                deadline,                               // deadline
                NKikimrBlobStorage::Discover,           // cls
                TEvBlobStorage::TEvVGet::EFlags::ShowInternals, // flags
                {},                                     // requestCookie
                FirstBlob,                              // fromId
                LastBlob,                               // toId
                BlobsAtOnce,                            // maxResults
                nullptr,                                // cookie
                TEvBlobStorage::TEvVGet::TForceBlockTabletData(TabletId, ForceBlockedGeneration));    // forceBlockTabletData

            // disable barrier checking
            query->Record.SetSuppressBarrierCheck(true);

            // put query in queue and mark ourselves processing a query
            GetQueryInFlight = true;
            return query;
        }

        return nullptr;
    }

    // processor for received TEvVGetResult message
    bool Apply(TEvBlobStorage::TEvVGetResult *ev) {
        // the disk worker can't be ready while request is in processing
        Y_ABORT_UNLESS(!IsReady(), "Unexpected Finished# %s BlobQueue# %s Erroneous# %s MoreBlobs# %s GetQueryInFlight# %s",
                Finished ? "true" : "false",
                BlobQueue ? "true" : "false",
                Erroneous ? "true" : "false",
                MoreBlobs ? "true" : "false",
                GetQueryInFlight ? "true" : "false");

        // ensure we have in flight query
        Y_ABORT_UNLESS(GetQueryInFlight);
        GetQueryInFlight = false;

        // extract record and check it contains the status field
        const auto& record = ev->Record;
        Y_ABORT_UNLESS(record.HasStatus());

        // ensure response came from our VDisk
        Y_ABORT_UNLESS(record.HasVDiskID() && VDiskIDFromVDiskID(record.GetVDiskID()) == VDiskId);

        // apply record according to the returned status
        switch (NKikimrProto::EReplyStatus status = record.GetStatus()) {
            case NKikimrProto::OK:
                // request has been successfully processed and we should put items into queue
                if (record.GetIsRangeOverflow() && !record.ResultSize()) {
                    LOG_CRIT_S(*TlsActivationContext, NKikimrServices::BS_PROXY_DISCOVER,
                            "Don't know how to process RangeOverflow with ResultSize# 0. Marker# DSPDM10");
                    Finished = true;
                    Erroneous = true;
                } else {
                    ApplySuccessfulResult(record);
                }
                break;

            case NKikimrProto::ERROR:
            case NKikimrProto::VDISK_ERROR_STATE:
                // here we just start ignoring this VDisk; mark ourselves finished and continue working
                Finished = true;
                Erroneous = true;
                break;

            default:
                Y_ABORT("unexpected reply status# %s", NKikimrProto::EReplyStatus_Name(status).data());
        }

        return true;
    }

    // obtain the identifier of most recent blob contained within this structure
    TMaybe<TLogoBlobID> GetLatestBlob() const {
        Y_ABORT_UNLESS(IsReady());
        return BlobQueue
            ? BlobQueue.front().Id
            : TMaybe<TLogoBlobID>();
    }

    // pop the blob with its status and other info; returns true if there is such blob and fields are filled in correctly;
    // false otherwise
    bool PopBlob(const TLogoBlobID& id, NKikimrProto::EReplyStatus *status, TIngress *ingress) {
        if (BlobQueue.empty()) {
            return false; // nothing to pop
        }

        TBlobQueueItem& item = BlobQueue.front();
        const TLogoBlobID& nextBlobId = item.Id;
        Y_ABORT_UNLESS(id >= nextBlobId);
        if (id != nextBlobId) {
            return false; // blob id does not match requested one
        }

        *status = item.Status;
        *ingress = item.Ingress;

        BlobQueue.pop_front();
        if (BlobQueue.empty()) {
            Finished = !MoreBlobs;
        }

        return true;
    }

    // check if this worker is ready to issue logoblobs
    bool IsReady() const {
        return Finished || BlobQueue;
    }

    // check if worker is over erroneous vdisk
    bool IsErroneous() const {
        return Erroneous;
    }

    const TVDiskID& GetVDiskId() const {
        return VDiskId;
    }

private:
    void ApplySuccessfulResult(const NKikimrBlobStorage::TEvVGetResult& record) {
        // ensure we haven't finished traversing index yet
        Y_ABORT_UNLESS(!Finished);

        // the blob queue must be empty on entry -- otherwise we have no reason to request more blobs
        Y_ABORT_UNLESS(BlobQueue.empty());

        // update queue with new items
        for (const NKikimrBlobStorage::TQueryResult& item : record.GetResult()) {
            TBlobQueueItem newItem(item);
            Y_ABORT_UNLESS(newItem.Id.PartId() == 0);
            BlobQueue.emplace_back(newItem);
        }
        if (record.ResultSize() < BlobsAtOnce && !record.GetIsRangeOverflow()) {
            MoreBlobs = false;
        }

        // and then check for strict ordering
        if (BlobQueue.size() > 1) {
            for (auto it1 = BlobQueue.begin(), it2 = std::next(it1); it2 != BlobQueue.end(); ++it1, ++it2) {
                Y_ABORT_UNLESS(it1->Id > it2->Id, "id1# %s id2# %s", it1->Id.ToString().data(), it2->Id.ToString().data());
            }
        }

        // if there were no blobs in answer, we consired ourselves finished; otherwise we shall update our lower bound
        // and order to continue requesting
        if (BlobQueue) {
            // 'decrement' FirstBlob to continue processing
            const TLogoBlobID& least = BlobQueue.back().Id;
            const ui64 tabletId = least.TabletID();
            const ui32 channel = least.Channel();
            ui32 gen = least.Generation();
            ui32 step = least.Step();
            ui32 cookie = least.Cookie();
            if (cookie) {
                --cookie;
            } else {
                cookie = TLogoBlobID::MaxCookie;
                if (step) {
                    --step;
                } else {
                    step = Max<ui32>();
                    if (gen) {
                        --gen;
                    } else {
                        Finished = true;
                    }
                }
            }
            FirstBlob = TLogoBlobID(tabletId, gen, step, channel, TLogoBlobID::MaxBlobSize, cookie);
            Finished |= FirstBlob < LastBlob;
        } else {
            Finished = true;
        }
    }
};

class TDiscoverWorker {
public:
    struct TDiscoveryState {
        TLogoBlobID BlobId; // TLogoBlobID() when there are no blobs left
        bool MustExist; // set to true when this blob was written and surely confirmed to tablet
    };

private:
    const TIntrusivePtr<TBlobStorageGroupInfo> Info;
    TVector<TDiscoverVDiskWorker> VDiskWorkers;
    ui32 NumReadyWorkers = 0;
    TQueue<TDiscoveryState> StateQ;

public:
    TDiscoverWorker(TIntrusivePtr<TBlobStorageGroupInfo> info, ui64 tabletId, ui32 minGeneration,
            ui32 forceBlockedGeneration)
        : Info(std::move(info))
    {
        const ui32 numDisks = Info->GetTotalVDisksNum();
        VDiskWorkers.reserve(numDisks);
        for (ui32 i = 0; i < numDisks; ++i) {
            VDiskWorkers.emplace_back(Info->GetVDiskId(i), tabletId, minGeneration, forceBlockedGeneration);
        }
    }

    void GenerateGetRequests(TVector<std::unique_ptr<TEvBlobStorage::TEvVGet>>& msgs, TInstant deadline) {
        for (TDiscoverVDiskWorker& worker : VDiskWorkers) {
            if (auto query = worker.GenerateGetRequest(deadline)) {
                msgs.push_back(std::move(query));
            }
        }
    }

    bool Apply(TEvBlobStorage::TEvVGetResult *ev) {
        // get a worker for this event
        const auto& record = ev->Record;
        Y_ABORT_UNLESS(record.HasVDiskID());
        const TVDiskID vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        const TVDiskIdShort shortId(vdiskId);
        ui32 index = Info->GetOrderNumber(shortId);
        Y_ABORT_UNLESS(index < VDiskWorkers.size());

        // apply event
        TDiscoverVDiskWorker& worker = VDiskWorkers[index];
        if (!worker.Apply(ev)) {
            return false;
        }
        NumReadyWorkers += worker.IsReady();

        return ProcessDiscovery();
    }

    bool IsReady() const {
        return !StateQ.empty();
    }

    const TDiscoveryState& GetState() const {
        return StateQ.front();
    }

    void PopState() {
        StateQ.pop();
    }

private:
    // this function checks whether erroneous disk count exceeds failure model for our erasure type or not; on success
    // returns true
    bool CheckGroupFailModel() {
        TBlobStorageGroupInfo::TGroupVDisks failedGroupDisks(&Info->GetTopology());
        for (const TDiscoverVDiskWorker& worker : VDiskWorkers) {
            if (worker.IsErroneous()) {
                failedGroupDisks += TBlobStorageGroupInfo::TGroupVDisks(&Info->GetTopology(), worker.GetVDiskId());
            }
        }
        const auto& checker = Info->GetQuorumChecker();
        return checker.CheckFailModelForGroup(failedGroupDisks);
    }

    bool ProcessDiscovery() {
        // some of disks haven't replied yet, so we skip processing for now
        if (NumReadyWorkers != VDiskWorkers.size()) {
            return true;
        }

        // let's check that VDisk workers didn't register excessive failures
        if (!CheckGroupFailModel()) {
            return false;
        }

        while (NumReadyWorkers == VDiskWorkers.size()) {
            // find the most recent blob id; maxId will be empty if there are no blobs left
            TLogoBlobID maxId;
            for (TDiscoverVDiskWorker& worker : VDiskWorkers) {
                TMaybe<TLogoBlobID> vdiskBlobId = worker.GetLatestBlob();
                if (!vdiskBlobId) {
                    continue;
                } else {
                    maxId = Max(maxId, *vdiskBlobId);
                }
            }

            // check if there are no blobs -- it is successful discovery of empty tablet
            if (!maxId) {
                StateQ.push(TDiscoveryState{maxId, false});
                break;
            }

            // pick a subgroup of VDisks for this blob -- it is not necessary to check all the disks
            TBlobStorageGroupInfo::TVDiskIds vdisks;
            Info->PickSubgroup(maxId.Hash(), &vdisks, nullptr);

            // now we have logo blob id; merge ingress for this id and check whether this blob is restorable
            TIngress mergedIngress;
            TStackVec<NKikimrProto::EReplyStatus, 16> perDiskStatus;
            for (const TVDiskID& vdisk : vdisks) {
                // get order number for this vdisk and find matching worker
                const TVDiskIdShort shortId(vdisk);
                ui32 orderNumber = Info->GetOrderNumber(shortId);
                Y_ABORT_UNLESS(orderNumber < VDiskWorkers.size());
                TDiscoverVDiskWorker& worker = VDiskWorkers[orderNumber];

                // try to extract blob information from this worker and apply its status and ingress
                NKikimrProto::EReplyStatus status;
                TIngress ingress;
                if (!worker.PopBlob(maxId, &status, &ingress)) {
                    status = NKikimrProto::NODATA; // this disk haven't synced this blob yet, so mark it as NODATA
                }
                perDiskStatus.push_back(status);
                mergedIngress.Merge(ingress);
                NumReadyWorkers -= !worker.IsReady();
            }

            // check for failure model; if number of reported errors exceeds failure model, return error in discovery
            TBlobStorageGroupInfo::TSubgroupVDisks failedSubgroupDisks(&Info->GetTopology());
            if (!CheckSubgroupFailModel(perDiskStatus.data(), perDiskStatus.size(), failedSubgroupDisks)) {
                return false;
            }

            // TODO(alexvru): get rid of ingress here
            auto layout = TSubgroupPartLayout::CreateFromIngress(mergedIngress, Info->Type);
            const auto& checker = Info->GetQuorumChecker();
            auto state = checker.GetBlobState(layout, failedSubgroupDisks);
            if (state & (TBlobStorageGroupInfo::EBSF_FULL | TBlobStorageGroupInfo::EBSF_RECOVERABLE)) {
                const bool mustExist = state & TBlobStorageGroupInfo::EBSF_FULL;
                StateQ.push(TDiscoveryState{maxId, mustExist});
            }
        }

        return true;
    }

    bool CheckSubgroupFailModel(NKikimrProto::EReplyStatus *perDiskStatus, ui32 count,
            TBlobStorageGroupInfo::TSubgroupVDisks& failedSubgroupDisks) {
        for (ui32 i = 0; i < count; ++i) {
            switch (NKikimrProto::EReplyStatus status = perDiskStatus[i]) {
                case NKikimrProto::OK:
                case NKikimrProto::NODATA:
                    break;

                case NKikimrProto::ERROR:
                    failedSubgroupDisks += TBlobStorageGroupInfo::TSubgroupVDisks(&Info->GetTopology(), i);
                    break;

                default:
                    Y_ABORT("unexpected status# %s", NKikimrProto::EReplyStatus_Name(status).data());
            }
        }

        const auto& checker = Info->GetQuorumChecker();
        return checker.CheckFailModelForSubgroup(failedSubgroupDisks);
    }
};

class TBlobStorageGroupMirror3dcDiscoverRequest : public TBlobStorageGroupRequestActor<TBlobStorageGroupMirror3dcDiscoverRequest>{
    const ui64 TabletId;
    const ui32 MinGeneration;
    const TInstant StartTime;
    const TInstant Deadline;
    const bool ReadBody;
    const bool DiscoverBlockedGeneration;
    const ui32 ForceBlockedGeneration;
    const bool FromLeader;

    std::unique_ptr<TDiscoverWorker> Worker;
    TVector<std::unique_ptr<TEvBlobStorage::TEvVGet>> Msgs;

    ui32 BlockedGeneration = 0;
    TString Buffer;
    bool GetFinished = false;
    bool GetInFlight = false;
    TLogoBlobID ResultBlobId;

    // quorum tracker for EvVGetBlock queries
    TGroupQuorumTracker GetBlockTracker;
    bool GetBlockFinished = false;

    // number of in flight requests of all kind
    ui32 RequestsInFlight = 0;
    bool Responded = false;

public:
    static const auto& ActiveCounter(const TIntrusivePtr<TBlobStorageGroupProxyMon>& mon) {
        return mon->ActiveDiscover;
    }

    static constexpr ERequestType RequestType() {
        return ERequestType::Discover;
    }

    TBlobStorageGroupMirror3dcDiscoverRequest(TIntrusivePtr<TBlobStorageGroupInfo> info,
            TIntrusivePtr<TGroupQueues> state, const TActorId& source,
            TIntrusivePtr<TBlobStorageGroupProxyMon> mon, TEvBlobStorage::TEvDiscover *ev,
            ui64 cookie, NWilson::TTraceId traceId, TInstant now,
            TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters)
        : TBlobStorageGroupRequestActor(std::move(info), std::move(state), std::move(mon), source, cookie,
                NKikimrServices::BS_PROXY_DISCOVER, false, {}, now, storagePoolCounters, ev->RestartCounter,
                NWilson::TSpan(TWilson::BlobStorage, std::move(traceId), "DSProxy.Discover(mirror-3-dc)"),
                std::move(ev->ExecutionRelay))
        , TabletId(ev->TabletId)
        , MinGeneration(ev->MinGeneration)
        , StartTime(now)
        , Deadline(ev->Deadline)
        , ReadBody(ev->ReadBody)
        , DiscoverBlockedGeneration(ev->DiscoverBlockedGeneration)
        , ForceBlockedGeneration(ev->ForceBlockedGeneration)
        , FromLeader(ev->FromLeader)
        , GetBlockTracker(Info.Get())
    {}

    std::unique_ptr<IEventBase> RestartQuery(ui32 counter) {
        ++*Mon->NodeMon->RestartDiscover;
        auto ev = std::make_unique<TEvBlobStorage::TEvDiscover>(TabletId, MinGeneration, ReadBody, DiscoverBlockedGeneration,
            Deadline, ForceBlockedGeneration, FromLeader);
        ev->RestartCounter = counter;
        return ev;
    }

    void Bootstrap() {
        A_LOG_DEBUG_S("DSPDM01", "bootstrap"
            << " ActorId# " << SelfId()
            << " Group# " << Info->GroupID
            << " TabletId# " << TabletId
            << " MinGeneration# " << MinGeneration
            << " Deadline# " << Deadline
            << " ReadBody# " << (ReadBody ? "true" : "false")
            << " DiscoverBlockedGeneration# " << (DiscoverBlockedGeneration ? "true" : "false")
            << " ForceBlockedGeneration# " << ForceBlockedGeneration
            << " FromLeader# " << (FromLeader ? "true" : "false")
            << " RestartCounter# " << RestartCounter);

        Worker = std::make_unique<TDiscoverWorker>(Info, TabletId, MinGeneration, ForceBlockedGeneration);

        // generate GetBlock queries if we need 'em
        if (DiscoverBlockedGeneration) {
            for (const auto& vdisk : Info->GetVDisks()) {
                auto vd = Info->GetVDiskId(vdisk.OrderNumber);
                auto query = std::make_unique<TEvBlobStorage::TEvVGetBlock>(TabletId, vd, Deadline);

                A_LOG_DEBUG_S("DSPDM06", "sending TEvVGetBlock# " << query->ToString());

                SendToQueue(std::move(query), 0);
                ++RequestsInFlight;
            }
        }

        // initial kick for workers -- send messages to corresponding VDisks
        SendWorkerMessages();

        // set correct state function
        Become(&TBlobStorageGroupMirror3dcDiscoverRequest::StateFunc);
    }

    void SendWorkerMessages() {
        Worker->GenerateGetRequests(Msgs, Deadline);
        for (auto& msg : Msgs) {
            A_LOG_DEBUG_S("DSPDM07", "sending TEvVGet# " << msg->ToString());

            CountEvent(*msg);
            SendToQueue(std::move(msg), 0);
            ++RequestsInFlight;
        }
        Msgs.clear();
    }

    void Handle(TEvBlobStorage::TEvVGetResult::TPtr& ev) {
        ProcessReplyFromQueue(ev);
        CountEvent(*ev->Get());

        Y_ABORT_UNLESS(RequestsInFlight > 0);
        --RequestsInFlight;

        // extract VDisk id
        TEvBlobStorage::TEvVGetResult *msg = ev->Get();
        const auto& record = msg->Record;
        Y_ABORT_UNLESS(record.HasVDiskID());

        A_LOG_DEBUG_S("DSPDM04", "received TEvVGetResult# " << msg->ToString());

        // get worker for this ring and apply result
        if (!Worker->Apply(msg)) {
            ReplyAndDie(NKikimrProto::ERROR);
        } else if (Worker->IsReady()) {
            // continue processing discovery
            TryToProcessNextProbe();
        } else {
            // may be we have to send even more messages to Vdisks, so let's do it
            SendWorkerMessages();
        }

        Y_ABORT_UNLESS(RequestsInFlight || Responded);
    }

    void TryToProcessNextProbe() {
        if (Worker->IsReady() && !GetInFlight && !GetFinished) {
            const TDiscoverWorker::TDiscoveryState& state = Worker->GetState();
            ResultBlobId = state.BlobId;
            if (ResultBlobId) {
                GetInFlight = true;
                Y_ABORT_UNLESS(ResultBlobId.PartId() == 0);
                auto query = std::make_unique<TEvBlobStorage::TEvGet>(ResultBlobId, 0U, 0U, Deadline,
                        NKikimrBlobStorage::Discover, true, !ReadBody, TEvBlobStorage::TEvGet::TForceBlockTabletData(TabletId, ForceBlockedGeneration));
                query->IsInternal = true;

                A_LOG_DEBUG_S("DSPDM17", "sending TEvGet# " << query->ToString());

                SendToProxy(std::move(query));
                ++RequestsInFlight;
            } else {
                GetFinished = true;
                TryToSatisfyRequest();
            }
        }
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr& ev) {
        Y_ABORT_UNLESS(RequestsInFlight > 0);
        --RequestsInFlight;

        A_LOG_DEBUG_S("DSPDM05", "received TEvGetResult# " << ev->Get()->ToString());

        // get item from probe queue and ensure that we receive answer for exactly this query
        Y_ABORT_UNLESS(Worker->IsReady());
        const TDiscoverWorker::TDiscoveryState state = Worker->GetState();
        Y_ABORT_UNLESS(state.BlobId == ResultBlobId);
        Worker->PopState();

        // verify in flight flag
        Y_ABORT_UNLESS(GetInFlight);
        GetInFlight = false;

        // process message status -- any _message_ other than OK is treated as uncorrectable error (at least at this
        // point in time)
        TEvBlobStorage::TEvGetResult *msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            ReplyAndDie(NKikimrProto::ERROR);
            return;
        }

        Y_ABORT_UNLESS(msg->ResponseSz == 1);
        auto& resp = msg->Responses[0];
        switch (resp.Status) {
            case NKikimrProto::OK:
                // okay response -- blob is read and stored in all replicas
                Y_ABORT_UNLESS(resp.Id == ResultBlobId);
                Buffer = resp.Buffer.ConvertToString();
                GetFinished = true;
                TryToSatisfyRequest();
                break;

            case NKikimrProto::ERROR:
                ReplyAndDie(NKikimrProto::ERROR);
                return;

            case NKikimrProto::NODATA:
                if (state.MustExist) {
                    // we have just lost the blob
                    R_LOG_ALERT_S("DSPDM09", "!!! LOST THE BLOB !!! BlobId# " << ResultBlobId.ToString()
                            << " Group# " << Info->GroupID);
                    return ReplyAndDie(NKikimrProto::ERROR);
                } else if (Worker->IsReady()) {
                    // try to process another probe in queue (if available) as this blob is not restorable, but this was
                    // expectable
                    TryToProcessNextProbe();
                } else {
                    // worker is exhausted, so we have to issue more messages to VDisks to determine further blobs
                    SendWorkerMessages();
                }
                break;

            default:
                Y_ABORT("unexpected item status# %s", NKikimrProto::EReplyStatus_Name(resp.Status).data());
        }

        Y_ABORT_UNLESS(RequestsInFlight || Responded, "Status# %s GetInFlight# %s GetBlockFinished# %s",
                NKikimrProto::EReplyStatus_Name(resp.Status).data(), GetInFlight ? "true" : "false",
                GetBlockFinished ? "true" : "false");
    }

    void TryToSatisfyRequest() {
        if (GetFinished && GetBlockFinished) {
            std::unique_ptr<TEvBlobStorage::TEvDiscoverResult> response;
            if (ResultBlobId) {
                response.reset(new TEvBlobStorage::TEvDiscoverResult(ResultBlobId, MinGeneration,
                        ReadBody ? Buffer : TString(), BlockedGeneration));
            } else {
                response.reset(new TEvBlobStorage::TEvDiscoverResult(NKikimrProto::NODATA, MinGeneration,
                            BlockedGeneration));
            }

            R_LOG_DEBUG_S("DSPDM03", "Response# " << response->ToString());

            Y_ABORT_UNLESS(!Responded);
            const TDuration duration = TActivationContext::Now() - StartTime;
            LWPROBE(DSProxyRequestDuration, TEvBlobStorage::EvDiscover, 0, duration.SecondsFloat() * 1000.0,
                    TabletId, Info->GroupID.GetRawId(), TLogoBlobID::MaxChannel, "", true);
            SendResponseAndDie(std::move(response));
            Responded = true;
        }
    }

    void ReplyAndDie(NKikimrProto::EReplyStatus status) {
        R_LOG_ERROR_S("DSPDM02", "Status# " << NKikimrProto::EReplyStatus_Name(status));

        Y_ABORT_UNLESS(!Responded);
        Y_ABORT_UNLESS(status != NKikimrProto::OK);
        const TDuration duration = TActivationContext::Now() - StartTime;
        LWPROBE(DSProxyRequestDuration, TEvBlobStorage::EvDiscover, 0, duration.SecondsFloat() * 1000.0,
                TabletId, Info->GroupID.GetRawId(), TLogoBlobID::MaxChannel, "", false);
        std::unique_ptr<TEvBlobStorage::TEvDiscoverResult> response(new TEvBlobStorage::TEvDiscoverResult(
                    status, MinGeneration, 0U));
        response->ErrorReason = ErrorReason;
        SendResponseAndDie(std::move(response));
        Responded = true;
    }

    void Handle(TEvBlobStorage::TEvVGetBlockResult::TPtr& ev) {
        ProcessReplyFromQueue(ev);
        Y_ABORT_UNLESS(RequestsInFlight > 0);
        --RequestsInFlight;

        TEvBlobStorage::TEvVGetBlockResult *msg = ev->Get();

        A_LOG_DEBUG_S("DSPDM08", "received TEvVGetBlockResult# " << msg->ToString()
                << " BlockedGeneration# " << BlockedGeneration);

        const auto& record = msg->Record;
        Y_ABORT_UNLESS(record.HasStatus());

        Y_ABORT_UNLESS(record.HasVDiskID());
        const TVDiskID& vdisk = VDiskIDFromVDiskID(record.GetVDiskID());

        // update blocked generation -- we need maximum
        NKikimrProto::EReplyStatus status = record.GetStatus();
        if (status == NKikimrProto::OK) {
            BlockedGeneration = Max(BlockedGeneration, record.GetGeneration());
        } else if (status == NKikimrProto::NODATA) {
            status = NKikimrProto::OK; // assume OK for quorum tracker
        }

        switch (NKikimrProto::EReplyStatus quorumStatus = GetBlockTracker.ProcessReply(vdisk, status)) {
            case NKikimrProto::OK:
                GetBlockFinished = true;
                TryToSatisfyRequest();
                break;

            case NKikimrProto::ERROR:
                return ReplyAndDie(NKikimrProto::ERROR);

            case NKikimrProto::UNKNOWN:
                break;

            default:
                Y_ABORT("unexpected TEvVGetBlockResult status# %s", NKikimrProto::EReplyStatus_Name(quorumStatus).data());
        }

        Y_ABORT_UNLESS(RequestsInFlight || Responded);
    }

    STATEFN(StateFunc) {
        if (ProcessEvent(ev)) {
            return;
        }
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVGetResult, Handle);
            hFunc(TEvBlobStorage::TEvGetResult, Handle);
            hFunc(TEvBlobStorage::TEvVGetBlockResult, Handle);
        }
    }
};

IActor* CreateBlobStorageGroupMirror3dcDiscoverRequest(const TIntrusivePtr<TBlobStorageGroupInfo> &info,
        const TIntrusivePtr<TGroupQueues> &state, const TActorId &source,
        const TIntrusivePtr<TBlobStorageGroupProxyMon> &mon, TEvBlobStorage::TEvDiscover *ev,
        ui64 cookie, NWilson::TTraceId traceId, TInstant now,
        TIntrusivePtr<TStoragePoolCounters> &storagePoolCounters) {
    return new TBlobStorageGroupMirror3dcDiscoverRequest(info, state, source, mon, ev, cookie, std::move(traceId), now,
            storagePoolCounters);
}

}//NKikimr
