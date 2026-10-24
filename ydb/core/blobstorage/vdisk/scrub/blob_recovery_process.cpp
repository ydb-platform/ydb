#include "blob_recovery_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT BS_VDISK_SCRUB

namespace NKikimr {

    void TBlobRecoveryActor::AddBlobQuery(const TLogoBlobID& id, NMatrix::TVectorType needed,
            const std::shared_ptr<TInFlightContext>& context, TEvRecoverBlobResult::TItem *item) {
        YDB_LOG_DEBUG(VDISKP(LogPrefix, "AddBlobQuery"),
            {"marker", "VDS32"},
            {"selfId", SelfId()},
            {"id", id},
            {"needed", needed},
            {"requestId", context->RequestId});
        const TInstant deadline = context->Iterator->first;
        TBlobStorageGroupInfo::TOrderNums nums;
        Info->GetTopology().PickSubgroup(id.Hash(), nums);
        ui32 blobReplyCounter = 0;
        for (ui32 i = 0; i < nums.size(); ++i) {
            const TVDiskID& vdiskId = Info->GetVDiskId(nums[i]); // obtain VDisk
            if (TVDiskIdShort(vdiskId) != VCtx->ShortSelfVDisk) {
                AddExtremeQuery(vdiskId, id, deadline, i);
                ++blobReplyCounter;
            }
        }
        VGetResultMap.emplace(id, TPerBlobInfo{context, item, blobReplyCounter});
    }

    void TBlobRecoveryActor::AddExtremeQuery(const TVDiskID& vdiskId, const TLogoBlobID& id, TInstant deadline, ui32 idxInSubgroup) {
        const auto [_, inserted] = GetsInFlight.emplace(vdiskId, id);

        ui32 worstReplySize = 0;
        if (inserted) {
            const TBlobStorageGroupType& gtype = Info->Type;
            switch (TIngress::IngressMode(gtype)) {
                case TIngress::EMode::GENERIC:
                    if (gtype.GetErasure() == TBlobStorageGroupType::ErasureMirror3dc) {
                        worstReplySize = gtype.PartSize(TLogoBlobID(id, idxInSubgroup % 3 + 1));
                    } else {
                        for (ui32 k = 0; k < gtype.TotalPartCount(); ++k) {
                            worstReplySize += idxInSubgroup >= gtype.TotalPartCount() || k == idxInSubgroup
                                ? gtype.PartSize(TLogoBlobID(id, k + 1)) : 0;
                        }
                    }
                    break;

                case TIngress::EMode::MIRROR3OF4:
                    for (ui32 i = 0; i < 2; ++i) {
                        if (idxInSubgroup % 2 == i || idxInSubgroup >= 4) {
                            worstReplySize += gtype.PartSize(TLogoBlobID(id, i + 1));
                        }
                    }
                    break;
            }
        }

        YDB_LOG_DEBUG(VDISKP(LogPrefix, "AddExtremeQuery"),
            {"marker", "VDS33"},
            {"selfId", SelfId()},
            {"VDiskId", vdiskId},
            {"id", id},
            {"worstReplySize", worstReplySize},
            {"alreadyInFlight", !inserted});
        if (!inserted) { // the request is already in flight
            return;
        }

        TQuery& query = Queries[vdiskId];

        const ui32 maxReplySize = 32_MB;
        if (query.VGet && query.WorstReplySize + worstReplySize > maxReplySize) { // send the request on overflow
            query.Pending.push_back(std::move(query.VGet));
            query.WorstReplySize = 0;
        }

        if (!query.VGet) {
            query.VGet = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(vdiskId, deadline,
                NKikimrBlobStorage::EGetHandleClass::AsyncRead);
        }

        query.VGet->AddExtremeQuery(id, 0, 0);
        query.WorstReplySize += worstReplySize;
    }

    void TBlobRecoveryActor::SendPendingQueries() {
        for (auto& [vdiskId, query] : std::exchange(Queries, {})) {
            Y_VERIFY_S(query.VGet, LogPrefix);
            query.Pending.push_back(std::move(query.VGet));
            auto queueIt = Queues.find(vdiskId);
            Y_VERIFY_S(queueIt != Queues.end(), LogPrefix);
            for (auto& vget : query.Pending) {
                YDB_LOG_DEBUG(VDISKP(LogPrefix, "sending TEvVGet"),
                    {"marker", "VDS34"},
                    {"selfId", SelfId()},
                    {"msg", vget->ToString()});
                Send(queueIt->second.QueueActorId, vget.release());
            }
        }
    }

    void TBlobRecoveryActor::Handle(TEvBlobStorage::TEvVGetResult::TPtr ev) {
        YDB_LOG_DEBUG(VDISKP(LogPrefix, "received TEvVGetResult"),
            {"marker", "VDS35"},
            {"selfId", SelfId()},
            {"msg", ev->Get()->ToString()});

        const TInstant now = TActivationContext::Now();
        const auto& record = ev->Get()->Record;
        const TVDiskID vdiskId = VDiskIDFromVDiskID(record.GetVDiskID());
        std::unordered_map<TLogoBlobID, TInstant, THash<TLogoBlobID>> rerequest;
        std::unordered_map<TLogoBlobID, std::vector<const NKikimrBlobStorage::TQueryResult*>, THash<TLogoBlobID>> results;

        for (const auto& res : record.GetResult()) {
            results[LogoBlobIDFromLogoBlobID(res.GetBlobID()).FullID()].push_back(&res);
        }

        // A VDisk may return several parts of one blob. Consume them together and
        // count this VDisk's response only once, after all its parts have been read.
        for (const auto& [fullId, parts] : results) {
            if (!GetsInFlight.erase(std::make_tuple(vdiskId, fullId))) {
                continue;
            }
            auto [begin, end] = VGetResultMap.equal_range(fullId);
            for (auto it = begin; it != end; ) {
                TPerBlobInfo& info = it->second;
                if (auto context = info.Context.lock()) { // context acquired, request is still intact
                    auto& item = *info.Item;
                    bool update = false;
                    bool retry = false;
                    for (const auto* res : parts) {
                        item.Keep |= res->GetKeep();
                        item.DoNotKeep |= res->GetDoNotKeep();
                        const TLogoBlobID id = LogoBlobIDFromLogoBlobID(res->GetBlobID());
                        if (res->GetStatus() == NKikimrProto::OK && id.PartId()
                                && id.PartId() <= Info->Type.TotalPartCount()) {
                            TRope data = ev->Get()->GetBlobData(*res);
                            if (data.size() == Info->Type.PartSize(id)) {
                                item.SetPartData(id, std::move(data));
                                update = true;
                            }
                        }
                        retry |= res->GetStatus() == NKikimrProto::DEADLINE && now < context->Iterator->first;
                    }
                    if (retry) {
                        auto& deadline = rerequest[fullId];
                        deadline = Max(deadline, context->Iterator->first);
                    }
                    const bool term = !retry && !--info.BlobReplyCounter;
                    if (term || update) {
                        item.Status = ProcessItemData(item);
                        if (item.Status == NKikimrProto::UNKNOWN && term) {
                            item.Status = NKikimrProto::NODATA;
                        }
                        YDB_LOG_DEBUG(VDISKP(LogPrefix, "processing item"),
                            {"marker", "VDS36"},
                            {"selfId", SelfId()},
                            {"requestId", context->RequestId},
                            {"id", fullId},
                            {"last", term},
                            {"dataUpdated", update},
                            {"exitStatus", item.Status});
                    }
                    if (item.Status != NKikimrProto::UNKNOWN) {
                        if (!--context->NumUnrespondedBlobs) {
                            context->SendResult(SelfId());
                            InFlight.erase(context->Iterator);
                        }
                        it = VGetResultMap.erase(it);
                        continue;
                    }
                    ++it;
                } else { // request deadlined or canceled, we erase it from the map
                    it = VGetResultMap.erase(it);
                }
            }
        }

        for (const auto& [id, deadline] : rerequest) {
            AddExtremeQuery(vdiskId, id, deadline, Info->GetTopology().GetIdxInSubgroup(vdiskId, id.Hash()));
        }
        SendPendingQueries();
    }

    NKikimrProto::EReplyStatus TBlobRecoveryActor::ProcessItemData(TEvRecoverBlobResult::TItem& item) {
        if (item.GetAvailableParts().IsSupersetOf(item.Needed)) {
            return NKikimrProto::OK;
        }
        // Empty metadata parts satisfy Needed directly, but cannot reconstruct payload-bearing parts.
        ui32 numParts = 0;
        for (ui32 i = 0; i < item.Parts.size(); ++i) {
            if ((item.PartsMask & (1u << i)) && Info->Type.PartSize(TLogoBlobID(item.BlobId, i + 1))) {
                ++numParts;
            }
        }
        if (numParts >= Info->Type.MinimalRestorablePartCount()) {
            Y_DEBUG_ABORT_UNLESS(item.Parts.size() == Info->Type.TotalPartCount());

            ui32 restoreMask = 0;
            for (ui8 i = item.Needed.FirstPosition(); i != item.Needed.GetSize(); i = item.Needed.NextPosition(i)) {
                restoreMask |= 1 << i;
            }
            restoreMask &= ~item.PartsMask;

            ErasureRestore((TErasureType::ECrcMode)item.BlobId.CrcMode(), Info->Type, item.BlobId.BlobSize(), nullptr,
                item.Parts, restoreMask);
            item.PartsMask |= restoreMask;

            // clear metadata parts in mirror erasures
            for (ui32 i = 0; i < item.Parts.size(); ++i) {
                if (!Info->Type.PartSize(TLogoBlobID(item.BlobId, i + 1))) {
                    item.Parts[i] = TRope();
                }
            }
            return NKikimrProto::OK;
        } else {
            return NKikimrProto::UNKNOWN;
        }
    }

} // NKikimr
