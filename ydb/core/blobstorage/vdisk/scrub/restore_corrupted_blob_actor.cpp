#include "restore_corrupted_blob_actor.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_heap_it.h>

namespace NKikimr {

    class TRestoreCorruptedBlobActor : public TActorBootstrapped<TRestoreCorruptedBlobActor> {
        const TActorId SkeletonId;
        const TIntrusivePtr<TBlobStorageGroupInfo> Info;
        const TIntrusivePtr<TVDiskContext> VCtx;
        const TString& LogPrefix;
        const TPDiskCtxPtr PDiskCtx;
        const TInstant Deadline;
        std::vector<TEvRecoverBlobResult::TItem> Items;
        const bool WriteRestoredParts;
        const bool ReportNonrestoredParts;
        const TActorId Sender;
        const ui64 Cookie;
        bool AllQueriesDone = false;
        ui32 ReadsPending = 0;
        ui32 WritesPending = 0;
        std::optional<THullDsSnap> Snap;

        enum {
            EvIssueQuery = EventSpaceBegin(TEvents::ES_PRIVATE),
        };
        struct TEvIssueQuery : TEventLocal<TEvIssueQuery, EvIssueQuery> {};

        struct TReadCmd {
            TDiskPart Location;
            TEvRestoreCorruptedBlobResult::TItem *Item;
            NMatrix::TVectorType Parts;

            TReadCmd(TDiskPart location, TEvRestoreCorruptedBlobResult::TItem *item, NMatrix::TVectorType parts)
                : Location(location)
                , Item(item)
                , Parts(parts)
            {}
        };

        std::vector<TReadCmd> ReadQ;

        struct TDataExtractorMerger {
            std::vector<TReadCmd>& ReadQ;
            const TBlobStorageGroupType GType;
            TEvRestoreCorruptedBlobResult::TItem *Item = nullptr;

            static constexpr bool HaveToMergeData() { return true; }

            void Begin(TEvRestoreCorruptedBlobResult::TItem *item) {
                Item = item;
            }

            void AddFromSegment(const TMemRecLogoBlob& memRec, const TDiskPart *outbound, const TKeyLogoBlob& /*key*/,
                    ui64 /*circaLsn*/) {
                const NMatrix::TVectorType local = memRec.GetLocalParts(GType);
                if ((local & Item->Needed).Empty()) {
                    return; // no useful parts here
                }
                TDiskDataExtractor extr;
                memRec.GetDiskData(&extr, outbound);
                switch (extr.BlobType) {
                    case TBlobType::DiskBlob:
                    case TBlobType::HugeBlob:
                        ReadQ.emplace_back(extr.SwearOne(), Item, local);
                        break;

                    case TBlobType::ManyHugeBlobs:
                        for (ui32 i = local.FirstPosition(); i != local.GetSize(); i = local.NextPosition(i), ++extr.Begin) {
                            if (Item->Needed.Get(i)) {
                                ReadQ.emplace_back(*extr.Begin, Item, NMatrix::TVectorType::MakeOneHot(i, local.GetSize()));
                            }
                        }
                        break;

                    case TBlobType::MemBlob:
                        Y_ABORT();
                }
            }

            void AddFromFresh(const TMemRecLogoBlob& memRec, const TRope *data, const TKeyLogoBlob& key, ui64 /*lsn*/) {
                if (data) {
                    const NMatrix::TVectorType local = memRec.GetLocalParts(GType);
                    TDiskBlob blob(data, local, GType, key.LogoBlobID());
                    for (auto it = blob.begin(); it != blob.end(); ++it) {
                        if (Item->Needed.Get(it.GetPartId() - 1)) {
                            Item->SetPartData(TLogoBlobID(key.LogoBlobID(), it.GetPartId()), it.GetPart());
                        }
                    }
                } else {
                    // process possible on-disk huge blob stored in fresh segment
                    AddFromSegment(memRec, nullptr, key, Max<ui64>());
                }
            }
        };

    public:
        TRestoreCorruptedBlobActor(TActorId skeletonId, TEvRestoreCorruptedBlob::TPtr& ev,
                TIntrusivePtr<TBlobStorageGroupInfo> info, TIntrusivePtr<TVDiskContext> vctx,
                TPDiskCtxPtr pdiskCtx)
            : SkeletonId(skeletonId)
            , Info(std::move(info))
            , VCtx(std::move(vctx))
            , LogPrefix(VCtx->VDiskLogPrefix)
            , PDiskCtx(std::move(pdiskCtx))
            , Deadline(ev->Get()->Deadline)
            , WriteRestoredParts(ev->Get()->WriteRestoredParts)
            , ReportNonrestoredParts(ev->Get()->ReportNonrestoredParts)
            , Sender(ev->Sender)
            , Cookie(ev->Cookie)
        {
            auto& items = ev->Get()->Items;
            Items.reserve(items.size());
            for (auto& item : items) {
                Items.push_back(std::move(item));
            }
        }

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_RESTORE_CORRUPTED_BLOB_ACTOR;
        }

        void Bootstrap() {
            STLOG(PRI_DEBUG, BS_VDISK_SCRUB, VDS09, VDISKP(LogPrefix, "bootstrapping TRestoreCorruptedBlobActor"),
                (SelfId, SelfId()));
            Send(SkeletonId, new TEvTakeHullSnapshot(false));
            Become(&TThis::StateFunc, Deadline, new TEvents::TEvWakeup);
        }

        void Handle(TEvTakeHullSnapshotResult::TPtr ev) {
            Snap.emplace(std::move(ev->Get()->Snap));
            Snap->BlocksSnap.Destroy();
            Snap->BarriersSnap.Destroy();
            using TLevelIndexSnapshot = NKikimr::TLevelIndexSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;
            TLevelIndexSnapshot::TForwardIterator iter(Snap->HullCtx, &Snap->LogoBlobsSnap);
            TDataExtractorMerger merger{ReadQ, Info->Type};
            for (auto& item : Items) {
                iter.Seek(item.BlobId);
                if (iter.Valid() && iter.GetCurKey() == item.BlobId) { // item found, process through merger to extract data parts
                    merger.Begin(&item);
                    iter.PutToMerger(&merger);
                } else { // item not found in local metabase -- report error for this one
                    item.Status = NKikimrProto::ERROR;
                }
            }

            // filter out the read queue -- remove items that are already available or not needed
            auto remove = [](const TReadCmd& item) {
                const NMatrix::TVectorType missing = item.Item->Needed - item.Item->GetAvailableParts();
                return (missing & item.Parts).Empty() || item.Location == item.Item->CorruptedPart;
            };
            ReadQ.erase(std::remove_if(ReadQ.begin(), ReadQ.end(), remove), ReadQ.end());

            if (ReadQ.empty()) { // nothing to read, issue restore queries
                Snap.reset();
                IssueQuery();
            } else { // some data to read, generate reads
                for (auto& cmd : ReadQ) {
                    STLOG(PRI_DEBUG, BS_VDISK_SCRUB, VDS18, VDISKP(LogPrefix, "sending read to PDisk"),
                        (SelfId, SelfId()), (Location, cmd.Location), (BlobId, cmd.Item->BlobId), (Parts, cmd.Parts));
                    auto msg = std::make_unique<NPDisk::TEvChunkRead>(PDiskCtx->Dsk->Owner, PDiskCtx->Dsk->OwnerRound,
                        cmd.Location.ChunkIdx, cmd.Location.Offset, cmd.Location.Size, NPriRead::HullLow, &cmd);
                    Send(PDiskCtx->PDiskId, msg.release());
                    ++ReadsPending;
                }
            }
        }

        void Handle(NPDisk::TEvChunkReadResult::TPtr ev) {
            Y_ABORT_UNLESS(ReadsPending);
            --ReadsPending;

            auto *msg = ev->Get();
            TReadCmd *cmd = static_cast<TReadCmd*>(msg->Cookie);
            if (msg->Status == NKikimrProto::OK && msg->Data.IsReadable()) {
                auto& item = *cmd->Item;
                TRope rope(msg->Data.ToString());
                TDiskBlob blob(&rope, cmd->Parts, Info->Type, item.BlobId);
                for (auto it = blob.begin(); it != blob.end(); ++it) {
                    if (item.Needed.Get(it.GetPartId() - 1)) {
                        item.SetPartData(TLogoBlobID(item.BlobId, it.GetPartId()), it.GetPart());
                    }
                }
                const NMatrix::TVectorType& avail = item.GetAvailableParts();
                if (avail.IsSupersetOf(item.Needed)) {
                    item.Status = NKikimrProto::OK; // item has been fully read from local sources
                }
            } else {
                STLOG(PRI_WARN, BS_VDISK_SCRUB, VDS26, VDISKP(LogPrefix, "failed to read data from PDisk"),
                    (SelfId, SelfId()), (Location, cmd->Location), (BlobId, cmd->Item->BlobId), (Parts, cmd->Parts),
                    (Status, msg->Status));
            }

            if (!ReadsPending) {
                Snap.reset();
                IssueQuery();
            }
        }

        void IssueQuery() {
            STLOG(PRI_DEBUG, BS_VDISK_SCRUB, VDS00, VDISKP(LogPrefix, "IssueQuery"), (SelfId, SelfId()));

            std::unique_ptr<TEvRecoverBlob> ev;
            for (size_t i = 0; i < Items.size(); ++i) {
                const auto& item = Items[i];
                if (item.Status == NKikimrProto::UNKNOWN) {
                    if (!ev) {
                        ev.reset(new TEvRecoverBlob);
                        ev->Deadline = Deadline;
                    }
                    ev->Items.emplace_back(item.BlobId, TStackVec<TRope, 8>(item.Parts), item.PartsMask, item.Needed, TDiskPart(), i);
                    STLOG(PRI_DEBUG, BS_VDISK_SCRUB, VDS17, VDISKP(LogPrefix, "IssueQuery item"), (SelfId, SelfId()),
                        (BlobId, item.BlobId), (PartsMask, item.PartsMask), (Needed, item.Needed));
                }
            }
            if (ev) {
                Send(SkeletonId, ev.release());
            } else {
                AllQueriesDone = true;
                TryToComplete();
            }
        }

        void TryToComplete() {
            if (AllQueriesDone && (!WriteRestoredParts || !WritesPending)) {
                PassAway();
            }
        }

        void Handle(TEvRecoverBlobResult::TPtr ev) {
            STLOG(PRI_DEBUG, BS_VDISK_SCRUB, VDS24, VDISKP(LogPrefix, "Handle(TEvRecoverBlobResult)"), (SelfId, SelfId()));

            for (auto& item : ev->Get()->Items) {
                auto& myItem = Items[item.Cookie];
                Y_ABORT_UNLESS(myItem.Status == NKikimrProto::UNKNOWN);
                myItem.Parts = std::move(item.Parts);
                myItem.PartsMask = item.PartsMask;
                if (item.Status != NKikimrProto::NODATA) { // we keep trying to fetch NODATA's till deadline
                    myItem.Status = item.Status;
                    if (myItem.Status == NKikimrProto::OK && WriteRestoredParts) {
                        IssueWrite(myItem, item.Cookie);
                    }
                }
                STLOG(PRI_DEBUG, BS_VDISK_SCRUB, VDS43, VDISKP(LogPrefix, "Handle(TEvRecoverBlobResult) item"),
                    (SelfId, SelfId()), (BlobId, item.BlobId), (Status, item.Status), (PartsMask, item.PartsMask));
            }
            for (const auto& item : Items) {
                if (item.Status == NKikimrProto::UNKNOWN) {
                    return Schedule(TDuration::Seconds(5), new TEvIssueQuery);
                }
            }
            IssueQuery();
        }

        void IssueWrite(TEvRestoreCorruptedBlobResult::TItem& item, ui64 index) {
            const TVDiskID& vdiskId = Info->GetVDiskId(VCtx->ShortSelfVDisk);
            for (ui32 i = item.Needed.FirstPosition(); i != item.Needed.GetSize(); i = item.Needed.NextPosition(i)) {
                const TLogoBlobID blobId(item.BlobId, i + 1);
                const TRope& buffer = item.GetPartData(blobId);
                Y_ABORT_UNLESS(buffer.size() == Info->Type.PartSize(blobId));
                Y_ABORT_UNLESS(WriteRestoredParts);
                auto ev = std::make_unique<TEvBlobStorage::TEvVPut>(blobId, buffer, vdiskId, true, &index, Deadline,
                    NKikimrBlobStorage::EPutHandleClass::AsyncBlob);
                ev->RewriteBlob = true;
                Send(SkeletonId, ev.release());
                ++WritesPending;
            }
        }

        void Handle(TEvBlobStorage::TEvVPutResult::TPtr ev) {
            STLOG(PRI_DEBUG, BS_VDISK_SCRUB, VDS37, VDISKP(LogPrefix, "received TEvVPutResult"), (SelfId, SelfId()),
                (Msg, ev->Get()->ToString()));
            Y_ABORT_UNLESS(WritesPending);
            --WritesPending;
            const auto& record = ev->Get()->Record;
            auto& item = Items[record.GetCookie()];
            switch (record.GetStatus()) {
                case NKikimrProto::RACE:
                case NKikimrProto::ERROR:
                case NKikimrProto::OUT_OF_SPACE:
                case NKikimrProto::VDISK_ERROR_STATE:
                    STLOG(PRI_ERROR, BS_VDISK_SCRUB, VDS10, VDISKP(LogPrefix, "failed to restore blob"),
                        (SelfId, SelfId()), (Msg, ev->Get()->ToString()));
                    if (item.Status == NKikimrProto::OK) {
                        item.Status = NKikimrProto::ERROR;
                    }
                    break;

                case NKikimrProto::DEADLINE:
                    item.Status = NKikimrProto::DEADLINE;
                    break;

                case NKikimrProto::OK: // keep item status untouched
                    break;

                default:
                    Y_FAIL_S("unexpected TEvVPutResult status TEvVPutResult# " << ev->Get()->ToString());
            }
            TryToComplete();
        }

        void PassAway() override {
            STLOG(PRI_DEBUG, BS_VDISK_SCRUB, VDS15, VDISKP(LogPrefix, "TRestoreCorruptedBlobActor terminating"),
                (SelfId, SelfId()));
            ReduceStatus(NKikimrProto::ERROR);

            if (ReportNonrestoredParts) {
                std::vector<TEvNonrestoredCorruptedBlobNotify::TItem> nonrestoredItems;
                for (const auto& item : Items) {
                    if (item.Status != NKikimrProto::OK) {
                        nonrestoredItems.emplace_back(item.BlobId, item.Needed, item.CorruptedPart);
                    }
                }
                if (!nonrestoredItems.empty()) {
                    Send(SkeletonId, new TEvNonrestoredCorruptedBlobNotify(std::move(nonrestoredItems)));
                }
            }

            Send(Sender, new TEvRestoreCorruptedBlobResult(std::move(Items)), 0, Cookie);
            TActorBootstrapped::PassAway();
        }

        void HandleWakeup() {
            ReduceStatus(NKikimrProto::DEADLINE);
            PassAway();
        }

        void ReduceStatus(NKikimrProto::EReplyStatus status) {
            for (auto& item : Items) {
                item.Status = item.Status != NKikimrProto::UNKNOWN ? item.Status : status;
            }
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvTakeHullSnapshotResult, Handle);
            hFunc(NPDisk::TEvChunkReadResult, Handle);
            hFunc(TEvRecoverBlobResult, Handle);
            cFunc(EvIssueQuery, IssueQuery);
            hFunc(TEvBlobStorage::TEvVPutResult, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        )
    };

    IActor *CreateRestoreCorruptedBlobActor(TActorId skeletonId, TEvRestoreCorruptedBlob::TPtr& ev,
            TIntrusivePtr<TBlobStorageGroupInfo> info, TIntrusivePtr<TVDiskContext> vctx,
            TPDiskCtxPtr pdiskCtx) {
        return new TRestoreCorruptedBlobActor(skeletonId, ev, std::move(info), std::move(vctx), std::move(pdiskCtx));
    }

} // NKikimr
