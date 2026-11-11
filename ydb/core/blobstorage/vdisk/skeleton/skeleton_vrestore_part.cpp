#include "skeleton_vrestore_part.h"

#include <ydb/core/blobstorage/vdisk/scrub/blob_recovery.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr {
namespace {

    class TVRestorePartActor : public TActorBootstrapped<TVRestorePartActor> {
        const TVDiskID SelfVDiskId;
        const TActorId NotifyId;
        const TActorId SkeletonId;
        const TIntrusivePtr<TVDiskConfig> Cfg;
        const std::shared_ptr<TBlobStorageGroupInfo::TTopology> Top;
        TEvVRestorePart::TPtr Ev;
        TLogoBlobID BlobId;
        TInstant Deadline;
        bool LocalReadDone = false;
        TEvRecoverBlobResult::TPtr RecoveryResult;

        void Reply(NKikimrProto::EReplyStatus status, const TString& error = {},
                NKikimrBlobStorage::TEvVRestorePartResult::EResponseOrigin origin = NKikimrBlobStorage::TEvVRestorePartResult::VRESTORE_PART_ACTOR) {
            auto result = std::make_unique<TEvVRestorePartResult>();
            result->Record.SetStatus(status);
            result->Record.SetErrorReason(error);
            result->Record.SetResponseOrigin(origin);
            Send(Ev->Sender, result.release(), 0, Ev->Cookie);
            PassAway();
        }

        void Handle(TEvRecoverBlobResult::TPtr ev) {
            if (!LocalReadDone) {
                RecoveryResult = std::move(ev);
                return;
            }
            auto& items = ev->Get()->Items;
            if (items.size() != 1 || items.front().BlobId != BlobId.FullID()) {
                return Reply(NKikimrProto::ERROR, "Invalid TEvRecoverBlobResult");
            }
            auto& item = items.front();
            if (item.Status != NKikimrProto::OK) {
                return Reply(item.Status, {}, NKikimrBlobStorage::TEvVRestorePartResult::BLOB_RECOVERY_ACTOR);
            }
            if (!(item.PartsMask & (1u << (BlobId.PartId() - 1)))) {
                return Reply(NKikimrProto::ERROR, "TEvRecoverBlobResult has no requested part");
            }
            const TRope& data = item.GetPartData(BlobId);
            if (data.size() != Top->GType.PartSize(BlobId)) {
                return Reply(NKikimrProto::ERROR, "Recovered part size mismatch");
            }
            if (TActivationContext::Now() >= Deadline) {
                return HandleTimeout();
            }

            // TEvRecoverBlob returns the bytes stored by VDisks, including group encryption.
            // RewriteBlob also replaces corrupted huge parts instead of retaining their old location.
            auto put = std::make_unique<TEvBlobStorage::TEvVPut>(BlobId, data, SelfVDiskId, true, nullptr,
                Deadline, NKikimrBlobStorage::AsyncBlob,
                Cfg->BlobHeaderMode == EBlobHeaderMode::XXH3_64BIT_HEADER, TWriteSource::RestoredCorruptedBlob);
            put->Record.SetIssueKeepFlag(item.Keep && !item.DoNotKeep);
            put->RewriteBlob = true;
            put->IsInternal = true;
            Send(SkeletonId, put.release(), IEventHandle::FlagTrackDelivery);
            Become(&TThis::StateWrite);
        }

        void Recover() {
            if (TActivationContext::Now() >= Deadline) {
                return HandleTimeout();
            }
            auto recover = std::make_unique<TEvRecoverBlob>();
            recover->Deadline = Deadline;
            recover->IgnoreConnectionQuorum = true;
            recover->Items.emplace_back(BlobId.FullID(),
                NMatrix::TVectorType::MakeOneHot(BlobId.PartId() - 1, Top->GType.TotalPartCount()),
                Top->GType, TDiskPart());
            Send(SkeletonId, recover.release(), IEventHandle::FlagTrackDelivery);
            auto get = TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(SelfVDiskId, Deadline,
                NKikimrBlobStorage::AsyncRead, TEvBlobStorage::TEvVGet::EFlags::None, {}, {BlobId});
            get->SuppressCorruptedBlobRestoration = true;
            Send(SkeletonId, get.release(), IEventHandle::FlagTrackDelivery);
            Become(&TThis::StateRecover);
        }

        void Handle(TEvBlobStorage::TEvVGetResult::TPtr ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetStatus() != NKikimrProto::OK) {
                return Reply(record.GetStatus(), record.GetErrorReason(),
                    NKikimrBlobStorage::TEvVRestorePartResult::SKELETON);
            }
            for (const auto& result : record.GetResult()) {
                if (result.GetStatus() == NKikimrProto::OK
                        && LogoBlobIDFromLogoBlobID(result.GetBlobID()) == BlobId
                        && ev->Get()->GetBlobData(result).size() == Top->GType.PartSize(BlobId)) {
                    return Reply(NKikimrProto::ALREADY);
                }
            }
            LocalReadDone = true;
            if (RecoveryResult) {
                Handle(std::move(RecoveryResult));
            }
        }

        void Handle(TEvBlobStorage::TEvVPutResult::TPtr ev) {
            const auto& record = ev->Get()->Record;
            Reply(record.GetStatus(), record.GetErrorReason(), NKikimrBlobStorage::TEvVRestorePartResult::SKELETON);
        }

        void HandleTimeout() {
            Reply(NKikimrProto::DEADLINE, "Timed out waiting for VRestorePart on this VDisk");
        }

        void HandleUndelivered() {
            Reply(NKikimrProto::NOTREADY, "VDisk is unavailable");
        }

        void HandlePoison() {
            Reply(NKikimrProto::NOTREADY, "VDisk is shutting down");
        }

        void PassAway() override {
            Send(NotifyId, new TEvents::TEvGone);
            TActorBootstrapped::PassAway();
        }

    public:
        TVRestorePartActor(const TVDiskID& selfVDiskId, const TActorId& notifyId, const TActorId& skeletonId,
                TIntrusivePtr<TVDiskConfig> cfg, std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
                TEvVRestorePart::TPtr& ev)
            : SelfVDiskId(selfVDiskId)
            , NotifyId(notifyId)
            , SkeletonId(skeletonId)
            , Cfg(std::move(cfg))
            , Top(std::move(top))
            , Ev(ev)
        {}

        void Bootstrap() {
            if (Top->GType.GetErasure() != TBlobStorageGroupType::ErasureMirror3dc) {
                return Reply(NKikimrProto::ERROR, "VRestorePart is only supported for mirror-3-dc");
            }
            const auto& record = Ev->Get()->Record;
            const auto& vdisk = record.GetVDiskID();
            if (vdisk.GetRing() > Max<ui8>() || vdisk.GetDomain() > Max<ui8>() || vdisk.GetVDisk() > Max<ui8>()
                    || VDiskIDFromVDiskID(vdisk) != SelfVDiskId) {
                return Reply(NKikimrProto::RACE, TStringBuilder() << "VDiskId mismatch: expected# " << SelfVDiskId
                    << " provided# {" << vdisk.ShortDebugString() << "}");
            }
            BlobId = LogoBlobIDFromLogoBlobID(record.GetBlobID());
            if (!BlobId || !BlobId.BlobSize() || !BlobId.PartId() || BlobId.PartId() > Top->GType.TotalPartCount()) {
                return Reply(NKikimrProto::ERROR, "Invalid BlobId or PartId for this group");
            }
            if (Cfg->BaseInfo.ReadOnly || Cfg->BaseInfo.DonorMode) {
                return Reply(NKikimrProto::ERROR, "VDisk is in readonly or donor mode");
            }
            Deadline = TInstant::FromValue(record.GetDeadline());
            if (TActivationContext::Now() >= Deadline) {
                return HandleTimeout();
            }
            Schedule(Deadline, new TEvents::TEvWakeup);
            Recover();
        }

        STRICT_STFUNC(StateRecover,
            hFunc(TEvRecoverBlobResult, Handle);
            hFunc(TEvBlobStorage::TEvVGetResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            cFunc(TEvents::TSystem::Undelivered, HandleUndelivered);
            cFunc(TEvents::TSystem::Poison, HandlePoison);
        )

        STRICT_STFUNC(StateWrite,
            hFunc(TEvBlobStorage::TEvVPutResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
            cFunc(TEvents::TSystem::Undelivered, HandleUndelivered);
            cFunc(TEvents::TSystem::Poison, HandlePoison);
        )
    };

} // anonymous namespace

IActor* CreateVRestorePartActor(const TVDiskID& selfVDiskId, const TActorId& notifyId, const TActorId& skeletonId,
        TIntrusivePtr<TVDiskConfig> cfg, std::shared_ptr<TBlobStorageGroupInfo::TTopology> top,
        TEvVRestorePart::TPtr& ev) {
    return new TVRestorePartActor(selfVDiskId, notifyId, skeletonId, std::move(cfg), std::move(top), ev);
}

} // NKikimr
