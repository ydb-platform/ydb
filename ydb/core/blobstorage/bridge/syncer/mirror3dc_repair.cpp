#include "syncer.h"

#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_mirror3dc_write_quorum.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <map>

namespace NKikimr::NBridge {
namespace {

class TMirror3dcRepairSyncer : public TActorBootstrapped<TMirror3dcRepairSyncer> {
    using TBlob = TEvBlobStorage::TEvAssimilateResult::TBlob;
    using TBarrier = TEvBlobStorage::TEvAssimilateResult::TBarrier;

    const TIntrusivePtr<TBlobStorageGroupInfo> Info;
    const TActorId NotifyId;
    static constexpr TDuration RestorePartTimeout = TDuration::Seconds(30);
    std::optional<ui64> SkipBlocks;
    std::optional<std::tuple<ui64, ui8>> SkipBarriers;
    std::optional<TLogoBlobID> SkipBlobs;
    std::map<std::tuple<ui64, ui8>, TBarrier> Barriers;
    std::deque<TBlob> Blobs;
    ui32 PendingDisk = 0;
    ui32 PendingPart = 0;

    void Finish(NKikimrProto::EReplyStatus status) {
        Send(NotifyId, new TEvMirror3dcRepairScanResult(status));
        PassAway();
    }

    void ReadPage() {
        std::unique_ptr<TEvBlobStorage::TEvAssimilate> request = std::make_unique<TEvBlobStorage::TEvAssimilate>(
            SkipBlocks, SkipBarriers, SkipBlobs, /*ignoreDecommitState=*/ true, /*reverse=*/ true);
        request->ExpectedGeneration = Info->GroupGeneration;
        request->IncludePartPlacement = true;
        SendToBSProxy(SelfId(), Info->GroupID, request.release());
    }

    void Handle(TEvBlobStorage::TEvAssimilateResult::TPtr& ev) {
        TEvBlobStorage::TEvAssimilateResult& result = *ev->Get();
        if (result.Status != NKikimrProto::OK) {
            return Finish(result.Status);
        }
        if (result.Blocks.empty() && result.Barriers.empty() && result.Blobs.empty()) {
            return Finish(NKikimrProto::OK);
        }
        if (!result.Blocks.empty()) {
            SkipBlocks = result.Blocks.back().TabletId;
        }
        for (const TBarrier& barrier : result.Barriers) {
            Barriers.insert_or_assign(barrier.GetKey(), barrier);
        }
        if (!result.Barriers.empty()) {
            const TBarrier& barrier = result.Barriers.back();
            SkipBarriers.emplace(barrier.TabletId, barrier.Channel);
        }
        if (!result.Blobs.empty()) {
            SkipBlobs = result.Blobs.back().Id;
        }
        Blobs.swap(result.Blobs);
        RepairNext();
    }

    void RepairNext() {
        ui32 subgroupSize = Info->Type.BlobSubgroupSize();
        while (!Blobs.empty()) {
            const TBlob& blob = Blobs.front();
            if (auto it = Barriers.find({blob.Id.TabletID(), blob.Id.Channel()}); it != Barriers.end()) {
                const TBarrier& barrier = it->second;
                std::pair<ui32, ui32> blobPosition(blob.Id.Generation(), blob.Id.Step());
                auto coversBlob = [&blobPosition](const TBarrier::TValue& value) {
                    bool hasBarrier = value.RecordGeneration || value.PerGenerationCounter || value.CollectGeneration || value.CollectStep;
                    std::pair<ui32, ui32> collectPosition(value.CollectGeneration, value.CollectStep);
                    return hasBarrier && blobPosition <= collectPosition;
                };
                bool keep = blob.Keep && !blob.DoNotKeep;
                if (coversBlob(barrier.Hard) || (!keep && coversBlob(barrier.Soft))) {
                    Blobs.pop_front();
                    continue;
                }
            }
            std::optional<TMirror3dcWriteQuorumChecker> checker = TMirror3dcWriteQuorumChecker::TryCreate(*Info, blob.Id.Hash());
            if (!checker) {
                return Finish(NKikimrProto::ERROR);
            }
            TSubgroupPartLayout layout;
            for (ui32 part = 0; part < blob.PartDisks.size(); ++part) {
                for (ui32 disk = 0; disk < subgroupSize; ++disk) {
                    if (blob.PartDisks[part] & (1u << disk)) {
                        layout.AddItem(disk, part, Info->Type);
                    }
                }
            }
            for (ui32 disk = 0; disk < subgroupSize; ++disk) {
                ui32 parts = checker->GetPartsToResurrect(layout, disk);
                if (parts) {
                    PendingDisk = disk;
                    PendingPart = std::countr_zero(parts);
                    RestorePart(blob.Id);
                    return;
                }
            }
            Blobs.pop_front();
        }
        ReadPage();
    }

    void RestorePart(const TLogoBlobID& blobId) {
        TVDiskID diskId = Info->GetVDiskInSubgroup(PendingDisk, blobId.Hash());
        std::unique_ptr<TEvVRestorePart> request = std::make_unique<TEvVRestorePart>();
        VDiskIDFromVDiskID(diskId, request->Record.MutableVDiskID());
        LogoBlobIDFromLogoBlobID(TLogoBlobID(blobId, PendingPart + 1), request->Record.MutableBlobID());
        TInstant deadline = TActivationContext::Now() + RestorePartTimeout;
        request->Record.SetDeadline(deadline.GetValue());
        Send(Info->GetActorId(TVDiskIdShort(diskId)), request.release(), IEventHandle::FlagTrackDelivery);
    }

    void Handle(TEvVRestorePartResult::TPtr& ev) {
        NKikimrProto::EReplyStatus status = ev->Get()->Record.GetStatus();
        if (status != NKikimrProto::OK && status != NKikimrProto::ALREADY) {
            return Finish(status);
        }
        Blobs.front().PartDisks[PendingPart] |= 1u << PendingDisk;
        RepairNext();
    }

    void HandleUndelivered() {
        Finish(NKikimrProto::ERROR);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvBlobStorage::TEvAssimilateResult, Handle)
        hFunc(TEvVRestorePartResult, Handle)
        cFunc(TEvents::TSystem::Undelivered, HandleUndelivered)
        cFunc(TEvents::TSystem::Poison, PassAway)
    )

public:
    TMirror3dcRepairSyncer(TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId notifyId)
        : Info(std::move(info))
        , NotifyId(notifyId)
    {}

    void Bootstrap() {
        if (!Info || Info->IsBridged() || Info->Type.GetErasure() != TBlobStorageGroupType::ErasureMirror3dc
                || Info->GetTopology().GetTotalFailRealmsNum() != 3) {
            return Finish(NKikimrProto::ERROR);
        }
        const TMirror3dcGroupState& state = Info->GetMirror3dcState();
        if (!IsValidMirror3dcGroupConfiguration({state.State, state.FailRealmStates})) {
            return Finish(NKikimrProto::ERROR);
        }
        std::optional<TMirror3dcGroupLayouts> layouts = GetMirror3dcGroupLayouts(state.State);
        if (layouts->Durability == EMirror3dcLayout::LAYOUT_0) {
            return Finish(NKikimrProto::ERROR);
        }
        Become(&TThis::StateWork);
        ReadPage();
    }
};

} // namespace

IActor* CreateMirror3dcRepairSyncer(TIntrusivePtr<TBlobStorageGroupInfo> info, TActorId notifyId) {
    return new TMirror3dcRepairSyncer(std::move(info), notifyId);
}

} // namespace NKikimr::NBridge
