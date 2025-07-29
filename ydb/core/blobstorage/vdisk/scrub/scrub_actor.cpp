#include "scrub_actor.h"
#include "scrub_actor_impl.h"
#include "blob_recovery.h"
#include "restore_corrupted_blob_actor.h"

namespace NKikimr {

    TScrubCoroImpl::TScrubCoroImpl(TScrubContext::TPtr scrubCtx, NKikimrVDiskData::TScrubEntrypoint scrubEntrypoint,
            ui64 scrubEntrypointLsn)
        : TActorCoroImpl(64_KB)
        , ScrubCtx(std::move(scrubCtx))
        , VCtx(ScrubCtx->VCtx)
        , Info(ScrubCtx->Info)
        , LogPrefix(VCtx->VDiskLogPrefix)
        , Counters(VCtx->VDiskCounters->GetSubgroup("subsystem", "scrub"))
        , MonGroup(Counters)
        , DeepScrubbingSubgroups(VCtx->VDiskCounters->GetSubgroup("subsystem", "deepScrubbing"))
        , Arena(&TScrubCoroImpl::AllocateRopeArenaChunk)
        , ScrubEntrypoint(std::move(scrubEntrypoint))
        , ScrubEntrypointLsn(scrubEntrypointLsn)
    {}

    void TScrubCoroImpl::ProcessUnexpectedEvent(TAutoPtr<IEventHandle> ev) {
        switch (const ui32 type = ev->GetTypeRewrite()) {
            hFunc(NMon::TEvHttpInfo, Handle);
            fFunc(TEvBlobStorage::EvScrubAwait, HandleScrubAwait);
            fFunc(TEvBlobStorage::EvVGenerationChange, ForwardToBlobRecoveryActor);
            fFunc(TEvBlobStorage::EvRecoverBlob, ForwardToBlobRecoveryActor);
            hFunc(TEvRestoreCorruptedBlobResult, Handle);
            hFunc(TEvNonrestoredCorruptedBlobNotify, Handle);
            hFunc(NPDisk::TEvLogResult, Handle);
            hFunc(NPDisk::TEvCutLog, Handle);
            hFunc(TEvTakeHullSnapshotResult, Handle);

            case TEvents::TSystem::Poison:
                throw TExPoison();

            default:
                Y_ABORT("unexpected event Type# 0x%08" PRIx32, type);
        }
    }

    void TScrubCoroImpl::HandleScrubAwait(TAutoPtr<IEventHandle> ev) {
        ScrubAwaitQueue.emplace(ScrubIterationCounter + 1, std::make_pair(ev->Sender, ev->Cookie));
    }

    void TScrubCoroImpl::ForwardToBlobRecoveryActor(TAutoPtr<IEventHandle> ev) {
        Send(IEventHandle::Forward(std::move(ev), BlobRecoveryActorId));
    }

    void TScrubCoroImpl::Run() {
        // unpack entrypoint
        for (const auto& item : ScrubEntrypoint.GetUnreadableBlobs()) {
            UnreadableBlobs.emplace(LogoBlobIDFromLogoBlobID(item.GetBlobId()), TUnreadableBlobState(
                NMatrix::TVectorType(item.GetUnreadableParts(), Info->Type.TotalPartCount()),
                item.GetCorruptedPart()));
        }

        // update log cutter
        if (ScrubEntrypointLsn) {
            Send(ScrubCtx->LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::Scrub, ScrubEntrypointLsn));
        }

        BlobRecoveryActorId = Register(CreateBlobRecoveryActor(VCtx, Info, Counters));
        try {
            for (;;) {
                RequestState();
                const TInstant start = TActorCoroImpl::Now();
                const TInstant end = start + TDuration::Seconds(30);
                do {
                    Quantum();
                    Send(ScrubCtx->SkeletonId, new TEvReportScrubStatus(!UnreadableBlobs.empty()));
                } while (State && TActorCoroImpl::Now() < end);
                Send(ScrubCtx->SkeletonId, new TEvReportScrubStatus(!UnreadableBlobs.empty()));
                CommitStateUpdate();
            }
        } catch (const TExDie&) {
            STLOGX(GetActorContext(), PRI_DEBUG, BS_VDISK_SCRUB, VDS23, VDISKP(LogPrefix, "catched TExDie"));
        } catch (const TDtorException&) {
            return; // actor system is stopping, no actor activities allowed
        } catch (const TExPoison&) { // poison pill from the skeleton
            STLOGX(GetActorContext(), PRI_DEBUG, BS_VDISK_SCRUB, VDS25, VDISKP(LogPrefix, "caught TExPoison"));
        }
        Send(new IEventHandle(TEvents::TSystem::Poison, 0, std::exchange(BlobRecoveryActorId, {}), {}, nullptr, 0));
    }

    void TScrubCoroImpl::RequestState() {
        STLOGX(GetActorContext(), PRI_DEBUG, BS_VDISK_SCRUB, VDS01, VDISKP(LogPrefix, "requesting scrub state"));
        Send(MakeBlobStorageNodeWardenID(SelfActorId.NodeId()), new TEvBlobStorage::TEvControllerScrubQueryStartQuantum(
            ScrubCtx->NodeId, ScrubCtx->PDiskId, ScrubCtx->VSlotId), 0, ScrubCtx->ScrubCookie);
        CurrentState = TStringBuilder() << "in queue for scrub state";
        auto res = WaitForSpecificEvent<TEvBlobStorage::TEvControllerScrubStartQuantum>(&TScrubCoroImpl::ProcessUnexpectedEvent);
        const auto& r = res->Get()->Record;
        if (r.HasState()) {
            State.emplace();
            if (!State->ParseFromString(r.GetState())) {
                STLOGX(GetActorContext(), PRI_CRIT, BS_VDISK_SCRUB, VDS06, VDISKP(LogPrefix, "failed to parse scrub state protobuf"));
                State.reset();
            } else if (State->HasIncarnationGuid() && State->GetIncarnationGuid() != ScrubCtx->IncarnationGuid) {
                State.reset(); // restart scrub from the beginning
            }
        } else {
            State.reset();
        }
        STLOGX(GetActorContext(), PRI_INFO, BS_VDISK_SCRUB, VDS02, VDISKP(LogPrefix, "requested scrub state"), (State, State));
    }

    void TScrubCoroImpl::Quantum() {
        TakeSnapshot();

        // create an ordered set of available SSTs
        auto& slice = Snap->LogoBlobsSnap.SliceSnap;
        using T = std::decay_t<decltype(slice)>;
        T::TSstIterator iter(&slice);
        std::vector<TLevelSegmentPtr> segs;
        for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
            segs.push_back(iter.Get().SstPtr);
        }
        auto comp = [](const TLevelSegmentPtr& x, const TLevelSegmentPtr& y) { return x->AssignedSstId < y->AssignedSstId; };
        std::sort(segs.begin(), segs.end(), comp);

        if (!State) { // we are starting a new cycle of scrubbing; it is started by scrubbing huge blobs of the VDisk
            ++ScrubIterationCounter;
            Checkpoints = 0;
            Success = true;
        } else {
            Success = State->GetSuccess();
        }
        if (!State || State->HasBlobId()) { // start or resume huge blob scrubbing
            if (!State) {
                State.emplace();
            }
            ScrubHugeBlobs();
        } else {
            const ui64 sstId = State->HasNextSstIdToScrub() ? State->GetNextSstIdToScrub() : Max<ui64>();
            auto comp = [](ui64 x, const TLevelSegmentPtr& y) { return x < y->AssignedSstId; };
            if (auto it = std::upper_bound(segs.begin(), segs.end(), sstId, comp); it != segs.begin()) {
                --it;
                if (it != segs.begin()) {
                    State->SetNextSstIdToScrub((*std::prev(it))->AssignedSstId);
                } else {
                    State.reset(); // we're done just after this SST
                }
                ScrubSst(*it);
            } else {
                State.reset(); // we're done
            }
        }

        // validate currently held blobs
        FilterUnreadableBlobs(*Snap, *GetBarriersEssence());

        ReleaseSnapshot();

        if (const ui64 cookie = GenerateRestoreCorruptedBlobQuery()) {
            while (LastReceivedRestoreCookie < cookie) {
                ProcessUnexpectedEvent(WaitForEvent());
            }
        }

        if (!State) { // full scrubbing cycle has finished
            ++ScrubIterationCounter;
            TScrubAwaitQueue::iterator it;
            for (it = ScrubAwaitQueue.begin(); it != ScrubAwaitQueue.end() && it->first < ScrubIterationCounter; ++it) {
                const auto& [actorId, cookie] = it->second;
                Send(actorId, new TEvScrubNotify(Checkpoints, Success && UnreadableBlobs.empty()), 0, cookie);
            }
            ScrubAwaitQueue.erase(ScrubAwaitQueue.begin(), it);
        }

        if (State) {
            State->SetSuccess(Success && UnreadableBlobs.empty());
            State->SetIncarnationGuid(ScrubCtx->IncarnationGuid);
        }
    }

    void TScrubCoroImpl::CommitStateUpdate() {
        STLOGX(GetActorContext(), PRI_INFO, BS_VDISK_SCRUB, VDS05, VDISKP(LogPrefix, "reporting scrub complete"), (State, State),
            (Success, Success), (UnreadableBlobsEmpty, UnreadableBlobs.empty()));

        auto finish = [&](auto&& result) {
            Send(MakeBlobStorageNodeWardenID(SelfActorId.NodeId()), new TEvBlobStorage::TEvControllerScrubQuantumFinished(
                ScrubCtx->NodeId, ScrubCtx->PDiskId, ScrubCtx->VSlotId, std::move(result)), 0, ScrubCtx->ScrubCookie);
        };

        if (State) {
            TString serialized;
            const bool success = State->SerializeToString(&serialized);
            Y_VERIFY_S(success, LogPrefix);
            finish(serialized);
            ScrubEntrypoint.MutableScrubState()->CopyFrom(*State);
        } else {
            finish(Success && UnreadableBlobs.empty());
            ScrubEntrypoint.ClearScrubState();
        }

        IssueEntrypoint();
    }

    void TScrubCoroImpl::IssueEntrypoint() {
        // prepare entrypoint record
        ScrubEntrypoint.ClearUnreadableBlobs();
        for (const auto& [blobId, state] : UnreadableBlobs) {
            auto *pb = ScrubEntrypoint.AddUnreadableBlobs();
            LogoBlobIDFromLogoBlobID(blobId, pb->MutableBlobId());
            pb->SetUnreadableParts(state.UnreadableParts.Raw());
            state.CorruptedPart.SerializeToProto(*pb->MutableCorruptedPart());
        }

        TRcBuf data(TRcBuf::Uninitialized(ScrubEntrypoint.ByteSizeLong()));
        //FIXME(innokentii): better use SerializeWithCachedSizesToArray + check that all fields are set
        const bool success = ScrubEntrypoint.SerializeToArray(reinterpret_cast<uint8_t*>(data.UnsafeGetDataMut()), data.GetSize());
        Y_VERIFY_S(success, LogPrefix);

        auto seg = ScrubCtx->LsnMngr->AllocLsnForLocalUse();
        ScrubEntrypointLsn = seg.Point();
        NPDisk::TCommitRecord cr;
        cr.IsStartingPoint = true;
        Send(ScrubCtx->LoggerId, new NPDisk::TEvLog(ScrubCtx->PDiskCtx->Dsk->Owner, ScrubCtx->PDiskCtx->Dsk->OwnerRound,
            TLogSignature::SignatureScrub, cr, data, seg, nullptr));
    }

    void TScrubCoroImpl::Handle(NPDisk::TEvLogResult::TPtr ev) {
        if (ev->Get()->Status == NKikimrProto::OK) {
            auto& lastItem = ev->Get()->Results.back();
            Send(ScrubCtx->LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::Scrub, lastItem.Lsn));
        }
    }

    void TScrubCoroImpl::Handle(NPDisk::TEvCutLog::TPtr ev) {
        if (ScrubEntrypointLsn < ev->Get()->FreeUpToLsn) {
            IssueEntrypoint();
        }
    }

    TIntrusivePtr<IContiguousChunk> TScrubCoroImpl::AllocateRopeArenaChunk() {
        return TRopeAlignedBuffer::Allocate(1 << 20); // 1 MB
    }

    void TScrubCoroImpl::CheckIntegrity(const TLogoBlobID& blobId, bool isHuge) {
        SendToBSProxy(SelfActorId, Info->GroupID, new TEvBlobStorage::TEvCheckIntegrity(blobId, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::LowRead, true));
        auto res = WaitForPDiskEvent<TEvBlobStorage::TEvCheckIntegrityResult>();

        TErasureType::EErasureSpecies erasure = Info->Type.GetErasure();
        
        NMonGroup::TDeepScrubbingGroup* counters = DeepScrubbingSubgroups.GetCounters(isHuge, erasure);
        if (counters) {
            ++counters->BlobsChecked();
        }

        if (res->Get()->Status != NKikimrProto::OK) {
            STLOGX(GetActorContext(), PRI_WARN, BS_VDISK_SCRUB, VDS97, VDISKP(LogPrefix, "TEvCheckIntegrity request failed"),
                    (BlobId, blobId), (ErrorReason, res->Get()->ErrorReason));
            if (counters) {
                ++counters->CheckIntegrityErrors();
            }
        } else {
            if (counters) {
                ++counters->CheckIntegritySuccesses();
            }

            switch (res->Get()->PlacementStatus) {
            case TEvBlobStorage::TEvCheckIntegrityResult::PS_UNKNOWN:
            case TEvBlobStorage::TEvCheckIntegrityResult::PS_REPLICATION_IN_PROGRESS:
                if (counters) {
                    ++counters->UnknownPlacementStatus();
                }
                break;
            case TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_LOST:
            case TEvBlobStorage::TEvCheckIntegrityResult::PS_BLOB_IS_RECOVERABLE:
                STLOGX(GetActorContext(), PRI_CRIT, BS_VDISK_SCRUB, VDS98, VDISKP(LogPrefix, "TEvCheckIntegrity discovered placement issue"),
                        (BlobId, blobId), (Erasure, TErasureType::ErasureSpeciesName(erasure)), (CheckIntegrityResult, res->Get()->ToString()));
                if (counters) {
                    ++counters->PlacementIssues();
                }
                break;
            case TEvBlobStorage::TEvCheckIntegrityResult::PS_OK:
            default:
                break; // nothing to do
            }

            switch (res->Get()->DataStatus) {
            case TEvBlobStorage::TEvCheckIntegrityResult::DS_UNKNOWN:
                if (counters) {
                    ++counters->UnknownDataStatus();
                }
                break;
            case TEvBlobStorage::TEvCheckIntegrityResult::DS_ERROR:
                STLOGX(GetActorContext(), PRI_CRIT, BS_VDISK_SCRUB, VDS99, VDISKP(LogPrefix, "TEvCheckIntegrity discovered data issue"),
                        (BlobId, blobId), (Erasure, TErasureType::ErasureSpeciesName(erasure)), (CheckIntegrityResult, res->Get()->ToString()));
                if (counters) {
                    ++counters->DataIssues();
                }
                break;
            case TEvBlobStorage::TEvCheckIntegrityResult::DS_OK:
            default:
                break; // nothing to do
            }
        }
    }

    IActor *CreateScrubActor(TScrubContext::TPtr scrubCtx, NKikimrVDiskData::TScrubEntrypoint scrubEntrypoint,
            ui64 scrubEntrypointLsn) {
        return new TActorCoro(MakeHolder<TScrubCoroImpl>(std::move(scrubCtx), std::move(scrubEntrypoint),
            scrubEntrypointLsn), NKikimrServices::TActivity::BS_SCRUB_ACTOR);
    }

} // NKikimr
