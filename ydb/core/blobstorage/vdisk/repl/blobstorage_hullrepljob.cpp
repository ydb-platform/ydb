#include "defs.h"
#include "blobstorage_repl.h"
#include "blobstorage_replproxy.h"
#include "blobstorage_replrecoverymachine.h"
#include <ydb/core/blobstorage/vdisk/hulldb/bulksst_add/hulldb_bulksst_add.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <ydb/core/blobstorage/vdisk/skeleton/blobstorage_takedbsnap.h>
#include <util/datetime/cputimer.h>

// FIXME: we need a process that asyncronously transfers handoff parts to their correct vdisk
// FIXME: when VDiskProxy reports error, we can get lot of errors during recovery, we want to distinguish them
//        from other errors


namespace NKikimr {

    using namespace NRepl;

    // a vector of proxies we are using to interoperate with other disks; index designates VDisk order number inside the
    // group; for those disks we don't use, the pointer is set to null
    using TVDiskProxySet = TStackVec<TVDiskProxyPtr, 32>;

    struct TEvReplPlanFinished : public TEventLocal<TEvReplPlanFinished, TEvBlobStorage::EvReplPlanFinished> {
        std::unique_ptr<TRecoveryMachine> RecoveryMachine;
        TLogoBlobID LastKey;
        bool Eof;
        std::deque<TLogoBlobID> DroppedBlobs;
        TMilestoneQueue MilestoneQueue;

        TEvReplPlanFinished(std::unique_ptr<TRecoveryMachine>&& recoveryMachine, const TLogoBlobID& lastKey, bool eof,
                std::deque<TLogoBlobID>&& droppedBlobs, TMilestoneQueue&& milestoneQueue)
            : RecoveryMachine(std::move(recoveryMachine))
            , LastKey(lastKey)
            , Eof(eof)
            , DroppedBlobs(std::move(droppedBlobs))
            , MilestoneQueue(std::move(milestoneQueue))
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // THullReplPlannerActor
    ////////////////////////////////////////////////////////////////////////////
    class THullReplPlannerActor : public TActorBootstrapped<THullReplPlannerActor> {
        std::unique_ptr<TRecoveryMachine> RecoveryMachine;
        std::shared_ptr<TReplCtx> ReplCtx;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        TActorId Recipient;
        TLogoBlobID StartKey;
        std::optional<TLogoBlobID> KeyToResumeNextTime;
        TEvReplFinished::TInfoPtr ReplInfo;
        TBlobIdQueuePtr BlobsToReplicatePtr;
        TBlobIdQueuePtr UnreplicatedBlobsPtr;
        std::deque<TLogoBlobID> DroppedBlobs;
        ui64 QuantumBytes = 0;
        bool AddingTasks = true;
        TMilestoneQueue MilestoneQueue;

    public:
        void Bootstrap(const TActorId& parentId) {
            Recipient = parentId;

            // count unreplicated so far blobs in this work too
            ReplInfo->WorkUnitsTotal += UnreplicatedBlobsPtr->GetNumWorkUnits();
            ReplInfo->ItemsTotal += UnreplicatedBlobsPtr->GetNumItems();

            // prepare the recovery machine
            RecoveryMachine = std::make_unique<TRecoveryMachine>(ReplCtx, ReplInfo);

            // request for snapshot
            Send(ReplCtx->SkeletonId, new TEvTakeHullSnapshot(true));

            // switch state func
            Become(&TThis::StateFunc);
        }

        void Handle(TEvTakeHullSnapshotResult::TPtr ev) {
            auto& snap = ev->Get()->Snap;
            const bool allowKeepFlags = snap.HullCtx->AllowKeepFlags;

            // create barriers essence
            auto barriers = snap.BarriersSnap.CreateEssence(snap.HullCtx);

            // create iterator for the logoblobs metabase
            TLogoBlobsSnapshot::TIndexForwardIterator it(snap.HullCtx, &snap.LogoBlobsSnap);
            bool eof = false;
            const ui64 plannedEndTime = GetCycleCountFast() + DurationToCycles(ReplCtx->VDiskCfg->ReplPlanQuantum);
            ui32 counter = 0;

            if (BlobsToReplicatePtr) {
                // iterate over queue items and match them with iterator
                for (; !BlobsToReplicatePtr->IsEmpty() && AddingTasks; BlobsToReplicatePtr->PopFront()) {
                    if (++counter % 1024 == 0 && GetCycleCountFast() >= plannedEndTime) {
                        Send(ReplCtx->SkeletonId, new TEvTakeHullSnapshot(true));
                        return;
                    } else {
                        const TLogoBlobID& key = BlobsToReplicatePtr->Front();
                        it.Seek(key);
                        const bool processed = it.Valid() && it.GetCurKey().LogoBlobID() == key &&
                            ProcessItem(it, *barriers, allowKeepFlags);
                        if (!processed) {
                            DroppedBlobs.push_back(key);
                        }
                    }
                }
                ReplInfo->WorkUnitsTotal += BlobsToReplicatePtr->GetNumWorkUnits();
                ReplInfo->ItemsTotal += BlobsToReplicatePtr->GetNumItems();
                eof = BlobsToReplicatePtr->IsEmpty();
            } else {
                // scan through the index until we have enough blobs to recover or the time is out
                const TBlobStorageGroupInfo::TTopology& topology = *ReplCtx->VCtx->Top;

                if (StartKey == TLogoBlobID()) {
                    it.SeekToFirst();
                } else {
                    it.Seek(StartKey);
                    if (it.Valid() && it.GetCurKey() == StartKey) {
                        it.Next();
                    }
                }

                auto checkRestart = [&] {
                    if (++counter % 1024 == 0 && GetCycleCountFast() >= plannedEndTime) {
                        // we have event processing timer expired, restart processing later with new snapshot starting
                        // with current key
                        Send(ReplCtx->SkeletonId, new TEvTakeHullSnapshot(true));
                        return true;
                    }
                    return false;
                };

                if (AddingTasks) {
                    for (; it.Valid(); it.Next()) {
                        if (checkRestart()) {
                            return;
                        }

                        StartKey = it.GetCurKey().LogoBlobID();

                        // we still have some space in recovery machine logic, so we can add new item
                        ProcessItem(it, *barriers, allowKeepFlags);
                        MilestoneQueue.PopIfNeeded(StartKey);

                        if (!AddingTasks) { // we have finished adding tasks after this key, remember it
                            it.Next();
                            Y_ABORT_UNLESS(!KeyToResumeNextTime);
                            if (it.Valid()) {
                                KeyToResumeNextTime.emplace(it.GetCurKey().LogoBlobID());
                            }
                            break;
                        }
                    }
                    eof = !it.Valid(); // we finish this quantum when there are no more tasks to generate
                }

                for (; it.Valid(); it.Next()) {
                    if (checkRestart()) {
                        return;
                    }

                    StartKey = it.GetCurKey().LogoBlobID();

                    // check the milestone queue, if we have requested blob
                    if (MilestoneQueue.Match(StartKey, &ReplInfo->ItemsTotal, &ReplInfo->WorkUnitsTotal)) {
                        break;
                    }

                    // no space in recovery machine logic, but we still have to count remaining work
                    const TMemRecLogoBlob memRec = it.GetMemRec();
                    const TIngress ingress = memRec.GetIngress();
                    const auto parts = ingress.PartsWeMustHaveLocally(&topology, ReplCtx->VCtx->ShortSelfVDisk,
                        StartKey) - ingress.LocalParts(topology.GType);
                    if (!parts.Empty() && barriers->Keep(StartKey, memRec, {}, allowKeepFlags,
                            true /*allowGarbageCollection*/).KeepData) {
                        ++ReplInfo->ItemsTotal;
                        ReplInfo->WorkUnitsTotal += StartKey.BlobSize();
                        MilestoneQueue.Push(StartKey, StartKey.BlobSize());
                    }
                }

                if (!it.Valid()) {
                    MilestoneQueue.Finish();
                }
            }

            // the planning stage has finished, issue reply to the job actor
            Send(Recipient, new TEvReplPlanFinished(std::move(RecoveryMachine), KeyToResumeNextTime.value_or(TLogoBlobID()),
                eof, std::move(DroppedBlobs), std::move(MilestoneQueue)));

            // finish processing for this actor
            PassAway();
        }

        bool ProcessItem(const TLogoBlobsSnapshot::TIndexForwardIterator& it,
                TBarriersSnapshot::TBarriersEssence& barriers, bool allowKeepFlags) {
            // aliases for convenient access
            const TBlobStorageGroupInfo::TTopology& topology = *ReplCtx->VCtx->Top;
            const TBlobStorageGroupType gtype = topology.GType;
            const TLogoBlobID& key = it.GetCurKey().LogoBlobID();
            const TMemRecLogoBlob &memRec = it.GetMemRec();
            const TIngress &ingress = memRec.GetIngress();
            NMatrix::TVectorType parts = ingress.PartsWeMustHaveLocally(&topology, ReplCtx->VCtx->ShortSelfVDisk,
                key) - ingress.LocalParts(topology.GType);
            if (parts.Empty()) {
                return false; // nothing to recover
            }

            const NGc::TKeepStatus status = barriers.Keep(key, it.GetMemRec(), {}, allowKeepFlags, true /*allowGarbageCollection*/);
            if (!status.KeepData) {
                return false; // no need to recover
            }

            // scan for metadata parts
            for (ui8 i = parts.FirstPosition(); i != parts.GetSize(); i = parts.NextPosition(i)) {
                const TLogoBlobID id(key, i + 1);
                if (!gtype.PartSize(id)) {
                    parts.Clear(i);
                    RecoveryMachine->AddMetadataPart(id);
                }
            }

            const bool phantomLike = !status.KeepByBarrier && ReplInfo->DonorVDiskId == TVDiskID();
            RecoveryMachine->AddTask(key, parts, phantomLike, ingress);

            ++ReplInfo->ItemsPlanned;
            ReplInfo->WorkUnitsPlanned += key.BlobSize();

            ++ReplInfo->ItemsTotal;
            ReplInfo->WorkUnitsTotal += key.BlobSize();

            if (phantomLike) {
                ++ReplCtx->MonGroup.ReplPhantomLikeDiscovered();
                ReplCtx->MonGroup.ReplUnreplicatedPhantoms() = 1;
            } else {
                ReplCtx->MonGroup.ReplUnreplicatedNonPhantoms() = 1;
            }

            // calculate part size and total size to recover
            for (ui8 partIdx = parts.FirstPosition(); partIdx != parts.GetSize(); partIdx = parts.NextPosition(partIdx)) {
                QuantumBytes += gtype.PartSize(TLogoBlobID(key, partIdx + 1));
            }

            if (RecoveryMachine->FullOfTasks() || QuantumBytes >= ReplCtx->VDiskCfg->ReplMaxQuantumBytes) {
                AddingTasks = false;
            }

            return true;
        }

        STRICT_STFUNC(StateFunc,
            hFunc(TEvTakeHullSnapshotResult, Handle);
            cFunc(TEvents::TSystem::Poison, PassAway);
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULL_REPL_PLANNER;
        }

        THullReplPlannerActor(std::shared_ptr<TReplCtx> replCtx,
                TIntrusivePtr<TBlobStorageGroupInfo> ginfo,
                const TLogoBlobID &startKey,
                TEvReplFinished::TInfoPtr replInfo,
                TBlobIdQueuePtr blobsToReplicatePtr,
                TBlobIdQueuePtr unreplicatedBlobsPtr,
                TMilestoneQueue milestoneQueue)
            : ReplCtx(std::move(replCtx))
            , GInfo(std::move(ginfo))
            , StartKey(startKey)
            , ReplInfo(replInfo)
            , BlobsToReplicatePtr(std::move(blobsToReplicatePtr))
            , UnreplicatedBlobsPtr(std::move(unreplicatedBlobsPtr))
            , MilestoneQueue(std::move(milestoneQueue))
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // THullReplJobActor
    ////////////////////////////////////////////////////////////////////////////
    class THullReplJobActor : public TActorBootstrapped<THullReplJobActor> {
    private:
        struct TLogoBlobInfo {
            TLogoBlobID Id;
            TIngress Ingress;
        };

        enum class EProcessQueueAction {
            Continue,
            Restart,
            Exit
        };

        std::shared_ptr<TReplCtx> ReplCtx;
        TIntrusivePtr<TBlobStorageGroupInfo> GInfo;
        const TActorId ParentId;
        const TLogoBlobID StartKey;
        TVector<TVDiskProxyPtr> MergeHeap;
        TEvReplFinished::TInfoPtr ReplInfo;
        TRecoveryMachine::TRecoveredBlobsQueue RecoveryQueue;
        TReplSstStreamWriter Writer;
        bool RecoveryMachineFinished, WriterFinished;
        TTimeAccount TimeAccount;
        TActiveActors ActiveActors;

        // huge blob replication part
        ui32 HugeBlobsInFlight;
        const ui32 HugeBlobsInFlightMax;

        TQueueActorMapPtr QueueActorMapPtr;
        TBlobIdQueuePtr BlobsToReplicatePtr;
        TBlobIdQueuePtr UnreplicatedBlobsPtr;
        TUnreplicatedBlobRecords UnreplicatedBlobRecords;
        TMilestoneQueue MilestoneQueue;
        std::optional<std::pair<TVDiskID, TActorId>> Donor;

        // parameters from planner
        std::unique_ptr<TRecoveryMachine> RecoveryMachine;
        TLogoBlobID LastKey;
        bool Eof = false;
        TVDiskProxySet DiskProxySet;
        ui32 NumRunningProxies = 0;

        using TPhantomCheck = std::tuple<TLogoBlobID, NMatrix::TVectorType, TRecoveryMachine::TPartSet, TIngress>;
        std::deque<TPhantomCheck> PhantomChecksPending;
        std::unordered_multimap<ui64, TPhantomCheck> PhantomChecksInFlight;
        ui32 LastPhantomCheckId = 0;
        TDeque<TLogoBlobID> Phantoms;

        THashSet<TChunkIdx> WrittenChunkIdxSet;

        friend class TActorBootstrapped<THullReplJobActor>;

        std::optional<TRecoveryMachine::TPartSet> CurrentItem;
        TLogoBlobID LastProcessedKey;

        void Finish() {
            STLOG(PRI_DEBUG, BS_REPL, BSVR01, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "finished replication job"),
                (LastKey, LastKey), (Eof, Eof));

            if (Phantoms.empty()) {
                HandleDetectedPhantomBlobCommitted();
            } else {
                STLOG(PRI_DEBUG, BS_REPL, BSVR06, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "sending phantoms"),
                    (NumPhantoms, Phantoms.size()));
                Send(ReplCtx->SkeletonId, new TEvDetectedPhantomBlob(std::exchange(Phantoms, {})));
            }
        }

        void HandleDetectedPhantomBlobCommitted() {
            bool dropDonor = true;
            for (const auto& proxy : DiskProxySet) {
                dropDonor = dropDonor && proxy && proxy->NoTransientErrors();
            }
            ReplInfo->Finish(LastKey, Eof, Donor && dropDonor, std::move(UnreplicatedBlobRecords), std::move(MilestoneQueue));

            TProxyStat stat;
            for (const TVDiskProxyPtr& p : DiskProxySet) {
                if (p) {
                    stat += p->Stat;
                }
            }
            ReplInfo->ProxyStat = std::make_unique<TProxyStat>(stat);

            TimeAccount.SetState(ETimeState::COUNT);
            TimeAccount.UpdateInfo(*ReplInfo);

            Send(ParentId, new TEvReplFinished(ReplInfo));
            PassAway();
        }

        void Bootstrap() {
            STLOG(PRI_DEBUG, BS_REPL, BSVR02, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "THullReplJobActor::Bootstrap"));

            TimeAccount.SetState(ETimeState::PREPARE_PLAN);
            auto actor = std::make_unique<THullReplPlannerActor>(ReplCtx, GInfo, StartKey, ReplInfo, BlobsToReplicatePtr,
                UnreplicatedBlobsPtr, std::move(MilestoneQueue));
            auto aid = RunInBatchPool(ActorContext(), actor.release());
            ActiveActors.Insert(aid, __FILE__, __LINE__, TActivationContext::AsActorContext(), NKikimrServices::BLOBSTORAGE);
            Become(&TThis::StatePreparePlan);
        }

        void Handle(TEvReplPlanFinished::TPtr& ev) {
            STLOG(PRI_DEBUG, BS_REPL, BSVR03, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "THullReplJobActor::Handle(TEvReplPlanFinished)"));
            ActiveActors.Erase(ev->Sender);
            RecoveryMachine = std::move(ev->Get()->RecoveryMachine);
            LastKey = ev->Get()->LastKey;
            Eof = ev->Get()->Eof;
            MilestoneQueue = std::move(ev->Get()->MilestoneQueue);

            for (const TLogoBlobID& id : ev->Get()->DroppedBlobs) {
                DropUnreplicatedBlobRecord(id);
            }

            auto& mon = ReplCtx->MonGroup;

            if ((mon.ReplWorkUnitsRemaining() && ReplInfo->WorkUnitsTotal > (ui64)mon.ReplWorkUnitsRemaining()) ||
                    (mon.ReplItemsRemaining() && ReplInfo->ItemsTotal > (ui64)mon.ReplItemsRemaining())) {
                STLOG(PRI_WARN, BS_REPL, BSVR36, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "replication work added"),
                    (WorkUnitsTotal, ReplInfo->WorkUnitsTotal),
                    (ReplWorkUnitsRemaining, (ui64)mon.ReplWorkUnitsRemaining()),
                    (ItemsTotal, ReplInfo->ItemsTotal),
                    (ReplItemsRemaining, (ui64)mon.ReplItemsRemaining()),
                    (LastKey, LastKey),
                    (Eof, Eof),
                    (BlobsToReplicatePtr.size, ssize_t(BlobsToReplicatePtr ? BlobsToReplicatePtr->GetNumItems() : (ssize_t)-1)),
                    (UnreplicatedBlobsPtr.size, UnreplicatedBlobsPtr->GetNumItems()));
            }

            mon.ReplWorkUnitsRemaining() = ReplInfo->WorkUnitsTotal;
            mon.ReplItemsRemaining() = ReplInfo->ItemsTotal;

            if (RecoveryMachine->NoTasks()) {
                Finish();
                return;
            }

            // we will receive TEvReplResume from the Recipient a bit later
            Send(ParentId, new TEvReplStarted);
            TimeAccount.SetState(ETimeState::TOKEN_WAIT);
            Become(&TThis::StateToken);
        }

        void HandleResume() {
            STLOG(PRI_DEBUG, BS_REPL, BSVR04, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "THullReplJobActor::HandleResume"));
            TimeAccount.SetState(ETimeState::PROXY_WAIT);

            // run proxies
            SetupDiskProxies();
            Y_ABORT_UNLESS(!NumRunningProxies);
            for (const TVDiskProxyPtr& p : DiskProxySet) {
                if (p) {
                    ActiveActors.Insert(p->Run(SelfId()), __FILE__, __LINE__, TActivationContext::AsActorContext(),
                        NKikimrServices::BLOBSTORAGE);
                    ++NumRunningProxies;
                }
            }
            if (NumRunningProxies) {
                Become(&TThis::StateInit);
            } else {
                Become(&TThis::StateMerge);
                Merge();
            }
        }

        void SetupDiskProxies() {
            DiskProxySet.clear();
            DiskProxySet.resize(Donor ? 1 : ReplCtx->VCtx->Top->GetTotalVDisksNum());
            if (Donor) {
                RecoveryMachine->ClearPossiblePhantom(); // no phantoms in donor mode
            }

            const TBlobStorageGroupInfo::TTopology& topology = *ReplCtx->VCtx->Top;
            const TBlobStorageGroupType gtype = topology.GType;

            if (Donor) {
                TVDiskProxyPtr& proxy = DiskProxySet[0];
                RecoveryMachine->ForEach([&](const TLogoBlobID& fullId, NMatrix::TVectorType parts, TIngress /*ingress*/) {
                    if (!proxy) {
                        proxy = MakeIntrusive<TVDiskProxy>(ReplCtx, Donor->first, Donor->second);
                    }
                    for (ui8 i = parts.FirstPosition(); i != parts.GetSize(); i = parts.NextPosition(i)) {
                        const TLogoBlobID id(fullId, i + 1);
                        proxy->Put(id, gtype.PartSize(id));
                    }
                });
            } else {
                RecoveryMachine->ForEach([&](const TLogoBlobID& fullId, NMatrix::TVectorType /*parts*/, TIngress ingress) {
                    // calculate subgroup layout for this blob
                    TBlobStorageGroupInfo::TOrderNums vdiskOrderNums;
                    topology.PickSubgroup(fullId.Hash(), vdiskOrderNums);

                    // traverse through all of the disks and create proxies
                    for (ui32 idx = 0; idx < vdiskOrderNums.size(); ++idx) {
                        const ui32 orderNum = vdiskOrderNums[idx];
                        const TVDiskID& vdisk = GInfo->GetVDiskId(orderNum);
                        if (TVDiskIdShort(vdisk) == ReplCtx->VCtx->ShortSelfVDisk) {
                            continue;
                        }

                        TVDiskProxyPtr &ptr = DiskProxySet.at(orderNum);
                        if (!ptr) {
                            auto queueIt = QueueActorMapPtr->find(vdisk);
                            Y_ABORT_UNLESS(queueIt != QueueActorMapPtr->end());
                            ptr = MakeIntrusive<TVDiskProxy>(ReplCtx, vdisk, queueIt->second);
                        }

                        // count number of known parts on this disk according to ingress
                        const NMatrix::TVectorType partsOnDisk = ingress.KnownParts(gtype, idx);
                        ui32 expectedReplySize = 0;
                        for (ui8 i = partsOnDisk.FirstPosition(); i != partsOnDisk.GetSize(); i = partsOnDisk.NextPosition(i)) {
                            expectedReplySize += gtype.PartSize(TLogoBlobID(fullId, i + 1));
                        }

                        ptr->Put(fullId, expectedReplySize);
                    }
                });
            }

        }

        void Merge() {
            while (MergeIteration())
                ;
        }

        bool MergeIteration() {
            for (;;) {
                const TReplSstStreamWriter::EState state = Writer.GetState();
                const bool noWorkForWriter = RecoveryQueue.empty() || RecoveryQueue.front().IsHugeBlob;
                if (state == TReplSstStreamWriter::EState::COLLECT) {
                    Y_ABORT_UNLESS(!WriterFinished);
                    break;
                } else if (state == TReplSstStreamWriter::EState::STOPPED && noWorkForWriter) {
                    break;
                }

                switch (state) {
                    case TReplSstStreamWriter::EState::STOPPED:
                        Y_ABORT_UNLESS(RecoveryQueue && !RecoveryQueue.front().IsHugeBlob && !WriterFinished);
                        Writer.Begin();
                        break;

                    case TReplSstStreamWriter::EState::PDISK_MESSAGE_PENDING: {
                        // obtain pending message
                        std::unique_ptr<IEventBase> msg = Writer.GetPendingPDiskMsg();

                        // if this is chunk write, then check if we are writing new chunk; if so, count it
                        if (msg->Type() == TEvBlobStorage::EvChunkWrite) {
                            auto *write = static_cast<NPDisk::TEvChunkWrite*>(msg.get());
                            // if we have seen new chunk index, then increase some counters
                            if (WrittenChunkIdxSet.insert(write->ChunkIdx).second) {
                                ++ReplInfo->ChunksWritten;
                                ++ReplCtx->MonGroup.ReplChunksWritten();
                            }
                            const ui64 bytes = write->PartsPtr ? write->PartsPtr->ByteSize() : 0;
                            ReplInfo->SstBytesWritten += bytes;
                            // and check if we have to postpone it
                            TReplQuoter::QuoteMessage(ReplCtx->VCtx->ReplPDiskWriteQuoter, std::make_unique<IEventHandle>(
                                ReplCtx->PDiskCtx->PDiskId, SelfId(), msg.release()), bytes);
                        } else {
                            Send(ReplCtx->PDiskCtx->PDiskId, msg.release());
                        }
                        break;
                    }

                    case TReplSstStreamWriter::EState::NOT_READY:
                        TimeAccount.SetState(ETimeState::PDISK_OP);
                        return false; // we can't proceed right now

                    case TReplSstStreamWriter::EState::COLLECT:
                        Y_ABORT(); // should have exited a few lines above

                    case TReplSstStreamWriter::EState::COMMIT_PENDING: {
                        // acquire commit message from writer and send to the level index actor, writer state will
                        // automatically switch to WAITING_FOR_COMMIT after this stage
                        auto msg = Writer.GetPendingCommitMsg();
                        msg->NotifyId = SelfId(); // receive notification after commit
                        TimeAccount.SetState(ETimeState::COMMIT);
                        Send(ReplCtx->HullDs->LogoBlobs->LIActor, msg.release());
                        return false; // no further processing now
                    }

                    case TReplSstStreamWriter::EState::WAITING_FOR_COMMIT:
                        return false; // just waiting for something to happen

                    case TReplSstStreamWriter::EState::ERROR:
                        Y_ABORT("replication failed"); // FIXME: do something sane

                    default:
                        Y_ABORT("unexpected state");
                }
            }

            // preprocess existing items, if any
            switch (ProcessQueue()) {
                case EProcessQueueAction::Continue:
                    break;
                case EProcessQueueAction::Restart:
                    return true;
                case EProcessQueueAction::Exit:
                    return false;
            }

            // merge queue is not empty, but we are waiting for some events from proxies to come
            Y_DEBUG_ABORT_UNLESS(MergeHeap.size() <= NumRunningProxies);
            if (MergeHeap.size() != NumRunningProxies) {
                return false;
            }

            { Y_DEFER {
                RunPhantomChecks();
            };

            while (!MergeHeap.empty()) {
                TimeAccount.SetState(ETimeState::MERGE);

                // acquire current key; front item contains the least key
                if (!CurrentItem) {
                    const TLogoBlobID id = MergeHeap.front()->GenLogoBlobId();
                    CurrentItem.emplace(id, ReplCtx->VCtx->Top->GType);
                    Y_ABORT_UNLESS(std::exchange(LastProcessedKey, id) < id);
                }
                auto& item = *CurrentItem;

                // find out which proxies carry items with the same key
                TVector<TVDiskProxyPtr>::iterator lastIter = MergeHeap.end();
                while (lastIter != MergeHeap.begin() && MergeHeap.front()->GenLogoBlobId() == item.Id) {
                    PopHeap(MergeHeap.begin(), lastIter, TVDiskProxy::TPtrGreater());
                    --lastIter;
                }

                // now proxies in range [ lastIter, MergeHeap.end() ) have the same current key; some of them may
                // contain runs of items with this key, so we should check it also; process those proxies and put
                // data to merger
                while (lastIter != MergeHeap.end()) {
                    // process all items with specified current key
                    TVDiskProxyPtr proxy = *lastIter;
                    while (proxy->Valid() && proxy->GenLogoBlobId() == item.Id) {
                        TLogoBlobID id;
                        NKikimrProto::EReplyStatus status;
                        TRope data;
                        proxy->FetchData(&id, &status, &data);
                        if (status != NKikimrProto::OK || data) {
                            item.AddData(ReplCtx->VCtx->Top->GetIdxInSubgroup(proxy->VDiskId, id.Hash()), id, status,
                                std::move(data));
                        }
                    }
                    Y_DEBUG_ABORT_UNLESS(!proxy->Valid() || item.Id < proxy->GenLogoBlobId());

                    // if proxy is not exhausted yet, then put it back into merge queue
                    if (proxy->Valid()) {
                        PushHeap(MergeHeap.begin(), ++lastIter, TVDiskProxy::TPtrGreater());
                    } else {
                        // there's no more data in proxy, we don't put it back to merger; moreover we remove this
                        // proxy from merger queue and check if it is in EOF state or just needs some more requests
                        // to VDisk
                        DoSwap(*lastIter, MergeHeap.back());
                        MergeHeap.pop_back();
                        if (proxy->IsEof()) {
                            // count this proxy as finished one
                            STLOG(PRI_DEBUG, BS_REPL, BSVR05, VDISKP(ReplCtx->VCtx->VDiskLogPrefix,
                                    "proxy finished"), (VDiskId, proxy->VDiskId));
                            --NumRunningProxies;
                        } else {
                            // put this proxy on wait queue
                            proxy->SendNextRequest();
                        }
                    }
                }

                // if we're waiting for proxy data to arrive, then exit main cycle
                if (MergeHeap.size() != NumRunningProxies) {
                    TimeAccount.SetState(ETimeState::PROXY_WAIT);
                    return false;
                }

                // recover data
                RecoveryMachine->Recover(item, RecoveryQueue, *this);
                CurrentItem.reset();

                // process recovered items, if any; queueProcessed.first will be false when writer is not ready for new data
                EProcessQueueAction action = ProcessQueue();

                // if merger state has changed, then restart merge cycle; maybe writer wants to put some chunks to disks or make a commit
                if (action != EProcessQueueAction::Continue) {
                    TimeAccount.SetState(ETimeState::OTHER);
                    switch (action) {
                        case EProcessQueueAction::Restart:
                            return true;
                        case EProcessQueueAction::Exit:
                            return false;
                        default:
                            Y_ABORT("invalid EProcessQueueAction");
                    }
                }
            }

            } // Y_DEFER

            if (!PhantomChecksInFlight.empty()) {
                TimeAccount.SetState(ETimeState::PHANTOM);
                return false; // still waiting for proxy response about phantom validation
            }
            Y_ABORT_UNLESS(PhantomChecksPending.empty());

            Y_ABORT_UNLESS(!NumRunningProxies && MergeHeap.empty() && RecoveryQueue.empty());
            TimeAccount.SetState(ETimeState::OTHER);

            if (!RecoveryMachineFinished) {
                RecoveryMachine->Finish(RecoveryQueue, *this);
                RecoveryMachineFinished = true;
                STLOG(PRI_DEBUG, BS_REPL, BSVR07, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "finished recovery machine"),
                    (RecoveryQueueSize, RecoveryQueue.size()));

                // sort unreplicated blobs vector as it may contain records in incorrect order due to phantom checking
                UnreplicatedBlobsPtr->Sort();
                return true;
            }

            if (!WriterFinished && Writer.GetState() != TReplSstStreamWriter::EState::STOPPED) {
                STLOG(PRI_DEBUG, BS_REPL, BSVR08, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "finished writer"));
                Writer.Finish();
                WriterFinished = true;
                return true;
            }

            if (HugeBlobsInFlight != 0) {
                // do not finish until all in-flight requests are completed
                STLOG(PRI_DEBUG, BS_REPL, BSVR09, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "huge blobs unwritten"),
                    (HugeBlobsInFlight, HugeBlobsInFlight));
                return false;
            }

            if (Writer.GetState() == TReplSstStreamWriter::EState::STOPPED) {
                Y_ABORT_UNLESS(RecoveryQueue.empty());
                Finish();
                return false;
            }

            Y_ABORT("incorrect merger state State# %" PRIu32, ui32(Writer.GetState()));
        }

        void RunPhantomChecks() {
            while (!PhantomChecksPending.empty() && PhantomChecksInFlight.size() < 32) {
                const ui64 cookie = ++LastPhantomCheckId;

                size_t numItems = 0;
                const ui64 tabletId = std::get<0>(PhantomChecksPending.front()).TabletID();
                for (auto it = PhantomChecksPending.begin(); it != PhantomChecksPending.end() && numItems < 32 &&
                    std::get<0>(*it).TabletID() == tabletId; ++it, ++numItems) {}

                TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queries(new TEvBlobStorage::TEvGet::TQuery[numItems]);
                for (size_t i = 0; i < numItems; ++i) {
                    auto& pending = PhantomChecksPending.front();
                    auto& [id, parts, partSet, ingress] = pending;
                    queries[i].Set(id);
                    PhantomChecksInFlight.emplace(cookie, pending);
                    PhantomChecksPending.pop_front();
                }

                auto ev = std::make_unique<TEvBlobStorage::TEvGet>(queries, numItems, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::AsyncRead);
                ev->PhantomCheck = true;
                SendToBSProxy(SelfId(), GInfo->GroupID, ev.release(), cookie);
            }
        }

        void Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
            STLOG(PRI_INFO, BS_REPL, BSVR34, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "Received phantom validation reply"),
                (Msg, ev->Get()->ToString()));

            auto [begin, end] = PhantomChecksInFlight.equal_range(ev->Cookie);
            Y_ABORT_UNLESS(begin != end);

            std::unordered_map<TLogoBlobID, std::tuple<bool, bool>> isPhantom;
            auto *msg = ev->Get();
            for (size_t i = 0; i < msg->ResponseSz; ++i) {
                auto& r = msg->Responses[i];
                auto looksLikePhantom = r.LooksLikePhantom;
                bool isNoData = r.Status == NKikimrProto::NODATA;
                if (!looksLikePhantom) {
                    Y_VERIFY_DEBUG_S(r.Status == NKikimrProto::ERROR, "LooksLikePhantom undefined r.Status# " << r.Status
                        << " msg.Status# " << msg->Status);
                    looksLikePhantom = isNoData = false;
                }
                isPhantom.try_emplace(r.Id, isNoData, *looksLikePhantom);
            }

            for (auto it = begin; it != end; ++it) {
                const auto& [_, item] = *it;
                const auto& [id, parts, partSet, ingress] = item;
                auto node = isPhantom.extract(id);
                Y_ABORT_UNLESS(node);
                auto [phantom, looksLikePhantom] = node.mapped();
                RecoveryMachine->ProcessPhantomBlob(partSet, parts, phantom, looksLikePhantom, ingress, *this);
                if (phantom) {
                    Phantoms.push_back(id);
                }
            }

            PhantomChecksInFlight.erase(begin, end);
            Y_ABORT_UNLESS(isPhantom.empty());

            Merge();
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // TRecoveryMachine interoperation

        friend class TRecoveryMachine;

        void AddUnreplicatedBlobRecord(const TRecoveryMachine::TPartSet& item, TIngress ingress, bool looksLikePhantom) {
            TUnreplicatedBlobRecord record{ingress, item.PartsMask, item.DisksRepliedOK, item.DisksRepliedNODATA,
                item.DisksRepliedNOT_YET, item.DisksRepliedOther, looksLikePhantom};
            const auto [it, inserted] = UnreplicatedBlobRecords.try_emplace(item.Id, record);
            ReplCtx->MonGroup.ReplTotalBlobsWithProblems() += inserted;
            if (!inserted) {
                if (it->second.LooksLikePhantom < record.LooksLikePhantom) {
                    ++ReplCtx->MonGroup.ReplPhantomBlobsWithProblems();
                } else if (record.LooksLikePhantom < it->second.LooksLikePhantom) {
                    --ReplCtx->MonGroup.ReplPhantomBlobsWithProblems();
                }
                it->second = record;
            } else if (record.LooksLikePhantom) {
                ++ReplCtx->MonGroup.ReplPhantomBlobsWithProblems();
            }
            UnreplicatedBlobsPtr->Push(item.Id);
        }

        void DropUnreplicatedBlobRecord(const TLogoBlobID& id) {
            if (const auto it = UnreplicatedBlobRecords.find(id); it != UnreplicatedBlobRecords.end()) {
                --ReplCtx->MonGroup.ReplTotalBlobsWithProblems();
                if (it->second.LooksLikePhantom) {
                    --ReplCtx->MonGroup.ReplPhantomBlobsWithProblems();
                }
                UnreplicatedBlobRecords.erase(it);
            }
        }

        void AddPhantomBlobRecord(const TRecoveryMachine::TPartSet& item, TIngress ingress, NMatrix::TVectorType partsToRecover) {
            STLOG(PRI_INFO, BS_REPL, BSVR33, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "Sending phantom validation query"),
                (GroupId, GInfo->GroupID), (CurKey, item.Id));
            PhantomChecksPending.emplace_back(item.Id, partsToRecover, item, ingress);
        }

        EProcessQueueAction ProcessQueue() {
            while (!RecoveryQueue.empty()) {
                auto& front = RecoveryQueue.front();

                // special handling of hugeblobs through Skeleton
                if (front.IsHugeBlob) {
                    if (HugeBlobsInFlight == HugeBlobsInFlightMax) {
                        // we are already at in flight limit, do not accept more messages
                        return EProcessQueueAction::Exit;
                    }
                    Y_ABORT_UNLESS(HugeBlobsInFlight < HugeBlobsInFlightMax);
                    ++HugeBlobsInFlight;

                    ++ReplCtx->MonGroup.ReplHugeBlobsRecovered();
                    ReplCtx->MonGroup.ReplHugeBlobBytesRecovered() += front.Data.GetSize();

                    const ui64 bytes = front.Data.GetSize();
                    TReplQuoter::QuoteMessage(ReplCtx->VCtx->ReplPDiskWriteQuoter, std::make_unique<IEventHandle>(
                        ReplCtx->SkeletonId, SelfId(), new TEvRecoveredHugeBlob(front.Id, std::move(front.Data))),
                        bytes);

                    RecoveryQueue.pop();
                    continue;
                }

                switch (Writer.GetState()) {
                    case TReplSstStreamWriter::EState::STOPPED:
                        return EProcessQueueAction::Restart;
                    case TReplSstStreamWriter::EState::COLLECT:
                        break;
                    default:
                        Y_ABORT("unexpected State# %" PRIu32, static_cast<ui32>(Writer.GetState()));
                }

                if (Writer.AddRecoveredBlob(front)) {
                    ++ReplCtx->MonGroup.ReplBlobsRecovered();
                    ReplCtx->MonGroup.ReplBlobBytesRecovered() += front.Data.GetSize();
                    RecoveryQueue.pop();
                }

                // restart cycle if we have output data pending or something has changed
                if (Writer.GetState() != TReplSstStreamWriter::EState::COLLECT) {
                    return EProcessQueueAction::Restart;
                }
            }

            return EProcessQueueAction::Continue;
        }

        void HandleYard(NPDisk::TEvChunkWriteResult::TPtr& ev) {
            CHECK_PDISK_RESPONSE(ReplCtx->VCtx, ev, ActorContext());
            Writer.Apply(ev->Get());
            Merge();
        }

        void HandleYard(NPDisk::TEvChunkReserveResult::TPtr& ev) {
            CHECK_PDISK_RESPONSE(ReplCtx->VCtx, ev, ActorContext());
            STLOG(PRI_INFO, BS_REPL, BSVR10, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "reserved chunks"),
                (ChunkIds, FormatList(ev->Get()->ChunkIds)));
            Writer.Apply(ev->Get());
            Merge();
        }

        void Handle(TEvReplProxyNextResult::TPtr &ev) {
            STLOG(PRI_DEBUG, BS_REPL, BSVR11, VDISKP(ReplCtx->VCtx->VDiskLogPrefix,
                "THullReplJobActor::Handle(TEvReplProxyNextResult)"));
            TEvReplProxyNextResult *msg = ev->Get();
            TIntrusivePtr<TVDiskProxy> proxy = DiskProxySet.at(Donor ? 0 : ReplCtx->VCtx->Top->GetOrderNumber(msg->VDiskId));
            proxy->HandleNext(ev);

            if (proxy->IsEof()) {
                STLOG(PRI_DEBUG, BS_REPL, BSVR12, VDISKP(ReplCtx->VCtx->VDiskLogPrefix, "proxy finished"),
                    (VDiskId, msg->VDiskId.ToString()));
                --NumRunningProxies;
            } else {
                Y_ABORT_UNLESS(proxy->Valid());
                MergeHeap.push_back(proxy);
                PushHeap(MergeHeap.begin(), MergeHeap.end(), TVDiskProxy::TPtrGreater());
            }

            STLOG(PRI_DEBUG, BS_REPL, BSVR13, VDISKP(ReplCtx->VCtx->VDiskLogPrefix,
                "THullReplJobActor::Handle(TEvReplProxyNextResult)"), (MergeHeapSize, MergeHeap.size()),
                (NumRunningProxies, NumRunningProxies));

            if (MergeHeap.size() == NumRunningProxies) {
                Become(&TThis::StateMerge);
                Merge();
            }
        }

        void Handle(TEvAddBulkSstResult::TPtr& ev) {
            Y_UNUSED(ev);
            Writer.ApplyCommit();
            Merge();
        }

        void Handle(TEvBlobStorage::TEvVPutResult::TPtr& /*ev*/) {
            // FIXME: Handle NotOK
            // this message is received when huge blob is written by Skeleton
            Y_ABORT_UNLESS(HugeBlobsInFlight != 0);
            --HugeBlobsInFlight;
            Merge();
        }

        void PassAway() override {
            ActiveActors.KillAndClear(ActorContext());
            TActorBootstrapped::PassAway();
        }

        void Handle(TEvReplInvoke::TPtr ev) {
            ev->Get()->Callback(UnreplicatedBlobRecords, {});
        }

        STRICT_STFUNC(StatePreparePlan,
            hFunc(TEvReplPlanFinished, Handle)
            cFunc(TEvents::TSystem::Poison, PassAway)
            hFunc(TEvReplInvoke, Handle)
        )

        STRICT_STFUNC(StateMerge,
            hFunc(TEvReplProxyNextResult, Handle)

            // yard messages coming to Writer
            hFunc(NPDisk::TEvChunkWriteResult, HandleYard)
            hFunc(NPDisk::TEvChunkReserveResult, HandleYard)
            hFunc(TEvBlobStorage::TEvGetResult, Handle)
            hFunc(TEvAddBulkSstResult, Handle)
            hFunc(TEvBlobStorage::TEvVPutResult, Handle)
            cFunc(TEvBlobStorage::EvDetectedPhantomBlobCommitted, HandleDetectedPhantomBlobCommitted)
            cFunc(TEvents::TSystem::Poison, PassAway)
            hFunc(TEvReplInvoke, Handle)
        )

        STRICT_STFUNC(StateInit,
            hFunc(TEvReplProxyNextResult, Handle)
            cFunc(TEvents::TSystem::Poison, PassAway)
            hFunc(TEvReplInvoke, Handle)
        )

        STRICT_STFUNC(StateToken,
            cFunc(TEvBlobStorage::EvReplResume, HandleResume)
            cFunc(TEvents::TSystem::Poison, PassAway)
            hFunc(TEvReplInvoke, Handle)
        )

        STATEFN(TerminateStateFunc) {
            switch (ev->GetTypeRewrite()) {
                cFunc(TEvents::TSystem::Poison, PassAway)
                hFunc(TEvReplInvoke, Handle)
            }
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULL_REPL_JOB;
        }

        THullReplJobActor(
                std::shared_ptr<TReplCtx> replCtx,
                const TActorId &parentId,
                const TLogoBlobID &startKey,
                TQueueActorMapPtr&& queueActorMapPtr,
                TBlobIdQueuePtr&& blobsToReplicatePtr,
                TBlobIdQueuePtr&& unreplicatedBlobsPtr,
                const std::optional<std::pair<TVDiskID, TActorId>>& donor,
                TUnreplicatedBlobRecords&& ubr,
                TMilestoneQueue&& milestoneQueue)
            : TActorBootstrapped<THullReplJobActor>()
            , ReplCtx(std::move(replCtx))
            , GInfo(ReplCtx->GInfo) // it is safe to take it here
            , ParentId(parentId)
            , StartKey(startKey)
            , ReplInfo(new TEvReplFinished::TInfo())
            , Writer(ReplCtx, ReplCtx->HullDs)
            , RecoveryMachineFinished(false)
            , WriterFinished(false)
            , HugeBlobsInFlight(0)
            , HugeBlobsInFlightMax(3)
            , QueueActorMapPtr(std::move(queueActorMapPtr))
            , BlobsToReplicatePtr(std::move(blobsToReplicatePtr))
            , UnreplicatedBlobsPtr(std::move(unreplicatedBlobsPtr))
            , UnreplicatedBlobRecords(std::move(ubr))
            , MilestoneQueue(std::move(milestoneQueue))
            , Donor(donor)
        {
            if (Donor) {
                ReplInfo->DonorVDiskId = Donor->first;
            }
            ui64 phantomLike = 0;
            for (const auto& [id, item] : UnreplicatedBlobRecords) {
                phantomLike += item.LooksLikePhantom;
            }
            auto& mg = ReplCtx->MonGroup;
            Y_DEBUG_ABORT_UNLESS(mg.ReplTotalBlobsWithProblems() == (i64)UnreplicatedBlobRecords.size(),
                "%" PRIi64 " != %zu", (i64)mg.ReplTotalBlobsWithProblems(), UnreplicatedBlobRecords.size());
           Y_DEBUG_ABORT_UNLESS(mg.ReplPhantomBlobsWithProblems() == (i64)phantomLike,
                "%" PRIi64 " != %" PRIu64, (i64)mg.ReplPhantomBlobsWithProblems(), phantomLike);
            mg.ReplTotalBlobsWithProblems() = UnreplicatedBlobRecords.size();
            mg.ReplPhantomBlobsWithProblems() = phantomLike;
        }
    };


    ////////////////////////////////////////////////////////////////////////////
    // CreateReplJobActor
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateReplJobActor(
            std::shared_ptr<TReplCtx> replCtx,
            const TActorId &parentId,
            const TLogoBlobID &startKey,
            TQueueActorMapPtr queueActorMapPtr,
            TBlobIdQueuePtr blobsToReplicatePtr,
            TBlobIdQueuePtr unreplicatedBlobsPtr,
            const std::optional<std::pair<TVDiskID, TActorId>>& donor,
            TUnreplicatedBlobRecords&& ubr,
            TMilestoneQueue&& milestoneQueue)
    {
        return new THullReplJobActor(std::move(replCtx), parentId, startKey, std::move(queueActorMapPtr),
            std::move(blobsToReplicatePtr), std::move(unreplicatedBlobsPtr), donor, std::move(ubr),
            std::move(milestoneQueue));
    }

} // NKikimr

