#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_private_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_public_events.h>
#include <util/random/random.h>
#include <deque>
#include <utility>

Y_UNIT_TEST_SUITE(BlobStorageSync) {

    void TestCutting(TBlobStorageGroupType groupType) {
        const ui32 groupSize = groupType.BlobSubgroupSize();

        // for (ui32 mask = 0; mask < (1 << groupSize); ++mask) {  // TIMEOUT
        {
            ui32 mask = RandomNumber(1ull << groupSize);
            for (bool compressChunks : { true, false }) {
                TEnvironmentSetup env{{
                    .NodeCount = groupSize,
                    .Erasure = groupType,
                }};

                env.CreateBoxAndPool(1, 1);
                std::vector<ui32> groups = env.GetGroups();
                UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
                ui32 groupId = groups[0];

                const ui64 tabletId = 5000;
                const ui32 channel = 10;
                ui32 gen = 1;
                ui32 step = 1;
                ui64 cookie = 1;

                ui64 totalSize = 0;

                std::vector<TControlWrapper> cutLocalSyncLogControls;
                std::vector<TControlWrapper> compressChunksControls;
                std::vector<TActorId> edges;

                for (ui32 nodeId = 1; nodeId <= groupSize; ++nodeId) {
                    cutLocalSyncLogControls.emplace_back(0, 0, 1);
                    compressChunksControls.emplace_back(1, 0, 1);
                    TAppData* appData = env.Runtime->GetNode(nodeId)->AppData.get();
                    appData->Icb->RegisterSharedControl(cutLocalSyncLogControls.back(), "VDiskControls.EnableLocalSyncLogDataCutting");
                    appData->Icb->RegisterSharedControl(compressChunksControls.back(), "VDiskControls.EnableSyncLogChunkCompressionHDD");
                    edges.push_back(env.Runtime->AllocateEdgeActor(nodeId));
                }

                for (ui32 i = 0; i < groupSize; ++i) {
                    env.Runtime->WrapInActorContext(edges[i], [&] {
                        SendToBSProxy(edges[i], groupId, new TEvBlobStorage::TEvStatus(TInstant::Max()));
                    });
                    auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvStatusResult>(edges[i], false);
                    UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
                }

                auto writeBlob = [&](ui32 nodeId, ui32 blobSize) {
                    TLogoBlobID blobId(tabletId, gen, step, channel, blobSize, ++cookie);
                    totalSize += blobSize;
                    TString data = MakeData(blobSize);
                    
                    const TActorId& sender = edges[nodeId - 1];
                    env.Runtime->WrapInActorContext(sender, [&] () {
                        SendToBSProxy(sender, groupId, new TEvBlobStorage::TEvPut(blobId, std::move(data), TInstant::Max()));
                    });
                };

                env.Runtime->FilterFunction = [&](ui32/* nodeId*/, std::unique_ptr<IEventHandle>& ev) {
                    switch(ev->Type) {
                        case TEvBlobStorage::TEvPutResult::EventType:
                            UNIT_ASSERT_VALUES_EQUAL(ev->Get<TEvBlobStorage::TEvPutResult>()->Status, NKikimrProto::OK);
                            return false;
                        default:
                            return true;
                    }
                };
                
                while (totalSize < 16_MB) {
                    writeBlob(GenerateRandom(1, groupSize + 1), GenerateRandom(1, 1_MB));
                }
                env.Sim(TDuration::Minutes(5));

                for (ui32 i = 0; i < groupSize; ++i) {
                    cutLocalSyncLogControls[i] = !!(mask & (1 << i));
                    compressChunksControls[i] = compressChunks;
                }

                while (totalSize < 32_MB) {
                    writeBlob(GenerateRandom(1, groupSize + 1), GenerateRandom(1, 1_MB));
                }

                env.Sim(TDuration::Minutes(5));
            }
        }
    }

    Y_UNIT_TEST(TestSyncLogCuttingMirror3dc) {
        TestCutting(TBlobStorageGroupType::ErasureMirror3dc);
    }

    Y_UNIT_TEST(TestSyncLogCuttingMirror3of4) {
        TestCutting(TBlobStorageGroupType::ErasureMirror3of4);
    }

    Y_UNIT_TEST(TestSyncLogCuttingBlock4Plus2) {
        TestCutting(TBlobStorageGroupType::Erasure4Plus2Block);
    }

    Y_UNIT_TEST(SyncLogChunkDeletionRaceWithoutVDiskRestart) {
        /*
         * Regression scenario for d91b9939b245190565892128822a642aba2a5bb1.
         *
         * The old SyncLog chunk deletion protocol had two independent events:
         *
         * 1. trimming the SyncLog removes old disk chunks from the in-memory SyncLog index;
         * 2. the removed chunk object sends TEvSyncLogFreeChunk only after the last snapshot
         *    reference to that chunk is destroyed.
         *
         * Before the fix, trimming serialized removed chunks as delayed-deletion state in a
         * SyncLog entry point. When the last snapshot reference was later destroyed, the keeper
         * reacted to TEvSyncLogFreeChunk by writing another SyncLog entry point with plain
         * CommitRecord.DeleteChunks. That returned the chunk to PDisk's free pool immediately.
         * If stale delete state for the same chunk was acted on again before the chunk was
         * recommitted by this VDisk, the next plain DeleteChunks hit a chunk that PDisk already
         * considered free. Real PDisk reports this as BPD77 with trueOwnerId# OwnerUnallocated.
         *
         * This test creates the race without restarting VDisk:
         *
         * 1. write enough real VDisk records through TEvVMultiPut to make SyncLog spill to PDisk
         *    chunks;
         * 2. force a SyncLog entry point commit and remember the committed SyncLog chunks;
         * 3. take a SyncLog snapshot so trimmed chunks cannot be released immediately;
         * 4. send peer TEvVSync requests that make SyncLog trim itself through the same cut-log
         *    path used before a FULL_RECOVER/no-data response. Fixed code immediately starts a
         *    decommitted-delete entry point here, and the test pauses it; old code only records
         *    delayed deletion state and waits for TEvSyncLogFreeChunk;
         * 5. destroy the snapshot while the fixed-code commit is still paused, or trigger the
         *    old-code TEvSyncLogFreeChunk path that writes a plain DeleteChunks commit;
         * 6. let the paused commit proceed and observe how chunk deletion is sent to PDisk.
         *
         * Fixed code must first move deleted chunks to PDisk's decommitted state via
         * DeleteToDecommitted and send TEvChunkForget only after both conditions are satisfied:
         * the entry point deletion is committed and the snapshot reference is gone. Old code sends
         * plain DeleteChunks, which this test reports through observedImmediateFree. If later
         * sync-read cut-log traffic naturally emits another plain DeleteChunks for an already
         * freed chunk, the filter drops it before PDiskMock's Y_VERIFY and reports the
         * BPD77-equivalent condition through observedDuplicateImmediateFree.
         */
        const TBlobStorageGroupType erasure = TBlobStorageGroupType::Erasure4Plus2Block;
        TEnvironmentSetup env{{
            .NodeCount = erasure.BlobSubgroupSize(),
            .VDiskReplPausedAtStart = true,
            .Erasure = erasure,
            // VDisks keep MaxLogoBlobDataSize at 10 MB, and huge heap expects
            // more than one huge slot per chunk.
            .PDiskChunkSize = 32_MB,
        }};
        auto& runtime = env.Runtime;

        env.CreateBoxAndPool(1, 1);
        std::vector<ui32> groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());
        const TVDiskID vdiskId = info->GetVDiskId(0);
        const TActorId vdiskActorId = info->GetActorId(0);
        const TActorId edge = runtime->AllocateEdgeActor(vdiskActorId.NodeId(), __FILE__, __LINE__);
        TVector<TVDiskID> peerVDiskIds;
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            const TVDiskID peerVDiskId = info->GetVDiskId(i);
            if (peerVDiskId != vdiskId) {
                peerVDiskIds.push_back(peerVDiskId);
            }
        }
        UNIT_ASSERT_C(!peerVDiskIds.empty(), "the test needs peer VDisks to drive TEvVSync cut-log requests");

        TActorId syncLogId;
        TActorId syncLogKeeperId;
        bool gotOwner = false;
        NPDisk::TOwner owner = 0;
        NPDisk::TOwnerRound ownerRound = 0;

        bool postponeNextSyncLogEntryPointCommit = false;
        bool raceCommitPostponed = false;
        bool observedSyncLogDataCommit = false;
        bool observedImmediateFree = false;
        bool observedDuplicateImmediateFree = false;
        bool observedDecommittedDelete = false;
        ui32 chunkForgetEvents = 0;
        ui32 syncLogCommitDoneEvents = 0;
        ui64 maxObservedDataLsn = 0;
        TVector<TChunkIdx> syncLogDataChunks;
        TVector<TChunkIdx> immediateFreeChunks;
        TVector<TChunkIdx> duplicateImmediateFreeChunks;
        TVector<TChunkIdx> decommittedDeleteChunks;
        THashSet<TChunkIdx> immediatelyFreedChunks;
        std::deque<std::pair<ui32, std::unique_ptr<IEventHandle>>> postponedEvents;

        auto observePDiskLog = [&](const NPDisk::TEvLog& msg, bool& duplicateImmediateFreeInThisLog) {
            const ui32 signature = msg.Signature.GetUnmasked();
            if (signature == TLogSignature::SignatureLogoBlobOpt) {
                if (!gotOwner) {
                    owner = msg.Owner;
                    ownerRound = msg.OwnerRound;
                    gotOwner = true;
                } else if (msg.Owner != owner) {
                    return false;
                }
                maxObservedDataLsn = Max(maxObservedDataLsn, msg.Lsn);
            } else if (!gotOwner || msg.Owner != owner) {
                return false;
            }

            const bool syncLogEntryPointCommit = signature == TLogSignature::SignatureSyncLogIdx &&
                msg.CommitRecord.IsStartingPoint;
            if (!syncLogEntryPointCommit) {
                return false;
            }

            if (msg.CommitRecord.CommitChunks) {
                observedSyncLogDataCommit = true;
                syncLogDataChunks.insert(syncLogDataChunks.end(),
                    msg.CommitRecord.CommitChunks.begin(), msg.CommitRecord.CommitChunks.end());
            }

            if (msg.CommitRecord.DeleteChunks) {
                if (msg.CommitRecord.DeleteToDecommitted) {
                    observedDecommittedDelete = true;
                    decommittedDeleteChunks.insert(decommittedDeleteChunks.end(),
                        msg.CommitRecord.DeleteChunks.begin(), msg.CommitRecord.DeleteChunks.end());
                } else {
                    observedImmediateFree = true;
                    for (const TChunkIdx chunkIdx : msg.CommitRecord.DeleteChunks) {
                        if (immediatelyFreedChunks.contains(chunkIdx)) {
                            duplicateImmediateFreeInThisLog = true;
                            observedDuplicateImmediateFree = true;
                            duplicateImmediateFreeChunks.push_back(chunkIdx);
                        } else {
                            immediatelyFreedChunks.insert(chunkIdx);
                        }
                    }
                    immediateFreeChunks.insert(immediateFreeChunks.end(),
                        msg.CommitRecord.DeleteChunks.begin(), msg.CommitRecord.DeleteChunks.end());
                }
            }

            return true;
        };

        runtime->FilterFunction = [&](ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvSyncLogPut:
                    if (gotOwner) {
                        if (!syncLogId) {
                            syncLogId = ev->Recipient;
                        } else if (!syncLogKeeperId && ev->Recipient != syncLogId) {
                            syncLogKeeperId = ev->Recipient;
                        }
                    }
                    break;

                case TEvBlobStorage::EvLog: {
                    const auto *msg = ev->Get<NPDisk::TEvLog>();
                    bool duplicateImmediateFreeInThisLog = false;
                    const bool syncLogEntryPointCommit = observePDiskLog(*msg, duplicateImmediateFreeInThisLog);
                    if (duplicateImmediateFreeInThisLog) {
                        return false;
                    }
                    if (syncLogEntryPointCommit && postponeNextSyncLogEntryPointCommit) {
                        postponeNextSyncLogEntryPointCommit = false;
                        raceCommitPostponed = true;
                        postponedEvents.emplace_back(nodeId, std::exchange(ev, nullptr));
                        return false;
                    }
                    break;
                }

                case TEvBlobStorage::EvMultiLog: {
                    const auto *msg = ev->Get<NPDisk::TEvMultiLog>();
                    bool duplicateImmediateFreeInThisLog = false;
                    for (const auto& [log, traceId] : msg->Logs) {
                        Y_UNUSED(traceId);
                        observePDiskLog(*log, duplicateImmediateFreeInThisLog);
                    }
                    if (duplicateImmediateFreeInThisLog) {
                        return false;
                    }
                    break;
                }

                case TEvBlobStorage::EvChunkForget:
                    ++chunkForgetEvents;
                    break;

                case TEvBlobStorage::EvSyncLogCommitDone:
                    ++syncLogCommitDoneEvents;
                    break;
            }

            return true;
        };

        auto simUntil = [&](auto predicate, TDuration timeout) {
            const TInstant deadline = runtime->GetClock() + timeout;
            runtime->Sim([&] { return runtime->GetClock() <= deadline && !predicate(); });
            return predicate();
        };

        auto sendVSync = [&](const TVDiskID& sourceVDiskId, TSyncState syncState) {
            runtime->Send(new IEventHandle(syncLogId, edge,
                new TEvBlobStorage::TEvVSync(syncState, sourceVDiskId, vdiskId)), syncLogId.NodeId());
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVSyncResult>(edge, false);
            env.Sim(TDuration::MilliSeconds(1));
            return res;
        };

        auto getSyncStateWithActualGuid = [&](const TVDiskID& sourceVDiskId) {
            auto res = sendVSync(sourceVDiskId, TSyncState());
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Record.GetStatus(), NKikimrProto::RESTART);
            return SyncStateFromSyncState(res->Get()->Record.GetNewSyncState());
        };

        auto trimSyncLogThroughPeerSyncReads = [&](ui64 syncedLsn) {
            for (const TVDiskID& sourceVDiskId : peerVDiskIds) {
                TSyncState syncState = getSyncStateWithActualGuid(sourceVDiskId);
                syncState.SyncedLsn = syncedLsn;
                auto res = sendVSync(sourceVDiskId, syncState);
                const auto status = res->Get()->Record.GetStatus();
                UNIT_ASSERT_C(status != NKikimrProto::RESTART && status != NKikimrProto::BLOCKED &&
                    status != NKikimrProto::RACE,
                    "TEvVSync with the actual VDisk guid did not enter the sync-log read path; status# "
                    << NKikimrProto::EReplyStatus_Name(status));
            }
        };

        env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](const TActorId& queueId) {
            auto multiPut = std::make_unique<TEvBlobStorage::TEvVMultiPut>(vdiskId, TInstant::Max(),
                NKikimrBlobStorage::EPutHandleClass::TabletLog, false);
            const TString data = MakeData(1);
            const ui32 records = 4'096;

            for (ui32 i = 0, step = 1; i < records; ++i, ++step) {
                TLogoBlobID fullBlobId;
                for (;;) {
                    fullBlobId = TLogoBlobID(42, 1, step, 0, data.size(), i + 1);
                    if (info->GetVDiskInSubgroup(0, fullBlobId.Hash()) == vdiskId) {
                        break;
                    }
                    ++step;
                }

                TDataPartSet partSet;
                info->Type.SplitData((TBlobStorageGroupType::ECrcMode)fullBlobId.CrcMode(), data, partSet);
                const TLogoBlobID partId(fullBlobId, 1);
                multiPut->AddVPut(partId, TRcBuf(partSet.Parts[partId.PartId() - 1].OwnedString.ConvertToString()),
                    nullptr, nullptr, {});
            }

            runtime->Send(new IEventHandle(queueId, edge, multiPut.release()), edge.NodeId());
            auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVMultiPutResult>(edge, false);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Record.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Record.ItemsSize(), records);
            for (ui32 i = 0; i < records; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Record.GetItems(i).GetStatus(), NKikimrProto::OK);
            }
        });

        UNIT_ASSERT_C(simUntil([&] {
            return syncLogId && syncLogKeeperId && gotOwner && maxObservedDataLsn;
        }, TDuration::Seconds(10)), "failed to discover SyncLog actor, SyncLogKeeper actor, PDisk owner, or data LSN");

        const ui32 commitDoneEventsBeforeDataCommit = syncLogCommitDoneEvents;
        runtime->Send(new IEventHandle(syncLogId, edge,
            new NPDisk::TEvCutLog(owner, ownerRound, maxObservedDataLsn + 1, 0, 0, 0, 0)), syncLogId.NodeId());

        UNIT_ASSERT_C(simUntil([&] {
            return observedSyncLogDataCommit && syncLogCommitDoneEvents > commitDoneEventsBeforeDataCommit;
        }, TDuration::Seconds(30)),
            "failed to move sync log records to PDisk chunks");
        env.Sim(TDuration::MilliSeconds(1));

        THashSet<TChunkIdx> chunksOfInterest(syncLogDataChunks.begin(), syncLogDataChunks.end());
        runtime->Send(new IEventHandle(syncLogId, edge, new TEvListChunks(chunksOfInterest)), syncLogId.NodeId());
        auto chunksBeforeTrim = env.WaitForEdgeActorEvent<TEvListChunksResult>(edge, false);
        UNIT_ASSERT_C(!chunksBeforeTrim->Get()->ChunksSyncLog.empty(), "sync log chunks are not owned");

        runtime->Send(new IEventHandle(syncLogKeeperId, edge, new NSyncLog::TEvSyncLogSnapshot()), syncLogKeeperId.NodeId());
        auto snapshot = env.WaitForEdgeActorEvent<NSyncLog::TEvSyncLogSnapshotResult>(edge, false);
        UNIT_ASSERT_C(snapshot->Get()->SnapshotPtr && !snapshot->Get()->SnapshotPtr->DiskSnapPtr->Empty(),
            "sync log snapshot must keep disk chunks alive");

        postponeNextSyncLogEntryPointCommit = true;
        trimSyncLogThroughPeerSyncReads(maxObservedDataLsn);
        simUntil([&] { return raceCommitPostponed; }, TDuration::Seconds(1));

        snapshot.Destroy();
        env.Sim(TDuration::Seconds(1));
        UNIT_ASSERT_C(raceCommitPostponed || simUntil([&] { return raceCommitPostponed; }, TDuration::Seconds(30)),
            "failed to pause sync log chunk deletion commit");

        for (auto& [nodeId, ev] : std::exchange(postponedEvents,
                std::deque<std::pair<ui32, std::unique_ptr<IEventHandle>>>())) {
            runtime->Send(ev.release(), nodeId);
        }

        UNIT_ASSERT_C(simUntil([&] {
            return observedImmediateFree || (observedDecommittedDelete && chunkForgetEvents);
        }, TDuration::Seconds(30)), "sync log chunk deletion did not finish");

        if (observedImmediateFree) {
            trimSyncLogThroughPeerSyncReads(maxObservedDataLsn + 1);
            simUntil([&] { return observedDuplicateImmediateFree; }, TDuration::Seconds(5));
        }

        UNIT_ASSERT_C(!observedDuplicateImmediateFree,
            "sync read/full-recover cut-log path issued a second plain DeleteChunks for already freed chunks: "
            << FormatList(duplicateImmediateFreeChunks)
            << "; real PDisk reports this as BPD77 ownerId != trueOwnerId without a VDisk restart");
        UNIT_ASSERT_C(!observedImmediateFree,
            "sync log returned chunks to PDisk directly via DeleteChunks: " << FormatList(immediateFreeChunks)
            << "; this is the double-free/reuse window fixed by d91b9939b245190565892128822a642aba2a5bb1");
        UNIT_ASSERT_C(observedDecommittedDelete,
            "sync log must first move deleted chunks to PDisk decommitted state: "
            << FormatList(decommittedDeleteChunks));
        UNIT_ASSERT_C(chunkForgetEvents,
            "sync log must forget decommitted chunks after the last snapshot reference is released");
    }

    Y_UNIT_TEST(SyncWhenDiskGetsDown) {
        return; // re-enable when protocol issue is resolved

        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::Erasure4Plus2Block,
        }};
        auto& runtime = env.Runtime;

        env.CreateBoxAndPool(1, 1);
        auto groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());

        const TActorId edge = runtime->AllocateEdgeActor(1, __FILE__, __LINE__);
        const TString buffer = "hello, world!";
        TLogoBlobID id(1, 1, 1, 0, buffer.size(), 0);
        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvPut(id, buffer, TInstant::Max()));
        });
        auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

        std::unordered_map<TVDiskID, TActorId, THash<TVDiskID>> queues;
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            const TVDiskID vdiskId = info->GetVDiskId(i);
            queues[vdiskId] = env.CreateQueueActor(vdiskId, NKikimrBlobStorage::EVDiskQueueId::GetFastRead, 1000);
        }

        struct TBlobInfo {
            TVDiskID VDiskId;
            TLogoBlobID BlobId;
            NKikimrProto::EReplyStatus Status;
            std::optional<TIngress> Ingress;
        };
        auto collectBlobInfo = [&] {
            std::vector<TBlobInfo> blobs;
            for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
                const TVDiskID vdiskId = info->GetVDiskId(i);
                const TActorId queueId = queues.at(vdiskId);
                const TActorId edge = runtime->AllocateEdgeActor(queueId.NodeId(), __FILE__, __LINE__);
                runtime->Send(new IEventHandle(queueId, edge, TEvBlobStorage::TEvVGet::CreateExtremeDataQuery(
                    vdiskId, TInstant::Max(), NKikimrBlobStorage::EGetHandleClass::FastRead,
                    TEvBlobStorage::TEvVGet::EFlags::ShowInternals, {}, {{id}}).release()), queueId.NodeId());
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVGetResult>(edge);
                auto& record = res->Get()->Record;
                UNIT_ASSERT(record.GetStatus() == NKikimrProto::OK || record.GetStatus() == NKikimrProto::NOTREADY);
                for (auto& result : record.GetResult()) {
                    blobs.push_back({.VDiskId = vdiskId, .BlobId = LogoBlobIDFromLogoBlobID(result.GetBlobID()),
                        .Status = result.GetStatus(), .Ingress = result.HasIngress() ? std::make_optional(
                        TIngress(result.GetIngress())) : std::nullopt});
                }
            }
            return blobs;
        };
        auto dumpBlobs = [&](const char *name, const std::vector<TBlobInfo>& blobs) {
            Cerr << "Blobs(" << name <<"):" << Endl;
            for (const auto& item : blobs) {
                Cerr << item.VDiskId << " " << item.BlobId << " " << NKikimrProto::EReplyStatus_Name(item.Status)
                    << " " << (item.Ingress ? item.Ingress->ToString(&info->GetTopology(), item.VDiskId, item.BlobId) :
                    "none") << Endl;
            }
        };

        env.Sim(TDuration::Seconds(10)); // wait for blob to get synced across the group

        auto blobsInitial = collectBlobInfo();

        runtime->WrapInActorContext(edge, [&] {
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), id.Generation(),
                0, id.Channel(), true, id.Generation(), id.Step(), new TVector<TLogoBlobID>(1, id), nullptr, TInstant::Max(),
                false));
        });
        auto res1 = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

        const ui32 suspendedNodeId = 2;
        env.StopNode(suspendedNodeId);

        runtime->WrapInActorContext(edge, [&] {
            // send the sole do not keep flag
            SendToBSProxy(edge, info->GroupID, new TEvBlobStorage::TEvCollectGarbage(id.TabletID(), 0, 0, 0, false, 0,
                0, nullptr, new TVector<TLogoBlobID>(1, id), TInstant::Max(), false));
        });
        res1 = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvCollectGarbageResult>(edge, false);
        UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);

        // sync barriers and then compact them all
        env.Sim(TDuration::Seconds(10));
        for (ui32 i = 0; i < info->GetTotalVDisksNum(); ++i) {
            const TActorId actorId = info->GetActorId(i);
            if (actorId.NodeId() != suspendedNodeId) {
                const auto& sender = env.Runtime->AllocateEdgeActor(actorId.NodeId());
                auto ev = std::make_unique<IEventHandle>(actorId, sender, TEvCompactVDisk::Create(EHullDbType::LogoBlobs));
                ev->Rewrite(TEvBlobStorage::EvForwardToSkeleton, actorId);
                runtime->Send(ev.release(), sender.NodeId());
                auto res = env.WaitForEdgeActorEvent<TEvCompactVDiskResult>(sender);
            }
        }
        env.Sim(TDuration::Minutes(1));
        auto blobsIntermediate = collectBlobInfo();

        env.StartNode(suspendedNodeId);
        env.Sim(TDuration::Minutes(1));

        auto blobsFinal = collectBlobInfo();

        dumpBlobs("initial", blobsInitial);
        dumpBlobs("intermediate", blobsIntermediate);
        dumpBlobs("final", blobsFinal);

        for (auto& item : blobsIntermediate) {
            UNIT_ASSERT(!item.Ingress || item.Ingress->Raw() == 0);
        }

        for (auto& item : blobsFinal) {
            UNIT_ASSERT(!item.Ingress || item.Ingress->Raw() == 0);
        }
    }
}
