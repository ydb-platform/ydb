#include <ydb/core/blobstorage/ut_blobstorage/lib/ut_helpers.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_private_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_private_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclog_public_events.h>
#include <ydb/core/blobstorage/vdisk/synclog/blobstorage_synclogkeeper_committer.h>
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
                    TControlBoard::RegisterSharedControl(cutLocalSyncLogControls.back(), appData->Icb->VDiskControls.EnableLocalSyncLogDataCutting);
                    TControlBoard::RegisterSharedControl(compressChunksControls.back(), appData->Icb->VDiskControls.EnableSyncLogChunkCompressionHDD);
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

    Y_UNIT_TEST(SyncLogDiskOverflowOldSnapshotCreatesDuplicateFreeChunkWithoutRestart) {
        /*
         * Actor-level reproducer for the live duplicate TOneChunk scenario.
         * This test does not manually build SyncLog commit deltas. It writes real VDisk records,
         * forces SyncLog disk spills, and lets the real SyncLog committer actor append swap pages.
         *
         * The bad sequence is:
         *
         * 1. SyncLog has disk chunks, and the last one still has free pages.
         * 2. PrepareCommitData() takes a snapshot of that disk log.
         * 3. The same PrepareCommitData() fixes disk overflow and removes that old last chunk
         *    from the live disk log, putting it into delayed deletion.
         * 4. The committer appends through the old snapshot into old LastChunkIdx().
         * 5. ApplyCommitResult() sees that the live disk log no longer ends with this chunk and
         *    creates a new TOneChunk for the same chunkIdx.
         * 6. Releasing the old snapshot and then trimming the new live chunk naturally produce two
         *    distinct TEvSyncLogFreeChunk events for the same chunkIdx.
         */
        TEnvironmentSetup env{{
            .NodeCount = 8,
            .Erasure = TBlobStorageGroupType::ErasureMirror3of4,
            .VDiskConfigPreprocessor = [](TVDiskConfig& config) {
                config.MaxLogoBlobDataSize = 8_KB;
                config.MinHugeBlobInBytes = 4_KB;
                config.MilestoneHugeBlobInBytes = 6_KB;
                config.SyncLogMaxMemAmount = 128_KB;
                config.SyncLogMaxDiskAmount = 32_KB;
            },
            .PDiskChunkSize = 32_KB,
        }};
        auto& runtime = env.Runtime;

        env.CreateBoxAndPool(1, 1);
        std::vector<ui32> groups = env.GetGroups();
        UNIT_ASSERT_VALUES_EQUAL(groups.size(), 1);
        const TIntrusivePtr<TBlobStorageGroupInfo> info = env.GetGroupInfo(groups.front());
        const TVDiskID vdiskId = info->GetVDiskId(0);
        const TActorId vdiskActorId = info->GetActorId(0);
        const TActorId edge = runtime->AllocateEdgeActor(vdiskActorId.NodeId(), __FILE__, __LINE__);

        TActorId syncLogId;
        TActorId syncLogKeeperId;
        bool gotOwner = false;
        NPDisk::TOwner owner = 0;
        NPDisk::TOwnerRound ownerRound = 0;
        ui64 maxObservedDataLsn = 0;
        ui32 syncLogCommitDoneEvents = 0;
        ui32 freeChunkNotifications = 0;
        bool observedSyncLogDataCommit = false;
        bool observedAppendToDeletedChunk = false;
        bool observedDuplicateFreeChunk = false;
        bool observedDuplicateImmediateFree = false;
        bool observedInvalidChunkDelete = false;
        bool postponeNextSyncLogDataCommit = false;
        bool dataCommitPostponed = false;
        bool reorderRaceFreeChunks = false;
        ui64 postponedDataCommitLsn = 0;
        TVector<TChunkIdx> postponedDataCommitChunks;
        TMaybe<TChunkIdx> raceChunkToFreeFirst;
        std::unique_ptr<IEventHandle> postponedDataCommit;
        std::deque<std::unique_ptr<IEventHandle>> postponedFreeChunkEvents;
        TVector<TString> duplicateFreeChunkDetails;
        TVector<TString> appendToDeletedChunkDetails;
        TVector<TChunkIdx> duplicateImmediateFreeChunks;
        TVector<TString> invalidChunkDeleteDetails;
        TVector<TString> syncLogCommitDetails;
        TVector<TString> syncLogDeleteDetails;
        TVector<TString> postponeCandidateDetails;
        THashMap<TChunkIdx, ui32> freeChunkNotificationCounts;
        THashSet<TChunkIdx> freeChunkNotificationsAwaitingForget;
        THashSet<TChunkIdx> immediatelyFreedChunks;
        THashSet<TString> observedDeleteLogKeys;
        THashMap<NPDisk::TOwner, THashSet<TChunkIdx>> reservedChunksByOwner;
        THashMap<NPDisk::TOwner, THashSet<TChunkIdx>> committedChunksByOwner;
        THashMap<TActorId, std::deque<NPDisk::TOwner>> chunkReserveOwnerByRecipient;
        auto formatFreeChunkNotifications = [&] {
            TStringStream str;
            str << "[";
            bool first = true;
            for (const auto& [chunkIdx, count] : freeChunkNotificationCounts) {
                if (!first) {
                    str << " ";
                }
                first = false;
                str << "{chunkIdx# " << chunkIdx << " count# " << count << "}";
            }
            str << "]";
            return str.Str();
        };
        auto formatChunkSet = [](const THashSet<TChunkIdx>& chunks) {
            TStringStream str;
            str << "[";
            bool first = true;
            for (const TChunkIdx chunkIdx : chunks) {
                if (!first) {
                    str << " ";
                }
                first = false;
                str << chunkIdx;
            }
            str << "]";
            return str.Str();
        };
        auto findChunkOwner = [&](const THashMap<NPDisk::TOwner, THashSet<TChunkIdx>>& chunksByOwner,
                TChunkIdx chunkIdx) {
            TMaybe<NPDisk::TOwner> result;
            for (const auto& [chunkOwner, chunks] : chunksByOwner) {
                if (chunks.contains(chunkIdx)) {
                    result = chunkOwner;
                    break;
                }
            }
            return result;
        };
        auto formatChunkState = [&](NPDisk::TOwner chunkOwner) {
            return TStringBuilder()
                << "{owner# " << ui32(chunkOwner)
                << " reserved# " << formatChunkSet(reservedChunksByOwner[chunkOwner])
                << " committed# " << formatChunkSet(committedChunksByOwner[chunkOwner])
                << "}";
        };
        auto markInvalidChunkDelete = [&](const NPDisk::TEvLog& msg, TChunkIdx chunkIdx, const char *reason) {
            observedInvalidChunkDelete = true;
            invalidChunkDeleteDetails.push_back(TStringBuilder()
                << "{reason# " << reason
                << " lsn# " << msg.Lsn
                << " signature# " << msg.Signature.GetUnmasked()
                << " isStartingPoint# " << msg.CommitRecord.IsStartingPoint
                << " owner# " << ui32(msg.Owner)
                << " reservedOwner# " << findChunkOwner(reservedChunksByOwner, chunkIdx).GetOrElse(0)
                << " committedOwner# " << findChunkOwner(committedChunksByOwner, chunkIdx).GetOrElse(0)
                << " chunkIdx# " << chunkIdx
                << " deleteToDecommitted# " << msg.CommitRecord.DeleteToDecommitted
                << " chunkState# " << formatChunkState(msg.Owner)
                << " chunks# " << FormatList(msg.CommitRecord.DeleteChunks)
                << "}");
        };

        auto observePDiskLog = [&](const NPDisk::TEvLog& msg) {
            const ui32 signature = msg.Signature.GetUnmasked();
            const bool syncLogEntryPointCommit = signature == TLogSignature::SignatureSyncLogIdx &&
                msg.CommitRecord.IsStartingPoint;
            if (signature == TLogSignature::SignatureLogoBlobOpt) {
                owner = msg.Owner;
                ownerRound = msg.OwnerRound;
                gotOwner = true;
                maxObservedDataLsn = Max(maxObservedDataLsn, msg.Lsn);
            }

            if (!msg.Signature.HasCommitRecord()) {
                return false;
            }

            if (syncLogEntryPointCommit && msg.CommitRecord.CommitChunks) {
                observedSyncLogDataCommit = true;
                syncLogCommitDetails.push_back(TStringBuilder()
                    << "{lsn# " << msg.Lsn
                    << " owner# " << ui32(msg.Owner)
                    << " chunks# " << FormatList(msg.CommitRecord.CommitChunks)
                    << "}");
            }

            auto& reservedChunks = reservedChunksByOwner[msg.Owner];
            auto& committedChunks = committedChunksByOwner[msg.Owner];
            for (const TChunkIdx chunkIdx : msg.CommitRecord.CommitChunks) {
                reservedChunks.erase(chunkIdx);
                committedChunks.insert(chunkIdx);
                immediatelyFreedChunks.erase(chunkIdx);
            }

            if (syncLogEntryPointCommit && msg.CommitRecord.DeleteChunks) {
                syncLogDeleteDetails.push_back(TStringBuilder()
                    << "{lsn# " << msg.Lsn
                    << " owner# " << ui32(msg.Owner)
                    << " chunks# " << FormatList(msg.CommitRecord.DeleteChunks)
                    << " deleteToDecommitted# " << msg.CommitRecord.DeleteToDecommitted
                    << "}");
            }

            if (syncLogEntryPointCommit && msg.CommitRecord.CommitChunks && msg.CommitRecord.DeleteChunks) {
                THashSet<TChunkIdx> deletedChunks(msg.CommitRecord.DeleteChunks.begin(), msg.CommitRecord.DeleteChunks.end());
                for (const TChunkIdx chunkIdx : msg.CommitRecord.CommitChunks) {
                    if (deletedChunks.contains(chunkIdx)) {
                        observedAppendToDeletedChunk = true;
                        appendToDeletedChunkDetails.push_back(TStringBuilder()
                            << "{lsn# " << msg.Lsn
                            << " owner# " << ui32(msg.Owner)
                            << " chunkIdx# " << chunkIdx
                            << " commitChunks# " << FormatList(msg.CommitRecord.CommitChunks)
                            << " deleteChunks# " << FormatList(msg.CommitRecord.DeleteChunks)
                            << "}");
                    }
                }
            }

            if (msg.CommitRecord.DeleteChunks) {
                const TString deleteLogKey = TStringBuilder()
                    << msg.Lsn << ":" << msg.Owner << ":" << msg.OwnerRound << ":"
                    << msg.CommitRecord.DeleteToDecommitted << ":"
                    << FormatList(msg.CommitRecord.DeleteChunks);
                if (!observedDeleteLogKeys.insert(deleteLogKey).second) {
                    return false;
                }

                for (const TChunkIdx chunkIdx : msg.CommitRecord.DeleteChunks) {
                    if (msg.CommitRecord.DeleteToDecommitted) {
                        if (immediatelyFreedChunks.contains(chunkIdx)) {
                            markInvalidChunkDelete(msg, chunkIdx, "delete-to-decommitted-after-plain-delete");
                            continue;
                        }

                        if (reservedChunks.contains(chunkIdx)) {
                            continue;
                        }

                        if (committedChunks.erase(chunkIdx)) {
                            reservedChunks.insert(chunkIdx);
                            continue;
                        }

                        markInvalidChunkDelete(msg, chunkIdx, "delete-to-decommitted-missing-chunk");
                    } else {
                        const ui32 erased = reservedChunks.erase(chunkIdx) + committedChunks.erase(chunkIdx);
                        if (!erased) {
                            markInvalidChunkDelete(msg, chunkIdx, "plain-delete-missing-chunk");
                        }

                        if (!immediatelyFreedChunks.insert(chunkIdx).second) {
                            observedDuplicateImmediateFree = true;
                            duplicateImmediateFreeChunks.push_back(chunkIdx);
                        }
                    }
                }
            }

            return observedAppendToDeletedChunk || observedDuplicateImmediateFree || observedInvalidChunkDelete;
        };

        auto shouldPostponeSyncLogDataCommit = [&](const NPDisk::TEvLog& msg) {
            if (postponeNextSyncLogDataCommit &&
                    msg.Signature.GetUnmasked() == TLogSignature::SignatureSyncLogIdx &&
                    msg.CommitRecord.IsStartingPoint &&
                    msg.CommitRecord.CommitChunks) {
                postponeCandidateDetails.push_back(TStringBuilder()
                    << "{lsn# " << msg.Lsn
                    << " owner# " << ui32(msg.Owner)
                    << " expectedOwner# " << ui32(owner)
                    << " gotOwner# " << gotOwner
                    << " chunks# " << FormatList(msg.CommitRecord.CommitChunks)
                    << " deletes# " << FormatList(msg.CommitRecord.DeleteChunks)
                    << "}");
            }
            return postponeNextSyncLogDataCommit &&
                gotOwner &&
                msg.Owner == owner &&
                msg.Signature.GetUnmasked() == TLogSignature::SignatureSyncLogIdx &&
                msg.CommitRecord.IsStartingPoint &&
                msg.CommitRecord.CommitChunks &&
                msg.CommitRecord.DeleteChunks;
        };

        auto shouldDropRaceChunkDelete = [&](const NPDisk::TEvLog& msg) {
            if (!msg.Signature.HasCommitRecord() || !msg.CommitRecord.DeleteChunks ||
                    !msg.CommitRecord.DeleteToDecommitted) {
                return false;
            }

            for (const TChunkIdx chunkIdx : msg.CommitRecord.DeleteChunks) {
                if (immediatelyFreedChunks.contains(chunkIdx)) {
                    markInvalidChunkDelete(msg, chunkIdx, "drop-delete-to-decommitted-after-plain-delete");
                    return true;
                }
            }
            return false;
        };

        runtime->FilterFunction = [&](ui32, std::unique_ptr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::EvSyncLogPut:
                    if (!syncLogId) {
                        syncLogId = ev->Recipient;
                    } else if (!syncLogKeeperId && ev->Recipient != syncLogId) {
                        syncLogKeeperId = ev->Recipient;
                    }
                    break;

                case TEvBlobStorage::EvLog: {
                    const auto *msg = ev->Get<NPDisk::TEvLog>();
                    if (shouldDropRaceChunkDelete(*msg)) {
                        return false;
                    }

                    if (shouldPostponeSyncLogDataCommit(*msg)) {
                        postponeNextSyncLogDataCommit = false;
                        dataCommitPostponed = true;
                        postponedDataCommitLsn = msg->Lsn;
                        postponedDataCommitChunks = msg->CommitRecord.CommitChunks;
                        if (postponedDataCommitChunks) {
                            raceChunkToFreeFirst = postponedDataCommitChunks.back();
                            reorderRaceFreeChunks = true;
                        }
                        postponedDataCommit.reset(ev.release());
                        return false;
                    }

                    if (observePDiskLog(*ev->Get<NPDisk::TEvLog>())) {
                        return false;
                    }
                    break;
                }

                case TEvBlobStorage::EvMultiLog:
                    for (const auto& [log, traceId] : ev->Get<NPDisk::TEvMultiLog>()->Logs) {
                        Y_UNUSED(traceId);
                        if (shouldDropRaceChunkDelete(*log)) {
                            return false;
                        }
                    }

                    if (postponeNextSyncLogDataCommit) {
                        for (const auto& [log, traceId] : ev->Get<NPDisk::TEvMultiLog>()->Logs) {
                            Y_UNUSED(traceId);
                            if (shouldPostponeSyncLogDataCommit(*log)) {
                                postponeNextSyncLogDataCommit = false;
                                dataCommitPostponed = true;
                                postponedDataCommitLsn = log->Lsn;
                                postponedDataCommitChunks = log->CommitRecord.CommitChunks;
                                if (postponedDataCommitChunks) {
                                    raceChunkToFreeFirst = postponedDataCommitChunks.back();
                                    reorderRaceFreeChunks = true;
                                }
                                postponedDataCommit.reset(ev.release());
                                return false;
                            }
                        }
                    }
                    for (const auto& [log, traceId] : ev->Get<NPDisk::TEvMultiLog>()->Logs) {
                        Y_UNUSED(traceId);
                        if (observePDiskLog(*log)) {
                            return false;
                        }
                    }
                    break;

                case TEvBlobStorage::EvChunkReserve: {
                    const auto *msg = ev->Get<NPDisk::TEvChunkReserve>();
                    chunkReserveOwnerByRecipient[ev->Sender].push_back(msg->Owner);
                    break;
                }

                case TEvBlobStorage::EvChunkReserveResult: {
                    const auto *msg = ev->Get<NPDisk::TEvChunkReserveResult>();
                    if (msg->Status == NKikimrProto::OK) {
                        auto it = chunkReserveOwnerByRecipient.find(ev->Recipient);
                        if (it != chunkReserveOwnerByRecipient.end() && !it->second.empty()) {
                            const NPDisk::TOwner chunkOwner = it->second.front();
                            it->second.pop_front();
                            for (const TChunkIdx chunkIdx : msg->ChunkIds) {
                                reservedChunksByOwner[chunkOwner].insert(chunkIdx);
                            }
                        }
                    }
                    break;
                }

                case TEvBlobStorage::EvSyncLogCommitDone:
                    ++syncLogCommitDoneEvents;
                    break;

                case TEvBlobStorage::EvSyncLogFreeChunk: {
                    const auto *msg = ev->Get<NSyncLog::TEvSyncLogFreeChunk>();
                    if (reorderRaceFreeChunks && raceChunkToFreeFirst && msg->ChunkIdx != *raceChunkToFreeFirst) {
                        postponedFreeChunkEvents.emplace_back(ev.release());
                        return false;
                    }
                    if (reorderRaceFreeChunks && raceChunkToFreeFirst && msg->ChunkIdx == *raceChunkToFreeFirst) {
                        reorderRaceFreeChunks = false;
                    }
                    ui32& notificationCount = freeChunkNotificationCounts[msg->ChunkIdx];
                    ++notificationCount;
                    ++freeChunkNotifications;
                    if (!freeChunkNotificationsAwaitingForget.insert(msg->ChunkIdx).second) {
                        observedDuplicateFreeChunk = true;
                        duplicateFreeChunkDetails.push_back(TStringBuilder()
                            << "{chunkIdx# " << msg->ChunkIdx
                            << " count# " << notificationCount
                            << "}");
                    }
                    break;
                }

                case TEvBlobStorage::EvChunkForget:
                    for (const TChunkIdx chunkIdx : ev->Get<NPDisk::TEvChunkForget>()->ForgetChunks) {
                        freeChunkNotificationsAwaitingForget.erase(chunkIdx);
                    }
                    if (const auto *msg = ev->Get<NPDisk::TEvChunkForget>()) {
                        auto& reservedChunks = reservedChunksByOwner[msg->Owner];
                        auto& committedChunks = committedChunksByOwner[msg->Owner];
                        for (const TChunkIdx chunkIdx : msg->ForgetChunks) {
                            reservedChunks.erase(chunkIdx);
                            committedChunks.erase(chunkIdx);
                            immediatelyFreedChunks.insert(chunkIdx);
                        }
                    }
                    break;
            }

            return true;
        };

        auto simUntil = [&](auto predicate, TDuration timeout) {
            const TInstant deadline = runtime->GetClock() + timeout;
            runtime->Sim([&] { return runtime->GetClock() <= deadline && !predicate(); });
            return predicate();
        };

        ui32 nextStep = 1;
        ui64 nextCookie = 1;
        const TString data = MakeData(1);
        auto writeVDiskRecords = [&](const TActorId& queueId, ui32 records, const char *phase) {
            const ui32 batchSize = 4'096;
            for (ui32 firstRecord = 0; firstRecord < records; firstRecord += batchSize) {
                if (observedAppendToDeletedChunk || observedDuplicateFreeChunk ||
                        observedDuplicateImmediateFree || observedInvalidChunkDelete) {
                    return;
                }

                const ui32 batch = Min(batchSize, records - firstRecord);
                auto multiPut = std::make_unique<TEvBlobStorage::TEvVMultiPut>(vdiskId, TInstant::Max(),
                    NKikimrBlobStorage::EPutHandleClass::TabletLog, false);

                for (ui32 i = 0; i < batch; ++i) {
                    const TLogoBlobID blobId(TLogoBlobID(42, 1, nextStep++, 0, data.size(), nextCookie++), 1);
                    multiPut->AddVPut(blobId, TRcBuf(data), nullptr, false, false, false, nullptr, {}, false);
                }

                runtime->Send(new IEventHandle(queueId, edge, multiPut.release()), edge.NodeId());
                auto res = env.WaitForEdgeActorEvent<TEvBlobStorage::TEvVMultiPutResult>(edge, false);
                if (res->Get()->Record.GetStatus() != NKikimrProto::OK &&
                        (observedDuplicateFreeChunk || observedDuplicateImmediateFree || observedInvalidChunkDelete)) {
                    return;
                }
                UNIT_ASSERT_C(res->Get()->Record.GetStatus() == NKikimrProto::OK,
                    "TEvVMultiPut failed during " << phase
                    << " status# " << NKikimrProto::EReplyStatus_Name(res->Get()->Record.GetStatus()));
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Record.ItemsSize(), batch);
                for (ui32 i = 0; i < batch; ++i) {
                    UNIT_ASSERT_C(res->Get()->Record.GetItems(i).GetStatus() == NKikimrProto::OK,
                        "TEvVMultiPut item failed during " << phase
                        << " item# " << i
                        << " status# " << NKikimrProto::EReplyStatus_Name(res->Get()->Record.GetItems(i).GetStatus()));
                }
            }
        };

        auto requestSyncLogCut = [&](const char *phase) {
            runtime->Send(new IEventHandle(syncLogId, edge,
                new NPDisk::TEvCutLog(owner, ownerRound, maxObservedDataLsn + 1, 0, 0, 0, 0)), syncLogId.NodeId());
            Y_UNUSED(phase);
            env.Sim(TDuration::MilliSeconds(1));
        };

        env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](const TActorId& queueId) {
            writeVDiskRecords(queueId, 3'000, "initial sync-log fill");
        });

        UNIT_ASSERT_C(simUntil([&] {
            return syncLogId && syncLogKeeperId && gotOwner && maxObservedDataLsn;
        }, TDuration::Seconds(10)), "failed to discover SyncLog actor, SyncLogKeeper actor, PDisk owner, or data LSN");

        requestSyncLogCut("initial sync-log disk spill");
        UNIT_ASSERT_C(simUntil([&] {
            return observedSyncLogDataCommit && syncLogCommitDoneEvents;
        }, TDuration::Seconds(30)), "initial writes did not move SyncLog data to disk"
            << "; commits# " << FormatList(syncLogCommitDetails)
            << "; deletes# " << FormatList(syncLogDeleteDetails));

        const ui32 commitDoneBeforePostponedDataCommit = syncLogCommitDoneEvents;
        postponeNextSyncLogDataCommit = true;
        env.WithQueueId(vdiskId, NKikimrBlobStorage::EVDiskQueueId::PutTabletLog, [&](const TActorId& queueId) {
            writeVDiskRecords(queueId, 1'024, "pre-race sync-log fill");
        });
        requestSyncLogCut("pre-race sync-log cut and postpone data commit");
        UNIT_ASSERT_C(simUntil([&] {
            return dataCommitPostponed;
        }, TDuration::Seconds(30)), "failed to postpone a natural SyncLog data commit before the large swap"
            << "; candidates# " << FormatList(postponeCandidateDetails)
            << "; commits# " << FormatList(syncLogCommitDetails)
            << "; deletes# " << FormatList(syncLogDeleteDetails));

        UNIT_ASSERT_C(postponedDataCommit, "data commit was marked as postponed but no event was saved");
        runtime->Send(postponedDataCommit.release(), edge.NodeId());
        UNIT_ASSERT_C(simUntil([&] {
            return syncLogCommitDoneEvents > commitDoneBeforePostponedDataCommit ||
                observedAppendToDeletedChunk || observedDuplicateFreeChunk ||
                observedDuplicateImmediateFree || observedInvalidChunkDelete;
        }, TDuration::Seconds(30)), "postponed SyncLog data commit did not complete"
            << "; lsn# " << postponedDataCommitLsn
            << "; chunks# " << FormatList(postponedDataCommitChunks));

        UNIT_ASSERT_C(!observedAppendToDeletedChunk,
            "SyncLog committer appended swap data into a chunk deleted by the same entrypoint commit: "
            << FormatList(appendToDeletedChunkDetails)
            << "; this creates two live TOneChunk objects for one SyncLog chunkIdx without restart"
            << "; commits# " << FormatList(syncLogCommitDetails)
            << "; deletes# " << FormatList(syncLogDeleteDetails));

        UNIT_ASSERT_C(!observedDuplicateFreeChunk,
            "SyncLog produced two TEvSyncLogFreeChunk notifications for the same chunk before TEvChunkForget: "
            << FormatList(duplicateFreeChunkDetails)
            << "; this means two live TOneChunk objects existed for one SyncLog chunkIdx without restart"
            << "; freeChunkNotifications# " << formatFreeChunkNotifications()
            << "; commits# " << FormatList(syncLogCommitDetails)
            << "; deletes# " << FormatList(syncLogDeleteDetails));

        UNIT_ASSERT_C(!observedDuplicateImmediateFree,
            "SyncLog produced a second plain DeleteChunks for already freed chunks: "
            << FormatList(duplicateImmediateFreeChunks)
            << "; real PDisk reports this as BPD77 ownerId != trueOwnerId");

        UNIT_ASSERT_C(!observedInvalidChunkDelete,
            "SyncLog sent DeleteChunks for chunks that PDiskMock no longer considers reserved or committed: "
            << FormatList(invalidChunkDeleteDetails)
            << "; this would make PDiskMock crash in DeleteChunk/UncommitChunk"
            << "; freeChunkNotifications# " << formatFreeChunkNotifications()
            << "; commits# " << FormatList(syncLogCommitDetails)
            << "; deletes# " << FormatList(syncLogDeleteDetails));
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
