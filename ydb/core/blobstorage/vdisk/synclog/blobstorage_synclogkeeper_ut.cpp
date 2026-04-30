#include "blobstorage_synclogkeeper_state.h"
#include "blobstorage_synclogreader.h"
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/null.h>

#define STR Cnull

namespace NKikimr {

    using namespace NSyncLog;

    ////////////////////////////////////////////////////////////////////////////
    // TEntryPointPair
    // Entry point serialized data + entry point lsn
    ////////////////////////////////////////////////////////////////////////////
    struct TEntryPointPair {
        TString EntryPoint;
        ui64 EntryPointLsn = 0;
    };

    ////////////////////////////////////////////////////////////////////////////
    // PrintStatus
    // Entry point serialized data + entry point lsn
    ////////////////////////////////////////////////////////////////////////////
    static void PrintStatus(const TSyncLogKeeperState *state, const TString &str = {}) {
        STR << "    " << str << "FirstLsnToKeep# " << state->CalculateFirstLsnToKeep()
            << " Decomposed# " << state->CalculateFirstLsnToKeepDecomposed() << "\n"
            << "    boundaries# " << state->GetSyncLogSnapshot()->BoundariesToString() << "\n";
    }

    ////////////////////////////////////////////////////////////////////////////
    // TPayloadWriter
    // Write one sample record to log/synclog, allocates next lsn for it
    ////////////////////////////////////////////////////////////////////////////
    class TPayloadWriter {
    public:
        void WriteToLog(TSyncLogKeeperState *state, ui64 *lsn) {
            ++*lsn;
            ++Gen;
            ui32 size = NSyncLog::TSerializeRoutines::SetBlock(Buf, *lsn, TabletId, Gen, 0);
            state->PutOne((const NSyncLog::TRecordHdr *)Buf, size);
            STR << "Put lsn# " << *lsn << "\n";
        }

    private:
        const ui64 TabletId = 1;
        ui64 Gen = 1;
        char Buf[NSyncLog::MaxRecFullSize];
    };

    ////////////////////////////////////////////////////////////////////////////
    // TSyncLogKeeperTest
    ////////////////////////////////////////////////////////////////////////////
    class TSyncLogKeeperTest {
    public:
        void CreateState(TEntryPointPair ep);
        void Run();

        void PrintStatus(const TString &str = {}) {
            ::NKikimr::PrintStatus(State.get(), str);
        }
    private:
        std::unique_ptr<TSyncLogKeeperState> State;
        TPayloadWriter PayloadWriter;

        bool Trim(ui64 lsn);
        bool CutLog(ui64 lsn);
    };

    struct TSyncLogKeeperTestSettings {
        ui64 SyncLogMaxMemAmount = ui64(64) << ui64(20);
        ui64 SyncLogMaxDiskAmount = 0;
        ui32 ChunkSize = 512u << 10u;
        ui32 AppendBlockSize = 4064;
    };

    static TIntrusivePtr<TVDiskContext> CreateSyncLogKeeperTestVDiskContext() {
        TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::ErasureMirror3, 2, 4);
        return new TVDiskContext(
                TActorId(),
                groupInfo.PickTopology(),
                new ::NMonitoring::TDynamicCounters(),
                TVDiskID(),
                nullptr,
                NPDisk::DEVICE_TYPE_UNKNOWN);
    }

    static NSyncLog::TSyncLogParams CreateSyncLogParams(
            const TIntrusivePtr<TVDiskContext>& vctx,
            const TSyncLogKeeperTestSettings& settings)
    {
        const ui64 pdiskGuid = 19283489374;
        const ui32 syncLogAdvisedIndexedBlockSize = ui32(1) << ui32(20);

        return {
            pdiskGuid,
            settings.ChunkSize,
            settings.AppendBlockSize,
            syncLogAdvisedIndexedBlockSize,
            vctx->SyncLogCache
        };
    }

    std::unique_ptr<TSyncLogKeeperState> CreateSyncLogKeeperState(
            TEntryPointPair ep,
            const TSyncLogKeeperTestSettings& settings)
    {
        TIntrusivePtr<TVDiskContext> vctx = CreateSyncLogKeeperTestVDiskContext();

        const ui64 syncLogMaxEntryPointSize = ui64(128) << ui64(10);

        NSyncLog::TSyncLogParams params = CreateSyncLogParams(vctx, settings);

        TString explanation;
        auto r = TSyncLogRepaired::Construct(std::move(params), ep.EntryPoint, ep.EntryPointLsn, explanation);
        Y_ABORT_UNLESS(r);
        std::unique_ptr<NSyncLog::TSyncLogRecovery> recovery = std::make_unique<NSyncLog::TSyncLogRecovery>(std::move(r));
        std::unique_ptr<TSyncLogRepaired> repaired = recovery->ReleaseRepaired();

        auto state = std::make_unique<TSyncLogKeeperState>(
                vctx,
                std::move(repaired),
                settings.SyncLogMaxMemAmount,
                settings.SyncLogMaxDiskAmount,
                syncLogMaxEntryPointSize);
        state->Init(nullptr, std::make_shared<TFakeLoggerCtx>());
        return state;
    }

    void TSyncLogKeeperTest::CreateState(TEntryPointPair ep) {
        State = CreateSyncLogKeeperState(ep, {});

        STR << "CREATE STATE entryPointLsn# " << ep.EntryPointLsn <<
            " entryPoint# " << (ep.EntryPoint.empty() ? "<empty>" : "<exists>") << "\n";
        PrintStatus();
    }

    bool TSyncLogKeeperTest::Trim(ui64 lsn) {
        State->TrimTailEvent(lsn);
        STR << "Trim lsn# " << lsn << "\n";
        bool commit = State->PerformTrimTailAction();
        PrintStatus();
        return commit;
    }

    bool TSyncLogKeeperTest::CutLog(ui64 lsn) {
        State->CutLogEvent(lsn);
        STR << "CutLog lsn# " << lsn << "\n";
        bool commit = State->PerformCutLogAction([] (ui64) {});
        PrintStatus();
        return commit;
    }

    class TCommitWithNoSwapAndDelChunks {
    public:
        TCommitWithNoSwapAndDelChunks()
        {}

        void Start(TSyncLogKeeperState *state, ui64 recoveryLogConfirmedLsn) {
            CommitData = std::make_unique<TSyncLogKeeperCommitData>(state->PrepareCommitData(recoveryLogConfirmedLsn));
            Y_ABORT_UNLESS((!CommitData->SwapSnap || CommitData->SwapSnap->Empty()) &&
                    CommitData->ChunksToDeleteDelayed.empty() &&
                    CommitData->ChunksToDelete.empty());
            STR << "Commit started\n";
            PrintStatus(state);
        }

        TEntryPointPair Finish(TSyncLogKeeperState *state, ui64 commitLsn) {
            TStringStream s;
            TDeltaToDiskRecLog delta(10);
            TEntryPointSerializer entryPointSerializer(CommitData->SyncLogSnap,
                std::move(CommitData->ChunksToDeleteDelayed), CommitData->RecoveryLogConfirmedLsn);
            entryPointSerializer.Serialize(delta);

            TCommitHistory commitHistory(TInstant(), commitLsn, Max(commitLsn, CommitData->RecoveryLogConfirmedLsn));
            TEvSyncLogCommitDone commitDone(commitHistory, entryPointSerializer.GetEntryPointDbgInfo(),
                std::move(delta));

            // apply commit result
            state->ApplyCommitResult(&commitDone);
            STR << "Commit finished lsn# " << commitLsn << "\n";
            PrintStatus(state);

            return {entryPointSerializer.GetSerializedData(), commitLsn};
        }
    private:
        std::unique_ptr<TSyncLogKeeperCommitData> CommitData;
    };

    void TSyncLogKeeperTest::Run() {
        TEntryPointPair entryPointPair;
        CreateState(TEntryPointPair{TString(), 0});
        // start with empty log
        ui64 lsn = 0;
        bool commit = false;
        // write sample payload
        for (ui64 i = 0; i < 10; ++i) {
            PayloadWriter.WriteToLog(State.get(), &lsn);
        }
        PrintStatus();

        // Trim all written data
        commit = Trim(10);
        Y_ABORT_UNLESS(!commit);

        // Try to cut log and initiate commit
        commit = CutLog(12);
        Y_ABORT_UNLESS(commit);

        // start parallel commit
        TCommitWithNoSwapAndDelChunks parallelCommit;
        parallelCommit.Start(State.get(), 10);

        // write more messages during parallel commit
        lsn = 21;
        for (ui64 i = 0; i < 10; ++i) {
            PayloadWriter.WriteToLog(State.get(), &lsn);
        }
        PrintStatus();

        // commit finished with lsn=31
        entryPointPair = parallelCommit.Finish(State.get(), 31);

        // trim all written data
        commit = Trim(31);
        Y_ABORT_UNLESS(!commit);

        commit = CutLog(31);
        Y_ABORT_UNLESS(commit);

        // start parallel commit
        TCommitWithNoSwapAndDelChunks parallelCommit2;
        parallelCommit2.Start(State.get(), 31);

        // commit finished with lsn=33
        entryPointPair = parallelCommit2.Finish(State.get(), 33);

        STR << "\n************************** RESTART ***********************************************************\n\n";

        ////////////////////////////////////////////////////////////////////////////////////
        // RESTART
        ////////////////////////////////////////////////////////////////////////////////////
        State.reset();
        CreateState(entryPointPair);

        // imitate other VDisk that is syncing with current VDisk
        const ui64 syncedLsn = 31;
        const ui64 dbBirthLsn = 0;
        NSyncLog::TLogEssence e;
        State->FillInSyncLogEssence(&e);
        auto reportInternals = [] () { return TString(); };
        TWhatsNextOutcome outcome = WhatsNext(syncedLsn, dbBirthLsn, &e, reportInternals);
        STR << "Sync result: outcome# " << Name2Str(outcome.WhatsNext)
            << " explanation# " << outcome.Explanation << "\n";
        UNIT_ASSERT(outcome.WhatsNext == EReadWhatsNext::EWnDiskSynced);
    }

    ////////////////////////////////////////////////////////////////////////////
    // Unit tests
    ////////////////////////////////////////////////////////////////////////////
    Y_UNIT_TEST_SUITE(TBlobStorageSyncLogKeeper) {

        // Y_UNIT_TEST(CutLog_EntryPointNewFormat) {
        //     TSyncLogKeeperTest test;
        //     test.Run();
        // }

        // Y_UNIT_TEST(WhatsNextAllowsCachedMemPageBeforeLogStartLsnAfterCutLog) {
        //     /*
        //      * End-to-end reproducer for the first production VERIFY in WhatsNext().
        //      *
        //      * Production shape:
        //      *   boundaries# {LogStartLsn: 62402361870
        //      *       {Mem# [62402262890, 62408853246] ...}
        //      *       {Dsk: [62402361870, 62408853246]}}
        //      * In this compact test, Disk.first may be LogStartLsn - 1 because the first indexed disk page
        //      * can straddle the cut boundary. The important reproducer property is the same:
        //      * Mem.first < Disk.first, which is exactly what used to trip WhatsNext().
        //      *
        //      * This test produces the same kind of shape via real TSyncLogKeeperState transitions:
        //      *   1. Keep SyncLog mem cache large enough, so cut-log commits do not discard cached pages.
        //      *   2. Write several pages and commit them to DiskRecLog through CutLogEvent().
        //      *   3. Write more pages and start another CutLogEvent() commit. PrepareCommitData() has to add a
        //      *      new disk chunk and, because of the disk chunk limit, trims the oldest disk chunk.
        //      *   4. After the committer finishes, live DiskRecLog contains the new disk tail, but MemRecLog
        //      *      still contains cached pages starting before LogStartLsn.
        //      *
        //      * The reader must ignore the cached dead prefix below LogStartLsn and answer normally; it must not
        //      * crash on the old FirstDiskLsn <= FirstMemLsn assumption.
        //      *
        //      * This is related to the entry point ownership test below: both exercise the same cut/trim area.
        //      * The first symptom is a live sync read hitting the stale boundary invariant; the second symptom is
        //      * recovery seeing a persistent entry point where a trimmed chunk is both delayed-for-delete and
        //      * still listed in DiskRecLogSerialized.
        //      */
        //     const ui32 appendBlockSize = 4064;
        //     const ui32 pagesInChunk = 2;
        //     TSyncLogKeeperTestSettings settings;
        //     settings.SyncLogMaxMemAmount = appendBlockSize * 100;
        //     settings.SyncLogMaxDiskAmount = appendBlockSize * pagesInChunk * 2;
        //     settings.ChunkSize = appendBlockSize * pagesInChunk;
        //     settings.AppendBlockSize = appendBlockSize;

        //     std::unique_ptr<TSyncLogKeeperState> state = CreateSyncLogKeeperState(TEntryPointPair{TString(), 0}, settings);
        //     TPayloadWriter writer;
        //     TCommitWithSwapSimulator committer(1);
        //     ui64 lsn = 0;

        //     {
        //         WriteUntilMemPages(state.get(), writer, &lsn, 4);
        //         auto commitData = committer.PrepareCutLog(state.get(), lsn + 1, lsn);
        //         UNIT_ASSERT(commitData.ChunksToDeleteDelayed.empty());
        //         committer.Finish(state.get(), std::move(commitData), ++lsn);
        //     }

        //     WriteUntilMemPages(state.get(), writer, &lsn, 6);
        //     auto commitData = committer.PrepareCutLog(state.get(), lsn + 1, lsn);
        //     UNIT_ASSERT_C(commitData.SwapSnap && !commitData.SwapSnap->Empty(),
        //         "second cut-log commit must write the new disk tail; commitData# " << commitData.ToString());
        //     committer.Finish(state.get(), std::move(commitData), ++lsn);

        //     TLogEssence e;
        //     state->FillInSyncLogEssence(&e);
        //     UNIT_ASSERT_C(!e.MemLogEmpty && !e.DiskLogEmpty, "e# " << LogEssenceToString(e));
        //     UNIT_ASSERT_C(e.FirstMemLsn < e.LogStartLsn, "e# " << LogEssenceToString(e));
        //     UNIT_ASSERT_C(e.FirstDiskLsn <= e.LogStartLsn, "e# " << LogEssenceToString(e));
        //     UNIT_ASSERT_C(e.LogStartLsn <= e.LastDiskLsn, "e# " << LogEssenceToString(e));
        //     UNIT_ASSERT_C(e.FirstMemLsn < e.FirstDiskLsn, "e# " << LogEssenceToString(e));

        //     auto reportInternals = [&]() {
        //         return LogEssenceToString(e);
        //     };
        //     TWhatsNextOutcome outcome = WhatsNext(e.LastDiskLsn, 0, &e, reportInternals);
        //     UNIT_ASSERT_VALUES_EQUAL(ui32(outcome.WhatsNext), ui32(EReadWhatsNext::EWnDiskSynced));
        // }

        Y_UNIT_TEST(PrepareCommitDataDoesNotMixTrimmedChunksWithOldDiskSnapshot) {
            /*
             * Regression scenario for the crash sequence seen.
             *
             * SyncLog keeps recently written records in MemRecLog and periodically swaps full pages to DiskRecLog.
             * When DiskRecLog reaches the configured disk chunk limit, PrepareCommitData() may trim the oldest
             * disk chunks and put them into ChunksToDeleteDelayed: those chunks are no longer part of live
             * DiskRecLog, but they are still owned until all snapshots/readers release them and a later commit
             * deletes them from PDisk.
             *
             * The incident had two visible symptoms in the same area:
             *   - live sync read saw LogStartLsn == FirstDiskLsn while the first memory page started slightly
             *     before it, so WhatsNext() tripped over the old FirstDiskLsn <= FirstMemLsn invariant;
             *   - after restart, local recovery failed in TOneChunk::GetOwnedChunks() because the same chunk id
             *     was present twice in recovered SyncLog ownership.
             *
             * The dangerous interleaving this test captures is:
             *   1. PrepareCommitData() takes a SyncLog snapshot containing disk chunks [A, B].
             *   2. The same PrepareCommitData() call fixes disk overflow and moves A into ChunksToDeleteDelayed.
             *   3. The entry point must not be assembled from the old snapshot [A, B] and the new delayed list [A],
             *      because after restart recovery would first insert A from ChunksToDeleteDelayed and then insert
             *      A again from DiskRecLogSerialized, failing GetOwnedChunks().
             *
             * What we want to see: every chunk listed in ChunksToDeleteDelayed is absent from the
             * DiskRecLogSerialized field written to the same entry point.
             */
            const ui32 appendBlockSize = 4064;
            const ui32 pagesInChunk = 2;
            TSyncLogKeeperTestSettings settings;
            settings.SyncLogMaxMemAmount = appendBlockSize * 2;
            settings.SyncLogMaxDiskAmount = appendBlockSize * pagesInChunk * 2;
            settings.ChunkSize = appendBlockSize * pagesInChunk;
            settings.AppendBlockSize = appendBlockSize;

            std::unique_ptr<TSyncLogKeeperState> state = CreateSyncLogKeeperState(TEntryPointPair{TString(), 0}, settings);
            TPayloadWriter writer;
            TCommitWithSwapSimulator committer(1);
            ui64 lsn = 0;

            {
                WriteUntilMemPages(state.get(), writer, &lsn, 4);
                auto commitData = committer.Prepare(state.get(), lsn);
                UNIT_ASSERT(commitData.ChunksToDeleteDelayed.empty());
                committer.Finish(state.get(), std::move(commitData), ++lsn);
            }

            {
                WriteUntilMemPages(state.get(), writer, &lsn, 6);
                auto commitData = committer.Prepare(state.get(), lsn);
                UNIT_ASSERT(commitData.ChunksToDeleteDelayed.empty());
                committer.Finish(state.get(), std::move(commitData), ++lsn);
            }

            WriteUntilMemPages(state.get(), writer, &lsn, 6);
            auto commitData = committer.Prepare(state.get(), lsn);
            UNIT_ASSERT_C(!commitData.ChunksToDeleteDelayed.empty(),
                "test scenario must trigger delayed chunk deletion; commitData# " << commitData.ToString());
            const TString commitDataDebug = commitData.ToString();

            const TString entryPoint = committer.Serialize(std::move(commitData));
            const TSet<TChunkIdx> diskChunks = CollectSerializedDiskChunks(entryPoint);
            const TVector<ui32> delayedChunks = CollectSerializedChunksToDeleteDelayed(entryPoint);
            for (TChunkIdx delayedChunk : delayedChunks) {
                UNIT_ASSERT_C(diskChunks.find(delayedChunk) == diskChunks.end(),
                    "chunk is present both in DiskRecLogSerialized and ChunksToDeleteDelayed; "
                    << "chunk# " << delayedChunk
                    << " DiskChunks# " << FormatList(diskChunks)
                    << " ChunksToDeleteDelayed# " << FormatList(delayedChunks)
                    << " commitData# " << commitDataDebug);
            }
        }

        Y_UNIT_TEST(RecoveryEntryPointParserResolvesDiskChunkAlsoListedAsDelayedDelete) {
            /*
             * Compatibility test for already persisted entry points affected by the old PrepareCommitData()
             * interleaving.
             *
             * Such an entry point may list the same SyncLog chunk twice:
             *   - in DiskRecLogSerialized, because the entry point was serialized from an older disk snapshot;
             *   - in ChunksToDeleteDelayed, because the same commit already trimmed the chunk from live
             *     DiskRecLog and scheduled it for delayed deletion.
             *
             * During recovery this is not a cross-subsystem ownership conflict. The chunk still belongs to
             * SyncLog and must stay owned until delayed delete finishes. The entry point parser should therefore
             * log the duplicate and normalize the recovered SyncLog state in favor of ChunksToDeleteDelayed:
             * the chunk is removed from recovered DiskRecLog, but remains in the delayed-delete list. After
             * that GetOwnedChunks() should see a consistent state and must not need its own conflict resolver.
             */
            const ui32 appendBlockSize = 4064;
            const ui32 pagesInChunk = 2;
            TSyncLogKeeperTestSettings settings;
            settings.SyncLogMaxMemAmount = appendBlockSize * 2;
            settings.SyncLogMaxDiskAmount = appendBlockSize * pagesInChunk * 4;
            settings.ChunkSize = appendBlockSize * pagesInChunk;
            settings.AppendBlockSize = appendBlockSize;

            std::unique_ptr<TSyncLogKeeperState> state = CreateSyncLogKeeperState(TEntryPointPair{TString(), 0}, settings);
            TPayloadWriter writer;
            TCommitWithSwapSimulator committer(1);
            ui64 lsn = 0;

            WriteUntilMemPages(state.get(), writer, &lsn, 4);
            auto commitData = committer.Prepare(state.get(), lsn);
            UNIT_ASSERT_C(commitData.ChunksToDeleteDelayed.empty(),
                "initial entry point must not already contain delayed deletes; commitData# " << commitData.ToString());
            const TEntryPointPair entryPoint = committer.Finish(state.get(), std::move(commitData), ++lsn);

            const TSet<TChunkIdx> diskChunks = CollectSerializedDiskChunks(entryPoint.EntryPoint);
            UNIT_ASSERT_C(!diskChunks.empty(), "test scenario must create DiskRecLog chunks");

            const TChunkIdx duplicateChunk = *diskChunks.begin();
            NKikimrVDiskData::TSyncLogEntryPoint pb = ParseEntryPointProto(entryPoint.EntryPoint);
            pb.AddChunksToDeleteDelayed(duplicateChunk);
            const TString malformedEntryPoint = SerializeEntryPointProto(pb);

            std::unique_ptr<TSyncLogRepaired> repaired = ConstructRepairedFromEntryPoint(
                TEntryPointPair{malformedEntryPoint, entryPoint.EntryPointLsn},
                settings);

            TSet<TChunkIdx> diskChunksAfterRecovery;
            repaired->SyncLogPtr->GetOwnedChunks(diskChunksAfterRecovery);
            UNIT_ASSERT_C(diskChunksAfterRecovery.find(duplicateChunk) == diskChunksAfterRecovery.end(),
                "duplicate chunk must be removed from recovered DiskRecLog; "
                << "chunk# " << duplicateChunk
                << " DiskChunksAfterRecovery# " << FormatList(diskChunksAfterRecovery));
            UNIT_ASSERT_VALUES_EQUAL(diskChunksAfterRecovery.size() + 1, diskChunks.size());

            bool foundInDelayedDelete = false;
            for (TChunkIdx chunkIdx : repaired->ChunksToDelete) {
                if (chunkIdx == duplicateChunk) {
                    foundInDelayedDelete = true;
                }
            }
            UNIT_ASSERT_C(foundInDelayedDelete,
                "duplicate chunk must stay in ChunksToDeleteDelayed; chunk# " << duplicateChunk
                << " ChunksToDeleteDelayed# " << FormatList(repaired->ChunksToDelete));

            TSyncLogRecovery recovery(std::move(repaired));
            TSet<TChunkIdx> ownedChunks;
            recovery.GetOwnedChunks(ownedChunks);
            UNIT_ASSERT_C(ownedChunks.find(duplicateChunk) != ownedChunks.end(),
                "duplicate chunk must remain owned through ChunksToDeleteDelayed; chunk# " << duplicateChunk);
            UNIT_ASSERT_VALUES_EQUAL(ownedChunks.size(), diskChunks.size());
        }

        Y_UNIT_TEST(CutLogPrepareCommitDataDoesNotCreateBothLiveBoundaryAndSerializedOwnershipFailures) {
            /*
             * Bridge test for the two production VERIFY failures.
             *
             * The live WhatsNext() VERIFY does not literally cause the later GetOwnedChunks() VERIFY; a VERIFY
             * aborts the process. The suspected link is that one PrepareCommitData() call can expose both
             * failure modes at once:
             *
             *   - live SyncLog state after FixDiskOverflow() has Mem.first < Disk.first around LogStartLsn,
             *     which would trip the old FirstDiskLsn <= FirstMemLsn assertion in WhatsNext();
             *   - the entry point being serialized from the same commit data may still contain the trimmed
             *     chunk in DiskRecLogSerialized while also listing it in ChunksToDeleteDelayed, which later
             *     trips TOneChunk::GetOwnedChunks() during recovery.
             *
             * We intentionally do not call WhatsNext() here: on the broken code it aborts immediately and would
             * hide the serialized-entry-point half of the same scenario. The test first checks that the live
             * state has the old WhatsNext-failing shape, then runs recovery ownership collection over the same
             * entry point, then checks that the serialized entry point does not contain duplicate chunk
             * ownership.
             */
            const ui32 appendBlockSize = 4064;
            const ui32 pagesInChunk = 2;
            TSyncLogKeeperTestSettings settings;
            settings.SyncLogMaxMemAmount = appendBlockSize * 100;
            settings.SyncLogMaxDiskAmount = appendBlockSize * pagesInChunk * 2;
            settings.ChunkSize = appendBlockSize * pagesInChunk;
            settings.AppendBlockSize = appendBlockSize;

            std::unique_ptr<TSyncLogKeeperState> state = CreateSyncLogKeeperState(TEntryPointPair{TString(), 0}, settings);
            TPayloadWriter writer;
            TCommitWithSwapSimulator committer(1);
            ui64 lsn = 0;

            {
                WriteUntilMemPages(state.get(), writer, &lsn, 4);
                auto commitData = committer.PrepareCutLog(state.get(), lsn + 1, lsn);
                UNIT_ASSERT(commitData.ChunksToDeleteDelayed.empty());
                committer.Finish(state.get(), std::move(commitData), ++lsn);
            }

            WriteUntilMemPages(state.get(), writer, &lsn, 6);
            auto commitData = committer.PrepareCutLog(state.get(), lsn + 1, lsn);
            UNIT_ASSERT_C(commitData.SwapSnap && !commitData.SwapSnap->Empty(),
                "second cut-log commit must write the new disk tail; commitData# " << commitData.ToString());
            const TString commitDataDebug = commitData.ToString();
            UNIT_ASSERT_C(!commitData.ChunksToDeleteDelayed.empty(),
                "test scenario must trigger delayed chunk deletion; commitData# " << commitDataDebug);
            const TEntryPointPair entryPoint = committer.Finish(state.get(), std::move(commitData), ++lsn);

            TLogEssence e;
            state->FillInSyncLogEssence(&e);
            UNIT_ASSERT_C(!e.MemLogEmpty && !e.DiskLogEmpty, "e# " << LogEssenceToString(e));
            UNIT_ASSERT_C(e.FirstMemLsn < e.LogStartLsn, "e# " << LogEssenceToString(e));
            UNIT_ASSERT_C(e.FirstDiskLsn <= e.LogStartLsn, "e# " << LogEssenceToString(e));
            UNIT_ASSERT_C(e.LogStartLsn <= e.LastDiskLsn, "e# " << LogEssenceToString(e));
            UNIT_ASSERT_C(e.FirstMemLsn < e.FirstDiskLsn, "e# " << LogEssenceToString(e));

            const TString liveEssenceDebug = LogEssenceToString(e);
            const TSet<TChunkIdx> diskChunks = CollectSerializedDiskChunks(entryPoint.EntryPoint);
            const TVector<ui32> delayedChunks = CollectSerializedChunksToDeleteDelayed(entryPoint.EntryPoint);
            UNIT_ASSERT_C(!delayedChunks.empty(),
                "entry point must include delayed chunks from commit data; "
                << "LiveEssence# " << liveEssenceDebug
                << " DiskChunks# " << FormatList(diskChunks)
                << " commitData# " << commitDataDebug);

            TSet<TChunkIdx> expectedOwnedChunks = diskChunks;
            for (TChunkIdx delayedChunk : delayedChunks) {
                expectedOwnedChunks.insert(delayedChunk);
            }
            const TSet<TChunkIdx> ownedChunks = RecoverOwnedChunksFromEntryPoint(entryPoint, settings);
            UNIT_ASSERT_VALUES_EQUAL_C(ownedChunks.size(), expectedOwnedChunks.size(),
                "GetOwnedChunks must preserve SyncLog ownership after entry point parsing; "
                << "LiveEssence# " << liveEssenceDebug
                << " OwnedChunks# " << FormatList(ownedChunks)
                << " ExpectedOwnedChunks# " << FormatList(expectedOwnedChunks)
                << " DiskChunks# " << FormatList(diskChunks)
                << " ChunksToDeleteDelayed# " << FormatList(delayedChunks)
                << " commitData# " << commitDataDebug);
            for (TChunkIdx expectedChunk : expectedOwnedChunks) {
                UNIT_ASSERT_C(ownedChunks.find(expectedChunk) != ownedChunks.end(),
                    "GetOwnedChunks lost SyncLog ownership; "
                    << "chunk# " << expectedChunk
                    << " LiveEssence# " << liveEssenceDebug
                    << " OwnedChunks# " << FormatList(ownedChunks)
                    << " ExpectedOwnedChunks# " << FormatList(expectedOwnedChunks)
                    << " DiskChunks# " << FormatList(diskChunks)
                    << " ChunksToDeleteDelayed# " << FormatList(delayedChunks)
                    << " commitData# " << commitDataDebug);
            }

            for (TChunkIdx delayedChunk : delayedChunks) {
                UNIT_ASSERT_C(diskChunks.find(delayedChunk) == diskChunks.end(),
                    "same PrepareCommitData produced both failure modes; "
                    << "chunk# " << delayedChunk
                    << " LiveEssence# " << liveEssenceDebug
                    << " DiskChunks# " << FormatList(diskChunks)
                    << " ChunksToDeleteDelayed# " << FormatList(delayedChunks)
                    << " commitData# " << commitDataDebug);
            }
        }

        Y_UNIT_TEST(WhatsNextAllowsCachedMemPageBeforeLogStartLsnAfterCutLog) {
            /*
             * End-to-end reproducer for the first production VERIFY in WhatsNext().
             *
             * Production shape:
             *   boundaries# {LogStartLsn: 62402361870
             *       {Mem# [62402262890, 62408853246] ...}
             *       {Dsk: [62402361870, 62408853246]}}
             * In this compact test, Disk.first may be LogStartLsn - 1 because the first indexed disk page
             * can straddle the cut boundary. The important reproducer property is the same:
             * Mem.first < Disk.first, which is exactly what used to trip WhatsNext().
             *
             * This test produces the same kind of shape via real TSyncLogKeeperState transitions:
             *   1. Keep SyncLog mem cache large enough, so cut-log commits do not discard cached pages.
             *   2. Write several pages and commit them to DiskRecLog through CutLogEvent().
             *   3. Write more pages and start another CutLogEvent() commit. PrepareCommitData() has to add a
             *      new disk chunk and, because of the disk chunk limit, trims the oldest disk chunk.
             *   4. After the committer finishes, live DiskRecLog contains the new disk tail, but MemRecLog
             *      still contains cached pages starting before LogStartLsn.
             *
             * The reader must ignore the cached dead prefix below LogStartLsn and answer normally; it must not
             * crash on the old FirstDiskLsn <= FirstMemLsn assumption.
             *
             * This is related to the entry point ownership test below: both exercise the same cut/trim area.
             * The first symptom is a live sync read hitting the stale boundary invariant; the second symptom is
             * recovery seeing a persistent entry point where a trimmed chunk is both delayed-for-delete and
             * still listed in DiskRecLogSerialized.
             */
            const ui32 appendBlockSize = 4064;
            const ui32 pagesInChunk = 2;
            TSyncLogKeeperTestSettings settings;
            settings.SyncLogMaxMemAmount = appendBlockSize * 100;
            settings.SyncLogMaxDiskAmount = appendBlockSize * pagesInChunk * 2;
            settings.ChunkSize = appendBlockSize * pagesInChunk;
            settings.AppendBlockSize = appendBlockSize;

            std::unique_ptr<TSyncLogKeeperState> state = CreateSyncLogKeeperState(TEntryPointPair{TString(), 0}, settings);
            TPayloadWriter writer;
            TCommitWithSwapSimulator committer(1);
            ui64 lsn = 0;

            {
                WriteUntilMemPages(state.get(), writer, &lsn, 4);
                auto commitData = committer.PrepareCutLog(state.get(), lsn + 1, lsn);
                UNIT_ASSERT(commitData.ChunksToDeleteDelayed.empty());
                committer.Finish(state.get(), std::move(commitData), ++lsn);
            }

            WriteUntilMemPages(state.get(), writer, &lsn, 6);
            auto commitData = committer.PrepareCutLog(state.get(), lsn + 1, lsn);
            UNIT_ASSERT_C(commitData.SwapSnap && !commitData.SwapSnap->Empty(),
                "second cut-log commit must write the new disk tail; commitData# " << commitData.ToString());
            committer.Finish(state.get(), std::move(commitData), ++lsn);

            TLogEssence e;
            state->FillInSyncLogEssence(&e);
            UNIT_ASSERT_C(!e.MemLogEmpty && !e.DiskLogEmpty, "e# " << LogEssenceToString(e));
            UNIT_ASSERT_C(e.FirstMemLsn < e.LogStartLsn, "e# " << LogEssenceToString(e));
            UNIT_ASSERT_C(e.FirstDiskLsn <= e.LogStartLsn, "e# " << LogEssenceToString(e));
            UNIT_ASSERT_C(e.LogStartLsn <= e.LastDiskLsn, "e# " << LogEssenceToString(e));
            UNIT_ASSERT_C(e.FirstMemLsn < e.FirstDiskLsn, "e# " << LogEssenceToString(e));

            auto reportInternals = [&]() {
                return LogEssenceToString(e);
            };
            TWhatsNextOutcome outcome = WhatsNext(e.LastDiskLsn, 0, &e, reportInternals);
            UNIT_ASSERT_VALUES_EQUAL(ui32(outcome.WhatsNext), ui32(EReadWhatsNext::EWnDiskSynced));
        }

        Y_UNIT_TEST(PrepareCommitDataDoesNotMixTrimmedChunksWithOldDiskSnapshot) {
            /*
             * Regression scenario for the crash sequence seen.
             *
             * SyncLog keeps recently written records in MemRecLog and periodically swaps full pages to DiskRecLog.
             * When DiskRecLog reaches the configured disk chunk limit, PrepareCommitData() may trim the oldest
             * disk chunks and put them into ChunksToDeleteDelayed: those chunks are no longer part of live
             * DiskRecLog, but they are still owned until all snapshots/readers release them and a later commit
             * deletes them from PDisk.
             *
             * The incident had two visible symptoms in the same area:
             *   - live sync read saw LogStartLsn == FirstDiskLsn while the first memory page started slightly
             *     before it, so WhatsNext() tripped over the old FirstDiskLsn <= FirstMemLsn invariant;
             *   - after restart, local recovery failed in TOneChunk::GetOwnedChunks() because the same chunk id
             *     was present twice in recovered SyncLog ownership.
             *
             * The dangerous interleaving this test captures is:
             *   1. PrepareCommitData() takes a SyncLog snapshot containing disk chunks [A, B].
             *   2. The same PrepareCommitData() call fixes disk overflow and moves A into ChunksToDeleteDelayed.
             *   3. The entry point must not be assembled from the old snapshot [A, B] and the new delayed list [A],
             *      because after restart recovery would first insert A from ChunksToDeleteDelayed and then insert
             *      A again from DiskRecLogSerialized, failing GetOwnedChunks().
             *
             * What we want to see: every chunk listed in ChunksToDeleteDelayed is absent from the
             * DiskRecLogSerialized field written to the same entry point.
             */
            const ui32 appendBlockSize = 4064;
            const ui32 pagesInChunk = 2;
            TSyncLogKeeperTestSettings settings;
            settings.SyncLogMaxMemAmount = appendBlockSize * 2;
            settings.SyncLogMaxDiskAmount = appendBlockSize * pagesInChunk * 2;
            settings.ChunkSize = appendBlockSize * pagesInChunk;
            settings.AppendBlockSize = appendBlockSize;

            std::unique_ptr<TSyncLogKeeperState> state = CreateSyncLogKeeperState(TEntryPointPair{TString(), 0}, settings);
            TPayloadWriter writer;
            TCommitWithSwapSimulator committer(1);
            ui64 lsn = 0;

            {
                WriteUntilMemPages(state.get(), writer, &lsn, 4);
                auto commitData = committer.Prepare(state.get(), lsn);
                UNIT_ASSERT(commitData.ChunksToDeleteDelayed.empty());
                committer.Finish(state.get(), std::move(commitData), ++lsn);
            }

            {
                WriteUntilMemPages(state.get(), writer, &lsn, 6);
                auto commitData = committer.Prepare(state.get(), lsn);
                UNIT_ASSERT(commitData.ChunksToDeleteDelayed.empty());
                committer.Finish(state.get(), std::move(commitData), ++lsn);
            }

            WriteUntilMemPages(state.get(), writer, &lsn, 6);
            auto commitData = committer.Prepare(state.get(), lsn);
            UNIT_ASSERT_C(!commitData.ChunksToDeleteDelayed.empty(),
                "test scenario must trigger delayed chunk deletion; commitData# " << commitData.ToString());
            const TString commitDataDebug = commitData.ToString();

            const TString entryPoint = committer.Serialize(std::move(commitData));
            const TSet<TChunkIdx> diskChunks = CollectSerializedDiskChunks(entryPoint);
            const TVector<ui32> delayedChunks = CollectSerializedChunksToDeleteDelayed(entryPoint);
            for (TChunkIdx delayedChunk : delayedChunks) {
                UNIT_ASSERT_C(diskChunks.find(delayedChunk) == diskChunks.end(),
                    "chunk is present both in DiskRecLogSerialized and ChunksToDeleteDelayed; "
                    << "chunk# " << delayedChunk
                    << " DiskChunks# " << FormatList(diskChunks)
                    << " ChunksToDeleteDelayed# " << FormatList(delayedChunks)
                    << " commitData# " << commitDataDebug);
            }
        }

        Y_UNIT_TEST(RecoveryEntryPointParserResolvesDiskChunkAlsoListedAsDelayedDelete) {
            /*
             * Compatibility test for already persisted entry points affected by the old PrepareCommitData()
             * interleaving.
             *
             * Such an entry point may list the same SyncLog chunk twice:
             *   - in DiskRecLogSerialized, because the entry point was serialized from an older disk snapshot;
             *   - in ChunksToDeleteDelayed, because the same commit already trimmed the chunk from live
             *     DiskRecLog and scheduled it for delayed deletion.
             *
             * During recovery this is not a cross-subsystem ownership conflict. The chunk still belongs to
             * SyncLog and must stay owned until delayed delete finishes. The entry point parser should therefore
             * log the duplicate and normalize the recovered SyncLog state in favor of ChunksToDeleteDelayed:
             * the chunk is removed from recovered DiskRecLog, but remains in the delayed-delete list. After
             * that GetOwnedChunks() should see a consistent state and must not need its own conflict resolver.
             *
             * After restart the delayed-delete list no longer has to wait for old in-memory snapshots: they died
             * with the previous process. The recovered keeper state should therefore immediately schedule a
             * delete-chunk commit for this chunk instead of leaving it owned until some unrelated SyncLog event
             * happens to trigger the next commit.
             */
            const ui32 appendBlockSize = 4064;
            const ui32 pagesInChunk = 2;
            TSyncLogKeeperTestSettings settings;
            settings.SyncLogMaxMemAmount = appendBlockSize * 2;
            settings.SyncLogMaxDiskAmount = appendBlockSize * pagesInChunk * 4;
            settings.ChunkSize = appendBlockSize * pagesInChunk;
            settings.AppendBlockSize = appendBlockSize;

            std::unique_ptr<TSyncLogKeeperState> state = CreateSyncLogKeeperState(TEntryPointPair{TString(), 0}, settings);
            TPayloadWriter writer;
            TCommitWithSwapSimulator committer(1);
            ui64 lsn = 0;

            WriteUntilMemPages(state.get(), writer, &lsn, 4);
            auto commitData = committer.Prepare(state.get(), lsn);
            UNIT_ASSERT_C(commitData.ChunksToDeleteDelayed.empty(),
                "initial entry point must not already contain delayed deletes; commitData# " << commitData.ToString());
            const TEntryPointPair entryPoint = committer.Finish(state.get(), std::move(commitData), ++lsn);

            const TSet<TChunkIdx> diskChunks = CollectSerializedDiskChunks(entryPoint.EntryPoint);
            UNIT_ASSERT_C(!diskChunks.empty(), "test scenario must create DiskRecLog chunks");

            const TChunkIdx duplicateChunk = *diskChunks.begin();
            NKikimrVDiskData::TSyncLogEntryPoint pb = ParseEntryPointProto(entryPoint.EntryPoint);
            pb.AddChunksToDeleteDelayed(duplicateChunk);
            const TString malformedEntryPoint = SerializeEntryPointProto(pb);

            std::unique_ptr<TSyncLogRepaired> repaired = ConstructRepairedFromEntryPoint(
                TEntryPointPair{malformedEntryPoint, entryPoint.EntryPointLsn},
                settings);

            TSet<TChunkIdx> diskChunksAfterRecovery;
            repaired->SyncLogPtr->GetOwnedChunks(diskChunksAfterRecovery);
            UNIT_ASSERT_C(diskChunksAfterRecovery.find(duplicateChunk) == diskChunksAfterRecovery.end(),
                "duplicate chunk must be removed from recovered DiskRecLog; "
                << "chunk# " << duplicateChunk
                << " DiskChunksAfterRecovery# " << FormatList(diskChunksAfterRecovery));
            UNIT_ASSERT_VALUES_EQUAL(diskChunksAfterRecovery.size() + 1, diskChunks.size());

            bool foundInDelayedDelete = false;
            for (TChunkIdx chunkIdx : repaired->ChunksToDelete) {
                if (chunkIdx == duplicateChunk) {
                    foundInDelayedDelete = true;
                }
            }
            UNIT_ASSERT_C(foundInDelayedDelete,
                "duplicate chunk must stay in ChunksToDeleteDelayed; chunk# " << duplicateChunk
                << " ChunksToDeleteDelayed# " << FormatList(repaired->ChunksToDelete));

            std::unique_ptr<TSyncLogKeeperState> recoveredState = CreateSyncLogKeeperState(
                TEntryPointPair{malformedEntryPoint, entryPoint.EntryPointLsn},
                settings);
            UNIT_ASSERT_C(recoveredState->PerformDeleteChunkAction(),
                "recovered ChunksToDeleteDelayed must immediately schedule delete commit after restart");
            TSyncLogKeeperCommitData deleteCommitData = recoveredState->PrepareCommitData(entryPoint.EntryPointLsn);
            UNIT_ASSERT_C(deleteCommitData.ChunksToDeleteDelayed.empty(),
                "recovered delayed deletes should be converted to immediate delete commit after restart; "
                << "commitData# " << deleteCommitData.ToString());
            UNIT_ASSERT_VALUES_EQUAL(deleteCommitData.ChunksToDelete.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(deleteCommitData.ChunksToDelete[0], duplicateChunk);

            TSyncLogRecovery recovery(std::move(repaired));
            TSet<TChunkIdx> ownedChunks;
            recovery.GetOwnedChunks(ownedChunks);
            UNIT_ASSERT_C(ownedChunks.find(duplicateChunk) != ownedChunks.end(),
                "duplicate chunk must remain owned through ChunksToDeleteDelayed; chunk# " << duplicateChunk);
            UNIT_ASSERT_VALUES_EQUAL(ownedChunks.size(), diskChunks.size());
        }

        Y_UNIT_TEST(CutLogUsesBoundedCheckpointsAndKeepsRetryingWhileAdvancing) {
            /*
             * Recovery can leave SyncLog with a huge MemRecLog tail and an empty DiskRecLog:
             *
             *   SyncLog LogStartLsn = old entry point
             *   TDiskRecLog        = empty
             *   TMemRecLog         = [old + 1, current] with millions of pages
             *
             * PDisk then asks the VDisk to cut the recovery log far ahead. The old code built one SwapSnap up
             * to FreeUpToLsn, so the SyncLog committer had to write the whole recovered memory tail before it
             * could write a new SyncLogIdx entry point. Until that entry point appeared, LogCutter still saw the
             * old SyncLog boundary and could not cut any log chunks.
             *
             * What we want instead is a sequence of small, crash-safe checkpoints:
             *   1. write a bounded prefix of MemRecLog to SyncLog disk chunks;
             *   2. commit a SyncLogIdx entry point for that prefix;
             *   3. report the advanced FirstLsnToKeep to LogCutter;
             *   4. retry the same FreeUpToLsn while the boundary keeps moving.
             *
             * This test keeps the batch size at two pages and requires four cut-log commits. The old retry limit
             * allowed only the initial commit plus two retries, so the fourth PerformCutLogAction() would return
             * false even though each previous commit advanced FirstLsnToKeep.
             */
            const ui32 appendBlockSize = 4064;
            const ui32 pagesInChunk = 4;
            TSyncLogKeeperTestSettings settings;
            settings.SyncLogMaxMemAmount = appendBlockSize * 2;
            settings.SyncLogMaxDiskAmount = appendBlockSize * pagesInChunk * 16;
            settings.ChunkSize = appendBlockSize * pagesInChunk;
            settings.AppendBlockSize = appendBlockSize;

            std::unique_ptr<TSyncLogKeeperState> state = CreateSyncLogKeeperState(TEntryPointPair{TString(), 0}, settings);
            TPayloadWriter writer;
            TCommitWithSwapSimulator committer(1);
            ui64 lsn = 1000;

            WriteUntilMemPages(state.get(), writer, &lsn, 9);

            const ui64 freeUpToLsn = lsn + 1;
            ui64 recoveryLogConfirmedLsn = freeUpToLsn;
            ui64 commitLsn = freeUpToLsn;
            ui64 previousFirstLsnToKeep = state->CalculateFirstLsnToKeep();
            state->CutLogEvent(freeUpToLsn);

            for (ui32 commitIdx = 0; commitIdx < 4; ++commitIdx) {
                const bool commit = state->PerformCutLogAction([](ui64) {});
                UNIT_ASSERT_C(commit, "bounded cut-log checkpoint must keep retrying while it advances; "
                    << "commitIdx# " << commitIdx
                    << " firstLsnToKeep# " << state->CalculateFirstLsnToKeep()
                    << " state# " << state->CalculateFirstLsnToKeepDecomposed());

                TSyncLogKeeperCommitData commitData = state->PrepareCommitData(recoveryLogConfirmedLsn);
                UNIT_ASSERT_C(commitData.SwapSnap && !commitData.SwapSnap->Empty(),
                    "cut-log checkpoint must write a bounded MemRecLog prefix; commitData# "
                    << commitData.ToString());
                UNIT_ASSERT_VALUES_EQUAL_C(commitData.SwapSnap->Size(), 2,
                    "cut-log checkpoint must be bounded by MaxMemPages; commitData# "
                    << commitData.ToString());

                committer.Finish(state.get(), std::move(commitData), ++commitLsn);

                const ui64 firstLsnToKeep = state->CalculateFirstLsnToKeep();
                UNIT_ASSERT_C(firstLsnToKeep > previousFirstLsnToKeep,
                    "each bounded checkpoint must advance SyncLog first lsn to keep; "
                    << "commitIdx# " << commitIdx
                    << " previousFirstLsnToKeep# " << previousFirstLsnToKeep
                    << " firstLsnToKeep# " << firstLsnToKeep
                    << " state# " << state->CalculateFirstLsnToKeepDecomposed());
                previousFirstLsnToKeep = firstLsnToKeep;

                if (!state->FreeUpToLsnSatisfied()) {
                    state->RetryCutLogEvent();
                }
            }
        }

        Y_UNIT_TEST(CutLogCheckpointAdvancesWithStaleRecoveryConfirmedLsn) {
            /*
             * A recovered VDisk may have no fresh user SyncLog confirmations: LsnMngr can still report an old
             * recoveryLogConfirmedLsn when PDisk asks SyncLog to cut a large recovered MemRecLog tail. Once the
             * SyncLog committer successfully writes a SyncLogIdx entry point, that entry point's own LSN proves
             * that all lower recovery-log records were written in order by RecoveryLogWriter. The checkpoint must
             * therefore advance FirstLsnToKeep immediately instead of waiting for an unrelated user write.
             */
            const ui32 appendBlockSize = 4064;
            const ui32 pagesInChunk = 4;
            TSyncLogKeeperTestSettings settings;
            settings.SyncLogMaxMemAmount = appendBlockSize * 100;
            settings.SyncLogMaxDiskAmount = appendBlockSize * pagesInChunk * 16;
            settings.ChunkSize = appendBlockSize * pagesInChunk;
            settings.AppendBlockSize = appendBlockSize;

            std::unique_ptr<TSyncLogKeeperState> state = CreateSyncLogKeeperState(TEntryPointPair{TString(), 0}, settings);
            TPayloadWriter writer;
            TCommitWithSwapSimulator committer(1);
            ui64 lsn = 1000;

            WriteUntilMemPages(state.get(), writer, &lsn, 6);

            const ui64 previousFirstLsnToKeep = state->CalculateFirstLsnToKeep();
            const ui64 staleRecoveryLogConfirmedLsn = previousFirstLsnToKeep;
            const ui64 freeUpToLsn = lsn + 1;
            state->CutLogEvent(freeUpToLsn);

            const bool commit = state->PerformCutLogAction([](ui64) {});
            UNIT_ASSERT(commit);

            TSyncLogKeeperCommitData commitData = state->PrepareCommitData(staleRecoveryLogConfirmedLsn);
            UNIT_ASSERT_C(commitData.SwapSnap && !commitData.SwapSnap->Empty(),
                "cut-log checkpoint must write recovered MemRecLog pages; commitData# " << commitData.ToString());

            committer.Finish(state.get(), std::move(commitData), freeUpToLsn + 100);

            UNIT_ASSERT_C(state->CalculateFirstLsnToKeep() > previousFirstLsnToKeep,
                "successful SyncLogIdx checkpoint must advance even with stale recoveryLogConfirmedLsn; "
                << "previousFirstLsnToKeep# " << previousFirstLsnToKeep
                << " state# " << state->CalculateFirstLsnToKeepDecomposed());
        }

        Y_UNIT_TEST(MemOverflowUsesBoundedCheckpointsAndKeepsRetrying) {
            /*
             * Production recovery exposed a second way to starve recovery-log cutting. The VDisk received
             * TEvCutLog while local recovery was finishing, delivered it after startup, and SyncLog had a huge
             * recovered MemRecLog tail. The first SyncLog action that actually built a SwapSnap was not marked
             * as wantToCutRecoveryLog; it was a plain mem-overflow checkpoint:
             *
             *   wantToCutRecoveryLog = false
             *   stillMemOverflow     = true
             *   maxSwapPages         = Max<ui32>()
             *
             * That single committer then tried to write hundreds of thousands of pages before producing a
             * SyncLogIdx entry point, so LogCutter kept seeing the old SyncLog boundary and wrote no HullCutLog.
             *
             * Mem-overflow checkpoints must be bounded exactly like cut-log checkpoints. After each successful
             * SyncLogIdx commit, the keeper should notice that recovered memory is still above the limit and
             * schedule the next small checkpoint, instead of launching one enormous swap or stopping after the
             * first small step.
             */
            const ui32 appendBlockSize = 4064;
            const ui32 pagesInChunk = 4;
            TSyncLogKeeperTestSettings settings;
            settings.SyncLogMaxMemAmount = appendBlockSize * 2;
            settings.SyncLogMaxDiskAmount = appendBlockSize * pagesInChunk * 16;
            settings.ChunkSize = appendBlockSize * pagesInChunk;
            settings.AppendBlockSize = appendBlockSize;

            std::unique_ptr<TSyncLogKeeperState> state = CreateSyncLogKeeperState(TEntryPointPair{TString(), 0}, settings);
            TPayloadWriter writer;
            TCommitWithSwapSimulator committer(1);
            ui64 lsn = 1000;

            WriteUntilMemPages(state.get(), writer, &lsn, 9);

            const bool firstMemOverflowCommit = state->PerformMemOverflowAction();
            UNIT_ASSERT(firstMemOverflowCommit);
            TSyncLogKeeperCommitData firstCommitData = state->PrepareCommitData(lsn);
            UNIT_ASSERT_C(firstCommitData.SwapSnap && !firstCommitData.SwapSnap->Empty(),
                "mem-overflow checkpoint must write recovered MemRecLog pages; commitData# "
                << firstCommitData.ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(firstCommitData.SwapSnap->Size(), 2,
                "mem-overflow checkpoint must be bounded by MaxMemPages; commitData# "
                << firstCommitData.ToString());

            committer.Finish(state.get(), std::move(firstCommitData), ++lsn);

            const bool secondMemOverflowCommit = state->PerformMemOverflowAction();
            UNIT_ASSERT_C(secondMemOverflowCommit,
                "keeper must schedule another bounded mem-overflow checkpoint while recovered memory "
                "is still above the limit; state# " << state->CalculateFirstLsnToKeepDecomposed());
            TSyncLogKeeperCommitData secondCommitData = state->PrepareCommitData(lsn);
            UNIT_ASSERT_C(secondCommitData.SwapSnap && !secondCommitData.SwapSnap->Empty(),
                "second mem-overflow checkpoint must continue writing recovered MemRecLog pages; commitData# "
                << secondCommitData.ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(secondCommitData.SwapSnap->Size(), 2,
                "second mem-overflow checkpoint must also be bounded; commitData# "
                << secondCommitData.ToString());
        }

        Y_UNIT_TEST(CutLogPrepareCommitDataDoesNotCreateBothLiveBoundaryAndSerializedOwnershipFailures) {
            /*
             * Bridge test for the two production VERIFY failures.
             *
             * The live WhatsNext() VERIFY does not literally cause the later GetOwnedChunks() VERIFY; a VERIFY
             * aborts the process. The suspected link is that one PrepareCommitData() call can expose both
             * failure modes at once:
             *
             *   - live SyncLog state after FixDiskOverflow() has Mem.first < Disk.first around LogStartLsn,
             *     which would trip the old FirstDiskLsn <= FirstMemLsn assertion in WhatsNext();
             *   - the entry point being serialized from the same commit data may still contain the trimmed
             *     chunk in DiskRecLogSerialized while also listing it in ChunksToDeleteDelayed, which later
             *     trips TOneChunk::GetOwnedChunks() during recovery.
             *
             * We intentionally do not call WhatsNext() here: on the broken code it aborts immediately and would
             * hide the serialized-entry-point half of the same scenario. The test first checks that the live
             * state has the old WhatsNext-failing shape, then runs recovery ownership collection over the same
             * entry point, then checks that the serialized entry point does not contain duplicate chunk
             * ownership.
             */
            const ui32 appendBlockSize = 4064;
            const ui32 pagesInChunk = 2;
            TSyncLogKeeperTestSettings settings;
            settings.SyncLogMaxMemAmount = appendBlockSize * 100;
            settings.SyncLogMaxDiskAmount = appendBlockSize * pagesInChunk * 2;
            settings.ChunkSize = appendBlockSize * pagesInChunk;
            settings.AppendBlockSize = appendBlockSize;

            std::unique_ptr<TSyncLogKeeperState> state = CreateSyncLogKeeperState(TEntryPointPair{TString(), 0}, settings);
            TPayloadWriter writer;
            TCommitWithSwapSimulator committer(1);
            ui64 lsn = 0;

            {
                WriteUntilMemPages(state.get(), writer, &lsn, 4);
                auto commitData = committer.PrepareCutLog(state.get(), lsn + 1, lsn);
                UNIT_ASSERT(commitData.ChunksToDeleteDelayed.empty());
                committer.Finish(state.get(), std::move(commitData), ++lsn);
            }

            WriteUntilMemPages(state.get(), writer, &lsn, 6);
            auto commitData = committer.PrepareCutLog(state.get(), lsn + 1, lsn);
            UNIT_ASSERT_C(commitData.SwapSnap && !commitData.SwapSnap->Empty(),
                "second cut-log commit must write the new disk tail; commitData# " << commitData.ToString());
            const TString commitDataDebug = commitData.ToString();
            UNIT_ASSERT_C(!commitData.ChunksToDeleteDelayed.empty(),
                "test scenario must trigger delayed chunk deletion; commitData# " << commitDataDebug);
            const TEntryPointPair entryPoint = committer.Finish(state.get(), std::move(commitData), ++lsn);

            TLogEssence e;
            state->FillInSyncLogEssence(&e);
            UNIT_ASSERT_C(!e.MemLogEmpty && !e.DiskLogEmpty, "e# " << LogEssenceToString(e));
            UNIT_ASSERT_C(e.FirstMemLsn < e.LogStartLsn, "e# " << LogEssenceToString(e));
            UNIT_ASSERT_C(e.FirstDiskLsn <= e.LogStartLsn, "e# " << LogEssenceToString(e));
            UNIT_ASSERT_C(e.LogStartLsn <= e.LastDiskLsn, "e# " << LogEssenceToString(e));
            UNIT_ASSERT_C(e.FirstMemLsn < e.FirstDiskLsn, "e# " << LogEssenceToString(e));

            const TString liveEssenceDebug = LogEssenceToString(e);
            const TSet<TChunkIdx> diskChunks = CollectSerializedDiskChunks(entryPoint.EntryPoint);
            const TVector<ui32> delayedChunks = CollectSerializedChunksToDeleteDelayed(entryPoint.EntryPoint);
            UNIT_ASSERT_C(!delayedChunks.empty(),
                "entry point must include delayed chunks from commit data; "
                << "LiveEssence# " << liveEssenceDebug
                << " DiskChunks# " << FormatList(diskChunks)
                << " commitData# " << commitDataDebug);

            TSet<TChunkIdx> expectedOwnedChunks = diskChunks;
            for (TChunkIdx delayedChunk : delayedChunks) {
                expectedOwnedChunks.insert(delayedChunk);
            }
            const TSet<TChunkIdx> ownedChunks = RecoverOwnedChunksFromEntryPoint(entryPoint, settings);
            UNIT_ASSERT_VALUES_EQUAL_C(ownedChunks.size(), expectedOwnedChunks.size(),
                "GetOwnedChunks must preserve SyncLog ownership after entry point parsing; "
                << "LiveEssence# " << liveEssenceDebug
                << " OwnedChunks# " << FormatList(ownedChunks)
                << " ExpectedOwnedChunks# " << FormatList(expectedOwnedChunks)
                << " DiskChunks# " << FormatList(diskChunks)
                << " ChunksToDeleteDelayed# " << FormatList(delayedChunks)
                << " commitData# " << commitDataDebug);
            for (TChunkIdx expectedChunk : expectedOwnedChunks) {
                UNIT_ASSERT_C(ownedChunks.find(expectedChunk) != ownedChunks.end(),
                    "GetOwnedChunks lost SyncLog ownership; "
                    << "chunk# " << expectedChunk
                    << " LiveEssence# " << liveEssenceDebug
                    << " OwnedChunks# " << FormatList(ownedChunks)
                    << " ExpectedOwnedChunks# " << FormatList(expectedOwnedChunks)
                    << " DiskChunks# " << FormatList(diskChunks)
                    << " ChunksToDeleteDelayed# " << FormatList(delayedChunks)
                    << " commitData# " << commitDataDebug);
            }

            for (TChunkIdx delayedChunk : delayedChunks) {
                UNIT_ASSERT_C(diskChunks.find(delayedChunk) == diskChunks.end(),
                    "same PrepareCommitData produced both failure modes; "
                    << "chunk# " << delayedChunk
                    << " LiveEssence# " << liveEssenceDebug
                    << " DiskChunks# " << FormatList(diskChunks)
                    << " ChunksToDeleteDelayed# " << FormatList(delayedChunks)
                    << " commitData# " << commitDataDebug);
            }
        }

    }

} // NKikimr
