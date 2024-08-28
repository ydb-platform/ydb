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

    void TSyncLogKeeperTest::CreateState(TEntryPointPair ep) {
        TBlobStorageGroupInfo groupInfo(TBlobStorageGroupType::ErasureMirror3, 2, 4);
        TIntrusivePtr<TVDiskContext> vctx = new TVDiskContext(
                TActorId(),
                groupInfo.PickTopology(),
                new ::NMonitoring::TDynamicCounters(),
                TVDiskID(),
                nullptr,
                NPDisk::DEVICE_TYPE_UNKNOWN);

        const ui64 pdiskGuid = 19283489374;
        const ui32 chunkSize = 512u << 10u;
        const ui32 appendBlockSize = 4064;
        const ui32 syncLogAdvisedIndexedBlockSize = ui32(1) << ui32(20);
        const ui64 syncLogMaxEntryPointSize = ui64(128) << ui64(10);

        // Create SyncLogRecovery
        NSyncLog::TSyncLogParams params = {
            pdiskGuid,
            chunkSize,
            appendBlockSize,
            syncLogAdvisedIndexedBlockSize,
            vctx->SyncLogCache
        };
        TString explanation;
        auto r = TSyncLogRepaired::Construct(std::move(params), ep.EntryPoint, ep.EntryPointLsn, explanation);
        Y_ABORT_UNLESS(r);
        std::unique_ptr<NSyncLog::TSyncLogRecovery> recovery = std::make_unique<NSyncLog::TSyncLogRecovery>(std::move(r));
        const ui64 lastLsnOfIndexRecord = recovery->GetLastLsnOfIndexRecord();
        std::unique_ptr<TSyncLogRepaired> repaired = recovery->ReleaseRepaired();

        const ui64 syncLogMaxMemAmount = ui64(64) << ui64(20);
        const ui64 syncLogMaxDiskAmount = 0;

        State = std::make_unique<TSyncLogKeeperState>(vctx, std::move(repaired), syncLogMaxMemAmount, syncLogMaxDiskAmount,
                syncLogMaxEntryPointSize);
        State->Init(nullptr, std::make_shared<TFakeLoggerCtx>());

        STR << "CREATE STATE entryPointLsn# " << ep.EntryPointLsn <<
            " entryPoint# " << (ep.EntryPoint.empty() ? "<empty>" : "<exists>") << "\n";
        STR << "    GetLastLsnOfIndexRecord# " << lastLsnOfIndexRecord << "\n";
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

            TCommitHistory commitHistory(TInstant(), commitLsn, CommitData->RecoveryLogConfirmedLsn);
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

        Y_UNIT_TEST(CutLog_EntryPointNewFormat) {
            TSyncLogKeeperTest test;
            test.Run();
        }

    }

} // NKikimr
