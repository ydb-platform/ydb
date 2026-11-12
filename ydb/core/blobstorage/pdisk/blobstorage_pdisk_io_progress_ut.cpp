#include "blobstorage_pdisk_io_progress.h"
#include "blobstorage_pdisk_internal_interface.h"
#include "blobstorage_pdisk_ut_env.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPDisk {

namespace {

class TBackendReadGate {
    struct TGate {
        TManualEvent Entered;
        TManualEvent Resume;
    };

    TActorTestContext& TestCtx;
    TPDisk* const PDisk;
    const std::shared_ptr<TGate> Gate = std::make_shared<TGate>();

public:
    TBackendReadGate(TActorTestContext& testCtx, const TVDiskMock& vdisk, TChunkIdx chunk)
        : TestCtx(testCtx)
        , PDisk(testCtx.GetPDisk())
    {
        TestCtx.TestCtx.SectorMap->SetReadCallback([gate = Gate] {
            gate->Entered.Signal();
            gate->Resume.WaitI();
        });
        TestCtx.Send(new TEvChunkReadRaw(vdisk.PDiskParams->Owner,
            vdisk.PDiskParams->OwnerRound, chunk, 0, 4096));
    }

    ~TBackendReadGate() {
        Gate->Resume.Signal();
        TestCtx.TestCtx.SectorMap->SetReadCallback({});
    }

    bool Wait() {
        return Gate->Entered.WaitT(TDuration::Seconds(10));
    }

    auto Query(ui64 cookie) {
        TestCtx.Send(new TEvIoProgressRequest, cookie);
        auto reply = TestCtx.GetRuntime()->GrabEdgeEventRethrow<TEvIoProgressResult>(
            TestCtx.Sender, TDuration::Seconds(2));
        UNIT_ASSERT_C(reply, "PDisk actor did not answer a local progress query");
        UNIT_ASSERT_VALUES_EQUAL(reply->Cookie, cookie);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Incarnation, reply->Sender);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->PDiskId, PDisk->PCtx->PDiskId);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Guid, TestCtx.TestCtx.PDiskGuid);
        return reply;
    }

    void Release() {
        Gate->Resume.Signal();
        TestCtx.TestResponse<TEvChunkReadRawResult>(nullptr, NKikimrProto::OK);
        TestCtx.TestCtx.SectorMap->SetReadCallback({});
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TPDiskIoProgress) {
    using NActors::TMonotonic;

    Y_UNIT_TEST(BusyAgeCrossesTenSecondsDespiteNewAdmissions) {
        TIoProgressTracker tracker;
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(1), 10, 9).value().NoCompletionMs, 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::MilliSeconds(10999), 20, 9).value().NoCompletionMs, 9999);
        const auto sample = tracker.Update(TMonotonic::Seconds(11), 30, 9);
        UNIT_ASSERT_VALUES_EQUAL(sample.value().PendingOperations, 21);
        UNIT_ASSERT_VALUES_EQUAL(sample.value().NoCompletionMs, 10000);
    }

    Y_UNIT_TEST(AnyRetirementResetsAgeWithWorkStillPending) {
        TIoProgressTracker tracker;
        tracker.Update(TMonotonic::Seconds(1), 10, 9);
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(10), 20, 9).value().NoCompletionMs, 9000);
        for (ui64 seconds = 11; seconds != 31; ++seconds) {
            const auto sample = tracker.Update(TMonotonic::Seconds(seconds), seconds + 10, seconds);
            UNIT_ASSERT_GT(sample.value().PendingOperations, 0);
            UNIT_ASSERT_VALUES_EQUAL(sample.value().NoCompletionMs, 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(35), 40, 30).value().NoCompletionMs, 5000);
    }

    Y_UNIT_TEST(IdleAndDrainDoNotAccumulateAge) {
        TIoProgressTracker tracker;
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(1), 10, 10).value().NoCompletionMs, 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(100), 10, 10).value().NoCompletionMs, 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(101), 11, 10).value().NoCompletionMs, 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(111), 11, 10).value().NoCompletionMs, 10000);
        const auto drained = tracker.Update(TMonotonic::Seconds(112), 11, 11);
        UNIT_ASSERT_VALUES_EQUAL(drained.value().PendingOperations, 0);
        UNIT_ASSERT_VALUES_EQUAL(drained.value().NoCompletionMs, 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(200), 12, 11).value().NoCompletionMs, 0);
    }

    Y_UNIT_TEST(RestartAndCounterRollbackDoNotReuseAge) {
        TIoProgressTracker tracker;
        tracker.Update(TMonotonic::Seconds(1), 10, 9);
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(11), 10, 9).value().NoCompletionMs, 10000);
        tracker.Reset();
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(20), 10, 9).value().NoCompletionMs, 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(30), 10, 9).value().NoCompletionMs, 10000);
        UNIT_ASSERT(!tracker.Update(TMonotonic::Seconds(31), 9, 8));
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(32), 9, 8).value().NoCompletionMs, 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(42), 9, 8).value().NoCompletionMs, 10000);
        UNIT_ASSERT(!tracker.Update(TMonotonic::Seconds(43), 8, 8));
        const auto idle = tracker.Update(TMonotonic::Seconds(44), 8, 8);
        UNIT_ASSERT(idle);
        UNIT_ASSERT_VALUES_EQUAL(idle->PendingOperations, 0);
        UNIT_ASSERT_VALUES_EQUAL(idle->NoCompletionMs, 0);
    }

    Y_UNIT_TEST(ImpossibleCountersAndClockRollbackDoNotReuseAge) {
        TIoProgressTracker tracker;
        tracker.Update(TMonotonic::Seconds(1), 10, 9);
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(11), 10, 9).value().NoCompletionMs, 10000);
        UNIT_ASSERT(!tracker.Update(TMonotonic::Seconds(12), 9, 10));
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(20), 11, 10).value().NoCompletionMs, 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(30), 11, 10).value().NoCompletionMs, 10000);
        UNIT_ASSERT(!tracker.Update(TMonotonic::Seconds(25), 11, 10));
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(26), 11, 10).value().NoCompletionMs, 0);
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(36), 11, 10).value().NoCompletionMs, 10000);
        UNIT_ASSERT(!tracker.Update(TMonotonic::Seconds(37), -1, 0));
        UNIT_ASSERT(!tracker.Update(TMonotonic::Seconds(38), 0, -1));
        UNIT_ASSERT_VALUES_EQUAL(tracker.Update(TMonotonic::Seconds(40), 11, 10).value().NoCompletionMs, 0);
    }

    Y_UNIT_TEST(ActorAnswersWhileBackendReadAndWorkerMutexAreHeld) {
        TActorTestContext::TSettings settings;
        settings.PlainDataChunks = true;
        settings.SmallDisk = true;
        settings.DiskSize = ui64{16} << 30;
        TActorTestContext testCtx(settings);
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.ReserveChunk();
        const auto chunk = *vdisk.Chunks[EChunkState::RESERVED].begin();
        auto* pdisk = testCtx.GetPDisk();

        TBackendReadGate gate(testCtx, vdisk, chunk);
        UNIT_ASSERT_C(gate.Wait(), "raw read did not reach the backend");
        {
            TGuard<TMutex> guard(pdisk->StateMutex);
            const auto reply = gate.Query(12345);
            UNIT_ASSERT(reply->Get()->Ready);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->CurrentState, NKikimrBlobStorage::TPDiskState::Normal);
            UNIT_ASSERT_GT(reply->Get()->PendingOperations, 0);
        }
        gate.Release();

        testCtx.TestResponse<TEvYardControlResult>(
            new TEvYardControl(TEvYardControl::PDiskStop, nullptr), NKikimrProto::OK);
        const auto stopped = gate.Query(54321);
        UNIT_ASSERT(!stopped->Get()->Ready);
        UNIT_ASSERT_VALUES_EQUAL(stopped->Get()->CurrentState, NKikimrBlobStorage::TPDiskState::Stopped);
        UNIT_ASSERT_VALUES_EQUAL(stopped->Get()->PendingOperations, 0);
        UNIT_ASSERT_VALUES_EQUAL(stopped->Get()->NoCompletionMs, 0);

        // The worker is stopped, so this explicit actor state cannot depend on
        // a queued whiteboard report reaching it and returning.
        testCtx.Send(new TEvDeviceError("injected terminal state for progress query"));
        const auto failed = gate.Query(67890);
        UNIT_ASSERT(!failed->Get()->Ready);
        UNIT_ASSERT_VALUES_EQUAL(failed->Get()->CurrentState, NKikimrBlobStorage::TPDiskState::DeviceIoError);
        UNIT_ASSERT_VALUES_EQUAL(failed->Get()->PendingOperations, 0);
        UNIT_ASSERT_VALUES_EQUAL(failed->Get()->NoCompletionMs, 0);
    }

    Y_UNIT_TEST(MetadataReadFailsPromptlyDuringIoStallAndRecoversWithQueuedWrite) {
        TActorTestContext::TSettings settings;
        settings.PlainDataChunks = true;
        settings.SmallDisk = true;
        settings.DiskSize = ui64{16} << 30;
        TActorTestContext testCtx(settings);
        TVDiskMock vdisk(&testCtx);
        vdisk.InitFull();
        vdisk.ReserveChunk();
        const auto chunk = *vdisk.Chunks[EChunkState::RESERVED].begin();
        auto* pdisk = testCtx.GetPDisk();

        const TString metadata = "known metadata before I/O stall";
        testCtx.Send(new TEvWriteMetadata(TRcBuf(metadata)));
        const auto seeded = testCtx.Recv<TEvWriteMetadataResult>();
        UNIT_ASSERT_VALUES_EQUAL(seeded->Outcome, EPDiskMetadataOutcome::OK);
        testCtx.Send(new TEvReadMetadata);
        const auto before = testCtx.Recv<TEvReadMetadataResult>();
        UNIT_ASSERT_VALUES_EQUAL(before->Outcome, EPDiskMetadataOutcome::OK);
        UNIT_ASSERT_VALUES_EQUAL(TRcBuf(before->Metadata).ExtractUnderlyingContainerOrCopy<TString>(), metadata);

        TBackendReadGate gate(testCtx, vdisk, chunk);
        UNIT_ASSERT_C(gate.Wait(), "raw read did not reach the backend");
        const TString updated = "known metadata from the queued write";
        {
            TGuard<TMutex> guard(pdisk->StateMutex);
            const auto initial = gate.Query(99);
            UNIT_ASSERT(initial->Get()->Ready);
            UNIT_ASSERT_LT(initial->Get()->NoCompletionMs, 10'000);
            // An earlier read is already queued before the stall threshold.
            // It must retain its original successful outcome after recovery.
            testCtx.Send(new TEvReadMetadata);

            const auto deadline = TMonotonic::Now() + TDuration::Seconds(15);
            while (true) {
                const auto progress = gate.Query(100);
                UNIT_ASSERT(progress->Get()->Ready);
                UNIT_ASSERT_VALUES_EQUAL(progress->Get()->CurrentState, NKikimrBlobStorage::TPDiskState::Normal);
                UNIT_ASSERT_GT(progress->Get()->PendingOperations, 0);
                // Measure the real live signal; do not manufacture counter or clock state.
                if (progress->Get()->NoCompletionMs >= 10'000) {
                    break;
                }
                UNIT_ASSERT_C(TMonotonic::Now() < deadline, "backend did not become service-stalled");
                Sleep(TDuration::MilliSeconds(25));
            }

            // The write remains queued with an unresolved outcome. Rejecting a new
            // read must neither reject nor cancel that already admitted write.
            testCtx.Send(new TEvWriteMetadata(TRcBuf(updated)));
            testCtx.Send(new TEvReadMetadata, 123456);
            const auto failed = testCtx.GetRuntime()->GrabEdgeEventRethrow<TEvReadMetadataResult>(
                testCtx.Sender, TDuration::Seconds(2));
            UNIT_ASSERT_C(failed, "PDisk actor did not answer a metadata read during an I/O service stall");
            UNIT_ASSERT_VALUES_EQUAL(failed->Cookie, 123456);
            UNIT_ASSERT_VALUES_EQUAL(failed->Get()->Outcome, EPDiskMetadataOutcome::ERROR);
            UNIT_ASSERT(failed->Get()->PDiskGuid);
            UNIT_ASSERT_VALUES_EQUAL(*failed->Get()->PDiskGuid, testCtx.TestCtx.PDiskGuid);
            UNIT_ASSERT_VALUES_EQUAL(failed->Get()->Metadata.size(), 0);
            const auto progress = gate.Query(101);
            UNIT_ASSERT(progress->Get()->Ready);
            UNIT_ASSERT_VALUES_EQUAL(progress->Get()->CurrentState, NKikimrBlobStorage::TPDiskState::Normal);
        }

        gate.Release();
        const auto resumed = testCtx.Recv<TEvReadMetadataResult>();
        UNIT_ASSERT_VALUES_EQUAL(resumed->Outcome, EPDiskMetadataOutcome::OK);
        UNIT_ASSERT(resumed->PDiskGuid);
        UNIT_ASSERT_VALUES_EQUAL(*resumed->PDiskGuid, testCtx.TestCtx.PDiskGuid);
        UNIT_ASSERT_VALUES_EQUAL(TRcBuf(resumed->Metadata).ExtractUnderlyingContainerOrCopy<TString>(), metadata);
        const auto written = testCtx.Recv<TEvWriteMetadataResult>();
        UNIT_ASSERT_VALUES_EQUAL(written->Outcome, EPDiskMetadataOutcome::OK);
        UNIT_ASSERT(written->PDiskGuid);
        UNIT_ASSERT_VALUES_EQUAL(*written->PDiskGuid, testCtx.TestCtx.PDiskGuid);
        testCtx.Send(new TEvReadMetadata);
        const auto recovered = testCtx.Recv<TEvReadMetadataResult>();
        UNIT_ASSERT_VALUES_EQUAL(recovered->Outcome, EPDiskMetadataOutcome::OK);
        UNIT_ASSERT(recovered->PDiskGuid);
        UNIT_ASSERT_VALUES_EQUAL(*recovered->PDiskGuid, testCtx.TestCtx.PDiskGuid);
        UNIT_ASSERT_VALUES_EQUAL(TRcBuf(recovered->Metadata).ExtractUnderlyingContainerOrCopy<TString>(), updated);
        const auto progress = gate.Query(102);
        UNIT_ASSERT(progress->Get()->Ready);
        UNIT_ASSERT_VALUES_EQUAL(progress->Get()->CurrentState, NKikimrBlobStorage::TPDiskState::Normal);
        UNIT_ASSERT_LT(progress->Get()->NoCompletionMs, 10'000);
    }
}

} // namespace NKikimr::NPDisk
