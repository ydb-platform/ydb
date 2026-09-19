#include "dq_channel_test_harness.h"

// Spilling of the output side, remote (TOutputDescriptor) and local (TLocalBuffer): what does not fit
// in the inflight window goes to the channel storage and comes back in order when the window reopens,
// through the deferred load of the storage when it is not ready at once.

// One channel with a TMockChannelStorage on its output buffer, the window small enough to spill
struct TSpillTest : public TSessionTest {

    void Prepare() override {
        Limits.RemoteChannelInflightBytes = Window;
        Limits.RemoteChannelColdInflightBytes = ColdWindow;
        Limits.LocalChannelInflightBytes = Window;
        Limits.LocalChannelColdInflightBytes = ColdWindow;
        TSessionTest::Prepare();
    }

    void Init() override {
        TSessionTest::Init();
        Storage = MakeIntrusive<TMockChannelStorage>(StorageCapacity);
        ProducerSettings.StorageFactory = [this](ui32) { return IDqChannelStorage::TPtr(Storage); };
        ProducerSettings.PushOnSoftLimit = true;
    }

    // the output buffer of the single channel, remote or local
    std::shared_ptr<TOutputDescriptor> OutputDescriptor() {
        auto session = FindNodeState(Service0, Runtime->GetNodeId(1));
        if (!session) {
            return nullptr;
        }
        auto descriptors = GetOutputDescriptors(session);
        return descriptors.empty() ? nullptr : descriptors.front();
    }

    std::shared_ptr<TLocalBuffer> LocalBuffer() {
        auto& registry = Service0->LocalBufferRegistry;
        std::lock_guard lock(registry->Mutex);
        for (auto& [info, weak] : registry->LocalBuffers) {
            if (auto buffer = weak.lock()) {
                return buffer;
            }
        }
        return nullptr;
    }

    // the buffer is looked up once it exists and kept: a finished channel takes it off the registry
    // while the numbers of its last state are still what the test asserts
    bool Bind() {
        if (Local) {
            if (!Buffer) {
                Buffer = LocalBuffer();
            }
            return Buffer != nullptr;
        }
        if (!Descriptor) {
            Descriptor = OutputDescriptor();
        }
        return Descriptor != nullptr;
    }

    ui64 SpilledBytes() {
        return Local ? Buffer->SpilledBytes.load() : Descriptor->SpilledBytes.load();
    }

    EDqFillLevel FillLevel() {
        if (Local) {
            return Buffer->GetFillLevel();
        }
        std::lock_guard lock(Descriptor->FlowControlMutex);
        return Descriptor->FillLevel;
    }

    ui64 LoadingQueueSize() {
        if (Local) {
            std::lock_guard lock(Buffer->Mutex);
            return Buffer->LoadingQueue.size();
        }
        std::lock_guard lock(Descriptor->FlowControlMutex);
        return Descriptor->LoadingQueue.size();
    }

    std::pair<ui64, ui64> BlobIds() {
        if (Local) {
            std::lock_guard lock(Buffer->Mutex);
            return {Buffer->HeadBlobId, Buffer->TailBlobId};
        }
        std::lock_guard lock(Descriptor->FlowControlMutex);
        return {Descriptor->HeadBlobId, Descriptor->TailBlobId};
    }

    ui64 PushBytes() {
        return Local ? Buffer->PushStats.Bytes.load() : Descriptor->PushBytes.load();
    }

    TString Details() {
        if (!Bind()) {
            return "no output buffer";
        }
        auto [head, tail] = BlobIds();
        return TStringBuilder() << "SpilledBytes=" << SpilledBytes() << ", FillLevel=" << FillLevel()
            << ", LoadingQueue=" << LoadingQueueSize() << ", HeadBlobId=" << head << ", TailBlobId=" << tail
            << ", PushBytes=" << PushBytes() << ", storage: puts=" << Storage->GetPutCount() << ", gets=" << Storage->GetGetCount()
            << ", empty=" << Storage->IsEmpty() << ", full=" << Storage->IsFull() << ", " << ErrorDetails();
    }

    // the wake-up of a storage comes from an actor: the local buffer notifies through the activation context
    void WakeUp() {
        RunInActor(NodeIndex0, [storage = Storage]() { storage->WakeUp(); });
    }

    void WaitSpilled() {
        UNIT_ASSERT_C(WaitFor([&]() { return Bind() && SpilledBytes() > 0; }, TDuration::Seconds(10)),
            TStringBuilder() << "nothing was spilled, " << Details());
    }

    // everything spilled came back and the storage was read exactly as often as written
    void CheckDrained() {
        auto [head, tail] = BlobIds();
        UNIT_ASSERT_VALUES_EQUAL_C(SpilledBytes(), 0, Details());
        UNIT_ASSERT_VALUES_EQUAL_C(LoadingQueueSize(), 0, Details());
        UNIT_ASSERT_VALUES_EQUAL_C(head, tail, Details());
        UNIT_ASSERT_C(Storage->GetPutCount() > 0, Details());
        UNIT_ASSERT_VALUES_EQUAL_C(Storage->GetGetCount(), Storage->GetPutCount(), Details());
        UNIT_ASSERT_C(Storage->IsEmpty(), Details());
    }

    void Finish() {
        Buffer.reset();
        Descriptor.reset();
        CheckSensors();
        Destroy();
        CheckQuota();
    }

    ui64 Window = 64_KB;
    ui64 ColdWindow = 64_KB;
    ui64 StorageCapacity = 1ull << 30;
    TIntrusivePtr<TMockChannelStorage> Storage;
    std::shared_ptr<TOutputDescriptor> Descriptor;
    std::shared_ptr<TLocalBuffer> Buffer;
};

// The consumer stalls, the producer keeps going at SoftLimit past the window, into the storage; once
// the consumer drains, everything is reloaded in order and the storage is left empty
struct TSpillAndReloadTest : public TSpillTest {

    void Run() override {
        Prepare();
        Init();

        ProducerSettings.MessageCount = 200;
        ProducerSettings.MinMessageSize = ProducerSettings.MaxMessageSize = 10000;
        ProducerSettings.CheckpointEvery = CheckpointEvery;
        ProducerSettings.WatermarkEvery = WatermarkEvery;
        ConsumerSettings = TWorkerSettings{ .MessageCount = 200, .MinMessageSize = 10000, .MaxMessageSize = 10000,
            .PauseMessageIndex = 0, .PauseDelayMs = 1000, .CheckpointEvery = CheckpointEvery, .WatermarkEvery = WatermarkEvery };

        StartChannel(1, true);
        WaitSpilled();
        UNIT_ASSERT_VALUES_EQUAL_C(FillLevel(), EDqFillLevel::SoftLimit, Details());

        // the producer is not held by SoftLimit: all of it ends up spilled while the consumer sleeps
        UNIT_ASSERT_C(WaitFor([&]() { return Storage->GetPutCount() >= 190; }, TDuration::Seconds(10)),
            TStringBuilder() << "the producer stopped at SoftLimit, " << Details());

        WaitChannel([&]() { return Details(); });
        CheckDrained();
        Finish();
    }

    int CheckpointEvery = 0;
    int WatermarkEvery = 0;
};

// The storage answers the 1st loads with "not ready": those chunks queue up for the wake-up of the
// storage and nothing loaded after them may overtake them
struct TSpillAsyncLoadTest : public TSpillTest {

    void Run() override {
        Prepare();
        Init();

        ProducerSettings.MessageCount = 200;
        ProducerSettings.MinMessageSize = ProducerSettings.MaxMessageSize = 10000;
        ConsumerSettings = TWorkerSettings{ .MessageCount = 200, .MinMessageSize = 10000, .MaxMessageSize = 10000,
            .PauseMessageIndex = 0, .PauseDelayMs = 1000 };

        Storage->SetBlankGetRequests(3);
        StartChannel(1, true);
        WaitSpilled();

        UNIT_ASSERT_C(WaitFor([&]() { return LoadingQueueSize() > 0; }, TDuration::Seconds(10)),
            TStringBuilder() << "no deferred load, " << Details());
        // the blank answers are used up by now, the wake-up completes the deferred loads in order
        UNIT_ASSERT_C(WaitFor([&]() { WakeUp(); return LoadingQueueSize() == 0; }, TDuration::Seconds(10)),
            TStringBuilder() << "the wake-up did not drain the loading queue, " << Details());

        WaitChannel([&]() { return Details(); });
        CheckDrained();
        Finish();
    }
};

// A full storage is HardLimit for the producer; the wake-up of the storage, once it has room again,
// takes the level back to SoftLimit and resumes the producer
struct TStorageFullTest : public TSpillTest {

    void Run() override {
        Prepare();
        Init();

        ProducerSettings.MessageCount = 200;
        ProducerSettings.MinMessageSize = ProducerSettings.MaxMessageSize = 10000;
        ConsumerSettings = TWorkerSettings{ .MessageCount = 200, .MinMessageSize = 10000, .MaxMessageSize = 10000,
            .PauseMessageIndex = 0, .PauseDelayMs = 3000 };

        Storage->SetFull(true);
        StartChannel(1, true);
        WaitSpilled();
        UNIT_ASSERT_C(WaitFor([&]() { return FillLevel() == EDqFillLevel::HardLimit; }, TDuration::Seconds(5)),
            TStringBuilder() << "a full storage is not HardLimit, " << Details());

        // the producer holds still at HardLimit
        auto puts = Storage->GetPutCount();
        Sleep(TDuration::MilliSeconds(200));
        UNIT_ASSERT_VALUES_EQUAL_C(Storage->GetPutCount(), puts, TStringBuilder() << "the producer pushed at HardLimit, " << Details());
        UNIT_ASSERT_C(puts < 190, Details());

        Storage->SetFull(false);
        WakeUp();
        UNIT_ASSERT_C(WaitFor([&]() { return FillLevel() == EDqFillLevel::SoftLimit; }, TDuration::Seconds(5)),
            TStringBuilder() << "the wake-up did not lift HardLimit, " << Details());
        UNIT_ASSERT_C(WaitFor([&]() { return Storage->GetPutCount() >= 190; }, TDuration::Seconds(5)),
            TStringBuilder() << "the producer was not resumed, " << Details());

        WaitChannel([&]() { return Details(); });
        CheckDrained();
        Finish();
    }
};

// The consumer lets go of the channel while most of it is still in the storage: the data there is never
// read again, the control chunks - checkpoints and the finish - are
struct TSpillEarlyFinishTest : public TSpillTest {

    void Run() override {
        Prepare();
        Init();

        ProducerSettings.MessageCount = 200;
        ProducerSettings.MinMessageSize = ProducerSettings.MaxMessageSize = 10000;
        ProducerSettings.CheckpointEvery = 10;
        ProducerSettings.ExpectEarlyFinished = true;
        ConsumerSettings = TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 10000, .MaxMessageSize = 10000,
            .EarlyFinish = true, .PauseMessageIndex = 0, .PauseDelayMs = 1000, .CheckpointEvery = 10 };

        StartChannel(1, true);
        WaitSpilled();
        UNIT_ASSERT_C(WaitFor([&]() { return Storage->GetPutCount() >= 190; }, TDuration::Seconds(10)),
            TStringBuilder() << "the producer stopped at SoftLimit, " << Details());

        WaitChannel([&]() { return Details(); });
        if (!Local) {
            // 200 data chunks went in, a handful were read back before the early finish; the rest stays
            UNIT_ASSERT_C(Storage->GetPutCount() - Storage->GetGetCount() > 150, Details());
            UNIT_ASSERT_VALUES_EQUAL_C(SpilledBytes(), 0, Details());
            UNIT_ASSERT_VALUES_EQUAL_C(LoadingQueueSize(), 0, Details());
        }
        Finish();
    }
};

// The consumer early-finishes with a checkpoint after the finish in the storage and the storage slow to
// load: the confirmation of the finish is pushed while that checkpoint is still loading
struct TSpillEarlyFinishConfirmTest : public TSpillTest {

    void Run() override {
        Prepare();
        Init();

        ProducerSettings.MessageCount = 200;
        ProducerSettings.MinMessageSize = ProducerSettings.MaxMessageSize = 10000;
        ProducerSettings.CheckpointAfterFinish = true;
        ProducerSettings.ExpectEarlyFinished = true;
        ConsumerSettings = TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 10000, .MaxMessageSize = 10000,
            .EarlyFinish = true, .PauseMessageIndex = 0, .PauseDelayMs = 1000 };

        StartChannel(1, true);
        WaitSpilled();
        // the last blob in is the checkpoint after the finish, pushed right behind it; it is the one
        // which is slow to load
        UNIT_ASSERT_C(WaitFor([&]() { return Bind() && Descriptor->FinishPushed.load(); }, TDuration::Seconds(10)),
            TStringBuilder() << "the producer stopped at SoftLimit, " << Details());
        Sleep(TDuration::MilliSeconds(200));
        Storage->SetStuckBlob(Storage->GetLastPutBlobId());
        UNIT_ASSERT_VALUES_EQUAL_C(Storage->GetLastPutBlobId(), BlobIds().first, Details());

        // the early finish reads the finish back and the peer confirms it while the checkpoint still loads
        UNIT_ASSERT_C(WaitFor([&]() { return Bind() && Descriptor->EarlyFinished.load() && Descriptor->Finished.load(); }, TDuration::Seconds(10)),
            TStringBuilder() << "the finish did not come back, " << Details());
        UNIT_ASSERT_C(LoadingQueueSize() > 0, TStringBuilder() << "the checkpoint is not loading, " << Details());
        Storage->SetStuckBlob(std::nullopt);
        UNIT_ASSERT_C(WaitFor([&]() { WakeUp(); return LoadingQueueSize() == 0; }, TDuration::Seconds(10)),
            TStringBuilder() << "the wake-up did not drain the loading queue, " << Details());

        WaitChannel([&]() { return Details(); });
        Finish();
    }
};

// Node level memory pressure at the receiver shrinks the window to the cold one; with a storage the
// producer spills past it instead of stalling at HardLimit
struct TSpillUnderPressureTest : public TSpillTest {

    void Prepare() override {
        Limits.EnableSpillingChannelBackpressure = true;
        // the cold window is what throttles, keep the warm one wide
        Window = 16_MB;
        TSpillTest::Prepare();
    }

    void Run() override {
        Prepare();
        Init();

        ProducerSettings.MessageCount = 200;
        ProducerSettings.MinMessageSize = ProducerSettings.MaxMessageSize = 10000;
        ConsumerSettings = TWorkerSettings{ .MessageCount = 200, .MinMessageSize = 10000, .MaxMessageSize = 10000,
            .PauseMessageIndex = 5, .PauseDelayMs = 1000 };

        InputQuotaManager->MemoryPressure = true;
        StartChannel(1, true);
        WaitSpilled();
        UNIT_ASSERT_C(WaitFor([&]() { return Descriptor->PeerMemoryPressure.load(); }, TDuration::Seconds(5)),
            TStringBuilder() << "the pressure did not reach the sender, " << Details());
        UNIT_ASSERT_C(FillLevel() != EDqFillLevel::HardLimit, Details());
        UNIT_ASSERT_C(WaitFor([&]() { return Storage->GetPutCount() >= 190; }, TDuration::Seconds(10)),
            TStringBuilder() << "the producer stopped at SoftLimit, " << Details());

        InputQuotaManager->MemoryPressure = false;
        WaitChannel([&]() { return Details(); });
        CheckDrained();
        Finish();
    }
};

// Spilled chunks are off the quota: a limit which holds the window does not reject anything
struct TSpillQuotaTest : public TSpillTest {

    void Run() override {
        Prepare();
        Init();

        OutputQuotaManager->Limit = 256_KB;
        ProducerSettings.MessageCount = 200;
        ProducerSettings.MinMessageSize = ProducerSettings.MaxMessageSize = 10000;
        ConsumerSettings = TWorkerSettings{ .MessageCount = 200, .MinMessageSize = 10000, .MaxMessageSize = 10000,
            .PauseMessageIndex = 0, .PauseDelayMs = 1000 };

        StartChannel(1, true);
        WaitSpilled();
        UNIT_ASSERT_C(WaitFor([&]() { return Storage->GetPutCount() >= 190; }, TDuration::Seconds(10)),
            TStringBuilder() << "the producer stopped at SoftLimit, " << Details());
        UNIT_ASSERT_VALUES_EQUAL_C(OutputQuotaManager->Rejected.load(), 0, Details());
        UNIT_ASSERT_LE_C(OutputQuotaManager->Quota.load(), static_cast<i64>(256_KB), Details());

        WaitChannel([&]() { return Details(); });
        UNIT_ASSERT_VALUES_EQUAL_C(OutputQuotaManager->Rejected.load(), 0, Details());
        CheckDrained();
        Finish();
    }
};

Y_UNIT_TEST_SUITE(Channels20Spilling) {

    Y_UNIT_TEST(RemoteSpillAndReload2n) {
        TSpillAndReloadTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(LocalSpillAndReload1n) {
        TSpillAndReloadTest test;
        test.Local = true;
        test.Run();
    }

    Y_UNIT_TEST(SpillAsyncLoadPreservesOrder2n) {
        TSpillAsyncLoadTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(SpillAsyncLoadPreservesOrder1n) {
        TSpillAsyncLoadTest test;
        test.Local = true;
        test.Run();
    }

    Y_UNIT_TEST(StorageFullHardLimit2n) {
        TStorageFullTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(StorageFullHardLimit1n) {
        TStorageFullTest test;
        test.Local = true;
        test.Run();
    }

    // the control chunks go through the storage with their data, DataToBuffer / BufferToData
    Y_UNIT_TEST(SpilledCheckpointAndWatermark2n) {
        TSpillAndReloadTest test;
        test.Local = false;
        test.CheckpointEvery = 10;
        test.WatermarkEvery = 7;
        test.Run();
    }

    Y_UNIT_TEST(SpilledCheckpointAndWatermark1n) {
        TSpillAndReloadTest test;
        test.Local = true;
        test.CheckpointEvery = 10;
        test.WatermarkEvery = 7;
        test.Run();
    }

    // the finish of the early finish must not queue behind the spilled backlog nobody reads
    Y_UNIT_TEST(SpillThenEarlyFinish2n) {
        TSpillEarlyFinishTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(SpillThenEarlyFinishConfirm2n) {
        TSpillEarlyFinishConfirmTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(SpillThenEarlyFinish1n) {
        TSpillEarlyFinishTest test;
        test.Local = true;
        test.Run();
    }

    Y_UNIT_TEST(SpillUnderMemoryPressure2n) {
        TSpillUnderPressureTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(SpillWithQuotaLimit2n) {
        TSpillQuotaTest test;
        test.Local = false;
        test.Run();
    }
}
