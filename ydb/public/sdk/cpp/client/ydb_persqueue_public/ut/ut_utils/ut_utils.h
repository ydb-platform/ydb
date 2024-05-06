#pragma once

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/test_utils.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/sdk_test_setup.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/include/aliases.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/impl/write_session.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/common/executor_impl.h>

using namespace NKikimr;
using namespace NKikimr::NPersQueueTests;

using NYdb::NTopic::IAsyncExecutor;

namespace NYdb::NPersQueue::NTests {

class TPersQueueYdbSdkTestSetup : public ::NPersQueue::SDKTestSetup {
    THolder<NYdb::TDriver> Driver;
    THolder<NYdb::NPersQueue::TPersQueueClient> PersQueueClient;

    TAdaptiveLock Lock;
public:
    TPersQueueYdbSdkTestSetup(const TString& testCaseName, bool start = true,
                              const TVector<NKikimrServices::EServiceKikimr>& logServices = ::NPersQueue::TTestServer::LOGGED_SERVICES,
                              NActors::NLog::EPriority logPriority = NActors::NLog::PRI_DEBUG,
                              ui32 nodeCount = NKikimr::NPersQueueTests::PQ_DEFAULT_NODE_COUNT,
                              size_t topicPartitionsCount = 1)
        : SDKTestSetup(testCaseName, start, logServices, logPriority, nodeCount, topicPartitionsCount)
    {
    }

    ~TPersQueueYdbSdkTestSetup() {
        if (PersQueueClient) {
            PersQueueClient = nullptr;
        }

        if (Driver) {
            Driver->Stop(true);
            Driver = nullptr;
        }
    }

    NYdb::TDriver& GetDriver() {
        if (!Driver) {
            NYdb::TDriverConfig cfg;
            cfg.SetEndpoint(TStringBuilder() << "localhost:" << Server.GrpcPort);
            cfg.SetDatabase("/Root");
            cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
            Driver = MakeHolder<NYdb::TDriver>(cfg);
        }
        return *Driver;
    }

    NYdb::NPersQueue::TPersQueueClient& GetPersQueueClient() {
        with_lock(Lock) {
            if (!PersQueueClient) {
                PersQueueClient = MakeHolder<NYdb::NPersQueue::TPersQueueClient>(GetDriver());
            }
            return *PersQueueClient;
        }
    }

    NYdb::NPersQueue::TReadSessionSettings GetReadSessionSettings() {
        NYdb::NPersQueue::TReadSessionSettings settings;
        settings
                .ConsumerName(GetTestConsumer())
                .AppendTopics(GetTestTopic());
        return settings;
    }

    NYdb::NPersQueue::TWriteSessionSettings GetWriteSessionSettings() {
        TWriteSessionSettings settings;
        settings
                .Path(GetTestTopic())
                .MessageGroupId(GetTestMessageGroupId())
                .ClusterDiscoveryMode(EClusterDiscoveryMode::On);
        return settings;
    }
};

struct TYDBClientEventLoop : public ::NPersQueue::IClientEventLoop {
public:
    std::shared_ptr<TPersQueueYdbSdkTestSetup> Setup;
    using TAcksCallback = std::function<void (const TVector<ui64>&)>;

    TYDBClientEventLoop(
            std::shared_ptr<TPersQueueYdbSdkTestSetup> setup,
            IRetryPolicy::TPtr retryPolicy = nullptr,
            IExecutor::TPtr compressExecutor = nullptr,
            const TString& preferredCluster = TString(),
            const TString& sourceId = TString(),
            bool autoSeqNo = false
    )
        : IClientEventLoop()
        , Setup(setup)
        , AutoSeqNo(autoSeqNo)
    {
        Log = Setup->GetLog();
        Thread = std::make_unique<TThread>([setup, retryPolicy, compressExecutor, preferredCluster, sourceId, this]() {
            auto writerConfig = Setup->GetWriteSessionSettings();
            writerConfig.MaxMemoryUsage(100_MB);
            if (!sourceId.empty()) {
                writerConfig.MessageGroupId(sourceId);
            }
            if (retryPolicy != nullptr)
                writerConfig.RetryPolicy(retryPolicy);
            if (compressExecutor != nullptr)
                writerConfig.CompressionExecutor(compressExecutor);
            if (preferredCluster)
                writerConfig.PreferredCluster(preferredCluster);
            auto writer = setup->GetPersQueueClient().CreateWriteSession(writerConfig);

            TMaybe<TContinuationToken> continueToken;
            NThreading::TFuture<void> waitEventFuture = writer->WaitEvent();
            THashMap<ui64, NThreading::TPromise<::NPersQueue::TWriteResult>> ackPromiseBySequenceNumber;
            TDeque<NThreading::TPromise<::NPersQueue::TWriteResult>> ackPromiseQueue;
            while (!MustStop) {
                if (!continueToken) {
                    Log << TLOG_INFO << "Wait for writer event";
                    waitEventFuture.Wait();
                }

                bool closed = false;
                while (waitEventFuture.HasValue() && !closed) {
                    TWriteSessionEvent::TEvent event = *writer->GetEvent(true);
                    waitEventFuture = writer->WaitEvent();
                    std::visit(TOverloaded {
                            [&](const TWriteSessionEvent::TAcksEvent& event) {
                                for (const auto& ack : event.Acks) {
                                    if (AutoSeqNo) {
                                        UNIT_ASSERT(!ackPromiseQueue.empty());
                                        ackPromiseQueue.front().SetValue({true, false});
                                        ackPromiseQueue.pop_front();
                                    } else {
                                        UNIT_ASSERT(ackPromiseBySequenceNumber.contains(ack.SeqNo));
                                        ackPromiseBySequenceNumber[ack.SeqNo].SetValue({true, false});
                                        ackPromiseBySequenceNumber.erase(ack.SeqNo);
                                    }
                                }
                            },
                            [&](TWriteSessionEvent::TReadyToAcceptEvent& event) {
                                Log << TLOG_INFO << "Got new continue token";
                                continueToken = std::move(event.ContinuationToken);
                            },
                            [&](const TSessionClosedEvent& event) {
                                Log << TLOG_INFO << "Got close event: " << event.DebugString() << Endl;
                                if (!MayStop) {
                                    UNIT_ASSERT(MustStop);
                                    UNIT_ASSERT(MessageBuffer.IsEmpty());
                                    UNIT_ASSERT(ackPromiseBySequenceNumber.empty());
                                    UNIT_ASSERT(ackPromiseQueue.empty());
                                } else {
                                    MustStop = true;
                                    closed = true;
                                }
                            }
                    }, event);
                }

                if (continueToken && !MessageBuffer.IsEmpty()) {
                    ::NPersQueue::TAcknowledgableMessage acknowledgeableMessage;
                    Y_ABORT_UNLESS(MessageBuffer.Dequeue(acknowledgeableMessage));
                    if (AutoSeqNo) {
                        ackPromiseQueue.emplace_back(acknowledgeableMessage.AckPromise);
                    } else {
                        ackPromiseBySequenceNumber.emplace(acknowledgeableMessage.SequenceNumber,
                                                           acknowledgeableMessage.AckPromise);
                    }
                    Y_ABORT_UNLESS(continueToken);

                    TMaybe<ui64> seqNo = Nothing();
                    if (!AutoSeqNo) {
                        seqNo = acknowledgeableMessage.SequenceNumber;
                        Log << TLOG_INFO << "[" << sourceId << "] Write messages with sequence numbers "
                            << acknowledgeableMessage.SequenceNumber;
                    }
                    writer->Write(
                            std::move(*continueToken),
                            std::move(acknowledgeableMessage.Value),
                            seqNo,
                            acknowledgeableMessage.CreatedAt
                    );
                    continueToken = Nothing();
                }
            }
            Log << TLOG_DEBUG << "Close writer (stop)";
            writer->Close(TDuration::Zero());
            writer = nullptr;
            Log << TLOG_DEBUG << "Writer closed";
        });
        Thread->Start();
    }

private:
    bool AutoSeqNo;
};

struct TYdbPqTestRetryState : NYdb::NPersQueue::IRetryPolicy::IRetryState {
    TYdbPqTestRetryState(
            std::function<void ()> retryCallback, std::function<void ()> destroyCallback, const TDuration& delay
    )
        : RetryDone(retryCallback)
        , DestroyDone(destroyCallback)
        , Delay(delay)
    {}

    TMaybe<TDuration> GetNextRetryDelay(NYdb::EStatus) override {
        Cerr << "Test retry state: get retry delay\n";
        RetryDone();
        return Delay;
    }
    std::function<void ()> RetryDone;
    std::function<void ()> DestroyDone;
    TDuration Delay;

    ~TYdbPqTestRetryState() {
        DestroyDone();
    }
};
struct TYdbPqNoRetryState : NYdb::NPersQueue::IRetryPolicy::IRetryState {
    TAtomic DelayCalled = 0;
    TMaybe<TDuration> GetNextRetryDelay(NYdb::EStatus) override {
        auto res = AtomicSwap(&DelayCalled, 0);
        UNIT_ASSERT(!res);
        return Nothing();
    }
};

struct TYdbPqTestRetryPolicy : IRetryPolicy {
    TYdbPqTestRetryPolicy(const TDuration& delay = TDuration::MilliSeconds(2000))
        : Delay(delay)
    {
        Cerr << "====TYdbPqTestRetryPolicy()\n";
    }

    IRetryState::TPtr CreateRetryState() const override {
        Cerr << "====CreateRetryState\n";
        if (AtomicSwap(&OnFatalBreakDown, 0)) {
            return std::make_unique<TYdbPqNoRetryState>();
        }
        if (AtomicGet(Initialized_))
        {
            Cerr << "====CreateRetryState Initialized\n";
            auto res = AtomicSwap(&OnBreakDown, 0);
            UNIT_ASSERT(res);
            for (size_t i = 0; i < 100; i++) {
                if (AtomicGet(CurrentRetries) == 0)
                    break;
                Sleep(TDuration::MilliSeconds(100));
            }
            UNIT_ASSERT(AtomicGet(CurrentRetries) == 0);
        }
        auto retryCb = [this]() mutable {this->RetryDone();};
        auto destroyCb = [this]() mutable {this->StateDestroyed();};
        return std::make_unique<TYdbPqTestRetryState>(retryCb, destroyCb, Delay);
    }

    void RetryDone() const {
        AtomicAdd(CurrentRetries, 1);
        auto expected = AtomicGet(RetriesExpected);
        if (expected > 0 && AtomicGet(CurrentRetries) >= expected) {
            with_lock(Lock) {
                RetryPromise.SetValue();
            }
            AtomicSet(RetriesExpected, 0);
        }
    }
    void StateDestroyed() const {
        auto expected = AtomicSwap(&RepairExpected, 0);
        if (expected) {
            with_lock(Lock) {
                RepairPromise.SetValue();
            }
        }
    }
    void ExpectBreakDown() {
        // Either TYdbPqTestRetryPolicy() or Initialize() should be called beforehand in order to set OnBreakDown=0
        Cerr << "====ExpectBreakDown\n";
        for (size_t i = 0; i < 100; i++) {
            if (AtomicGet(OnBreakDown) == 0)
                break;
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT(AtomicGet(OnBreakDown) == 0);
        AtomicSet(CurrentRetries, 0);
        AtomicSet(OnBreakDown, 1);
    }
    void ExpectFatalBreakDown() {
        AtomicSet(OnFatalBreakDown, 1);
    }

    void WaitForRetries(ui64 retryCount, NThreading::TPromise<void>& promise) {
        AtomicSet(RetriesExpected, retryCount);
        with_lock(Lock) {
            RetryPromise = promise;
        }
    }
    void WaitForRetriesSync(ui64 retryCount) {
        NThreading::TPromise<void> retriesPromise = NThreading::NewPromise();
        auto retriesFuture = retriesPromise.GetFuture();
        WaitForRetries(retryCount, retriesPromise);
        retriesFuture.Wait();
    }

    void WaitForRepair(NThreading::TPromise<void>& promise) {
        AtomicSet(RepairExpected, 1 );
        with_lock(Lock) {
            RepairPromise = promise;
        }
    }

    void WaitForRepairSync() {
        NThreading::TPromise<void> repairPromise = NThreading::NewPromise();
        auto repairFuture = repairPromise.GetFuture();
        WaitForRepair(repairPromise);
        repairFuture.Wait();
    }

    void Initialize() {
        AtomicSet(Initialized_, 1);
        AtomicSet(CurrentRetries, 0);
    }
private:
    TDuration Delay;
    mutable TAtomic CurrentRetries = 0;
    mutable TAtomic Initialized_ = 0;
    mutable TAtomic OnBreakDown = 0;
    mutable TAtomic OnFatalBreakDown = 0;
    mutable NThreading::TPromise<void> RetryPromise;
    mutable NThreading::TPromise<void> RepairPromise;
    mutable TAtomic RetriesExpected = 0;
    mutable TAtomic RepairExpected = 0;
    mutable TAdaptiveLock Lock;
};

class TYdbPqTestExecutor : public IAsyncExecutor {
public:
    TYdbPqTestExecutor(std::shared_ptr<TLockFreeQueue<ui64>> idsQueue)
        : Stop()
        , ExecIdsQueue(idsQueue)
        , Thread([idsQueue, this]() {
            while(!Stop) {
                TFunction f;
                while (TasksQueue.Dequeue(&f)) {
                    ++CurrentTaskId;
                    Cerr << "Enqueue task with id " << CurrentTaskId << Endl;
                    Tasks[CurrentTaskId] = f;
                }
                ui64 id = 0;
                while (ExecIdsQueue->Dequeue(&id)) {
                    ExecIds.push(id);
                    Cerr << "Got ok to execute task with id " << id << Endl;

                }
                while (!ExecIds.empty()) {
                    auto id = ExecIds.front();
                    auto iter = Tasks.find(id);
                    if (iter == Tasks.end())
                        break;
                    Cerr << "Executing compression of " << id << Endl;
                    ExecIds.pop();
                    try {
                        (iter->second)();
                    } catch (...) {
                        Cerr << "Failed on compression call: " << CurrentExceptionMessage() << Endl;
                        Y_ABORT();
                    }
                    Cerr << "Compression of " << id << " Done\n";
                    Tasks.erase(iter);
                }

            }
        })
    {
    }
    ~TYdbPqTestExecutor() {
        Stop = true;
        Thread.Join();
    }
    void PostImpl(TVector<TFunction>&& fs) override {
        for (auto& f : fs) {
            TasksQueue.Enqueue(std::move(f));
        }
    }

    void PostImpl(TFunction&& f) override {
        TasksQueue.Enqueue(std::move(f));
    }

    void DoStart() override {
        Thread.Start();
    }

private:
    std::atomic_bool Stop;
    TLockFreeQueue<TFunction> TasksQueue;
    std::shared_ptr<TLockFreeQueue<ui64>> ExecIdsQueue;
    THashMap<ui64, TFunction> Tasks;
    TQueue<ui64> ExecIds;
    ui64 CurrentTaskId = 0;
    TThread Thread;

};


struct TYdbPqWriterTestHelper {
    std::shared_ptr<TPersQueueYdbSdkTestSetup> Setup;
    std::shared_ptr<TYdbPqTestRetryPolicy> Policy;
    std::unique_ptr<TYDBClientEventLoop> EventLoop;
    TIntrusivePtr<TYdbPqTestExecutor> CompressExecutor;

    TAutoEvent MessagesWrittenToBuffer;
    ui64 SeqNo = 1;
    TString Message = "message";
public:
    TYdbPqWriterTestHelper(
            const TString& name,
            std::shared_ptr<TLockFreeQueue<ui64>> executorQueue = nullptr,
            const TString& preferredCluster = TString(),
            std::shared_ptr<TPersQueueYdbSdkTestSetup> setup = nullptr,
            const TString& sourceId = TString(),
            bool autoSeqNo = false
    )
        : Setup(setup ? setup : std::make_shared<TPersQueueYdbSdkTestSetup>(name))
        , Policy(std::make_shared<TYdbPqTestRetryPolicy>())
    {
        if (executorQueue)
            CompressExecutor = MakeIntrusive<TYdbPqTestExecutor>(executorQueue);
        EventLoop = std::make_unique<TYDBClientEventLoop>(Setup, Policy, CompressExecutor, preferredCluster, sourceId,
                                                          autoSeqNo);
    }

    NThreading::TFuture<::NPersQueue::TWriteResult> Write(bool doWait = false, const TString& message = TString()) {
        //auto f = ClientWrite(Message, SeqNo, TInstant::Now());
        auto promise = NThreading::NewPromise<::NPersQueue::TWriteResult>();
        auto log = Setup->GetLog();
        log << TLOG_INFO << "Enqueue message with sequence number " << SeqNo;
        EventLoop->MessageBuffer.Enqueue(::NPersQueue::TAcknowledgableMessage{
                message.Empty() ? Message : message,
                SeqNo, TInstant::Now(), promise
        });
        MessagesWrittenToBuffer.Signal();
        auto f = promise.GetFuture();
        ++SeqNo;
        if (doWait)
            f.Wait();
        return f;
    }
    ~TYdbPqWriterTestHelper() {
        EventLoop = nullptr;
        Setup = nullptr;
        CompressExecutor = nullptr;
        Policy = nullptr;
    }
};

class TSimpleWriteSessionTestAdapter {
public:
    TSimpleWriteSessionTestAdapter(TSimpleBlockingWriteSession* session);
    ui64 GetAcquiredMessagesCount() const;

private:
    TSimpleBlockingWriteSession* Session;
};


void WaitMessagesAcked(std::shared_ptr<IWriteSession> writer, ui64 startSeqNo, ui64 endSeqNo);
} // namespace NYdb::NPersQueue::NTests
