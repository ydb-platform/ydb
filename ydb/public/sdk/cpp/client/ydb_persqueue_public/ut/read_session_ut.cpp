#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/ut_utils.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/logger/log.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/impl/read_session.h>

#include <library/cpp/streams/zstd/zstd.h>
#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/zlib.h>

#include <future>

using namespace NYdb;
using namespace NYdb::NPersQueue;
using IExecutor = NYdb::NPersQueue::IExecutor;
using namespace ::testing; // Google mock.

#define UNIT_ASSERT_EVENT_TYPE(event, type)                 \
    UNIT_ASSERT_C(                                          \
                  std::holds_alternative<type>(event),      \
                  "Real event got: " << DebugString(event)) \
    /**/

#define UNIT_ASSERT_NOT_EVENT_TYPE(event, type)             \
    UNIT_ASSERT_C(                                          \
                  !std::holds_alternative<type>(event),     \
                  "Real event got: " << DebugString(event)) \
    /**/

TString Compress(const TString& sourceData, Ydb::PersQueue::V1::Codec codec = Ydb::PersQueue::V1::CODEC_GZIP) {
    if (codec == Ydb::PersQueue::V1::CODEC_RAW || codec == Ydb::PersQueue::V1::CODEC_UNSPECIFIED) {
        return sourceData;
    }

    TString compressed;
    TStringOutput out(compressed);
    THolder<IOutputStream> coder;
    switch (codec) {
    case Ydb::PersQueue::V1::CODEC_GZIP:
        coder = MakeHolder<TZLibCompress>(&out, ZLib::GZip);
        break;
    case Ydb::PersQueue::V1::CODEC_LZOP:
        throw yexception() << "LZO codec is disabled";
        break;
    case Ydb::PersQueue::V1::CODEC_ZSTD:
        coder = MakeHolder<TZstdCompress>(&out);
        break;
    default:
        UNIT_ASSERT(false);
    }
    coder->Write(sourceData);
    coder->Finish();
    return compressed;
}

template <class TRequest, class TResponse>
struct TMockProcessorFactory : public ISessionConnectionProcessorFactory<TRequest, TResponse> {
    using IFactory = ISessionConnectionProcessorFactory<TRequest, TResponse>;

    virtual ~TMockProcessorFactory() {
        Wait();
    }

    void CreateProcessor( // ISessionConnectionProcessorFactory method.
        typename IFactory::TConnectedCallback callback,
        const TRpcRequestSettings& requestSettings,
        NYdbGrpc::IQueueClientContextPtr connectContext,
        TDuration connectTimeout,
        NYdbGrpc::IQueueClientContextPtr connectTimeoutContext,
        typename IFactory::TConnectTimeoutCallback connectTimeoutCallback,
        TDuration connectDelay,
        NYdbGrpc::IQueueClientContextPtr connectDelayOperationContext) override
    {
        UNIT_ASSERT_C(!ConnectedCallback, "Only one connect at a time is expected");
        UNIT_ASSERT_C(!ConnectTimeoutCallback, "Only one connect at a time is expected");
        ConnectedCallback = callback;
        ConnectTimeoutCallback = connectTimeoutCallback;

        Y_UNUSED(requestSettings);
        UNIT_ASSERT(connectContext);
        UNIT_ASSERT(connectTimeout);
        UNIT_ASSERT(connectTimeoutContext);
        UNIT_ASSERT(connectTimeoutCallback);
        UNIT_ASSERT(!connectDelay || connectDelayOperationContext);

        OnCreateProcessor(++CreateCallsCount);
    }

    MOCK_METHOD(void, ValidateConnectTimeout, (TDuration), ());

    // Handler is called in CreateProcessor() method after parameter validation.
    MOCK_METHOD(void, OnCreateProcessor, (size_t callNumber)); // 1-based

    // Actions to use in OnCreateProcessor handler:
    void CreateProcessor(typename IFactory::IProcessor::TPtr processor) { // Success.
        UNIT_ASSERT(ConnectedCallback);
        auto cb = std::move(ConnectedCallback);
        ConnectedCallback = nullptr;
        ConnectTimeoutCallback = nullptr;
        with_lock (Lock) {
            CallbackFutures.push(std::async(std::launch::async, std::move(cb), TPlainStatus(), processor));
        }
    }

    void FailCreation(EStatus status = EStatus::INTERNAL_ERROR, const TString& message = {}) { // Fail.
        UNIT_ASSERT(ConnectedCallback);
        auto cb = std::move(ConnectedCallback);
        ConnectedCallback = nullptr;
        ConnectTimeoutCallback = nullptr;
        with_lock (Lock) {
            CallbackFutures.push(std::async(std::launch::async, std::move(cb), TPlainStatus(status, message), nullptr));
        }
    }

    void Timeout() { // Timeout.
        UNIT_ASSERT(ConnectTimeoutCallback);
        auto cb = std::move(ConnectTimeoutCallback);
        ConnectedCallback = nullptr;
        ConnectTimeoutCallback = nullptr;
        with_lock (Lock) {
            CallbackFutures.push(std::async(std::launch::async, std::move(cb), true));
        }
    }

    void CreateAndThenTimeout(typename IFactory::IProcessor::TPtr processor) {
        UNIT_ASSERT(ConnectedCallback);
        UNIT_ASSERT(ConnectTimeoutCallback);
        auto cb2 = [cbt = std::move(ConnectTimeoutCallback), cb = std::move(ConnectedCallback), processor]() mutable {
            cb(TPlainStatus(), std::move(processor));
            cbt(true);
        };
        ConnectedCallback = nullptr;
        ConnectTimeoutCallback = nullptr;
        with_lock (Lock) {
            CallbackFutures.push(std::async(std::launch::async, std::move(cb2)));
        }
    }

    void FailAndThenTimeout(EStatus status = EStatus::INTERNAL_ERROR, const TString& message = {}) {
        UNIT_ASSERT(ConnectedCallback);
        UNIT_ASSERT(ConnectTimeoutCallback);
        auto cb2 = [cbt = std::move(ConnectTimeoutCallback), cb = std::move(ConnectedCallback), status, message]() mutable {
            cb(TPlainStatus(status, message), nullptr);
            cbt(true);
        };
        ConnectedCallback = nullptr;
        ConnectTimeoutCallback = nullptr;
        with_lock (Lock) {
            CallbackFutures.push(std::async(std::launch::async, std::move(cb2)));
        }
    }

    void TimeoutAndThenCreate(typename IFactory::IProcessor::TPtr processor) {
        UNIT_ASSERT(ConnectedCallback);
        UNIT_ASSERT(ConnectTimeoutCallback);
        auto cb2 = [cbt = std::move(ConnectTimeoutCallback), cb = std::move(ConnectedCallback), processor]() mutable {
            cbt(true);
            cb(TPlainStatus(), std::move(processor));
        };
        ConnectedCallback = nullptr;
        ConnectTimeoutCallback = nullptr;
        with_lock (Lock) {
            CallbackFutures.push(std::async(std::launch::async, std::move(cb2)));
        }
    }

    void Wait() {
        std::queue<std::future<void>> futuresQueue;
        with_lock (Lock) {
            CallbackFutures.swap(futuresQueue);
        }
        while (!futuresQueue.empty()) {
            futuresQueue.front().wait();
            futuresQueue.pop();
        }
    }

    void Validate() {
        UNIT_ASSERT(CallbackFutures.empty());
        ConnectedCallback = nullptr;
        ConnectTimeoutCallback = nullptr;
    }

    std::atomic<size_t> CreateCallsCount = 0;

private:
    TAdaptiveLock Lock;
    typename IFactory::TConnectedCallback ConnectedCallback;
    typename IFactory::TConnectTimeoutCallback ConnectTimeoutCallback;
    std::queue<std::future<void>> CallbackFutures;
};

struct TMockReadSessionProcessor : public TMockProcessorFactory<Ydb::PersQueue::V1::MigrationStreamingReadClientMessage, Ydb::PersQueue::V1::MigrationStreamingReadServerMessage>::IProcessor {
    // Request to read.
    struct TClientReadInfo {
        TReadCallback Callback;
        Ydb::PersQueue::V1::MigrationStreamingReadServerMessage* Dst;

        operator bool() const {
            return Dst != nullptr;
        }
    };

    // Response from server.
    struct TServerReadInfo {
        NYdbGrpc::TGrpcStatus Status;
        Ydb::PersQueue::V1::MigrationStreamingReadServerMessage Response;

        TServerReadInfo& Failure(grpc::StatusCode status = grpc::StatusCode::UNAVAILABLE, const TString& message = {}, bool internal = false) {
            Status.GRpcStatusCode = status;
            Status.InternalError = internal;
            Status.Msg = message;
            return *this;
        }

        TServerReadInfo& InitResponse(const TString& sessionId) {
            Response.mutable_init_response()->set_session_id(sessionId);
            return *this;
        }

        TServerReadInfo& CreatePartitionStream(const TString& topic = "TestTopic", const TString& cluster = "TestCluster", const ui64 partition = 1, const ui64 assignId = 1, ui64 readOffset = 0, ui64 endOffset = 0) {
            auto* req = Response.mutable_assigned();
            req->mutable_topic()->set_path(topic);
            req->set_cluster(cluster);
            req->set_partition(partition);
            req->set_assign_id(assignId);
            req->set_read_offset(readOffset);
            req->set_end_offset(endOffset);
            return *this;
        }

        TServerReadInfo& ReleasePartitionStream(const TString& topic = "TestTopic", const TString& cluster = "TestCluster", const ui64 partition = 1, const ui64 assignId = 1, ui64 commitOffset = 0, bool forceful = false) {
            auto* req = Response.mutable_release();
            req->mutable_topic()->set_path(topic);
            req->set_cluster(cluster);
            req->set_partition(partition);
            req->set_assign_id(assignId);
            req->set_commit_offset(commitOffset);
            req->set_forceful_release(forceful);
            return *this;
        }

        TServerReadInfo& ForcefulReleasePartitionStream(const TString& topic = "TestTopic", const TString& cluster = "TestCluster", const ui64 partition = 1, const ui64 assignId = 1, ui64 commitOffset = 0) {
            return ReleasePartitionStream(topic, cluster, partition, assignId, commitOffset, true);
        }

        // Data helpers.
        TServerReadInfo& PartitionData(const ui64 cookie, const TString& topic = "TestTopic", const TString& cluster = "TestCluster", const ui64 partition = 1, const ui64 assignId = 1) {
            auto* req = Response.mutable_data_batch()->add_partition_data();
            req->mutable_topic()->set_path(topic);
            req->set_cluster(cluster);
            req->set_partition(partition);
            auto* cookieMsg = req->mutable_cookie();
            cookieMsg->set_assign_id(assignId);
            cookieMsg->set_partition_cookie(cookie);
            return *this;
        }

        TServerReadInfo& Batch(const TString& sourceId, TInstant writeTimestamp = TInstant::MilliSeconds(42), const TString& ip = "::1", const std::vector<std::pair<TString, TString>>& extraFields = {}) {
            const int lastPartitionData = Response.data_batch().partition_data_size();
            UNIT_ASSERT(lastPartitionData > 0);
            auto* partitionData = Response.mutable_data_batch()->mutable_partition_data(lastPartitionData - 1);
            auto* req = partitionData->add_batches();
            req->set_source_id(sourceId);
            req->set_write_timestamp_ms(writeTimestamp.MilliSeconds());
            req->set_ip(ip);
            for (auto&& [k, v] : extraFields) {
                auto* kv = req->add_extra_fields();
                kv->set_key(k);
                kv->set_value(v);
            }
            return *this;
        }

        TServerReadInfo& Message(ui64 offset, const TString& data, Ydb::PersQueue::V1::Codec codec, ui64 seqNo = 1, TInstant createTimestamp = TInstant::MilliSeconds(42)) {
            const int lastPartitionData = Response.data_batch().partition_data_size();
            UNIT_ASSERT(lastPartitionData > 0);
            auto* partitionData = Response.mutable_data_batch()->mutable_partition_data(lastPartitionData - 1);
            const int lastBatch = partitionData->batches_size();
            UNIT_ASSERT(lastBatch > 0);
            auto* batch = partitionData->mutable_batches(lastBatch - 1);
            auto* req = batch->add_message_data();
            req->set_offset(offset);
            req->set_seq_no(seqNo);
            req->set_create_timestamp_ms(createTimestamp.MilliSeconds());
            req->set_data(data);
            req->set_codec(codec);
            return *this;
        }

        TServerReadInfo& CompressMessage(ui64 offset, const TString& sourceData, Ydb::PersQueue::V1::Codec codec = Ydb::PersQueue::V1::CODEC_GZIP, ui64 seqNo = 1, TInstant createTimestamp = TInstant::MilliSeconds(42)) {
            return Message(offset, Compress(sourceData, codec), codec, seqNo, createTimestamp);
        }

        TServerReadInfo& BrokenCompressMessage(ui64 offset, const TString& sourceData, Ydb::PersQueue::V1::Codec codec = Ydb::PersQueue::V1::CODEC_GZIP, ui64 seqNo = 1, TInstant createTimestamp = TInstant::MilliSeconds(42)) {
            return Message(offset, "broken_header_" + Compress(sourceData, codec), codec, seqNo, createTimestamp);
        }


        TServerReadInfo& PartitionStreamStatus(ui64 committedOffset, ui64 endOffset, TInstant writeWatermark, const TString& topic = "TestTopic", const TString& cluster = "TestCluster", const ui64 partition = 1, const ui64 assignId = 1) {
            auto* req = Response.mutable_partition_status();
            req->mutable_topic()->set_path(topic);
            req->set_cluster(cluster);
            req->set_partition(partition);
            req->set_assign_id(assignId);
            req->set_committed_offset(committedOffset);
            req->set_end_offset(endOffset);
            req->set_write_watermark_ms(writeWatermark.MilliSeconds());
            return *this;
        }

        TServerReadInfo& CommitAcknowledgement(ui64 cookie, ui64 assignId = 1) {
            auto* req = Response.mutable_committed();
            auto* cookieInfo = req->add_cookies();
            cookieInfo->set_partition_cookie(cookie);
            cookieInfo->set_assign_id(assignId);
            return *this;
        }
    };

    ~TMockReadSessionProcessor() {
        Wait();
    }

    void Cancel() override {
    }

    void ReadInitialMetadata(std::unordered_multimap<TString, TString>* metadata, TReadCallback callback) override {
        Y_UNUSED(metadata);
        Y_UNUSED(callback);
        UNIT_ASSERT_C(false, "This method is not expected to be called");
    }

    void Finish(TReadCallback callback) override {
        Y_UNUSED(callback);
        UNIT_ASSERT_C(false, "This method is not expected to be called");
    }

    void AddFinishedCallback(TReadCallback callback) override {
        Y_UNUSED(callback);
        UNIT_ASSERT_C(false, "This method is not expected to be called");
    }

    void Read(Ydb::PersQueue::V1::MigrationStreamingReadServerMessage* response, TReadCallback callback) override {
        with_lock (Lock) {
            UNIT_ASSERT(!ActiveRead);
            ActiveRead.Callback = std::move(callback);
            ActiveRead.Dst = response;
            if (!ReadResponses.empty()) {
                StartProcessReadImpl();
            }
        }
    }

    void Write(Ydb::PersQueue::V1::MigrationStreamingReadClientMessage&& request, TWriteCallback callback) override {
        UNIT_ASSERT(!callback); // Read session doesn't set callbacks.
        switch (request.request_case()) {
        case Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::kInitRequest:
            OnInitRequest(request.init_request());
            break;
        case Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::kRead:
            OnReadRequest(request.read());
            break;
        case Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::kStartRead:
            OnStartReadRequest(request.start_read());
            break;
        case Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::kCommit:
            OnCommitRequest(request.commit());
            break;
        case Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::kReleased:
            OnReleasedRequest(request.released());
            break;
        case Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::kStatus:
            OnStatusRequest(request.status());
            break;
        case Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::REQUEST_NOT_SET:
            UNIT_ASSERT_C(false, "Invalid request");
            break;
        }
    }

    MOCK_METHOD(void, OnInitRequest, (const Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::InitRequest&), ());
    MOCK_METHOD(void, OnReadRequest, (const Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::Read&), ());
    MOCK_METHOD(void, OnStartReadRequest, (const Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::StartRead&), ());
    MOCK_METHOD(void, OnCommitRequest, (const Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::Commit&), ());
    MOCK_METHOD(void, OnReleasedRequest, (const Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::Released&), ());
    MOCK_METHOD(void, OnStatusRequest, (const Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::Status&), ());

    void Wait() {
        std::queue<std::future<void>> callbackFutures;
        with_lock (Lock) {
            CallbackFutures.swap(callbackFutures);
        }

        while (!callbackFutures.empty()) {
            callbackFutures.front().wait();
            callbackFutures.pop();
        }
    }

    void Validate() {
        with_lock (Lock) {
            UNIT_ASSERT(ReadResponses.empty());
            UNIT_ASSERT(CallbackFutures.empty());

            ActiveRead = TClientReadInfo{};
        }
    }


    void ProcessRead() {
        NYdbGrpc::TGrpcStatus status;
        TReadCallback callback;
        with_lock (Lock) {
            *ActiveRead.Dst = ReadResponses.front().Response;
            ActiveRead.Dst = nullptr;
            status = std::move(ReadResponses.front().Status);
            ReadResponses.pop();
            callback = std::move(ActiveRead.Callback);
        }
        callback(std::move(status));
    }

    void StartProcessReadImpl() {
        CallbackFutures.push(std::async(std::launch::async, &TMockReadSessionProcessor::ProcessRead, this));
    }

    void AddServerResponse(TServerReadInfo result) {
        bool hasActiveRead = false;
        with_lock (Lock) {
            ReadResponses.emplace(std::move(result));
            if (ActiveRead) {
                hasActiveRead = true;
            }
        }
        if (hasActiveRead) {
            ProcessRead();
        }
    }


    TAdaptiveLock Lock;
    TClientReadInfo ActiveRead;
    std::queue<TServerReadInfo> ReadResponses;
    std::queue<std::future<void>> CallbackFutures;
};

// Class for testing read session impl
// with mocks.
class TReadSessionImplTestSetup {
public:
    // Types
    using IReadSessionConnectionProcessorFactory = ISessionConnectionProcessorFactory<Ydb::PersQueue::V1::MigrationStreamingReadClientMessage, Ydb::PersQueue::V1::MigrationStreamingReadServerMessage>;
    using TMockProcessorFactory = ::TMockProcessorFactory<Ydb::PersQueue::V1::MigrationStreamingReadClientMessage, Ydb::PersQueue::V1::MigrationStreamingReadServerMessage>;


    struct TFakeContext : public NYdbGrpc::IQueueClientContext {
        IQueueClientContextPtr CreateContext() override {
            return std::make_shared<TFakeContext>();
        }

        grpc::CompletionQueue* CompletionQueue() override {
            UNIT_ASSERT_C(false, "This method is not expected to be called");
            return nullptr;
        }

        bool IsCancelled() const override {
            UNIT_ASSERT_C(false, "This method is not expected to be called");
            return false;
        }

        bool Cancel() override {
            return false;
        }

        void SubscribeCancel(std::function<void()>) override {
            UNIT_ASSERT_C(false, "This method is not expected to be called");
        }
    };

    // Methods
    TReadSessionImplTestSetup();
    ~TReadSessionImplTestSetup() noexcept(false); // Performs extra validation and UNIT_ASSERTs

    TSingleClusterReadSessionImpl* GetSession();

    std::shared_ptr<TReadSessionEventsQueue> GetEventsQueue();
    ::IExecutor::TPtr GetDefaultExecutor();

    void SuccessfulInit(bool flag = true);
    TPartitionStream::TPtr CreatePartitionStream(const TString& topic = "TestTopic", const TString& cluster = "TestCluster", ui64 partition = 1, ui64 assignId = 1);

    // Assertions.
    void AssertNoEvents();

public:
    // Members
    TReadSessionSettings Settings;
    TString ClusterName = "cluster";
    TLog Log = CreateLogBackend("cerr");
    std::shared_ptr<TReadSessionEventsQueue> EventsQueue;
    std::shared_ptr<TFakeContext> FakeContext = std::make_shared<TFakeContext>();
    std::shared_ptr<TMockProcessorFactory> MockProcessorFactory = std::make_shared<TMockProcessorFactory>();
    TIntrusivePtr<TMockReadSessionProcessor> MockProcessor = MakeIntrusive<TMockReadSessionProcessor>();
    ui64 PartitionIdStart = 1;
    ui64 PartitionIdStep = 1;
    typename TSingleClusterReadSessionImpl::TPtr Session;
    std::shared_ptr<TCallbackContext<TSingleClusterReadSessionImpl>> CbContext;
    std::shared_ptr<TThreadPool> ThreadPool;
    ::IExecutor::TPtr DefaultExecutor;
    std::shared_ptr<std::unordered_map<ECodec, THolder<NTopic::ICodec>>> ProvidedCodecs = std::make_shared<std::unordered_map<ECodec, THolder<NTopic::ICodec>>>();
};

class TReorderingExecutor : public ::IExecutor {
public:
    TReorderingExecutor(size_t cycleCount = 2, ::IExecutor::TPtr executor = CreateThreadPoolExecutor(1))
        : CycleCount(cycleCount)
        , Executor(std::move(executor))
    {
    }

    bool IsAsync() const override {
        return Executor->IsAsync();
    }

    void Post(TFunction&& f) override {
        with_lock (Lock) {
            Cerr << "Post function" << Endl;
            ++TasksAdded;
            if (Functions.empty()) {
                Functions.reserve(CycleCount);
            }
            Functions.emplace_back(std::move(f));
            if (Functions.size() == CycleCount) {
                Executor->Post([functions = std::move(Functions)]() {
                    for (auto i = functions.rbegin(), end = functions.rend(); i != end; ++i) {
                        (*i)();
                    }
                });
                Functions.clear();
            }
        }
    }

    void DoStart() override {
        Executor->Start();
    }

    size_t GetTasksAdded() {
        with_lock (Lock) {
            return TasksAdded;
        }
    }

private:
    TAdaptiveLock Lock;
    size_t CycleCount;
    size_t TasksAdded = 0;
    ::IExecutor::TPtr Executor;
    std::vector<TFunction> Functions;
};

class TSynchronousExecutor : public ::IExecutor {
    bool IsAsync() const override {
        return false;
    }

    void Post(TFunction&& f) override {
        f();
    }

    void DoStart() override {
    }
};

extern TLogFormatter NYdb::GetPrefixLogFormatter(const TString& prefix); // Defined in ydb.cpp.

TReadSessionImplTestSetup::TReadSessionImplTestSetup() {
    Settings
        .AppendTopics({"TestTopic"})
        .ConsumerName("TestConsumer")
        .RetryPolicy(NYdb::NPersQueue::IRetryPolicy::GetFixedIntervalPolicy(TDuration::MilliSeconds(10)))
        .Counters(MakeIntrusive<NYdb::NPersQueue::TReaderCounters>(MakeIntrusive<::NMonitoring::TDynamicCounters>()));

    Log.SetFormatter(GetPrefixLogFormatter(""));

    (*ProvidedCodecs)[ECodec::GZIP] = MakeHolder<NTopic::TGzipCodec>();
    (*ProvidedCodecs)[ECodec::LZOP] = MakeHolder<NTopic::TUnsupportedCodec>();
    (*ProvidedCodecs)[ECodec::ZSTD] = MakeHolder<NTopic::TZstdCodec>();
}

TReadSessionImplTestSetup::~TReadSessionImplTestSetup() noexcept(false) {
    if (!std::uncaught_exception()) { // Exiting from test successfully. Check additional expectations.
        MockProcessorFactory->Wait();
        MockProcessor->Wait();

        MockProcessorFactory->Validate();
        MockProcessor->Validate();
    }

    CbContext->Cancel();
    Session = nullptr;

    ThreadPool->Stop();
}

::IExecutor::TPtr TReadSessionImplTestSetup::GetDefaultExecutor() {
    if (!DefaultExecutor) {
        ThreadPool = std::make_shared<TThreadPool>();
        ThreadPool->Start(1);
        DefaultExecutor = CreateThreadPoolExecutorAdapter(ThreadPool);
    }
    return DefaultExecutor;
}

TSingleClusterReadSessionImpl* TReadSessionImplTestSetup::GetSession() {
    if (!Session) {
        if (!Settings.DecompressionExecutor_) {
            Settings.DecompressionExecutor(GetDefaultExecutor());
        }
        if (!Settings.EventHandlers_.HandlersExecutor_) {
            Settings.EventHandlers_.HandlersExecutor(GetDefaultExecutor());
        }
        CbContext = MakeWithCallbackContext<TSingleClusterReadSessionImpl>(
            Settings,
            "db",
            "sessionid",
            ClusterName,
            Log,
            MockProcessorFactory,
            GetEventsQueue(),
            FakeContext,
            PartitionIdStart,
            PartitionIdStep);
        Session = CbContext->TryGet();
    }
    return Session.get();
}

std::shared_ptr<TReadSessionEventsQueue> TReadSessionImplTestSetup::GetEventsQueue() {
    if (!EventsQueue) {
        EventsQueue = std::make_shared<TReadSessionEventsQueue>(Settings);
    }
    return EventsQueue;
}

void TReadSessionImplTestSetup::SuccessfulInit(bool hasInitRequest) {
    EXPECT_CALL(*MockProcessorFactory, OnCreateProcessor(1))
        .WillOnce([&](){ MockProcessorFactory->CreateProcessor(MockProcessor); });
    if (hasInitRequest)
        EXPECT_CALL(*MockProcessor, OnInitRequest(_));
    MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo().InitResponse("123-session-id-321"));
    GetSession()->Start();
    MockProcessorFactory->Wait();
    MockProcessor->Wait();
}

TPartitionStream::TPtr TReadSessionImplTestSetup::CreatePartitionStream(const TString& topic, const TString& cluster, ui64 partition, ui64 assignId) {
    MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo().CreatePartitionStream(topic, cluster, partition, assignId)); // Callback will be called.
    TMaybe<TReadSessionEvent::TEvent> event = EventsQueue->GetEvent(true);
    UNIT_ASSERT(event);
    UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TCreatePartitionStreamEvent);
    auto& createEvent = std::get<TReadSessionEvent::TCreatePartitionStreamEvent>(*event);
    auto stream = createEvent.GetPartitionStream();
    UNIT_ASSERT(stream);
    createEvent.Confirm();
    return stream;
}

void TReadSessionImplTestSetup::AssertNoEvents() {
    TMaybe<TReadSessionEvent::TEvent> event = GetEventsQueue()->GetEvent(false);
    UNIT_ASSERT(!event);
}

using NYdb::NPersQueue::NTests::TPersQueueYdbSdkTestSetup;
Y_UNIT_TEST_SUITE(PersQueueSdkReadSessionTest) {
    void ReadSessionImpl(bool close, bool commit, bool explicitlySpecifiedPartitions = false) {
        NYdb::NPersQueue::NTests::TPersQueueYdbSdkTestSetup setup("ReadSession");
        setup.WriteToTopic({"message1", "message2"});
        auto settings = setup.GetReadSessionSettings();
        if (explicitlySpecifiedPartitions) {
            settings.Topics_[0].AppendPartitionGroupIds(1);
        }
        std::shared_ptr<IReadSession> session = setup.GetPersQueueClient().CreateReadSession(settings);

        TDeferredCommit dc;
        // Event 1: create partition stream.
        {
            TMaybe<TReadSessionEvent::TEvent> event = session->GetEvent(true);
            UNIT_ASSERT(event);
            UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TCreatePartitionStreamEvent);
            std::get<TReadSessionEvent::TCreatePartitionStreamEvent>(*event).Confirm();
            Cerr << "create event " << DebugString(*event) << Endl;
        }
        // Event 2: data.
        {
            TMaybe<TReadSessionEvent::TEvent> event = session->GetEvent(true);
            UNIT_ASSERT(event);
            UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);
            TReadSessionEvent::TDataReceivedEvent& dataEvent = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
            UNIT_ASSERT_VALUES_EQUAL(dataEvent.GetMessages().size(), 2);
            for (auto& msg : dataEvent.GetMessages()) {
                UNIT_ASSERT(msg.GetData() == "message1" || msg.GetData() == "message2");
            }
            Cerr << "data event " << DebugString(*event) << Endl;
            if (commit) {
                dc.Add(dataEvent);
            }
        }
        setup.WriteToTopic({"message3"});
        // Event 3: data.
        {
            TMaybe<TReadSessionEvent::TEvent> event = session->GetEvent(true);
            UNIT_ASSERT(event);
            UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);
            TReadSessionEvent::TDataReceivedEvent& dataEvent = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
            UNIT_ASSERT_VALUES_EQUAL(dataEvent.GetMessages().size(), 1);
            for (auto& msg : dataEvent.GetMessages()) {
                UNIT_ASSERT(msg.GetData() == "message3");
            }
            Cerr << "data event " << DebugString(*event) << Endl;

            dataEvent.Commit(); // Commit right now!
        }

        dc.Commit();

        if (close) {
            session->Close(TDuration::Seconds(30));
        }

        // Event 4: commit ack.
        if (commit) {  // (commit && close) branch check is broken with current TReadSession::Close quick fix
            TMaybe<TReadSessionEvent::TEvent> event = session->GetEvent(!close); // Event is expected to be already in queue if closed.
            UNIT_ASSERT(event);
            Cerr << "commit ack or close event " << DebugString(*event) << Endl;
            UNIT_ASSERT(std::holds_alternative<TReadSessionEvent::TCommitAcknowledgementEvent>(*event) || std::holds_alternative<TSessionClosedEvent>(*event));
        }

        if (close && !commit) {
            TMaybe<TReadSessionEvent::TEvent> event = session->GetEvent(false);
            UNIT_ASSERT(event);
            Cerr << "close event " << DebugString(*event) << Endl;
            UNIT_ASSERT(std::holds_alternative<TSessionClosedEvent>(*event));
            UNIT_ASSERT_STRING_CONTAINS(DebugString(*event), "Session was gracefully closed");
        }
    }

    Y_UNIT_TEST(ReadSessionWithAbort) {
        ReadSessionImpl(false, true);
    }

    Y_UNIT_TEST(ReadSessionWithClose) {
        ReadSessionImpl(true, true);
    }

    Y_UNIT_TEST(ReadSessionWithCloseNotCommitted) {
        ReadSessionImpl(true, false);
    }

    Y_UNIT_TEST(ReadSessionWithExplicitlySpecifiedPartitions) {
        ReadSessionImpl(true, true, true);
    }

    Y_UNIT_TEST(SettingsValidation) {
        TPersQueueYdbSdkTestSetup setup("SettingsValidation");
        const auto goodSettings = setup.GetReadSessionSettings();

#define ASSERT_BAD_VALIDATION                                           \
        std::shared_ptr<IReadSession> session =                         \
            setup.GetPersQueueClient().CreateReadSession(settings);     \
        session->WaitEvent().Wait();                                    \
        TMaybe<TReadSessionEvent::TEvent> event =                       \
            session->GetEvent(true);                                    \
        UNIT_ASSERT(event);                                             \
        Cerr << DebugString(*event) << Endl;                            \
        UNIT_ASSERT_EVENT_TYPE(*event, TSessionClosedEvent);            \
        /**/

        // No topics to read.
        {
            auto settings = goodSettings;
            settings.Topics_.clear();
            ASSERT_BAD_VALIDATION;
        }

        // No consumer name.
        {
            auto settings = goodSettings;
            settings.ConsumerName("");
            ASSERT_BAD_VALIDATION;
        }

        // Small max memory usage.
        {
            auto settings = goodSettings;
            settings.MaxMemoryUsageBytes(100);
            ASSERT_BAD_VALIDATION;
        }
    }

    Y_UNIT_TEST(ClosesAfterFailedConnectionToCds) {
        TPersQueueYdbSdkTestSetup setup("ClosesAfterFailedConnectionToCds");
        setup.ShutdownGRpc();

        TReadSessionSettings settings = setup.GetReadSessionSettings();
        // Set policy with max retries == 3.
        settings.RetryPolicy(NYdb::NPersQueue::IRetryPolicy::GetExponentialBackoffPolicy(TDuration::MilliSeconds(10), TDuration::MilliSeconds(10), TDuration::MilliSeconds(100), 3));
        std::shared_ptr<IReadSession> session = setup.GetPersQueueClient().CreateReadSession(settings);
        TMaybe<TReadSessionEvent::TEvent> event = session->GetEvent(true);
        UNIT_ASSERT(event);
        Cerr << DebugString(*event) << Endl;
        UNIT_ASSERT_EVENT_TYPE(*event, TSessionClosedEvent);
    }

    Y_UNIT_TEST(SpecifyClustersExplicitly) {
        TPersQueueYdbSdkTestSetup setup("SpecifyClustersExplicitly");

        auto settings = setup.GetReadSessionSettings();
        settings.ReadOriginal({setup.GetLocalCluster()});

        // Success.
        {
            std::shared_ptr<IReadSession> session = setup.GetPersQueueClient().CreateReadSession(settings);
            TMaybe<TReadSessionEvent::TEvent> event = session->GetEvent(true);
            UNIT_ASSERT(event);
            Cerr << DebugString(*event) << Endl;
            UNIT_ASSERT_NOT_EVENT_TYPE(*event, TSessionClosedEvent);
        }

        // Failure: one cluster endpoint is invalid.
        {
            settings.AppendClusters("unknown_cluster");
            std::shared_ptr<IReadSession> session = setup.GetPersQueueClient().CreateReadSession(settings);
            TMaybe<TReadSessionEvent::TEvent> event = session->GetEvent(true);
            UNIT_ASSERT(event);
            Cerr << DebugString(*event) << Endl;
            UNIT_ASSERT_EVENT_TYPE(*event, TSessionClosedEvent);
        }

        // Failure: no valid cluster endpoints.
        {
            settings.ReadOriginal({"unknown_cluster"});
            std::shared_ptr<IReadSession> session = setup.GetPersQueueClient().CreateReadSession(settings);
            TMaybe<TReadSessionEvent::TEvent> event = session->GetEvent(true);
            UNIT_ASSERT(event);
            Cerr << DebugString(*event) << Endl;
            UNIT_ASSERT_EVENT_TYPE(*event, TSessionClosedEvent);
        }
    }

    Y_UNIT_TEST(StopResumeReadingData) {
        NYdb::NPersQueue::NTests::TPersQueueYdbSdkTestSetup setup("ReadSession");
        setup.WriteToTopic({"message1"});
        std::shared_ptr<IReadSession> session = setup.GetPersQueueClient().CreateReadSession(setup.GetReadSessionSettings());

        // Event 1: create partition stream.
        {
            TMaybe<TReadSessionEvent::TEvent> event = session->GetEvent(true);
            UNIT_ASSERT(event);
            UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TCreatePartitionStreamEvent);
            std::get<TReadSessionEvent::TCreatePartitionStreamEvent>(*event).Confirm();
            Cerr << DebugString(*event) << Endl;
        }

        // Event 2: receive data.
        auto GetDataEvent = [&](const TString& content) -> TMaybe<TReadSessionEvent::TEvent> {
            TMaybe<TReadSessionEvent::TEvent> event = session->GetEvent(true);
            UNIT_ASSERT(event);
            UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);
            TReadSessionEvent::TDataReceivedEvent& dataEvent = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
            UNIT_ASSERT_VALUES_EQUAL(dataEvent.GetMessages().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(dataEvent.GetMessages()[0].GetData(), content);
            Cerr << DebugString(*event) << Endl;
            return event;
        };

        TMaybe<TReadSessionEvent::TEvent> dataEvents[2];

        dataEvents[0] = GetDataEvent("message1");

        // Stop reading data.
        session->StopReadingData();

        // Write data.
        setup.WriteToTopic({"message2"});

        // Already requested read.
        dataEvents[1] = GetDataEvent("message2");

        // Write data.
        setup.WriteToTopic({"message3"});

        // Commit and check that other events will come.
        for (int i = 0; i < 2; ++i) {
            std::get<TReadSessionEvent::TDataReceivedEvent>(*dataEvents[i]).Commit();
            TMaybe<TReadSessionEvent::TEvent> event = session->GetEvent(true);
            UNIT_ASSERT(event);
            Y_ASSERT(std::holds_alternative<TReadSessionEvent::TCommitAcknowledgementEvent>(*event));
            Cerr << DebugString(*event) << Endl;
        }

        auto eventFuture = session->WaitEvent();
        UNIT_ASSERT_C(!eventFuture.Wait(TDuration::Seconds(1)), DebugString(*session->GetEvent(false)));

        // Resume reading data.
        session->ResumeReadingData();

        // And now we can read.
        auto dataEvent3 = GetDataEvent("message3");

        session->Close(TDuration::Seconds(3));
    }
}

Y_UNIT_TEST_SUITE(ReadSessionImplTest) {
    void SuccessfulInitImpl(bool thenTimeout) {
        TReadSessionImplTestSetup setup;
        setup.Settings
            .MaxTimeLag(TDuration::Seconds(32))
            .StartingMessageTimestamp(TInstant::Seconds(42));

        setup.Settings.Topics_[0]
            .StartingMessageTimestamp(TInstant::Seconds(146))
            .AppendPartitionGroupIds(100)
            .AppendPartitionGroupIds(101);

        EXPECT_CALL(*setup.MockProcessorFactory, OnCreateProcessor(_))
            .WillOnce([&](){
                if (thenTimeout) {
                    setup.MockProcessorFactory->CreateAndThenTimeout(setup.MockProcessor);
                } else {
                    setup.MockProcessorFactory->CreateProcessor(setup.MockProcessor);
                }
            });
        EXPECT_CALL(*setup.MockProcessor, OnInitRequest(_))
            .WillOnce(Invoke([](const Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::InitRequest& req) {
                UNIT_ASSERT_STRINGS_EQUAL(req.consumer(), "TestConsumer");
                UNIT_ASSERT_VALUES_EQUAL(req.max_lag_duration_ms(), 32000);
                UNIT_ASSERT_VALUES_EQUAL(req.start_from_written_at_ms(), 42000);
                UNIT_ASSERT_VALUES_EQUAL(req.topics_read_settings_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(req.topics_read_settings(0).topic(), "TestTopic");
                UNIT_ASSERT_VALUES_EQUAL(req.topics_read_settings(0).start_from_written_at_ms(), 146000);
                UNIT_ASSERT_VALUES_EQUAL(req.topics_read_settings(0).partition_group_ids_size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(req.topics_read_settings(0).partition_group_ids(0), 100);
                UNIT_ASSERT_VALUES_EQUAL(req.topics_read_settings(0).partition_group_ids(1), 101);
            }));
        setup.GetSession()->Start();
        setup.MockProcessorFactory->Wait();

        EXPECT_CALL(*setup.MockProcessor, OnReadRequest(_));
        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo().InitResponse("session id"));

        setup.AssertNoEvents();
    }

    Y_UNIT_TEST(SuccessfulInit) {
        SuccessfulInitImpl(false);
    }

    Y_UNIT_TEST(SuccessfulInitAndThenTimeoutCallback) {
        SuccessfulInitImpl(true);
    }

    void ReconnectOnTmpErrorImpl(bool timeout, bool thenSecondCallback) {
        TReadSessionImplTestSetup setup;
        EXPECT_CALL(*setup.MockProcessorFactory, OnCreateProcessor(_))
            .WillOnce([&](){
                if (timeout) {
                    if (thenSecondCallback) {
                        setup.MockProcessorFactory->TimeoutAndThenCreate(nullptr);
                    } else {
                        setup.MockProcessorFactory->Timeout();
                    }
                } else {
                    if (thenSecondCallback) {
                        setup.MockProcessorFactory->FailAndThenTimeout();
                    } else {
                        setup.MockProcessorFactory->FailCreation();
                    }
                }
            })
            .WillOnce([&](){ setup.MockProcessorFactory->CreateProcessor(setup.MockProcessor); });
        EXPECT_CALL(*setup.MockProcessor, OnInitRequest(_));
        setup.GetSession()->Start();
        setup.MockProcessorFactory->Wait();

        EXPECT_CALL(*setup.MockProcessor, OnReadRequest(_));
        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo().InitResponse("session id"));

        setup.AssertNoEvents();
    }

    Y_UNIT_TEST(ReconnectOnTmpError) {
        ReconnectOnTmpErrorImpl(false, false);
    }

    Y_UNIT_TEST(ReconnectOnTmpErrorAndThenTimeout) {
        ReconnectOnTmpErrorImpl(false, true);
    }

    Y_UNIT_TEST(ReconnectOnTimeout) {
        ReconnectOnTmpErrorImpl(true, false);
    }

    Y_UNIT_TEST(ReconnectOnTimeoutAndThenCreate) {
        ReconnectOnTmpErrorImpl(true, true);
    }

    void StopsRetryAfterFailedAttemptImpl(bool timeout) {
        TReadSessionImplTestSetup setup;
        setup.Settings.RetryPolicy(NYdb::NPersQueue::IRetryPolicy::GetNoRetryPolicy());
        EXPECT_CALL(*setup.MockProcessorFactory, OnCreateProcessor(_))
            .WillOnce([&]() {
                if (timeout)
                    setup.MockProcessorFactory->Timeout();
                else
                    setup.MockProcessorFactory->FailCreation();
            });

        //EXPECT_CALL(*setup.MockErrorHandler, AbortSession(_));

        setup.GetSession()->Start();
        setup.MockProcessorFactory->Wait();
        UNIT_ASSERT(setup.GetEventsQueue()->IsClosed());
    }


    Y_UNIT_TEST(StopsRetryAfterFailedAttempt) {
        StopsRetryAfterFailedAttemptImpl(false);
    }

    Y_UNIT_TEST(StopsRetryAfterTimeout) {
        StopsRetryAfterFailedAttemptImpl(true);
    }

    Y_UNIT_TEST(UsesOnRetryStateDuringRetries) {
        class TTestRetryState : public NYdb::NPersQueue::IRetryPolicy::IRetryState {
            TDuration Delay;

            TMaybe<TDuration> GetNextRetryDelay(NYdb::EStatus) override {
                Delay += TDuration::Seconds(1);
                return Delay;
            }
        };

        class TTestRetryPolicy : public NYdb::NPersQueue::IRetryPolicy {
            IRetryState::TPtr CreateRetryState() const override {
                return IRetryState::TPtr(new TTestRetryState());
            }
        };

        std::shared_ptr<NYdb::NPersQueue::IRetryPolicy::IRetryState> state(new TTestRetryState());
        TReadSessionImplTestSetup setup;
        ON_CALL(*setup.MockProcessorFactory, ValidateConnectTimeout(_))
            .WillByDefault([state](TDuration timeout) mutable {
                    UNIT_ASSERT_VALUES_EQUAL(timeout, *state->GetNextRetryDelay(NYdb::EStatus::INTERNAL_ERROR));
            });
        auto failCreation = [&]() {
            setup.MockProcessorFactory->FailCreation();
        };
        EXPECT_CALL(*setup.MockProcessorFactory, OnCreateProcessor(_))
            .WillOnce(failCreation)
            .WillOnce(failCreation)
            .WillOnce(failCreation)
            .WillOnce([](){}); // No action. The end of test.

        setup.GetSession()->Start();
        while (setup.MockProcessorFactory->CreateCallsCount < 4) {
            Sleep(TDuration::MilliSeconds(10));
        }
        setup.MockProcessorFactory->Wait();

        setup.AssertNoEvents();
    }

    Y_UNIT_TEST(ReconnectsAfterFailure) {
        TReadSessionImplTestSetup setup;
        setup.SuccessfulInit();

        auto secondProcessor = MakeIntrusive<TMockReadSessionProcessor>();
        EXPECT_CALL(*setup.MockProcessorFactory, OnCreateProcessor(2))
            .WillOnce([&](){ setup.MockProcessorFactory->CreateProcessor(secondProcessor); });

        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo().Failure()); // Callback will be called.

        setup.Session->Abort();

        setup.AssertNoEvents();

        setup.MockProcessor->Wait();
        secondProcessor->Wait();
        secondProcessor->Validate();

    }

    Y_UNIT_TEST(CreatePartitionStream) {
        TReadSessionImplTestSetup setup;
        setup.SuccessfulInit();

        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo().CreatePartitionStream()); // Callback will be called.
        {
            TVector<TReadSessionEvent::TEvent> events = setup.EventsQueue->GetEvents(true);
            UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
            UNIT_ASSERT_EVENT_TYPE(events[0], TReadSessionEvent::TCreatePartitionStreamEvent);
            auto& event = std::get<TReadSessionEvent::TCreatePartitionStreamEvent>(events[0]);
            auto stream = event.GetPartitionStream();
            UNIT_ASSERT(stream);

            EXPECT_CALL(*setup.MockProcessor, OnStartReadRequest(_))
                .WillOnce(Invoke([](const Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::StartRead& req) {
                    UNIT_ASSERT_STRINGS_EQUAL(req.topic().path(), "TestTopic");
                    UNIT_ASSERT_STRINGS_EQUAL(req.cluster(), "TestCluster");
                    UNIT_ASSERT_VALUES_EQUAL(req.partition(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(req.assign_id(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(req.read_offset(), 13);
                    UNIT_ASSERT_VALUES_EQUAL(req.commit_offset(), 31);
                }));

            event.Confirm(13, 31);
        }
    }

    void DestroyPartitionStreamImpl(bool forceful) {
        TReadSessionImplTestSetup setup;
        setup.SuccessfulInit();

        Y_UNUSED(forceful);
        TPartitionStream::TPtr stream = setup.CreatePartitionStream();

        // Send release by server.
        {
            TMockReadSessionProcessor::TServerReadInfo resp;
            if (forceful) {
                resp.ForcefulReleasePartitionStream();
            } else {
                resp.ReleasePartitionStream();
            }
            setup.MockProcessor->AddServerResponse(resp); // Callback will be called.
        }

        // Check destroy event.
        if (!forceful) {
            TMaybe<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
            UNIT_ASSERT(event);
            UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDestroyPartitionStreamEvent);
            auto& destroyEvent = std::get<TReadSessionEvent::TDestroyPartitionStreamEvent>(*event);
            UNIT_ASSERT_EQUAL(destroyEvent.GetPartitionStream(), stream);

            EXPECT_CALL(*setup.MockProcessor, OnReleasedRequest(_))
                .WillOnce(Invoke([](const Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::Released& req) {
                    UNIT_ASSERT_STRINGS_EQUAL(req.topic().path(), "TestTopic");
                    UNIT_ASSERT_STRINGS_EQUAL(req.cluster(), "TestCluster");
                    UNIT_ASSERT_VALUES_EQUAL(req.partition(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(req.assign_id(), 1);
                }));

            destroyEvent.Confirm();
        }

        // Check closed event.
        {
            TMaybe<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
            UNIT_ASSERT(event);
            UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TPartitionStreamClosedEvent);
            auto& closedEvent = std::get<TReadSessionEvent::TPartitionStreamClosedEvent>(*event);
            UNIT_ASSERT_EQUAL(closedEvent.GetPartitionStream(), stream);

            if (forceful) {
                UNIT_ASSERT_EQUAL_C(closedEvent.GetReason(), TReadSessionEvent::TPartitionStreamClosedEvent::EReason::Lost, DebugString(*event));
            } else {
                UNIT_ASSERT_EQUAL_C(closedEvent.GetReason(), TReadSessionEvent::TPartitionStreamClosedEvent::EReason::DestroyConfirmedByUser, DebugString(*event));
            }
        }
    }

    Y_UNIT_TEST(ForcefulDestroyPartitionStream) {
        DestroyPartitionStreamImpl(true);
    }

    Y_UNIT_TEST(DestroyPartitionStreamRequest) {
        DestroyPartitionStreamImpl(false);
    }

    Y_UNIT_TEST(ProperlyOrdersDecompressedData) {
        TReadSessionImplTestSetup setup;
        setup.Settings.DecompressionExecutor(new TReorderingExecutor());
        setup.SuccessfulInit();
        TPartitionStream::TPtr stream = setup.CreatePartitionStream();
        for (ui64 i = 1; i <= 2; ++i) {
            setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                                   .PartitionData(i)
                                                   .Batch("src_id")
                                                   .CompressMessage(i, TStringBuilder() << "message" << i)); // Callback will be called.
        }

        for (ui64 i = 1; i <= 2; ) {
            TMaybe<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
            UNIT_ASSERT(event);
            UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);
            auto& dataEvent = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
            for (ui32 j = 0; j < dataEvent.GetMessages().size(); ++j, ++i) {
                UNIT_ASSERT_VALUES_EQUAL(dataEvent.GetMessages()[j].GetData(), TStringBuilder() << "message" << i);
            }
        }

        setup.AssertNoEvents();
    }

    Y_UNIT_TEST(BrokenCompressedData) {
        TReadSessionImplTestSetup setup;
        setup.Settings.DecompressionExecutor(new TReorderingExecutor(1));
        setup.SuccessfulInit();
        TPartitionStream::TPtr stream = setup.CreatePartitionStream();
        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                                   .PartitionData(1)
                                                   .Batch("src_id")
                                                   .BrokenCompressMessage(1, "message")
                                                   .CompressMessage(2, "message2")
                                                   .CompressMessage(3, "message3"));

        // Exception was passed during decompression.
        {
            TMaybe<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
            UNIT_ASSERT(event);
            UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);
            auto& dataEvent = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
            Cerr << dataEvent.DebugString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(dataEvent.GetMessages().size(), 3);
            UNIT_ASSERT_EXCEPTION(dataEvent.GetMessages()[0].GetData(), TZLibDecompressorError);
            UNIT_ASSERT_VALUES_EQUAL(dataEvent.GetMessages()[1].GetData(), "message2");
            UNIT_ASSERT_VALUES_EQUAL(dataEvent.GetMessages()[2].GetData(), "message3");
        }

        setup.AssertNoEvents();
    }

    void DecompressImpl(Ydb::PersQueue::V1::Codec codec, const TString& data = "msg", ::IExecutor::TPtr executor = nullptr) {
        TReadSessionImplTestSetup setup;
        if (executor) {
            setup.Settings.DecompressionExecutor(executor);
        }
        setup.SuccessfulInit(false);
        TPartitionStream::TPtr stream = setup.CreatePartitionStream();

        EXPECT_CALL(*setup.MockProcessor, OnReadRequest(_));
        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                               .PartitionData(1)
                                               .Batch("src_id")
                                               .CompressMessage(1, data, codec));

        TMaybe<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
        UNIT_ASSERT(event);
        UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);
        auto& dataEvent = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
        UNIT_ASSERT_VALUES_EQUAL(dataEvent.GetMessages().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(dataEvent.GetMessages()[0].GetData(), data);

        setup.AssertNoEvents();
    }

    Y_UNIT_TEST(DecompressRaw) {
        DecompressImpl(Ydb::PersQueue::V1::CODEC_RAW);
        DecompressImpl(Ydb::PersQueue::V1::CODEC_UNSPECIFIED); // The same.
    }

    Y_UNIT_TEST(DecompressGzip) {
        DecompressImpl(Ydb::PersQueue::V1::CODEC_GZIP);
    }

    Y_UNIT_TEST(DecompressZstd) {
        DecompressImpl(Ydb::PersQueue::V1::CODEC_ZSTD);
    }

    Y_UNIT_TEST(DecompressRawEmptyMessage) {
        DecompressImpl(Ydb::PersQueue::V1::CODEC_RAW, "");
        DecompressImpl(Ydb::PersQueue::V1::CODEC_UNSPECIFIED, ""); // The same.
    }

    Y_UNIT_TEST(DecompressGzipEmptyMessage) {
        DecompressImpl(Ydb::PersQueue::V1::CODEC_GZIP, "");
    }

    Y_UNIT_TEST(DecompressZstdEmptyMessage) {
        DecompressImpl(Ydb::PersQueue::V1::CODEC_ZSTD, "");
    }

    Y_UNIT_TEST(DecompressWithSynchronousExecutor) {
        DecompressImpl(Ydb::PersQueue::V1::CODEC_ZSTD, "msg", new TSynchronousExecutor());
    }

    TString GenerateMessageData(size_t size) {
        TString result;
        result.reserve(size);
        unsigned char ch = static_cast<unsigned char>(size);
        while (size--) {
            result.append(static_cast<char>(ch));
            do {
                ch = ch * 7 + 23;
            } while (ch == 0);
        }
        return result;
    }

    void PacksBatchesImpl(size_t serverBatchesCount, size_t messagesInServerBatchCount, size_t messageSize, size_t batchLimit, size_t batches, size_t messagesInBatch, size_t expectedTasks = 0, size_t reorderedCycleSize = 0, size_t memoryLimit = 0) {
        if (!expectedTasks) {
            expectedTasks = serverBatchesCount;
        }
        if (!reorderedCycleSize) {
            reorderedCycleSize = expectedTasks;
        }
        TReadSessionImplTestSetup setup;
        if (memoryLimit) {
            setup.Settings.MaxMemoryUsageBytes(memoryLimit);
        }
        auto executor = MakeIntrusive<TReorderingExecutor>(reorderedCycleSize);
        setup.Settings.DecompressionExecutor(executor);
        setup.SuccessfulInit();
        TPartitionStream::TPtr stream = setup.CreatePartitionStream();

        const TString messageData = GenerateMessageData(messageSize);
        const TString compressedMessageData = Compress(messageData);
        Cerr << "Message data size: " << messageData.size() << Endl;
        Cerr << "Compressed message data size: " << compressedMessageData.size() << Endl;
        ui64 offset = 1;
        ui64 seqNo = 42;
        THashSet<ui64> committedCookies;
        THashSet<ui64> committedOffsets;
        EXPECT_CALL(*setup.MockProcessor, OnCommitRequest(_))
            .WillRepeatedly(Invoke([&committedCookies, &committedOffsets](const Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::Commit& req) {
                for (const auto& commit : req.cookies()) {
                    committedCookies.insert(commit.partition_cookie());
                }
                for (const auto& range : req.offset_ranges()) {
                    Cerr << "GOT RANGE " << range.start_offset() << " " << range.end_offset() << "\n";
                    for (ui64 i = range.start_offset(); i < range.end_offset(); ++i) {
                        committedOffsets.insert(i);
                    }
                }
            }));

        for (ui64 i = 1; i <= serverBatchesCount; ++i) {
            TMockReadSessionProcessor::TServerReadInfo resp;
            resp.PartitionData(i).Batch("src_id", TInstant::Seconds(123), "127.0.0.1", { { "k", "v" }, { "k1", "v1" }});
            for (size_t j = 0; j < messagesInServerBatchCount; ++j) {
                resp.Message(offset++, compressedMessageData, Ydb::PersQueue::V1::CODEC_GZIP, seqNo++);
                if (j == messagesInServerBatchCount / 2) { // This may lead to empty batch. Client is expected to behave well with empty batch in protobuf.
                    resp.Batch("src_id_2", TInstant::Seconds(321), "1.0.0.127", { { "v", "k" }, { "v1", "k1" }});
                }
            }
            setup.MockProcessor->AddServerResponse(resp);
        }

        ui64 prevOffset = 0;
        ui64 prevSeqNo = 41;
        for (size_t i = 0; i < batches; ++i) {
            Cerr << "Getting new event" << Endl;
            TMaybe<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true, batchLimit);
            UNIT_ASSERT(event);
            UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);
            Cerr << DebugString(*event) << Endl;
            auto& dataEvent = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
            UNIT_ASSERT_VALUES_EQUAL(dataEvent.GetMessages().size(), messagesInBatch);
            for (const auto& message : dataEvent.GetMessages()) {
                auto meta = message.GetMeta();
                UNIT_ASSERT_VALUES_EQUAL(message.GetData(), messageData);
                UNIT_ASSERT_VALUES_EQUAL(message.GetOffset(), prevOffset + 1);
                UNIT_ASSERT_VALUES_EQUAL(message.GetSeqNo(), prevSeqNo + 1);
                ++prevOffset;
                ++prevSeqNo;
                UNIT_ASSERT_VALUES_EQUAL(message.GetCreateTime(), TInstant::MilliSeconds(42));
                if (message.GetMessageGroupId() == "src_id") {
                    UNIT_ASSERT_VALUES_EQUAL(message.GetWriteTime(), TInstant::Seconds(123));
                    UNIT_ASSERT_VALUES_EQUAL(message.GetIp(), "127.0.0.1");
                    UNIT_ASSERT_VALUES_EQUAL(meta->Fields.at("k"), "v");
                    UNIT_ASSERT_VALUES_EQUAL(meta->Fields.at("k1"), "v1");
                } else if (message.GetMessageGroupId() == "src_id_2") {
                    UNIT_ASSERT_VALUES_EQUAL(message.GetWriteTime(), TInstant::Seconds(321));
                    UNIT_ASSERT_VALUES_EQUAL(message.GetIp(), "1.0.0.127");
                    UNIT_ASSERT_VALUES_EQUAL(meta->Fields.at("v"), "k");
                    UNIT_ASSERT_VALUES_EQUAL(meta->Fields.at("v1"), "k1");
                } else {
                    UNIT_ASSERT_C(false, message.GetMessageGroupId());
                }
            }
            dataEvent.Commit();
        }
        if (committedOffsets.empty()) {
            UNIT_ASSERT_VALUES_EQUAL(committedCookies.size(), serverBatchesCount);
            for (ui64 i = 1; i <= serverBatchesCount; ++i) {
                UNIT_ASSERT(committedCookies.contains(i));
            }
        } else {
            UNIT_ASSERT_VALUES_EQUAL(committedOffsets.size(), batches * messagesInBatch + 1);
            for (ui64 i = 0; i <= batches * messagesInBatch; ++i) {
                UNIT_ASSERT(committedOffsets.contains(i));
            }

        }
        UNIT_ASSERT_VALUES_EQUAL(executor->GetTasksAdded(), expectedTasks);

        setup.AssertNoEvents();
    }

    Y_UNIT_TEST(PacksBatches_BatchABitBiggerThanLimit) {
        const size_t serverBatchesCount = 2;
        const size_t messagesInServerBatchCount = 4;
        const size_t messageSize = 11;
        const size_t batchLimit = 20;
        const size_t batches = 4;
        const size_t messagesInBatch = 2;
        PacksBatchesImpl(serverBatchesCount, messagesInServerBatchCount, messageSize, batchLimit, batches, messagesInBatch);
    }

    Y_UNIT_TEST(PacksBatches_ExactlyTwoMessagesInBatch) {
        const size_t serverBatchesCount = 2;
        const size_t messagesInServerBatchCount = 4;
        const size_t messageSize = 10; // Exactly two messages in batch.
        const size_t batchLimit = 20;
        const size_t batches = 4;
        const size_t messagesInBatch = 2;
        PacksBatchesImpl(serverBatchesCount, messagesInServerBatchCount, messageSize, batchLimit, batches, messagesInBatch);
    }

    Y_UNIT_TEST(PacksBatches_BatchesEqualToServerBatches) {
        const size_t serverBatchesCount = 2;
        const size_t messagesInServerBatchCount = 4;
        const size_t messageSize = 10;
        const size_t batchLimit = 40; // As in server messages.
        const size_t batches = 2;
        const size_t messagesInBatch = 4;
        PacksBatchesImpl(serverBatchesCount, messagesInServerBatchCount, messageSize, batchLimit, batches, messagesInBatch);
    }

    Y_UNIT_TEST(PacksBatches_OneMessageInEveryBatch) {
        const size_t serverBatchesCount = 2;
        const size_t messagesInServerBatchCount = 4;
        const size_t messageSize = 100;
        const size_t batchLimit = 90; // One message in every batch.
        const size_t batches = 8;
        const size_t messagesInBatch = 1;
        PacksBatchesImpl(serverBatchesCount, messagesInServerBatchCount, messageSize, batchLimit, batches, messagesInBatch);
    }

    Y_UNIT_TEST(PacksBatches_BigBatchDecompressWithTwoBatchTasks) {
        const size_t serverBatchesCount = 1;
        const size_t messagesInServerBatchCount = 200; // Many messages in order to fit in two 512 KB-tasks packs.
        const size_t messageSize = 1000000;
        const size_t batchLimit = std::numeric_limits<size_t>::max();
        const size_t batches = 1;
        const size_t messagesInBatch = messagesInServerBatchCount;
        const size_t expectedDecompressionTasksCount = 2;
        PacksBatchesImpl(serverBatchesCount, messagesInServerBatchCount, messageSize, batchLimit, batches, messagesInBatch, expectedDecompressionTasksCount);
    }

    Y_UNIT_TEST(PacksBatches_DecompressesOneMessagePerTime) {
        const size_t serverBatchesCount = 1;
        const size_t messagesInServerBatchCount = 10; // Many messages in order to fit in two 512 KB-tasks packs.
        const size_t messageSize = 1000000;
        const size_t batchLimit = std::numeric_limits<size_t>::max();
        const size_t batches = 1;
        const size_t messagesInBatch = 10;
        const size_t expectedDecompressionTasksCount = 1;
        const size_t reorderedCycleSize = 1;
        const size_t memoryLimit = 10;
        PacksBatchesImpl(serverBatchesCount, messagesInServerBatchCount, messageSize, batchLimit, batches, messagesInBatch, expectedDecompressionTasksCount, reorderedCycleSize, memoryLimit);
    }

    Y_UNIT_TEST(UnpackBigBatchWithTwoPartitions) {
        TReadSessionImplTestSetup setup;

        setup.Settings.MaxMemoryUsageBytes(5000);
        setup.SuccessfulInit();
        TPartitionStream::TPtr stream1 = setup.CreatePartitionStream("TestTopic", "TestCluster", 1, 1);
        TPartitionStream::TPtr stream2 = setup.CreatePartitionStream("TestTopic", "TestCluster", 2, 2);


        const TString messageData = GenerateMessageData(100);
        const TString compressedMessageData = Compress(messageData);
        ui64 offset = 1;
        ui64 seqNo = 42;

        for (ui64 partition = 1; partition <= 2; ++partition) {
            TMockReadSessionProcessor::TServerReadInfo resp;
            resp.PartitionData(partition, "TestTopic", "TestCluster", partition, partition)
                .Batch("src_id", TInstant::Seconds(123), "127.0.0.1", { { "k", "v" }, { "k1", "v1" }});

            for (size_t i = 0; i < 50; ++i) {
                if (i == 22) {
                    resp.Batch("src_id2", TInstant::Seconds(123), "127.0.0.1", { { "k", "v" }, { "k1", "v1" }});
                }
                resp.Message(offset++, compressedMessageData, Ydb::PersQueue::V1::CODEC_GZIP, seqNo++);
            }
            setup.MockProcessor->AddServerResponse(resp);
        }

        {
            TVector<TReadSessionEvent::TEvent> events = setup.EventsQueue->GetEvents(true);
            UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
            UNIT_ASSERT_EVENT_TYPE(events[0], TReadSessionEvent::TDataReceivedEvent);
            TReadSessionEvent::TDataReceivedEvent& dataEvent = std::get<TReadSessionEvent::TDataReceivedEvent>(events[0]);
            UNIT_ASSERT_VALUES_EQUAL(dataEvent.GetMessages().size(), 50);
            UNIT_ASSERT_EQUAL(dataEvent.GetPartitionStream(), stream1);
        }

        {
            TVector<TReadSessionEvent::TEvent> events = setup.EventsQueue->GetEvents(true);
            UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
            UNIT_ASSERT_EVENT_TYPE(events[0], TReadSessionEvent::TDataReceivedEvent);
            TReadSessionEvent::TDataReceivedEvent& dataEvent = std::get<TReadSessionEvent::TDataReceivedEvent>(events[0]);
            UNIT_ASSERT_VALUES_EQUAL(dataEvent.GetMessages().size(), 50);
            UNIT_ASSERT_EQUAL(dataEvent.GetPartitionStream(), stream2);
        }

        setup.AssertNoEvents();
    }

    Y_UNIT_TEST(PartitionStreamStatus) {
        TReadSessionImplTestSetup setup;
        setup.SuccessfulInit();
        TPartitionStream::TPtr stream = setup.CreatePartitionStream();
        EXPECT_CALL(*setup.MockProcessor, OnStatusRequest(_))
            .WillOnce(Invoke([](const Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::Status& req) {
                UNIT_ASSERT_VALUES_EQUAL(req.topic().path(), "TestTopic");
                UNIT_ASSERT_VALUES_EQUAL(req.cluster(), "TestCluster");
                UNIT_ASSERT_VALUES_EQUAL(req.partition(), 1);
                UNIT_ASSERT_VALUES_EQUAL(req.assign_id(), 1);
            }));
        // Another assign id.
        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                               .PartitionStreamStatus(11, 34, TInstant::Seconds(4), "TestTopic", "TestCluster", 1 /*partition*/, 13/*assign id to ignore*/));

        // Proper assign id.
        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo().PartitionStreamStatus(30, 42, TInstant::Seconds(20)));

        stream->RequestStatus();

        TMaybe<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
        UNIT_ASSERT(event);
        UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TPartitionStreamStatusEvent);
        auto& statusEvent = std::get<TReadSessionEvent::TPartitionStreamStatusEvent>(*event);
        UNIT_ASSERT_VALUES_EQUAL(statusEvent.GetCommittedOffset(), 30);
        UNIT_ASSERT_VALUES_EQUAL(statusEvent.GetEndOffset(), 42);
        UNIT_ASSERT_VALUES_EQUAL(statusEvent.GetWriteWatermark(), TInstant::MilliSeconds(20000));

        setup.AssertNoEvents();
    }

    Y_UNIT_TEST(HoleBetweenOffsets) {
        TReadSessionImplTestSetup setup;
        setup.Settings.DecompressionExecutor(MakeIntrusive<TReorderingExecutor>(2ull));
        setup.SuccessfulInit();
        TPartitionStream::TPtr stream = setup.CreatePartitionStream();
        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                               .PartitionData(1)
                                               .Batch("src_id")
                                               .CompressMessage(1, "message1")
                                               .CompressMessage(2, "message2"));

        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                               .PartitionData(2)
                                               .Batch("src_id")
                                               .CompressMessage(10, "message3")
                                               .CompressMessage(11, "message4"));

        bool has1 = false;
        bool has2 = false;
        EXPECT_CALL(*setup.MockProcessor, OnCommitRequest(_))
            .WillRepeatedly(Invoke([&](const Ydb::PersQueue::V1::MigrationStreamingReadClientMessage::Commit& req) {
                Cerr << "Got commit req " << req << "\n";
                for (const auto& commit : req.cookies()) {
                    if (commit.partition_cookie() == 1) {
                        has1 = true;
                    } else if (commit.partition_cookie() == 2) {
                        has2 = true;
                    } else {
                        UNIT_ASSERT(false);
                    }
                }
                for (const auto& range : req.offset_ranges()) {
                    Cerr << "RANGE " << range.start_offset() << " " << range.end_offset() << "\n";
                    if (range.start_offset() == 3 && range.end_offset() == 12) has1 = true;
                    else if (range.start_offset() == 0 && range.end_offset() == 3) has2 = true;
                    else UNIT_ASSERT(false);
                }
            }));

        for (int i = 0; i < 2; ) {
            TMaybe<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
            UNIT_ASSERT(event);
            UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);
            TReadSessionEvent::TDataReceivedEvent& dataEvent = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
            Cerr << "got data event: " << dataEvent.DebugString() << "\n";
            dataEvent.Commit();

            i += dataEvent.GetMessagesCount();
        }

        UNIT_ASSERT(has1);
        UNIT_ASSERT(has2);

        setup.AssertNoEvents();
    }

    Y_UNIT_TEST(CommitOffsetTwiceIsError) {
        TReadSessionImplTestSetup setup;
        setup.SuccessfulInit();
        TPartitionStream::TPtr stream = setup.CreatePartitionStream();
        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                               .PartitionData(1)
                                               .Batch("src_id")
                                               .CompressMessage(1, "message1"));

        TMaybe<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
        UNIT_ASSERT(event);
        UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);
        TReadSessionEvent::TDataReceivedEvent& dataEvent = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);

        // First time.
        dataEvent.GetMessages()[0].Commit();

        UNIT_ASSERT_EXCEPTION(dataEvent.GetMessages()[0].Commit(), NYdb::TContractViolation);
    }

    Y_UNIT_TEST(DataReceivedCallbackReal) {
        NYdb::NPersQueue::NTests::TPersQueueYdbSdkTestSetup setup("ReadSession");
        auto settings = setup.GetReadSessionSettings();

        auto calledPromise = NThreading::NewPromise<void>();
        int time = 0;

        settings.EventHandlers_.SimpleDataHandlers([&](TReadSessionEvent::TDataReceivedEvent& event) {
            for (auto& message: event.GetMessages()) {
                ++time;
                Cerr << "GOT MESSAGE: " << message.DebugString(true) << "\n";
                UNIT_ASSERT_VALUES_EQUAL(message.GetData(), TStringBuilder() << "message" << time);
                if (time == 3) {
                    calledPromise.SetValue();
                }
            }
            UNIT_ASSERT(time <= 3);
        }, true);

        std::shared_ptr<IReadSession> session = setup.GetPersQueueClient().CreateReadSession(settings);

        UNIT_ASSERT(!calledPromise.GetFuture().Wait(TDuration::Seconds(5)));

        setup.WriteToTopic({"message1"}, false);
        setup.WriteToTopic({"message2"}, false);
        Sleep(TDuration::Seconds(1));
        setup.WriteToTopic({"message3"}, false);

        calledPromise.GetFuture().Wait();
        Sleep(TDuration::Seconds(10));
    }

    Y_UNIT_TEST(DataReceivedCallback) {
        TReadSessionImplTestSetup setup;
        setup.Settings.DecompressionExecutor(MakeIntrusive<TReorderingExecutor>(2ull));
        auto calledPromise = NThreading::NewPromise<void>();
        int time = 0;
        setup.Settings.EventHandlers_.DataReceivedHandler([&](TReadSessionEvent::TDataReceivedEvent& event) {
            for (ui32 i = 0; i < event.GetMessages().size(); ++i) {
                ++time;
                UNIT_ASSERT_VALUES_EQUAL(event.GetMessages()[i].GetData(), TStringBuilder() << "message" << time);

                if (time == 2) {
                    calledPromise.SetValue();
                }
            }
        });
        setup.SuccessfulInit();
        TPartitionStream::TPtr stream = setup.CreatePartitionStream();
        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                               .PartitionData(1)
                                               .Batch("src_id")
                                               .CompressMessage(1, "message1"));
        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                               .PartitionData(2)
                                               .Batch("src_id")
                                               .CompressMessage(2, "message2"));

        //
        // when the PartitionStreamClosed arrives the raw messages are deleted
        // we give time to process the messages
        //
        Sleep(TDuration::Seconds(2));

        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                               .ForcefulReleasePartitionStream());
        TMaybe<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
        UNIT_ASSERT(event);
        UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TPartitionStreamClosedEvent);
        calledPromise.GetFuture().Wait();
    }

    Y_UNIT_TEST(PartitionStreamCallbacks) {
        TReadSessionImplTestSetup setup;
        bool createCalled = false;
        setup.Settings.EventHandlers_.CreatePartitionStreamHandler([&](TReadSessionEvent::TCreatePartitionStreamEvent&) {
            createCalled = true;
        });
        auto destroyCalledPromise = NThreading::NewPromise<TReadSessionEvent::TDestroyPartitionStreamEvent>();
        auto destroyCalled = destroyCalledPromise.GetFuture();
        setup.Settings.EventHandlers_.DestroyPartitionStreamHandler([destroyCalledPromise = std::move(destroyCalledPromise)](TReadSessionEvent::TDestroyPartitionStreamEvent& event) mutable {
            destroyCalledPromise.SetValue(std::move(event));
        });
        auto closedCalledPromise = NThreading::NewPromise<void>();
        auto closedCalled = closedCalledPromise.GetFuture();
        setup.Settings.EventHandlers_.PartitionStreamClosedHandler([&](TReadSessionEvent::TPartitionStreamClosedEvent&) {
            closedCalledPromise.SetValue();
        });

        setup.SuccessfulInit();
        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                               .CreatePartitionStream());
        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                               .PartitionData(1)
                                               .Batch("src_id")
                                               .CompressMessage(1, "message1"));

        TMaybe<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
        UNIT_ASSERT(event);
        UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);

        UNIT_ASSERT(createCalled);
        UNIT_ASSERT(!destroyCalled.HasValue());
        UNIT_ASSERT(!closedCalled.HasValue());

        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                               .ReleasePartitionStream());
        destroyCalled.Wait();
        UNIT_ASSERT(!closedCalled.HasValue());

        destroyCalled.ExtractValue().Confirm();
        closedCalled.Wait();
    }

    Y_UNIT_TEST(CommonHandler) {
        bool createCalled = false;
        bool dataCalled = false;
        auto callPromise = NThreading::NewPromise<void>();
        TReadSessionImplTestSetup setup;
        setup.Settings.EventHandlers_.CommonHandler([&](TReadSessionEvent::TEvent& event) {
            if (std::holds_alternative<TReadSessionEvent::TCreatePartitionStreamEvent>(event)) {
                UNIT_ASSERT(!createCalled);
                createCalled = true;
            } else if (std::holds_alternative<TReadSessionEvent::TDataReceivedEvent>(event)) {
                UNIT_ASSERT(!dataCalled);
                dataCalled = true;
            } else {
                UNIT_ASSERT(false);
            }
            if (createCalled && dataCalled) {
                UNIT_ASSERT_C(!callPromise.HasValue(), "Event: " << DebugString(event));
                callPromise.SetValue();
            }
        });
        setup.SuccessfulInit();
        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                               .CreatePartitionStream());
        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                               .PartitionData(1)
                                               .Batch("src_id")
                                               .CompressMessage(1, "message1"));

        callPromise.GetFuture().Wait();
    }

    void SimpleDataHandlersImpl(bool withCommit, bool withGracefulRelease) {
        TReadSessionImplTestSetup setup;
        auto dataReceived = std::make_shared<NThreading::TPromise<void>>(NThreading::NewPromise<void>());
        auto dataReceivedFuture = dataReceived->GetFuture();
        std::shared_ptr<TMaybe<TReadSessionEvent::TDataReceivedEvent>> dataReceivedEvent = std::make_shared<TMaybe<TReadSessionEvent::TDataReceivedEvent>>();
        setup.Settings.EventHandlers_.SimpleDataHandlers([dataReceivedEvent,dataReceived](TReadSessionEvent::TDataReceivedEvent& event) mutable {
            *dataReceivedEvent = std::move(event);
            dataReceived->SetValue();
        }, withCommit, withGracefulRelease);
        setup.SuccessfulInit();

        auto commitCalled = std::make_shared<NThreading::TPromise<void>>(NThreading::NewPromise<void>());
        auto commitCalledFuture = commitCalled->GetFuture();

        if (withCommit) {
            EXPECT_CALL(*setup.MockProcessor, OnCommitRequest(_))
                .WillOnce([commitCalled](){ commitCalled->SetValue(); });
        }

        auto destroyCalled = std::make_shared<NThreading::TPromise<void>>(NThreading::NewPromise<void>());
        auto destroyCalledFuture = destroyCalled->GetFuture();
        EXPECT_CALL(*setup.MockProcessor, OnReleasedRequest(_))
            .WillOnce([destroyCalled](){ destroyCalled->SetValue(); });

        EXPECT_CALL(*setup.MockProcessor, OnStartReadRequest(_));
        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                               .CreatePartitionStream());

        UNIT_ASSERT(!dataReceivedFuture.Wait(TDuration::MilliSeconds(100)));

        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                               .PartitionData(1)
                                               .Batch("src_id")
                                               .CompressMessage(1, "msg")
                                               .Batch("src_id")
                                               .CompressMessage(2, "msg"));

        dataReceivedFuture.Wait();
        UNIT_ASSERT(*dataReceivedEvent);

        UNIT_ASSERT_VALUES_EQUAL((*dataReceivedEvent)->GetMessages().size(), 2);

        if (withCommit) {
            commitCalledFuture.Wait();
        } else {
            UNIT_ASSERT(!commitCalledFuture.Wait(TDuration::MilliSeconds(100)));
        }

        setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                               .ReleasePartitionStream());

        if (!withCommit && withGracefulRelease) {
            UNIT_ASSERT(!destroyCalledFuture.Wait(TDuration::MilliSeconds(100)));

            (*dataReceivedEvent)->Commit();
            UNIT_ASSERT(!destroyCalledFuture.Wait(TDuration::MilliSeconds(100)));
        }

        if (withCommit) {
            commitCalledFuture.Wait();
        }
        if (withCommit || withGracefulRelease) {
            setup.MockProcessor->AddServerResponse(TMockReadSessionProcessor::TServerReadInfo()
                                                   .CommitAcknowledgement(1));
        }

        destroyCalledFuture.Wait();

        // Guarantee that all callbacks are finished.
        setup.Session->Abort();
    }

    Y_UNIT_TEST(SimpleDataHandlers) {
        SimpleDataHandlersImpl(false, false);
    }

    Y_UNIT_TEST(SimpleDataHandlersWithCommit) {
        SimpleDataHandlersImpl(true, false);
    }

    Y_UNIT_TEST(SimpleDataHandlersWithGracefulRelease) {
        SimpleDataHandlersImpl(false, true);
    }

    Y_UNIT_TEST(SimpleDataHandlersWithGracefulReleaseWithCommit) {
        SimpleDataHandlersImpl(true, true);
    }

    Y_UNIT_TEST(LOGBROKER_7702) {
        using namespace NYdb::NPersQueue;

        using TServiceEvent =
            typename NTopic::TAReadSessionEvent<true>::TPartitionStreamStatusEvent;

#define UNIT_ASSERT_CONTROL_EVENT() \
        { \
            using TExpectedEvent = typename NTopic::TAReadSessionEvent<true>::TPartitionStreamStatusEvent; \
\
            size_t maxByteSize = std::numeric_limits<size_t>::max();\
            NTopic::TUserRetrievedEventsInfoAccumulator<true> accumulator; \
\
            auto event = sessionQueue.GetEventImpl(maxByteSize, accumulator); \
\
            UNIT_ASSERT(std::holds_alternative<TExpectedEvent>(event.GetEvent()));\
        }

#define UNIT_ASSERT_DATA_EVENT(count) \
        { \
            using TExpectedEvent = typename NTopic::TAReadSessionEvent<true>::TDataReceivedEvent; \
\
            size_t maxByteSize = std::numeric_limits<size_t>::max(); \
            NTopic::TUserRetrievedEventsInfoAccumulator<true> accumulator; \
\
            auto event = sessionQueue.GetEventImpl(maxByteSize, accumulator); \
\
            UNIT_ASSERT(std::holds_alternative<TExpectedEvent>(event.GetEvent())); \
            UNIT_ASSERT_VALUES_EQUAL(std::get<TExpectedEvent>(event.GetEvent()).GetMessagesCount(), count); \
        }

        NTopic::TAReadSessionSettings<true> settings;
        std::shared_ptr<TSingleClusterReadSessionImpl> session;
        auto cbCtx = std::make_shared<TCallbackContext<TSingleClusterReadSessionImpl>>(session);
        TReadSessionEventsQueue sessionQueue{settings};

        auto stream = MakeIntrusive<TPartitionStreamImpl>(1ull,
                                                                "",
                                                                "",
                                                                1ull,
                                                                1ull,
                                                                1ull,
                                                                0ull,
                                                                cbCtx);

        NTopic::TPartitionData<true> message;
        Ydb::PersQueue::V1::MigrationStreamingReadServerMessage_DataBatch_Batch* batch =
            message.mutable_batches()->Add();
        Ydb::PersQueue::V1::MigrationStreamingReadServerMessage_DataBatch_MessageData* messageData =
            batch->mutable_message_data()->Add();
        *messageData->mutable_data() = "*";

        auto data = std::make_shared<NTopic::TDataDecompressionInfo<true>>(std::move(message),
                                                                   cbCtx,
                                                                   false,
                                                                   0);

        std::atomic<bool> ready = true;

        stream->InsertDataEvent(0, 0, data, ready);
        stream->InsertEvent(TServiceEvent{stream, 0, 0, 0, {}});
        stream->InsertDataEvent(0, 0, data, ready);
        stream->InsertDataEvent(0, 0, data, ready);
        stream->InsertEvent(TServiceEvent{stream, 0, 0, 0, {}});
        stream->InsertEvent(TServiceEvent{stream, 0, 0, 0, {}});
        stream->InsertDataEvent(0, 0, data, ready);

        TDeferredActions actions;

        TPartitionStreamImpl::SignalReadyEvents(stream,
                                                &sessionQueue,
                                                actions);

        UNIT_ASSERT_DATA_EVENT(1);
        UNIT_ASSERT_CONTROL_EVENT();
        UNIT_ASSERT_DATA_EVENT(2);
        UNIT_ASSERT_CONTROL_EVENT();
        UNIT_ASSERT_CONTROL_EVENT();
        UNIT_ASSERT_DATA_EVENT(1);

#undef UNIT_ASSERT_CONTROL_EVENT
#undef UNIT_ASSERT_DATA_EVENT

        cbCtx->Cancel();
    }
}
