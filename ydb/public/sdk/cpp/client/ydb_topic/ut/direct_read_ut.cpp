#include "ut_utils/topic_sdk_test_setup.h"
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/ut_utils.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/common/executor_impl.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/impl/write_session.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/write_session.h>

#include <library/cpp/retry/retry_policy.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>

#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <future>

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


namespace NYdb::NTopic::NTests {


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


struct TStartPartitionSessionRequest {
    TPartitionId PartitionId;
    TPartitionSessionId PartitionSessionId;
    TNodeId NodeId;
    TGeneration Generation;
};


struct TMockReadSessionProcessor : public TMockProcessorFactory<Ydb::Topic::StreamReadMessage::FromClient, Ydb::Topic::StreamReadMessage::FromServer>::IProcessor {
    // Request to read.
    struct TClientReadInfo {
        TReadCallback Callback;
        Ydb::Topic::StreamReadMessage::FromServer* Dst;

        operator bool() const {
            return Dst != nullptr;
        }
    };

    // Response from server.
    struct TServerReadInfo {
        NYdbGrpc::TGrpcStatus Status;
        Ydb::Topic::StreamReadMessage::FromServer Response;

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

        TServerReadInfo& StartPartitionSessionRequest(TStartPartitionSessionRequest request) {
            auto* req = Response.mutable_start_partition_session_request();

            auto* session = req->mutable_partition_session();
            session->set_partition_session_id(request.PartitionSessionId);
            session->set_partition_id(request.PartitionId);

            auto* location = req->mutable_partition_location();
            location->set_node_id(request.NodeId);
            location->set_generation(request.Generation);

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

    void Read(Ydb::Topic::StreamReadMessage::FromServer* response, TReadCallback callback) override {
        with_lock (Lock) {
            UNIT_ASSERT(!ActiveRead);
            ActiveRead.Callback = std::move(callback);
            ActiveRead.Dst = response;
            if (!ReadResponses.empty()) {
                StartProcessReadImpl();
            }
        }
    }

    void Write(Ydb::Topic::StreamReadMessage::FromClient&& request, TWriteCallback callback) override {
        UNIT_ASSERT(!callback); // Read session doesn't set callbacks.
        using FromClient = Ydb::Topic::StreamReadMessage_FromClient;

        switch (request.client_message_case()) {
        case FromClient::kInitRequest:
            OnInitRequest(request.init_request());
            break;
        case FromClient::kReadRequest:
            OnReadRequest(request.read_request());
            break;
        case FromClient::kDirectReadAck:
            OnDirectReadAck(request.direct_read_ack());
            break;
        case FromClient::kStartPartitionSessionResponse:
            OnStartPartitionSessionResponse(request.start_partition_session_response());
            break;
        case FromClient::kStopPartitionSessionResponse:
            OnStopPartitionSessionResponse(request.stop_partition_session_response());
            break;
        case FromClient::CLIENT_MESSAGE_NOT_SET:
            UNIT_ASSERT_C(false, "Invalid request");
            break;
        default:
            Y_UNREACHABLE();
        }
    }

    MOCK_METHOD(void, OnInitRequest, (const Ydb::Topic::StreamReadMessage::InitRequest&), ());
    MOCK_METHOD(void, OnReadRequest, (const Ydb::Topic::StreamReadMessage::ReadRequest&), ());
    MOCK_METHOD(void, OnDirectReadAck, (const Ydb::Topic::StreamReadMessage::DirectReadAck&), ());
    MOCK_METHOD(void, OnStartPartitionSessionResponse, (const Ydb::Topic::StreamReadMessage::StartPartitionSessionResponse&), ());
    MOCK_METHOD(void, OnStopPartitionSessionResponse, (const Ydb::Topic::StreamReadMessage::StopPartitionSessionResponse&), ());

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

struct TMockDirectReadSessionProcessor : public TMockProcessorFactory<TDirectReadClientMessage, TDirectReadServerMessage>::IProcessor {
    // Request to read.
    struct TClientReadInfo {
        TReadCallback Callback;
        TDirectReadServerMessage* Dst;

        operator bool() const {
            return Dst != nullptr;
        }
    };

    // Response from server.
    struct TServerReadInfo {
        NYdbGrpc::TGrpcStatus Status;
        TDirectReadServerMessage Response;

        TServerReadInfo& Failure(grpc::StatusCode status = grpc::StatusCode::UNAVAILABLE, const TString& message = {}, bool internal = false) {
            Status.GRpcStatusCode = status;
            Status.InternalError = internal;
            Status.Msg = message;
            return *this;
        }

        TServerReadInfo& InitDirectReadResponse() {
            Response.mutable_init_direct_read_response();
            return *this;
        }

        TServerReadInfo& StartDirectReadPartitionSessionResponse(TPartitionSessionId partitionSessionId) {
            auto* resp = Response.mutable_start_direct_read_partition_session_response();
            resp->set_partition_session_id(partitionSessionId);
            return *this;
        }

        TServerReadInfo& StopDirectReadPartitionSession(Ydb::StatusIds::StatusCode status, TPartitionSessionId partitionSessionId) {
            auto* req = Response.mutable_stop_direct_read_partition_session();
            req->set_status(status);
            req->set_partition_session_id(partitionSessionId);
            return *this;
        }

        // Data helpers.
        TServerReadInfo& PartitionData(const TPartitionSessionId partitionSessionId) {
            Response.mutable_direct_read_response()->set_partition_session_id(partitionSessionId);
            auto* data = Response.mutable_direct_read_response()->mutable_partition_data();
            data->set_partition_session_id(partitionSessionId);
            return *this;
        }

        TServerReadInfo& Batch(
            const TString& producerId,
            Ydb::Topic::Codec codec,
            TInstant writeTimestamp = TInstant::MilliSeconds(42),
            const std::vector<std::pair<TString, TString>>& writeSessionMeta = {}
        ) {
            auto* batch = Response.mutable_direct_read_response()->mutable_partition_data()->add_batches();
            batch->set_producer_id(producerId);
            batch->set_codec(codec);
            *batch->mutable_written_at() = ::google::protobuf::util::TimeUtil::MillisecondsToTimestamp(writeTimestamp.MilliSeconds());
            auto* meta = batch->mutable_write_session_meta();
            for (auto&& [k, v] : writeSessionMeta) {
                (*meta)[k] = v;
            }
            return *this;
        }

        TServerReadInfo& Message(
            ui64 offset,
            const TString& data,
            ui64 seqNo = 1,
            TInstant createdAt = TInstant::MilliSeconds(42),
            i64 uncompressedSize = 135,
            const TString& messageGroupId = "",
            const std::vector<std::pair<TString, TString>>& meta = {}
        ) {
            const int lastBatch = Response.direct_read_response().partition_data().batches_size();
            UNIT_ASSERT(lastBatch > 0);
            auto* batch = Response.mutable_direct_read_response()->mutable_partition_data()->mutable_batches(lastBatch - 1);
            auto* req = batch->add_message_data();
            req->set_offset(offset);
            req->set_seq_no(seqNo);
            *req->mutable_created_at() = ::google::protobuf::util::TimeUtil::MillisecondsToTimestamp(createdAt.MilliSeconds());
            req->set_data(data);
            req->set_message_group_id(messageGroupId);
            req->set_uncompressed_size(uncompressedSize);
            for (auto&& [k, v] : meta) {
                auto* pair = req->add_metadata_items();
                pair->set_key(k);
                pair->set_value(v);
            }
            return *this;
        }
    };

    virtual ~TMockDirectReadSessionProcessor() {
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

    void Read(TDirectReadServerMessage* response, TReadCallback callback) override {
        with_lock (Lock) {
            UNIT_ASSERT(!ActiveRead);
            ActiveRead.Callback = std::move(callback);
            ActiveRead.Dst = response;
            if (!ReadResponses.empty()) {
                StartProcessReadImpl();
            }
        }
    }

    void Write(TDirectReadClientMessage&& request, TWriteCallback callback) override {
        UNIT_ASSERT(!callback); // Read session doesn't set callbacks.
        switch (request.client_message_case()) {
        case TDirectReadClientMessage::kInitDirectReadRequest:
            OnInitDirectReadRequest(request.init_direct_read_request());
            break;
        case TDirectReadClientMessage::kStartDirectReadPartitionSessionRequest:
            OnStartDirectReadPartitionSessionRequest(request.start_direct_read_partition_session_request());
            break;
        case TDirectReadClientMessage::kUpdateTokenRequest:
            OnUpdateTokenRequest(request.update_token_request());
            break;
        case TDirectReadClientMessage::CLIENT_MESSAGE_NOT_SET:
            UNIT_ASSERT_C(false, "Invalid request");
            break;
        }
    }

    MOCK_METHOD(void, OnInitDirectReadRequest, (const Ydb::Topic::StreamDirectReadMessage::InitDirectReadRequest&), ());
    MOCK_METHOD(void, OnStartDirectReadPartitionSessionRequest, (const Ydb::Topic::StreamDirectReadMessage::StartDirectReadPartitionSessionRequest&), ());
    MOCK_METHOD(void, OnUpdateTokenRequest, (const Ydb::Topic::UpdateTokenRequest&), ());

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
        CallbackFutures.push(std::async(std::launch::async, &TMockDirectReadSessionProcessor::ProcessRead, this));
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


// Class for testing read session impl with mocks.
class TDirectReadSessionImplTestSetup {
public:
    // Types
    using IDirectReadSessionConnectionProcessorFactory = ISessionConnectionProcessorFactory<TDirectReadClientMessage, TDirectReadServerMessage>;
    using TMockDirectReadProcessorFactory = TMockProcessorFactory<TDirectReadClientMessage, TDirectReadServerMessage>;
    using TMockReadProcessorFactory = TMockProcessorFactory<Ydb::Topic::StreamReadMessage::FromClient, Ydb::Topic::StreamReadMessage::FromServer>;

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
    TDirectReadSessionImplTestSetup();
    ~TDirectReadSessionImplTestSetup() noexcept(false); // Performs extra validation and UNIT_ASSERTs

    TSingleClusterReadSessionImpl<false>* GetControlSession();
    TDirectReadSession* GetDirectReadSession(TDirectReadSession::TOnDirectReadDoneCallback, TDirectReadSession::TOnAbortSessionCallback);

    std::shared_ptr<TReadSessionEventsQueue<false>> GetEventsQueue();
    IExecutor::TPtr GetDefaultExecutor();

    void SuccessfulInit(bool flag = true);

    void AddControlResponse(TMockReadSessionProcessor::TServerReadInfo&);
    void AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo&);

    // Assertions.
    void AssertNoEvents();

public:
    // Members
    TReadSessionSettings ReadSessionSettings;
    TLog Log = CreateLogBackend("cerr");
    std::shared_ptr<TReadSessionEventsQueue<false>> EventsQueue;
    std::shared_ptr<TFakeContext> FakeContext = std::make_shared<TFakeContext>();
    std::shared_ptr<TMockReadProcessorFactory> MockReadProcessorFactory = std::make_shared<TMockReadProcessorFactory>();
    std::shared_ptr<TMockDirectReadProcessorFactory> MockDirectReadProcessorFactory = std::make_shared<TMockDirectReadProcessorFactory>();
    TIntrusivePtr<TMockReadSessionProcessor> MockReadProcessor = MakeIntrusive<TMockReadSessionProcessor>();
    TIntrusivePtr<TMockDirectReadSessionProcessor> MockDirectReadProcessor = MakeIntrusive<TMockDirectReadSessionProcessor>();

    TSingleClusterReadSessionImpl<false>::TPtr Session;
    std::shared_ptr<TCallbackContext<TSingleClusterReadSessionImpl<false>>> CbContext;

    TDirectReadSession::TPtr DirectReadSession;
    std::shared_ptr<TCallbackContext<TDirectReadSession>> DirectReadSessionCbContext;

    std::shared_ptr<TThreadPool> ThreadPool;
    IExecutor::TPtr DefaultExecutor;
};

TDirectReadSessionImplTestSetup::TDirectReadSessionImplTestSetup() {
    ReadSessionSettings
        .DirectRead(true)
        .AppendTopics({"TestTopic"})
        .ConsumerName("TestConsumer")
        .RetryPolicy(NYdb::NTopic::IRetryPolicy::GetFixedIntervalPolicy(TDuration::MilliSeconds(10)))
        .Counters(MakeIntrusive<NYdb::NTopic::TReaderCounters>(MakeIntrusive<::NMonitoring::TDynamicCounters>()));

    Log.SetFormatter(GetPrefixLogFormatter(""));
}

TDirectReadSessionImplTestSetup::~TDirectReadSessionImplTestSetup() noexcept(false) {
    if (!std::uncaught_exception()) { // Exiting from test successfully. Check additional expectations.
        MockReadProcessorFactory->Wait();
        MockReadProcessor->Wait();

        MockReadProcessorFactory->Validate();
        MockReadProcessor->Validate();

        MockDirectReadProcessorFactory->Wait();
        MockDirectReadProcessor->Wait();

        MockDirectReadProcessorFactory->Validate();
        MockDirectReadProcessor->Validate();
    }

    if (CbContext) {
        if (auto session = CbContext->LockShared()) {
            session->Close({});
        }
        CbContext->Cancel();
    }

    if (DirectReadSessionCbContext) {
        if (auto session = DirectReadSessionCbContext->LockShared()) {
            session->Close();
        }
        DirectReadSessionCbContext->Cancel();
    }

    Session = nullptr;

    if (ThreadPool) {
        ThreadPool->Stop();
    }
}

void TDirectReadSessionImplTestSetup::AddControlResponse(TMockReadSessionProcessor::TServerReadInfo& response) {
    MockReadProcessor->AddServerResponse(response);
}

void TDirectReadSessionImplTestSetup::AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo& response) {
    MockDirectReadProcessor->AddServerResponse(response);
}

void TDirectReadSessionImplTestSetup::SuccessfulInit(bool hasInitRequest) {
    EXPECT_CALL(*MockReadProcessorFactory, OnCreateProcessor(1))
        .WillOnce([&](){ MockReadProcessorFactory->CreateProcessor(MockReadProcessor); });
    if (hasInitRequest) {
        EXPECT_CALL(*MockReadProcessor, OnInitRequest(_));
    }
    AddControlResponse(TMockReadSessionProcessor::TServerReadInfo().InitResponse("session-1"));
    GetControlSession()->Start();
    MockReadProcessorFactory->Wait();
    MockReadProcessor->Wait();
}

std::shared_ptr<TReadSessionEventsQueue<false>> TDirectReadSessionImplTestSetup::GetEventsQueue() {
    if (!EventsQueue) {
        EventsQueue = std::make_shared<TReadSessionEventsQueue<false>>(ReadSessionSettings);
    }
    return EventsQueue;
}

void TDirectReadSessionImplTestSetup::AssertNoEvents() {
    TMaybe<TReadSessionEvent::TEvent> event = GetEventsQueue()->GetEvent(false);
    UNIT_ASSERT(!event);
}

IExecutor::TPtr TDirectReadSessionImplTestSetup::GetDefaultExecutor() {
    if (!DefaultExecutor) {
        ThreadPool = std::make_shared<TThreadPool>();
        ThreadPool->Start(1);
        DefaultExecutor = CreateThreadPoolExecutorAdapter(ThreadPool);
    }
    return DefaultExecutor;
}

TSingleClusterReadSessionImpl<false>* TDirectReadSessionImplTestSetup::GetControlSession() {
    if (!Session) {
        if (!ReadSessionSettings.DecompressionExecutor_) {
            ReadSessionSettings.DecompressionExecutor(GetDefaultExecutor());
        }
        if (!ReadSessionSettings.EventHandlers_.HandlersExecutor_) {
            ReadSessionSettings.EventHandlers_.HandlersExecutor(GetDefaultExecutor());
        }
        CbContext = MakeWithCallbackContext<TSingleClusterReadSessionImpl<false>>(
            ReadSessionSettings,
            "db",
            "client-session-id-1",
            "",
            Log,
            MockReadProcessorFactory,
            GetEventsQueue(),
            FakeContext,
            1,
            1,
            MockDirectReadProcessorFactory);
        Session = CbContext->TryGet();
    }
    return Session.get();
}

TDirectReadSession* TDirectReadSessionImplTestSetup::GetDirectReadSession(
    TDirectReadSession::TOnDirectReadDoneCallback readDoneCallback = {},
    TDirectReadSession::TOnAbortSessionCallback abortCallback = {}
) {
    if (!DirectReadSession) {
        DirectReadSessionCbContext = MakeWithCallbackContext<TDirectReadSession>(
            TNodeId(1),
            TString("client-session-id-1"),
            ReadSessionSettings,
            readDoneCallback,
            abortCallback,
            FakeContext,
            MockDirectReadProcessorFactory,
            Log);
        DirectReadSession = DirectReadSessionCbContext->TryGet();
    }
    return DirectReadSession.get();
}


Y_UNIT_TEST_SUITE(DirectReadWithClient) {

    /*
    This suite tests direct read mode only through IReadSession, without using internal classes.
    */

    Y_UNIT_TEST(OneMessage) {
        /*
        The simplest case: write one message and read it back.
        */

        TTopicSdkTestSetup setup(TEST_CASE_NAME);
        TTopicClient client = setup.MakeClient();

        {
            // Write a message:

            auto settings = TWriteSessionSettings()
                .Path(TEST_TOPIC)
                .ProducerId(TEST_MESSAGE_GROUP_ID)
                .MessageGroupId(TEST_MESSAGE_GROUP_ID);
            auto writer = client.CreateSimpleBlockingWriteSession(settings);
            UNIT_ASSERT(writer->Write("message"));
            writer->Close();
        }

        {
            // Read the message:

            auto settings = TReadSessionSettings()
                .ConsumerName(TEST_CONSUMER)
                .AppendTopics(TEST_TOPIC)
                .DirectRead(true);
            auto reader = client.CreateReadSession(settings);

            {
                // Start partition session:
                auto event = reader->GetEvent(true);
                UNIT_ASSERT(event.Defined());
                UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TStartPartitionSessionEvent);
                std::get<TReadSessionEvent::TStartPartitionSessionEvent>(*event).Confirm();
            }

            {
                // Receive the message and commit.
                auto event = reader->GetEvent(true);
                UNIT_ASSERT(event.Defined());
                UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);
                auto& dataReceived = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
                auto& messages = dataReceived.GetMessages();
                UNIT_ASSERT_EQUAL(messages.size(), 1);
                dataReceived.Commit();
            }

            {
                // Get commit ack.
                auto event = reader->GetEvent(true);
                UNIT_ASSERT(event.Defined());
                UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TCommitOffsetAcknowledgementEvent);
            }

        }
    }
} // Y_UNIT_TEST_SUITE(DirectReadWithClient)


Y_UNIT_TEST_SUITE(DirectReadWithControlSession) {

    /*
    This suite tests direct read sessions together with a control session.
    */

    void SuccessfulInitImpl(bool thenTimeout) {
        TDirectReadSessionImplTestSetup setup;
        setup.ReadSessionSettings
            .MaxLag(TDuration::Seconds(32))
            .ReadFromTimestamp(TInstant::Seconds(42));

        setup.ReadSessionSettings.Topics_[0]
            .ReadFromTimestamp(TInstant::Seconds(146))
            .AppendPartitionIds(100)
            .AppendPartitionIds(101);

        EXPECT_CALL(*setup.MockReadProcessorFactory, OnCreateProcessor(_))
            .WillOnce([&](){
                if (thenTimeout) {
                    setup.MockReadProcessorFactory->CreateAndThenTimeout(setup.MockReadProcessor);
                } else {
                    setup.MockReadProcessorFactory->CreateProcessor(setup.MockReadProcessor);
                }
            });
        EXPECT_CALL(*setup.MockReadProcessor, OnInitRequest(_))
            .WillOnce(Invoke([&setup](const Ydb::Topic::StreamReadMessage::InitRequest& req) {
                UNIT_ASSERT_STRINGS_EQUAL(req.consumer(), setup.ReadSessionSettings.ConsumerName_);
                UNIT_ASSERT(req.direct_read());
                UNIT_ASSERT_VALUES_EQUAL(req.topics_read_settings_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(req.topics_read_settings(0).path(), setup.ReadSessionSettings.Topics_[0].Path_);
                UNIT_ASSERT_VALUES_EQUAL(req.topics_read_settings(0).read_from().seconds(), 146);
                UNIT_ASSERT_VALUES_EQUAL(req.topics_read_settings(0).partition_ids_size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(req.topics_read_settings(0).partition_ids(0), 100);
                UNIT_ASSERT_VALUES_EQUAL(req.topics_read_settings(0).partition_ids(1), 101);
            }));
        setup.GetControlSession()->Start();
        setup.MockReadProcessorFactory->Wait();

        EXPECT_CALL(*setup.MockReadProcessor, OnReadRequest(_));
        setup.AddControlResponse(TMockReadSessionProcessor::TServerReadInfo().InitResponse("session id"));

        setup.AssertNoEvents();
    }

    Y_UNIT_TEST(Init) {
        SuccessfulInitImpl(false);
    }

    Y_UNIT_TEST(HappyWay) {
        TDirectReadSessionImplTestSetup setup;
        setup.ReadSessionSettings.Topics_[0].AppendPartitionIds(100);

        TString const serverSessionId = "server-session-id-1";

        auto const startPartitionSessionRequest = TStartPartitionSessionRequest{
            .PartitionId = 1,
            .PartitionSessionId = 2,
            .NodeId = 3,
            .Generation = 4,
        };

        {
            ::testing::InSequence sequence;

            EXPECT_CALL(*setup.MockReadProcessorFactory, OnCreateProcessor(_))
                .WillOnce([&]() {
                    setup.MockReadProcessorFactory->CreateProcessor(setup.MockReadProcessor);
                });

            EXPECT_CALL(*setup.MockReadProcessor, OnInitRequest(_))
                .WillOnce(Invoke([&setup](const Ydb::Topic::StreamReadMessage::InitRequest& req) {
                    UNIT_ASSERT(req.direct_read());
                    UNIT_ASSERT_EQUAL(req.topics_read_settings_size(), 1);
                    UNIT_ASSERT_EQUAL(req.topics_read_settings(0).path(), setup.ReadSessionSettings.Topics_[0].Path_);
                    UNIT_ASSERT_EQUAL(req.topics_read_settings(0).partition_ids_size(), 1);
                    UNIT_ASSERT_EQUAL(req.topics_read_settings(0).partition_ids(0), 100);
                }));

            EXPECT_CALL(*setup.MockReadProcessor, OnReadRequest(_));

            EXPECT_CALL(*setup.MockReadProcessor, OnStartPartitionSessionResponse(_))
                .WillOnce(Invoke([&startPartitionSessionRequest](const Ydb::Topic::StreamReadMessage::StartPartitionSessionResponse& resp) {
                    UNIT_ASSERT_EQUAL(resp.partition_session_id(), startPartitionSessionRequest.PartitionSessionId);
                }));
        }

        // There are two sequences, because OnCreateProcessor from the second sequence may be called
        // before OnStartPartitionSessionResponse from the first sequence.

        {
            ::testing::InSequence sequence;

            EXPECT_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(_))
                .WillOnce([&]() {
                    setup.MockDirectReadProcessorFactory->CreateProcessor(setup.MockDirectReadProcessor);
                });

            EXPECT_CALL(*setup.MockDirectReadProcessor, OnInitDirectReadRequest(_))
                .WillOnce(Invoke([&serverSessionId, &setup](const Ydb::Topic::StreamDirectReadMessage::InitDirectReadRequest& req) {
                    UNIT_ASSERT_EQUAL(req.session_id(), serverSessionId);
                    UNIT_ASSERT_VALUES_EQUAL(req.topics_read_settings_size(), setup.ReadSessionSettings.Topics_.size());
                    UNIT_ASSERT_VALUES_EQUAL(req.topics_read_settings(0).path(), setup.ReadSessionSettings.Topics_[0].Path_);
                    UNIT_ASSERT_VALUES_EQUAL(req.consumer(), setup.ReadSessionSettings.ConsumerName_);
                }));

            EXPECT_CALL(*setup.MockDirectReadProcessor, OnStartDirectReadPartitionSessionRequest(_))
                .WillOnce(Invoke([&startPartitionSessionRequest](const Ydb::Topic::StreamDirectReadMessage::StartDirectReadPartitionSessionRequest& request) {
                    UNIT_ASSERT_VALUES_EQUAL(request.partition_session_id(), startPartitionSessionRequest.PartitionSessionId);
                    UNIT_ASSERT_VALUES_EQUAL(request.generation(), startPartitionSessionRequest.Generation);
                }));

            // Expect OnReadRequest in case it is called before the test ends.
            EXPECT_CALL(*setup.MockReadProcessor, OnReadRequest(_)).Times(AtMost(1));
        }

        setup.GetControlSession()->Start();
        setup.MockReadProcessorFactory->Wait();
        setup.AddControlResponse(TMockReadSessionProcessor::TServerReadInfo().InitResponse(serverSessionId));
        setup.AddControlResponse(TMockReadSessionProcessor::TServerReadInfo().StartPartitionSessionRequest(startPartitionSessionRequest));

        {
            TMaybe<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
            UNIT_ASSERT(event);
            UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TStartPartitionSessionEvent);
            auto e = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*event);
            e->Confirm();
        }

        setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
            .InitDirectReadResponse());

        setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
            .StartDirectReadPartitionSessionResponse(startPartitionSessionRequest.PartitionSessionId));

        setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
            .PartitionData(startPartitionSessionRequest.PartitionSessionId)
            .Batch("producer-id-1", Ydb::Topic::Codec::CODEC_RAW)
            .Message(0, "message-1")
            .Message(1, "message-2"));

        {
            // Verify that the session receives data sent to direct read session:

            TMaybe<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
            UNIT_ASSERT(event);
            UNIT_ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);
            auto e = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
            UNIT_ASSERT_EQUAL(e->GetMessagesCount(), 2);
        }

        setup.MockDirectReadProcessorFactory->Wait();

        setup.AssertNoEvents();
    }

} // Y_UNIT_TEST_SUITE(DirectReadWithControlSession)


Y_UNIT_TEST_SUITE(DirectReadSession) {

    /*
    This suite test TDirectReadSession in isolation, without control session.
    */

    Y_UNIT_TEST(DirectReadSession) {
        TDirectReadSessionImplTestSetup setup;

        auto gotStart = NThreading::NewPromise();

        TPartitionSessionId partitionSessionId = 1;
        TPartitionLocation location = {2, 3};

        {
            ::testing::InSequence sequence;

            EXPECT_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(_))
                .WillOnce([&]() {
                    setup.MockDirectReadProcessorFactory->CreateProcessor(setup.MockDirectReadProcessor);
                });

            EXPECT_CALL(*setup.MockDirectReadProcessor, OnInitDirectReadRequest(_))
                .WillOnce(Invoke([](const Ydb::Topic::StreamDirectReadMessage::InitDirectReadRequest&) {}));

            EXPECT_CALL(*setup.MockDirectReadProcessor, OnStartDirectReadPartitionSessionRequest(_))
                .WillOnce(Invoke([&gotStart, partitionSessionId](const Ydb::Topic::StreamDirectReadMessage::StartDirectReadPartitionSessionRequest& req) {
                    UNIT_ASSERT_EQUAL(req.partition_session_id(), static_cast<i64>(partitionSessionId));
                    gotStart.SetValue();
                }));
        }

        auto session = setup.GetDirectReadSession();
        session->Start();
        session->AddPartitionSession({ .PartitionSessionId = partitionSessionId, .Location = location });

        setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
            .InitDirectReadResponse());

        setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
            .StartDirectReadPartitionSessionResponse(1));

        gotStart.GetFuture().Wait();
    }

    Y_UNIT_TEST(NoRetry) {
        TDirectReadSessionImplTestSetup setup;
        setup.ReadSessionSettings.RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy());

        auto gotClosedEvent = NThreading::NewPromise();

        EXPECT_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(_))
            .WillOnce([&]() { setup.MockDirectReadProcessorFactory->FailCreation(); });

        auto session = setup.GetDirectReadSession({}, [&gotClosedEvent](TSessionClosedEvent&&) { gotClosedEvent.SetValue(); });
        session->Start();
        setup.MockDirectReadProcessorFactory->Wait();
        gotClosedEvent.GetFuture().Wait();
    }

    Y_UNIT_TEST(Retry) {
        TDirectReadSessionImplTestSetup setup;
        size_t nRetries = 2;
        setup.ReadSessionSettings.RetryPolicy(NYdb::NTopic::IRetryPolicy::GetFixedIntervalPolicy(
            TDuration::MilliSeconds(1), TDuration::MilliSeconds(1), nRetries));

        auto gotClosedEvent = NThreading::NewPromise();

        ON_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(_))
            .WillByDefault([&]() { setup.MockDirectReadProcessorFactory->FailCreation(); });

        EXPECT_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(_))
            .Times(1 + nRetries);  // First call + N retries.

        auto session = setup.GetDirectReadSession({}, [&gotClosedEvent](TSessionClosedEvent&&) { gotClosedEvent.SetValue(); });
        session->Start();
        setup.MockDirectReadProcessorFactory->Wait();

        gotClosedEvent.GetFuture().Wait();
    }

} // Y_UNIT_TEST_SUITE(DirectReadSession)

} // namespace NYdb::NTopic::NTests
