#include "setup/fixture.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <library/cpp/retry/retry_policy.h>

#include <ydb/public/sdk/cpp/src/client/topic/impl/common.h>
#include <ydb/public/sdk/cpp/src/client/topic/common/executor_impl.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/impl/write_session.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/write_session.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <algorithm>
#include <future>

using namespace ::testing; // Google mock.


#define ASSERT_EVENT_TYPE(event, type)                    \
    ASSERT_TRUE(std::holds_alternative<type>(event))      \
        << "Real event got: " << DebugString(event)

#define ASSERT_NOT_EVENT_TYPE(event, type)                \
    ASSERT_TRUE(!std::holds_alternative<type>(event))     \
        << "Real event got: " << DebugString(event)



namespace NYdb::inline Dev::NTopic::NTests {

namespace {
    const char* SERVER_SESSION_ID = "server-session-id-1";
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
        ASSERT_FALSE(ConnectedCallback) << "Only one connect at a time is expected";
        ASSERT_FALSE(ConnectTimeoutCallback) << "Only one connect at a time is expected";
        ConnectedCallback = callback;
        ConnectTimeoutCallback = connectTimeoutCallback;

        Y_UNUSED(requestSettings);
        // TODO Check requestSettings.PreferredEndpoint.GetNodeId()?
        EXPECT_TRUE(connectContext);
        EXPECT_TRUE(connectTimeout);
        EXPECT_TRUE(connectTimeoutContext);
        EXPECT_TRUE(connectTimeoutCallback);
        EXPECT_TRUE(!connectDelay || connectDelayOperationContext);

        OnCreateProcessor(++CreateCallsCount);
    }

    // Handler is called in CreateProcessor() method after parameter validation.
    MOCK_METHOD(void, OnCreateProcessor, (size_t callNumber)); // 1-based

    // Actions to use in OnCreateProcessor handler:
    void CreateProcessor(typename IFactory::IProcessor::TPtr processor) { // Success.
        EXPECT_TRUE(ConnectedCallback);
        auto cb = std::move(ConnectedCallback);
        ConnectedCallback = nullptr;
        ConnectTimeoutCallback = nullptr;
        {
            std::lock_guard lock(Lock);
            CallbackFutures.push(std::async(std::launch::async, std::move(cb), TPlainStatus(), processor));
        }
    }

    void FailCreation(EStatus status = EStatus::INTERNAL_ERROR, const std::string& message = {}) { // Fail.
        EXPECT_TRUE(ConnectedCallback);
        auto cb = std::move(ConnectedCallback);
        ConnectedCallback = nullptr;
        ConnectTimeoutCallback = nullptr;
        {
            std::lock_guard lock(Lock);
            CallbackFutures.push(std::async(std::launch::async, std::move(cb), TPlainStatus(status, message), nullptr));
        }
    }

    void Timeout() { // Timeout.
        EXPECT_TRUE(ConnectTimeoutCallback);
        auto cb = std::move(ConnectTimeoutCallback);
        ConnectedCallback = nullptr;
        ConnectTimeoutCallback = nullptr;
        {
            std::lock_guard lock(Lock);
            CallbackFutures.push(std::async(std::launch::async, std::move(cb), true));
        }
    }

    void CreateAndThenTimeout(typename IFactory::IProcessor::TPtr processor) {
        EXPECT_TRUE(ConnectedCallback);
        EXPECT_TRUE(ConnectTimeoutCallback);
        auto cb2 = [cbt = std::move(ConnectTimeoutCallback), cb = std::move(ConnectedCallback), processor]() mutable {
            cb(TPlainStatus(), std::move(processor));
            cbt(true);
        };
        ConnectedCallback = nullptr;
        ConnectTimeoutCallback = nullptr;
        {
            std::lock_guard lock(Lock);
            CallbackFutures.push(std::async(std::launch::async, std::move(cb2)));
        }
    }

    void FailAndThenTimeout(EStatus status = EStatus::INTERNAL_ERROR, const std::string& message = {}) {
        EXPECT_TRUE(ConnectedCallback);
        EXPECT_TRUE(ConnectTimeoutCallback);
        auto cb2 = [cbt = std::move(ConnectTimeoutCallback), cb = std::move(ConnectedCallback), status, message]() mutable {
            cb(TPlainStatus(status, message), nullptr);
            cbt(true);
        };
        ConnectedCallback = nullptr;
        ConnectTimeoutCallback = nullptr;
        {
            std::lock_guard lock(Lock);
            CallbackFutures.push(std::async(std::launch::async, std::move(cb2)));
        }
    }

    void TimeoutAndThenCreate(typename IFactory::IProcessor::TPtr processor) {
        EXPECT_TRUE(ConnectedCallback);
        EXPECT_TRUE(ConnectTimeoutCallback);
        auto cb2 = [cbt = std::move(ConnectTimeoutCallback), cb = std::move(ConnectedCallback), processor]() mutable {
            cbt(true);
            cb(TPlainStatus(), std::move(processor));
        };
        ConnectedCallback = nullptr;
        ConnectTimeoutCallback = nullptr;
        {
            std::lock_guard lock(Lock);
            CallbackFutures.push(std::async(std::launch::async, std::move(cb2)));
        }
    }

    void Wait() {
        std::queue<std::future<void>> futuresQueue;
        {
            std::lock_guard lock(Lock);
            CallbackFutures.swap(futuresQueue);
        }
        while (!futuresQueue.empty()) {
            futuresQueue.front().wait();
            futuresQueue.pop();
        }
    }

    void Validate() {
        EXPECT_TRUE(CallbackFutures.empty());
        ConnectedCallback = nullptr;
        ConnectTimeoutCallback = nullptr;
    }

    std::atomic<std::size_t> CreateCallsCount = 0;

private:
    std::mutex Lock;
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

struct TStopPartitionSessionRequest {
    TPartitionSessionId PartitionSessionId;
    bool Graceful;
    std::int64_t CommittedOffset;
    TDirectReadId LastDirectReadId;
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

        TServerReadInfo& Failure(grpc::StatusCode status = grpc::StatusCode::UNAVAILABLE, const std::string& message = {}, bool internal = false) {
            Status.GRpcStatusCode = status;
            Status.InternalError = internal;
            Status.Msg = message;
            return *this;
        }

        TServerReadInfo& InitResponse(const std::string& sessionId) {
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

        TServerReadInfo& StopPartitionSession(TStopPartitionSessionRequest request) {
            auto* req = Response.mutable_stop_partition_session_request();
            req->set_partition_session_id(request.PartitionSessionId);
            req->set_graceful(request.Graceful);
            req->set_committed_offset(request.CommittedOffset);
            req->set_last_direct_read_id(request.LastDirectReadId);
            return *this;
        }

    };

    ~TMockReadSessionProcessor() {
        Wait();
    }

    void Cancel() override {
    }

    void ReadInitialMetadata(std::unordered_multimap<std::string, std::string>* metadata, TReadCallback callback) override {
        Y_UNUSED(metadata);
        Y_UNUSED(callback);
        EXPECT_TRUE(false) << "This method is not expected to be called";
    }

    void Finish(TReadCallback callback) override {
        Y_UNUSED(callback);
        EXPECT_TRUE(false) << "This method is not expected to be called";
    }

    void AddFinishedCallback(TReadCallback callback) override {
        Y_UNUSED(callback);
        EXPECT_TRUE(false) << "This method is not expected to be called";
    }

    void Read(Ydb::Topic::StreamReadMessage::FromServer* response, TReadCallback callback) override {
        {
            std::lock_guard lock(Lock);
            EXPECT_FALSE(ActiveRead);
            ActiveRead.Callback = std::move(callback);
            ActiveRead.Dst = response;
            if (!ReadResponses.empty()) {
                StartProcessReadImpl();
            }
        }
    }

    void StartProcessReadImpl() {
        CallbackFutures.push(std::async(std::launch::async, &TMockReadSessionProcessor::ProcessRead, this));
    }

    void Write(Ydb::Topic::StreamReadMessage::FromClient&& request, TWriteCallback callback) override {
        EXPECT_FALSE(callback); // Read session doesn't set callbacks.
        using FromClient = Ydb::Topic::StreamReadMessage_FromClient;

        switch (request.client_message_case()) {
        case FromClient::kInitRequest:
            OnInitRequest(request.init_request());
            break;
        case FromClient::kReadRequest:
            OnReadRequest(request.read_request());
            break;
        case FromClient::kCommitOffsetRequest:
            OnCommitOffsetRequest(request.commit_offset_request());
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
            EXPECT_TRUE(false) << "Invalid request";
            break;
        default:
            Y_UNREACHABLE();
        }
    }
    MOCK_METHOD(void, OnInitRequest, (const Ydb::Topic::StreamReadMessage::InitRequest&), ());
    MOCK_METHOD(void, OnReadRequest, (const Ydb::Topic::StreamReadMessage::ReadRequest&), ());
    MOCK_METHOD(void, OnDirectReadAck, (const Ydb::Topic::StreamReadMessage::DirectReadAck&), ());
    MOCK_METHOD(void, OnCommitOffsetRequest, (const Ydb::Topic::StreamReadMessage::CommitOffsetRequest&), ());
    MOCK_METHOD(void, OnStartPartitionSessionResponse, (const Ydb::Topic::StreamReadMessage::StartPartitionSessionResponse&), ());
    MOCK_METHOD(void, OnStopPartitionSessionResponse, (const Ydb::Topic::StreamReadMessage::StopPartitionSessionResponse&), ());

    void Wait() {
        std::queue<std::future<void>> callbackFutures;
        {
            std::lock_guard lock(Lock);
            CallbackFutures.swap(callbackFutures);
        }

        while (!callbackFutures.empty()) {
            callbackFutures.front().wait();
            callbackFutures.pop();
        }
    }

    void Validate() {
        {
            std::lock_guard lock(Lock);
            EXPECT_TRUE(ReadResponses.empty());
            EXPECT_TRUE(CallbackFutures.empty());

            ActiveRead = TClientReadInfo{};
        }
    }

    void ProcessRead() {
        NYdbGrpc::TGrpcStatus status;
        TReadCallback callback;
        {
            std::lock_guard lock(Lock);
            if (ActiveRead) {
                *ActiveRead.Dst = ReadResponses.front().Response;
                ActiveRead.Dst = nullptr;
                status = std::move(ReadResponses.front().Status);
                ReadResponses.pop();
                callback = std::move(ActiveRead.Callback);
            }
        }
        if (callback) {
            callback(std::move(status));
        }
    }

    void AddServerResponse(TServerReadInfo result) {
        NYdbGrpc::TGrpcStatus status;
        TReadCallback callback;
        {
            std::lock_guard lock(Lock);
            ReadResponses.emplace(std::move(result));
            if (ActiveRead) {
                *ActiveRead.Dst = ReadResponses.front().Response;
                ActiveRead.Dst = nullptr;
                status = std::move(ReadResponses.front().Status);
                ReadResponses.pop();
                callback = std::move(ActiveRead.Callback);
            }
        }
        if (callback) {
            callback(std::move(status));
        }
    }

    std::mutex Lock;
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

        TServerReadInfo& Failure(grpc::StatusCode status = grpc::StatusCode::UNAVAILABLE, const std::string& message = {}, bool internal = false) {
            Status.GRpcStatusCode = status;
            Status.InternalError = internal;
            Status.Msg = message;
            return *this;
        }

        TServerReadInfo& InitResponse() {
            Response.mutable_init_response();
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
        TServerReadInfo& PartitionData(TPartitionSessionId partitionSessionId, TDirectReadId directReadId, std::uint64_t bytesSize = 0) {
            auto* response = Response.mutable_direct_read_response();
            response->set_partition_session_id(partitionSessionId);
            response->set_direct_read_id(directReadId);
            response->set_bytes_size(bytesSize);
            response->mutable_partition_data()->set_partition_session_id(partitionSessionId);
            return *this;
        }

        TServerReadInfo& Batch(
            const std::string& producerId,
            Ydb::Topic::Codec codec,
            TInstant writeTimestamp = TInstant::MilliSeconds(42),
            const std::vector<std::pair<std::string, std::string>>& writeSessionMeta = {}
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
            std::uint64_t offset,
            const std::string& data,
            std::uint64_t seqNo = 1,
            TInstant createdAt = TInstant::MilliSeconds(42),
            std::int64_t uncompressedSize = 135,
            const std::string& messageGroupId = "",
            const std::vector<std::pair<std::string, std::string>>& meta = {}
        ) {
            const int lastBatch = Response.direct_read_response().partition_data().batches_size();
            EXPECT_GT(lastBatch, 0);
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

    void ReadInitialMetadata(std::unordered_multimap<std::string, std::string>* metadata, TReadCallback callback) override {
        Y_UNUSED(metadata);
        Y_UNUSED(callback);
        EXPECT_TRUE(false) << "This method is not expected to be called";
    }

    void Finish(TReadCallback callback) override {
        Y_UNUSED(callback);
        EXPECT_TRUE(false) << "This method is not expected to be called";
    }

    void AddFinishedCallback(TReadCallback callback) override {
        Y_UNUSED(callback);
        EXPECT_TRUE(false) << "This method is not expected to be called";
    }

    void Read(TDirectReadServerMessage* response, TReadCallback callback) override {
        NYdbGrpc::TGrpcStatus status;
        TReadCallback cb;
        {
            std::lock_guard lock(Lock);
            std::cerr << "XXXXX Read 1 " << response->DebugString() << "\n";
            EXPECT_FALSE(ActiveRead);
            ActiveRead.Callback = std::move(callback);
            ActiveRead.Dst = response;
            if (!ReadResponses.empty()) {
                std::cerr << "XXXXX Read 2 " << response->DebugString() << "\n";
                *ActiveRead.Dst = ReadResponses.front().Response;
                ActiveRead.Dst = nullptr;
                status = std::move(ReadResponses.front().Status);
                ReadResponses.pop();
                cb = std::move(ActiveRead.Callback);
            }
        }
        if (cb) {
            std::cerr << "XXXXX Read 3 " << response->DebugString() << "\n";
            cb(std::move(status));
        }
    }

    void StartProcessReadImpl() {
        CallbackFutures.push(std::async(std::launch::async, &TMockDirectReadSessionProcessor::ProcessRead, this));
    }

    void Write(TDirectReadClientMessage&& request, TWriteCallback callback) override {
        EXPECT_FALSE(callback); // Read session doesn't set callbacks.
        switch (request.client_message_case()) {
        case TDirectReadClientMessage::kInitRequest:
            OnInitRequest(request.init_request());
            break;
        case TDirectReadClientMessage::kStartDirectReadPartitionSessionRequest:
            OnStartDirectReadPartitionSessionRequest(request.start_direct_read_partition_session_request());
            break;
        case TDirectReadClientMessage::kUpdateTokenRequest:
            OnUpdateTokenRequest(request.update_token_request());
            break;
        case TDirectReadClientMessage::CLIENT_MESSAGE_NOT_SET:
            EXPECT_TRUE(false) << "Invalid request";
            break;
        }
    }

    MOCK_METHOD(void, OnInitRequest, (const Ydb::Topic::StreamDirectReadMessage::InitRequest&), ());
    MOCK_METHOD(void, OnStartDirectReadPartitionSessionRequest, (const Ydb::Topic::StreamDirectReadMessage::StartDirectReadPartitionSessionRequest&), ());
    MOCK_METHOD(void, OnUpdateTokenRequest, (const Ydb::Topic::UpdateTokenRequest&), ());

    void Wait() {
        std::queue<std::future<void>> callbackFutures;
        {
            std::lock_guard lock(Lock);
            CallbackFutures.swap(callbackFutures);
        }

        while (!callbackFutures.empty()) {
            callbackFutures.front().wait();
            callbackFutures.pop();
        }
    }

    void Validate() {
        std::cerr << "XXXXX Validate\n";
        {
            std::lock_guard lock(Lock);
            EXPECT_TRUE(ReadResponses.empty());
            EXPECT_TRUE(CallbackFutures.empty());

            ActiveRead = TClientReadInfo{};
        }
    }

    void ProcessRead() {
        std::cerr << "XXXXX ProcessRead\n";
        NYdbGrpc::TGrpcStatus status;
        TReadCallback callback;
        // GotActiveRead.GetFuture().Wait();
        {
            std::lock_guard lock(Lock);
            *ActiveRead.Dst = ReadResponses.front().Response;
            ActiveRead.Dst = nullptr;
            status = std::move(ReadResponses.front().Status);
            ReadResponses.pop();
            callback = std::move(ActiveRead.Callback);
        }
        callback(std::move(status));
    }

    void AddServerResponse(TServerReadInfo result) {
        NYdbGrpc::TGrpcStatus status;
        TReadCallback callback;
        {
            std::lock_guard lock(Lock);
            std::cerr << "XXXXX AddServerResponse 1 " << result.Response.DebugString() << "\n";
            ReadResponses.emplace(std::move(result));
            if (ActiveRead) {
                std::cerr << "XXXXX AddServerResponse 2\n";
                *ActiveRead.Dst = ReadResponses.front().Response;
                ActiveRead.Dst = nullptr;
                status = std::move(ReadResponses.front().Status);
                ReadResponses.pop();
                callback = std::move(ActiveRead.Callback);
            }
        }
        if (callback) {
            std::cerr << "XXXXX AddServerResponse 3\n";
            callback(std::move(status));
        }
    }

    std::mutex Lock;
    // NThreading::TPromise<void> GotActiveRead = NThreading::NewPromise();
    TClientReadInfo ActiveRead;
    std::queue<TServerReadInfo> ReadResponses;
    std::queue<std::future<void>> CallbackFutures;
};

class TMockRetryPolicy : public IRetryPolicy {
public:
    MOCK_METHOD(IRetryPolicy::IRetryState::TPtr, CreateRetryState, (), (const, override));
    TMaybe<TDuration> Delay;
};

class TMockRetryState : public IRetryPolicy::IRetryState {
public:
    TMockRetryState(std::shared_ptr<TMockRetryPolicy> policy)
        : Policy(policy) {}

    TMaybe<TDuration> GetNextRetryDelay(EStatus) {
        return Policy->Delay;
    }
private:
    std::shared_ptr<TMockRetryPolicy> Policy;
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
            EXPECT_TRUE(false) << "This method is not expected to be called";
            return nullptr;
        }

        bool IsCancelled() const override {
            EXPECT_TRUE(false) << "This method is not expected to be called";
            return false;
        }

        bool Cancel() override {
            return false;
        }

        void SubscribeCancel(std::function<void()>) override {
            EXPECT_TRUE(false) << "This method is not expected to be called";
        }
    };

    // Methods
    TDirectReadSessionImplTestSetup();
    ~TDirectReadSessionImplTestSetup() noexcept(false); // Performs extra validation and UNIT_ASSERTs

    TSingleClusterReadSessionImpl<false>* GetControlSession();
    TDirectReadSession* GetDirectReadSession(IDirectReadSessionControlCallbacks::TPtr);
    void WaitForWorkingDirectReadSession();

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
    std::shared_ptr<TMockRetryPolicy> MockRetryPolicy = std::make_shared<TMockRetryPolicy>();
    std::shared_ptr<TMockReadProcessorFactory> MockReadProcessorFactory = std::make_shared<TMockReadProcessorFactory>();
    std::shared_ptr<TMockDirectReadProcessorFactory> MockDirectReadProcessorFactory = std::make_shared<TMockDirectReadProcessorFactory>();
    TIntrusivePtr<TMockReadSessionProcessor> MockReadProcessor = MakeIntrusive<TMockReadSessionProcessor>();
    TIntrusivePtr<TMockDirectReadSessionProcessor> MockDirectReadProcessor = MakeIntrusive<TMockDirectReadSessionProcessor>();

    TSingleClusterReadSessionImpl<false>::TPtr SingleClusterReadSession;
    TSingleClusterReadSessionContextPtr SingleClusterReadSessionContextPtr;

    TDirectReadSessionManager::TPtr DirectReadSessionManagerPtr;
    TDirectReadSession::TPtr DirectReadSessionPtr;
    TDirectReadSessionContextPtr DirectReadSessionContextPtr;

    std::shared_ptr<TThreadPool> ThreadPool;
    IExecutor::TPtr DefaultExecutor;
};

TDirectReadSessionImplTestSetup::TDirectReadSessionImplTestSetup() {
    ReadSessionSettings
        // .DirectRead(true)
        .AppendTopics({"TestTopic"})
        .ConsumerName("TestConsumer")
        .RetryPolicy(NYdb::NTopic::IRetryPolicy::GetFixedIntervalPolicy(TDuration::MilliSeconds(10)))
        .Counters(MakeIntrusive<NYdb::NTopic::TReaderCounters>(MakeIntrusive<::NMonitoring::TDynamicCounters>()));

    Log.SetFormatter(GetPrefixLogFormatter(""));
}

TDirectReadSessionImplTestSetup::~TDirectReadSessionImplTestSetup() noexcept(false) {
    if (!std::uncaught_exceptions()) { // Exiting from test successfully. Check additional expectations.
        MockReadProcessorFactory->Wait();
        MockReadProcessor->Wait();

        MockReadProcessorFactory->Validate();
        MockReadProcessor->Validate();

        MockDirectReadProcessorFactory->Wait();
        MockDirectReadProcessor->Wait();

        MockDirectReadProcessorFactory->Validate();
        MockDirectReadProcessor->Validate();
    }

    if (SingleClusterReadSessionContextPtr) {
        if (auto session = SingleClusterReadSessionContextPtr->LockShared()) {
            session->Close({});
        }
        SingleClusterReadSessionContextPtr->Cancel();
    }

    if (DirectReadSessionContextPtr) {
        if (auto session = DirectReadSessionContextPtr->LockShared()) {
            session->Close();
        }
        DirectReadSessionContextPtr->Cancel();
    }

    SingleClusterReadSession = nullptr;

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
    std::optional<TReadSessionEvent::TEvent> event = GetEventsQueue()->GetEvent(false);
    EXPECT_FALSE(event);
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
    if (!SingleClusterReadSession) {
        if (!ReadSessionSettings.DecompressionExecutor_) {
            ReadSessionSettings.DecompressionExecutor(GetDefaultExecutor());
        }
        if (!ReadSessionSettings.EventHandlers_.HandlersExecutor_) {
            ReadSessionSettings.EventHandlers_.HandlersExecutor(GetDefaultExecutor());
        }
        SingleClusterReadSessionContextPtr = MakeWithCallbackContext<TSingleClusterReadSessionImpl<false>>(
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
            TSingleClusterReadSessionImpl<false>::TScheduleCallbackFunc {},
            MockDirectReadProcessorFactory);
        SingleClusterReadSession = SingleClusterReadSessionContextPtr->TryGet();
    }
    return SingleClusterReadSession.get();
}

TDirectReadSession* TDirectReadSessionImplTestSetup::GetDirectReadSession(IDirectReadSessionControlCallbacks::TPtr controlCallbacks) {
    if (!DirectReadSessionPtr) {
        DirectReadSessionContextPtr = MakeWithCallbackContext<TDirectReadSession>(
            TNodeId(1),
            SERVER_SESSION_ID,
            ReadSessionSettings,
            controlCallbacks,
            FakeContext,
            MockDirectReadProcessorFactory,
            Log);
        DirectReadSessionPtr = DirectReadSessionContextPtr->TryGet();
    }
    return DirectReadSessionPtr.get();
}

void TDirectReadSessionImplTestSetup::WaitForWorkingDirectReadSession() {
    while (DirectReadSessionPtr->State != TDirectReadSession::EState::WORKING) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

/*
This suite tests direct read mode only through IReadSession, without using internal classes.
*/

class DirectReadWithClient : public TTopicTestFixture {};

TEST_F(DirectReadWithClient, OneMessage) {
    /*
    The simplest case: write one message and read it back.
    */

    TTopicClient client{MakeDriver()};

    {
        // Write a message:

        auto settings = TWriteSessionSettings()
            .Path(GetTopicPath())
            .ProducerId(TEST_MESSAGE_GROUP_ID)
            .MessageGroupId(TEST_MESSAGE_GROUP_ID);
        auto writer = client.CreateSimpleBlockingWriteSession(settings);
        ASSERT_TRUE(writer->Write("message"));
        writer->Close();
    }

    {
        // Read the message:

        auto settings = TReadSessionSettings()
            .ConsumerName(GetConsumerName())
            .AppendTopics(GetTopicPath())
            // .DirectRead(true)
            ;
        auto reader = client.CreateReadSession(settings);

        {
            // Start partition session:
            auto event = reader->GetEvent(true);
            ASSERT_TRUE(event);
            ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TStartPartitionSessionEvent);
            std::get<TReadSessionEvent::TStartPartitionSessionEvent>(*event).Confirm();
        }

        {
            // Receive the message and commit.
            auto event = reader->GetEvent(true);
            ASSERT_TRUE(event);
            ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);
            auto& dataReceived = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
            auto& messages = dataReceived.GetMessages();
            ASSERT_EQ(messages.size(), 1u);
            dataReceived.Commit();
        }

        {
            // Get commit ack.
            auto event = reader->GetEvent(true);
            ASSERT_TRUE(event);
            ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TCommitOffsetAcknowledgementEvent);
        }
    }
}

TEST_F(DirectReadWithClient, ManyMessages) {
    /*
    Write many messages and read them back.

    Don't compress messages and set MaxMemoryUsageBytes for the reader to 1MB,
    so the server sends multiple DirectReadResponses.
    */

    DropTopic(TEST_TOPIC);

    constexpr std::size_t partitionCount = 2;
    std::size_t messageCount = 100;
    std::size_t totalMessageCount = partitionCount * messageCount;
    CreateTopic(TEST_TOPIC, TEST_CONSUMER, partitionCount);
    TTopicClient client{MakeDriver()};

    std::string message(950_KB, 'x');

    // Write messages to all partitions:
    for (std::size_t partitionId = 0; partitionId < partitionCount; ++partitionId) {
        std::string messageGroup = TEST_MESSAGE_GROUP_ID + "_" + std::to_string(partitionId);
        auto settings = TWriteSessionSettings()
            .Path(GetTopicPath())
            .Codec(ECodec::RAW)
            .PartitionId(partitionId)
            .ProducerId(messageGroup)
            .MessageGroupId(messageGroup);

        auto writer = client.CreateSimpleBlockingWriteSession(settings);
        for (std::size_t i = 0; i < messageCount; ++i) {
            ASSERT_TRUE(writer->Write(message)) << "Failed to write message " << i << " to partition " << partitionId;
        }
        ASSERT_TRUE(writer->Close());
    }

    std::atomic<bool> work = true;

    auto killer = std::thread([&]() {
        while (work.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
            // setup.GetServer().KillTopicPqrbTablet(setup.GetTopicPath());
        }
    });

    {
        // Read messages:

        std::size_t gotMessages = 0;
        std::array<std::size_t, partitionCount> committedOffset{};
        auto settings = TReadSessionSettings()
            .ConsumerName(GetConsumerName())
            .AppendTopics(GetTopicPath())
            .MaxMemoryUsageBytes(1_MB)
            // .DirectRead(GetEnv("DIRECT", "0") == "1")
            ;

        std::shared_ptr<IReadSession> reader;

        settings.EventHandlers_.SimpleDataHandlers(
            [&](NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& e) {
                gotMessages += e.GetMessages().size();
                std::cerr << "XXXXX gotMessages: " << gotMessages << " partition_id: " << e.GetPartitionSession()->GetPartitionId() << "\n";
                e.Commit();
            });

        settings.EventHandlers_.CommitOffsetAcknowledgementHandler(
            [&](NYdb::NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent& e) {
                auto partitionId = e.GetPartitionSession()->GetPartitionId();
                committedOffset[partitionId] = e.GetCommittedOffset();
                std::cerr << "XXXXX committedOffset: ";
                for (auto offset : committedOffset) {
                    std::cerr << offset << " ";
                }
                std::cerr << std::endl;
                if (std::ranges::all_of(committedOffset, [&](size_t offset) { return offset == messageCount; })) {
                    reader->Close();
                }
            });

        reader = client.CreateReadSession(settings);

        reader->GetEvent(/*block = */true);

        ASSERT_EQ(gotMessages, totalMessageCount);
    }

    work.store(false);
    killer.join();
}

/*
This suite tests direct read sessions together with a control session.
*/

class DirectReadWithControlSession : public TTopicTestFixture {};

void SuccessfulInitImpl(bool thenTimeout) {
    TDirectReadSessionImplTestSetup setup;
    setup.ReadSessionSettings
        .MaxLag(TDuration::Seconds(32))
        .ReadFromTimestamp(TInstant::Seconds(42));

    setup.ReadSessionSettings.Topics_[0]
        .ReadFromTimestamp(TInstant::Seconds(146))
        .AppendPartitionIds(100)
        .AppendPartitionIds(101);

    {
        ::testing::InSequence seq;

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
                ASSERT_EQ(req.consumer(), setup.ReadSessionSettings.ConsumerName_);
                ASSERT_TRUE(req.direct_read());
                ASSERT_EQ(req.topics_read_settings_size(), 1);
                ASSERT_EQ(req.topics_read_settings(0).path(), setup.ReadSessionSettings.Topics_[0].Path_);
                ASSERT_EQ(req.topics_read_settings(0).read_from().seconds(), 146);
                ASSERT_EQ(req.topics_read_settings(0).partition_ids_size(), 2);
                ASSERT_EQ(req.topics_read_settings(0).partition_ids(0), 100);
                ASSERT_EQ(req.topics_read_settings(0).partition_ids(1), 101);
            }));

        EXPECT_CALL(*setup.MockReadProcessor, OnReadRequest(_));
    }

    setup.GetControlSession()->Start();
    setup.MockReadProcessorFactory->Wait();

    setup.AddControlResponse(TMockReadSessionProcessor::TServerReadInfo().InitResponse("session id"));

    setup.AssertNoEvents();
}

TEST_F(DirectReadWithControlSession, Init) {
    SuccessfulInitImpl(true);
    SuccessfulInitImpl(false);
}

TEST_F(DirectReadWithControlSession, StopPartitionSessionGracefully) {
#ifdef __GNUC__
    GTEST_SKIP() << "Skip for gcc";
#endif
    auto const startPartitionSessionRequest = TStartPartitionSessionRequest{
        .PartitionId = 1,
        .PartitionSessionId = 2,
        .NodeId = 3,
        .Generation = 4,
    };

    auto const stopPartitionSessionRequest = TStopPartitionSessionRequest{
        .PartitionSessionId = 2,
        .Graceful = true,
        .CommittedOffset = 0,
        .LastDirectReadId = 5,
    };

    TDirectReadSessionImplTestSetup setup;
    setup.ReadSessionSettings.Topics_[0].AppendPartitionIds(startPartitionSessionRequest.PartitionId);

    {
        {
            ::testing::InSequence seq;

            EXPECT_CALL(*setup.MockReadProcessorFactory, OnCreateProcessor(_))
                .WillOnce([&]() {
                    setup.MockReadProcessorFactory->CreateProcessor(setup.MockReadProcessor);
                });

            EXPECT_CALL(*setup.MockReadProcessor, OnInitRequest(_))
                .WillOnce(Invoke([&](const Ydb::Topic::StreamReadMessage::InitRequest& req) {
                    ASSERT_TRUE(req.direct_read());
                    ASSERT_EQ(req.topics_read_settings_size(), 1);
                    ASSERT_EQ(req.topics_read_settings(0).path(), setup.ReadSessionSettings.Topics_[0].Path_);
                    ASSERT_EQ(req.topics_read_settings(0).partition_ids_size(), 1);
                    ASSERT_EQ(req.topics_read_settings(0).partition_ids(0), startPartitionSessionRequest.PartitionId);
                }));

            EXPECT_CALL(*setup.MockReadProcessor, OnReadRequest(_));

            EXPECT_CALL(*setup.MockReadProcessor, OnStartPartitionSessionResponse(_))
                .WillOnce(Invoke([&startPartitionSessionRequest](const Ydb::Topic::StreamReadMessage::StartPartitionSessionResponse& resp) {
                    ASSERT_EQ(static_cast<std::uint64_t>(resp.partition_session_id()), startPartitionSessionRequest.PartitionSessionId);
                }));

            EXPECT_CALL(*setup.MockReadProcessor, OnDirectReadAck(_))
                .Times(4);
        }

        // There are two sequences, because OnCreateProcessor from the second sequence may be called
        // before OnStartPartitionSessionResponse from the first sequence.

        {
            ::testing::InSequence sequence;

            EXPECT_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(_))
                .WillOnce([&]() {
                    setup.MockDirectReadProcessorFactory->CreateProcessor(setup.MockDirectReadProcessor);
                });

            EXPECT_CALL(*setup.MockDirectReadProcessor, OnInitRequest(_))
                .WillOnce(Invoke([&setup](const Ydb::Topic::StreamDirectReadMessage::InitRequest& req) {
                    ASSERT_EQ(req.session_id(), SERVER_SESSION_ID);
                    ASSERT_EQ(static_cast<std::size_t>(req.topics_read_settings_size()), setup.ReadSessionSettings.Topics_.size());
                    ASSERT_EQ(req.topics_read_settings(0).path(), setup.ReadSessionSettings.Topics_[0].Path_);
                    ASSERT_EQ(req.consumer(), setup.ReadSessionSettings.ConsumerName_);
                }));

            EXPECT_CALL(*setup.MockDirectReadProcessor, OnStartDirectReadPartitionSessionRequest(_))
                .WillOnce(Invoke([&startPartitionSessionRequest](const Ydb::Topic::StreamDirectReadMessage::StartDirectReadPartitionSessionRequest& request) {
                    ASSERT_EQ(static_cast<std::uint64_t>(request.partition_session_id()), startPartitionSessionRequest.PartitionSessionId);
                    ASSERT_EQ(request.generation(), startPartitionSessionRequest.Generation);
                }));

            // Expect OnReadRequest in case it is called before the test ends.
            // TODO(qyryq) Fix number, not 10.
            EXPECT_CALL(*setup.MockReadProcessor, OnReadRequest(_)).Times(AtMost(10));
        }
    }

    setup.GetControlSession()->Start();
    setup.MockReadProcessorFactory->Wait();
    setup.AddControlResponse(TMockReadSessionProcessor::TServerReadInfo().InitResponse(SERVER_SESSION_ID));
    setup.AddControlResponse(TMockReadSessionProcessor::TServerReadInfo().StartPartitionSessionRequest(startPartitionSessionRequest));

    {
        std::optional<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
        ASSERT_TRUE(event);
        ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TStartPartitionSessionEvent);
        std::get<TReadSessionEvent::TStartPartitionSessionEvent>(*event).Confirm();
    }

    setup.AddControlResponse(TMockReadSessionProcessor::TServerReadInfo()
        .StopPartitionSession(stopPartitionSessionRequest));

    setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
        .InitResponse());

    setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
        .StartDirectReadPartitionSessionResponse(startPartitionSessionRequest.PartitionSessionId));

    std::size_t offset = 0, i = 0;

    // Verify that the session receives data sent to direct read session:
    for (std::size_t directReadId = 1; directReadId < stopPartitionSessionRequest.LastDirectReadId; ++directReadId) {
        auto resp = TMockDirectReadSessionProcessor::TServerReadInfo()
            .PartitionData(startPartitionSessionRequest.PartitionSessionId, directReadId)
            // TODO(qyryq) Test with compression!
            // .Batch("producer-id-1", Ydb::Topic::Codec::CODEC_ZSTD);
            .Batch("producer-id-1", Ydb::Topic::Codec::CODEC_RAW);

        resp.Message(offset, TStringBuilder() << "message-" << offset, offset);
        ++offset;
        resp.Message(offset, TStringBuilder() << "message-" << offset, offset);
        ++offset;
        setup.AddDirectReadResponse(resp);

        std::optional<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
        ASSERT_TRUE(event);
        ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);
        auto& e = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
        i += e.GetMessagesCount();
    }

    while (i < offset) {
        std::optional<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
        ASSERT_TRUE(event);
        ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);
        auto& e = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
        i += e.GetMessagesCount();
    }

    {
        // Verify that the session receives TStopPartitionSessionEvent(graceful=true) after data was received:

        std::optional<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
        ASSERT_TRUE(event);
        ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TStopPartitionSessionEvent);
        auto e = std::get_if<TReadSessionEvent::TStopPartitionSessionEvent>(&*event);
        e->Confirm();
    }

    {
        // Verify that the session receives TPartitionSessionClosedEvent after data was received:

        std::optional<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
        ASSERT_TRUE(event);
        ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TPartitionSessionClosedEvent);
        // auto e = std::get_if<TReadSessionEvent::TPartitionSessionClosedEvent>(&*event);
    }

    setup.AssertNoEvents();

    // ::testing::Mock::VerifyAndClear(setup.MockDirectReadProcessorFactory);
    // ::testing::Mock::VerifyAndClear(setup.MockDirectReadProcessor);
}

TEST_F(DirectReadWithControlSession, StopPartitionSession) {
    auto const startPartitionSessionRequest = TStartPartitionSessionRequest{
        .PartitionId = 1,
        .PartitionSessionId = 2,
        .NodeId = 3,
        .Generation = 4,
    };

    TDirectReadSessionImplTestSetup setup;
    setup.ReadSessionSettings.Topics_[0].AppendPartitionIds(startPartitionSessionRequest.PartitionId);

    {
        {
            ::testing::InSequence seq;

            EXPECT_CALL(*setup.MockReadProcessorFactory, OnCreateProcessor(_))
                .WillOnce([&]() {
                    setup.MockReadProcessorFactory->CreateProcessor(setup.MockReadProcessor);
                });

            EXPECT_CALL(*setup.MockReadProcessor, OnInitRequest(_))
                .WillOnce(Invoke([&](const Ydb::Topic::StreamReadMessage::InitRequest& req) {
                    ASSERT_TRUE(req.direct_read());
                    ASSERT_EQ(req.topics_read_settings_size(), 1);
                    ASSERT_EQ(req.topics_read_settings(0).path(), setup.ReadSessionSettings.Topics_[0].Path_);
                    ASSERT_EQ(req.topics_read_settings(0).partition_ids_size(), 1);
                    ASSERT_EQ(req.topics_read_settings(0).partition_ids(0), startPartitionSessionRequest.PartitionId);
                }));

            EXPECT_CALL(*setup.MockReadProcessor, OnReadRequest(_));

            EXPECT_CALL(*setup.MockReadProcessor, OnStartPartitionSessionResponse(_))
                .WillOnce(Invoke([&startPartitionSessionRequest](const Ydb::Topic::StreamReadMessage::StartPartitionSessionResponse& resp) {
                    ASSERT_EQ(static_cast<std::uint64_t>(resp.partition_session_id()), startPartitionSessionRequest.PartitionSessionId);
                }));

            EXPECT_CALL(*setup.MockReadProcessor, OnDirectReadAck(_))
                .Times(4);
        }

        // There are two sequences, because OnCreateProcessor from the second sequence may be called
        // before OnStartPartitionSessionResponse from the first sequence.

        {
            ::testing::InSequence sequence;

            EXPECT_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(_))
                .WillOnce([&]() {
                    setup.MockDirectReadProcessorFactory->CreateProcessor(setup.MockDirectReadProcessor);
                });

            EXPECT_CALL(*setup.MockDirectReadProcessor, OnInitRequest(_))
                .WillOnce(Invoke([&setup](const Ydb::Topic::StreamDirectReadMessage::InitRequest& req) {
                    ASSERT_EQ(req.session_id(), SERVER_SESSION_ID);
                    ASSERT_EQ(static_cast<std::size_t>(req.topics_read_settings_size()), setup.ReadSessionSettings.Topics_.size());
                    ASSERT_EQ(req.topics_read_settings(0).path(), setup.ReadSessionSettings.Topics_[0].Path_);
                    ASSERT_EQ(req.consumer(), setup.ReadSessionSettings.ConsumerName_);
                }));

            EXPECT_CALL(*setup.MockDirectReadProcessor, OnStartDirectReadPartitionSessionRequest(_))
                .WillOnce(Invoke([&startPartitionSessionRequest](const Ydb::Topic::StreamDirectReadMessage::StartDirectReadPartitionSessionRequest& request) {
                    ASSERT_EQ(static_cast<std::uint64_t>(request.partition_session_id()), startPartitionSessionRequest.PartitionSessionId);
                    ASSERT_EQ(request.generation(), startPartitionSessionRequest.Generation);
                }));

            // Expect OnReadRequest in case it is called before the test ends.
            // TODO(qyryq) Fix number, not 10.
            EXPECT_CALL(*setup.MockReadProcessor, OnReadRequest(_)).Times(AtMost(10));
        }
    }

    {
        // Send a StartDirectReadPartitionSessionResponse with a random partition session id,
        // the session should ignore it.
        auto r = TMockDirectReadSessionProcessor::TServerReadInfo();
        setup.AddDirectReadResponse(r.StartDirectReadPartitionSessionResponse(333));
    }

    setup.GetControlSession()->Start();
    {
        auto r = TMockReadSessionProcessor::TServerReadInfo();
        setup.AddControlResponse(r.InitResponse(SERVER_SESSION_ID));
    }

    {
        auto r = TMockReadSessionProcessor::TServerReadInfo();
        setup.AddControlResponse(r.StartPartitionSessionRequest(startPartitionSessionRequest));
    }

    {
        std::optional<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
        ASSERT_TRUE(event);
        ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TStartPartitionSessionEvent);
        std::get<TReadSessionEvent::TStartPartitionSessionEvent>(*event).Confirm();
    }

    {
        auto r = TMockDirectReadSessionProcessor::TServerReadInfo();
        setup.AddDirectReadResponse(r.InitResponse());
    }

    {
        auto r = TMockDirectReadSessionProcessor::TServerReadInfo();
        setup.AddDirectReadResponse(r.StartDirectReadPartitionSessionResponse(startPartitionSessionRequest.PartitionSessionId));
    }

    std::int64_t offset = 0, i = 0;

    // Verify that the session receives data sent to direct read session:
    for (std::size_t directReadId = 1; directReadId < 5; ++directReadId) {
        auto resp = TMockDirectReadSessionProcessor::TServerReadInfo();
        resp.PartitionData(startPartitionSessionRequest.PartitionSessionId, directReadId)
            // TODO(qyryq) Test with compression!
            // .Batch("producer-id-1", Ydb::Topic::Codec::CODEC_ZSTD);
            .Batch("producer-id-1", Ydb::Topic::Codec::CODEC_RAW);

        resp.Message(offset, TStringBuilder() << "message-" << offset, offset);
        ++offset;
        resp.Message(offset, TStringBuilder() << "message-" << offset, offset);
        ++offset;
        setup.AddDirectReadResponse(resp);

        std::optional<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
        ASSERT_TRUE(event);
        ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);
        auto& e = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
        i += e.GetMessagesCount();
        e.Commit();
    }

    while (i < offset) {
        std::optional<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
        ASSERT_TRUE(event);
        ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TDataReceivedEvent);
        auto& e = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
        i += e.GetMessagesCount();
    }

    {
        auto r = TMockReadSessionProcessor::TServerReadInfo();
        setup.AddControlResponse(
            r.StopPartitionSession({
                .PartitionSessionId = 2,
                .Graceful = false,
                .CommittedOffset = offset,
            }));
    }

    // TODO(qyryq) Send some bogus events from server, the client should ignore them.

    {
        // Verify that the session receives TStopPartitionSessionEvent after data was received:

        std::optional<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
        ASSERT_TRUE(event);
        ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TPartitionSessionClosedEvent);
        // auto e = std::get_if<TReadSessionEvent::TStopPartitionSessionEvent>(&*event);
        // UNIT_ASSERT(!e.Graceful);
        // UNIT_ASSERT(e.CommittedOffset == offset);
    }

    setup.MockReadProcessorFactory->Wait();
    setup.MockDirectReadProcessorFactory->Wait();

    setup.AssertNoEvents();
}

TEST_F(DirectReadWithControlSession, EmptyDirectReadResponse) {
    // Sometimes the server might send a DirectReadResponse with no data, but with bytes_size value > 0.
    // It can happen, if the server tried to send DirectReadResponse, but did not succeed,
    // and in the meantime the messages that should had been sent have been rotated by retention period,
    // and do not exist anymore. To keep ReadSizeBudget bookkeeping correct, the server still sends the an DirectReadResponse,
    // and SDK should process it correctly: basically it should immediately send a ReadRequest(bytes_size=DirectReadResponse.bytes_size).

    auto const startPartitionSessionRequest = TStartPartitionSessionRequest{
        .PartitionId = 1,
        .PartitionSessionId = 2,
        .NodeId = 3,
        .Generation = 4,
    };

    std::int64_t bytesSize = 12345;

    TDirectReadSessionImplTestSetup setup;
    setup.ReadSessionSettings.Topics_[0].AppendPartitionIds(startPartitionSessionRequest.PartitionId);

    {
        {
            ::testing::InSequence seq;

            EXPECT_CALL(*setup.MockReadProcessorFactory, OnCreateProcessor(_))
                .WillOnce([&]() {
                    setup.MockReadProcessorFactory->CreateProcessor(setup.MockReadProcessor);
                });

            EXPECT_CALL(*setup.MockReadProcessor, OnInitRequest(_))
                .WillOnce(Invoke([&](const Ydb::Topic::StreamReadMessage::InitRequest& req) {
                    ASSERT_TRUE(req.direct_read());
                    ASSERT_EQ(req.topics_read_settings_size(), 1);
                    ASSERT_EQ(req.topics_read_settings(0).path(), setup.ReadSessionSettings.Topics_[0].Path_);
                    ASSERT_EQ(req.topics_read_settings(0).partition_ids_size(), 1);
                    ASSERT_EQ(req.topics_read_settings(0).partition_ids(0), startPartitionSessionRequest.PartitionId);
                }));

            EXPECT_CALL(*setup.MockReadProcessor, OnReadRequest(_));

            EXPECT_CALL(*setup.MockReadProcessor, OnStartPartitionSessionResponse(_))
                .WillOnce(Invoke([&startPartitionSessionRequest](const Ydb::Topic::StreamReadMessage::StartPartitionSessionResponse& resp) {
                    ASSERT_EQ(static_cast<std::uint64_t>(resp.partition_session_id()), startPartitionSessionRequest.PartitionSessionId);
                }));

            EXPECT_CALL(*setup.MockReadProcessor, OnDirectReadAck(_))
                .Times(1);

            EXPECT_CALL(*setup.MockReadProcessor, OnReadRequest(_))
                .WillOnce(Invoke([&](const Ydb::Topic::StreamReadMessage::ReadRequest& req) {
                    ASSERT_EQ(req.bytes_size(), bytesSize);
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

            EXPECT_CALL(*setup.MockDirectReadProcessor, OnInitRequest(_))
                .WillOnce(Invoke([&setup](const Ydb::Topic::StreamDirectReadMessage::InitRequest& req) {
                    ASSERT_EQ(req.session_id(), SERVER_SESSION_ID);
                    ASSERT_EQ(static_cast<std::size_t>(req.topics_read_settings_size()), setup.ReadSessionSettings.Topics_.size());
                    ASSERT_EQ(req.topics_read_settings(0).path(), setup.ReadSessionSettings.Topics_[0].Path_);
                    ASSERT_EQ(req.consumer(), setup.ReadSessionSettings.ConsumerName_);
                }));

            EXPECT_CALL(*setup.MockDirectReadProcessor, OnStartDirectReadPartitionSessionRequest(_))
                .WillOnce(Invoke([&startPartitionSessionRequest](const Ydb::Topic::StreamDirectReadMessage::StartDirectReadPartitionSessionRequest& request) {
                    ASSERT_EQ(static_cast<std::uint64_t>(request.partition_session_id()), startPartitionSessionRequest.PartitionSessionId);
                    ASSERT_EQ(request.generation(), startPartitionSessionRequest.Generation);
                }));
        }
    }

    setup.GetControlSession()->Start();
    {
        auto r = TMockReadSessionProcessor::TServerReadInfo();
        setup.AddControlResponse(r.InitResponse(SERVER_SESSION_ID));
    }

    {
        auto r = TMockReadSessionProcessor::TServerReadInfo();
        setup.AddControlResponse(r.StartPartitionSessionRequest(startPartitionSessionRequest));
    }

    {
        std::optional<TReadSessionEvent::TEvent> event = setup.EventsQueue->GetEvent(true);
        ASSERT_TRUE(event);
        ASSERT_EVENT_TYPE(*event, TReadSessionEvent::TStartPartitionSessionEvent);
        std::get<TReadSessionEvent::TStartPartitionSessionEvent>(*event).Confirm();
    }

    {
        auto r = TMockDirectReadSessionProcessor::TServerReadInfo();
        setup.AddDirectReadResponse(r.InitResponse());
    }

    {
        auto r = TMockDirectReadSessionProcessor::TServerReadInfo();
        setup.AddDirectReadResponse(r.StartDirectReadPartitionSessionResponse(startPartitionSessionRequest.PartitionSessionId));
    }

    std::int64_t directReadId = 1;

    auto resp = TMockDirectReadSessionProcessor::TServerReadInfo();
    resp.PartitionData(startPartitionSessionRequest.PartitionSessionId, directReadId, bytesSize);
    setup.AddDirectReadResponse(resp);

    setup.MockReadProcessorFactory->Wait();
    setup.MockDirectReadProcessorFactory->Wait();

    setup.AssertNoEvents();
}

/*
This suite tests TDirectReadSession in isolation, without control session.
*/

class DirectReadSession : public TTopicTestFixture {};

TEST_F(DirectReadSession, InitAndStartPartitionSession) {
    /*
    Create DirectRead processor, send InitRequest, StartDirectReadPartitionSessionRequest.
    */

    TDirectReadSessionImplTestSetup setup;

    auto gotStart = NThreading::NewPromise();

    TPartitionSessionId partitionSessionId = 1;

    class TControlCallbacks : public IDirectReadSessionControlCallbacks {};
    auto session = setup.GetDirectReadSession(std::make_shared<TControlCallbacks>());

    {
        ::testing::InSequence seq;

        EXPECT_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(_))
            .WillOnce([&]() { setup.MockDirectReadProcessorFactory->CreateProcessor(setup.MockDirectReadProcessor); });

        EXPECT_CALL(*setup.MockDirectReadProcessor, OnInitRequest(_))
            .WillOnce(Invoke([&](const Ydb::Topic::StreamDirectReadMessage::InitRequest& req) {
                ASSERT_EQ(req.session_id(), SERVER_SESSION_ID);
                ASSERT_EQ(req.consumer(), setup.ReadSessionSettings.ConsumerName_);
            }));

        EXPECT_CALL(*setup.MockDirectReadProcessor, OnStartDirectReadPartitionSessionRequest(_))
            .WillOnce(Invoke([&](const Ydb::Topic::StreamDirectReadMessage::StartDirectReadPartitionSessionRequest& req) {
                ASSERT_EQ(req.partition_session_id(), static_cast<std::int64_t>(partitionSessionId));
                gotStart.SetValue();
            }));
    }

    session->Start();

    setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
        .InitResponse());

    session->AddPartitionSession({ .PartitionSessionId = partitionSessionId, .Location = {2, 3} });

    gotStart.GetFuture().Wait();
}

TEST_F(DirectReadSession, NoRetryDirectReadSession) {
    /*
    If the session cannot establish a connection, and the retry policy does not allow to make another retry,
    the session should be aborted and the client should receive TSessionClosedEvent.
    */

    TDirectReadSessionImplTestSetup setup;
    setup.ReadSessionSettings.RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy());

    auto gotClosedEvent = NThreading::NewPromise();

    EXPECT_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(_))
        .WillOnce([&]() { setup.MockDirectReadProcessorFactory->FailCreation(); });

    class TControlCallbacks : public IDirectReadSessionControlCallbacks {
    public:
        TControlCallbacks(NThreading::TPromise<void>& gotClosedEvent) : GotClosedEvent(gotClosedEvent) {}
        void AbortSession(TSessionClosedEvent&&) override { GotClosedEvent.SetValue(); }
        NThreading::TPromise<void>& GotClosedEvent;
    };

    auto session = setup.GetDirectReadSession(std::make_shared<TControlCallbacks>(gotClosedEvent));

    session->Start();
    setup.MockDirectReadProcessorFactory->Wait();
    gotClosedEvent.GetFuture().Wait();
}

TEST_F(DirectReadSession, RetryDirectReadSession) {
    /*
    If the retry policy allows retries, keep trying to establish connection.
    */
    TDirectReadSessionImplTestSetup setup;
    std::size_t nRetries = 2;
    setup.ReadSessionSettings.RetryPolicy(NYdb::NTopic::IRetryPolicy::GetFixedIntervalPolicy(
        TDuration::MilliSeconds(1), TDuration::MilliSeconds(1), nRetries));

    auto gotClosedEvent = NThreading::NewPromise();

    ON_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(_))
        .WillByDefault([&]() { setup.MockDirectReadProcessorFactory->FailCreation(); });

    EXPECT_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(_))
        .Times(1 + nRetries);  // First call + N retries.

    class TControlCallbacks : public IDirectReadSessionControlCallbacks {
    public:
        TControlCallbacks(NThreading::TPromise<void>& gotClosedEvent) : GotClosedEvent(gotClosedEvent) {}
        void AbortSession(TSessionClosedEvent&&) override { GotClosedEvent.SetValue(); }
        NThreading::TPromise<void>& GotClosedEvent;
    };

    auto session = setup.GetDirectReadSession(std::make_shared<TControlCallbacks>(gotClosedEvent));
    session->Start();
    setup.MockDirectReadProcessorFactory->Wait();

    gotClosedEvent.GetFuture().Wait();
}

    // Y_UNIT_TEST(NoRetryPartitionSession) {
    //     /*
    //     If we get a StopDirectReadPartitionSession event, and the retry policy does not allow to send another Start-request,
    //     the session should be aborted and the client should receive TSessionClosedEvent.
    //     */
    //     TDirectReadSessionImplTestSetup setup;
    //     setup.ReadSessionSettings.RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy());

    //     auto gotClosedEvent = NThreading::NewPromise();

    //     {
    //         ::testing::InSequence seq;

    //         EXPECT_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(_))
    //             .WillOnce([&]() { setup.MockDirectReadProcessorFactory->CreateProcessor(setup.MockDirectReadProcessor); });

    //         EXPECT_CALL(*setup.MockDirectReadProcessor, OnInitRequest(_));

    //         EXPECT_CALL(*setup.MockDirectReadProcessor, OnStartDirectReadPartitionSessionRequest(_));
    //     }

    //     class TControlCallbacks : public IDirectReadSessionControlCallbacks {
    //     public:
    //         TControlCallbacks(NThreading::TPromise<void>& gotClosedEvent) : GotClosedEvent(gotClosedEvent) {}
    //         void AbortSession(TSessionClosedEvent&&) override { GotClosedEvent.SetValue(); }
    //         NThreading::TPromise<void>& GotClosedEvent;
    //     };

    //     auto session = setup.GetDirectReadSession(std::make_shared<TControlCallbacks>(gotClosedEvent));
    //     session->Start();
    //     setup.MockDirectReadProcessorFactory->Wait();

    //     setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
    //         .InitResponse());

    //     session->AddPartitionSession({ .PartitionSessionId = 1, .Location = {2, 3} });

    //     setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
    //         .StopDirectReadPartitionSession(Ydb::StatusIds::OVERLOADED, TPartitionSessionId(1)));

    //     gotClosedEvent.GetFuture().Wait();
    // }

    // Y_UNIT_TEST(RetryPartitionSession) {
    //     /*
    //     Keep sending Start-requests until the retry policy denies next retry.
    //     */
    //     TDirectReadSessionImplTestSetup setup;
    //     size_t nRetries = 2;
    //     setup.ReadSessionSettings.RetryPolicy(NYdb::NTopic::IRetryPolicy::GetFixedIntervalPolicy(
    //         TDuration::MilliSeconds(1), TDuration::MilliSeconds(1), nRetries));

    //     auto gotClosedEvent = NThreading::NewPromise();

    //     {
    //         ::testing::InSequence seq;

    //         EXPECT_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(_))
    //             .WillOnce([&]() { setup.MockDirectReadProcessorFactory->CreateProcessor(setup.MockDirectReadProcessor); });

    //         EXPECT_CALL(*setup.MockDirectReadProcessor, OnInitRequest(_));

    //         EXPECT_CALL(*setup.MockDirectReadProcessor, OnStartDirectReadPartitionSessionRequest(_))
    //             .Times(1 + nRetries);
    //     }

    //     class TControlCallbacks : public IDirectReadSessionControlCallbacks {
    //     public:
    //         TControlCallbacks(NThreading::TPromise<void>& gotClosedEvent) : GotClosedEvent(gotClosedEvent) {}
    //         void AbortSession(TSessionClosedEvent&&) override { GotClosedEvent.SetValue(); }
    //         void ScheduleCallback(TDuration, std::function<void()> cb, TDeferredActions<false>& deferred) override {
    //             deferred.DeferCallback(cb);
    //         }
    //         NThreading::TPromise<void>& GotClosedEvent;
    //     };

    //     auto session = setup.GetDirectReadSession(std::make_shared<TControlCallbacks>(gotClosedEvent));

    //     setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
    //         .InitResponse());

    //     session->Start();
    //     setup.MockDirectReadProcessorFactory->Wait();

    //     TPartitionSessionId partitionSessionId = 1;

    //     session->AddPartitionSession({ .PartitionSessionId = partitionSessionId, .Location = {2, 3} });

    //     for (size_t i = 0; i < 1 + nRetries; ++i) {
    //         setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
    //             .StopDirectReadPartitionSession(Ydb::StatusIds::OVERLOADED, partitionSessionId));
    //     }

    //     gotClosedEvent.GetFuture().Wait();
    // }

    // Y_UNIT_TEST(ResetRetryStateOnSuccess) {
    //     /*
    //     Test that the client creates a new retry state on the first error after a successful response.

    //     With the default retry policy (exponential backoff), retry delays grow after each unsuccessful request.
    //     After the first successful request retry state should be reset, so the delay after another unsuccessful request will be small.

    //     E.g. if the exponential backoff policy is used, and minDelay is 1ms, and scaleFactor is 1000, then the following should happen:

    //     client -> server: InitRequest
    //     client <-- server: InitResponse
    //     client -> server: StartDirectReadPartitionSessionRequest
    //     client <- server: StopDirectReadPartitionSession(OVERLOADED)
    //     note over client: Wait 1 ms
    //     client -> server: StartDirectReadPartitionSessionRequest
    //     client <-- server: StartDirectReadPartitionSessionResponse
    //     note over client: Reset RetryState
    //     client <- server: StopDirectReadPartitionSession(OVERLOADED)
    //     note over client: Wait 1 ms, not 1 second
    //     client -> server: StartDirectReadPartitionSessionRequest
    //     */

    //     TDirectReadSessionImplTestSetup setup;
    //     setup.ReadSessionSettings.RetryPolicy(setup.MockRetryPolicy);

    //     auto gotFinalStart = NThreading::NewPromise();
    //     TPartitionSessionId partitionSessionId = 1;

    //     {
    //         ::testing::InSequence sequence;

    //         EXPECT_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(_))
    //             .WillOnce([&]() { setup.MockDirectReadProcessorFactory->CreateProcessor(setup.MockDirectReadProcessor); });

    //         EXPECT_CALL(*setup.MockDirectReadProcessor, OnInitRequest(_));
    //         EXPECT_CALL(*setup.MockDirectReadProcessor, OnStartDirectReadPartitionSessionRequest(_));

    //         // The client receives StopDirectReadPartitionSession, create TDirectReadSession::PartitionSessions[i].RetryState
    //         EXPECT_CALL(*setup.MockRetryPolicy, CreateRetryState())
    //             .WillOnce(Return(std::make_unique<TMockRetryState>(setup.MockRetryPolicy)));

    //         EXPECT_CALL(*setup.MockDirectReadProcessor, OnStartDirectReadPartitionSessionRequest(_));

    //         // The client receives StartDirectReadPartitionSessionResponse, resets retry state,
    //         // then receives StopDirectReadPartitionSession and has to create a new retry state.
    //         EXPECT_CALL(*setup.MockRetryPolicy, CreateRetryState())
    //             .WillOnce(Return(std::make_unique<TMockRetryState>(setup.MockRetryPolicy)));

    //         EXPECT_CALL(*setup.MockDirectReadProcessor, OnStartDirectReadPartitionSessionRequest(_))
    //             .WillOnce([&]() { gotFinalStart.SetValue(); });
    //     }

    //     class TControlCallbacks : public IDirectReadSessionControlCallbacks {
    //     public:
    //         void ScheduleCallback(TDuration, std::function<void()> cb, TDeferredActions<false>& deferred) override {
    //             deferred.DeferCallback(cb);
    //         }
    //     };

    //     auto session = setup.GetDirectReadSession(std::make_shared<TControlCallbacks>());

    //     session->Start();
    //     setup.MockDirectReadProcessorFactory->Wait();

    //     session->AddPartitionSession({ .PartitionSessionId = partitionSessionId, .Location = {2, 3} });

    //     setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
    //         .InitResponse());

    //     setup.MockRetryPolicy->Delay = TDuration::MilliSeconds(1);

    //     setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
    //         .StopDirectReadPartitionSession(Ydb::StatusIds::OVERLOADED, partitionSessionId));

    //     setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
    //         .StartDirectReadPartitionSessionResponse(partitionSessionId));

    //     setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
    //         .StopDirectReadPartitionSession(Ydb::StatusIds::OVERLOADED, partitionSessionId));

    //     gotFinalStart.GetFuture().Wait();
    // }

    // Y_UNIT_TEST(PartitionSessionRetainsRetryStateOnReconnects) {
    //     /*
    //     We need to retain retry states of separate partition sessions
    //     even after reestablishing the connection to a node.

    //     E.g. partition session receives StopDirectReadPartitionSession
    //     and we need to send StartDirectReadPartitionSessionRequest in 5 minutes due to the retry policy.

    //     But in the meantime, the session loses connection to the server and reconnects within several seconds.

    //     We must not send that StartDirectReadPartitionSessionRequest right away, but wait ~5 minutes.

    //     client -> server: InitRequest
    //     client <-- server: InitResponse
    //     client -> server: StartDirectReadPartitionSessionRequest
    //     client <- server: StopDirectReadPartitionSession(OVERLOADED)
    //     note over client: Wait N seconds before sending Start again
    //     ... Connection lost, client reconnects to the server ...
    //     client -> server: InitRequest
    //     client <-- server: InitResponse
    //     note over client: Still has to wait ~N seconds
    //     client -> server: StartDirectReadPartitionSessionRequest
    //     */

    //     TDirectReadSessionImplTestSetup setup;
    //     setup.ReadSessionSettings.RetryPolicy(setup.MockRetryPolicy);

    //     auto gotFinalStart = NThreading::NewPromise();
    //     auto gotInitRequest = NThreading::NewPromise();
    //     auto calledRead = NThreading::NewPromise();
    //     TPartitionSessionId partitionSessionId = 1;
    //     auto secondProcessor = MakeIntrusive<TMockDirectReadSessionProcessor>();
    //     auto delay = TDuration::Seconds(300);

    //     {
    //         ::testing::InSequence sequence;

    //         EXPECT_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(1))
    //             .WillOnce([&]() { setup.MockDirectReadProcessorFactory->CreateProcessor(setup.MockDirectReadProcessor); });

    //         EXPECT_CALL(*setup.MockDirectReadProcessor, OnInitRequest(_));

    //         EXPECT_CALL(*setup.MockDirectReadProcessor, OnStartDirectReadPartitionSessionRequest(_));

    //         // The client receives StopDirectReadPartitionSession, create TDirectReadSession::PartitionSessions[i].RetryState
    //         EXPECT_CALL(*setup.MockRetryPolicy, CreateRetryState())
    //             .WillOnce(Return(std::make_unique<TMockRetryState>(setup.MockRetryPolicy)));

    //         // The client loses connection, create TDirectReadSession.RetryState
    //         EXPECT_CALL(*setup.MockRetryPolicy, CreateRetryState())
    //             .WillOnce(Return(std::make_unique<TMockRetryState>(setup.MockRetryPolicy)));

    //         // The connection is lost at this point, the client tries to reconnect.
    //         EXPECT_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(2))
    //             .WillOnce([&]() { setup.MockDirectReadProcessorFactory->CreateProcessor(secondProcessor); });

    //         EXPECT_CALL(*secondProcessor, OnInitRequest(_))
    //             .WillOnce([&]() { gotInitRequest.SetValue(); });

    //         // The client waits `delay` seconds before sending the StartDirectReadPartitionSessionRequest.

    //         EXPECT_CALL(*secondProcessor, OnStartDirectReadPartitionSessionRequest(_))
    //             .WillOnce([&]() { gotFinalStart.SetValue(); });
    //     }

    //     std::function<void()> callback;

    //     class TControlCallbacks : public IDirectReadSessionControlCallbacks {
    //     public:
    //         TControlCallbacks(std::function<void()>& callback) : Callback(callback) {}
    //         void ScheduleCallback(TDuration, std::function<void()> cb, TDeferredActions<false>&) override {
    //             Callback = cb;
    //         }
    //         std::function<void()>& Callback;
    //     };

    //     auto session = setup.GetDirectReadSession(std::make_shared<TControlCallbacks>(callback));

    //     session->Start();
    //     setup.MockDirectReadProcessorFactory->Wait();

    //     session->AddPartitionSession({ .PartitionSessionId = partitionSessionId, .Location = {2, 3} });

    //     setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
    //         .InitResponse());

    //     setup.MockRetryPolicy->Delay = delay;

    //     setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
    //         .StopDirectReadPartitionSession(Ydb::StatusIds::OVERLOADED, partitionSessionId));

    //     // Besides logs, these durations don't really affect anything in tests.
    //     setup.MockRetryPolicy->Delay = TDuration::Seconds(1);

    //     setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
    //         .Failure());

    //     gotInitRequest.GetFuture().Wait();
    //     secondProcessor->AddServerResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
    //         .InitResponse());

    //     // Ensure that the callback is called after the direct session got InitResponse.
    //     setup.WaitForWorkingDirectReadSession();

    //     callback();

    //     gotFinalStart.GetFuture().Wait();

    //     secondProcessor->Wait();
    //     secondProcessor->Validate();
    // }

    // Y_UNIT_TEST(RetryWithoutConnectionResetsPartitionSession) {
    //     /*
    //     If there are pending StartDirectReadPartitionSession requests that were delayed due to previous errors,
    //     and the entire session then loses connection for an extended period of time (greater than the callback delays),
    //     the following process should be followed:

    //     When the session finally reconnects, the pending Start requests should be sent immediately.
    //     This is because their callbacks have already been fired, but the requests were not sent due to the lack of connection.

    //     client -> server: InitRequest
    //     client <-- server: InitResponse
    //     client -> server: StartDirectReadPartitionSessionRequest
    //     client <- server: StopDirectReadPartitionSession(OVERLOADED)
    //     note over client: Wait 1 second before sending Start again
    //     ... Connection lost ...
    //     note over client: SendStart... callback fires, resets state
    //     ... Connection reestablished in 1 minute ...
    //     client -> server: InitRequest
    //     client <-- server: InitResponse
    //     note over client: Send the Start request immediately
    //     client -> server: StartDirectReadPartitionSessionRequest
    //     */

    //     TDirectReadSessionImplTestSetup setup;
    //     setup.ReadSessionSettings.RetryPolicy(setup.MockRetryPolicy);

    //     auto gotFinalStart = NThreading::NewPromise();
    //     auto calledRead = NThreading::NewPromise();
    //     TPartitionSessionId partitionSessionId = 1;
    //     auto secondProcessor = MakeIntrusive<TMockDirectReadSessionProcessor>();
    //     auto delay = TDuration::MilliSeconds(1);

    //     {
    //         ::testing::InSequence sequence;

    //         EXPECT_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(1))
    //             .WillOnce([&]() { setup.MockDirectReadProcessorFactory->CreateProcessor(setup.MockDirectReadProcessor); });

    //         EXPECT_CALL(*setup.MockDirectReadProcessor, OnInitRequest(_))
    //             .Times(1);

    //         EXPECT_CALL(*setup.MockDirectReadProcessor, OnStartDirectReadPartitionSessionRequest(_))
    //             .Times(1);

    //         // The client receives StopDirectReadPartitionSession, create TDirectReadSession::PartitionSessions[i].RetryState
    //         EXPECT_CALL(*setup.MockRetryPolicy, CreateRetryState())
    //             .WillOnce(Return(std::make_unique<TMockRetryState>(setup.MockRetryPolicy)));

    //         // The client loses connection, create TDirectReadSession.RetryState
    //         EXPECT_CALL(*setup.MockRetryPolicy, CreateRetryState())
    //             .WillOnce(Return(std::make_unique<TMockRetryState>(setup.MockRetryPolicy)));

    //         // The connection is lost at this point, the client tries to reconnect.
    //         EXPECT_CALL(*setup.MockDirectReadProcessorFactory, OnCreateProcessor(2))
    //             .WillOnce([&]() { setup.MockDirectReadProcessorFactory->CreateProcessor(secondProcessor); });

    //         EXPECT_CALL(*secondProcessor, OnInitRequest(_))
    //             .Times(1);

    //         EXPECT_CALL(*secondProcessor, OnStartDirectReadPartitionSessionRequest(_))
    //             .WillOnce([&]() { gotFinalStart.SetValue(); });
    //     }

    //     std::function<void()> callback;

    //     class TControlCallbacks : public IDirectReadSessionControlCallbacks {
    //     public:
    //         TControlCallbacks(TDuration delay, std::function<void()>& callback) : Delay(delay), Callback(callback) {}
    //         void ScheduleCallback(TDuration d, std::function<void()> cb, TDeferredActions<false>&) override {
    //             UNIT_ASSERT_EQUAL(Delay, d);
    //             Callback = cb;
    //         }
    //         TDuration Delay;
    //         std::function<void()>& Callback;
    //     };

    //     auto session = setup.GetDirectReadSession(std::make_shared<TControlCallbacks>(delay, callback));

    //     session->Start();
    //     setup.MockDirectReadProcessorFactory->Wait();

    //     session->AddPartitionSession({ .PartitionSessionId = partitionSessionId, .Location = {2, 3} });

    //     setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
    //         .InitResponse());

    //     setup.MockRetryPolicy->Delay = delay;

    //     setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
    //         .StopDirectReadPartitionSession(Ydb::StatusIds::OVERLOADED, partitionSessionId));

    //     // Besides logs, these durations don't really affect anything in tests.
    //     setup.MockRetryPolicy->Delay = TDuration::Seconds(10);

    //     setup.AddDirectReadResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
    //         .Failure());

    //     // Delayed callback is fired, but there is no connection, so the partition session state changes to IDLE,
    //     // and the request should be sent after receiving InitResponse.
    //     callback();

    //     secondProcessor->AddServerResponse(TMockDirectReadSessionProcessor::TServerReadInfo()
    //         .InitResponse());

    //     gotFinalStart.GetFuture().Wait();

    //     secondProcessor->Wait();
    //     secondProcessor->Validate();
    // }

// } Y_UNIT_TEST_SUITE_F(DirectReadSession)

/*
This suite tests direct read session interaction with server.

It complements tests from basic_usage_ut.cpp etc, as we run them with direct read disabled/enabled.
*/

class DirectReadWithServer : public TTopicTestFixture {};

TEST_F(DirectReadWithServer, Devslice) {
    GTEST_SKIP() << "Skipping devslice test";

    auto driverConfig = NYdb::TDriverConfig()
        .SetEndpoint(std::getenv("YDB_ENDPOINT"))
        .SetDatabase("/Root/testdb")
        .SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG).Release()))
        .SetAuthToken(std::getenv("YDB_TOKEN"));

    auto driver = NYdb::TDriver(driverConfig);

    auto clientSettings = TTopicClientSettings();
    auto client = TTopicClient(driver, clientSettings);

    auto settings = TReadSessionSettings()
        .AppendTopics(TTopicReadSettings("t1").AppendPartitionIds({0}))
        .ConsumerName("c1")
        // .DirectRead(true)
        ;

    settings.EventHandlers_
        .StartPartitionSessionHandler([](TReadSessionEvent::TStartPartitionSessionEvent& e) {
            e.Confirm();
        })
        .StopPartitionSessionHandler([](TReadSessionEvent::TStopPartitionSessionEvent& e) {
            e.Confirm();
        })
        .DataReceivedHandler([](TReadSessionEvent::TDataReceivedEvent& e) {
            for (std::size_t i = 0; i < e.GetMessages().size(); ++i) {
                auto& m = e.GetMessages()[i];
                std::cerr << "Message: " << m.GetData() << std::endl;
                m.Commit();
            }
        });

    auto reader = client.CreateReadSession(settings);

    std::this_thread::sleep_for(std::chrono::seconds(1000));

    reader->Close();
}

} // namespace NYdb::NTopic::NTests
