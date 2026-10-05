#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/testlib/common/test_with_actor_system.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_control_plane_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor_base.h>
#include <ydb/library/yql/providers/pq/common/events.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>

#include <atomic>
#include <optional>
#include <stdexcept>

namespace NYql::NDq {
namespace {

using namespace NActors;
using namespace NYdb::NTopic;
using TEvResult = TPqControlPlaneEvents::TEvDescribeConsumerResult;

struct TEvDescribeCalled : TEventLocal<TEvDescribeCalled, EventSpaceBegin(TEvents::ES_PRIVATE) + 130> {};
struct TEvCommitCalled : TEventLocal<TEvCommitCalled, EventSpaceBegin(TEvents::ES_PRIVATE) + 131> {};

void InitializeRuntime(TTestActorRuntime& runtime) {
    runtime.SetDispatchTimeout(TDuration::Seconds(10));
    auto names = MakeIntrusive<TTableNameserverSetup>();
    for (ui32 i = 0; i < runtime.GetNodeCount(); ++i) {
        names->StaticNodeTable[runtime.GetNodeId(i)] = std::make_pair(TString("localhost"), 10000u + i);
    }
    for (ui32 i = 0; i < runtime.GetNodeCount(); ++i) {
        runtime.AddLocalService(GetNameserviceActorId(),
            TActorSetupCmd(CreateNameserverTable(names), TMailboxType::ReadAsFilled, runtime.InterconnectPoolId()), i);
    }
    NKikimr::TAppPrepare app;
    runtime.Initialize(app.Unwrap());
}

void CheckUnavailableResponse(TTestActorRuntime& runtime, TActorId reader, ui64 cookie, const TString& message) {
    const auto response = runtime.GrabEdgeEvent<TEvResult>(reader);
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
    const auto& record = response->Get()->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), Ydb::StatusIds::UNAVAILABLE);
    UNIT_ASSERT_VALUES_EQUAL(record.IssuesSize(), 1);
    UNIT_ASSERT_STRING_CONTAINS(record.GetIssues(0).message(), message);
    UNIT_ASSERT_VALUES_EQUAL(record.PartitionsSize(), 0);
}

class TTopicClient final : public ITopicClient {
public:
    NThreading::TPromise<TDescribeConsumerResult> Description = NThreading::NewPromise<TDescribeConsumerResult>();
    THashMap<TString, TAsyncDescribeConsumerResult> DescriptionResults;
    std::atomic<ui32> Calls = 0;
    std::atomic<bool> ThrowOnDescribe = false;
    std::optional<NYdb::TAsyncStatus> CommitResult;
    bool ThrowOnCommit = false;
    TActorId Observer;

    TAsyncDescribeConsumerResult DescribeConsumer(const TString& path, const TString&, const TDescribeConsumerSettings& settings) override {
        UNIT_ASSERT(settings.IncludeStats_);
        ++Calls;
        TActivationContext::ActorSystem()->Send(Observer, new TEvDescribeCalled());
        if (ThrowOnDescribe) {
            throw std::runtime_error("describe exception");
        }
        return DescriptionResults.Value(path, Description.GetFuture());
    }

    TAsyncDescribeTopicResult DescribeTopic(const TString&, const TDescribeTopicSettings&) override {
        Y_ABORT("Unexpected DescribeTopic");
    }
    TAsyncDescribePartitionResult DescribePartition(const TString&, i64, const TDescribePartitionSettings&) override {
        Y_ABORT("Unexpected DescribePartition");
    }
    std::shared_ptr<IReadSession> CreateReadSession(const TReadSessionSettings&) override {
        Y_ABORT("Unexpected CreateReadSession");
    }
    std::shared_ptr<ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(const TWriteSessionSettings&) override {
        Y_ABORT("Unexpected CreateSimpleBlockingWriteSession");
    }
    std::shared_ptr<IWriteSession> CreateWriteSession(const TWriteSessionSettings&) override {
        Y_ABORT("Unexpected CreateWriteSession");
    }
    NYdb::TAsyncStatus CommitOffset(const TString&, ui64, const TString&, ui64, const TCommitOffsetSettings&) override {
        UNIT_ASSERT_C(CommitResult, "Unexpected CommitOffset");
        TActivationContext::ActorSystem()->Send(Observer, new TEvCommitCalled());
        if (ThrowOnCommit) {
            throw std::runtime_error("rewind exception");
        }
        return *CommitResult;
    }
};

class TGateway final : public IPqStaticGateway {
public:
    explicit TGateway(ITopicClient::TPtr client)
        : Client(std::move(client))
    {}

    ITopicClient::TPtr GetTopicClient(const NYdb::TDriver&, const TTopicClientSettings&) override {
        return Client;
    }
    TTopicClientSettings GetTopicClientSettings() const override {
        return {};
    }
    IFederatedTopicClient::TPtr GetFederatedTopicClient(const NYdb::TDriver&, const NYdb::NFederatedTopic::TFederatedTopicClientSettings&) override {
        Y_ABORT("Unexpected GetFederatedTopicClient");
    }
    IDeferredPublishClient::TPtr GetDeferredPublishClient(const NYdb::TDriver&, const NYdb::TCommonClientSettings&) override {
        Y_ABORT("Unexpected GetDeferredPublishClient");
    }
    NYdb::NFederatedTopic::TFederatedTopicClientSettings GetFederatedTopicClientSettings() const override {
        Y_ABORT("Unexpected GetFederatedTopicClientSettings");
    }

private:
    const ITopicClient::TPtr Client;
};

class TCredentialsFactory final : public IStructuredTokenCredentialsFactory {
public:
    TString LastToken;
    bool LastAddBearerToToken = false;

    std::shared_ptr<NYdb::ICredentialsProviderFactory> Create(const TString& token, bool addBearerToToken) override {
        LastToken = token;
        LastAddBearerToToken = addBearerToToken;
        return NYdb::CreateInsecureCredentialsProviderFactory();
    }
};

class TFixture : public NTestUtils::TTestWithActorSystemFixture {
public:
    void SetUp(NUnitTest::TTestContext& ctx) override {
        TTestWithActorSystemFixture::SetUp(ctx);
        Client->Observer = Runtime.AllocateEdgeActor();
        TDqAsyncIoFactory factory;
        RegisterDqPqControlPlaneActorFactory(factory, Driver, CredentialsFactory, MakeIntrusive<TGateway>(Client));
        const THashMap<TString, TString> secureParams = {
            {"token-name", "secret-token"},
            {"another-token-name", "another-secret-token"},
        };
        ControlPlaneId = Runtime.Register(factory.CreateDqControlPlane({
            .Type = PqControlPlaneActorType,
            .TxId = "test",
            .SecureParams = secureParams,
        }));
    }

protected:
    static NPq::NProto::TEvDescribeConsumer MakeRequest(ui64 partitionId = 0) {
        NPq::NProto::TEvDescribeConsumer request;
        auto* connection = request.MutableConnection();
        connection->SetEndpoint("endpoint");
        connection->SetDatabase("database");
        connection->SetTopicPath("topic");
        connection->SetConsumerName("consumer");
        connection->SetUseSsl(false);
        connection->SetAddBearerToToken(false);
        connection->SetTokenName("token-name");
        request.AddPartitionIds(partitionId);
        return request;
    }

    TActorId Request(const NPq::NProto::TEvDescribeConsumer& record, ui64 cookie = 0) {
        const auto reader = Runtime.AllocateEdgeActor();
        auto event = MakeHolder<TPqControlPlaneEvents::TEvDescribeConsumer>();
        event->Record = record;
        Runtime.Send(new IEventHandle(ControlPlaneId, reader, event.Release(), 0, cookie));
        return reader;
    }

    void WaitForDescribe() {
        UNIT_ASSERT(Runtime.GrabEdgeEvent<TEvDescribeCalled>(Client->Observer));
    }

    static TDescribeConsumerResult MakeDescription(NYdb::EStatus status = NYdb::EStatus::SUCCESS) {
        Ydb::Topic::DescribeConsumerResult result;
        for (ui64 id = 0; id < 3; ++id) {
            auto* partition = result.add_partitions();
            partition->set_partition_id(id);
            partition->mutable_partition_stats()->mutable_partition_offsets()->set_start(id + 10);
            partition->mutable_partition_consumer_stats()->set_committed_offset(id + 20);
        }
        return TDescribeConsumerResult(
            NYdb::TStatus(status, {NYdb::NIssue::TIssue("describe details")}), std::move(result));
    }

    void CompleteDescription(NYdb::EStatus status = NYdb::EStatus::SUCCESS) {
        Client->Description.SetValue(MakeDescription(status));
    }

    void CheckResponse(TActorId reader, ui64 partitionId, ui64 cookie = 0) {
        const auto response = Runtime.GrabEdgeEvent<TEvResult>(reader);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
        const auto& record = response->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(record.PartitionsSize(), 1);
        const auto& offsets = record.GetPartitions(0);
        UNIT_ASSERT_VALUES_EQUAL(offsets.GetPartitionId(), partitionId);
        UNIT_ASSERT_VALUES_EQUAL(offsets.GetStartOffset(), partitionId + 10);
        UNIT_ASSERT_VALUES_EQUAL(offsets.GetCommittedOffset(), partitionId + 20);
    }

    const NYdb::TDriver Driver{NYdb::TDriverConfig()};
    const TIntrusivePtr<TTopicClient> Client = MakeIntrusive<TTopicClient>();
    const std::shared_ptr<TCredentialsFactory> CredentialsFactory = std::make_shared<TCredentialsFactory>();
    TActorId ControlPlaneId;
};

// Exercises the shared startup implementation without a data-plane session.
class TReader final : public TActorBootstrapped<TReader>, public NInternal::TDqPqReadActorBase {
public:
    TReader(TActorId compute, TActorId controlPlane, ITopicClient::TPtr topicClient)
        : TDqPqReadActorBase(0, 0, {}, "test", Source(), ReadTaskParams(), compute, controlPlane)
        , TopicClient(std::move(topicClient))
    {}

    void Bootstrap() {
        Become(&TThis::StateFunc);
        InitConsumerOffsets(SelfId(), {}, TopicClient, 1);
    }

private:
    static NPq::NProto::TDqPqTopicSource Source() {
        NPq::NProto::TDqPqTopicSource source;
        source.SetConsumerName("consumer");
        source.SetTopicPath("topic");
        source.MutableToken()->SetName("token-name");
        source.MutableDisposition()->mutable_oldest();
        source.SetAllowConsumerRewindForDisposition(true);
        return source;
    }

    static TVector<NPq::NProto::TDqReadTaskParams> ReadTaskParams() {
        TVector<NPq::NProto::TDqReadTaskParams> params(1);
        auto* partitioning = params[0].AddPartitioningParams();
        partitioning->SetTopicPartitionsCount(1);
        partitioning->SetDqPartitionsCount(1);
        return params;
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            case TEvents::TEvPing::EventType:
                Send(ev->Sender, new TEvents::TEvPong(), 0, ConsumerOffsetsInitialized());
                break;
            hFunc(TEvents::TEvInvokeResult, HandleConsumerOffsets);
            default:
                Y_ABORT("Unexpected event %s", ev->GetTypeName().data());
        }
    }

    void OnConsumerOffsetsInitialized() override {
        Send(ComputeActorId, new TEvents::TEvWakeup());
    }
    void SchedulePartitionIdlenessCheck(TInstant) override {}
    void InitWatermarkTracker() override {}
    void CommitState(const NDqProto::TCheckpoint&) override {}
    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch&, TMaybe<TInstant>&, bool&, i64) override {
        return 0;
    }
    void PassAway() override {
        StopConsumerOffsetInitialization();
        TActorBootstrapped<TReader>::PassAway();
    }

    const ITopicClient::TPtr TopicClient;
};

} // namespace

Y_UNIT_TEST_SUITE(TDqPqControlPlaneTest) {
    Y_UNIT_TEST_F(ResolvesTokenFromSecureParams, TFixture) {
        CompleteDescription();
        auto request = MakeRequest();
        request.MutableConnection()->SetAddBearerToToken(true);
        CheckResponse(Request(request), 0);
        UNIT_ASSERT_VALUES_EQUAL(CredentialsFactory->LastToken, "secret-token");
        UNIT_ASSERT(CredentialsFactory->LastAddBearerToToken);
    }

    Y_UNIT_TEST_F(CoalescesInflightAndCachesDescription, TFixture) {
        const auto first = Request(MakeRequest(0), 11);
        const auto second = Request(MakeRequest(1), 12);
        auto otherTopic = MakeRequest(2);
        otherTopic.MutableConnection()->SetTopicPath("other-topic");
        const auto third = Request(otherTopic);
        // The second SDK call is a barrier: both requests for the first topic
        // have been processed, and neither description has completed yet.
        WaitForDescribe();
        WaitForDescribe();
        UNIT_ASSERT_VALUES_EQUAL(Client->Calls.load(), 2);
        CompleteDescription();
        CheckResponse(first, 0, 11);
        CheckResponse(second, 1, 12);
        CheckResponse(third, 2);
        CheckResponse(Request(MakeRequest(2)), 2);
        UNIT_ASSERT_VALUES_EQUAL(Client->Calls.load(), 2);
    }

    Y_UNIT_TEST_F(CacheSeparatesConnectionsAndCredentials, TFixture) {
        CompleteDescription();
        CheckResponse(Request(MakeRequest()), 0);
        for (ui32 field = 0; field < 7; ++field) {
            auto request = MakeRequest();
            auto& connection = *request.MutableConnection();
            switch (field) {
                case 0: connection.SetEndpoint("another-endpoint"); break;
                case 1: connection.SetDatabase("another-database"); break;
                case 2: connection.SetTopicPath("another-topic"); break;
                case 3: connection.SetConsumerName("another-consumer"); break;
                case 4: connection.SetUseSsl(true); break;
                case 5: connection.SetAddBearerToToken(true); break;
                case 6: connection.SetTokenName("another-token-name"); break;
            }
            CheckResponse(Request(request), 0);
            UNIT_ASSERT_VALUES_EQUAL(CredentialsFactory->LastToken, field == 6 ? "another-secret-token" : "secret-token");
            UNIT_ASSERT_VALUES_EQUAL(CredentialsFactory->LastAddBearerToToken, field == 5);
        }
        CheckResponse(Request(MakeRequest()), 0);
        UNIT_ASSERT_VALUES_EQUAL(Client->Calls.load(), 8);
    }

    Y_UNIT_TEST_TWIN_F(DescribeErrorIsSharedAndNotRetried, TransportError, TFixture) {
        CompleteDescription(TransportError ? NYdb::EStatus::TRANSPORT_UNAVAILABLE : NYdb::EStatus::UNAUTHORIZED);
        for (ui32 i = 0; i < 2; ++i) {
            const auto response = Runtime.GrabEdgeEvent<TEvResult>(Request(MakeRequest()));
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(),
                TransportError ? Ydb::StatusIds::EXTERNAL_ERROR : Ydb::StatusIds::UNAUTHORIZED);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetIssues(0).message(), "describe details");
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.PartitionsSize(), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(Client->Calls.load(), 1);
    }

    Y_UNIT_TEST_TWIN_F(FatalExceptionFailsCurrentAndFutureRequests, Synchronous, TFixture) {
        auto failedDescription = NThreading::NewPromise<TDescribeConsumerResult>();
        Client->DescriptionResults.emplace("failed-topic", failedDescription.GetFuture());
        Client->DescriptionResults.emplace("cached-topic", NThreading::MakeFuture(MakeDescription()));

        auto cachedRequest = MakeRequest();
        cachedRequest.MutableConnection()->SetTopicPath("cached-topic");
        CheckResponse(Request(cachedRequest), 0);
        WaitForDescribe();

        const auto first = Request(MakeRequest(0), 11);
        const auto second = Request(MakeRequest(1), 12);
        auto otherTopic = MakeRequest(2);
        otherTopic.MutableConnection()->SetTopicPath("other-topic");
        const auto third = Request(otherTopic, 13);
        WaitForDescribe();
        WaitForDescribe();

        Client->ThrowOnDescribe = Synchronous;
        auto failedRequest = MakeRequest();
        failedRequest.MutableConnection()->SetTopicPath("failed-topic");
        const auto failedReader = Request(failedRequest, 14);
        WaitForDescribe();
        if constexpr (!Synchronous) {
            failedDescription.SetException(std::make_exception_ptr(std::runtime_error("describe exception")));
        }

        const auto checkError = [&](TActorId reader, ui64 cookie) {
            const auto response = Runtime.GrabEdgeEvent<TEvResult>(reader);
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
            const auto& record = response->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), Ydb::StatusIds::INTERNAL_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(record.IssuesSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(record.GetIssues(0).message(), "describe exception");
            UNIT_ASSERT_VALUES_EQUAL(record.PartitionsSize(), 0);
        };
        checkError(first, 11);
        checkError(second, 12);
        checkError(third, 13);
        checkError(failedReader, 14);

        // Late successful SDK results must not replace the fatal error or revive the cache.
        CompleteDescription();
        checkError(Request(MakeRequest(), 21), 21);
        checkError(Request(cachedRequest, 22), 22);
        checkError(Request(failedRequest, 23), 23);
        auto newTopic = MakeRequest();
        newTopic.MutableConnection()->SetTopicPath("new-topic");
        checkError(Request(newTopic, 24), 24);
        UNIT_ASSERT_VALUES_EQUAL(Client->Calls.load(), 4);
    }

    Y_UNIT_TEST_F(UndeliveredResponseFailsPendingAndFutureRequests, TFixture) {
        Client->DescriptionResults.emplace("ready-topic", NThreading::MakeFuture(MakeDescription()));
        const auto pendingReader = Request(MakeRequest(), 11);
        WaitForDescribe();

        auto readyRequest = MakeRequest();
        readyRequest.MutableConnection()->SetTopicPath("ready-topic");
        auto event = MakeHolder<TPqControlPlaneEvents::TEvDescribeConsumer>();
        event->Record = readyRequest;
        const TActorId missingReader(Runtime.GetNodeId(0), "gone-reader");
        Runtime.Send(new IEventHandle(ControlPlaneId, missingReader, event.Release(), 0, 12));
        WaitForDescribe();

        constexpr char error[] = "Failed to deliver PQ control-plane event";
        CheckUnavailableResponse(Runtime, pendingReader, 11, error);
        // An SDK completion already in flight cannot revive the failed actor.
        CompleteDescription();
        CheckUnavailableResponse(Runtime, Request(MakeRequest(), 13), 13, error);
        CheckUnavailableResponse(Runtime, Request(readyRequest, 14), 14, error);
        auto newRequest = MakeRequest();
        newRequest.MutableConnection()->SetTopicPath("new-topic");
        CheckUnavailableResponse(Runtime, Request(newRequest, 15), 15, error);
        UNIT_ASSERT_VALUES_EQUAL(Client->Calls.load(), 2);
    }

    Y_UNIT_TEST_TWIN_F(RetriesFatalErrorOnUndelivery, InitiallySuccessful, TFixture) {
        Client->ThrowOnDescribe = !InitiallySuccessful;
        if constexpr (InitiallySuccessful) {
            CompleteDescription();
        }
        const auto reader = Request(MakeRequest(), 71);
        auto response = Runtime.GrabEdgeEvent<TEvResult>(reader);
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(),
            InitiallySuccessful ? Ydb::StatusIds::SUCCESS : Ydb::StatusIds::INTERNAL_ERROR);

        TString fatalError;
        for (ui32 attempt = 0; attempt < 3; ++attempt) {
            Runtime.Send(new IEventHandle(ControlPlaneId, reader,
                new TEvents::TEvUndelivered(TEvResult::EventType, TEvents::TEvUndelivered::Disconnected), 0, 71));
            response = Runtime.GrabEdgeEvent<TEvResult>(reader);
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(response->Cookie, 71);
            const auto& record = response->Get()->Record;
            UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(),
                InitiallySuccessful ? Ydb::StatusIds::UNAVAILABLE : Ydb::StatusIds::INTERNAL_ERROR);
            UNIT_ASSERT_VALUES_EQUAL(record.IssuesSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(record.PartitionsSize(), 0);
            if (attempt == 0) {
                fatalError = record.SerializeAsString();
            } else {
                UNIT_ASSERT_VALUES_EQUAL(record.SerializeAsString(), fatalError);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(Client->Calls.load(), 1);
    }

    Y_UNIT_TEST_F(RetriesFatalRepliesIndependentlyForRequestCookies, TFixture) {
        Client->ThrowOnDescribe = true;
        const auto reader = Request(MakeRequest(), 71);
        const auto first = Runtime.GrabEdgeEvent<TEvResult>(reader);
        UNIT_ASSERT(first);
        const auto fatalError = first->Get()->Record.SerializeAsString();

        auto event = MakeHolder<TPqControlPlaneEvents::TEvDescribeConsumer>();
        event->Record = MakeRequest();
        Runtime.Send(new IEventHandle(ControlPlaneId, reader, event.Release(), 0, 72));
        const auto second = Runtime.GrabEdgeEvent<TEvResult>(reader);
        UNIT_ASSERT(second);
        UNIT_ASSERT_VALUES_EQUAL(second->Cookie, 72);

        for (ui64 cookie : {71, 72}) {
            Runtime.Send(new IEventHandle(ControlPlaneId, reader,
                new TEvents::TEvUndelivered(TEvResult::EventType, TEvents::TEvUndelivered::Disconnected), 0, cookie));
        }
        THashSet<ui64> cookies;
        for (ui32 i = 0; i < 2; ++i) {
            const auto response = Runtime.GrabEdgeEvent<TEvResult>(reader);
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.SerializeAsString(), fatalError);
            UNIT_ASSERT(cookies.insert(response->Cookie).second);
        }
        UNIT_ASSERT(cookies.contains(71));
        UNIT_ASSERT(cookies.contains(72));
        UNIT_ASSERT_VALUES_EQUAL(Client->Calls.load(), 1);
    }

    Y_UNIT_TEST_F(LateUndeliveredResponseGetsOriginalFatalError, TFixture) {
        CompleteDescription();
        const auto first = Request(MakeRequest(), 71);
        const auto second = Request(MakeRequest(), 72);
        CheckResponse(first, 0, 71);
        CheckResponse(second, 0, 72);

        Runtime.Send(new IEventHandle(ControlPlaneId, first,
            new TEvents::TEvUndelivered(TEvResult::EventType, TEvents::TEvUndelivered::Disconnected), 0, 71));
        const auto firstError = Runtime.GrabEdgeEvent<TEvResult>(first);
        UNIT_ASSERT(firstError);
        UNIT_ASSERT_VALUES_EQUAL(firstError->Get()->Record.GetStatus(), Ydb::StatusIds::UNAVAILABLE);

        Runtime.Send(new IEventHandle(ControlPlaneId, second,
            new TEvents::TEvUndelivered(TEvResult::EventType, TEvents::TEvUndelivered::Disconnected), 0, 72));
        const auto secondError = Runtime.GrabEdgeEvent<TEvResult>(second);
        UNIT_ASSERT(secondError);
        UNIT_ASSERT_VALUES_EQUAL(secondError->Cookie, 72);
        UNIT_ASSERT_VALUES_EQUAL(secondError->Get()->Record.SerializeAsString(), firstError->Get()->Record.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(Client->Calls.load(), 1);
    }

    Y_UNIT_TEST(FatalErrorRetriesAreCoalescedAndContinueUntilPoison) {
        TTestActorRuntime runtime;
        InitializeRuntime(runtime);
        const NYdb::TDriver driver{NYdb::TDriverConfig()};
        const auto client = MakeIntrusive<TTopicClient>();
        client->Observer = runtime.AllocateEdgeActor();
        client->ThrowOnDescribe = true;
        const auto controlPlane = runtime.Register(CreateDqPqControlPlaneActor(
            driver, std::make_shared<TCredentialsFactory>(), MakeIntrusive<TGateway>(client), {}));
        const auto reader = runtime.AllocateEdgeActor();
        const auto barrier = runtime.AllocateEdgeActor();
        runtime.Send(new IEventHandle(controlPlane, reader, new TPqControlPlaneEvents::TEvDescribeConsumer(), 0, 71));
        const auto original = runtime.GrabEdgeEvent<TEvResult>(reader);
        UNIT_ASSERT(original);
        UNIT_ASSERT_VALUES_EQUAL(original->Get()->Record.GetStatus(), Ydb::StatusIds::INTERNAL_ERROR);

        // Hold timers so duplicate failures are processed before a retry fires.
        std::vector<THolder<IEventHandle>> retries;
        runtime.SetScheduledEventFilter([&](auto&, auto& event, TDuration delay, auto&) {
            if (event->Recipient == controlPlane) {
                UNIT_ASSERT(delay > TDuration::Zero());
                UNIT_ASSERT(delay <= TDuration::Seconds(1));
                retries.emplace_back(event.Release());
                return true;
            }
            return false;
        });

        for (ui32 attempt = 0; attempt < 20; ++attempt) {
            for (ui32 duplicate = 0; duplicate < 2; ++duplicate) {
                runtime.Send(new IEventHandle(controlPlane, reader,
                    new TEvents::TEvUndelivered(TEvResult::EventType, TEvents::TEvUndelivered::Disconnected), 0, 71));
            }
            runtime.Send(new IEventHandle(controlPlane, barrier, new TPqControlPlaneEvents::TEvDescribeConsumer()));
            UNIT_ASSERT(runtime.GrabEdgeEvent<TEvResult>(barrier));
            UNIT_ASSERT_VALUES_EQUAL(retries.size(), 1);
            runtime.Send(retries.back().Release());
            retries.clear();
            const auto response = runtime.GrabEdgeEvent<TEvResult>(reader);
            UNIT_ASSERT(response);
            UNIT_ASSERT_VALUES_EQUAL(response->Cookie, 71);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.SerializeAsString(), original->Get()->Record.SerializeAsString());
        }

        runtime.Send(new IEventHandle(controlPlane, reader,
            new TEvents::TEvUndelivered(TEvResult::EventType, TEvents::TEvUndelivered::Disconnected), 0, 71));
        runtime.Send(new IEventHandle(controlPlane, barrier, new TPqControlPlaneEvents::TEvDescribeConsumer()));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvResult>(barrier));
        UNIT_ASSERT_VALUES_EQUAL(retries.size(), 1);

        // A pending retry must not revive the actor after its owner stops it.
        runtime.Send(new IEventHandle(controlPlane, reader, new TEvents::TEvPoison()));
        runtime.Send(retries.back().Release());
        retries.clear();
        runtime.Send(new IEventHandle(controlPlane, reader, new TPqControlPlaneEvents::TEvDescribeConsumer(),
            IEventHandle::FlagTrackDelivery, 72));
        const auto undelivered = runtime.GrabEdgeEvent<TEvents::TEvUndelivered>(reader);
        UNIT_ASSERT(undelivered);
        UNIT_ASSERT_VALUES_EQUAL(undelivered->Cookie, 72);
        UNIT_ASSERT_VALUES_EQUAL(undelivered->Get()->Reason, TEvents::TEvUndelivered::ReasonActorUnknown);
        UNIT_ASSERT(retries.empty());
        UNIT_ASSERT_VALUES_EQUAL(client->Calls.load(), 1);
    }

    Y_UNIT_TEST(DisconnectedReaderFailsPendingAndFutureRequests) {
        TTestActorRuntime runtime(2, true);
        InitializeRuntime(runtime);
        const NYdb::TDriver driver{NYdb::TDriverConfig()};
        const auto client = MakeIntrusive<TTopicClient>();
        client->Observer = runtime.AllocateEdgeActor();
        client->DescriptionResults.emplace("ready-topic", NThreading::MakeFuture(TDescribeConsumerResult(
            NYdb::TStatus(NYdb::EStatus::SUCCESS, {}), Ydb::Topic::DescribeConsumerResult{})));
        const auto controlPlane = runtime.Register(CreateDqPqControlPlaneActor(
            driver, std::make_shared<TCredentialsFactory>(), MakeIntrusive<TGateway>(client), {}));
        const auto request = [&](const TString& topic, ui64 cookie, ui32 nodeIndex = 0) {
            const auto reader = runtime.AllocateEdgeActor(nodeIndex);
            auto event = MakeHolder<TPqControlPlaneEvents::TEvDescribeConsumer>();
            event->Record.MutableConnection()->SetTopicPath(topic);
            runtime.Send(new IEventHandle(controlPlane, reader, event.Release(), 0, cookie), nodeIndex);
            return reader;
        };

        const auto pendingReader = request("pending-topic", 11);
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvDescribeCalled>(client->Observer));
        const auto readyReader = request("ready-topic", 12, 1);
        const auto ready = runtime.GrabEdgeEvent<TEvResult>(readyReader);
        UNIT_ASSERT(ready);
        UNIT_ASSERT(ready->InterconnectSession);
        UNIT_ASSERT_VALUES_EQUAL(ready->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvDescribeCalled>(client->Observer));
        const auto remotePendingReader = request("remote-pending-topic", 13, 1);
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvDescribeCalled>(client->Observer));

        // Receiving the reply ensures its session subscription has been established.
        runtime.DisconnectNodes(0, 1);
        constexpr char error[] = "PQ control-plane reader node disconnected";
        CheckUnavailableResponse(runtime, pendingReader, 11, error);
        // The fatal reply can reach the waiting reader through a new session.
        CheckUnavailableResponse(runtime, remotePendingReader, 13, error);
        client->Description.SetValue(TDescribeConsumerResult(
            NYdb::TStatus(NYdb::EStatus::SUCCESS, {}), Ydb::Topic::DescribeConsumerResult{}));
        CheckUnavailableResponse(runtime, request("pending-topic", 14), 14, error);
        CheckUnavailableResponse(runtime, request("ready-topic", 15), 15, error);
        const auto newRemoteReader = request("new-topic", 16, 1);
        CheckUnavailableResponse(runtime, newRemoteReader, 16, error);

        // Further disconnects in the fatal state retry every error reply on that
        // node, including replies to requests received after the first failure.
        for (ui32 attempt = 0; attempt < 2; ++attempt) {
            runtime.DisconnectNodes(0, 1);
            CheckUnavailableResponse(runtime, remotePendingReader, 13, error);
            CheckUnavailableResponse(runtime, newRemoteReader, 16, error);
        }
        UNIT_ASSERT_VALUES_EQUAL(client->Calls.load(), 3);
    }

    Y_UNIT_TEST(UndeliveredFailsStartup) {
        TTestActorRuntime runtime(1, true);
        InitializeRuntime(runtime);
        const auto compute = runtime.AllocateEdgeActor();
        const TActorId missing(runtime.GetNodeId(0), "missing-cp");
        runtime.Register(new TReader(compute, missing, MakeIntrusive<TTopicClient>()));
        const auto error = runtime.GrabEdgeEvent<IDqComputeActorAsyncInput::TEvAsyncInputError>(compute);
        UNIT_ASSERT(error);
        UNIT_ASSERT_STRING_CONTAINS(error->Get()->Issues.ToString(), "Failed to deliver consumer description request");
    }

    Y_UNIT_TEST_TWIN(RewindExceptionFailsStartup, Synchronous) {
        TTestActorRuntime runtime(1, true);
        InitializeRuntime(runtime);
        const auto compute = runtime.AllocateEdgeActor();
        const auto controlPlane = runtime.AllocateEdgeActor();
        const auto client = MakeIntrusive<TTopicClient>();
        client->Observer = runtime.AllocateEdgeActor();
        auto commit = NThreading::NewPromise<NYdb::TStatus>();
        client->CommitResult = commit.GetFuture();
        client->ThrowOnCommit = Synchronous;
        const auto reader = runtime.Register(new TReader(compute, controlPlane, client));
        const auto request = runtime.GrabEdgeEvent<TPqControlPlaneEvents::TEvDescribeConsumer>(controlPlane);
        UNIT_ASSERT(request);
        auto response = MakeHolder<TEvResult>();
        response->Record.SetStatus(Ydb::StatusIds::SUCCESS);
        auto* partition = response->Record.AddPartitions();
        partition->SetPartitionId(0);
        partition->SetStartOffset(0);
        partition->SetCommittedOffset(1);
        runtime.Send(new IEventHandle(request->Sender, controlPlane, response.Release(), 0, request->Cookie));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvCommitCalled>(client->Observer));
        if constexpr (!Synchronous) {
            commit.SetException(std::make_exception_ptr(std::runtime_error("rewind exception")));
        }

        const auto error = runtime.GrabEdgeEvent<IDqComputeActorAsyncInput::TEvAsyncInputError>(compute);
        UNIT_ASSERT(error);
        UNIT_ASSERT_STRING_CONTAINS(error->Get()->Issues.ToString(), "rewind exception");
        runtime.Send(new IEventHandle(reader, compute, new TEvents::TEvPing()));
        const auto pong = runtime.GrabEdgeEvent<TEvents::TEvPong>(compute);
        UNIT_ASSERT(pong);
        UNIT_ASSERT_VALUES_EQUAL(pong->Cookie, 0);
    }

    Y_UNIT_TEST(RemoteDescriptionInitializesReader) {
        TTestActorRuntime runtime(2, true);
        InitializeRuntime(runtime);
        const NYdb::TDriver driver{NYdb::TDriverConfig()};
        const auto client = MakeIntrusive<TTopicClient>();
        client->Observer = runtime.AllocateEdgeActor(1);
        Ydb::Topic::DescribeConsumerResult description;
        auto* partition = description.add_partitions();
        partition->set_partition_id(0);
        partition->mutable_partition_stats()->mutable_partition_offsets()->set_start(0);
        partition->mutable_partition_consumer_stats()->set_committed_offset(0);
        client->Description.SetValue(TDescribeConsumerResult(
            NYdb::TStatus(NYdb::EStatus::SUCCESS, {}), std::move(description)));
        const auto credentialsFactory = std::make_shared<TCredentialsFactory>();
        const auto controlPlane = runtime.Register(CreateDqPqControlPlaneActor(
            driver, credentialsFactory, MakeIntrusive<TGateway>(client), {{"token-name", "secret-token"}}), 1);
        const auto compute = runtime.AllocateEdgeActor();
        runtime.Register(new TReader(compute, controlPlane, client));
        UNIT_ASSERT(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(compute));
        UNIT_ASSERT_VALUES_EQUAL(client->Calls.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(credentialsFactory->LastToken, "secret-token");
    }

    Y_UNIT_TEST(DisconnectFailsStartupAndLateResponseCannotResumeIt) {
        TTestActorRuntime runtime(2, true);
        InitializeRuntime(runtime);
        const auto compute = runtime.AllocateEdgeActor();
        const auto controlPlane = runtime.AllocateEdgeActor(1);
        const auto reader = runtime.Register(new TReader(compute, controlPlane, MakeIntrusive<TTopicClient>()));
        const auto request = runtime.GrabEdgeEvent<TPqControlPlaneEvents::TEvDescribeConsumer>(controlPlane);
        UNIT_ASSERT(request);
        UNIT_ASSERT(request->InterconnectSession);
        UNIT_ASSERT_VALUES_EQUAL(request->Get()->Record.GetConnection().GetTokenName(), "token-name");
        runtime.DisconnectNodes(0, 1);
        const auto error = runtime.GrabEdgeEvent<IDqComputeActorAsyncInput::TEvAsyncInputError>(compute);
        UNIT_ASSERT(error);
        UNIT_ASSERT_STRING_CONTAINS(error->Get()->Issues.ToString(), "control-plane actor disconnected");
        auto response = MakeHolder<TEvResult>();
        response->Record.SetStatus(Ydb::StatusIds::SUCCESS);
        auto* partition = response->Record.AddPartitions();
        partition->SetPartitionId(0);
        partition->SetStartOffset(0);
        partition->SetCommittedOffset(0);
        // Model a response already queued locally when the disconnect was reported.
        runtime.Send(new IEventHandle(request->Sender, controlPlane, response.Release(), 0, request->Cookie));
        runtime.Send(new IEventHandle(reader, compute, new TEvents::TEvPing()));
        const auto pong = runtime.GrabEdgeEvent<TEvents::TEvPong>(compute);
        UNIT_ASSERT(pong);
        UNIT_ASSERT_VALUES_EQUAL(pong->Cookie, 0);
    }
}

} // namespace NYql::NDq
