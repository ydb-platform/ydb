#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/federated_query/actors/pq_checkpoint_provider_integration/pq_checkpoint_provider_integration.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/testlib/pq_helpers/mock_pq_gateway.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <ydb/library/yql/providers/pq/proto/dq_task_params.pb.h>
#include <ydb/services/scheme_secret/service.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <tuple>
#include <vector>

namespace NKikimr::NKqp {

namespace {

using namespace NThreading;
using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NYql;

class TTestRuntime : public NActors::TTestActorRuntimeBase {
public:
    explicit TTestRuntime(bool useRealThreads = true)
        : NActors::TTestActorRuntimeBase(1, useRealThreads)
    {
        InitNodes();
        AppendToLogSettings(
            NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name<NActors::NLog::EComponent>);
        SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_TRACE);
        IsInitialized = true;
    }

private:
    void InitNodeImpl(TNodeDataBase* node, size_t nodeIndex) override {
        auto appData = std::make_shared<TAppData>(0, 0, 0, 0, TMap<TString, ui32>{}, nullptr, nullptr, nullptr, nullptr);
        appData->FeatureFlags.SetEnableSchemaSecrets(true);
        node->AppData0 = appData;
        TTestActorRuntimeBase::InitNodeImpl(node, nodeIndex);
    }
};

struct TSecretRequests {
    TVector<THolder<NSecret::TDescribeSchemaSecretsService::TEvResolveSecret>> Requests;
    size_t ExpectedRequests = 0;
    TPromise<void> AllStarted = NewPromise();
};

class TSecretsActor : public NActors::TActorBootstrapped<TSecretsActor> {
public:
    explicit TSecretsActor(std::shared_ptr<TSecretRequests> requests)
        : Requests(std::move(requests))
    {}

    void Bootstrap() {
        Become(&TSecretsActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NSecret::TDescribeSchemaSecretsService::TEvResolveSecret, Handle);
    )

private:
    void Handle(NSecret::TDescribeSchemaSecretsService::TEvResolveSecret::TPtr& ev) {
        Requests->Requests.push_back(ev->Get()->MakeCopy());
        if (Requests->Requests.size() == Requests->ExpectedRequests) {
            Requests->AllStarted.SetValue();
        }
    }

    const std::shared_ptr<TSecretRequests> Requests;
};

class TClient : public IDeferredPublishClient {
public:
    std::vector<TPublicationSummary> Publications;
    TVector<ui64> Canceled;
    EStatus ListStatus = EStatus::SUCCESS;
    EStatus CancelStatus = EStatus::SUCCESS;
    bool FailListFuture = false;
    bool DelayRequests = false;
    size_t ExpectedLists = 0;
    size_t ExpectedCancels = 0;
    TPromise<void> AllListsStarted = NewPromise();
    TPromise<void> AllCancelsStarted = NewPromise();
    std::map<TString, TPromise<TListPublicationsResult>> PendingLists;
    std::map<ui64, TPromise<TCancelPublicationResult>> PendingCancels;
    TVector<TString> ListedWriters;
    std::mutex Mutex;

    TAsyncBeginPublicationResult BeginPublication(const TString&, const TBeginPublicationSettings&) override {
        UNIT_FAIL("Unexpected BeginPublication");
        return {};
    }

    TAsyncPublishResult Publish(const TDeferredPublication&, const TPublishSettings&) override {
        UNIT_FAIL("Unexpected Publish");
        return {};
    }

    TAsyncListPublicationsResult ListPublications(const TListPublicationsSettings& settings) override {
        std::lock_guard guard(Mutex);
        UNIT_ASSERT(settings.WriterIdentity_);
        ListedWriters.push_back(TString(*settings.WriterIdentity_));
        if (DelayRequests) {
            auto promise = NewPromise<TListPublicationsResult>();
            PendingLists.emplace(TString(*settings.WriterIdentity_), promise);
            if (PendingLists.size() == ExpectedLists) {
                AllListsStarted.SetValue();
            }
            return promise.GetFuture();
        }
        if (FailListFuture) {
            auto promise = NewPromise<TListPublicationsResult>();
            promise.SetException("Test list publications exception");
            return promise.GetFuture();
        }
        std::vector<TPublicationSummary> result;
        for (const auto& publication : Publications) {
            if (publication.WriterIdentity == settings.WriterIdentity_) {
                result.push_back(publication);
            }
        }
        return MakeFuture(TListPublicationsResult(TStatus(ListStatus, {}), std::move(result)));
    }

    TAsyncCancelPublicationResult CancelPublication(const TDeferredPublication& publication, const TCancelPublicationSettings&) override {
        std::lock_guard guard(Mutex);
        Canceled.push_back(publication.IntPublicationId);
        if (DelayRequests) {
            auto promise = NewPromise<TCancelPublicationResult>();
            PendingCancels.emplace(publication.IntPublicationId, promise);
            if (PendingCancels.size() == ExpectedCancels) {
                AllCancelsStarted.SetValue();
            }
            return promise.GetFuture();
        }
        return MakeFuture(TCancelPublicationResult(TStatus(CancelStatus, {})));
    }
};

class TRecoveryClient {
public:
    enum class EBlockedOperation {
        None,
        DescribeConsumer,
        DescribePartition,
        CommitOffset,
        Read,
    };

    ui64 Start = 10;
    ui64 End = 100;
    ui64 Committed = 80;
    ui64 FirstWriteTimeMs = 1000;
    ui64 LastWriteTimeMs = 2000;
    std::atomic<ui64> Describes = 0;
    ui64 Reads = 0;
    ui64 PartitionCount = 1;
    bool FailCommit = false;
    bool FailRead = false;
    bool WithoutConsumer = false;
    EBlockedOperation BlockedOperation = EBlockedOperation::None;
    TPromise<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerDescription>> PendingConsumer =
        NewPromise<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerDescription>>();
    TPromise<NFq::TMessageStreamResult<NFq::TMessageStreamPartitionDescription>> PendingPartition =
        NewPromise<NFq::TMessageStreamResult<NFq::TMessageStreamPartitionDescription>>();
    TPromise<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerPosition>> PendingCommit =
        NewPromise<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerPosition>>();
    TVector<std::pair<ui64, ui64>> Commits;
    std::shared_ptr<TDriver> Driver;
    NTestUtils::IMockPqGateway::TPtr Gateway;

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamDescription>> DescribeStream(const TString&) {
        UNIT_FAIL("Unexpected describe topic");
        return {};
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerDescription>> DescribeConsumer(
        const TString&, const TString&, const NFq::TMessageStreamDescribeConsumerSettings&)
    {
        ++Describes;
        if (BlockedOperation == EBlockedOperation::DescribeConsumer) {
            return PendingConsumer.GetFuture();
        }
        NFq::TMessageStreamConsumerDescription description;
        description.Partitions.reserve(PartitionCount);
        for (ui64 id = 0; id < PartitionCount; ++id) {
            NFq::TMessageStreamConsumerPartition partition;
            partition.PartitionId = {id};
            partition.StartOffset = Start;
            partition.EndOffset = End;
            partition.CommittedOffset = Committed;
            partition.LastWriteTime = TInstant::MilliSeconds(LastWriteTimeMs);
            description.Partitions.push_back(std::move(partition));
        }
        return MakeFuture(NFq::TMessageStreamResult<NFq::TMessageStreamConsumerDescription>::Success(std::move(description)));
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamPartitionDescription>> DescribePartition(const TString&, NFq::TMessageStreamPartitionId id) {
        UNIT_ASSERT(WithoutConsumer);
        ++Describes;
        if (BlockedOperation == EBlockedOperation::DescribePartition) {
            return PendingPartition.GetFuture();
        }
        NFq::TMessageStreamPartitionDescription description;
        description.PartitionId = id;
        description.StartOffset = Start;
        description.EndOffset = End;
        return MakeFuture(NFq::TMessageStreamResult<NFq::TMessageStreamPartitionDescription>::Success(description));
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerPosition>> CommitPosition(
        const TString&, NFq::TMessageStreamPartitionId partition, const TString&, ui64 offset)
    {
        Commits.emplace_back(partition.Value, offset);
        if (BlockedOperation == EBlockedOperation::CommitOffset) {
            return PendingCommit.GetFuture();
        }
        NFq::TMessageStreamResult<NFq::TMessageStreamConsumerPosition> result;
        result.Status = FailCommit ? NFq::EMessageStreamStatus::Unauthorized : NFq::EMessageStreamStatus::Success;
        if (FailCommit) {
            result.Issues.AddIssue(NYql::TIssue("commit failed"));
        }
        result.Value = {.PartitionId = partition, .NextOffset = offset};
        return MakeFuture(std::move(result));
    }

    std::shared_ptr<NFq::IMessageStreamReadSession> CreateReadSession(const TString& stream, const NFq::TMessageStreamReadSessionSettings& settings) {
        ++Reads;
        UNIT_ASSERT_C(Start < End, "An empty partition cannot provide a retained message");
        Y_ENSURE(!FailRead, "Test read session failure");
        UNIT_ASSERT(settings.Consumer);
        UNIT_ASSERT_VALUES_EQUAL(*settings.Consumer, "consumer");
        Driver = std::make_shared<TDriver>(TDriverConfig{});
        Gateway = NTestUtils::CreateMockPqGateway();
        auto session = Gateway->GetTopicClient(stream, *Driver, {})->CreateReadSession(settings);
        if (BlockedOperation != EBlockedOperation::Read) {
            Gateway->WaitReadSession(stream)->AddDataReceivedEvent(Start, "probe", TInstant::MilliSeconds(FirstWriteTimeMs));
        }
        return session;
    }
};

// Each facade is bound to one stream; test counters/promises can be shared.
class TBoundRecoveryClient final : public NFq::IMessageStreamClient {
public:
    TBoundRecoveryClient(TString stream, std::shared_ptr<TRecoveryClient> state)
        : Stream(std::move(stream))
        , State(std::move(state))
    {}

    const TString& GetStream() const override {
        return Stream;
    }

    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamDescription>> DescribeStream() override {
        return State->DescribeStream(Stream);
    }
    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerDescription>> DescribeConsumer(
        const TString& consumer, const NFq::TMessageStreamDescribeConsumerSettings& settings) override
    {
        return State->DescribeConsumer(Stream, consumer, settings);
    }
    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamPartitionDescription>> DescribePartition(NFq::TMessageStreamPartitionId id) override {
        return State->DescribePartition(Stream, id);
    }
    std::shared_ptr<NFq::IMessageStreamReadSession> CreateReadSession(const NFq::TMessageStreamReadSessionSettings& settings) override {
        return State->CreateReadSession(Stream, settings);
    }
    NThreading::TFuture<NFq::TMessageStreamResult<NFq::TMessageStreamConsumerPosition>> CommitPosition(
        NFq::TMessageStreamPartitionId id, const TString& consumer, ui64 offset) override
    {
        return State->CommitPosition(Stream, id, consumer, offset);
    }

private:
    const TString Stream;
    const std::shared_ptr<TRecoveryClient> State;
};

class TGateway : public IPqStaticGateway {
public:
    const std::shared_ptr<TRecoveryClient> RecoveryClient = std::make_shared<TRecoveryClient>();
    const TIntrusivePtr<TClient> Client = MakeIntrusive<TClient>();
    std::atomic<size_t> CreatedClients = 0;
    std::optional<TString> ExpectedAuth;

    IDeferredPublishClient::TPtr GetDeferredPublishClient(const TDriver&, const TCommonClientSettings& settings) override {
        CheckClientSettings(settings);
        return Client;
    }

    std::shared_ptr<NFq::IMessageStreamClient> GetTopicClient(const TString& stream, const TDriver&, const TTopicClientSettings& settings) override { CheckClientSettings(settings); return std::make_shared<TBoundRecoveryClient>(stream, RecoveryClient); }
    IFederatedTopicClient::TPtr GetFederatedTopicClient(const TDriver&, const NFederatedTopic::TFederatedTopicClientSettings&) override { return {}; }
    TTopicClientSettings GetTopicClientSettings() const override { return {}; }
    NFederatedTopic::TFederatedTopicClientSettings GetFederatedTopicClientSettings() const override { return {}; }

    void CheckClientSettings(const TCommonClientSettings& settings) {
        UNIT_ASSERT_VALUES_EQUAL(*settings.Database_, "database");
        ++CreatedClients;
        const auto auth = (*settings.CredentialsProviderFactory_)->CreateProvider()->GetAuthInfo();
        if (ExpectedAuth) {
            UNIT_ASSERT_VALUES_EQUAL(auth, *ExpectedAuth);
        } else {
            const NACLib::TUserToken token(TString(auth.data(), auth.size()));
            UNIT_ASSERT_VALUES_EQUAL(token.GetUserSID(), "test-user");
            UNIT_ASSERT_VALUES_EQUAL(token.GetGroupSIDs(), (TVector<NACLib::TSID>{"test-group"}));
        }
    }
};

NYql::NDqProto::TTaskOutputSink MakeSink(bool deferredPublications = true) {
    NYql::NPq::NProto::TDqPqTopicSink settings;
    settings.SetDatabase("database");
    settings.MutableToken()->SetName("sink-token");
    if (deferredPublications) {
        settings.SetDeferredPublicationExtIdPrefix("query");
    }
    NYql::NDqProto::TTaskOutputSink sink;
    sink.SetType("PqSink");
    sink.MutableSettings()->PackFrom(settings);
    return sink;
}

NFq::ICheckpointProviderIntegration::TCleanupGraphSinkArguments MakeArgs() {
    return {
        .TaskIds = {1},
        .OutputIndex = 0,
        .SecureParams = {{"sink-token", TStructuredTokenBuilder().SetTransientTokenAuth("").ToJson()}},
        .RequestContext = {{"Database", "database"}, {"UserSID", "test-user"}, {"UserGroupSIDs", "[\"test-group\"]"}},
    };
}

NFq::ICheckpointProviderIntegration::TPrepareSource MakeSourceRecovery(std::optional<ui64> offset, ui64 timestampMs = 1500) {
    NFq::ICheckpointProviderIntegration::TPrepareSource result;
    NYql::NPq::NProto::TDqPqTopicSource settings;
    settings.SetDatabase("database");
    settings.SetTopicPath("topic");
    settings.SetConsumerName("consumer");
    settings.MutableToken()->SetName("sink-token");
    result.Source.SetType("PqSource");
    result.Source.MutableSettings()->PackFrom(settings);
    const auto auth = MakeArgs();
    result.SecureParams = auth.SecureParams;
    result.RequestContext = auth.RequestContext;
    auto& task = result.Tasks.emplace_back();
    task.TaskId = 1;
    NYql::NPq::NProto::TDqReadTaskParams params;
    auto* partitioning = params.AddPartitioningParams();
    partitioning->SetDqPartitionsCount(1);
    partitioning->SetTopicPartitionsCount(1);
    task.ReadRanges.push_back(params.SerializeAsString());
    NYql::NPq::NProto::TDqPqTopicSourceState state;
    state.AddTopics()->SetTopicPath("topic");
    state.SetStartingMessageTimestampMs(timestampMs);
    if (offset) {
        state.AddPartitions()->SetOffset(*offset);
    }
    task.State.Data.emplace_back(state.SerializeAsString(), 1);
    return result;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TPqCheckpointProviderIntegration) {
    Y_UNIT_TEST(SourceRecoveryResolvesSharedSecretsBeforeTopicOperations) {
        for (bool fail : {false, true}) {
            TTestRuntime runtime;
            auto requests = std::make_shared<TSecretRequests>();
            requests->ExpectedRequests = 1;
            runtime.RegisterService(NSecret::MakeDescribeSchemaSecretServiceId(runtime.GetNodeId(0)), runtime.Register(new TSecretsActor(requests)));
            const auto gateway = MakeIntrusive<TGateway>();
            gateway->ExpectedAuth = "resolved-token";
            auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
            auto source = MakeSourceRecovery(90);
            source.SecureParams["sink-token"] = TStructuredTokenBuilder().SetTokenAuthWithSecret("/secret", "").ToJson();
            const auto recovery = integration->PrepareSourceRecovery({source, source});
            UNIT_ASSERT(requests->AllStarted.GetFuture().Wait(TDuration::Seconds(10)));
            UNIT_ASSERT_VALUES_EQUAL(gateway->CreatedClients.load(), 0);
            auto& request = *requests->Requests.front();
            UNIT_ASSERT_VALUES_EQUAL(request.SecretNames, (TVector<TString>{"/secret"}));
            UNIT_ASSERT_VALUES_EQUAL(request.UserToken->GetUserSID(), "test-user");
            if (fail) {
                request.Promise.SetValue(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::UNAUTHORIZED, {TIssue("Test source secret failure")}));
            } else {
                request.Promise.SetValue(TEvDescribeSecretsResponse::TDescription(std::vector<TString>{"resolved-token"}));
            }
            const auto issues = recovery.GetValueSync();
            if (fail) {
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Test source secret failure");
                UNIT_ASSERT_VALUES_EQUAL(gateway->CreatedClients.load(), 0);
            } else {
                UNIT_ASSERT_C(issues.Empty(), issues.ToString());
                UNIT_ASSERT_VALUES_EQUAL(gateway->CreatedClients.load(), 2);
            }
        }
    }

    Y_UNIT_TEST(SourceRecoveryRewindsOnlyWhenNecessary) {
        for (const auto [offset, committed, expectedRewinds] : {std::tuple<ui64, ui64, ui64>{50, 80, 1}, {80, 80, 0}, {90, 80, 0}, {10, 10, 0}, {0, 80, 1}}) {
            TTestRuntime runtime;
            const auto gateway = MakeIntrusive<TGateway>();
            gateway->RecoveryClient->Committed = committed;
            if (!offset) {
                gateway->RecoveryClient->Start = 0;
            }
            auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
            const auto issues = integration->PrepareSourceRecovery({MakeSourceRecovery(offset)}).GetValueSync();
            UNIT_ASSERT_C(issues.Empty(), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(gateway->RecoveryClient->Commits.size(), expectedRewinds);
            UNIT_ASSERT_VALUES_EQUAL(gateway->RecoveryClient->Reads, expectedRewinds);
            if (expectedRewinds) {
                UNIT_ASSERT_VALUES_EQUAL(gateway->RecoveryClient->Commits.front().second, gateway->RecoveryClient->Start);
            }
        }
    }

    Y_UNIT_TEST(SourceRecoverySkipsFuturePartitionsWithoutOffsets) {
        const auto timestampMs = (TInstant::Now() + TDuration::Days(1)).MilliSeconds();
        for (const auto offset : {std::optional<ui64>{}, std::optional<ui64>{0}, std::optional<ui64>{50}}) {
            TTestRuntime runtime;
            const auto gateway = MakeIntrusive<TGateway>();
            gateway->RecoveryClient->Start = 0;
            auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
            const auto issues = integration->PrepareSourceRecovery({MakeSourceRecovery(offset, timestampMs)}).GetValueSync();
            UNIT_ASSERT_C(issues.Empty(), issues.ToString());
            UNIT_ASSERT_VALUES_EQUAL(gateway->CreatedClients.load(), offset.has_value());
            UNIT_ASSERT_VALUES_EQUAL(gateway->RecoveryClient->Describes.load(), offset.has_value());
            UNIT_ASSERT_VALUES_EQUAL(gateway->RecoveryClient->Commits.size(), offset.has_value());
            UNIT_ASSERT_VALUES_EQUAL(gateway->RecoveryClient->Reads, offset.has_value());
        }
    }

    Y_UNIT_TEST(SourceRecoveryChecksTimestampHistoryAfterRewind) {
        for (bool expired : {false, true}) {
            TTestRuntime runtime;
            const auto gateway = MakeIntrusive<TGateway>();
            gateway->RecoveryClient->FirstWriteTimeMs = expired ? 1501 : 1500;
            auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
            const auto issues = integration->PrepareSourceRecovery({MakeSourceRecovery(std::nullopt)}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(gateway->RecoveryClient->Commits.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(gateway->RecoveryClient->Reads, 1);
            if (expired) {
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Required history has expired");
            } else {
                UNIT_ASSERT_C(issues.Empty(), issues.ToString());
            }
        }
    }

    Y_UNIT_TEST(SourceRecoveryRejectsExpiredOffsetBeforeRewind) {
        TTestRuntime runtime;
        const auto gateway = MakeIntrusive<TGateway>();
        auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
        const auto issues = integration->PrepareSourceRecovery({MakeSourceRecovery(9)}).GetValueSync();
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Required checkpoint offset is unavailable");
        UNIT_ASSERT(gateway->RecoveryClient->Commits.empty());
    }

    Y_UNIT_TEST(SourceRecoveryReportsCommitAndReadFailures) {
        for (bool failCommit : {false, true}) {
            TTestRuntime runtime;
            const auto gateway = MakeIntrusive<TGateway>();
            gateway->RecoveryClient->FailCommit = failCommit;
            gateway->RecoveryClient->FailRead = !failCommit;
            auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
            const auto issues = integration->PrepareSourceRecovery({MakeSourceRecovery(50)}).GetValueSync();
            UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), failCommit ? "Cannot rewind consumer" : "Test read session failure");
        }
    }

    Y_UNIT_TEST(SourceRecoveryDescribesConsumerOnceForStageTasks) {
        TTestRuntime runtime;
        const auto gateway = MakeIntrusive<TGateway>();
        gateway->RecoveryClient->PartitionCount = 2;
        gateway->RecoveryClient->Committed = 50;
        auto source = MakeSourceRecovery(50);
        source.Tasks.push_back(source.Tasks.front());
        for (ui32 id = 0; id < 2; ++id) {
            NYql::NPq::NProto::TDqReadTaskParams params;
            auto* partitioning = params.AddPartitioningParams();
            partitioning->SetEachTopicPartitionGroupId(id);
            partitioning->SetDqPartitionsCount(2);
            partitioning->SetTopicPartitionsCount(2);
            source.Tasks[id].TaskId = id + 1;
            source.Tasks[id].ReadRanges = {params.SerializeAsString()};
            NYql::NPq::NProto::TDqPqTopicSourceState state;
            UNIT_ASSERT(state.ParseFromString(source.Tasks[id].State.Data.front().Blob));
            state.MutablePartitions(0)->SetPartition(id);
            source.Tasks[id].State.Data.front().Blob = state.SerializeAsString();
        }
        auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
        const auto issues = integration->PrepareSourceRecovery({std::move(source)}).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(gateway->RecoveryClient->Describes.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(gateway->CreatedClients.load(), 1);
    }

    Y_UNIT_TEST(SourceRecoveryEmptyRetainedPartitionDoesNotWaitForData) {
        TTestRuntime runtime;
        const auto gateway = MakeIntrusive<TGateway>();
        gateway->RecoveryClient->Start = gateway->RecoveryClient->End;
        gateway->RecoveryClient->Committed = gateway->RecoveryClient->End;
        auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
        const auto issues = integration->PrepareSourceRecovery({MakeSourceRecovery(100)}).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(gateway->RecoveryClient->Reads, 0);
    }

    Y_UNIT_TEST(SourceRecoveryChecksEmptyTimestampRangeWithoutWaitingForData) {
        for (const bool withoutConsumer : {false, true}) {
            // Last-write metadata cannot replace validation against retained history.
            for (const ui64 lastWriteTimeMs : {0, 10999, 11499, 11500, 11501, 12000}) {
                TTestRuntime runtime;
                const auto gateway = MakeIntrusive<TGateway>();
                gateway->RecoveryClient->Start = gateway->RecoveryClient->End;
                gateway->RecoveryClient->Committed = gateway->RecoveryClient->End;
                gateway->RecoveryClient->LastWriteTimeMs = lastWriteTimeMs;
                gateway->RecoveryClient->WithoutConsumer = withoutConsumer;
                auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
                auto source = MakeSourceRecovery(std::nullopt, 11500);
                if (withoutConsumer) {
                    NYql::NPq::NProto::TDqPqTopicSource settings;
                    UNIT_ASSERT(source.Source.GetSettings().UnpackTo(&settings));
                    settings.ClearConsumerName();
                    source.Source.MutableSettings()->PackFrom(settings);
                }
                const auto issues = integration->PrepareSourceRecovery({std::move(source)}).GetValue(TDuration::Seconds(10));
                UNIT_ASSERT(gateway->RecoveryClient->Commits.empty());
                UNIT_ASSERT_VALUES_EQUAL(gateway->RecoveryClient->Reads, 0);
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Required history has expired");
            }
        }
    }

    Y_UNIT_TEST(SourceRecoveryAcceptsNeverWrittenPartition) {
        TTestRuntime runtime;
        const auto gateway = MakeIntrusive<TGateway>();
        gateway->RecoveryClient->Start = 0;
        gateway->RecoveryClient->End = 0;
        gateway->RecoveryClient->Committed = 0;
        gateway->RecoveryClient->LastWriteTimeMs = 0;
        auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
        const auto issues = integration->PrepareSourceRecovery({MakeSourceRecovery(std::nullopt, 0)}).GetValue(TDuration::Seconds(10));
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT(gateway->RecoveryClient->Commits.empty());
        UNIT_ASSERT_VALUES_EQUAL(gateway->RecoveryClient->Reads, 0);
    }

    Y_UNIT_TEST(SourceRecoveryTimesOutStalledOperations) {
        using EOperation = TRecoveryClient::EBlockedOperation;
        for (const auto operation : {EOperation::DescribeConsumer, EOperation::DescribePartition, EOperation::CommitOffset, EOperation::Read}) {
            TTestRuntime runtime(/* useRealThreads */ false);
            runtime.SetScheduledEventFilter([](auto&, auto&, TDuration, TInstant&) { return false; });
            const auto gateway = MakeIntrusive<TGateway>();
            auto& client = *gateway->RecoveryClient;
            client.BlockedOperation = operation;
            client.WithoutConsumer = operation == EOperation::DescribePartition;
            auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
            auto source = MakeSourceRecovery(std::nullopt);
            if (client.WithoutConsumer) {
                NYql::NPq::NProto::TDqPqTopicSource settings;
                UNIT_ASSERT(source.Source.GetSettings().UnpackTo(&settings));
                settings.ClearConsumerName();
                source.Source.MutableSettings()->PackFrom(settings);
            }

            const auto started = runtime.GetCurrentTime();
            const auto recovery = integration->PrepareSourceRecovery({std::move(source)});
            runtime.WaitFor("source recovery timeout", [&] { return recovery.HasValue(); }, TDuration::Seconds(31));
            UNIT_ASSERT_VALUES_EQUAL(runtime.GetCurrentTime() - started, TDuration::Seconds(30));
            UNIT_ASSERT_STRING_CONTAINS(recovery.GetValue().ToString(), "Cannot prepare topic source recovery for topic: timed out after ");
            UNIT_ASSERT_VALUES_EQUAL(client.Describes.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(client.Commits.size(), operation == EOperation::CommitOffset || operation == EOperation::Read);
            UNIT_ASSERT_VALUES_EQUAL(client.Reads, operation == EOperation::Read);
            if (operation == EOperation::Read) {
                client.Gateway->WaitReadSession("topic")->ExpectSessionClosed(TDuration::Seconds(1));
            }
        }
    }

    Y_UNIT_TEST(CancelsAllGenerationsOfSinkWriter) {
        TTestRuntime runtime;
        const auto gateway = MakeIntrusive<TGateway>();
        ui64 id = 0;
        for (const auto& prefix : {"query:1:0:3", "query:1:0:3", "query:1:1:3", "query:2:0:3", "query:2:1:3",
                                  "query:1:0:4", "query:1:0:2", "other-query:1:0:3", "query:3:0:3", "query:1:0:30"}) {
            TPublicationSummary publication;
            publication.IntPublicationId = ++id;
            publication.ExtPublicationId = TStringBuilder() << prefix << ':' << id;
            const auto writer = TStringBuf(prefix).RBefore(':');
            publication.WriterIdentity = std::string(writer.data(), writer.size());
            gateway->Client->Publications.push_back(std::move(publication));
        }
        TPublicationSummary legacyPublication;
        legacyPublication.IntPublicationId = ++id;
        legacyPublication.WriterIdentity = "query:1:0";
        legacyPublication.ExtPublicationId = "legacy-publication";
        gateway->Client->Publications.push_back(std::move(legacyPublication));
        auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
        const auto issues = integration->CleanupGraphSinks({{MakeSink(), MakeArgs()}}, std::nullopt).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        Sort(gateway->Client->Canceled);
        UNIT_ASSERT_VALUES_EQUAL(gateway->Client->Canceled, (TVector<ui64>{1, 2, 6, 7, 10, 11}));
    }

    Y_UNIT_TEST(PropagatesListingFailure) {
        TTestRuntime runtime;
        const auto gateway = MakeIntrusive<TGateway>();
        gateway->Client->ListStatus = EStatus::UNAUTHORIZED;
        auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
        UNIT_ASSERT(integration->CleanupGraphSinks({{MakeSink(), MakeArgs()}}, std::nullopt).GetValueSync());
        UNIT_ASSERT(gateway->Client->Canceled.empty());
    }

    Y_UNIT_TEST(CancellationCanBeRetriedAndIgnoresMissingPublications) {
        TTestRuntime runtime;
        const auto gateway = MakeIntrusive<TGateway>();
        TPublicationSummary publication;
        publication.IntPublicationId = 1;
        publication.WriterIdentity = "query:1:0";
        publication.ExtPublicationId = "query:1:0:3:0";
        gateway->Client->Publications.push_back(publication);
        gateway->Client->CancelStatus = EStatus::UNAVAILABLE;
        auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
        UNIT_ASSERT(integration->CleanupGraphSinks({{MakeSink(), MakeArgs()}}, std::nullopt).GetValueSync());
        gateway->Client->CancelStatus = EStatus::NOT_FOUND;
        const auto issues = integration->CleanupGraphSinks({{MakeSink(), MakeArgs()}}, std::nullopt).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(gateway->Client->Canceled, (TVector<ui64>{1, 1}));
    }

    Y_UNIT_TEST(PropagatesFutureExceptions) {
        TTestRuntime runtime;
        const auto gateway = MakeIntrusive<TGateway>();
        gateway->Client->FailListFuture = true;
        auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
        const auto issues = integration->CleanupGraphSinks({{MakeSink(), MakeArgs()}}, std::nullopt).GetValueSync();
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Test list publications exception");
        UNIT_ASSERT(gateway->Client->Canceled.empty());
    }

    Y_UNIT_TEST(GcPreservesBoundaryAndFutureGenerations) {
        TTestRuntime runtime;
        const auto gateway = MakeIntrusive<TGateway>();
        ui64 id = 0;
        for (const auto& suffix : {"0:10", "1:99", "2:1000", "3:0", "3:999", "4:0", "30:0",
                                  "invalid:0", "2:invalid", "-1:0", "2:0:extra"}) {
            TPublicationSummary publication;
            publication.IntPublicationId = ++id;
            publication.WriterIdentity = "query:1:0";
            publication.ExtPublicationId = TStringBuilder() << "query:1:0:" << suffix;
            gateway->Client->Publications.push_back(std::move(publication));
        }
        auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
        const auto issues = integration->CleanupGraphSinks({{MakeSink(), MakeArgs()}}, 3).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(gateway->Client->Canceled, (TVector<ui64>{1, 2, 3}));
    }

    Y_UNIT_TEST(ZeroGenerationBoundDoesNotDeletePublications) {
        TTestRuntime runtime;
        const auto gateway = MakeIntrusive<TGateway>();
        TPublicationSummary publication;
        publication.IntPublicationId = 1;
        publication.WriterIdentity = "query:1:0";
        publication.ExtPublicationId = "query:1:0:0:0";
        gateway->Client->Publications.push_back(std::move(publication));
        auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
        const auto issues = integration->CleanupGraphSinks({{MakeSink(), MakeArgs()}}, 0).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT(gateway->Client->Canceled.empty());
    }

    Y_UNIT_TEST(IgnoresSinksWithoutDeferredPublications) {
        TTestRuntime runtime;
        const auto gateway = MakeIntrusive<TGateway>();
        gateway->Client->ListStatus = EStatus::UNAUTHORIZED;
        auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
        const auto issues = integration->CleanupGraphSinks({{MakeSink(false), {}}}, std::nullopt).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT(gateway->Client->Canceled.empty());
    }

    Y_UNIT_TEST(RejectsInvalidSinkSettings) {
        TTestRuntime runtime;
        const auto gateway = MakeIntrusive<TGateway>();
        auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
        auto sink = MakeSink();
        sink.ClearSettings();
        const auto issues = integration->CleanupGraphSinks({{sink, MakeArgs()}}, std::nullopt).GetValueSync();
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Failed to unpack PQ sink settings");
        UNIT_ASSERT(gateway->Client->Canceled.empty());
    }

    Y_UNIT_TEST(UsesTaskAndOutputIdentity) {
        TTestRuntime runtime;
        const auto gateway = MakeIntrusive<TGateway>();
        TPublicationSummary publication;
        publication.IntPublicationId = 1;
        publication.WriterIdentity = "query:7:3";
        publication.ExtPublicationId = "query:7:3:1:0";
        gateway->Client->Publications.push_back(std::move(publication));
        auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
        auto args = MakeArgs();
        args.TaskIds = {7};
        args.OutputIndex = 3;
        const auto issues = integration->CleanupGraphSinks({{MakeSink(), std::move(args)}}, std::nullopt).GetValueSync();
        UNIT_ASSERT_C(issues.Empty(), issues.ToString());
        UNIT_ASSERT_VALUES_EQUAL(gateway->Client->Canceled, (TVector<ui64>{1}));
    }

    Y_UNIT_TEST(RejectsMissingAuthReferences) {
        TTestRuntime runtime;
        const auto gateway = MakeIntrusive<TGateway>();
        auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
        auto args = MakeArgs();
        args.SecureParams.clear();
        const auto issues = integration->CleanupGraphSinks({{MakeSink(), std::move(args)}}, std::nullopt).GetValueSync();
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Missing auth references");
        UNIT_ASSERT(gateway->Client->Canceled.empty());
    }

    Y_UNIT_TEST(RejectsMissingAuthorizationContext) {
        TTestRuntime runtime;
        const auto gateway = MakeIntrusive<TGateway>();
        auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
        auto args = MakeArgs();
        args.RequestContext.clear();
        const auto issues = integration->CleanupGraphSinks({{MakeSink(), std::move(args)}}, std::nullopt).GetValueSync();
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Missing authorization context");
        UNIT_ASSERT(gateway->Client->Canceled.empty());
    }

    Y_UNIT_TEST(ReusesStageOutputClientsAndWaitsForParallelWriters) {
        for (bool fail : {false, true}) {
            TTestRuntime runtime;
            const auto gateway = MakeIntrusive<TGateway>();
            auto& client = *gateway->Client;
            client.DelayRequests = true;
            client.ExpectedLists = 4;
            client.ExpectedCancels = fail ? 3 : 4;
            auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
            TVector<NFq::ICheckpointProviderIntegration::TCleanupGraphSink> sinks;
            for (ui64 outputIndex : {0, 1}) {
                auto args = MakeArgs();
                args.TaskIds = {1, 2};
                args.OutputIndex = outputIndex;
                sinks.push_back({MakeSink(), std::move(args)});
            }
            auto cleanup = integration->CleanupGraphSinks(std::move(sinks), std::nullopt);
            UNIT_ASSERT(client.AllListsStarted.GetFuture().Wait(TDuration::Seconds(10)));
            UNIT_ASSERT_VALUES_EQUAL(gateway->CreatedClients.load(), 2);
            UNIT_ASSERT(!cleanup.HasValue() && !cleanup.HasException());
            ui64 publicationId = 0;
            for (auto& [writer, promise] : client.PendingLists) {
                ++publicationId;
                if (fail && publicationId == 1) {
                    promise.SetException("Test parallel listing failure");
                    continue;
                }
                TPublicationSummary publication;
                publication.IntPublicationId = publicationId;
                publication.WriterIdentity = std::string(writer.data(), writer.size());
                publication.ExtPublicationId = TStringBuilder() << writer << ":3:0";
                promise.SetValue(TListPublicationsResult(TStatus(EStatus::SUCCESS, {}), {publication}));
            }
            UNIT_ASSERT(client.AllCancelsStarted.GetFuture().Wait(TDuration::Seconds(10)));
            size_t completed = 0;
            for (auto& [id, promise] : client.PendingCancels) {
                if (++completed == client.PendingCancels.size()) {
                    break;
                }
                if (fail && completed == 1) {
                    promise.SetException("Test parallel cancellation failure");
                } else {
                    promise.SetValue(TCancelPublicationResult(TStatus(EStatus::SUCCESS, {})));
                }
            }
            UNIT_ASSERT_C(!cleanup.Wait(TDuration::MilliSeconds(100)), "Every writer must finish before the batch completes");
            client.PendingCancels.rbegin()->second.SetValue(TCancelPublicationResult(TStatus(EStatus::SUCCESS, {})));
            const auto issues = cleanup.GetValueSync();
            if (fail) {
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Test parallel listing failure");
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Test parallel cancellation failure");
            } else {
                UNIT_ASSERT_C(issues.Empty(), issues.ToString());
            }
        }
    }

    Y_UNIT_TEST(ResolvesUniqueSecretsBeforeCreatingClients) {
        for (int scenario : {0, 1, 2}) {
            TTestRuntime runtime;
            auto requests = std::make_shared<TSecretRequests>();
            requests->ExpectedRequests = 1;
            runtime.RegisterService(NSecret::MakeDescribeSchemaSecretServiceId(runtime.GetNodeId(0)), runtime.Register(new TSecretsActor(requests)));
            const auto gateway = MakeIntrusive<TGateway>();
            gateway->ExpectedAuth = "resolved-token";
            auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
            TVector<NFq::ICheckpointProviderIntegration::TCleanupGraphSink> sinks;
            for (ui64 outputIndex : {0, 1, 2}) {
                auto args = MakeArgs();
                args.TaskIds = {1, 2};
                args.OutputIndex = outputIndex;
                args.SecureParams["sink-token"] = TStructuredTokenBuilder()
                    .SetTokenAuthWithSecret(outputIndex == 2 ? "/secret-b" : "/secret-a", "").ToJson();
                sinks.push_back({MakeSink(), std::move(args)});
            }
            // Called without actor TLS, as checkpoint storage does in its SDK continuations.
            auto cleanup = integration->CleanupGraphSinks(std::move(sinks), std::nullopt);
            UNIT_ASSERT(requests->AllStarted.GetFuture().Wait(TDuration::Seconds(10)));
            UNIT_ASSERT_VALUES_EQUAL(gateway->CreatedClients.load(), 0);
            UNIT_ASSERT_VALUES_EQUAL(requests->Requests.size(), 1);
            auto& request = *requests->Requests.front();
            auto names = request.SecretNames;
            Sort(names);
            UNIT_ASSERT_VALUES_EQUAL(names, (TVector<TString>{"/secret-a", "/secret-b"}));
            UNIT_ASSERT_VALUES_EQUAL(request.Database, "database");
            UNIT_ASSERT_VALUES_EQUAL(request.UserToken->GetUserSID(), "test-user");
            UNIT_ASSERT_VALUES_EQUAL(request.UserToken->GetGroupSIDs(), (TVector<NACLib::TSID>{"test-group"}));
            UNIT_ASSERT(!cleanup.Wait(TDuration::MilliSeconds(100)));
            if (scenario == 1) {
                request.Promise.SetValue(TEvDescribeSecretsResponse::TDescription(Ydb::StatusIds::UNAUTHORIZED, {TIssue("Test secret resolution failure")}));
            } else if (scenario == 2) {
                request.Promise.SetException("Test secret resolution failure");
            } else {
                request.Promise.SetValue(TEvDescribeSecretsResponse::TDescription(std::vector<TString>(request.SecretNames.size(), "resolved-token")));
            }
            const auto issues = cleanup.GetValueSync();
            if (scenario != 0) {
                UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Test secret resolution failure");
                UNIT_ASSERT_VALUES_EQUAL(gateway->CreatedClients.load(), 0);
                UNIT_ASSERT(gateway->Client->ListedWriters.empty());
            } else {
                UNIT_ASSERT_C(issues.Empty(), issues.ToString());
                UNIT_ASSERT_VALUES_EQUAL(gateway->CreatedClients.load(), 3);
                UNIT_ASSERT_VALUES_EQUAL(gateway->Client->ListedWriters.size(), 6);
            }
        }
    }

    Y_UNIT_TEST(RejectsInconsistentAuthorizationContexts) {
        const std::map<TString, TString> changes = {
            {"Database", "other-database"},
            {"UserSID", "other-user"},
            {"UserGroupSIDs", "[\"other-group\"]"},
        };
        for (const auto& [key, value] : changes) {
            for (bool useSecrets : {false, true}) {
                TTestRuntime runtime;
                const auto gateway = MakeIntrusive<TGateway>();
                auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
                auto first = MakeArgs();
                if (useSecrets) {
                    first.SecureParams["sink-token"] = TStructuredTokenBuilder().SetTokenAuthWithSecret("/secret", "").ToJson();
                }
                auto second = first;
                second.OutputIndex = 1;
                second.RequestContext[key] = value;
                auto cleanup = integration->CleanupGraphSinks({{MakeSink(), std::move(first)}, {MakeSink(), std::move(second)}}, std::nullopt);
                UNIT_ASSERT(cleanup.Wait(TDuration::Seconds(10)));
                UNIT_ASSERT_STRING_CONTAINS(cleanup.GetValue().ToString(), "Inconsistent authorization context");
                UNIT_ASSERT_VALUES_EQUAL(gateway->CreatedClients.load(), 0);
                UNIT_ASSERT(gateway->Client->ListedWriters.empty());
            }
        }
    }

    Y_UNIT_TEST(PropagatesSynchronousSecretResolutionFailure) {
        TTestRuntime runtime;
        const auto gateway = MakeIntrusive<TGateway>();
        auto integration = CreatePqCheckpointProviderIntegration(runtime.GetActorSystem(0), gateway, TDriver(TDriverConfig{}), CreateStructuredTokenCredentialsFactory());
        auto args = MakeArgs();
        args.RequestContext["Database"].clear();
        args.SecureParams["sink-token"] = TStructuredTokenBuilder().SetTokenAuthWithSecret("/secret", "").ToJson();
        const auto cleanup = integration->CleanupGraphSinks({{MakeSink(), std::move(args)}}, std::nullopt);
        UNIT_ASSERT(cleanup.HasValue());
        const auto& issues = cleanup.GetValue();
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "Database name must be set in secret requests");
        UNIT_ASSERT_VALUES_EQUAL(gateway->CreatedClients.load(), 0);
        UNIT_ASSERT(gateway->Client->ListedWriters.empty());
    }
}

} // namespace NKikimr::NKqp
