#include <ydb/library/yql/providers/ydb_external/common/provider_names.h>
#include <ydb/library/yql/providers/ydb_external/provider/yql_ydb_external_provider_impl.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/completion_queue.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/async_unary_call.h>

#include <chrono>
#include <thread>

namespace NYql::NYdbExternal {
namespace {

constexpr TDuration WaitTimeout = TDuration::Seconds(10);

struct TTag {
    bool Complete = false;
    bool Ok = false;
};

template <typename TRequest, typename TResponse>
struct TUnaryCall {
    grpc::ServerContext Context;
    TRequest Request;
    grpc::ServerAsyncResponseWriter<TResponse> Writer{&Context};
    TTag Accepted;
    TTag Finished;
    TTag Done;
};

class TMetadataServer {
public:
    TMetadataServer() {
        NTesting::InitPortManagerFromEnv();
        Endpoint = TStringBuilder() << "127.0.0.1:" << NTesting::GetFreePort();
        grpc::ServerBuilder builder;
        builder.AddListeningPort(Endpoint, grpc::InsecureServerCredentials());
        builder.RegisterService(&Service_);
        Queue_ = builder.AddCompletionQueue();
        Server_ = builder.BuildAndStart();
        UNIT_ASSERT(Server_);
        Create.Context.AsyncNotifyWhenDone(&Create.Done);
        Describe.Context.AsyncNotifyWhenDone(&Describe.Done);
        Delete.Context.AsyncNotifyWhenDone(&Delete.Done);
        Service_.RequestCreateSession(&Create.Context, &Create.Request, &Create.Writer,
            Queue_.get(), Queue_.get(), &Create.Accepted);
        Service_.RequestDescribeTable(&Describe.Context, &Describe.Request, &Describe.Writer,
            Queue_.get(), Queue_.get(), &Describe.Accepted);
        Service_.RequestDeleteSession(&Delete.Context, &Delete.Request, &Delete.Writer,
            Queue_.get(), Queue_.get(), &Delete.Accepted);
    }

    ~TMetadataServer() {
        Server_->Shutdown(std::chrono::system_clock::now());
        Queue_->Shutdown();
        void* tag = nullptr;
        bool ok = false;
        while (Queue_->Next(&tag, &ok)) {
        }
    }

    bool WaitFor(TTag& expected, TDuration timeout = WaitTimeout, bool required = true) {
        const auto deadline = std::chrono::system_clock::now() + std::chrono::microseconds(timeout.MicroSeconds());
        while (!expected.Complete) {
            void* tag = nullptr;
            bool ok = false;
            if (Queue_->AsyncNext(&tag, &ok, deadline) != grpc::CompletionQueue::GOT_EVENT) {
                UNIT_ASSERT(!required);
                return false;
            }
            auto& received = *static_cast<TTag*>(tag);
            UNIT_ASSERT(!received.Complete);
            received.Complete = true;
            received.Ok = ok;
        }
        return true;
    }

    void ReturnSession() {
        WaitFor(Create.Accepted);
        UNIT_ASSERT(Create.Accepted.Ok);
        Ydb::Table::CreateSessionResult result;
        result.set_session_id("native-metadata-session");
        Ydb::Table::CreateSessionResponse response;
        response.mutable_operation()->set_ready(true);
        response.mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
        response.mutable_operation()->mutable_result()->PackFrom(result);
        Create.Writer.Finish(response, grpc::Status::OK, &Create.Finished);
        WaitFor(Create.Finished);
        UNIT_ASSERT(Create.Finished.Ok);
    }

    void ReturnSchema(Ydb::Type::PrimitiveTypeId type = Ydb::Type::UINT64) {
        WaitFor(Describe.Accepted);
        Ydb::Table::DescribeTableResult description;
        auto* column = description.add_columns();
        column->set_name("value");
        column->mutable_type()->set_type_id(type);
        Ydb::Table::DescribeTableResponse response;
        response.mutable_operation()->set_ready(true);
        response.mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
        response.mutable_operation()->mutable_result()->PackFrom(description);
        Describe.Writer.Finish(response, grpc::Status::OK, &Describe.Finished);
        WaitFor(Describe.Finished);
        UNIT_ASSERT(Describe.Finished.Ok);
    }

    void RejectDelete() {
        WaitFor(Delete.Accepted);
        UNIT_ASSERT(Delete.Accepted.Ok);
        Ydb::Table::DeleteSessionResponse response;
        response.mutable_operation()->set_ready(true);
        response.mutable_operation()->set_status(Ydb::StatusIds::UNAVAILABLE);
        Delete.Writer.Finish(response, grpc::Status::OK, &Delete.Finished);
        WaitFor(Delete.Finished);
        UNIT_ASSERT(Delete.Finished.Ok);
    }

    void CheckNoSecondDelete() {
        Service_.RequestDeleteSession(&SecondDelete.Context, &SecondDelete.Request, &SecondDelete.Writer,
            Queue_.get(), Queue_.get(), &SecondDelete.Accepted);
        UNIT_ASSERT(!WaitFor(SecondDelete.Accepted, TDuration::MilliSeconds(100), false));
    }

    TString Endpoint;
    TUnaryCall<Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse> Create;
    TUnaryCall<Ydb::Table::DescribeTableRequest, Ydb::Table::DescribeTableResponse> Describe;
    TUnaryCall<Ydb::Table::DeleteSessionRequest, Ydb::Table::DeleteSessionResponse> Delete;
    TUnaryCall<Ydb::Table::DeleteSessionRequest, Ydb::Table::DeleteSessionResponse> SecondDelete;

private:
    Ydb::Table::V1::TableService::AsyncService Service_;
    std::unique_ptr<grpc::ServerCompletionQueue> Queue_;
    std::unique_ptr<grpc::Server> Server_;
};

TExprNode::TPtr MakeRead(TExprContext& ctx) {
    const auto pos = ctx.AppendPosition({});
    return ctx.NewCallable(pos, "Read!", {
        ctx.NewWorld(pos), ctx.NewCallable(pos, "DataSource", {
            ctx.NewAtom(pos, YdbExternalProviderName), ctx.NewAtom(pos, "remote")}),
        ctx.NewCallable(pos, "Key", {ctx.NewList(pos, {ctx.NewAtom(pos, "table"),
            ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "items")})})}),
        ctx.NewCallable(pos, "Void", {}), ctx.NewList(pos, {})});
}

void CheckMetadataCleanup(bool rejectBeforeCompletion, bool unsupportedType = false) {
    TMetadataServer server;
    NYdb::TDriver driver(NYdb::TDriverConfig().SetDiscoveryMode(NYdb::EDiscoveryMode::Off)
        .SetNetworkThreadsNum(1).SetClientThreadsNum(1));
    auto types = MakeIntrusive<TTypeAnnotationContext>();
    auto state = MakeIntrusive<TState>(types.Get(),
        [cache = CreateYdbExternalMetadataClientCache(driver, driver)] { return cache; },
        CreateStructuredTokenCredentialsFactory());
    AddCluster(*state, "remote", {{"location", server.Endpoint}, {"database_name", "/Remote"},
        {"authMethod", "NONE"}, {"use_tls", "false"}});
    TExprContext ctx;
    auto input = MakeRead(ctx);
    auto transformer = CreateLoadMetadataTransformer(state);
    TExprNode::TPtr output;
    UNIT_ASSERT_VALUES_EQUAL(transformer->Transform(input, output, ctx).Level, IGraphTransformer::TStatus::Async);
    auto future = transformer->GetAsyncFuture(*input);
    server.ReturnSession();
    server.ReturnSchema(unsupportedType ? Ydb::Type::TIMESTAMP : Ydb::Type::UINT64);
    server.WaitFor(server.Delete.Accepted);
    if (rejectBeforeCompletion) {
        server.RejectDelete();
    }
    // A server that has accepted DeleteSession but never replies must not hold
    // metadata completion hostage, nor turn successful DescribeTable into error.
    UNIT_ASSERT(future.Wait(TDuration::Seconds(1)));
    const auto status = transformer->ApplyAsyncChanges(input, output, ctx);
    if (unsupportedType) {
        UNIT_ASSERT_VALUES_EQUAL(status.Level, IGraphTransformer::TStatus::Error);
        const auto issues = ctx.IssueManager.GetIssues().ToString();
        UNIT_ASSERT_C(issues.Contains("column 'value'") && issues.Contains("TIMESTAMP"), issues);
        UNIT_ASSERT(state->Tables.empty());
    } else {
        UNIT_ASSERT_C(status.Level == IGraphTransformer::TStatus::Ok || status.Level == IGraphTransformer::TStatus::Repeat,
            ctx.IssueManager.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(state->Tables.size(), 1);
    }
    if (!rejectBeforeCompletion) {
        server.RejectDelete();
    }
    server.CheckNoSecondDelete();
    driver.Stop(true);
}

void CheckMetadataCancellation(bool cancelDescribe, bool expireDeadline = false) {
    TMetadataServer server;
    NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(server.Endpoint)
        .SetDiscoveryMode(NYdb::EDiscoveryMode::Off).SetDatabase("/Remote")
        .SetNetworkThreadsNum(1).SetClientThreadsNum(1));
    NYdb::TDriver tlsDriver(NYdb::TDriverConfig().SetEndpoint(server.Endpoint)
        .SetDiscoveryMode(NYdb::EDiscoveryMode::Off).SetDatabase("/Remote")
        .SetNetworkThreadsNum(1).SetClientThreadsNum(1));
    auto types = MakeIntrusive<TTypeAnnotationContext>();
    auto state = MakeIntrusive<TState>(types.Get(),
        [cache = CreateYdbExternalMetadataClientCache(driver, tlsDriver)] { return cache; },
        CreateStructuredTokenCredentialsFactory(),
        TInstant::Now() + TDuration::Seconds(expireDeadline ? 5 : 30));
    AddCluster(*state, "remote", {{"location", server.Endpoint}, {"database_name", "/Remote"},
        {"authMethod", "NONE"}, {"use_tls", "false"}});
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});
    auto input = ctx.NewCallable(pos, "Read!", {
        ctx.NewWorld(pos), ctx.NewCallable(pos, "DataSource", {
            ctx.NewAtom(pos, YdbExternalProviderName), ctx.NewAtom(pos, "remote")}),
        ctx.NewCallable(pos, "Key", {ctx.NewList(pos, {ctx.NewAtom(pos, "table"),
            ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "items")})})}),
        ctx.NewCallable(pos, "Void", {}), ctx.NewList(pos, {})});
    auto transformer = CreateLoadMetadataTransformer(state);
    TExprNode::TPtr output;
    UNIT_ASSERT_VALUES_EQUAL(transformer->Transform(input, output, ctx).Level, IGraphTransformer::TStatus::Async);
    auto future = transformer->GetAsyncFuture(*input);
    server.WaitFor(server.Create.Accepted);
    UNIT_ASSERT(server.Create.Accepted.Ok);
    if (cancelDescribe) {
        if (expireDeadline) {
            // Make a fresh phase timeout measurably different from the original.
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
        }
        server.ReturnSession();
        server.WaitFor(server.Describe.Accepted);
        UNIT_ASSERT(server.Describe.Accepted.Ok);
        // Advancing to Describe must not grant a fresh metadata timeout.
        UNIT_ASSERT(server.Describe.Context.deadline() <=
            server.Create.Context.deadline() + std::chrono::milliseconds(100));
    }
    UNIT_ASSERT(!future.HasValue());
    if (!expireDeadline) {
        transformer->Rewind();
        UNIT_ASSERT(!future.HasValue());
        transformer.Reset();
        types.Reset();
        // The callback must remain safe after the transformer and type context
        // disappear. The existing SDK cannot cancel the outstanding unary RPC;
        // its late response must not populate the schema or start the next phase.
        if (cancelDescribe) {
            server.ReturnSchema();
        } else {
            server.ReturnSession();
        }
    } else {
        auto& done = cancelDescribe ? server.Describe.Done : server.Create.Done;
        auto& context = cancelDescribe ? server.Describe.Context : server.Create.Context;
        server.WaitFor(done);
        UNIT_ASSERT(context.IsCancelled());
    }
    UNIT_ASSERT(future.Wait(WaitTimeout));
    if (expireDeadline) {
        UNIT_ASSERT_VALUES_EQUAL(transformer->ApplyAsyncChanges(input, output, ctx).Level, IGraphTransformer::TStatus::Error);
        UNIT_ASSERT(ctx.IssueManager.GetIssues().ToString().Contains("deadline exceeded"));
    }
    if (!expireDeadline && !cancelDescribe) {
        UNIT_ASSERT(!server.WaitFor(server.Describe.Accepted, TDuration::MilliSeconds(100), false));
    }
    UNIT_ASSERT(state->Tables.empty());
    driver.Stop(true);
    tlsDriver.Stop(true);
}

// The public credential facility identifies the SDK database-state lifetime.
// It lets us check eviction without exposing the SDK's private callback vector.
class TTrackingCredentialsFactory final : public IStructuredTokenCredentialsFactory {
public:
    class TFactory final : public NYdb::ICredentialsProviderFactory {
    public:
        TFactory(std::shared_ptr<NYdb::ICredentialsProviderFactory> inner,
                 TVector<std::weak_ptr<NYdb::ICoreFacility>>& facilities)
            : Inner_(std::move(inner))
            , Facilities_(facilities)
        {
        }

        NYdb::TCredentialsProviderPtr CreateProvider() const override {
            return Inner_->CreateProvider();
        }

        NYdb::TCredentialsProviderPtr CreateProvider(std::weak_ptr<NYdb::ICoreFacility> facility) const override {
            Facilities_.push_back(facility);
            return Inner_->CreateProvider(std::move(facility));
        }

        std::string GetClientIdentity() const override {
            return Inner_->GetClientIdentity();
        }

    private:
        const std::shared_ptr<NYdb::ICredentialsProviderFactory> Inner_;
        TVector<std::weak_ptr<NYdb::ICoreFacility>>& Facilities_;
    };

    std::shared_ptr<NYdb::ICredentialsProviderFactory> Create(const TString& token, bool addBearer) override {
        return std::make_shared<TFactory>(Inner_->Create(token, addBearer), Facilities);
    }

    TVector<std::weak_ptr<NYdb::ICoreFacility>> Facilities;

private:
    const IStructuredTokenCredentialsFactory::TPtr Inner_ = CreateStructuredTokenCredentialsFactory();
};

} // namespace

Y_UNIT_TEST_SUITE(TYdbExternalMetadataRpc) {
    Y_UNIT_TEST(SlowSessionCleanupDoesNotDelaySuccessfulMetadata) {
        CheckMetadataCleanup(false);
    }

    Y_UNIT_TEST(FailedSessionCleanupDoesNotFailSuccessfulMetadata) {
        CheckMetadataCleanup(true);
    }

    Y_UNIT_TEST(UnsupportedColumnErrorNamesColumnAndType) {
        CheckMetadataCleanup(false, true);
    }

    Y_UNIT_TEST(RewindDiscardsLateSessionAndStopsProviderPhases) {
        CheckMetadataCancellation(false);
    }

    Y_UNIT_TEST(RewindDiscardsLateSchemaAfterTransformerDestruction) {
        CheckMetadataCancellation(true);
    }

    Y_UNIT_TEST(LocalDeadlineCancelsPendingTransport) {
        CheckMetadataCancellation(false, true);
    }

    Y_UNIT_TEST(LocalDeadlineIsSharedByMetadataPhases) {
        CheckMetadataCancellation(true, true);
    }

    Y_UNIT_TEST(MetadataClientsAreSharedAcrossCompilationsAndCredentialsAreIsolated) {
        NYdb::TDriver driver(NYdb::TDriverConfig().SetDiscoveryMode(NYdb::EDiscoveryMode::Off));
        NYdb::TDriver tlsDriver(NYdb::TDriverConfig().SetDiscoveryMode(NYdb::EDiscoveryMode::Off));
        auto cache = CreateYdbExternalMetadataClientCache(driver, tlsDriver);
        auto credentials = CreateStructuredTokenCredentialsFactory();
        auto types = MakeIntrusive<TTypeAnnotationContext>();
        TState first(types.Get(), [cache] { return cache; }, credentials);
        TState second(types.Get(), [cache] { return cache; }, credentials);
        const TString token = ComposeStructuredTokenJsonForTokenAuthWithSecret("same-secret", "first-token");
        const TString rotated = ComposeStructuredTokenJsonForTokenAuthWithSecret("same-secret", "second-token");
        auto client = first.MetadataClientCacheFactory()->GetClient("localhost:1", "/Remote", false, token, credentials);
        UNIT_ASSERT(client == second.MetadataClientCacheFactory()->GetClient("localhost:1", "/Remote", false, token, credentials));
        UNIT_ASSERT(client != cache->GetClient("localhost:2", "/Remote", false, token, credentials));
        UNIT_ASSERT(client != cache->GetClient("localhost:1", "/Other", false, token, credentials));
        UNIT_ASSERT(client != cache->GetClient("localhost:1", "/Remote", true, token, credentials));
        UNIT_ASSERT(client != cache->GetClient("localhost:1", "/Remote", false, rotated, credentials));
        UNIT_ASSERT(client != cache->GetClient("localhost:1", "/Remote", false, token, CreateStructuredTokenCredentialsFactory()));
        driver.Stop(true);
        tlsDriver.Stop(true);
    }

    Y_UNIT_TEST(EvictionReleasesSdkStateEvenWhenAnotherClientUsesTheSameCredentials) {
        NYdb::TDriver driver(NYdb::TDriverConfig().SetDiscoveryMode(NYdb::EDiscoveryMode::Off));
        auto credentials = std::make_shared<TTrackingCredentialsFactory>();
        const TString token = TStructuredTokenBuilder().SetIAMToken("private-token").ToJson();
        // A regular SDK client keeps its database state alive throughout the
        // test, as a concurrent query stream would in production.
        NYdb::NTable::TTableClient otherClient(driver, NYdb::NTable::TClientSettings()
            .Database("/Remote").DiscoveryEndpoint("localhost:1").DiscoveryMode(NYdb::EDiscoveryMode::Off)
            .CredentialsProviderFactory(credentials->Create(token, false)));
        UNIT_ASSERT_VALUES_EQUAL(credentials->Facilities.size(), 1);
        auto cache = CreateYdbExternalMetadataClientCache(driver, driver, 1);
        auto first = cache->GetClient("localhost:1", "/Remote", false, token, credentials);
        UNIT_ASSERT_VALUES_EQUAL(credentials->Facilities.size(), 2);
        UNIT_ASSERT(credentials->Facilities[0].lock() != credentials->Facilities[1].lock());
        const auto firstState = credentials->Facilities[1];
        first.reset();
        auto other = cache->GetClient("localhost:1", "/Other", false, token, credentials);
        UNIT_ASSERT(firstState.expired());
        auto recreated = cache->GetClient("localhost:1", "/Remote", false, token, credentials);
        UNIT_ASSERT_VALUES_EQUAL(credentials->Facilities.size(), 4);
        UNIT_ASSERT(!credentials->Facilities[0].expired());
        UNIT_ASSERT(credentials->Facilities[0].lock() != credentials->Facilities.back().lock());
        driver.Stop(true);
    }

    Y_UNIT_TEST(CacheHonorsLruAndIdleExpiry) {
        NYdb::TDriver driver(NYdb::TDriverConfig().SetDiscoveryMode(NYdb::EDiscoveryMode::Off));
        auto credentials = CreateStructuredTokenCredentialsFactory();
        const TString token = TStructuredTokenBuilder().SetNoAuth().ToJson();
        auto cache = CreateYdbExternalMetadataClientCache(driver, driver, 2);
        auto first = cache->GetClient("localhost:1", "/First", false, token, credentials);
        auto second = cache->GetClient("localhost:1", "/Second", false, token, credentials);
        UNIT_ASSERT(first == cache->GetClient("localhost:1", "/First", false, token, credentials));
        auto third = cache->GetClient("localhost:1", "/Third", false, token, credentials);
        UNIT_ASSERT(first == cache->GetClient("localhost:1", "/First", false, token, credentials));
        UNIT_ASSERT(second != cache->GetClient("localhost:1", "/Second", false, token, credentials));

        auto expiring = CreateYdbExternalMetadataClientCache(driver, driver, 2, TDuration::Zero());
        auto expired = expiring->GetClient("localhost:1", "/Remote", false, token, credentials);
        UNIT_ASSERT(expired != expiring->GetClient("localhost:1", "/Remote", false, token, credentials));
        driver.Stop(true);
    }

    Y_UNIT_TEST(OversizedCredentialKeysAreNotRetained) {
        NYdb::TDriver driver(NYdb::TDriverConfig().SetDiscoveryMode(NYdb::EDiscoveryMode::Off));
        auto cache = CreateYdbExternalMetadataClientCache(driver, driver);
        auto credentials = CreateStructuredTokenCredentialsFactory();
        const TString token = TStructuredTokenBuilder().SetIAMToken(TString(1 << 20, 'x')).ToJson();
        auto first = cache->GetClient("localhost:1", "/Remote", false, token, credentials);
        UNIT_ASSERT(first != cache->GetClient("localhost:1", "/Remote", false, token, credentials));
        driver.Stop(true);
    }
}

} // namespace NYql::NYdbExternal
