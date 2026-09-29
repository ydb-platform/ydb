#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_control.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/completion_queue.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/async_stream.h>
#include <grpcpp/support/async_unary_call.h>

#include <chrono>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NFederatedQueryTest;

constexpr TDuration WaitTimeout = TDuration::Seconds(10);

struct TMetadataTag {
    bool Complete = false;
    bool Ok = false;
};

template <typename TRequest, typename TResponse>
struct TMetadataCall {
    grpc::ServerContext Context;
    TRequest Request;
    grpc::ServerAsyncResponseWriter<TResponse> Writer{&Context};
    TMetadataTag Accepted;
    TMetadataTag Finished;
    TMetadataTag Done;
};

struct TReadCall {
    grpc::ServerContext Context;
    Ydb::Query::ExecuteQueryRequest Request;
    grpc::ServerAsyncWriter<Ydb::Query::ExecuteQueryResponsePart> Writer{&Context};
    TMetadataTag Accepted;
    TMetadataTag Done;
};

// Public RPCs are deliberately left pending. Cancellation must reach this
// server through KQP compilation or execution and the native provider's SDK.
class TDelayedMetadataServer {
public:
    TDelayedMetadataServer() {
        NTesting::InitPortManagerFromEnv();
        Endpoint = TStringBuilder() << "127.0.0.1:" << NTesting::GetFreePort();
        grpc::ServerBuilder builder;
        builder.AddListeningPort(Endpoint, grpc::InsecureServerCredentials());
        builder.RegisterService(&Service_);
        builder.RegisterService(&QueryService_);
        Queue_ = builder.AddCompletionQueue();
        Server_ = builder.BuildAndStart();
        UNIT_ASSERT(Server_);
        Create.Context.AsyncNotifyWhenDone(&Create.Done);
        Describe.Context.AsyncNotifyWhenDone(&Describe.Done);
        Delete.Context.AsyncNotifyWhenDone(&Delete.Done);
        Read.Context.AsyncNotifyWhenDone(&Read.Done);
        Service_.RequestCreateSession(&Create.Context, &Create.Request, &Create.Writer,
            Queue_.get(), Queue_.get(), &Create.Accepted);
        Service_.RequestDescribeTable(&Describe.Context, &Describe.Request, &Describe.Writer,
            Queue_.get(), Queue_.get(), &Describe.Accepted);
        Service_.RequestDeleteSession(&Delete.Context, &Delete.Request, &Delete.Writer,
            Queue_.get(), Queue_.get(), &Delete.Accepted);
        QueryService_.RequestExecuteQuery(&Read.Context, &Read.Request, &Read.Writer,
            Queue_.get(), Queue_.get(), &Read.Accepted);
    }

    ~TDelayedMetadataServer() {
        Server_->Shutdown(std::chrono::system_clock::now());
        Queue_->Shutdown();
        void* tag = nullptr;
        bool ok = false;
        while (Queue_->Next(&tag, &ok)) {
        }
    }

    void WaitFor(TMetadataTag& expected) {
        const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
        while (!expected.Complete) {
            void* tag = nullptr;
            bool ok = false;
            UNIT_ASSERT(Queue_->AsyncNext(&tag, &ok, deadline) == grpc::CompletionQueue::GOT_EVENT);
            auto& received = *static_cast<TMetadataTag*>(tag);
            UNIT_ASSERT(!received.Complete);
            received.Complete = true;
            received.Ok = ok;
        }
    }

    void ReturnSession() {
        WaitFor(Create.Accepted);
        UNIT_ASSERT(Create.Accepted.Ok);
        Ydb::Table::CreateSessionResult result;
        result.set_session_id("native-kqp-metadata-session");
        Ydb::Table::CreateSessionResponse response;
        response.mutable_operation()->set_ready(true);
        response.mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
        response.mutable_operation()->mutable_result()->PackFrom(result);
        Create.Writer.Finish(response, grpc::Status::OK, &Create.Finished);
        WaitFor(Create.Finished);
        UNIT_ASSERT(Create.Finished.Ok);
    }

    void ReturnSchemaAndClose() {
        WaitFor(Describe.Accepted);
        UNIT_ASSERT(Describe.Accepted.Ok);
        Ydb::Table::DescribeTableResult description;
        description.set_store_type(Ydb::Table::STORE_TYPE_ROW);
        description.add_primary_key("Key");
        auto* column = description.add_columns();
        column->set_name("Key");
        column->mutable_type()->set_type_id(Ydb::Type::UINT64);
        column->set_not_null(true);
        Ydb::Table::DescribeTableResponse response;
        response.mutable_operation()->set_ready(true);
        response.mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
        response.mutable_operation()->mutable_result()->PackFrom(description);
        Describe.Writer.Finish(response, grpc::Status::OK, &Describe.Finished);
        WaitFor(Describe.Finished);
        UNIT_ASSERT(Describe.Finished.Ok);

        WaitFor(Delete.Accepted);
        UNIT_ASSERT(Delete.Accepted.Ok);
        Ydb::Table::DeleteSessionResponse closed;
        closed.mutable_operation()->set_ready(true);
        closed.mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
        Delete.Writer.Finish(closed, grpc::Status::OK, &Delete.Finished);
        WaitFor(Delete.Finished);
        UNIT_ASSERT(Delete.Finished.Ok);
    }

    TString Endpoint;
    TMetadataCall<Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse> Create;
    TMetadataCall<Ydb::Table::DescribeTableRequest, Ydb::Table::DescribeTableResponse> Describe;
    TMetadataCall<Ydb::Table::DeleteSessionRequest, Ydb::Table::DeleteSessionResponse> Delete;
    TReadCall Read;

private:
    Ydb::Table::V1::TableService::AsyncService Service_;
    Ydb::Query::V1::QueryService::AsyncService QueryService_;
    std::unique_ptr<grpc::ServerCompletionQueue> Queue_;
    std::unique_ptr<grpc::Server> Server_;
};

struct TMetadataQueryFixture {
    TDelayedMetadataServer Remote;
    std::shared_ptr<TKikimrRunner> Consumer;

    TMetadataQueryFixture() {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableNativeYdbProvider(true);
        config.MutableQueryServiceConfig()->SetAllExternalDataSourcesAreAvailable(false);
        config.MutableQueryServiceConfig()->AddAvailableExternalDataSources("Ydb");
        Consumer = MakeKikimrRunner(false, nullptr, nullptr, config,
            NYql::NDq::CreateS3ActorsFactory(),
            {.DomainRoot = "Consumer", .CredentialsFactory = CreateCredentialsFactory("root@builtin"), .AuthToken = "root@builtin"});
        auto client = Consumer->GetQueryClient();
        auto secret = client.ExecuteQuery("CREATE SECRET remote_token WITH (value = 'root@builtin');",
            TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(secret.IsSuccess(), secret.GetIssues().ToString());
        auto source = client.ExecuteQuery(TStringBuilder()
            << "CREATE EXTERNAL DATA SOURCE remote_db WITH (SOURCE_TYPE='Ydb', LOCATION='"
            << Remote.Endpoint << "', DATABASE_NAME='/Remote', USE_TLS='false', "
            << "AUTH_METHOD='TOKEN', TOKEN_SECRET_PATH='remote_token');", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(source.IsSuccess(), source.GetIssues().ToString());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(KqpNativeYdbMetadata) {
    Y_UNIT_TEST(ClientRpcDeadlineSurvivesRemoteRequestSerialization) {
        using TRpc = NGRpcService::TGrpcRequestNoOperationCall<Ydb::Query::ExecuteQueryRequest,
            Ydb::Query::ExecuteQueryResponsePart>;
        using TCallback = std::function<void(const Ydb::Query::ExecuteQueryResponsePart&)>;
        using TContext = NRpcService::TLocalRpcCtx<TRpc, TCallback>;
        for (const auto deadline : {TInstant::Zero(), TInstant::MicroSeconds(123456789),
                                   TInstant::Now() + TDuration::Seconds(5), TInstant::Max()}) {
            auto context = std::make_shared<TContext>(Ydb::Query::ExecuteQueryRequest{},
                TCallback([](const auto&) {}), TContext::TSettings{.DatabaseName = "/Consumer", .Deadline = deadline});
            TEvKqp::TEvQueryRequest request(NKikimrKqp::QUERY_ACTION_EXECUTE,
                NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY, {}, context, "session", "SELECT 1", "",
                nullptr, nullptr, Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE, nullptr, nullptr);
            UNIT_ASSERT_VALUES_EQUAL(request.GetRequestDeadline(), deadline);
            // The same conversion runs before sending a request to another node.
            // Past and zero deadlines must not become a fresh relative timeout.
            UNIT_ASSERT(request.CalculateSerializedSize());
            UNIT_ASSERT(!request.GetRequestCtx());
            TEvKqp::TEvQueryRequest restored;
            UNIT_ASSERT(restored.Record.ParseFromString(request.Record.SerializeAsString()));
            UNIT_ASSERT_VALUES_EQUAL(restored.GetRequestDeadline(), deadline);
        }
    }

    Y_UNIT_TEST(QueryDeadlineCancelsDescribeTableTransport) {
        TMetadataQueryFixture fixture;
        auto client = fixture.Consumer->GetQueryClient();
        const auto started = std::chrono::system_clock::now();
        auto future = client.ExecuteQuery("SELECT * FROM remote_db.`items`;", TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(5))
                .RetrySettings(NRetry::TRetryOperationSettings().MaxRetries(0)));
        fixture.Remote.ReturnSession();
        fixture.Remote.WaitFor(fixture.Remote.Describe.Accepted);
        UNIT_ASSERT(fixture.Remote.Describe.Accepted.Ok);
        // Catch a fresh fixed 60-second provider deadline even if client-loss
        // cancellation later tears down the same compilation successfully.
        UNIT_ASSERT(fixture.Remote.Describe.Context.deadline() <= started + std::chrono::seconds(6));
        fixture.Remote.WaitFor(fixture.Remote.Describe.Done);
        UNIT_ASSERT(fixture.Remote.Describe.Context.IsCancelled());
        UNIT_ASSERT(future.Wait(WaitTimeout));
        const auto result = future.ExtractValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_C(result.GetStatus() == EStatus::TIMEOUT || result.GetStatus() == EStatus::CLIENT_DEADLINE_EXCEEDED ||
            result.GetStatus() == EStatus::CANCELLED || result.GetStatus() == EStatus::CLIENT_CANCELLED,
            result.GetIssues().ToString());
    }

    Y_UNIT_TEST(QueryCancellationCancelsCreateSessionTransport) {
        TMetadataQueryFixture fixture;
        auto client = fixture.Consumer->GetQueryClient();
        auto control = std::make_shared<TRequestControl>();
        auto future = client.ExecuteQuery("SELECT * FROM remote_db.`items`;", TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(30)).RequestControl(control)
                .RetrySettings(NRetry::TRetryOperationSettings().MaxRetries(0)));
        fixture.Remote.WaitFor(fixture.Remote.Create.Accepted);
        UNIT_ASSERT(fixture.Remote.Create.Accepted.Ok);
        UNIT_ASSERT(!future.HasValue());
        control->Cancel();
        // WaitFor is bounded by 10 seconds, well before the request's 30-second
        // deadline. A discarded local future alone cannot satisfy this check.
        fixture.Remote.WaitFor(fixture.Remote.Create.Done);
        UNIT_ASSERT(fixture.Remote.Create.Context.IsCancelled());
        UNIT_ASSERT(future.Wait(WaitTimeout));
        const auto result = future.ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::CLIENT_CANCELLED);
    }

    Y_UNIT_TEST(QueryDeadlineReachesRemoteReadAfterMetadata) {
        TMetadataQueryFixture fixture;
        auto client = fixture.Consumer->GetQueryClient();
        const auto started = std::chrono::system_clock::now();
        auto future = client.ExecuteQuery("SELECT Key FROM remote_db.`items`;", TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(5))
                .RetrySettings(NRetry::TRetryOperationSettings().MaxRetries(0)));
        fixture.Remote.ReturnSession();
        fixture.Remote.ReturnSchemaAndClose();
        fixture.Remote.WaitFor(fixture.Remote.Read.Accepted);
        UNIT_ASSERT(fixture.Remote.Read.Accepted.Ok);
        // This is the DQ source's Query Service request, after compilation has
        // completed. Source settings cached in the plan must not reset its deadline.
        UNIT_ASSERT(fixture.Remote.Read.Context.deadline() <= started + std::chrono::seconds(6));
        fixture.Remote.WaitFor(fixture.Remote.Read.Done);
        UNIT_ASSERT(fixture.Remote.Read.Context.IsCancelled());
        UNIT_ASSERT(future.Wait(WaitTimeout));
        const auto result = future.ExtractValueSync();
        UNIT_ASSERT(!result.IsSuccess());
    }

    Y_UNIT_TEST(SessionQueryCancellationStopsMetadataAndAllowsNextQuery) {
        TMetadataQueryFixture fixture;
        auto client = fixture.Consumer->GetQueryClient();
        auto sessionResult = client.GetSession().ExtractValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();
        auto control = std::make_shared<TRequestControl>();
        auto future = session.ExecuteQuery("SELECT * FROM remote_db.`items`;", TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(30)).RequestControl(control));
        fixture.Remote.WaitFor(fixture.Remote.Create.Accepted);
        UNIT_ASSERT(fixture.Remote.Create.Accepted.Ok);
        control->Cancel();
        fixture.Remote.WaitFor(fixture.Remote.Create.Done);
        UNIT_ASSERT(fixture.Remote.Create.Context.IsCancelled());
        UNIT_ASSERT(future.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(future.ExtractValueSync().GetStatus(), EStatus::CLIENT_CANCELLED);

        const auto next = session.ExecuteQuery("SELECT 1 AS Value;", TTxControl::NoTx(),
            TExecuteQuerySettings().ClientTimeout(TDuration::Seconds(5))).ExtractValueSync();
        UNIT_ASSERT_C(next.IsSuccess(), next.GetIssues().ToString());
        auto rows = next.GetResultSetParser(0);
        UNIT_ASSERT(rows.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(rows.ColumnParser("Value").GetInt32(), 1);
    }
}

} // namespace NKikimr::NKqp
