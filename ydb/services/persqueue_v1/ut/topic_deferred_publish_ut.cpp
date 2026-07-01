#include <ydb/public/api/grpc/ydb_topic_deferred_publish_v1.grpc.pb.h>

#include <ydb/core/cms/console/console.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>

#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <thread>

namespace NKikimr::NPersQueueTests {

namespace {

constexpr TStringBuf NotImplementedMessage = "Topic deferred publish is not implemented yet";
constexpr TStringBuf DisabledMessage = "Topic deferred publish is not enabled";

void AssertNotImplemented(const Ydb::Operations::Operation& operation) {
    UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::UNSUPPORTED);
    UNIT_ASSERT(operation.ready());
    UNIT_ASSERT_GT(operation.issues_size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(operation.issues(0).message(), TString(NotImplementedMessage));
}

void AssertUnsupported(const Ydb::Operations::Operation& operation, TStringBuf message) {
    UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::UNSUPPORTED);
    UNIT_ASSERT(operation.ready());
    UNIT_ASSERT_GT(operation.issues_size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(operation.issues(0).message(), TString(message));
}

constexpr TStringBuf PublicationsTableRelativePath = ".metadata/topic_deferred_publications";
constexpr TStringBuf DestinationsTableRelativePath = ".metadata/topic_deferred_publication_destinations";
constexpr TStringBuf WriterBuiltinUser = "writer@builtin";

void FillClientContext(
    grpc::ClientContext& context,
    const TString& database,
    const TString& authTicket = "root@builtin")
{
    context.AddMetadata(NYdb::YDB_DATABASE_HEADER, database);
    context.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, authTicket);
}

std::unique_ptr<Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService::Stub> MakeStub(
    const NPersQueue::TTestServer& server)
{
    auto channel = grpc::CreateChannel(
        "localhost:" + ToString(server.GrpcPort),
        grpc::InsecureChannelCredentials());
    return Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService::NewStub(channel);
}

struct TBeginPublicationOutcome {
    grpc::Status RpcStatus;
    Ydb::Operations::Operation Operation;
};

TBeginPublicationOutcome CallBeginPublication(
    Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService::Stub& stub,
    const TString& database,
    const TString& extPublicationId,
    const TMaybe<TString>& writerIdentity = Nothing(),
    bool setDatabaseHeader = true,
    const TString& authTicket = "root@builtin")
{
    grpc::ClientContext context;
    if (setDatabaseHeader) {
        FillClientContext(context, database, authTicket);
    } else {
        context.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, authTicket);
    }

    Ydb::Topic::DeferredPublish::BeginPublicationRequest request;
    request.set_ext_publication_id(extPublicationId);
    if (writerIdentity.Defined()) {
        request.set_writer_identity(*writerIdentity);
    }

    Ydb::Topic::DeferredPublish::BeginPublicationResponse response;
    const auto rpcStatus = stub.BeginPublication(&context, request, &response);
    return {rpcStatus, response.operation()};
}

ui64 BeginPublicationIntId(const TBeginPublicationOutcome& outcome) {
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT(outcome.Operation.ready());
    Ydb::Topic::DeferredPublish::BeginPublicationResult result;
    UNIT_ASSERT(outcome.Operation.result().UnpackTo(&result));
    UNIT_ASSERT_GT(result.int_publication_id(), 0u);
    return result.int_publication_id();
}

bool SchemePathExists(NPersQueue::TTestServer& server, const TString& path) {
    const auto response = server.AnnoyingClient->Ls(path);
    return response && response->Record.GetSchemeStatus() == NKikimrScheme::StatusSuccess;
}

void GrantPublicationTableRead(NPersQueue::TTestServer& server, const TString& subject) {
    server.AnnoyingClient->TestGrant(
        "/Root",
        ".metadata",
        subject,
        NACLib::EAccessRights::GenericRead);
    server.AnnoyingClient->TestGrant(
        "/Root/.metadata",
        "topic_deferred_publications",
        subject,
        NACLib::EAccessRights::GenericRead);
}

void AssertPublicationRow(
    const NPersQueue::TTestServer& server,
    const TString& authTicket,
    const TString& extPublicationId,
    const TString& writerIdentity,
    const TString& createdBy)
{
    NYdb::NTable::TTableClient client(
        server.GetDriver(),
        NYdb::NTable::TClientSettings().AuthToken(authTicket));

    NYdb::TParamsBuilder params;
    params.AddParam("$ext").Utf8(extPublicationId).Build();

    const auto query = TStringBuilder()
        << "SELECT ext_publication_id, writer_identity, created_by, created_at "
        << "FROM `" << PublicationsTableRelativePath << "` "
        << "WHERE ext_publication_id = $ext;";

    TMaybe<NYdb::TResultSet> rows;
    const auto status = client.RetryOperationSync([&](NYdb::NTable::TSession session) {
        auto result = session.ExecuteDataQuery(
            query,
            NYdb::NTable::TTxControl::BeginTx().CommitTx(),
            params.Build()).GetValueSync();
        if (result.IsSuccess() && !result.GetResultSets().empty()) {
            rows = result.GetResultSet(0);
        }
        return result;
    });
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    UNIT_ASSERT(rows.Defined());

    NYdb::TResultSetParser parser(*rows);
    UNIT_ASSERT(parser.TryNextRow());
    UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("ext_publication_id").GetUtf8(), extPublicationId);
    UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("writer_identity").GetOptionalUtf8().value(), writerIdentity);
    UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("created_by").GetOptionalUtf8().value(), createdBy);
    UNIT_ASSERT(parser.ColumnParser("created_at").GetTimestamp() > TInstant::Zero());
    UNIT_ASSERT(!parser.TryNextRow());
}

void AssertDestinationRowCount(
    const NPersQueue::TTestServer& server,
    const TString& authTicket,
    ui64 intPublicationId,
    ui64 expectedCount)
{
    NYdb::NTable::TTableClient client(
        server.GetDriver(),
        NYdb::NTable::TClientSettings().AuthToken(authTicket));

    NYdb::TParamsBuilder params;
    params.AddParam("$int_publication_id").Uint64(intPublicationId).Build();

    const auto query = TStringBuilder()
        << "SELECT COUNT(*) AS `count` "
        << "FROM `" << DestinationsTableRelativePath << "` "
        << "WHERE int_publication_id = $int_publication_id;";

    TMaybe<ui64> count;
    const auto status = client.RetryOperationSync([&](NYdb::NTable::TSession session) {
        auto result = session.ExecuteDataQuery(
            query,
            NYdb::NTable::TTxControl::BeginTx().CommitTx(),
            params.Build()).GetValueSync();
        if (result.IsSuccess() && !result.GetResultSets().empty()) {
            NYdb::TResultSetParser parser(result.GetResultSet(0));
            if (parser.TryNextRow()) {
                count = parser.ColumnParser("count").GetUint64();
            }
        }
        return result;
    });
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    UNIT_ASSERT(count.Defined());
    UNIT_ASSERT_VALUES_EQUAL(*count, expectedCount);
}

NPersQueue::TTestServer MakeServerWithDeferredPublishEnabled(
    bool forbidRequestsToStaticNodesWithoutDatabase = true)
{
    auto settings = NKikimr::NPersQueueTests::PQSettings()
        .SetEnableTopicDeferredPublish(true);
    settings.FeatureFlags.SetForbidRequestsToStaticNodesWithoutDatabase(
        forbidRequestsToStaticNodesWithoutDatabase);
    return NPersQueue::TTestServer(settings);
}

TBeginPublicationOutcome CallBeginPublicationWithEmptyDatabaseHeader(
    Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService::Stub& stub,
    const TString& extPublicationId = "pub-no-db")
{
    return CallBeginPublication(stub, "", extPublicationId);
}

TBeginPublicationOutcome CallBeginPublicationWithoutDatabaseHeader(
    Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService::Stub& stub,
    const TString& extPublicationId = "pub-no-db")
{
    return CallBeginPublication(stub, "", extPublicationId, Nothing(), false);
}

void WaitDatabaseRunning(NActors::TTestActorRuntime& runtime, const TString& path) {
    using namespace NKikimr::NConsole;

    Ydb::Cms::GetDatabaseStatusResult status;
    const TActorId edgeActor = runtime.AllocateEdgeActor();
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
    while (TInstant::Now() < deadline) {
        auto request = std::make_unique<TEvConsole::TEvGetTenantStatusRequest>();
        request->Record.MutableRequest()->set_path(path);
        runtime.SendToPipe(MakeConsoleID(), edgeActor, request.release(), 0, GetPipeConfigWithRetries());

        auto response = runtime.GrabEdgeEvent<TEvConsole::TEvGetTenantStatusResponse>(edgeActor, TDuration::Seconds(5));
        if (response) {
            response->Get()->Record.GetResponse().operation().result().UnpackTo(&status);
            if (status.state() == Ydb::Cms::GetDatabaseStatusResult::RUNNING) {
                return;
            }
        }
        Sleep(TDuration::MilliSeconds(100));
    }
    UNIT_FAIL(TStringBuilder() << "Database " << path << " is not RUNNING, last status:\n" << status.DebugString());
}

void CreateServerlessDatabase(
    NPersQueue::TTestServer& server,
    const TString& sharedPath,
    const TString& serverlessPath)
{
    auto& runtime = *server.CleverServer->GetRuntime();

    using TEvCreateDatabaseRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<
        Ydb::Cms::CreateDatabaseRequest,
        Ydb::Cms::CreateDatabaseResponse>;

    {
        Ydb::Cms::CreateDatabaseRequest request;
        request.set_path(sharedPath);
        auto* storage = request.mutable_shared_resources()->add_storage_units();
        storage->set_unit_kind(sharedPath);
        storage->set_count(1);

        const auto response = NKikimr::NRpcService::DoLocalRpc<TEvCreateDatabaseRequest>(
            std::move(request), "", "", runtime.GetActorSystem(0), true).ExtractValueSync();
        UNIT_ASSERT(response.operation().ready());
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    const ui32 dynamicNodeIdx = server.CleverServer->StaticNodes();
    server.CleverServer->SetupDynamicLocalService(dynamicNodeIdx, sharedPath);
    WaitDatabaseRunning(runtime, sharedPath);

    {
        Ydb::Cms::CreateDatabaseRequest request;
        request.set_path(serverlessPath);
        request.mutable_serverless_resources()->set_shared_database_path(sharedPath);

        const auto response = NKikimr::NRpcService::DoLocalRpc<TEvCreateDatabaseRequest>(
            std::move(request), "", "", runtime.GetActorSystem(0), true).ExtractValueSync();
        UNIT_ASSERT(response.operation().ready());
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }

    WaitDatabaseRunning(runtime, serverlessPath);

    if (!server.CleverServer->GetSettings().UseRealThreads) {
        runtime.SimulateSleep(TDuration::Seconds(1));
    } else {
        Sleep(TDuration::Seconds(1));
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TopicDeferredPublishService) {

Y_UNIT_TEST(BeginPublicationDisabledByDefault) {
    NPersQueue::TTestServer server;
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);

    grpc::ClientContext context;
    FillClientContext(context, "/Root");
    Ydb::Topic::DeferredPublish::BeginPublicationRequest request;
    request.set_ext_publication_id("pub-1");
    Ydb::Topic::DeferredPublish::BeginPublicationResponse response;
    UNIT_ASSERT(stub->BeginPublication(&context, request, &response).ok());
    AssertUnsupported(response.operation(), DisabledMessage);
}

Y_UNIT_TEST(BeginPublicationCreatesPublication) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    const auto outcome = CallBeginPublication(*MakeStub(server), "/Root", "pub-1", "writer-1");
    BeginPublicationIntId(outcome);
}

Y_UNIT_TEST(BeginPublicationCreatesMetadataTables) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    UNIT_ASSERT(!SchemePathExists(server, "/Root/.metadata/topic_deferred_publications"));
    UNIT_ASSERT(!SchemePathExists(server, "/Root/.metadata/topic_deferred_publication_destinations"));

    BeginPublicationIntId(CallBeginPublication(*MakeStub(server), "/Root", "lazy-ddl"));

    UNIT_ASSERT(SchemePathExists(server, "/Root/.metadata/topic_deferred_publications"));
    UNIT_ASSERT(SchemePathExists(server, "/Root/.metadata/topic_deferred_publication_destinations"));
}

Y_UNIT_TEST(BeginPublicationRejectsDuplicateExtPublicationId) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);

    {
        const auto outcome = CallBeginPublication(*stub, "/Root", "dup-ext");
        UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::SUCCESS);
    }

    {
        const auto outcome = CallBeginPublication(*stub, "/Root", "dup-ext");
        UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::ALREADY_EXISTS);
    }
}

Y_UNIT_TEST(BeginPublicationRejectsEmptyExtPublicationId) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    const auto outcome = CallBeginPublication(*MakeStub(server), "/Root", "");
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::BAD_REQUEST);
    UNIT_ASSERT_GT(outcome.Operation.issues_size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.issues(0).message(), "ext_publication_id must not be empty");
}

Y_UNIT_TEST(BeginPublicationRejectsEmptyDatabaseAtGrpcProxy) {
    auto server = MakeServerWithDeferredPublishEnabled(true);
    server.AnnoyingClient->GrantConnect("root@builtin");

    const auto outcome = CallBeginPublicationWithEmptyDatabaseHeader(*MakeStub(server));
    UNIT_ASSERT(!outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.RpcStatus.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

Y_UNIT_TEST(BeginPublicationRejectsEmptyDatabaseInHandler) {
    auto server = MakeServerWithDeferredPublishEnabled(false);
    server.AnnoyingClient->GrantConnect("root@builtin");

    const auto outcome = CallBeginPublicationWithEmptyDatabaseHeader(*MakeStub(server));
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::BAD_REQUEST);
    UNIT_ASSERT_GT(outcome.Operation.issues_size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.issues(0).message(), "Database name is not set");
}

Y_UNIT_TEST(BeginPublicationRejectsMissingDatabaseAtGrpcProxy) {
    auto server = MakeServerWithDeferredPublishEnabled(true);
    server.AnnoyingClient->GrantConnect("root@builtin");

    const auto outcome = CallBeginPublicationWithoutDatabaseHeader(*MakeStub(server));
    UNIT_ASSERT(!outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.RpcStatus.error_code(), grpc::StatusCode::UNAUTHENTICATED);
}

Y_UNIT_TEST(BeginPublicationRejectsMissingDatabaseInHandler) {
    auto server = MakeServerWithDeferredPublishEnabled(false);
    server.AnnoyingClient->GrantConnect("root@builtin");

    const auto outcome = CallBeginPublicationWithoutDatabaseHeader(*MakeStub(server));
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::BAD_REQUEST);
    UNIT_ASSERT_GT(outcome.Operation.issues_size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.issues(0).message(), "Database name is not set");
}

Y_UNIT_TEST(BeginPublicationAssignsDistinctIntIds) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    const ui64 firstId = BeginPublicationIntId(CallBeginPublication(*stub, "/Root", "ext-a"));
    const ui64 secondId = BeginPublicationIntId(CallBeginPublication(*stub, "/Root", "ext-b"));
    UNIT_ASSERT(firstId != secondId);
}

Y_UNIT_TEST(BeginPublicationConcurrentColdStart) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    constexpr ui32 parallelRequests = 8;
    std::atomic<ui32> successCount = 0;
    TVector<std::thread> threads;
    threads.reserve(parallelRequests);

    for (ui32 i = 0; i < parallelRequests; ++i) {
        threads.emplace_back([&, i] {
            auto stub = MakeStub(server);
            const auto outcome = CallBeginPublication(*stub, "/Root", TStringBuilder() << "cold-" << i);
            if (outcome.RpcStatus.ok() && outcome.Operation.status() == Ydb::StatusIds::SUCCESS) {
                ++successCount;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    UNIT_ASSERT_VALUES_EQUAL(successCount.load(), parallelRequests);
}

Y_UNIT_TEST(BeginPublicationConcurrentDuplicateExtId) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    BeginPublicationIntId(CallBeginPublication(*stub, "/Root", "warmup-for-dup-race"));

    constexpr ui32 parallelRequests = 4;
    std::atomic<ui32> readyThreads = 0;
    std::atomic<ui32> successCount = 0;
    std::atomic<ui32> alreadyExistsCount = 0;
    std::atomic<ui32> conflictCount = 0;
    TVector<std::thread> threads;
    threads.reserve(parallelRequests);

    for (ui32 i = 0; i < parallelRequests; ++i) {
        threads.emplace_back([&] {
            readyThreads.fetch_add(1, std::memory_order_release);
            while (readyThreads.load(std::memory_order_acquire) < parallelRequests) {
                std::this_thread::yield();
            }

            auto localStub = MakeStub(server);
            const auto outcome = CallBeginPublication(*localStub, "/Root", "race-dup-ext");
            if (!outcome.RpcStatus.ok()) {
                return;
            }
            if (outcome.Operation.status() == Ydb::StatusIds::SUCCESS) {
                ++successCount;
            } else if (outcome.Operation.status() == Ydb::StatusIds::ALREADY_EXISTS) {
                ++alreadyExistsCount;
            } else if (outcome.Operation.status() == Ydb::StatusIds::ABORTED) {
                ++conflictCount;
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    UNIT_ASSERT_VALUES_EQUAL(successCount.load(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(alreadyExistsCount.load() + conflictCount.load(), 3u);
}

Y_UNIT_TEST(BeginPublicationStoresCreatedByForWriterBuiltin) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");
    server.AnnoyingClient->GrantConnect(TString(WriterBuiltinUser));

    constexpr TStringBuf extPublicationId = "writer-builtin-pub";
    constexpr TStringBuf writerIdentity = "writer-identity-1";

    BeginPublicationIntId(CallBeginPublication(
        *MakeStub(server),
        "/Root",
        TString(extPublicationId),
        TString(writerIdentity),
        true,
        TString(WriterBuiltinUser)));

    GrantPublicationTableRead(server, TString(WriterBuiltinUser));
    AssertPublicationRow(
        server,
        TString(WriterBuiltinUser),
        TString(extPublicationId),
        TString(writerIdentity),
        TString(WriterBuiltinUser));
}

Y_UNIT_TEST(BeginPublicationDoesNotInsertDestinationRows) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    const ui64 intPublicationId = BeginPublicationIntId(
        CallBeginPublication(*MakeStub(server), "/Root", "no-dest"));
    AssertDestinationRowCount(server, "root@builtin", intPublicationId, 0);
}

Y_UNIT_TEST(BeginPublicationSucceedsWhenTablesAlreadyExist) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    BeginPublicationIntId(CallBeginPublication(*stub, "/Root", "warmup"));
    BeginPublicationIntId(CallBeginPublication(*stub, "/Root", "after-warmup"));
}

Y_UNIT_TEST(BeginPublicationWorksInServerlessDatabase) {
    const TString sharedPath = "/Root/shared";
    const TString serverlessPath = "/Root/serverless";

    auto settings = NKikimr::NPersQueueTests::PQSettings();
    settings.FeatureFlags.SetEnableTopicDeferredPublish(true);
    settings.SetNodeCount(1);
    settings.SetDynamicNodeCount(2);
    settings.AddStoragePoolType(sharedPath);

    // Skip FullInit/CheckClustersList: PQ cluster tables are not needed for this test.
    NPersQueue::TTestServer server(settings, false);
    server.StartServer(false);
    server.AnnoyingClient->GrantConnect("root@builtin");

    CreateServerlessDatabase(server, sharedPath, serverlessPath);

    auto stub = MakeStub(server);

    grpc::ClientContext context;
    FillClientContext(context, serverlessPath);
    Ydb::Topic::DeferredPublish::BeginPublicationRequest request;
    request.set_ext_publication_id("serverless-pub");
    Ydb::Topic::DeferredPublish::BeginPublicationResponse response;
    UNIT_ASSERT(stub->BeginPublication(&context, request, &response).ok());

    UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT(response.operation().ready());

    Ydb::Topic::DeferredPublish::BeginPublicationResult result;
    UNIT_ASSERT(response.operation().result().UnpackTo(&result));
    UNIT_ASSERT_GT(result.int_publication_id(), 0u);
}

Y_UNIT_TEST(OtherMethodsReturnNotImplemented) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);

    {
        grpc::ClientContext context;
        FillClientContext(context, "/Root");
        Ydb::Topic::DeferredPublish::PublishRequest request;
        Ydb::Topic::DeferredPublish::PublishResponse response;
        UNIT_ASSERT(stub->Publish(&context, request, &response).ok());
        AssertNotImplemented(response.operation());
    }

    {
        grpc::ClientContext context;
        FillClientContext(context, "/Root");
        Ydb::Topic::DeferredPublish::CancelPublicationRequest request;
        Ydb::Topic::DeferredPublish::CancelPublicationResponse response;
        UNIT_ASSERT(stub->CancelPublication(&context, request, &response).ok());
        AssertNotImplemented(response.operation());
    }

    {
        grpc::ClientContext context;
        FillClientContext(context, "/Root");
        Ydb::Topic::DeferredPublish::ListPublicationsRequest request;
        Ydb::Topic::DeferredPublish::ListPublicationsResponse response;
        UNIT_ASSERT(stub->ListPublications(&context, request, &response).ok());
        AssertNotImplemented(response.operation());
    }

    {
        grpc::ClientContext context;
        FillClientContext(context, "/Root");
        Ydb::Topic::DeferredPublish::DescribePublicationRequest request;
        Ydb::Topic::DeferredPublish::DescribePublicationResponse response;
        UNIT_ASSERT(stub->DescribePublication(&context, request, &response).ok());
        AssertNotImplemented(response.operation());
    }
}

} // Y_UNIT_TEST_SUITE

} // namespace NKikimr::NPersQueueTests
