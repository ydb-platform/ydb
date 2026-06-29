#include <ydb/public/api/grpc/ydb_topic_deferred_publish_v1.grpc.pb.h>

#include <ydb/core/testlib/test_client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>

#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/grpcpp.h>

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

NPersQueue::TTestServer MakeServerWithDeferredPublishEnabled() {
    auto settings = NKikimr::NPersQueueTests::PQSettings();
    settings.FeatureFlags.SetEnableTopicDeferredPublish(true);
    return NPersQueue::TTestServer(settings);
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

    const auto outcome = CallBeginPublication(*MakeStub(server), "/Root", "pub-1");
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

Y_UNIT_TEST(BeginPublicationRejectsMissingDatabase) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    grpc::ClientContext context;
    context.AddMetadata(NYdb::YDB_DATABASE_HEADER, "");
    context.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, "root@builtin");

    Ydb::Topic::DeferredPublish::BeginPublicationRequest request;
    request.set_ext_publication_id("pub-no-db");
    Ydb::Topic::DeferredPublish::BeginPublicationResponse response;
    const auto rpcStatus = MakeStub(server)->BeginPublication(&context, request, &response);

    UNIT_ASSERT(rpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::BAD_REQUEST);
    UNIT_ASSERT_GT(response.operation().issues_size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(response.operation().issues(0).message(), "Database name is not set");
}

Y_UNIT_TEST(BeginPublicationAssignsDistinctIntIds) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    const ui64 firstId = BeginPublicationIntId(CallBeginPublication(*stub, "/Root", "ext-a"));
    const ui64 secondId = BeginPublicationIntId(CallBeginPublication(*stub, "/Root", "ext-b"));
    UNIT_ASSERT(firstId != secondId);
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
