#include <ydb/public/api/grpc/draft/ydb_topic_deferred_publish_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <ydb/core/cms/console/console.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/persqueue/deferred_publish/delete_publication_query.h>
#include <ydb/core/persqueue/deferred_publish/destination_blob.h>
#include <ydb/core/persqueue/deferred_publish/events.h>
#include <ydb/core/persqueue/deferred_publish/registry_actor.h>
#include <ydb/core/persqueue/deferred_publish/upsert_destination_query.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>

#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/grpcpp.h>

#include <google/protobuf/util/time_util.h>

#include <atomic>
#include <thread>

namespace NKikimr::NPersQueueTests {

namespace {

constexpr TStringBuf DisabledMessage = "Topic deferred publish is not enabled";

void AssertUnsupported(const Ydb::Operations::Operation& operation, TStringBuf message) {
    UNIT_ASSERT_VALUES_EQUAL(operation.status(), Ydb::StatusIds::UNSUPPORTED);
    UNIT_ASSERT(operation.ready());
    UNIT_ASSERT_GT(operation.issues_size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(operation.issues(0).message(), TString(message));
}

constexpr TStringBuf PublicationsTableRelativePath = ".metadata/topic_deferred_publications";
constexpr TStringBuf DestinationsTableRelativePath = ".metadata/topic_deferred_publication_destinations";
constexpr TStringBuf WriterBuiltinUser = "writer@builtin";
constexpr size_t MaxDeferredPublishStringLength = 2048;
constexpr ui32 MaxDeferredPublishPendingQueueSize = 100;

void TestSleep(NActors::TTestActorRuntime& runtime, TDuration duration) {
    if (runtime.IsRealThreads()) {
        Sleep(duration);
    } else {
        runtime.SimulateSleep(duration);
    }
}

void FillClientContext(
    grpc::ClientContext& context,
    const TString& database,
    const TString& authTicket = "root@builtin")
{
    context.AddMetadata(NYdb::YDB_DATABASE_HEADER, database);
    if (!authTicket.empty()) {
        context.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, authTicket);
    }
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
    } else if (!authTicket.empty()) {
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

struct TListPublicationsOutcome {
    grpc::Status RpcStatus;
    Ydb::Operations::Operation Operation;
};

TListPublicationsOutcome CallListPublications(
    Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService::Stub& stub,
    const TString& database,
    const TMaybe<TString>& writerIdentity = Nothing(),
    const TString& authTicket = "root@builtin")
{
    grpc::ClientContext context;
    FillClientContext(context, database, authTicket);

    Ydb::Topic::DeferredPublish::ListPublicationsRequest request;
    if (writerIdentity.Defined()) {
        request.set_writer_identity(*writerIdentity);
    }

    Ydb::Topic::DeferredPublish::ListPublicationsResponse response;
    const auto rpcStatus = stub.ListPublications(&context, request, &response);
    return {rpcStatus, response.operation()};
}

struct TDescribePublicationOutcome {
    grpc::Status RpcStatus;
    Ydb::Operations::Operation Operation;
};

TDescribePublicationOutcome CallDescribePublication(
    Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService::Stub& stub,
    const TString& database,
    ui64 intPublicationId,
    const TString& authTicket = "root@builtin")
{
    grpc::ClientContext context;
    FillClientContext(context, database, authTicket);

    Ydb::Topic::DeferredPublish::DescribePublicationRequest request;
    request.set_int_publication_id(intPublicationId);

    Ydb::Topic::DeferredPublish::DescribePublicationResponse response;
    const auto rpcStatus = stub.DescribePublication(&context, request, &response);
    return {rpcStatus, response.operation()};
}

struct TFinalizePublicationOutcome {
    grpc::Status RpcStatus;
    Ydb::Operations::Operation Operation;
};

TFinalizePublicationOutcome CallPublish(
    Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService::Stub& stub,
    const TString& database,
    ui64 intPublicationId,
    const TString& authTicket = "root@builtin")
{
    grpc::ClientContext context;
    FillClientContext(context, database, authTicket);

    Ydb::Topic::DeferredPublish::PublishRequest request;
    request.set_int_publication_id(intPublicationId);

    Ydb::Topic::DeferredPublish::PublishResponse response;
    const auto rpcStatus = stub.Publish(&context, request, &response);
    return {rpcStatus, response.operation()};
}

TFinalizePublicationOutcome CallCancelPublication(
    Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService::Stub& stub,
    const TString& database,
    ui64 intPublicationId,
    const TString& authTicket = "root@builtin")
{
    grpc::ClientContext context;
    FillClientContext(context, database, authTicket);

    Ydb::Topic::DeferredPublish::CancelPublicationRequest request;
    request.set_int_publication_id(intPublicationId);

    Ydb::Topic::DeferredPublish::CancelPublicationResponse response;
    const auto rpcStatus = stub.CancelPublication(&context, request, &response);
    return {rpcStatus, response.operation()};
}

TMaybe<TString> ParseLegacyReadPayload(const TString& raw) {
    NKikimrPQClient::TDataChunk dataChunk;
    if (!dataChunk.ParseFromString(raw)) {
        return Nothing();
    }
    return dataChunk.GetData();
}

TMaybe<TString> TryReadFirstTopicMessage(
    NPersQueue::TTestServer& server,
    const TString& topicShortName,
    TDuration timeout = TDuration::Seconds(30),
    ui32 partitionId = 0)
{
    const TString topic = "rt3.dc1--" + topicShortName;
    const TInstant deadline = TInstant::Now() + timeout;
    while (TInstant::Now() < deadline) {
        THolder<NMsgBusProxy::TBusPersQueue> request = TRequestReadPQ{
            topic, partitionId, 0, 100, "user", 0}.GetRequest();
        request.Get()->Record.SetTicket("root@builtin");

        const auto response = server.AnnoyingClient->CallPersQueueGRPC(request->Record);
        if ((NMsgBusProxy::EResponseStatus)response.GetStatus() == NMsgBusProxy::MSTATUS_OK) {
            const auto& result = response.GetPartitionResponse().GetCmdReadResult();
            for (ui32 i = 0; i < result.ResultSize(); ++i) {
                const auto& r = result.GetResult(i);
                if (r.HasData()) {
                    if (const auto payload = ParseLegacyReadPayload(r.GetData())) {
                        return payload;
                    }
                }
            }
        }
        TestSleep(*server.CleverServer->GetRuntime(), TDuration::MilliSeconds(100));
    }
    return Nothing();
}

struct TTopicReadMessage {
    i64 PartitionId = 0;
    ui64 Offset = 0;
    TString Data;
};

TVector<TTopicReadMessage> TryReadTopicMessagesViaStreamRead(
    NPersQueue::TTestServer& server,
    Ydb::Topic::V1::TopicService::Stub& topicStub,
    const TString& topicShortName,
    TDuration timeout,
    ui32 minPartitions = 1)
{
    grpc::ClientContext context;
    FillClientContext(context, "/Root");

    auto stream = topicStub.StreamRead(&context);
    if (!stream) {
        return {};
    }

    Ydb::Topic::StreamReadMessage::FromClient req;
    Ydb::Topic::StreamReadMessage::FromServer resp;

    req.mutable_init_request()->add_topics_read_settings()->set_path(topicShortName);
    req.mutable_init_request()->set_consumer("user");
    if (!stream->Write(req) || !stream->Read(&resp)) {
        return {};
    }
    if (resp.server_message_case() != Ydb::Topic::StreamReadMessage::FromServer::kInitResponse) {
        return {};
    }

    auto sendReadRequests = [&]() {
        req.Clear();
        req.mutable_read_request()->set_bytes_size(1024 * 1024);
        for (ui32 i = 0; i < 10; ++i) {
            stream->Write(req);
        }
    };

    sendReadRequests();

    THashMap<i64, i64> partitionIdBySessionId;
    THashSet<i64> acceptedPartitions;
    TVector<TTopicReadMessage> messages;
    const TInstant deadline = TInstant::Now() + timeout;

    while (TInstant::Now() < deadline) {
        if (!stream->Read(&resp)) {
            if (!messages.empty()) {
                break;
            }
            TestSleep(*server.CleverServer->GetRuntime(), TDuration::MilliSeconds(50));
            sendReadRequests();
            continue;
        }

        switch (resp.server_message_case()) {
            case Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest: {
                const auto& partitionSession = resp.start_partition_session_request().partition_session();
                const i64 sessionId = partitionSession.partition_session_id();
                if (!partitionIdBySessionId.contains(sessionId)) {
                    partitionIdBySessionId[sessionId] = partitionSession.partition_id();
                    acceptedPartitions.insert(partitionSession.partition_id());

                    req.Clear();
                    req.mutable_start_partition_session_response()->set_partition_session_id(sessionId);
                    stream->Write(req);
                    sendReadRequests();
                }
                break;
            }
            case Ydb::Topic::StreamReadMessage::FromServer::kReadResponse: {
                for (const auto& partitionData : resp.read_response().partition_data()) {
                    const auto partitionId = partitionIdBySessionId.Value(
                        partitionData.partition_session_id(),
                        -1);
                    for (const auto& batch : partitionData.batches()) {
                        for (const auto& messageData : batch.message_data()) {
                            messages.push_back(TTopicReadMessage{
                                .PartitionId = partitionId,
                                .Offset = static_cast<ui64>(messageData.offset()),
                                .Data = messageData.data(),
                            });
                        }
                    }
                }
                if (!messages.empty() && acceptedPartitions.size() >= minPartitions) {
                    return messages;
                }
                sendReadRequests();
                break;
            }
            default:
                break;
        }
    }

    return messages;
}

void AssertTopicNotVisible(
    NPersQueue::TTestServer& server,
    const TString& topicShortName,
    TDuration probeDuration = TDuration::Seconds(2))
{
    UNIT_ASSERT(!TryReadFirstTopicMessage(server, topicShortName, probeDuration).Defined());
}

void AssertTopicVisible(
    NPersQueue::TTestServer& server,
    Ydb::Topic::V1::TopicService::Stub& topicStub,
    const TString& topicShortName,
    const TString& expectedPayload,
    i64 expectedPartitionId = 0,
    ui64 expectedOffset = 0,
    ui32 minPartitions = 1)
{
    const auto messages = TryReadTopicMessagesViaStreamRead(
        server,
        topicStub,
        topicShortName,
        TDuration::Seconds(30),
        minPartitions);
    if (messages.empty()) {
        const auto legacyMessage = TryReadFirstTopicMessage(server, topicShortName);
        UNIT_ASSERT_C(legacyMessage.Defined(), "Expected topic to have visible messages");
        UNIT_ASSERT_VALUES_EQUAL(*legacyMessage, expectedPayload);
        if (expectedPartitionId == 0 && expectedOffset == 0) {
            return;
        }
        UNIT_FAIL("Legacy read does not expose partition/offset metadata");
    }

    bool found = false;
    for (const auto& message : messages) {
        if (message.PartitionId == expectedPartitionId
            && message.Offset == expectedOffset
            && message.Data == expectedPayload)
        {
            found = true;
            break;
        }
    }
    UNIT_ASSERT_C(
        found,
        TStringBuilder()
            << "Expected message partition=" << expectedPartitionId
            << " offset=" << expectedOffset
            << " payload=" << expectedPayload);
}

void AssertTopicEmptyAfterCancel(
    NPersQueue::TTestServer& server,
    const TString& topicShortName)
{
    AssertTopicNotVisible(server, topicShortName, TDuration::Seconds(2));
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
    server.AnnoyingClient->TestGrant(
        "/Root/.metadata",
        "topic_deferred_publication_destinations",
        subject,
        NACLib::EAccessRights::GenericRead);
}

void GrantPublicationTableWrite(NPersQueue::TTestServer& server, const TString& subject) {
    GrantPublicationTableRead(server, subject);
    if (SchemePathExists(server, "/Root/.metadata/topic_deferred_publication_destinations")) {
        server.AnnoyingClient->TestGrant(
            "/Root/.metadata",
            "topic_deferred_publication_destinations",
            subject,
            NACLib::EAccessRights::GenericWrite);
    }
}

void InsertDestinationRow(
    NPersQueue::TTestServer& server,
    const TString& authTicket,
    ui64 intPublicationId,
    const TString& path)
{
    GrantPublicationTableWrite(server, authTicket);

    NYdb::NTable::TTableClient client(
        server.GetDriver(),
        NYdb::NTable::TClientSettings().AuthToken(authTicket));

    NYdb::TParamsBuilder params;
    params
        .AddParam("$int_publication_id").Uint64(intPublicationId).Build()
        .AddParam("$path").Utf8(path).Build()
        .AddParam("$destination_blob").String(TString()).Build();

    const auto query = TStringBuilder()
        << "DECLARE $int_publication_id AS Uint64; "
        << "DECLARE $path AS Text; "
        << "DECLARE $destination_blob AS String; "
        << "UPSERT INTO `" << DestinationsTableRelativePath << "` ("
        << "`int_publication_id`, `path`, `destination_blob`) "
        << "VALUES ($int_publication_id, $path, $destination_blob);";

    const auto status = client.RetryOperationSync([&](NYdb::NTable::TSession session) {
        return session.ExecuteDataQuery(
            query,
            NYdb::NTable::TTxControl::BeginTx().CommitTx(),
            params.Build()).GetValueSync();
    });
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
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
        << "DECLARE $ext AS Text; "
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
    NPersQueue::TTestServer& server,
    const TString& authTicket,
    ui64 intPublicationId,
    ui64 expectedCount)
{
    GrantPublicationTableRead(server, authTicket);

    NYdb::NTable::TTableClient client(
        server.GetDriver(),
        NYdb::NTable::TClientSettings().AuthToken(authTicket));

    NYdb::TParamsBuilder params;
    params.AddParam("$int_publication_id").Uint64(intPublicationId).Build();

    const auto query = TStringBuilder()
        << "DECLARE $int_publication_id AS Uint64; "
        << "SELECT COUNT(*) AS `count` "
        << "FROM `" << DestinationsTableRelativePath << "` "
        << "WHERE int_publication_id = $int_publication_id;";

    const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
    TMaybe<ui64> count;
    TMaybe<NYdb::TStatus> status;
    while (TInstant::Now() < deadline) {
        count = Nothing();
        status = client.RetryOperationSync([&](NYdb::NTable::TSession session) {
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
        if (status->IsSuccess() && count.Defined()) {
            UNIT_ASSERT_VALUES_EQUAL(*count, expectedCount);
            return;
        }
        if (status->GetStatus() != NYdb::EStatus::SCHEME_ERROR) {
            break;
        }
        TestSleep(*server.CleverServer->GetRuntime(), TDuration::MilliSeconds(100));
    }
    UNIT_ASSERT(status.Defined());
    UNIT_ASSERT_C(status->IsSuccess(), status->GetIssues().ToString());
    UNIT_ASSERT(count.Defined());
    UNIT_ASSERT_VALUES_EQUAL(*count, expectedCount);
}

TString ReadDestinationBlob(
    NPersQueue::TTestServer& server,
    const TString& authTicket,
    ui64 intPublicationId,
    const TString& path)
{
    GrantPublicationTableRead(server, authTicket);

    NYdb::NTable::TTableClient client(
        server.GetDriver(),
        NYdb::NTable::TClientSettings().AuthToken(authTicket));

    NYdb::TParamsBuilder params;
    params
        .AddParam("$int_publication_id").Uint64(intPublicationId).Build()
        .AddParam("$path").Utf8(path).Build();

    const auto query = TStringBuilder()
        << "DECLARE $int_publication_id AS Uint64; "
        << "DECLARE $path AS Text; "
        << "SELECT destination_blob "
        << "FROM `" << DestinationsTableRelativePath << "` "
        << "WHERE int_publication_id = $int_publication_id AND path = $path;";

    TMaybe<TString> blob;
    const auto status = client.RetryOperationSync([&](NYdb::NTable::TSession session) {
        auto result = session.ExecuteDataQuery(
            query,
            NYdb::NTable::TTxControl::BeginTx().CommitTx(),
            params.Build()).GetValueSync();
        if (result.IsSuccess() && !result.GetResultSets().empty()) {
            NYdb::TResultSetParser parser(result.GetResultSet(0));
            if (parser.TryNextRow()) {
                blob = parser.ColumnParser("destination_blob").GetString();
            }
        }
        return result;
    });
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    UNIT_ASSERT(blob.Defined());
    return *blob;
}

ui64 CountPublications(NPersQueue::TTestServer& server, const TString& authTicket) {
    GrantPublicationTableRead(server, authTicket);

    NYdb::NTable::TTableClient client(
        server.GetDriver(),
        NYdb::NTable::TClientSettings().AuthToken(authTicket));

    const auto query = TStringBuilder()
        << "SELECT COUNT(*) AS `count` FROM `" << PublicationsTableRelativePath << "`;";

    TMaybe<ui64> count;
    const auto status = client.RetryOperationSync([&](NYdb::NTable::TSession session) {
        auto result = session.ExecuteDataQuery(
            query,
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
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
    return *count;
}

template <typename TResponse>
Ydb::StatusIds::StatusCode RunDeferredPublishQueryActor(
    NPersQueue::TTestServer& server,
    const NActors::TActorId& edgeActor,
    NActors::IActor* queryActor)
{
    auto* runtime = server.CleverServer->GetRuntime();
    constexpr ui32 nodeIdx = 0;
    const ui32 userPoolId = runtime->GetAppData(nodeIdx).UserPoolId;
    runtime->Register(queryActor, nodeIdx, userPoolId, TMailboxType::Simple, 0, edgeActor);
    runtime->DispatchEvents();

    const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
    while (TInstant::Now() < deadline) {
        runtime->DispatchEvents();
        if (auto ev = runtime->GrabEdgeEvent<TResponse>(edgeActor, TDuration::Zero())) {
            return ev->Get()->Status;
        }
        TestSleep(*runtime, TDuration::MilliSeconds(10));
    }
    UNIT_FAIL("Missing deferred publish query response");
    return Ydb::StatusIds::INTERNAL_ERROR;
}

Ydb::StatusIds::StatusCode CallUpsertDestination(
    NPersQueue::TTestServer& server,
    const TString& database,
    ui64 intPublicationId,
    const TString& path,
    const TString& destinationBlob)
{
    auto* runtime = server.CleverServer->GetRuntime();
    const NActors::TActorId edgeActor = runtime->AllocateEdgeActor();
    return RunDeferredPublishQueryActor<NPQ::NDeferredPublish::TEvUpsertDestinationResponse>(
        server,
        edgeActor,
        NPQ::NDeferredPublish::CreateUpsertDestinationQueryActor(
            edgeActor, database, intPublicationId, path, destinationBlob));
}

Ydb::StatusIds::StatusCode CallDeletePublication(
    NPersQueue::TTestServer& server,
    const TString& database,
    ui64 intPublicationId)
{
    auto* runtime = server.CleverServer->GetRuntime();
    const NActors::TActorId edgeActor = runtime->AllocateEdgeActor();
    return RunDeferredPublishQueryActor<NPQ::NDeferredPublish::TEvDeletePublicationResponse>(
        server,
        edgeActor,
        NPQ::NDeferredPublish::CreateDeletePublicationQueryActor(
            edgeActor, database, intPublicationId));
}

NPersQueue::TTestServer MakeServerWithDeferredPublishEnabled(
    bool forbidRequestsToStaticNodesWithoutDatabase = true)
{
    auto settings = NKikimr::NPersQueueTests::PQSettings()
        .SetEnableTopicDeferredPublish(true);
    settings.PQConfig.SetCheckACL(false);
    settings.FeatureFlags.SetForbidRequestsToStaticNodesWithoutDatabase(
        forbidRequestsToStaticNodesWithoutDatabase);
    return NPersQueue::TTestServer(settings);
}

std::unique_ptr<Ydb::Topic::V1::TopicService::Stub> MakeTopicServiceStub(
    const NPersQueue::TTestServer& server)
{
    auto channel = grpc::CreateChannel(
        "localhost:" + ToString(server.GrpcPort),
        grpc::InsecureChannelCredentials());
    return Ydb::Topic::V1::TopicService::NewStub(channel);
}

TString MakeLegacyStreamWriteTopicPath(const TString& topicShortName) {
    return "/Root/PQ/rt3.dc1--" + topicShortName;
}

void AssertPartitionsOnSameTablet(
    NPersQueue::TTestServer& server,
    const TString& topicShortName,
    const TVector<ui32>& partitionIds)
{
    UNIT_ASSERT(!partitionIds.empty());

    const auto response = server.AnnoyingClient->Ls(MakeLegacyStreamWriteTopicPath(topicShortName));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);

    THashMap<ui32, ui64> tabletByPartition;
    for (const auto& partition : response->Record.GetPathDescription().GetPersQueueGroup().GetPartitions()) {
        tabletByPartition[partition.GetPartitionId()] = partition.GetTabletId();
    }

    UNIT_ASSERT(tabletByPartition.contains(partitionIds.front()));
    const ui64 expectedTabletId = tabletByPartition.at(partitionIds.front());
    for (const ui32 partitionId : partitionIds) {
        UNIT_ASSERT(tabletByPartition.contains(partitionId));
        UNIT_ASSERT_VALUES_EQUAL(tabletByPartition.at(partitionId), expectedTabletId);
    }
}

void CreateLegacyStreamWriteTopic(NPersQueue::TTestServer& server, const TString& topicShortName, ui32 partitions = 2) {
    const TString fullName = "rt3.dc1--" + topicShortName;
    auto pqClient = NYdb::NPersQueue::TPersQueueClient(*server.AnnoyingClient->GetDriver());
    auto settings = NYdb::NPersQueue::TCreateTopicSettings()
        .PartitionsCount(partitions)
        .PartitionsPerTablet(Max(partitions, 2u));
    settings.ReadRules({NYdb::NPersQueue::TReadRuleSettings{}.ConsumerName("user")});

    TString path = fullName;
    if (!path.StartsWith("/Root")) {
        path = TStringBuilder() << "/Root/PQ/" << fullName;
    }

    auto result = pqClient.CreateTopic(path, settings);
    result.Wait();
    UNIT_ASSERT_C(result.GetValue().IsSuccess(), result.GetValue().GetIssues().ToString());
    server.AnnoyingClient->AddTopic(fullName);

    if (partitions >= 2) {
        TVector<ui32> partitionIds(Reserve(partitions));
        for (ui32 partitionId = 0; partitionId < partitions; ++partitionId) {
            partitionIds.push_back(partitionId);
        }
        AssertPartitionsOnSameTablet(server, topicShortName, partitionIds);
    }
}

void AssertStreamWriteSuccess(const Ydb::Topic::StreamWriteMessage::FromServer& response) {
    UNIT_ASSERT_VALUES_EQUAL(response.status(), Ydb::StatusIds::SUCCESS);
}

void AssertStreamWriteFailed(
    const Ydb::Topic::StreamWriteMessage::FromServer& response,
    Ydb::StatusIds::StatusCode expectedStatus,
    const TMaybe<TString>& expectedMessage = Nothing())
{
    UNIT_ASSERT_VALUES_EQUAL(response.status(), expectedStatus);
    if (expectedMessage) {
        UNIT_ASSERT_GT(response.issues_size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response.issues(0).message(), *expectedMessage);
    }
}

void InitStreamWriteSession(
    grpc::ClientReaderWriter<Ydb::Topic::StreamWriteMessage::FromClient, Ydb::Topic::StreamWriteMessage::FromServer>& stream,
    const TString& topicPath,
    const TString& producerId,
    i64 partitionId)
{
    Ydb::Topic::StreamWriteMessage::FromClient req;
    Ydb::Topic::StreamWriteMessage::FromServer resp;

    req.mutable_init_request()->set_path(topicPath);
    req.mutable_init_request()->set_producer_id(producerId);
    req.mutable_init_request()->set_partition_id(partitionId);

    UNIT_ASSERT(stream.Write(req));
    UNIT_ASSERT(stream.Read(&resp));
    AssertStreamWriteSuccess(resp);
    UNIT_ASSERT_VALUES_EQUAL(resp.server_message_case(), Ydb::Topic::StreamWriteMessage::FromServer::kInitResponse);
}

Ydb::Topic::StreamWriteMessage::FromClient MakeStreamWriteRequest(
    ui64 seqNo,
    const TString& data,
    const TMaybe<std::pair<ui64, TMaybe<TString>>>& deferredPublish = Nothing(),
    const TMaybe<std::pair<TString, TString>>& tx = Nothing())
{
    Ydb::Topic::StreamWriteMessage::FromClient req;
    auto* write = req.mutable_write_request();
    write->set_codec(Ydb::Topic::CODEC_RAW);

    if (deferredPublish) {
        auto* deferred = write->mutable_deferred_publish();
        deferred->set_int_publication_id(deferredPublish->first);
        if (deferredPublish->second) {
            deferred->set_ext_publication_id(*deferredPublish->second);
        }
    }
    if (tx) {
        write->mutable_tx()->set_session(tx->first);
        write->mutable_tx()->set_id(tx->second);
    }

    auto* msg = write->add_messages();
    msg->set_seq_no(seqNo);
    msg->set_data(data);
    msg->set_uncompressed_size(data.size());
    *msg->mutable_created_at() = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(TInstant::Now().MilliSeconds());
    return req;
}

std::pair<ui64, TMaybe<TString>> DeferredPublishOmitExt(ui64 intPublicationId) {
    return {intPublicationId, Nothing()};
}

std::pair<ui64, TMaybe<TString>> DeferredPublishWithExt(ui64 intPublicationId, const TString& extPublicationId) {
    return {intPublicationId, MakeMaybe(extPublicationId)};
}

void WriteAndExpectWriteResponse(
    grpc::ClientReaderWriter<Ydb::Topic::StreamWriteMessage::FromClient, Ydb::Topic::StreamWriteMessage::FromServer>& stream,
    Ydb::Topic::StreamWriteMessage::FromClient req)
{
    Ydb::Topic::StreamWriteMessage::FromServer resp;
    UNIT_ASSERT(stream.Write(req));
    UNIT_ASSERT(stream.Read(&resp));
    AssertStreamWriteSuccess(resp);
    UNIT_ASSERT_VALUES_EQUAL(resp.server_message_case(), Ydb::Topic::StreamWriteMessage::FromServer::kWriteResponse);
}

void WriteAndExpectFailure(
    grpc::ClientReaderWriter<Ydb::Topic::StreamWriteMessage::FromClient, Ydb::Topic::StreamWriteMessage::FromServer>& stream,
    Ydb::Topic::StreamWriteMessage::FromClient req,
    Ydb::StatusIds::StatusCode expectedStatus,
    const TMaybe<TString>& expectedMessage = Nothing())
{
    Ydb::Topic::StreamWriteMessage::FromServer resp;
    UNIT_ASSERT(stream.Write(req));
    UNIT_ASSERT(stream.Read(&resp));
    AssertStreamWriteFailed(resp, expectedStatus, expectedMessage);
}

struct TStreamWriteSession {
    grpc::ClientContext Context;
    std::unique_ptr<grpc::ClientReaderWriter<Ydb::Topic::StreamWriteMessage::FromClient, Ydb::Topic::StreamWriteMessage::FromServer>> Stream;

    static std::unique_ptr<TStreamWriteSession> Open(
        Ydb::Topic::V1::TopicService::Stub& topicStub,
        const TString& topicPath,
        const TString& producerId,
        i64 partitionId = 0)
    {
        auto session = std::make_unique<TStreamWriteSession>();
        FillClientContext(session->Context, "/Root");
        session->Stream = topicStub.StreamWrite(&session->Context);
        UNIT_ASSERT(session->Stream);
        InitStreamWriteSession(*session->Stream, topicPath, producerId, partitionId);
        return session;
    }
};

struct TDeferredStreamWriteFixture {
    NPersQueue::TTestServer Server;
    std::unique_ptr<Ydb::Topic::DeferredPublish::V1::TopicDeferredPublishService::Stub> DeferredStub;
    std::unique_ptr<Ydb::Topic::V1::TopicService::Stub> TopicStub;
    TString TopicShortName;
    ui64 IntPublicationId = 0;
    TString ExtPublicationId;

    static TDeferredStreamWriteFixture Enabled(
        const TString& topicShortName = "deferred-stream-topic",
        const TString& extPublicationId = "ext-stream-write")
    {
        TDeferredStreamWriteFixture fixture;
        fixture.TopicShortName = topicShortName;
        fixture.ExtPublicationId = extPublicationId;
        fixture.Server = MakeServerWithDeferredPublishEnabled();
        fixture.Server.AnnoyingClient->GrantConnect("root@builtin");
        CreateLegacyStreamWriteTopic(fixture.Server, topicShortName, 2);
        fixture.DeferredStub = MakeStub(fixture.Server);
        fixture.TopicStub = MakeTopicServiceStub(fixture.Server);
        fixture.IntPublicationId = BeginPublicationIntId(
            CallBeginPublication(*fixture.DeferredStub, "/Root", extPublicationId));
        return fixture;
    }

    std::unique_ptr<TStreamWriteSession> OpenWriteStream(const TString& producerId, i64 partitionId = 0) const {
        return TStreamWriteSession::Open(*TopicStub, TopicShortName, producerId, partitionId);
    }
};
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
        TestSleep(runtime, TDuration::MilliSeconds(100));
    }
    UNIT_FAIL(TStringBuilder() << "Database " << path << " is not RUNNING, last status:\n" << status.DebugString());
}

void SendBeginPublicationToRegistry(
    NActors::TTestActorRuntime& runtime,
    ui32 nodeIdx,
    const NActors::TActorId& edgeActor,
    const TString& extPublicationId)
{
    auto* event = new NPQ::NDeferredPublish::TEvBeginPublicationRequest;
    event->Database = "/Root";
    event->ExtPublicationId = extPublicationId;
    event->CreatedBy = "root@builtin";
    runtime.Send(
        NPQ::NDeferredPublish::MakeDeferredPublishRegistryActorId(),
        edgeActor,
        event,
        nodeIdx);
}

TVector<Ydb::StatusIds::StatusCode> WaitRegistryBeginPublicationResponses(
    NActors::TTestActorRuntime& runtime,
    const TVector<NActors::TActorId>& edgeActors,
    TMaybe<NYql::TIssues>* overloadedIssues = nullptr)
{
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(60);
    TVector<TMaybe<Ydb::StatusIds::StatusCode>> responses(edgeActors.size());
    TVector<TMaybe<NYql::TIssues>> responseIssues(edgeActors.size());

    while (TInstant::Now() < deadline) {
        runtime.DispatchEvents();
        bool allReceived = true;
        for (ui32 i = 0; i < edgeActors.size(); ++i) {
            if (responses[i].Defined()) {
                continue;
            }
            auto ev = runtime.GrabEdgeEvent<NPQ::NDeferredPublish::TEvBeginPublicationResponse>(
                edgeActors[i], TDuration::Zero());
            if (!ev) {
                allReceived = false;
                continue;
            }
            responses[i] = ev->Get()->Status;
            responseIssues[i] = ev->Get()->Issues;
        }
        if (allReceived) {
            break;
        }
        TestSleep(runtime, TDuration::MilliSeconds(10));
    }

    TVector<Ydb::StatusIds::StatusCode> result;
    result.reserve(edgeActors.size());
    for (ui32 i = 0; i < edgeActors.size(); ++i) {
        UNIT_ASSERT_C(responses[i].Defined(), "Missing registry response for request index " << i);
        if (overloadedIssues && *responses[i] == Ydb::StatusIds::OVERLOADED) {
            *overloadedIssues = responseIssues[i];
        }
        result.push_back(*responses[i]);
    }
    return result;
}

void PoisonDeferredPublishRegistry(NPersQueue::TTestServer& server, ui32 nodeIdx = 0) {
    auto* runtime = server.CleverServer->GetRuntime();
    const NActors::TActorId edgeActor = runtime->AllocateEdgeActor(nodeIdx);

    runtime->Send(
        NPQ::NDeferredPublish::MakeDeferredPublishRegistryActorId(),
        edgeActor,
        new NActors::TEvents::TEvPoison(),
        nodeIdx);
}

void RestartDeferredPublishRegistry(NPersQueue::TTestServer& server, ui32 nodeIdx = 0) {
    auto* runtime = server.CleverServer->GetRuntime();
    const NActors::TActorId edgeActor = runtime->AllocateEdgeActor(nodeIdx);

    runtime->Send(
        NPQ::NDeferredPublish::MakeDeferredPublishRegistryActorId(),
        edgeActor,
        new NActors::TEvents::TEvPoison(),
        nodeIdx);
    runtime->DispatchEvents();

    const ui32 userPoolId = runtime->GetAppData(nodeIdx).UserPoolId;
    NActors::IActor* registry = NPQ::NDeferredPublish::CreateDeferredPublishRegistryActor();
    const NActors::TActorId registryId = runtime->Register(registry, nodeIdx, userPoolId);
    runtime->RegisterService(
        NPQ::NDeferredPublish::MakeDeferredPublishRegistryActorId(),
        registryId,
        nodeIdx);
    runtime->DispatchEvents();
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

    TestSleep(runtime, TDuration::Seconds(1));
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

Y_UNIT_TEST(BeginPublicationRejectsTooLongExtPublicationId) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    const TString tooLongExtPublicationId(MaxDeferredPublishStringLength + 1, 'a');
    const auto outcome = CallBeginPublication(*MakeStub(server), "/Root", tooLongExtPublicationId);
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::BAD_REQUEST);
    UNIT_ASSERT_GT(outcome.Operation.issues_size(), 0);
    UNIT_ASSERT(outcome.Operation.issues(0).message().Contains("ext_publication_id's length is not <= 2048"));
}

Y_UNIT_TEST(BeginPublicationRejectsTooLongWriterIdentity) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    grpc::ClientContext context;
    FillClientContext(context, "/Root");
    Ydb::Topic::DeferredPublish::BeginPublicationRequest request;
    request.set_ext_publication_id("pub-with-long-writer");
    request.set_writer_identity(TString(MaxDeferredPublishStringLength + 1, 'b'));
    Ydb::Topic::DeferredPublish::BeginPublicationResponse response;
    UNIT_ASSERT(MakeStub(server)->BeginPublication(&context, request, &response).ok());
    UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::BAD_REQUEST);
    UNIT_ASSERT_GT(response.operation().issues_size(), 0);
    UNIT_ASSERT(response.operation().issues(0).message().Contains("writer_identity's length is not <= 2048"));
}

Y_UNIT_TEST(BeginPublicationRejectsEmptyDatabaseAtGrpcProxy) {
    auto server = MakeServerWithDeferredPublishEnabled(true);
    server.AnnoyingClient->GrantConnect("root@builtin");

    const auto outcome = CallBeginPublicationWithEmptyDatabaseHeader(*MakeStub(server));
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::BAD_REQUEST);
    UNIT_ASSERT_GT(outcome.Operation.issues_size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.issues(0).message(), "Requests without specified database are not allowed");
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
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::BAD_REQUEST);
    UNIT_ASSERT_GT(outcome.Operation.issues_size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.issues(0).message(), "Requests without specified database are not allowed");
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

Y_UNIT_TEST(BeginPublicationRejectsWhenPendingQueueIsFull) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto* runtime = server.CleverServer->GetRuntime();
    constexpr ui32 nodeIdx = 0;
    constexpr ui32 totalRequests = MaxDeferredPublishPendingQueueSize + 1;

    TVector<NActors::TActorId> edgeActors;
    edgeActors.reserve(totalRequests);
    for (ui32 i = 0; i < totalRequests; ++i) {
        edgeActors.push_back(runtime->AllocateEdgeActor(nodeIdx));
    }

    for (ui32 i = 0; i < totalRequests; ++i) {
        SendBeginPublicationToRegistry(
            *runtime,
            nodeIdx,
            edgeActors[i],
            TStringBuilder() << "pending-queue-" << i);
    }

    TMaybe<NYql::TIssues> overloadedIssues;
    const auto responses = WaitRegistryBeginPublicationResponses(
        *runtime, edgeActors, &overloadedIssues);

    ui32 overloadedCount = 0;
    ui32 successCount = 0;
    for (const auto status : responses) {
        if (status == Ydb::StatusIds::OVERLOADED) {
            ++overloadedCount;
        } else if (status == Ydb::StatusIds::SUCCESS) {
            ++successCount;
        } else {
            UNIT_FAIL("Unexpected BeginPublication registry status: " << status);
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(overloadedCount, 1u);
    UNIT_ASSERT_VALUES_EQUAL(successCount, MaxDeferredPublishPendingQueueSize);
    UNIT_ASSERT_VALUES_EQUAL(responses.back(), Ydb::StatusIds::OVERLOADED);
    UNIT_ASSERT(overloadedIssues.Defined());
    UNIT_ASSERT_STRING_CONTAINS(
        overloadedIssues->ToString(),
        TStringBuilder()
            << "Deferred publish registry pending queue is full (limit "
            << MaxDeferredPublishPendingQueueSize << ")");
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

Y_UNIT_TEST(BeginPublicationRejectsMissingAuth) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    const auto outcome = CallBeginPublication(
        *MakeStub(server),
        "/Root",
        "no-auth-pub",
        Nothing(),
        true,
        TString());
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::UNAUTHORIZED);
    UNIT_ASSERT(outcome.Operation.ready());
    UNIT_ASSERT_GT(outcome.Operation.issues_size(), 0);
}

Y_UNIT_TEST(ListPublicationsReturnsOnlyCallerPublications) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");
    server.AnnoyingClient->GrantConnect(TString(WriterBuiltinUser));

    auto rootStub = MakeStub(server);
    auto writerStub = MakeStub(server);

    const ui64 rootIntId = BeginPublicationIntId(
        CallBeginPublication(*rootStub, "/Root", "pub-root", Nothing(), true, "root@builtin"));
    BeginPublicationIntId(CallBeginPublication(
        *writerStub,
        "/Root",
        "pub-writer",
        Nothing(),
        true,
        TString(WriterBuiltinUser)));

    {
        const auto outcome = CallListPublications(*rootStub, "/Root");
        UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::SUCCESS);
        Ydb::Topic::DeferredPublish::ListPublicationsResult result;
        UNIT_ASSERT(outcome.Operation.result().UnpackTo(&result));
        UNIT_ASSERT_VALUES_EQUAL(result.publications_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.publications(0).int_publication_id(), rootIntId);
        UNIT_ASSERT_VALUES_EQUAL(result.publications(0).ext_publication_id(), "pub-root");
    }

    {
        const auto outcome = CallListPublications(
            *writerStub, "/Root", Nothing(), TString(WriterBuiltinUser));
        UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::SUCCESS);
        Ydb::Topic::DeferredPublish::ListPublicationsResult result;
        UNIT_ASSERT(outcome.Operation.result().UnpackTo(&result));
        UNIT_ASSERT_VALUES_EQUAL(result.publications_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.publications(0).ext_publication_id(), "pub-writer");
    }
}

Y_UNIT_TEST(DescribePublicationRejectsNonOwner) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");
    server.AnnoyingClient->GrantConnect(TString(WriterBuiltinUser));

    auto rootStub = MakeStub(server);
    auto writerStub = MakeStub(server);

    const ui64 rootIntId = BeginPublicationIntId(
        CallBeginPublication(*rootStub, "/Root", "describe-owner-pub"));

    const auto outcome = CallDescribePublication(
        *writerStub, "/Root", rootIntId, TString(WriterBuiltinUser));
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::UNAUTHORIZED);
    UNIT_ASSERT(outcome.Operation.ready());
}

Y_UNIT_TEST(PublishRejectsNonOwner) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");
    server.AnnoyingClient->GrantConnect(TString(WriterBuiltinUser));

    auto rootStub = MakeStub(server);
    auto writerStub = MakeStub(server);

    const ui64 rootIntId = BeginPublicationIntId(
        CallBeginPublication(*rootStub, "/Root", "publish-owner-pub"));

    const auto outcome = CallPublish(
        *writerStub, "/Root", rootIntId, TString(WriterBuiltinUser));
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::UNAUTHORIZED);
    UNIT_ASSERT(outcome.Operation.ready());
}

Y_UNIT_TEST(CancelPublicationRejectsNonOwner) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");
    server.AnnoyingClient->GrantConnect(TString(WriterBuiltinUser));

    auto rootStub = MakeStub(server);
    auto writerStub = MakeStub(server);

    const ui64 rootIntId = BeginPublicationIntId(
        CallBeginPublication(*rootStub, "/Root", "cancel-owner-pub"));

    const auto outcome = CallCancelPublication(
        *writerStub, "/Root", rootIntId, TString(WriterBuiltinUser));
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::UNAUTHORIZED);
    UNIT_ASSERT(outcome.Operation.ready());
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

Y_UNIT_TEST(BeginPublicationSucceedsAfterRegistryRestart) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    BeginPublicationIntId(CallBeginPublication(*stub, "/Root", "warmup-before-registry-restart"));

    UNIT_ASSERT(SchemePathExists(server, "/Root/.metadata/topic_deferred_publications"));
    UNIT_ASSERT(SchemePathExists(server, "/Root/.metadata/topic_deferred_publication_destinations"));

    RestartDeferredPublishRegistry(server);

    BeginPublicationIntId(CallBeginPublication(*stub, "/Root", "after-registry-restart"));
}

Y_UNIT_TEST(RegistrySurvivesLateTablesCreationFinishedDuringShutdown) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto* runtime = server.CleverServer->GetRuntime();
    constexpr ui32 nodeIdx = 0;

    BeginPublicationIntId(CallBeginPublication(*MakeStub(server), "/Root", "warmup-late-tables-finish"));

    const NActors::TActorId edgeActor = runtime->AllocateEdgeActor(nodeIdx);
    SendBeginPublicationToRegistry(*runtime, nodeIdx, edgeActor, "in-flight-during-shutdown");
    PoisonDeferredPublishRegistry(server, nodeIdx);

    auto* lateFinish = new NPQ::NDeferredPublish::TEvTablesCreationFinished;
    lateFinish->Database = "/Root";
    lateFinish->Success = true;
    runtime->Send(
        NPQ::NDeferredPublish::MakeDeferredPublishRegistryActorId(),
        edgeActor,
        lateFinish,
        nodeIdx);

    const TInstant deadline = TInstant::Now() + TDuration::Seconds(60);
    while (TInstant::Now() < deadline) {
        runtime->DispatchEvents();
        if (auto ev = runtime->GrabEdgeEvent<NPQ::NDeferredPublish::TEvBeginPublicationResponse>(
                edgeActor, TDuration::Zero())) {
            // Insert started before poison; in-flight work is drained with its real outcome.
            UNIT_ASSERT(
                ev->Get()->Status == Ydb::StatusIds::SUCCESS
                || ev->Get()->Status == Ydb::StatusIds::ABORTED);
            return;
        }
        TestSleep(*runtime, TDuration::MilliSeconds(10));
    }
    UNIT_FAIL("Missing BeginPublication response after late TEvTablesCreationFinished");
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
    // Publish/Cancel are implemented in ticket 09; this test is kept for historical name only.
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    const ui64 intPublicationId = BeginPublicationIntId(
        CallBeginPublication(*stub, "/Root", "publish-stub-check"));

    {
        const auto outcome = CallPublish(*stub, "/Root", intPublicationId);
        UNIT_ASSERT(outcome.RpcStatus.ok());
        UNIT_ASSERT(outcome.Operation.ready());
        UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::ABORTED);
    }
}

Y_UNIT_TEST(ListPublicationsDisabledByDefault) {
    NPersQueue::TTestServer server;
    server.AnnoyingClient->GrantConnect("root@builtin");

    const auto outcome = CallListPublications(*MakeStub(server), "/Root");
    UNIT_ASSERT(outcome.RpcStatus.ok());
    AssertUnsupported(outcome.Operation, DisabledMessage);
}

Y_UNIT_TEST(DescribePublicationDisabledByDefault) {
    NPersQueue::TTestServer server;
    server.AnnoyingClient->GrantConnect("root@builtin");

    const auto outcome = CallDescribePublication(*MakeStub(server), "/Root", 1);
    UNIT_ASSERT(outcome.RpcStatus.ok());
    AssertUnsupported(outcome.Operation, DisabledMessage);
}

Y_UNIT_TEST(ListPublicationsReturnsEmptyBeforeBegin) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    const auto outcome = CallListPublications(*MakeStub(server), "/Root");
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT(outcome.Operation.ready());

    Ydb::Topic::DeferredPublish::ListPublicationsResult result;
    UNIT_ASSERT(outcome.Operation.result().UnpackTo(&result));
    UNIT_ASSERT_VALUES_EQUAL(result.publications_size(), 0);
}

Y_UNIT_TEST(ListPublicationsReturnsActivePublications) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    const ui64 firstIntId = BeginPublicationIntId(
        CallBeginPublication(*stub, "/Root", "pub-a", "writer-a"));
    const ui64 secondIntId = BeginPublicationIntId(
        CallBeginPublication(*stub, "/Root", "pub-b", "writer-b"));

    const auto outcome = CallListPublications(*stub, "/Root");
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::SUCCESS);

    Ydb::Topic::DeferredPublish::ListPublicationsResult result;
    UNIT_ASSERT(outcome.Operation.result().UnpackTo(&result));
    UNIT_ASSERT_VALUES_EQUAL(result.publications_size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(result.publications(0).int_publication_id(), firstIntId);
    UNIT_ASSERT_VALUES_EQUAL(result.publications(0).ext_publication_id(), "pub-a");
    UNIT_ASSERT_VALUES_EQUAL(result.publications(0).writer_identity(), "writer-a");
    UNIT_ASSERT_VALUES_EQUAL(result.publications(1).int_publication_id(), secondIntId);
    UNIT_ASSERT_VALUES_EQUAL(result.publications(1).ext_publication_id(), "pub-b");
    UNIT_ASSERT_VALUES_EQUAL(result.publications(1).writer_identity(), "writer-b");
}

Y_UNIT_TEST(ListPublicationsFiltersByWriterIdentity) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    const ui64 targetIntId = BeginPublicationIntId(
        CallBeginPublication(*stub, "/Root", "pub-filtered", "writer-target"));
    BeginPublicationIntId(CallBeginPublication(*stub, "/Root", "pub-other", "writer-other"));

    const auto outcome = CallListPublications(*stub, "/Root", "writer-target");
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::SUCCESS);

    Ydb::Topic::DeferredPublish::ListPublicationsResult result;
    UNIT_ASSERT(outcome.Operation.result().UnpackTo(&result));
    UNIT_ASSERT_VALUES_EQUAL(result.publications_size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(result.publications(0).int_publication_id(), targetIntId);
    UNIT_ASSERT_VALUES_EQUAL(result.publications(0).ext_publication_id(), "pub-filtered");
}

Y_UNIT_TEST(DescribePublicationReturnsNotFoundForUnknownInt) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    BeginPublicationIntId(CallBeginPublication(*stub, "/Root", "warmup-describe"));

    for (const ui64 intPublicationId : {0u, 999999u}) {
        const auto outcome = CallDescribePublication(*stub, "/Root", intPublicationId);
        UNIT_ASSERT(outcome.RpcStatus.ok());
        UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT(outcome.Operation.ready());
    }
}

Y_UNIT_TEST(DescribePublicationReturnsMetadataWithoutDestinations) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    const ui64 intPublicationId = BeginPublicationIntId(
        CallBeginPublication(*stub, "/Root", "describe-no-dest", "writer-1"));

    const auto outcome = CallDescribePublication(*stub, "/Root", intPublicationId);
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT(outcome.Operation.ready());

    Ydb::Topic::DeferredPublish::DescribePublicationResult result;
    UNIT_ASSERT(outcome.Operation.result().UnpackTo(&result));
    UNIT_ASSERT_VALUES_EQUAL(result.ext_publication_id(), "describe-no-dest");
    UNIT_ASSERT_VALUES_EQUAL(result.writer_identity(), "writer-1");
    UNIT_ASSERT_VALUES_EQUAL(result.created_by(), "root@builtin");
    UNIT_ASSERT_GT(result.created_at().seconds(), 0);
    UNIT_ASSERT_VALUES_EQUAL(result.destinations_size(), 0);
}

Y_UNIT_TEST(DescribePublicationReturnsDestinations) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    const ui64 intPublicationId = BeginPublicationIntId(
        CallBeginPublication(*stub, "/Root", "describe-with-dest", "writer-1"));
    InsertDestinationRow(server, "root@builtin", intPublicationId, "/Root/topic-a");

    const auto outcome = CallDescribePublication(*stub, "/Root", intPublicationId);
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::SUCCESS);

    Ydb::Topic::DeferredPublish::DescribePublicationResult result;
    UNIT_ASSERT(outcome.Operation.result().UnpackTo(&result));
    UNIT_ASSERT_VALUES_EQUAL(result.destinations_size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(result.destinations(0).topic_path(), "/Root/topic-a");
    UNIT_ASSERT_VALUES_EQUAL(result.destinations(0).partition_ids_size(), 0);
}

Y_UNIT_TEST(UpsertDestinationInsertsRow) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    const ui64 intPublicationId = BeginPublicationIntId(
        CallBeginPublication(*stub, "/Root", "upsert-insert"));

    const auto blob = NPQ::NDeferredPublish::SerializeDestinationBlob(
        NPQ::NDeferredPublish::MakeDestinationBlob(1, 100));
    UNIT_ASSERT_VALUES_EQUAL(
        CallUpsertDestination(server, "/Root", intPublicationId, "/Root/topic-a", blob),
        Ydb::StatusIds::SUCCESS);
    AssertDestinationRowCount(server, "root@builtin", intPublicationId, 1);
}

Y_UNIT_TEST(UpsertDestinationReplacesBlob) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    const ui64 intPublicationId = BeginPublicationIntId(
        CallBeginPublication(*stub, "/Root", "upsert-replace"));

    const TString path = "/Root/topic-a";
    const auto firstBlob = NPQ::NDeferredPublish::SerializeDestinationBlob(
        NPQ::NDeferredPublish::MakeDestinationBlob(1, 100));
    const auto secondBlob = NPQ::NDeferredPublish::SerializeDestinationBlob(
        NPQ::NDeferredPublish::MakeDestinationBlob(2, 200));

    UNIT_ASSERT_VALUES_EQUAL(
        CallUpsertDestination(server, "/Root", intPublicationId, path, firstBlob),
        Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(
        CallUpsertDestination(server, "/Root", intPublicationId, path, secondBlob),
        Ydb::StatusIds::SUCCESS);

    AssertDestinationRowCount(server, "root@builtin", intPublicationId, 1);
    UNIT_ASSERT_VALUES_EQUAL(ReadDestinationBlob(server, "root@builtin", intPublicationId, path), secondBlob);
}

Y_UNIT_TEST(UpsertDestinationNotFoundForUnknownInt) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    BeginPublicationIntId(CallBeginPublication(*stub, "/Root", "upsert-warmup"));

    const auto blob = NPQ::NDeferredPublish::SerializeDestinationBlob(
        NPQ::NDeferredPublish::MakeDestinationBlob(1, 100));
    UNIT_ASSERT_VALUES_EQUAL(
        CallUpsertDestination(server, "/Root", 999999, "/Root/topic-a", blob),
        Ydb::StatusIds::NOT_FOUND);
}

Y_UNIT_TEST(UpsertDestinationNotFoundBeforeBegin) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    const auto blob = NPQ::NDeferredPublish::SerializeDestinationBlob(
        NPQ::NDeferredPublish::MakeDestinationBlob(1, 100));
    UNIT_ASSERT_VALUES_EQUAL(
        CallUpsertDestination(server, "/Root", 1, "/Root/topic-a", blob),
        Ydb::StatusIds::NOT_FOUND);
}

Y_UNIT_TEST(UpsertDestinationThenDescribeShowsPath) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    const ui64 intPublicationId = BeginPublicationIntId(
        CallBeginPublication(*stub, "/Root", "upsert-describe"));

    const auto blob = NPQ::NDeferredPublish::SerializeDestinationBlob(
        NPQ::NDeferredPublish::MakeDestinationBlob(1, 100));
    UNIT_ASSERT_VALUES_EQUAL(
        CallUpsertDestination(server, "/Root", intPublicationId, "/Root/topic-a", blob),
        Ydb::StatusIds::SUCCESS);

    const auto outcome = CallDescribePublication(*stub, "/Root", intPublicationId);
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::SUCCESS);

    Ydb::Topic::DeferredPublish::DescribePublicationResult result;
    UNIT_ASSERT(outcome.Operation.result().UnpackTo(&result));
    UNIT_ASSERT_VALUES_EQUAL(result.destinations_size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(result.destinations(0).topic_path(), "/Root/topic-a");
}

Y_UNIT_TEST(DeletePublicationRemovesParentAndDestinations) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    const ui64 intPublicationId = BeginPublicationIntId(
        CallBeginPublication(*stub, "/Root", "delete-with-dest"));

    const auto blob = NPQ::NDeferredPublish::SerializeDestinationBlob(
        NPQ::NDeferredPublish::MakeDestinationBlob(1, 100));
    UNIT_ASSERT_VALUES_EQUAL(
        CallUpsertDestination(server, "/Root", intPublicationId, "/Root/topic-a", blob),
        Ydb::StatusIds::SUCCESS);

    UNIT_ASSERT_VALUES_EQUAL(
        CallDeletePublication(server, "/Root", intPublicationId),
        Ydb::StatusIds::SUCCESS);
    AssertDestinationRowCount(server, "root@builtin", intPublicationId, 0);
    UNIT_ASSERT_VALUES_EQUAL(CountPublications(server, "root@builtin"), 0u);
}

Y_UNIT_TEST(DeletePublicationWithoutDestinations) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    const ui64 intPublicationId = BeginPublicationIntId(
        CallBeginPublication(*stub, "/Root", "delete-no-dest"));

    UNIT_ASSERT_VALUES_EQUAL(
        CallDeletePublication(server, "/Root", intPublicationId),
        Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(CountPublications(server, "root@builtin"), 0u);
}

Y_UNIT_TEST(DeletePublicationNotFoundForUnknownInt) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    BeginPublicationIntId(CallBeginPublication(*stub, "/Root", "delete-warmup"));

    UNIT_ASSERT_VALUES_EQUAL(
        CallDeletePublication(server, "/Root", 999999),
        Ydb::StatusIds::NOT_FOUND);
}

Y_UNIT_TEST(DeletePublicationNotFoundOnRepeat) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    const ui64 intPublicationId = BeginPublicationIntId(
        CallBeginPublication(*stub, "/Root", "delete-repeat"));

    UNIT_ASSERT_VALUES_EQUAL(
        CallDeletePublication(server, "/Root", intPublicationId),
        Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(
        CallDeletePublication(server, "/Root", intPublicationId),
        Ydb::StatusIds::NOT_FOUND);
}

Y_UNIT_TEST(DeletePublicationThenListIsEmpty) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    const ui64 intPublicationId = BeginPublicationIntId(
        CallBeginPublication(*stub, "/Root", "delete-list"));

    UNIT_ASSERT_VALUES_EQUAL(
        CallDeletePublication(server, "/Root", intPublicationId),
        Ydb::StatusIds::SUCCESS);

    const auto outcome = CallListPublications(*stub, "/Root");
    UNIT_ASSERT(outcome.RpcStatus.ok());
    UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::SUCCESS);

    Ydb::Topic::DeferredPublish::ListPublicationsResult result;
    UNIT_ASSERT(outcome.Operation.result().UnpackTo(&result));
    UNIT_ASSERT_VALUES_EQUAL(result.publications_size(), 0);
}

Y_UNIT_TEST(DeletePublicationNotFoundBeforeBegin) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    UNIT_ASSERT_VALUES_EQUAL(
        CallDeletePublication(server, "/Root", 1),
        Ydb::StatusIds::NOT_FOUND);
}

} // Y_UNIT_TEST_SUITE

Y_UNIT_TEST_SUITE(TopicDeferredPublishStreamWrite) {

Y_UNIT_TEST(StreamWriteDeferredPublishAcksWrite) {
    auto fixture = TDeferredStreamWriteFixture::Enabled();
    auto session = fixture.OpenWriteStream("producer-deferred");

    WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
        1,
        "deferred-payload",
        DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));

    AssertDestinationRowCount(fixture.Server, "root@builtin", fixture.IntPublicationId, 1);

    NKikimrPQ::TDeferredPublishDestinationBlob blob;
    UNIT_ASSERT(NPQ::NDeferredPublish::ParseDestinationBlob(
        ReadDestinationBlob(fixture.Server, "root@builtin", fixture.IntPublicationId, fixture.TopicShortName),
        &blob));
    UNIT_ASSERT_VALUES_EQUAL(blob.PartitionsSize(), 1);
    UNIT_ASSERT(NPQ::NDeferredPublish::FindTopicPartitionDestination(blob, 0));
}

Y_UNIT_TEST(StreamWriteDeferredPublishDisabledByDefault) {
    NPersQueue::TTestServer server(
        NKikimr::NPersQueueTests::PQSettings().SetEnableTopicDeferredPublish(false));
    server.AnnoyingClient->GrantConnect("root@builtin");
    const TString topicShortName = "deferred-disabled-topic";
    CreateLegacyStreamWriteTopic(server, topicShortName, 1);

    auto topicStub = MakeTopicServiceStub(server);
    auto session = TStreamWriteSession::Open(*topicStub, topicShortName, "producer-disabled");

    WriteAndExpectFailure(
        *session->Stream,
        MakeStreamWriteRequest(1, "payload", DeferredPublishWithExt(1u, TString("ext-disabled"))),
        Ydb::StatusIds::UNSUPPORTED,
        TString(DisabledMessage));
}

Y_UNIT_TEST(StreamWriteAllowsOmitExtPublicationId) {
    auto fixture = TDeferredStreamWriteFixture::Enabled();
    auto session = fixture.OpenWriteStream("producer-omit-ext");

    WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
        1,
        "payload",
        DeferredPublishOmitExt(fixture.IntPublicationId)));
}

Y_UNIT_TEST(StreamWriteAllowsEmptyExtPublicationId) {
    auto fixture = TDeferredStreamWriteFixture::Enabled();
    auto session = fixture.OpenWriteStream("producer-empty-ext");

    WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
        1,
        "payload",
        DeferredPublishWithExt(fixture.IntPublicationId, TString())));
}

Y_UNIT_TEST(StreamWriteAllowsNonEmptyExtPublicationId) {
    auto fixture = TDeferredStreamWriteFixture::Enabled();
    auto session = fixture.OpenWriteStream("producer-nonempty-ext");

    WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
        1,
        "payload",
        DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));
}

Y_UNIT_TEST(StreamWriteRejectsTooLongExtPublicationId) {
    auto fixture = TDeferredStreamWriteFixture::Enabled();
    auto session = fixture.OpenWriteStream("producer-long-ext");

    WriteAndExpectFailure(
        *session->Stream,
        MakeStreamWriteRequest(
            1,
            "payload",
            DeferredPublishWithExt(fixture.IntPublicationId, TString(MaxDeferredPublishStringLength + 1, 'x'))),
        Ydb::StatusIds::BAD_REQUEST,
        TString("WriteRequest.deferred_publish.ext_publication_id is too long"));
}

Y_UNIT_TEST(StreamWriteFailsOnUnknownIntPublicationId) {
    auto fixture = TDeferredStreamWriteFixture::Enabled();
    auto session = fixture.OpenWriteStream("producer-unknown-int");

    Ydb::Topic::StreamWriteMessage::FromServer resp;
    UNIT_ASSERT(session->Stream->Write(MakeStreamWriteRequest(
        1,
        "payload",
        DeferredPublishWithExt(999999u, TString("missing-ext")))));
    UNIT_ASSERT(session->Stream->Read(&resp));
    UNIT_ASSERT(resp.status() != Ydb::StatusIds::SUCCESS);
}

Y_UNIT_TEST(StreamWriteDeferredThenRegularInSameSession) {
    auto fixture = TDeferredStreamWriteFixture::Enabled();
    auto session = fixture.OpenWriteStream("producer-mixed");

    WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
        1,
        "deferred-part",
        DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));
    WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(2, "regular-part"));
}

Y_UNIT_TEST(StreamWriteMergesPartitionsIntoDestinationBlob) {
    auto fixture = TDeferredStreamWriteFixture::Enabled("deferred-merge-topic", "ext-merge");

    {
        auto session = fixture.OpenWriteStream("producer-part-0", 0);
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            "partition-0",
            DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));
    }
    {
        auto session = fixture.OpenWriteStream("producer-part-1", 1);
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            "partition-1",
            DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));
    }

    AssertDestinationRowCount(fixture.Server, "root@builtin", fixture.IntPublicationId, 1);

    NKikimrPQ::TDeferredPublishDestinationBlob blob;
    UNIT_ASSERT(NPQ::NDeferredPublish::ParseDestinationBlob(
        ReadDestinationBlob(fixture.Server, "root@builtin", fixture.IntPublicationId, fixture.TopicShortName),
        &blob));
    UNIT_ASSERT_VALUES_EQUAL(blob.PartitionsSize(), 2);
    UNIT_ASSERT(NPQ::NDeferredPublish::FindTopicPartitionDestination(blob, 0));
    UNIT_ASSERT(NPQ::NDeferredPublish::FindTopicPartitionDestination(blob, 1));
}

} // Y_UNIT_TEST_SUITE

Y_UNIT_TEST_SUITE(TopicDeferredPublishFinalize) {

Y_UNIT_TEST(PublishAfterStreamWriteClearsRegistryAndMakesDataVisible) {
    auto fixture = TDeferredStreamWriteFixture::Enabled("finalize-publish-topic", "ext-publish");

    constexpr TStringBuf payload = "deferred-payload-visible";
    {
        auto session = fixture.OpenWriteStream("producer-publish");
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            TString(payload),
            DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));
    }

    const auto publishOutcome = CallPublish(*fixture.DeferredStub, "/Root", fixture.IntPublicationId);
    UNIT_ASSERT(publishOutcome.RpcStatus.ok());
    UNIT_ASSERT(publishOutcome.Operation.ready());
    UNIT_ASSERT_VALUES_EQUAL(publishOutcome.Operation.status(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(CountPublications(fixture.Server, "root@builtin"), 0u);

    const auto message = TryReadFirstTopicMessage(fixture.Server, fixture.TopicShortName);
    UNIT_ASSERT(message.Defined());
    UNIT_ASSERT_VALUES_EQUAL(*message, TString(payload));
}

Y_UNIT_TEST(PublishAfterStreamWriteToTwoPartitionsMakesDataVisible) {
    auto fixture = TDeferredStreamWriteFixture::Enabled("finalize-two-partitions-topic", "ext-two-partitions");

    constexpr TStringBuf payload0 = "deferred-payload-part-0";
    constexpr TStringBuf payload1 = "deferred-payload-part-1";
    {
        auto session = fixture.OpenWriteStream("producer-part-0", 0);
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            TString(payload0),
            DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));
    }
    {
        auto session = fixture.OpenWriteStream("producer-part-1", 1);
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            TString(payload1),
            DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));
    }

    const auto publishOutcome = CallPublish(*fixture.DeferredStub, "/Root", fixture.IntPublicationId);
    UNIT_ASSERT(publishOutcome.RpcStatus.ok());
    UNIT_ASSERT(publishOutcome.Operation.ready());
    UNIT_ASSERT_VALUES_EQUAL(publishOutcome.Operation.status(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(CountPublications(fixture.Server, "root@builtin"), 0u);

    const auto message0 = TryReadFirstTopicMessage(fixture.Server, fixture.TopicShortName, TDuration::Seconds(30), 0);
    const auto message1 = TryReadFirstTopicMessage(fixture.Server, fixture.TopicShortName, TDuration::Seconds(30), 1);
    UNIT_ASSERT(message0.Defined());
    UNIT_ASSERT(message1.Defined());
    UNIT_ASSERT_VALUES_EQUAL(*message0, TString(payload0));
    UNIT_ASSERT_VALUES_EQUAL(*message1, TString(payload1));
}

Y_UNIT_TEST(CancelAfterStreamWriteToTwoPartitionsClearsRegistryWithoutData) {
    auto fixture = TDeferredStreamWriteFixture::Enabled("finalize-cancel-two-partitions-topic", "ext-cancel-two-partitions");

    {
        auto session = fixture.OpenWriteStream("producer-cancel-0", 0);
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            "deferred-payload-cancel-0",
            DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));
    }
    {
        auto session = fixture.OpenWriteStream("producer-cancel-1", 1);
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            "deferred-payload-cancel-1",
            DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));
    }

    const auto cancelOutcome = CallCancelPublication(*fixture.DeferredStub, "/Root", fixture.IntPublicationId);
    UNIT_ASSERT(cancelOutcome.RpcStatus.ok());
    UNIT_ASSERT(cancelOutcome.Operation.ready());
    UNIT_ASSERT_VALUES_EQUAL(cancelOutcome.Operation.status(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(CountPublications(fixture.Server, "root@builtin"), 0u);

    UNIT_ASSERT(!TryReadFirstTopicMessage(fixture.Server, fixture.TopicShortName, TDuration::Seconds(2), 0).Defined());
    UNIT_ASSERT(!TryReadFirstTopicMessage(fixture.Server, fixture.TopicShortName, TDuration::Seconds(2), 1).Defined());
}

Y_UNIT_TEST(CancelAfterStreamWriteClearsRegistryWithoutData) {
    auto fixture = TDeferredStreamWriteFixture::Enabled("finalize-cancel-topic", "ext-cancel");

    {
        auto session = fixture.OpenWriteStream("producer-cancel");
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            "deferred-payload-cancel",
            DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));
    }

    const auto cancelOutcome = CallCancelPublication(*fixture.DeferredStub, "/Root", fixture.IntPublicationId);
    UNIT_ASSERT(cancelOutcome.RpcStatus.ok());
    UNIT_ASSERT(cancelOutcome.Operation.ready());
    UNIT_ASSERT_VALUES_EQUAL(cancelOutcome.Operation.status(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(CountPublications(fixture.Server, "root@builtin"), 0u);

    UNIT_ASSERT(!TryReadFirstTopicMessage(fixture.Server, fixture.TopicShortName).Defined());
}

Y_UNIT_TEST(RepeatFinalizeReturnsNotFound) {
    auto fixture = TDeferredStreamWriteFixture::Enabled("finalize-repeat-topic", "ext-repeat");

    {
        auto session = fixture.OpenWriteStream("producer-repeat");
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            "payload-repeat",
            DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));
    }

    const auto firstPublish = CallPublish(*fixture.DeferredStub, "/Root", fixture.IntPublicationId);
    UNIT_ASSERT_VALUES_EQUAL(firstPublish.Operation.status(), Ydb::StatusIds::SUCCESS);

    const auto secondPublish = CallPublish(*fixture.DeferredStub, "/Root", fixture.IntPublicationId);
    UNIT_ASSERT_VALUES_EQUAL(secondPublish.Operation.status(), Ydb::StatusIds::NOT_FOUND);

    const auto cancelOutcome = CallCancelPublication(*fixture.DeferredStub, "/Root", fixture.IntPublicationId);
    UNIT_ASSERT_VALUES_EQUAL(cancelOutcome.Operation.status(), Ydb::StatusIds::NOT_FOUND);
}

Y_UNIT_TEST(PublishAndCancelDisabledByDefault) {
    NPersQueue::TTestServer server;
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);

    {
        const auto outcome = CallPublish(*stub, "/Root", 1);
        UNIT_ASSERT(outcome.RpcStatus.ok());
        AssertUnsupported(outcome.Operation, DisabledMessage);
    }

    {
        const auto outcome = CallCancelPublication(*stub, "/Root", 1);
        UNIT_ASSERT(outcome.RpcStatus.ok());
        AssertUnsupported(outcome.Operation, DisabledMessage);
    }
}

Y_UNIT_TEST(FinalizeUnknownIntReturnsNotFound) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);
    BeginPublicationIntId(CallBeginPublication(*stub, "/Root", "warmup-finalize-not-found"));

    for (const ui64 intPublicationId : {0u, 999999u}) {
        const auto publishOutcome = CallPublish(*stub, "/Root", intPublicationId);
        UNIT_ASSERT_VALUES_EQUAL(publishOutcome.Operation.status(), Ydb::StatusIds::NOT_FOUND);

        const auto cancelOutcome = CallCancelPublication(*stub, "/Root", intPublicationId);
        UNIT_ASSERT_VALUES_EQUAL(cancelOutcome.Operation.status(), Ydb::StatusIds::NOT_FOUND);
    }
}

Y_UNIT_TEST(PublishMultipleDestinations) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");
    CreateLegacyStreamWriteTopic(server, "finalize-multi-topic-a", 1);
    CreateLegacyStreamWriteTopic(server, "finalize-multi-topic-b", 1);

    auto deferredStub = MakeStub(server);
    auto topicStub = MakeTopicServiceStub(server);
    const ui64 intPublicationId = BeginPublicationIntId(
        CallBeginPublication(*deferredStub, "/Root", "ext-multi"));

    {
        auto session = TStreamWriteSession::Open(*topicStub, "finalize-multi-topic-a", "producer-a", 0);
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            "topic-a-payload",
            DeferredPublishWithExt(intPublicationId, TString("ext-multi"))));
    }
    {
        auto session = TStreamWriteSession::Open(*topicStub, "finalize-multi-topic-b", "producer-b", 0);
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            "topic-b-payload",
            DeferredPublishWithExt(intPublicationId, TString("ext-multi"))));
    }

    const auto publishOutcome = CallPublish(*deferredStub, "/Root", intPublicationId);
    UNIT_ASSERT_VALUES_EQUAL(publishOutcome.Operation.status(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(CountPublications(server, "root@builtin"), 0u);
}

Y_UNIT_TEST(PublishBeforeWriteAckKeepsRegistry) {
    auto fixture = TDeferredStreamWriteFixture::Enabled("finalize-before-ack-topic", "ext-before-ack");

    TFinalizePublicationOutcome publishOutcome;
    std::thread publishThread([&]() {
        publishOutcome = CallPublish(*fixture.DeferredStub, "/Root", fixture.IntPublicationId);
    });

    auto session = fixture.OpenWriteStream("producer-before-ack");
    UNIT_ASSERT(session->Stream->Write(MakeStreamWriteRequest(
        1,
        "payload-before-ack",
        DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId))));
    publishThread.join();

    UNIT_ASSERT_VALUES_UNEQUAL(publishOutcome.Operation.status(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(CountPublications(fixture.Server, "root@builtin"), 1u);
}

Y_UNIT_TEST(PublishFailureOnInvalidDestinationKeepsRegistry) {
    auto fixture = TDeferredStreamWriteFixture::Enabled("finalize-bad-dest-topic", "ext-bad-dest");

    {
        auto session = fixture.OpenWriteStream("producer-bad-dest");
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            "payload-bad-dest",
            DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));
    }

    const TString badBlob = NPQ::NDeferredPublish::SerializeDestinationBlob(
        NPQ::NDeferredPublish::MakeDestinationBlob(0, 9'999'999'999ull));
    UNIT_ASSERT_VALUES_EQUAL(
        CallUpsertDestination(fixture.Server, "/Root", fixture.IntPublicationId, "bad-topic-path", badBlob),
        Ydb::StatusIds::SUCCESS);

    const auto publishOutcome = CallPublish(*fixture.DeferredStub, "/Root", fixture.IntPublicationId);
    UNIT_ASSERT_VALUES_UNEQUAL(publishOutcome.Operation.status(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(CountPublications(fixture.Server, "root@builtin"), 1u);
}

Y_UNIT_TEST(BeginOnlyPublishAndCancelUseDeleteOnly) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");

    auto stub = MakeStub(server);

    const ui64 publishOnlyId = BeginPublicationIntId(
        CallBeginPublication(*stub, "/Root", "begin-only-publish"));
    {
        const auto outcome = CallPublish(*stub, "/Root", publishOnlyId);
        UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::ABORTED);
        UNIT_ASSERT_VALUES_EQUAL(CountPublications(server, "root@builtin"), 1u);
    }

    const ui64 cancelOnlyId = BeginPublicationIntId(
        CallBeginPublication(*stub, "/Root", "begin-only-cancel"));
    {
        const auto outcome = CallCancelPublication(*stub, "/Root", cancelOnlyId);
        UNIT_ASSERT_VALUES_EQUAL(outcome.Operation.status(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(CountPublications(server, "root@builtin"), 1u);
    }
}

} // Y_UNIT_TEST_SUITE

Y_UNIT_TEST_SUITE(TopicDeferredPublishLifecycle) {

Y_UNIT_TEST(PublishMakesDataVisible) {
    auto fixture = TDeferredStreamWriteFixture::Enabled("lifecycle-publish-topic", "ext-lifecycle-publish");

    constexpr TStringBuf payload = "lifecycle-publish-payload";
    {
        auto session = fixture.OpenWriteStream("producer-lifecycle-publish");
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            TString(payload),
            DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));
    }

    const auto publishOutcome = CallPublish(*fixture.DeferredStub, "/Root", fixture.IntPublicationId);
    UNIT_ASSERT_VALUES_EQUAL(publishOutcome.Operation.status(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(CountPublications(fixture.Server, "root@builtin"), 0u);

    AssertTopicVisible(
        fixture.Server,
        *fixture.TopicStub,
        fixture.TopicShortName,
        TString(payload));
}

Y_UNIT_TEST(CancelDiscardsData) {
    auto fixture = TDeferredStreamWriteFixture::Enabled("lifecycle-cancel-topic", "ext-lifecycle-cancel");

    {
        auto session = fixture.OpenWriteStream("producer-lifecycle-cancel");
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            "lifecycle-cancel-payload",
            DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));
    }

    const auto cancelOutcome = CallCancelPublication(*fixture.DeferredStub, "/Root", fixture.IntPublicationId);
    UNIT_ASSERT_VALUES_EQUAL(cancelOutcome.Operation.status(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(CountPublications(fixture.Server, "root@builtin"), 0u);

    AssertTopicEmptyAfterCancel(fixture.Server, fixture.TopicShortName);
}

Y_UNIT_TEST(StagingNotVisibleBeforePublish) {
    auto fixture = TDeferredStreamWriteFixture::Enabled("lifecycle-staging-topic", "ext-lifecycle-staging");

    constexpr TStringBuf payload = "lifecycle-staging-payload";
    {
        auto session = fixture.OpenWriteStream("producer-lifecycle-staging");
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            TString(payload),
            DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));
    }

    AssertTopicNotVisible(fixture.Server, fixture.TopicShortName);
    UNIT_ASSERT_VALUES_EQUAL(CountPublications(fixture.Server, "root@builtin"), 1u);

    const auto publishOutcome = CallPublish(*fixture.DeferredStub, "/Root", fixture.IntPublicationId);
    UNIT_ASSERT_VALUES_EQUAL(publishOutcome.Operation.status(), Ydb::StatusIds::SUCCESS);

    AssertTopicVisible(
        fixture.Server,
        *fixture.TopicStub,
        fixture.TopicShortName,
        TString(payload));
}

Y_UNIT_TEST(MultiDestinationSinglePublication) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");
    CreateLegacyStreamWriteTopic(server, "lifecycle-multi-topic-a", 1);
    CreateLegacyStreamWriteTopic(server, "lifecycle-multi-topic-b", 1);

    auto deferredStub = MakeStub(server);
    auto topicStub = MakeTopicServiceStub(server);
    const ui64 intPublicationId = BeginPublicationIntId(
        CallBeginPublication(*deferredStub, "/Root", "ext-lifecycle-multi-dest"));

    constexpr TStringBuf payloadA = "lifecycle-topic-a";
    constexpr TStringBuf payloadB = "lifecycle-topic-b";
    {
        auto session = TStreamWriteSession::Open(*topicStub, "lifecycle-multi-topic-a", "producer-a", 0);
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            TString(payloadA),
            DeferredPublishWithExt(intPublicationId, TString("ext-lifecycle-multi-dest"))));
    }
    {
        auto session = TStreamWriteSession::Open(*topicStub, "lifecycle-multi-topic-b", "producer-b", 0);
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            TString(payloadB),
            DeferredPublishWithExt(intPublicationId, TString("ext-lifecycle-multi-dest"))));
    }

    const auto publishOutcome = CallPublish(*deferredStub, "/Root", intPublicationId);
    UNIT_ASSERT_VALUES_EQUAL(publishOutcome.Operation.status(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(CountPublications(server, "root@builtin"), 0u);

    AssertTopicVisible(server, *topicStub, "lifecycle-multi-topic-a", TString(payloadA));
    AssertTopicVisible(server, *topicStub, "lifecycle-multi-topic-b", TString(payloadB));
}

Y_UNIT_TEST(RepeatFinalizeNotFound) {
    auto fixture = TDeferredStreamWriteFixture::Enabled("lifecycle-repeat-topic", "ext-lifecycle-repeat");

    constexpr TStringBuf payload = "lifecycle-repeat-payload";
    {
        auto session = fixture.OpenWriteStream("producer-lifecycle-repeat");
        WriteAndExpectWriteResponse(*session->Stream, MakeStreamWriteRequest(
            1,
            TString(payload),
            DeferredPublishWithExt(fixture.IntPublicationId, fixture.ExtPublicationId)));
    }

    const auto firstPublish = CallPublish(*fixture.DeferredStub, "/Root", fixture.IntPublicationId);
    UNIT_ASSERT_VALUES_EQUAL(firstPublish.Operation.status(), Ydb::StatusIds::SUCCESS);

    AssertTopicVisible(
        fixture.Server,
        *fixture.TopicStub,
        fixture.TopicShortName,
        TString(payload));

    const auto secondPublish = CallPublish(*fixture.DeferredStub, "/Root", fixture.IntPublicationId);
    UNIT_ASSERT_VALUES_EQUAL(secondPublish.Operation.status(), Ydb::StatusIds::NOT_FOUND);

    const auto cancelOutcome = CallCancelPublication(*fixture.DeferredStub, "/Root", fixture.IntPublicationId);
    UNIT_ASSERT_VALUES_EQUAL(cancelOutcome.Operation.status(), Ydb::StatusIds::NOT_FOUND);

    const auto legacyMessage = TryReadFirstTopicMessage(fixture.Server, fixture.TopicShortName);
    UNIT_ASSERT(legacyMessage.Defined());
    UNIT_ASSERT_VALUES_EQUAL(*legacyMessage, TString(payload));
}

Y_UNIT_TEST(BeginOnlyPublishAborts) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");
    CreateLegacyStreamWriteTopic(server, "lifecycle-begin-only-publish");

    auto deferredStub = MakeStub(server);

    const ui64 intPublicationId = BeginPublicationIntId(
        CallBeginPublication(*deferredStub, "/Root", "ext-begin-only-publish"));

    const auto publishOutcome = CallPublish(*deferredStub, "/Root", intPublicationId);
    UNIT_ASSERT_VALUES_EQUAL(publishOutcome.Operation.status(), Ydb::StatusIds::ABORTED);
    UNIT_ASSERT_VALUES_EQUAL(CountPublications(server, "root@builtin"), 1u);

    AssertTopicNotVisible(server, "lifecycle-begin-only-publish");
}

Y_UNIT_TEST(BeginOnlyCancelDeletes) {
    auto server = MakeServerWithDeferredPublishEnabled();
    server.AnnoyingClient->GrantConnect("root@builtin");
    CreateLegacyStreamWriteTopic(server, "lifecycle-begin-only-cancel");

    auto deferredStub = MakeStub(server);

    const ui64 intPublicationId = BeginPublicationIntId(
        CallBeginPublication(*deferredStub, "/Root", "ext-begin-only-cancel"));

    const auto cancelOutcome = CallCancelPublication(*deferredStub, "/Root", intPublicationId);
    UNIT_ASSERT_VALUES_EQUAL(cancelOutcome.Operation.status(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(CountPublications(server, "root@builtin"), 0u);

    AssertTopicNotVisible(server, "lifecycle-begin-only-cancel");
}

} // Y_UNIT_TEST_SUITE

} // namespace NKikimr::NPersQueueTests
