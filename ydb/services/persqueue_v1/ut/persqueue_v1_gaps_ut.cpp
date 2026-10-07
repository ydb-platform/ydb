#include <ydb/library/persqueue/constants.h>
#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/data_plane_helpers.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/services/persqueue_v1/ut/test_utils.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <google/protobuf/util/time_util.h>

#include <functional>

namespace NKikimr::NPersQueueTests {

namespace {

constexpr TStringBuf DefaultTopicFullName = "rt3.dc1--topic1";
constexpr TStringBuf DefaultTopicShortName = "topic1";
constexpr TStringBuf DefaultConsumer = "user";

std::unique_ptr<Ydb::Topic::V1::TopicService::Stub> MakeTopicStub(const NPersQueue::TTestServer& server) {
    auto channel = grpc::CreateChannel(
        "localhost:" + ToString(server.GrpcPort),
        grpc::InsecureChannelCredentials());
    return Ydb::Topic::V1::TopicService::NewStub(channel);
}

std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> MakePersQueueStub(
    const NPersQueue::TTestServer& server)
{
    auto channel = grpc::CreateChannel(
        "localhost:" + ToString(server.GrpcPort),
        grpc::InsecureChannelCredentials());
    return Ydb::PersQueue::V1::PersQueueService::NewStub(channel);
}

void FillDatabaseHeader(
    grpc::ClientContext& context,
    const TString& database,
    const TMaybe<TString>& authTicket = Nothing())
{
    context.AddMetadata(NYdb::YDB_DATABASE_HEADER, database);
    if (authTicket.Defined() && !authTicket->empty()) {
        context.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, *authTicket);
    }
}

NPersQueue::TTestServer MakeServerWithTopic(ui32 partitions = 3, bool forbidEmptyDatabase = false) {
    auto settings = NKikimr::NPersQueueTests::PQSettings();
    if (forbidEmptyDatabase) {
        settings.FeatureFlags.SetAllowYdbRequestsWithoutDatabase(false);
        settings.FeatureFlags.SetForbidRequestsToStaticNodesWithoutDatabase(true);
    }
    NPersQueue::TTestServer server(settings);
    server.AnnoyingClient->CreateTopic(TString(DefaultTopicFullName), partitions);
    server.EnableLogs({NKikimrServices::PQ_READ_PROXY, NKikimrServices::PQ_WRITE_PROXY});
    return server;
}

NYdb::TDriver MakeDriver(const NPersQueue::TTestServer& server) {
    NYdb::TDriverConfig driverCfg;
    driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << server.GrpcPort).SetDatabase("/Root");
    return NYdb::TDriver(driverCfg);
}

void WriteMessages(NYdb::TDriver& driver, ui32 count, const TString& topic = TString(DefaultTopicShortName)) {
    auto writer = CreateSimpleWriter(driver, topic, "gaps-src");
    for (ui32 i = 1; i <= count; ++i) {
        UNIT_ASSERT(writer->Write("msg-" + ToString(i), i));
    }
    UNIT_ASSERT(writer->Close(TDuration::Seconds(10)));
}

struct TStreamReadSession {
    grpc::ClientContext Context;
    std::unique_ptr<grpc::ClientReaderWriter<
        Ydb::Topic::StreamReadMessage::FromClient,
        Ydb::Topic::StreamReadMessage::FromServer>> Stream;

    static std::unique_ptr<TStreamReadSession> Open(
        Ydb::Topic::V1::TopicService::Stub& stub,
        const TMaybe<TString>& database = TString("/Root"),
        bool withAuthTicket = false)
    {
        auto session = std::make_unique<TStreamReadSession>();
        if (database.Defined()) {
            FillDatabaseHeader(
                session->Context,
                *database,
                withAuthTicket ? TMaybe<TString>(TString("root@builtin")) : Nothing());
        } else if (withAuthTicket) {
            session->Context.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, "root@builtin");
        }
        session->Stream = stub.StreamRead(&session->Context);
        UNIT_ASSERT(session->Stream);
        return session;
    }
};

Ydb::Topic::StreamReadMessage::FromServer InitStreamRead(
    grpc::ClientReaderWriter<
        Ydb::Topic::StreamReadMessage::FromClient,
        Ydb::Topic::StreamReadMessage::FromServer>& stream,
    const TString& topicPath,
    const TString& consumer,
    bool autoPartitioningSupport,
    const TVector<ui32>& partitionIds = {})
{
    Ydb::Topic::StreamReadMessage::FromClient req;
    Ydb::Topic::StreamReadMessage::FromServer resp;

    auto* topicSettings = req.mutable_init_request()->add_topics_read_settings();
    topicSettings->set_path(topicPath);
    for (ui32 partitionId : partitionIds) {
        topicSettings->add_partition_ids(partitionId);
    }
    req.mutable_init_request()->set_consumer(consumer);
    req.mutable_init_request()->set_auto_partitioning_support(autoPartitioningSupport);

    UNIT_ASSERT(stream.Write(req));
    UNIT_ASSERT(stream.Read(&resp));
    return resp;
}

i64 ConfirmStartPartitionSession(
    grpc::ClientReaderWriter<
        Ydb::Topic::StreamReadMessage::FromClient,
        Ydb::Topic::StreamReadMessage::FromServer>& stream,
    i64 assignId,
    ui64 readOffset = 0,
    const TMaybe<ui64>& commitOffset = Nothing(),
    const TMaybe<ui64>& maxOffset = Nothing())
{
    Ydb::Topic::StreamReadMessage::FromClient req;
    req.mutable_start_partition_session_response()->set_partition_session_id(assignId);
    req.mutable_start_partition_session_response()->set_read_offset(readOffset);
    if (commitOffset) {
        req.mutable_start_partition_session_response()->set_commit_offset(*commitOffset);
    }
    if (maxOffset) {
        req.mutable_start_partition_session_response()->set_max_offset(*maxOffset);
    }
    UNIT_ASSERT(stream.Write(req));
    return assignId;
}

Ydb::Topic::StreamReadMessage::FromServer ReadNext(
    grpc::ClientReaderWriter<
        Ydb::Topic::StreamReadMessage::FromClient,
        Ydb::Topic::StreamReadMessage::FromServer>& stream)
{
    Ydb::Topic::StreamReadMessage::FromServer resp;
    UNIT_ASSERT(stream.Read(&resp));
    return resp;
}

void AssertBadRequestClose(const Ydb::Topic::StreamReadMessage::FromServer& resp, const TString& expectedSubstring = {}) {
    UNIT_ASSERT_VALUES_EQUAL_C(resp.status(), Ydb::StatusIds::BAD_REQUEST, resp);
    if (!expectedSubstring.empty()) {
        UNIT_ASSERT_GE(resp.issues_size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(resp.issues(0).message(), expectedSubstring);
    }
}

struct TStreamWriteSession {
    grpc::ClientContext Context;
    std::unique_ptr<grpc::ClientReaderWriter<
        Ydb::Topic::StreamWriteMessage::FromClient,
        Ydb::Topic::StreamWriteMessage::FromServer>> Stream;

    static std::unique_ptr<TStreamWriteSession> Open(
        Ydb::Topic::V1::TopicService::Stub& stub,
        const TMaybe<TString>& database = TString("/Root"),
        bool withAuthTicket = false)
    {
        auto session = std::make_unique<TStreamWriteSession>();
        if (database.Defined()) {
            FillDatabaseHeader(
                session->Context,
                *database,
                withAuthTicket ? TMaybe<TString>(TString("root@builtin")) : Nothing());
        } else if (withAuthTicket) {
            session->Context.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, "root@builtin");
        }
        session->Stream = stub.StreamWrite(&session->Context);
        UNIT_ASSERT(session->Stream);
        return session;
    }
};

Ydb::Topic::StreamWriteMessage::FromServer InitStreamWrite(
    grpc::ClientReaderWriter<
        Ydb::Topic::StreamWriteMessage::FromClient,
        Ydb::Topic::StreamWriteMessage::FromServer>& stream,
    const TString& topicPath,
    const TString& producerId,
    const THashMap<TString, TString>& writeSessionMeta = {})
{
    Ydb::Topic::StreamWriteMessage::FromClient req;
    Ydb::Topic::StreamWriteMessage::FromServer resp;
    req.mutable_init_request()->set_path(topicPath);
    req.mutable_init_request()->set_producer_id(producerId);
    for (const auto& [key, value] : writeSessionMeta) {
        (*req.mutable_init_request()->mutable_write_session_meta())[key] = value;
    }
    UNIT_ASSERT(stream.Write(req));
    UNIT_ASSERT(stream.Read(&resp));
    return resp;
}

Ydb::StatusIds::StatusCode CallUnaryStatus(
    const std::function<void(grpc::ClientContext&, Ydb::Operations::Operation&)>& call,
    const TMaybe<TString>& database)
{
    grpc::ClientContext context;
    if (database.Defined()) {
        FillDatabaseHeader(context, *database, TString("root@builtin"));
    } else {
        context.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, "root@builtin");
    }
    Ydb::Operations::Operation operation;
    call(context, operation);
    return operation.status();
}

void CreateTopicWithSharedConsumerYql(NPersQueue::TTestServer& server, const TString& fullPath, const TString& consumer) {
    const TString query = TStringBuilder()
        << "CREATE TOPIC `" << fullPath << "` "
        << "(CONSUMER `" << consumer << "` WITH (type = 'shared'))";
    server.AnnoyingClient->RunYqlSchemeQuery(query);
}

} // namespace

Y_UNIT_TEST_SUITE(PersQueueV1Gaps_ReadWithoutConsumer) {

Y_UNIT_TEST(RejectsEmptyPartitionsWithoutAutoscaleSupport) {
    auto server = MakeServerWithTopic(3);
    auto stub = MakeTopicStub(server);
    auto session = TStreamReadSession::Open(*stub);

    auto resp = InitStreamRead(
        *session->Stream,
        TString(DefaultTopicShortName),
        /*consumer=*/"",
        /*autoPartitioningSupport=*/false,
        /*partitionIds=*/{});
    // Init may succeed; rejection happens after describe when Groups are empty.
    if (resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse) {
        resp = ReadNext(*session->Stream);
    }
    AssertBadRequestClose(resp, "non-autoscale aware client");
}

Y_UNIT_TEST(LocksAllPartitionsWithAutoscaleSupport) {
    constexpr ui32 partitions = 4;
    auto server = MakeServerWithTopic(partitions);
    auto stub = MakeTopicStub(server);
    auto session = TStreamReadSession::Open(*stub);

    auto resp = InitStreamRead(
        *session->Stream,
        TString(DefaultTopicShortName),
        /*consumer=*/"",
        /*autoPartitioningSupport=*/true,
        /*partitionIds=*/{});
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse,
        resp);

    THashSet<ui32> assignedPartitions;
    while (assignedPartitions.size() < partitions) {
        resp = ReadNext(*session->Stream);
        UNIT_ASSERT_C(
            resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest,
            resp);
        const auto& start = resp.start_partition_session_request();
        assignedPartitions.insert(start.partition_session().partition_id());
        ConfirmStartPartitionSession(*session->Stream, start.partition_session().partition_session_id());
    }
    UNIT_ASSERT_VALUES_EQUAL(assignedPartitions.size(), partitions);
}

Y_UNIT_TEST(RejectsCommitWithoutConsumer) {
    auto server = MakeServerWithTopic(1);
    auto driver = MakeDriver(server);
    WriteMessages(driver, 2);

    auto stub = MakeTopicStub(server);
    auto session = TStreamReadSession::Open(*stub);

    auto resp = InitStreamRead(
        *session->Stream,
        TString(DefaultTopicShortName),
        /*consumer=*/"",
        /*autoPartitioningSupport=*/false,
        /*partitionIds=*/{0});
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse,
        resp);

    resp = ReadNext(*session->Stream);
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest,
        resp);
    const i64 assignId = resp.start_partition_session_request().partition_session().partition_session_id();
    ConfirmStartPartitionSession(*session->Stream, assignId);

    Ydb::Topic::StreamReadMessage::FromClient commitReq;
    auto* commit = commitReq.mutable_commit_offset_request()->add_commit_offsets();
    commit->set_partition_session_id(assignId);
    auto* offsets = commit->add_offsets();
    offsets->set_start(0);
    offsets->set_end(1);
    UNIT_ASSERT(session->Stream->Write(commitReq));

    resp = ReadNext(*session->Stream);
    AssertBadRequestClose(resp);
}

} // Y_UNIT_TEST_SUITE(PersQueueV1Gaps_ReadWithoutConsumer)

Y_UNIT_TEST_SUITE(PersQueueV1Gaps_TrackProducerIdInTx) {

Y_UNIT_TEST(AcceptsTrueAndFalse) {
    auto server = MakeServerWithTopic(1);
    auto stub = MakeTopicStub(server);

    for (const TString value : {"true", "false"}) {
        auto session = TStreamWriteSession::Open(*stub);
        auto resp = InitStreamWrite(
            *session->Stream,
            TString(DefaultTopicShortName),
            "producer-" + value,
            {{TString(NPersQueue::WRITE_SESSION_ATTRIBUTE_TRACK_PRODUCER_ID_IN_TX), value}});
        UNIT_ASSERT_C(
            resp.server_message_case() == Ydb::Topic::StreamWriteMessage::FromServer::kInitResponse,
            resp);
        UNIT_ASSERT_VALUES_EQUAL_C(resp.status(), Ydb::StatusIds::SUCCESS, resp);
    }
}

Y_UNIT_TEST(RejectsInvalidBoolean) {
    auto server = MakeServerWithTopic(1);
    auto stub = MakeTopicStub(server);
    auto session = TStreamWriteSession::Open(*stub);

    auto resp = InitStreamWrite(
        *session->Stream,
        TString(DefaultTopicShortName),
        "producer-bad-meta",
        {{TString(NPersQueue::WRITE_SESSION_ATTRIBUTE_TRACK_PRODUCER_ID_IN_TX), "not-a-bool"}});
    UNIT_ASSERT_VALUES_EQUAL_C(resp.status(), Ydb::StatusIds::BAD_REQUEST, resp);
    UNIT_ASSERT_GE(resp.issues_size(), 1);
    UNIT_ASSERT_STRING_CONTAINS(resp.issues(0).message(), "track_producer_id_in_tx");
}

} // Y_UNIT_TEST_SUITE(PersQueueV1Gaps_TrackProducerIdInTx)

Y_UNIT_TEST_SUITE(PersQueueV1Gaps_EmptyDatabase) {

Y_UNIT_TEST(UnaryRpcsRejectEmptyAndMissingDatabase) {
    auto server = MakeServerWithTopic(1, /*forbidEmptyDatabase=*/true);
    server.AnnoyingClient->GrantConnect("root@builtin");
    auto topicStub = MakeTopicStub(server);
    auto pqStub = MakePersQueueStub(server);

    const auto cases = TVector<TMaybe<TString>>{
        TString(),          // empty header value
        Nothing(),          // missing header
    };

    for (const auto& database : cases) {
        {
            const auto status = CallUnaryStatus(
                [&](grpc::ClientContext& context, Ydb::Operations::Operation& operation) {
                    Ydb::Topic::CreateTopicRequest request;
                    Ydb::Topic::CreateTopicResponse response;
                    request.set_path("/Root/PQ/rt3.dc1--empty-db-topic");
                    request.mutable_partitioning_settings()->set_min_active_partitions(1);
                    auto rpc = topicStub->CreateTopic(&context, request, &response);
                    UNIT_ASSERT(rpc.ok());
                    operation = response.operation();
                },
                database);
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        }
        {
            const auto status = CallUnaryStatus(
                [&](grpc::ClientContext& context, Ydb::Operations::Operation& operation) {
                    Ydb::Topic::DescribeTopicRequest request;
                    Ydb::Topic::DescribeTopicResponse response;
                    request.set_path(TStringBuilder() << "/Root/PQ/" << DefaultTopicFullName);
                    auto rpc = topicStub->DescribeTopic(&context, request, &response);
                    UNIT_ASSERT(rpc.ok());
                    operation = response.operation();
                },
                database);
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        }
        {
            const auto status = CallUnaryStatus(
                [&](grpc::ClientContext& context, Ydb::Operations::Operation& operation) {
                    Ydb::Topic::DescribeConsumerRequest request;
                    Ydb::Topic::DescribeConsumerResponse response;
                    request.set_path(TStringBuilder() << "/Root/PQ/" << DefaultTopicFullName);
                    request.set_consumer(TString(DefaultConsumer));
                    auto rpc = topicStub->DescribeConsumer(&context, request, &response);
                    UNIT_ASSERT(rpc.ok());
                    operation = response.operation();
                },
                database);
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        }
        {
            const auto status = CallUnaryStatus(
                [&](grpc::ClientContext& context, Ydb::Operations::Operation& operation) {
                    Ydb::Topic::DropTopicRequest request;
                    Ydb::Topic::DropTopicResponse response;
                    request.set_path(TStringBuilder() << "/Root/PQ/" << DefaultTopicFullName);
                    auto rpc = topicStub->DropTopic(&context, request, &response);
                    UNIT_ASSERT(rpc.ok());
                    operation = response.operation();
                },
                database);
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        }
        {
            const auto status = CallUnaryStatus(
                [&](grpc::ClientContext& context, Ydb::Operations::Operation& operation) {
                    Ydb::Topic::CommitOffsetRequest request;
                    Ydb::Topic::CommitOffsetResponse response;
                    request.set_path(TString(DefaultTopicShortName));
                    request.set_consumer(TString(DefaultConsumer));
                    request.set_offset(1);
                    auto rpc = topicStub->CommitOffset(&context, request, &response);
                    UNIT_ASSERT(rpc.ok());
                    operation = response.operation();
                },
                database);
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        }
        {
            const auto status = CallUnaryStatus(
                [&](grpc::ClientContext& context, Ydb::Operations::Operation& operation) {
                    Ydb::PersQueue::V1::DropTopicRequest request;
                    Ydb::PersQueue::V1::DropTopicResponse response;
                    request.set_path(TString(DefaultTopicFullName));
                    auto rpc = pqStub->DropTopic(&context, request, &response);
                    UNIT_ASSERT(rpc.ok());
                    operation = response.operation();
                },
                database);
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        }
    }
}

Y_UNIT_TEST(StreamWriteRejectsEmptyAndMissingDatabase) {
    auto server = MakeServerWithTopic(1, /*forbidEmptyDatabase=*/true);
    server.AnnoyingClient->GrantConnect("root@builtin");
    auto stub = MakeTopicStub(server);

    for (const auto& database : {TMaybe<TString>(TString()), TMaybe<TString>()}) {
        // Rejection happens on stream accept (before any client message) via WriteAndFinish(BAD_REQUEST).
        // Do not Write first: that races with server finish and can return false flakily.
        auto session = TStreamWriteSession::Open(*stub, database, /*withAuthTicket=*/true);
        Ydb::Topic::StreamWriteMessage::FromServer resp;
        UNIT_ASSERT_C(session->Stream->Read(&resp), "expected BAD_REQUEST on accept");
        UNIT_ASSERT_VALUES_EQUAL_C(resp.status(), Ydb::StatusIds::BAD_REQUEST, resp);
    }
}

Y_UNIT_TEST(StreamReadRejectsEmptyAndMissingDatabase) {
    auto server = MakeServerWithTopic(1, /*forbidEmptyDatabase=*/true);
    server.AnnoyingClient->GrantConnect("root@builtin");
    auto stub = MakeTopicStub(server);

    for (const auto& database : {TMaybe<TString>(TString()), TMaybe<TString>()}) {
        // Same as StreamWrite: empty/missing database is rejected on accept.
        auto session = TStreamReadSession::Open(*stub, database, /*withAuthTicket=*/true);
        Ydb::Topic::StreamReadMessage::FromServer resp;
        UNIT_ASSERT_C(session->Stream->Read(&resp), "expected BAD_REQUEST on accept");
        UNIT_ASSERT_VALUES_EQUAL_C(resp.status(), Ydb::StatusIds::BAD_REQUEST, resp);
    }
}

} // Y_UNIT_TEST_SUITE(PersQueueV1Gaps_EmptyDatabase)

Y_UNIT_TEST_SUITE(PersQueueV1Gaps_MaxOffset) {

Y_UNIT_TEST(RejectsReadOffsetGreaterThanMaxOffset) {
    auto server = MakeServerWithTopic(1);
    auto driver = MakeDriver(server);
    WriteMessages(driver, 10);

    auto stub = MakeTopicStub(server);
    auto session = TStreamReadSession::Open(*stub);

    auto resp = InitStreamRead(
        *session->Stream,
        TString(DefaultTopicShortName),
        TString(DefaultConsumer),
        /*autoPartitioningSupport=*/false);
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse,
        resp);

    resp = ReadNext(*session->Stream);
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest,
        resp);
    const i64 assignId = resp.start_partition_session_request().partition_session().partition_session_id();

    // max_offset is inclusive in API; read_offset > max_offset must fail.
    ConfirmStartPartitionSession(*session->Stream, assignId, /*readOffset=*/8, /*commitOffset=*/0, /*maxOffset=*/6);
    resp = ReadNext(*session->Stream);
    AssertBadRequestClose(resp, "larger than provided to max");
}

Y_UNIT_TEST(RejectsCommitOffsetGreaterThanMaxOffset) {
    auto server = MakeServerWithTopic(1);
    auto driver = MakeDriver(server);
    WriteMessages(driver, 10);

    auto stub = MakeTopicStub(server);
    auto session = TStreamReadSession::Open(*stub);

    auto resp = InitStreamRead(
        *session->Stream,
        TString(DefaultTopicShortName),
        TString(DefaultConsumer),
        /*autoPartitioningSupport=*/false);
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse,
        resp);

    resp = ReadNext(*session->Stream);
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest,
        resp);
    const i64 assignId = resp.start_partition_session_request().partition_session().partition_session_id();

    ConfirmStartPartitionSession(*session->Stream, assignId, /*readOffset=*/0, /*commitOffset=*/9, /*maxOffset=*/5);
    resp = ReadNext(*session->Stream);
    AssertBadRequestClose(resp, "commit");
}

Y_UNIT_TEST(MaxOffsetBeyondEndStopsAtAvailableData) {
    auto server = MakeServerWithTopic(1);
    auto driver = MakeDriver(server);
    WriteMessages(driver, 5);

    auto stub = MakeTopicStub(server);
    auto session = TStreamReadSession::Open(*stub);

    auto resp = InitStreamRead(
        *session->Stream,
        TString(DefaultTopicShortName),
        TString(DefaultConsumer),
        /*autoPartitioningSupport=*/false);
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse,
        resp);

    resp = ReadNext(*session->Stream);
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest,
        resp);
    const i64 assignId = resp.start_partition_session_request().partition_session().partition_session_id();
    ConfirmStartPartitionSession(*session->Stream, assignId, /*readOffset=*/0, /*commitOffset=*/0, /*maxOffset=*/100);

    Ydb::Topic::StreamReadMessage::FromClient readReq;
    readReq.mutable_read_request()->set_bytes_size(1_MB);
    UNIT_ASSERT(session->Stream->Write(readReq));

    resp = ReadNext(*session->Stream);
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kReadResponse,
        resp);
    UNIT_ASSERT_GE(resp.read_response().partition_data_size(), 1);
}

Y_UNIT_TEST(WithoutConsumerWithMaxOffset) {
    auto server = MakeServerWithTopic(1);
    auto driver = MakeDriver(server);
    WriteMessages(driver, 6);

    auto stub = MakeTopicStub(server);
    auto session = TStreamReadSession::Open(*stub);

    auto resp = InitStreamRead(
        *session->Stream,
        TString(DefaultTopicShortName),
        /*consumer=*/"",
        /*autoPartitioningSupport=*/false,
        /*partitionIds=*/{0});
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse,
        resp);

    resp = ReadNext(*session->Stream);
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest,
        resp);
    const i64 assignId = resp.start_partition_session_request().partition_session().partition_session_id();
    ConfirmStartPartitionSession(*session->Stream, assignId, /*readOffset=*/2, Nothing(), /*maxOffset=*/4);

    Ydb::Topic::StreamReadMessage::FromClient readReq;
    readReq.mutable_read_request()->set_bytes_size(1_MB);
    UNIT_ASSERT(session->Stream->Write(readReq));

    resp = ReadNext(*session->Stream);
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kReadResponse,
        resp);

    ui64 lastOffset = 0;
    ui32 messageCount = 0;
    for (const auto& part : resp.read_response().partition_data()) {
        for (const auto& batch : part.batches()) {
            for (const auto& msg : batch.message_data()) {
                lastOffset = msg.offset();
                ++messageCount;
            }
        }
    }
    UNIT_ASSERT_GT(messageCount, 0u);
    UNIT_ASSERT_LE(lastOffset, 4u);
}

} // Y_UNIT_TEST_SUITE(PersQueueV1Gaps_MaxOffset)

Y_UNIT_TEST_SUITE(PersQueueV1Gaps_SharedConsumerRead) {

Y_UNIT_TEST(StreamReadRejectsSharedConsumer) {
    NKikimrConfig::TFeatureFlags ff;
    ff.SetEnableTopicMessageLevelParallelism(true);
    auto settings = NKikimr::NPersQueueTests::PQSettings();
    settings.SetFeatureFlags(ff);
    NPersQueue::TTestServer server(settings);
    server.EnableLogs({NKikimrServices::PQ_READ_PROXY});

    const TString fullPath = "/Root/PQ/rt3.dc1--shared_read_topic";
    const TString shortPath = "shared_read_topic";
    const TString consumer = "shared_c1";
    CreateTopicWithSharedConsumerYql(server, fullPath, consumer);

    auto stub = MakeTopicStub(server);
    auto session = TStreamReadSession::Open(*stub);

    auto resp = InitStreamRead(
        *session->Stream,
        shortPath,
        consumer,
        /*autoPartitioningSupport=*/false);
    if (resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse) {
        resp = ReadNext(*session->Stream);
    }
    UNIT_ASSERT_C(resp.status() != Ydb::StatusIds::SUCCESS, resp);
    UNIT_ASSERT_GE(resp.issues_size(), 1);
    const TString message = resp.issues(0).message();
    UNIT_ASSERT_C(
        message.Contains("no read rule") || message.Contains("is not streaming"),
        resp);
}

} // Y_UNIT_TEST_SUITE(PersQueueV1Gaps_SharedConsumerRead)

Y_UNIT_TEST_SUITE(PersQueueV1Gaps_CommitPipeRestart) {

Y_UNIT_TEST(CommitSurvivesTabletRestart) {
    auto server = MakeServerWithTopic(1);
    auto driver = MakeDriver(server);
    WriteMessages(driver, 12);

    auto stub = MakeTopicStub(server);
    auto session = TStreamReadSession::Open(*stub);

    auto resp = InitStreamRead(
        *session->Stream,
        TString(DefaultTopicShortName),
        TString(DefaultConsumer),
        /*autoPartitioningSupport=*/false);
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse,
        resp);

    resp = ReadNext(*session->Stream);
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest,
        resp);
    const i64 assignId = resp.start_partition_session_request().partition_session().partition_session_id();
    ConfirmStartPartitionSession(*session->Stream, assignId);

    Ydb::Topic::StreamReadMessage::FromClient readReq;
    readReq.mutable_read_request()->set_bytes_size(1_MB);
    UNIT_ASSERT(session->Stream->Write(readReq));
    resp = ReadNext(*session->Stream);
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kReadResponse,
        resp);

    Ydb::Topic::StreamReadMessage::FromClient commitReq;
    auto* commit = commitReq.mutable_commit_offset_request()->add_commit_offsets();
    commit->set_partition_session_id(assignId);
    auto* offsets = commit->add_offsets();
    offsets->set_start(0);
    offsets->set_end(3);
    UNIT_ASSERT(session->Stream->Write(commitReq));

    // Kill partition tablet to force pipe reconnect / session recovery path.
    const TString oldPath = TStringBuilder() << "/Root/PQ/" << DefaultTopicFullName;
    auto pathDescr = server.AnnoyingClient->Ls(oldPath)->Record.GetPathDescription().GetPersQueueGroup();
    UNIT_ASSERT_GE(pathDescr.PartitionsSize(), 1);
    server.AnnoyingClient->KillTablet(*server.CleverServer, pathDescr.GetPartitions(0).GetTabletId());

    // Session may expire or destroy partition session; either way server must not abort.
    resp = ReadNext(*session->Stream);
    UNIT_ASSERT_C(
        resp.status() == Ydb::StatusIds::SESSION_EXPIRED
            || resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStopPartitionSessionRequest
            || resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kCommitOffsetResponse,
        resp);

    // Fresh session after restart: commit API still works and advances committed offset.
    {
        Ydb::Topic::CommitOffsetRequest req;
        Ydb::Topic::CommitOffsetResponse commitResp;
        req.set_path(TString(DefaultTopicShortName));
        req.set_consumer(TString(DefaultConsumer));
        req.set_offset(5);
        grpc::ClientContext context;
        FillDatabaseHeader(context, "/Root");
        auto status = stub->CommitOffset(&context, req, &commitResp);
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT_VALUES_EQUAL_C(commitResp.operation().status(), Ydb::StatusIds::SUCCESS, commitResp);
    }

    auto session2 = TStreamReadSession::Open(*stub);
    resp = InitStreamRead(
        *session2->Stream,
        TString(DefaultTopicShortName),
        TString(DefaultConsumer),
        /*autoPartitioningSupport=*/false);
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse,
        resp);
    resp = ReadNext(*session2->Stream);
    UNIT_ASSERT_C(
        resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest,
        resp);
    UNIT_ASSERT_VALUES_EQUAL(
        resp.start_partition_session_request().committed_offset(),
        5);
}

} // Y_UNIT_TEST_SUITE(PersQueueV1Gaps_CommitPipeRestart)

Y_UNIT_TEST_SUITE(PersQueueV1Gaps_SchemaSmoke) {

Y_UNIT_TEST(TopicAlterDropAndDescribeUnknownConsumer) {
    auto server = MakeServerWithTopic(1);
    auto stub = MakeTopicStub(server);
    const TString topicPath = TStringBuilder() << "/Root/PQ/" << DefaultTopicFullName;

    {
        Ydb::Topic::AlterTopicRequest request;
        Ydb::Topic::AlterTopicResponse response;
        request.set_path(topicPath);
        auto* consumer = request.add_add_consumers();
        consumer->set_name("schema-smoke-consumer");
        grpc::ClientContext context;
        FillDatabaseHeader(context, "/Root");
        auto status = stub->AlterTopic(&context, request, &response);
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response);
    }

    {
        Ydb::Topic::DescribeConsumerRequest request;
        Ydb::Topic::DescribeConsumerResponse response;
        request.set_path(topicPath);
        request.set_consumer("schema-smoke-consumer");
        request.set_include_stats(true);
        grpc::ClientContext context;
        FillDatabaseHeader(context, "/Root");
        auto status = stub->DescribeConsumer(&context, request, &response);
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response);
    }

    {
        Ydb::Topic::DescribeConsumerRequest request;
        Ydb::Topic::DescribeConsumerResponse response;
        request.set_path(topicPath);
        request.set_consumer("no-such-consumer");
        grpc::ClientContext context;
        FillDatabaseHeader(context, "/Root");
        auto status = stub->DescribeConsumer(&context, request, &response);
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SCHEME_ERROR, response);
    }

    {
        Ydb::Topic::AlterTopicRequest request;
        Ydb::Topic::AlterTopicResponse response;
        request.set_path(topicPath);
        request.add_drop_consumers("schema-smoke-consumer");
        grpc::ClientContext context;
        FillDatabaseHeader(context, "/Root");
        auto status = stub->AlterTopic(&context, request, &response);
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response);
    }

    {
        server.AnnoyingClient->CreateTopic("rt3.dc1--schema-drop-me", 1);
        Ydb::Topic::DropTopicRequest request;
        Ydb::Topic::DropTopicResponse response;
        request.set_path("/Root/PQ/rt3.dc1--schema-drop-me");
        grpc::ClientContext context;
        FillDatabaseHeader(context, "/Root");
        auto status = stub->DropTopic(&context, request, &response);
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response);
    }
}

Y_UNIT_TEST(Pqv1RemoveReadRuleSharedConsumer) {
    NKikimrConfig::TFeatureFlags ff;
    ff.SetEnableTopicMessageLevelParallelism(true);
    auto settings = NKikimr::NPersQueueTests::PQSettings();
    settings.SetFeatureFlags(ff);
    NPersQueue::TTestServer server(settings);

    const TString fullPath = "/Root/PQ/rt3.dc1--shared_remove_topic";
    const TString consumer = "shared_to_remove";
    CreateTopicWithSharedConsumerYql(server, fullPath, consumer);

    auto stub = MakePersQueueStub(server);
    {
        Ydb::PersQueue::V1::RemoveReadRuleRequest request;
        Ydb::PersQueue::V1::RemoveReadRuleResponse response;
        request.set_path(fullPath);
        request.set_consumer_name(consumer);
        grpc::ClientContext context;
        FillDatabaseHeader(context, "/Root");
        auto status = stub->RemoveReadRule(&context, request, &response);
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response);
    }

    {
        Ydb::Topic::DescribeConsumerRequest request;
        Ydb::Topic::DescribeConsumerResponse response;
        request.set_path(fullPath);
        request.set_consumer(consumer);
        grpc::ClientContext context;
        FillDatabaseHeader(context, "/Root");
        auto topicStub = MakeTopicStub(server);
        auto status = topicStub->DescribeConsumer(&context, request, &response);
        UNIT_ASSERT(status.ok());
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SCHEME_ERROR, response);
    }
}

Y_UNIT_TEST(DescribeConsumerSharedWithStats) {
    NKikimrConfig::TFeatureFlags ff;
    ff.SetEnableTopicMessageLevelParallelism(true);
    auto settings = NKikimr::NPersQueueTests::PQSettings();
    settings.SetFeatureFlags(ff);
    NPersQueue::TTestServer server(settings);

    const TString fullPath = "/Root/PQ/rt3.dc1--shared_describe_topic";
    const TString consumer = "shared_describe";
    CreateTopicWithSharedConsumerYql(server, fullPath, consumer);

    auto stub = MakeTopicStub(server);
    Ydb::Topic::DescribeConsumerRequest request;
    Ydb::Topic::DescribeConsumerResponse response;
    request.set_path(fullPath);
    request.set_consumer(consumer);
    request.set_include_stats(true);
    request.set_include_location(true);
    grpc::ClientContext context;
    FillDatabaseHeader(context, "/Root");
    auto status = stub->DescribeConsumer(&context, request, &response);
    UNIT_ASSERT(status.ok());
    UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), Ydb::StatusIds::SUCCESS, response);

    Ydb::Topic::DescribeConsumerResult result;
    UNIT_ASSERT(response.operation().result().UnpackTo(&result));
    UNIT_ASSERT(result.consumer().name().EndsWith(consumer));
}

} // Y_UNIT_TEST_SUITE(PersQueueV1Gaps_SchemaSmoke)

} // namespace NKikimr::NPersQueueTests
