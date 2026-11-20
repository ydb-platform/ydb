#include <ydb/library/aclib/aclib.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/create_channel.h>

#include <chrono>
#include <future>
#include <utility>

namespace NKikimr::NPersQueueTests {

namespace {

const TString TopicName = "rt3.dc1--topic1";
const TString TopicShortName = "topic1";
const TString ReaderTicket = "reader@builtin";
const TString StrangerTicket = "stranger@builtin";

struct TStreamReadSession {
    grpc::ClientContext Context;
    std::unique_ptr<grpc::ClientReaderWriter<
        Ydb::Topic::StreamReadMessage::FromClient,
        Ydb::Topic::StreamReadMessage::FromServer>> Stream;

    static std::unique_ptr<TStreamReadSession> Open(
        Ydb::Topic::V1::TopicService::Stub& stub,
        const TString& database = "/Root",
        const TMaybe<TString>& authTicket = Nothing())
    {
        auto session = std::make_unique<TStreamReadSession>();
        session->Context.AddMetadata(NYdb::YDB_DATABASE_HEADER, database);
        if (authTicket.Defined()) {
            session->Context.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, *authTicket);
        }
        session->Context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(60));
        session->Stream = stub.StreamRead(&session->Context);
        UNIT_ASSERT(session->Stream);
        return session;
    }
};

std::unique_ptr<Ydb::Topic::V1::TopicService::Stub> MakeTopicStub(const NPersQueue::TTestServer& server) {
    auto channel = grpc::CreateChannel(
        "localhost:" + ToString(server.GrpcPort),
        grpc::InsecureChannelCredentials());
    return Ydb::Topic::V1::TopicService::NewStub(channel);
}

Ydb::Topic::StreamReadMessage::FromServer ReadNext(TStreamReadSession& session) {
    Ydb::Topic::StreamReadMessage::FromServer resp;
    UNIT_ASSERT_C(session.Stream->Read(&resp), "stream closed before a server message");
    return resp;
}

void WriteInit(
    TStreamReadSession& session,
    const TVector<TString>& topics,
    const TString& consumer,
    const TVector<ui32>& partitionIds)
{
    Ydb::Topic::StreamReadMessage::FromClient req;
    for (const auto& topic : topics) {
        auto* settings = req.mutable_init_request()->add_topics_read_settings();
        settings->set_path(topic);
        for (ui32 partitionId : partitionIds) {
            settings->add_partition_ids(partitionId);
        }
    }
    req.mutable_init_request()->set_consumer(consumer);
    UNIT_ASSERT(session.Stream->Write(req));
}

void GrantTopicRead(NPersQueue::TTestServer& server, const TString& path, const TString& subject) {
    NYdb::NScheme::TSchemeClient scheme(server.GetDriver());
    NYdb::NScheme::TModifyPermissionsSettings settings;
    settings.AddSetPermissions(NYdb::NScheme::TPermissions(subject, {"ydb.generic.read"}));
    auto result = scheme.ModifyPermissions(path, settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

TString IssueText(const Ydb::Topic::StreamReadMessage::FromServer& resp) {
    TStringBuilder text;
    for (const auto& issue : resp.issues()) {
        if (!text.empty()) {
            text << "; ";
        }
        text << issue.message();
    }
    return text;
}

void AssertClose(
    const Ydb::Topic::StreamReadMessage::FromServer& resp,
    Ydb::StatusIds::StatusCode status,
    const TVector<TString>& fragments)
{
    UNIT_ASSERT_VALUES_EQUAL_C(resp.status(), status, resp);
    const TString text = IssueText(resp);
    for (const auto& fragment : fragments) {
        UNIT_ASSERT_STRING_CONTAINS_C(text, fragment, resp);
    }
}

NPersQueue::TTestServer MakeFederationServer() {
    NPersQueue::TTestServer server;
    server.EnableLogs({NKikimrServices::PQ_READ_PROXY, NKikimrServices::PQ_METACACHE});
    return server;
}

} // namespace

Y_UNIT_TEST_SUITE(ReadInitAuth) {

Y_UNIT_TEST(UnknownTopic) {
    auto server = MakeFederationServer();
    auto stub = MakeTopicStub(server);
    auto session = TStreamReadSession::Open(*stub);
    WriteInit(*session, {TopicShortName}, "user", {});
    AssertClose(ReadNext(*session), Ydb::StatusIds::SCHEME_ERROR, {
        "Discovery for all topics failed",
        "no path",
        "Marker# PQ15",
    });
}

Y_UNIT_TEST(DirectoryIsNotATopic) {
    auto server = MakeFederationServer();
    server.AnnoyingClient->MkDir("/Root/PQ", "rt3.dc1--not-a-topic");
    auto stub = MakeTopicStub(server);
    auto session = TStreamReadSession::Open(*stub);
    WriteInit(*session, {"not-a-topic"}, "user", {});
    AssertClose(ReadNext(*session), Ydb::StatusIds::SCHEME_ERROR, {
        "Discovery for all topics failed",
        "is not a topic",
        "Marker# PQ13",
    });
}

Y_UNIT_TEST(MissingReadRule) {
    auto server = MakeFederationServer();
    server.AnnoyingClient->CreateTopic(TopicName, 1);
    GrantTopicRead(server, "/Root/PQ/" + TopicName, ReaderTicket);

    auto stub = MakeTopicStub(server);
    auto session = TStreamReadSession::Open(*stub, "/Root", ReaderTicket);
    WriteInit(*session, {TopicShortName}, "ghost", {0});
    AssertClose(ReadNext(*session), Ydb::StatusIds::BAD_REQUEST, {
        "no read rule provided for consumer 'shared/ghost'",
        "in topic '/Root/PQ/" + TopicName + "'",
        "in current cluster 'dc1'",
    });
}

Y_UNIT_TEST(NoSelectRow) {
    auto server = MakeFederationServer();
    server.AnnoyingClient->CreateTopic(TopicName, 1);
    GrantTopicRead(server, "/Root/PQ/" + TopicName, ReaderTicket);

    auto stub = MakeTopicStub(server);
    auto session = TStreamReadSession::Open(*stub, "/Root", StrangerTicket);
    WriteInit(*session, {TopicShortName}, "user", {0});
    AssertClose(ReadNext(*session), Ydb::StatusIds::UNAUTHORIZED, {
        "No ReadTopic permissions",
        StrangerTicket,
    });
}

Y_UNIT_TEST(NoReadAsConsumer) {
    auto server = MakeFederationServer();
    server.AnnoyingClient->CreateTopic(TopicName, 1);
    server.AnnoyingClient->CreateConsumer("user");
    GrantTopicRead(server, "/Root/PQ/" + TopicName, ReaderTicket);

    auto stub = MakeTopicStub(server);
    auto session = TStreamReadSession::Open(*stub, "/Root", ReaderTicket);
    WriteInit(*session, {TopicShortName}, "user", {0});
    AssertClose(ReadNext(*session), Ydb::StatusIds::UNAUTHORIZED, {
        "No ReadAsConsumer permissions",
        ReaderTicket,
    });
}

Y_UNIT_TEST(OneOfTwoTopicsMissing) {
    auto server = MakeFederationServer();
    server.AnnoyingClient->CreateTopic(TopicName, 1);
    auto stub = MakeTopicStub(server);
    auto session = TStreamReadSession::Open(*stub);
    WriteInit(*session, {TopicShortName, "missing-topic"}, "user", {0});

    auto resp = ReadNext(*session);
    UNIT_ASSERT_VALUES_EQUAL_C(
        resp.server_message_case(),
        Ydb::Topic::StreamReadMessage::FromServer::kInitResponse,
        resp);

    resp = ReadNext(*session);
    UNIT_ASSERT_VALUES_EQUAL_C(
        resp.server_message_case(),
        Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest,
        resp);
    const auto& partition = resp.start_partition_session_request().partition_session();
    UNIT_ASSERT_STRING_CONTAINS(partition.path(), TopicShortName);
    UNIT_ASSERT_C(!partition.path().Contains("missing-topic"), partition.path());

    auto second = std::async(std::launch::async, [&] {
        Ydb::Topic::StreamReadMessage::FromServer next;
        const bool ok = session->Stream->Read(&next);
        return std::make_pair(ok, next);
    });
    if (second.wait_for(std::chrono::seconds(2)) == std::future_status::ready) {
        auto [ok, next] = second.get();
        UNIT_ASSERT_C(ok, "stream closed after the live topic partition session");
        UNIT_ASSERT_C(
            next.server_message_case() != Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest,
            next);
        UNIT_ASSERT_VALUES_EQUAL_C(next.status(), Ydb::StatusIds::SUCCESS, next);
    } else {
        session->Context.TryCancel();
        second.wait();
    }
}

Y_UNIT_TEST(CdcStreamReadUsesFeedPath) {
    auto settings = NYdb::NTopic::NTests::TTopicSdkTestSetup::MakeServerSettings();
    NPersQueue::TTestServer server(settings);
    server.EnableLogs({NKikimrServices::PQ_READ_PROXY, NKikimrServices::PQ_METACACHE});
    server.AnnoyingClient->RunYqlSchemeQuery(
        "CREATE TABLE `/Root/table_cdc` (id Uint64, PRIMARY KEY (id));");
    server.AnnoyingClient->RunYqlSchemeQuery(
        "ALTER TABLE `/Root/table_cdc` ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES');");

    auto stub = MakeTopicStub(server);
    auto session = TStreamReadSession::Open(*stub);
    WriteInit(*session, {"/Root/table_cdc/feed"}, "", {0});

    auto resp = ReadNext(*session);
    UNIT_ASSERT_VALUES_EQUAL_C(
        resp.server_message_case(),
        Ydb::Topic::StreamReadMessage::FromServer::kInitResponse,
        resp);

    resp = ReadNext(*session);
    UNIT_ASSERT_VALUES_EQUAL_C(
        resp.server_message_case(),
        Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest,
        resp);
    const TString path = resp.start_partition_session_request().partition_session().path();
    UNIT_ASSERT_STRING_CONTAINS(path, "table_cdc/feed");
    UNIT_ASSERT_C(!path.Contains("streamImpl"), path);
}

} // Y_UNIT_TEST_SUITE(ReadInitAuth)

} // namespace NKikimr::NPersQueueTests
