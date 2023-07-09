#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/data_plane_helpers.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <ydb/library/services/services.pb.h>

#include <util/stream/output.h>
#include <util/string/builder.h>

#include <library/cpp/testing/unittest/registar.h>

#include "pq_data_writer.h"

namespace NKikimr::NPersQueueTests {

Y_UNIT_TEST_SUITE(DemoTx) {

class TTopicNameConstructor {
public:
    TTopicNameConstructor(const TString& dc,
                          const TString& database);

    TString GetTopicParent() const;
    TString GetTopicPath(const TString& topicName) const;
    TString GetFullTopicName(const TString& topicName) const;

private:
    TString Dc;
    TString Database;
};

TTopicNameConstructor::TTopicNameConstructor(const TString& dc,
                                             const TString& database) :
    Dc(dc),
    Database(database)
{
}

TString TTopicNameConstructor::GetTopicParent() const
{
    return Database + "/PQ";
}

TString TTopicNameConstructor::GetTopicPath(const TString& topicName) const
{
    return GetTopicParent() + "/" + GetFullTopicName(topicName);
}

TString TTopicNameConstructor::GetFullTopicName(const TString& topicName) const
{
    return "rt3." + Dc + "--" + topicName;
}

class TTxFixture : public NUnitTest::TBaseFixture {
protected:
    TTxFixture();

    void SetUp(NUnitTest::TTestContext&) override;

    void CreateTestServer();
    void CreateTopic(const TString& topicPath);
    void CreateTopicStub();
    void CreateTable(const TString& parent, const TString& name);

    NYdb::NTable::TSession CreateSession(const TString& authToken);
    NYdb::NTable::TTransaction BeginTx(NYdb::NTable::TSession& session);
    void CommitTx(NYdb::NTable::TTransaction& tx, NYdb::EStatus status);

    using TTopicReadSession = NYdb::NPersQueue::IReadSession;
    using TTopicReadSessionPtr = std::shared_ptr<TTopicReadSession>;

    TTopicReadSessionPtr CreateTopicReadSession(const TString& topic,
                                                const TString& consumer);
    void Wait_CreatePartitionStreamEvent(TTopicReadSession& reader,
                                         ui64 committedOffset);
    void Wait_DataReceivedEvent(TTopicReadSession& reader,
                                ui64 offset);

    void Call_UpdateOffsetsInTransaction(const NYdb::NTable::TTransaction& tx,
                                         const TString& consumer,
                                         const TString& topicPath,
                                         ui64 rangeBegin,
                                         ui64 rangeEnd);

    void ExecSQL(const NYdb::NTable::TTransaction& tx,
                 const TString& query,
                 NYdb::EStatus status);

    void Ensure_In_Table(const TString& tablePath,
                         const THashMap<i64, i64>& values);

    const TString CONSUMER = "user";
    const TString SHORT_TOPIC_NAME = "demo";
    const TString DC = "dc1";
    const TString FULL_TOPIC_NAME = "rt3." + DC + "--" + SHORT_TOPIC_NAME;
    const TString AUTH_TOKEN = "x-user-x@builtin";
    const TString DATABASE = "/Root";
    const TString TOPIC_PARENT = DATABASE + "/PQ";
    const TString TOPIC_PATH = TOPIC_PARENT + "/" + FULL_TOPIC_NAME;

    TMaybe<NPersQueue::TTestServer> Server;
    std::unique_ptr<Ydb::Topic::V1::TopicService::Stub> TopicStub;
    TTopicNameConstructor NameCtor;
};

TTxFixture::TTxFixture() :
    NameCtor(DC, DATABASE)
{
}

void TTxFixture::SetUp(NUnitTest::TTestContext&)
{
    CreateTestServer();

    CreateTopic(SHORT_TOPIC_NAME);
    CreateTopicStub();
}

void TTxFixture::CreateTestServer()
{
    auto settings = PQSettings(0)
        .SetDomainName("Root")
        .SetEnableTopicServiceTx(true);

    Server.ConstructInPlace(settings);
}

void TTxFixture::CreateTopic(const TString& topicName)
{
    const TString topicPath = NameCtor.GetTopicPath(topicName);
    const TString fullTopicName = NameCtor.GetFullTopicName(topicName);

    //
    // создать топик...
    //
    Server->AnnoyingClient->CreateTopicNoLegacy(topicPath, 1);

    NACLib::TDiffACL acl;
    acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericFull, AUTH_TOKEN);
    Server->AnnoyingClient->ModifyACL(NameCtor.GetTopicParent(),
                                      fullTopicName,
                                      acl.SerializeAsString());

    //
    // ...с несколькими сообщениями
    //
    TPQDataWriter writer("source-id", *Server);

    for (ui32 offset = 0; offset < 4; ++offset) {
        writer.Write(topicPath, {"data"}, false, AUTH_TOKEN);
    }
}

void TTxFixture::CreateTopicStub()
{
    auto channel = grpc::CreateChannel("localhost:" + ToString(Server->GrpcPort), grpc::InsecureChannelCredentials());
    TopicStub = Ydb::Topic::V1::TopicService::NewStub(channel);
}

void TTxFixture::CreateTable(const TString& parent, const TString& name)
{
    auto session = CreateSession("");

    auto result = session.ExecuteSchemeQuery(Sprintf(R"___(
        CREATE TABLE `%s/%s` (
            key Int64,
            value Int64,
            PRIMARY KEY (key)
        );
    )___", parent.data(), name.data())).ExtractValueSync();
    UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);

    NACLib::TDiffACL acl;
    acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericFull, AUTH_TOKEN);
    Server->AnnoyingClient->ModifyACL(parent,
                                      name,
                                      acl.SerializeAsString());

    Server->AnnoyingClient->ModifyACL("/",
                                      "Root",
                                      acl.SerializeAsString());
}

NYdb::NTable::TSession TTxFixture::CreateSession(const TString& authToken)
{
    NYdb::TDriverConfig config;
    config.SetEndpoint(TStringBuilder() << "localhost:" << Server->GrpcPort);
    config.SetDatabase(DATABASE);
    if (!authToken.empty()) {
        config.SetAuthToken(authToken);
    }

    NYdb::TDriver driver(config);
    NYdb::NTable::TClientSettings settings;
    NYdb::NTable::TTableClient client(driver, settings);

    auto result = client.CreateSession().ExtractValueSync();
    UNIT_ASSERT_EQUAL(result.IsTransportError(), false);

    return result.GetSession();
}

NYdb::NTable::TTransaction TTxFixture::BeginTx(NYdb::NTable::TSession& session)
{
    auto result = session.BeginTransaction().ExtractValueSync();
    UNIT_ASSERT_EQUAL(result.IsTransportError(), false);

    return result.GetTransaction();
}

void TTxFixture::CommitTx(NYdb::NTable::TTransaction& tx, NYdb::EStatus status)
{
    auto result = tx.Commit().ExtractValueSync();
    UNIT_ASSERT_EQUAL(result.IsTransportError(), false);

    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), status);
}

auto TTxFixture::CreateTopicReadSession(const TString& topic,
                                        const TString& consumer) -> TTopicReadSessionPtr
{
    NYdb::NPersQueue::TReadSessionSettings settings;
    settings.AppendTopics(topic);
    settings.ConsumerName(consumer);
    settings.ReadOriginal({DC});

    return CreateReader(*Server->AnnoyingClient->GetDriver(), settings);
}

template<class E>
E ReadEvent(NYdb::NPersQueue::IReadSession& reader, bool block, size_t count)
{
    auto msg = reader.GetEvent(block, count);
    UNIT_ASSERT(msg);

    auto ev = std::get_if<E>(&*msg);
    UNIT_ASSERT(ev);

    return *ev;
}

void TTxFixture::Wait_CreatePartitionStreamEvent(TTopicReadSession& reader,
                                                 ui64 committedOffset)
{
    auto event = ReadEvent<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(reader, true, 1);
    Cerr << "TCreatePartitionStreamEvent: " << event.DebugString() << Endl;

    UNIT_ASSERT_VALUES_EQUAL(event.GetCommittedOffset(), committedOffset);

    event.Confirm();
}

void TTxFixture::Wait_DataReceivedEvent(TTopicReadSession& reader,
                                        ui64 offset)
{
    auto event = ReadEvent<NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent>(reader, true, 1);
    Cerr << "TDataReceivedEvent: " << event.DebugString() << Endl;

    UNIT_ASSERT_VALUES_EQUAL(event.GetMessages()[0].GetOffset(), offset);
}

void TTxFixture::Call_UpdateOffsetsInTransaction(const NYdb::NTable::TTransaction& tx,
                                                 const TString& consumer,
                                                 const TString& topicPath,
                                                 ui64 rangeBegin,
                                                 ui64 rangeEnd)
{
    grpc::ClientContext rcontext;
    rcontext.AddMetadata("x-ydb-auth-ticket", AUTH_TOKEN);
    rcontext.AddMetadata("x-ydb-database", DATABASE);

    Ydb::Topic::UpdateOffsetsInTransactionRequest request;
    Ydb::Topic::UpdateOffsetsInTransactionResponse response;

    request.mutable_tx()->set_id(tx.GetId());
    request.mutable_tx()->set_session(tx.GetSession().GetId());
    request.set_consumer(consumer);

    auto *topic = request.mutable_topics()->Add();
    topic->set_path(topicPath);

    auto *partition = topic->mutable_partitions()->Add();
    partition->set_partition_id(0);

    auto *range = partition->mutable_partition_offsets()->Add();
    range->set_start(rangeBegin);
    range->set_end(rangeEnd);

    grpc::Status status = TopicStub->UpdateOffsetsInTransaction(&rcontext,
                                                                request,
                                                                &response);
    UNIT_ASSERT(status.ok());

    UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
}

void TTxFixture::ExecSQL(const NYdb::NTable::TTransaction& tx,
                         const TString& query,
                         NYdb::EStatus status)
{
    auto result =
        tx.GetSession().ExecuteDataQuery(query,
                                         NYdb::NTable::TTxControl::Tx(tx)).ExtractValueSync();
    UNIT_ASSERT_EQUAL(result.IsTransportError(), false);

    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), status);
}

void TTxFixture::Ensure_In_Table(const TString& tablePath,
                                 const THashMap<i64, i64>& values)
{
    auto query = Sprintf(R"___(
        SELECT key, value FROM `%s`
    )___", tablePath.data());

    auto session = CreateSession("");

    auto result = session.ExecuteDataQuery(query,
                                           NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
    UNIT_ASSERT_EQUAL(result.IsTransportError(), false);

    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);

    auto rs = result.GetResultSetParser(0);
    UNIT_ASSERT_VALUES_EQUAL(rs.RowsCount(), values.size());

    for (size_t i = 0; i < values.size(); ++i) {
        UNIT_ASSERT(rs.TryNextRow());

        auto key = rs.ColumnParser("key").GetOptionalInt64();
        UNIT_ASSERT(key.Defined());

        auto value = rs.ColumnParser("value").GetOptionalInt64();
        UNIT_ASSERT(value.Defined());

        auto p = values.find(*key);
        UNIT_ASSERT(p != values.end());

        UNIT_ASSERT_VALUES_EQUAL(*value, p->second);
    }
}

Y_UNIT_TEST_F(Scenario_1, TTxFixture)
{
    {
        auto reader = CreateTopicReadSession(SHORT_TOPIC_NAME, CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader, 0);

        NYdb::NTable::TSession session = CreateSession(AUTH_TOKEN);
        NYdb::NTable::TTransaction tx = BeginTx(session);

        Wait_DataReceivedEvent(*reader, 0);
        Wait_DataReceivedEvent(*reader, 1);

        Call_UpdateOffsetsInTransaction(tx, CONSUMER, TOPIC_PATH, 0, 2);

        CommitTx(tx, NYdb::EStatus::SUCCESS);
    }

    {
        auto reader = CreateTopicReadSession(SHORT_TOPIC_NAME, CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader, 2);

        NYdb::NTable::TSession session = CreateSession(AUTH_TOKEN);
        NYdb::NTable::TTransaction tx = BeginTx(session);

        Wait_DataReceivedEvent(*reader, 2);
        Wait_DataReceivedEvent(*reader, 3);

        Call_UpdateOffsetsInTransaction(tx, CONSUMER, TOPIC_PATH, 2, 4);
    }

    {
        auto reader = CreateTopicReadSession(SHORT_TOPIC_NAME, CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader, 2);
    }
}

Y_UNIT_TEST_F(Scenario_2, TTxFixture)
{
    NYdb::NTable::TSession s1 = CreateSession(AUTH_TOKEN);
    NYdb::NTable::TTransaction t1 = BeginTx(s1);

    {
        auto reader = CreateTopicReadSession(SHORT_TOPIC_NAME, CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader, 0);

        Wait_DataReceivedEvent(*reader, 0);
        Wait_DataReceivedEvent(*reader, 1);
        Wait_DataReceivedEvent(*reader, 2);

        Call_UpdateOffsetsInTransaction(t1, CONSUMER, TOPIC_PATH, 0, 3);
    }

    NYdb::NTable::TSession s2 = CreateSession(AUTH_TOKEN);
    NYdb::NTable::TTransaction t2 = BeginTx(s2);

    {
        auto reader = CreateTopicReadSession(SHORT_TOPIC_NAME, CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader, 0);

        Wait_DataReceivedEvent(*reader, 0);
        Wait_DataReceivedEvent(*reader, 1);

        Call_UpdateOffsetsInTransaction(t2, CONSUMER, TOPIC_PATH, 0, 2);
    }

    CommitTx(t2, NYdb::EStatus::SUCCESS);
    CommitTx(t1, NYdb::EStatus::ABORTED);
}

Y_UNIT_TEST_F(Scenario_3, TTxFixture)
{
    CreateTopic("demo_1");
    CreateTopic("demo_2");

    {
        auto reader_1 = CreateTopicReadSession("demo_1", CONSUMER);
        auto reader_2 = CreateTopicReadSession("demo_2", CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader_1, 0);
        Wait_CreatePartitionStreamEvent(*reader_2, 0);

        NYdb::NTable::TSession session = CreateSession(AUTH_TOKEN);
        NYdb::NTable::TTransaction tx = BeginTx(session);

        Wait_DataReceivedEvent(*reader_1, 0);
        Wait_DataReceivedEvent(*reader_1, 1);

        Wait_DataReceivedEvent(*reader_2, 0);

        Call_UpdateOffsetsInTransaction(tx, CONSUMER, NameCtor.GetTopicPath("demo_1"), 0, 2);
        Call_UpdateOffsetsInTransaction(tx, CONSUMER, NameCtor.GetTopicPath("demo_2"), 0, 1);

        CommitTx(tx, NYdb::EStatus::SUCCESS);
    }

    {
        auto reader_1 = CreateTopicReadSession("demo_1", CONSUMER);
        auto reader_2 = CreateTopicReadSession("demo_2", CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader_1, 2);
        Wait_CreatePartitionStreamEvent(*reader_2, 1);

        NYdb::NTable::TSession session = CreateSession(AUTH_TOKEN);
        NYdb::NTable::TTransaction tx = BeginTx(session);

        Wait_DataReceivedEvent(*reader_1, 2);

        Wait_DataReceivedEvent(*reader_2, 1);
        Wait_DataReceivedEvent(*reader_2, 2);

        Call_UpdateOffsetsInTransaction(tx, CONSUMER, NameCtor.GetTopicPath("demo_1"), 2, 3);
        Call_UpdateOffsetsInTransaction(tx, CONSUMER, NameCtor.GetTopicPath("demo_2"), 1, 3);
    }

    {
        auto reader_1 = CreateTopicReadSession("demo_1", CONSUMER);
        auto reader_2 = CreateTopicReadSession("demo_2", CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader_1, 2);
        Wait_CreatePartitionStreamEvent(*reader_2, 1);
    }
}

Y_UNIT_TEST_F(Scenario_4, TTxFixture)
{
    CreateTable("/Root", "table");

    {
        auto reader = CreateTopicReadSession("demo", CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader, 0);

        NYdb::NTable::TSession session = CreateSession(AUTH_TOKEN);
        NYdb::NTable::TTransaction tx = BeginTx(session);

        ExecSQL(tx, "SELECT key, value FROM `/Root/table` WHERE key = 1;",
                NYdb::EStatus::SUCCESS);

        Wait_DataReceivedEvent(*reader, 0);
        Wait_DataReceivedEvent(*reader, 1);

        ExecSQL(tx, "UPSERT INTO `/Root/table` (key, value) VALUES (1, 1);",
                NYdb::EStatus::SUCCESS);

        Call_UpdateOffsetsInTransaction(tx, CONSUMER, NameCtor.GetTopicPath("demo"), 0, 2);

        CommitTx(tx, NYdb::EStatus::SUCCESS);
    }

    {
        Ensure_In_Table("/Root/table", {{1, 1}});

        auto reader = CreateTopicReadSession("demo", CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader, 2);

        NYdb::NTable::TSession session = CreateSession(AUTH_TOKEN);
        NYdb::NTable::TTransaction tx = BeginTx(session);

        Wait_DataReceivedEvent(*reader, 2);

        ExecSQL(tx, "UPSERT INTO `/Root/table` (key, value) VALUES (2, 2);",
                NYdb::EStatus::SUCCESS);

        Call_UpdateOffsetsInTransaction(tx, CONSUMER, NameCtor.GetTopicPath("demo"), 2, 3);
    }

    {
        Ensure_In_Table("/Root/table", {{1, 1}});

        auto reader = CreateTopicReadSession("demo", CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader, 2);
    }
}

Y_UNIT_TEST_F(Scenario_5, TTxFixture)
{
    CreateTable("/Root", "table");

    NYdb::NTable::TSession s1 = CreateSession(AUTH_TOKEN);
    NYdb::NTable::TTransaction t1 = BeginTx(s1);

    {
        auto reader = CreateTopicReadSession(SHORT_TOPIC_NAME, CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader, 0);

        Wait_DataReceivedEvent(*reader, 0);
        Wait_DataReceivedEvent(*reader, 1);
        Wait_DataReceivedEvent(*reader, 2);

        ExecSQL(t1, "SELECT key, value FROM `/Root/table` WHERE key = 1;",
                NYdb::EStatus::SUCCESS);

        Call_UpdateOffsetsInTransaction(t1, CONSUMER, TOPIC_PATH, 0, 3);
    }

    NYdb::NTable::TSession s2 = CreateSession(AUTH_TOKEN);
    NYdb::NTable::TTransaction t2 = BeginTx(s2);

    {
        auto reader = CreateTopicReadSession(SHORT_TOPIC_NAME, CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader, 0);

        Wait_DataReceivedEvent(*reader, 0);
        Wait_DataReceivedEvent(*reader, 1);

        Call_UpdateOffsetsInTransaction(t2, CONSUMER, TOPIC_PATH, 0, 2);
    }

    CommitTx(t2, NYdb::EStatus::SUCCESS);
    CommitTx(t1, NYdb::EStatus::ABORTED);
}

}

}
