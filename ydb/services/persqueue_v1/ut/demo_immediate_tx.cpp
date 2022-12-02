#include <ydb/public/api/grpc/draft/ydb_topic_tx_v1.grpc.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/data_plane_helpers.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <ydb/core/protos/services.pb.h>

#include <util/stream/output.h>
#include <util/string/builder.h>

#include <library/cpp/testing/unittest/registar.h>

#include "pq_data_writer.h"

namespace NKikimr::NPersQueueTests {

Y_UNIT_TEST_SUITE(ImmediateTx) {

class TImmediateTxFixture : public NUnitTest::TBaseFixture {
protected:
    void SetUp(NUnitTest::TTestContext&) override;

    void CreateTestServer();
    void CreateTopic();
    void CreateTopicTxStub();

    NYdb::NTable::TSession CreateSession();
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

    void Call_AddOffsetsToTransaction(const TString& sessionId,
                                      const TString& txId,
                                      const TString& consumer,
                                      ui64 rangeBegin,
                                      ui64 rangeEnd);

    const TString CONSUMER = "user";
    const TString SHORT_TOPIC_NAME = "demo";
    const TString DC = "dc1";
    const TString FULL_TOPIC_NAME = "rt3." + DC + "--" + SHORT_TOPIC_NAME;
    const TString AUTH_TOKEN = "x-user-x@builtin";
    const TString DATABASE = "/Root";
    const TString TOPIC_PARENT = DATABASE + "/PQ";
    const TString TOPIC_PATH = TOPIC_PARENT + "/" + FULL_TOPIC_NAME;

    TMaybe<NPersQueue::TTestServer> Server;
    std::unique_ptr<Ydb::Topic::V1::TopicServiceTx::Stub> TopicTxStub;
};

void TImmediateTxFixture::SetUp(NUnitTest::TTestContext&)
{
    CreateTestServer();
    CreateTopic();
    CreateTopicTxStub();
}

void TImmediateTxFixture::CreateTestServer()
{
    Server.ConstructInPlace(PQSettings(0).SetDomainName("Root"));

    Server->EnableLogs({NKikimrServices::FLAT_TX_SCHEMESHARD
                       , NKikimrServices::PERSQUEUE});
}

void TImmediateTxFixture::CreateTopic()
{
    //
    // создать топик...
    //
    Server->AnnoyingClient->CreateTopicNoLegacy(TOPIC_PATH, 1);

    NACLib::TDiffACL acl;
    acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericFull, AUTH_TOKEN);
    Server->AnnoyingClient->ModifyACL(TOPIC_PARENT, FULL_TOPIC_NAME, acl.SerializeAsString());

    //
    // ...с несколькими сообщениями
    //
    TPQDataWriter writer("source-id", *Server);

    for (ui32 offset = 0; offset < 4; ++offset) {
        writer.Write(TOPIC_PATH, {"data"}, false, AUTH_TOKEN);
    }
}

void TImmediateTxFixture::CreateTopicTxStub()
{
    auto channel = grpc::CreateChannel("localhost:" + ToString(Server->GrpcPort), grpc::InsecureChannelCredentials());
    TopicTxStub = Ydb::Topic::V1::TopicServiceTx::NewStub(channel);
}

NYdb::NTable::TSession TImmediateTxFixture::CreateSession()
{
    NYdb::TDriverConfig config;
    config.SetEndpoint(TStringBuilder() << "localhost:" << Server->GrpcPort);
    config.SetDatabase(DATABASE);
    config.SetAuthToken(AUTH_TOKEN);

    NYdb::TDriver driver(config);
    NYdb::NTable::TClientSettings settings;
    NYdb::NTable::TTableClient client(driver, settings);

    auto result = client.CreateSession().ExtractValueSync();
    UNIT_ASSERT_EQUAL(result.IsTransportError(), false);

    return result.GetSession();
}

NYdb::NTable::TTransaction TImmediateTxFixture::BeginTx(NYdb::NTable::TSession& session)
{
    auto result = session.BeginTransaction().ExtractValueSync();
    UNIT_ASSERT_EQUAL(result.IsTransportError(), false);

    return result.GetTransaction();
}

void TImmediateTxFixture::CommitTx(NYdb::NTable::TTransaction& tx, NYdb::EStatus status)
{
    auto result = tx.Commit().ExtractValueSync();
    UNIT_ASSERT_EQUAL(result.IsTransportError(), false);

    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), status);
}

auto TImmediateTxFixture::CreateTopicReadSession(const TString& topic,
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

void TImmediateTxFixture::Wait_CreatePartitionStreamEvent(TTopicReadSession& reader,
                                                          ui64 committedOffset)
{
    auto event = ReadEvent<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(reader, true, 1);
    Cerr << "TCreatePartitionStreamEvent: " << event.DebugString() << Endl;

    UNIT_ASSERT_VALUES_EQUAL(event.GetCommittedOffset(), committedOffset);

    event.Confirm();
}

void TImmediateTxFixture::Wait_DataReceivedEvent(TTopicReadSession& reader,
                                                 ui64 offset)
{
    auto event = ReadEvent<NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent>(reader, true, 1);
    Cerr << "TDataReceivedEvent: " << event.DebugString() << Endl;

    UNIT_ASSERT_VALUES_EQUAL(event.GetMessages()[0].GetOffset(), offset);
}

void TImmediateTxFixture::Call_AddOffsetsToTransaction(const TString& sessionId,
                                                       const TString& txId,
                                                       const TString& consumer,
                                                       ui64 rangeBegin,
                                                       ui64 rangeEnd)
{
    grpc::ClientContext rcontext;
    rcontext.AddMetadata("x-ydb-auth-ticket", AUTH_TOKEN);
    rcontext.AddMetadata("x-ydb-database", DATABASE);

    Ydb::Topic::AddOffsetsToTransactionRequest request;
    Ydb::Topic::AddOffsetsToTransactionResponse response;

    request.set_session_id(sessionId);
    request.mutable_tx_control()->set_tx_id(txId);
    request.set_consumer(consumer);

    auto *topic = request.mutable_topics()->Add();
    topic->set_path(TOPIC_PATH);

    auto *partition = topic->mutable_partitions()->Add();
    partition->set_partition_id(0);

    auto *range = partition->mutable_partition_offsets()->Add();
    range->set_start(rangeBegin);
    range->set_end(rangeEnd);

    grpc::Status status = TopicTxStub->AddOffsetsToTransaction(&rcontext,
                                                               request,
                                                               &response);
    UNIT_ASSERT(status.ok());

    UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
}

Y_UNIT_TEST_F(Scenario_1, TImmediateTxFixture)
{
    {
        auto reader = CreateTopicReadSession(SHORT_TOPIC_NAME, CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader, 0);

        NYdb::NTable::TSession session = CreateSession();
        NYdb::NTable::TTransaction tx = BeginTx(session);

        Wait_DataReceivedEvent(*reader, 0);
        Wait_DataReceivedEvent(*reader, 1);

        Call_AddOffsetsToTransaction(session.GetId(), tx.GetId(), CONSUMER, 0, 2);

        CommitTx(tx, NYdb::EStatus::SUCCESS);
    }

    {
        auto reader = CreateTopicReadSession(SHORT_TOPIC_NAME, CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader, 2);

        NYdb::NTable::TSession session = CreateSession();
        NYdb::NTable::TTransaction tx = BeginTx(session);

        Wait_DataReceivedEvent(*reader, 2);
        Wait_DataReceivedEvent(*reader, 3);

        Call_AddOffsetsToTransaction(session.GetId(), tx.GetId(), CONSUMER, 2, 4);
    }

    {
        auto reader = CreateTopicReadSession(SHORT_TOPIC_NAME, CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader, 2);
    }
}

Y_UNIT_TEST_F(Scenario_2, TImmediateTxFixture)
{
    NYdb::NTable::TSession s1 = CreateSession();
    NYdb::NTable::TTransaction t1 = BeginTx(s1);

    {
        auto reader = CreateTopicReadSession(SHORT_TOPIC_NAME, CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader, 0);

        Wait_DataReceivedEvent(*reader, 0);
        Wait_DataReceivedEvent(*reader, 1);
        Wait_DataReceivedEvent(*reader, 2);

        Call_AddOffsetsToTransaction(s1.GetId(), t1.GetId(), CONSUMER, 0, 3);
    }

    NYdb::NTable::TSession s2 = CreateSession();
    NYdb::NTable::TTransaction t2 = BeginTx(s2);

    {
        auto reader = CreateTopicReadSession(SHORT_TOPIC_NAME, CONSUMER);

        Wait_CreatePartitionStreamEvent(*reader, 0);

        Wait_DataReceivedEvent(*reader, 0);
        Wait_DataReceivedEvent(*reader, 1);

        Call_AddOffsetsToTransaction(s2.GetId(), t2.GetId(), CONSUMER, 0, 2);
    }

    CommitTx(t2, NYdb::EStatus::SUCCESS);
    CommitTx(t1, NYdb::EStatus::ABORTED);
}

}

}
