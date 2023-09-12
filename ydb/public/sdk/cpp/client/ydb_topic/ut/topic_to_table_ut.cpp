#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/ut_utils.h>

#include <library/cpp/logger/stream.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NTopic::NTests {

Y_UNIT_TEST_SUITE(TxUsage) {

class TFixture : public NUnitTest::TBaseFixture {
protected:
    void SetUp(NUnitTest::TTestContext&) override;

    NTable::TSession CreateSession();
    NTable::TTransaction BeginTx(NTable::TSession& session);
    void CommitTx(NTable::TTransaction& tx, EStatus status);

    using TTopicReadSession = NTopic::IReadSession;
    using TTopicReadSessionPtr = std::shared_ptr<TTopicReadSession>;

    TTopicReadSessionPtr CreateReader();

    void StartPartitionSession(TTopicReadSessionPtr reader, NTable::TTransaction& tx, ui64 offset);
    void StartPartitionSession(TTopicReadSessionPtr reader, ui64 offset);

    void ReadMessage(TTopicReadSessionPtr reader, NTable::TTransaction& tx, ui64 offset);

    void WriteMessage(const TString& data);

protected:
    const TDriver& GetDriver() const;

private:
    template<class E>
    E ReadEvent(TTopicReadSessionPtr reader, NTable::TTransaction& tx);
    template<class E>
    E ReadEvent(TTopicReadSessionPtr reader);

    std::unique_ptr<TTopicSdkTestSetup> Setup;
    std::unique_ptr<TDriver> Driver;
};

void TFixture::SetUp(NUnitTest::TTestContext&)
{
    NKikimr::Tests::TServerSettings settings = TTopicSdkTestSetup::MakeServerSettings();
    settings.SetEnableTopicServiceTx(true);
    Setup = std::make_unique<TTopicSdkTestSetup>(TEST_CASE_NAME, settings);

    Driver = std::make_unique<TDriver>(Setup->MakeDriver());
}

NTable::TSession TFixture::CreateSession()
{
    NTable::TTableClient client(GetDriver());
    auto result = client.CreateSession().ExtractValueSync();
    return result.GetSession();
}

NTable::TTransaction TFixture::BeginTx(NTable::TSession& session)
{
    auto result = session.BeginTransaction().ExtractValueSync();
    return result.GetTransaction();
}

void TFixture::CommitTx(NTable::TTransaction& tx, EStatus status)
{
    auto result = tx.Commit().ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), status);
}

auto TFixture::CreateReader() -> TTopicReadSessionPtr
{
    NTopic::TTopicClient client(GetDriver());
    TReadSessionSettings options;
    options.ConsumerName(TEST_CONSUMER);
    options.AppendTopics(TEST_TOPIC);
    return client.CreateReadSession(options);
}

void TFixture::StartPartitionSession(TTopicReadSessionPtr reader, NTable::TTransaction& tx, ui64 offset)
{
    auto event = ReadEvent<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(reader, tx);
    UNIT_ASSERT_VALUES_EQUAL(event.GetCommittedOffset(), offset);
    event.Confirm();
}

void TFixture::StartPartitionSession(TTopicReadSessionPtr reader, ui64 offset)
{
    auto event = ReadEvent<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(reader);
    UNIT_ASSERT_VALUES_EQUAL(event.GetCommittedOffset(), offset);
    event.Confirm();
}

void TFixture::ReadMessage(TTopicReadSessionPtr reader, NTable::TTransaction& tx, ui64 offset)
{
    auto event = ReadEvent<NTopic::TReadSessionEvent::TDataReceivedEvent>(reader, tx);
    UNIT_ASSERT_VALUES_EQUAL(event.GetMessages()[0].GetOffset(), offset);
}

template<class E>
E TFixture::ReadEvent(TTopicReadSessionPtr reader, NTable::TTransaction& tx)
{
    NTopic::TReadSessionGetEventSettings options;
    options.Block(true);
    options.MaxEventsCount(1);
    options.Tx(tx);

    auto event = reader->GetEvent(options);
    UNIT_ASSERT(event);

    auto ev = std::get_if<E>(&*event);
    UNIT_ASSERT(ev);

    return *ev;
}

template<class E>
E TFixture::ReadEvent(TTopicReadSessionPtr reader)
{
    auto event = reader->GetEvent(true, 1);
    UNIT_ASSERT(event);

    auto ev = std::get_if<E>(&*event);
    UNIT_ASSERT(ev);

    return *ev;
}

void TFixture::WriteMessage(const TString& data)
{
    NTopic::TWriteSessionSettings options;
    options.Path(TEST_TOPIC);
    options.MessageGroupId(TEST_MESSAGE_GROUP_ID);

    NTopic::TTopicClient client(GetDriver());
    auto session = client.CreateSimpleBlockingWriteSession(options);
    UNIT_ASSERT(session->Write(data));
    session->Close();
}

const TDriver& TFixture::GetDriver() const
{
    return *Driver;
}

Y_UNIT_TEST_F(SessionAbort, TFixture)
{
    {
        auto reader = CreateReader();
        auto session = CreateSession();
        auto tx = BeginTx(session);

        StartPartitionSession(reader, tx, 0);

        WriteMessage("message #0");
        ReadMessage(reader, tx, 0);

        WriteMessage("message #1");
        ReadMessage(reader, tx, 1);
    }

    {
        auto session = CreateSession();
        auto tx = BeginTx(session);
        auto reader = CreateReader();

        StartPartitionSession(reader, tx, 0);

        ReadMessage(reader, tx, 0);

        CommitTx(tx, EStatus::SUCCESS);
    }

    {
        auto reader = CreateReader();

        StartPartitionSession(reader, 2);
    }
}

Y_UNIT_TEST_F(TwoSessionOneConsumer, TFixture)
{
    WriteMessage("message #0");

    auto session1 = CreateSession();
    auto tx1 = BeginTx(session1);

    {
        auto reader = CreateReader();

        StartPartitionSession(reader, tx1, 0);

        ReadMessage(reader, tx1, 0);
    }

    auto session2 = CreateSession();
    auto tx2 = BeginTx(session2);

    {
        auto reader = CreateReader();

        StartPartitionSession(reader, tx2, 0);

        ReadMessage(reader, tx2, 0);
    }

    CommitTx(tx2, EStatus::SUCCESS);
    CommitTx(tx1, EStatus::ABORTED);
}

Y_UNIT_TEST_F(WriteToTopic, TFixture)
{
    NTopic::TWriteSessionSettings options;
    options.Path(TEST_TOPIC);
    options.MessageGroupId(TEST_MESSAGE_GROUP_ID);

    auto session = CreateSession();
    auto tx = BeginTx(session);

    auto writeMessages = [&](const TVector<TString>& messages) {
        NTopic::TTopicClient client(GetDriver());
        auto session = client.CreateSimpleBlockingWriteSession(options);

        for (auto& message : messages) {
            NTopic::TWriteMessage params(message);
            params.Tx(tx);
            UNIT_ASSERT(session->Write(std::move(params)));
        }

        UNIT_ASSERT(session->Close());
    };

    writeMessages({"a", "bb", "ccc", "dddd"});
    writeMessages({"eeeee", "ffffff", "ggggggg"});

    CommitTx(tx, EStatus::ABORTED);
}

}

}
