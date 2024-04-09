#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/ut_utils.h>

#include <library/cpp/logger/stream.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/dbgtrace/debug_trace.h>

namespace NYdb::NTopic::NTests {

Y_UNIT_TEST_SUITE(TxUsage) {

class TFixture : public NUnitTest::TBaseFixture {
protected:
    using TTopicReadSession = NTopic::IReadSession;
    using TTopicReadSessionPtr = std::shared_ptr<TTopicReadSession>;
    using TTopicWriteSession = NTopic::IWriteSession;
    using TTopicWriteSessionPtr = std::shared_ptr<TTopicWriteSession>;

    struct TTopicWriteSessionContext {
        TTopicWriteSessionPtr Session;
        TMaybe<NTopic::TContinuationToken> ContinuationToken;

        void WaitForContinuationToken();
        void Write(const TString& message);
        void WaitForAck();
    };

    void SetUp(NUnitTest::TTestContext&) override;

    NTable::TSession CreateTableSession();
    NTable::TTransaction BeginTx(NTable::TSession& session);
    void CommitTx(NTable::TTransaction& tx, EStatus status = EStatus::SUCCESS);

    TTopicReadSessionPtr CreateReader();

    void StartPartitionSession(TTopicReadSessionPtr reader, NTable::TTransaction& tx, ui64 offset);
    void StartPartitionSession(TTopicReadSessionPtr reader, ui64 offset);

    void ReadMessage(TTopicReadSessionPtr reader, NTable::TTransaction& tx, ui64 offset);

    void WriteMessage(const TString& message);
    void WriteMessages(const TVector<TString>& messages,
                       const TString& topic, const TString& groupId,
                       NTable::TTransaction& tx);

    void CreateTopic(const TString& path = TEST_TOPIC,
                     const TString& consumer = TEST_CONSUMER,
                     size_t partitionCount = 1,
                     std::optional<size_t> maxPartitionCount = std::nullopt);

    void WriteToTopicWithInvalidTxId(bool invalidTxId);

    TTopicWriteSessionPtr CreateTopicWriteSession(const TString& topicPath,
                                                  const TString& messageGroupId);
    TTopicWriteSessionContext& GetTopicWriteSession(const TString& topicPath,
                                                    const TString& messageGroupId);

    TTopicReadSessionPtr CreateTopicReadSession(const TString& topicPath,
                                                const TString& consumerName);
    TTopicReadSessionPtr GetTopicReadSession(const TString& topicPath,
                                             const TString& consumerName);

    void WriteToTopic(const TString& topicPath,
                      const TString& messageGroupId,
                      const TString& message);
    void WriteToTopic(const TString& topicPath,
                      const TString& messageGroupId,
                      const TString& message,
                      NTable::TTransaction& tx);
    TVector<TString> ReadFromTopic(const TString& topicPath,
                                   const TString& consumerName,
                                   size_t count);

//    TMaybe<NTopic::TContinuationToken> WaitForContinuationToken(TTopicWriteSessionPtr session);
//    void Write(TTopicWriteSessionPtr session,
//               NTopic::TContinuationToken&& continuationToken,
//               const TString& message);
//    void WaitForAck(TTopicWriteSessionPtr session);

protected:
    const TDriver& GetDriver() const;

private:
    template<class E>
    E ReadEvent(TTopicReadSessionPtr reader, NTable::TTransaction& tx);
    template<class E>
    E ReadEvent(TTopicReadSessionPtr reader);

    std::unique_ptr<TTopicSdkTestSetup> Setup;
    std::unique_ptr<TDriver> Driver;

    THashMap<TString, TTopicWriteSessionContext> TopicWriteSessions;
    THashMap<TString, TTopicReadSessionPtr> TopicReadSessions;
};

void TFixture::SetUp(NUnitTest::TTestContext&)
{
    NKikimr::Tests::TServerSettings settings = TTopicSdkTestSetup::MakeServerSettings();
    settings.SetEnableTopicServiceTx(true);
    Setup = std::make_unique<TTopicSdkTestSetup>(TEST_CASE_NAME, settings);

    Driver = std::make_unique<TDriver>(Setup->MakeDriver());
}

NTable::TSession TFixture::CreateTableSession()
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

void TFixture::WriteMessage(const TString& message)
{
    NTopic::TWriteSessionSettings options;
    options.Path(TEST_TOPIC);
    options.MessageGroupId(TEST_MESSAGE_GROUP_ID);

    NTopic::TTopicClient client(GetDriver());
    auto session = client.CreateSimpleBlockingWriteSession(options);
    UNIT_ASSERT(session->Write(message));
    session->Close();
}

void TFixture::WriteMessages(const TVector<TString>& messages,
                             const TString& topic, const TString& groupId,
                             NTable::TTransaction& tx)
{
    NTopic::TWriteSessionSettings options;
    options.Path(topic);
    options.MessageGroupId(groupId);

    NTopic::TTopicClient client(GetDriver());
    auto session = client.CreateSimpleBlockingWriteSession(options);

    for (auto& message : messages) {
        NTopic::TWriteMessage params(message);
        params.Tx(tx);
        UNIT_ASSERT(session->Write(std::move(params)));
    }

    UNIT_ASSERT(session->Close());
}

void TFixture::CreateTopic(const TString& path,
                           const TString& consumer,
                           size_t partitionCount,
                           std::optional<size_t> maxPartitionCount)

{
    Setup->CreateTopic(path, consumer, partitionCount, maxPartitionCount);
}

const TDriver& TFixture::GetDriver() const
{
    return *Driver;
}

void TFixture::WriteToTopicWithInvalidTxId(bool invalidTxId)
{
    auto tableSession = CreateTableSession();
    auto tx = BeginTx(tableSession);

    NTopic::TWriteSessionSettings options;
    options.Path(TEST_TOPIC);
    options.MessageGroupId(TEST_MESSAGE_GROUP_ID);

    NTopic::TTopicClient client(GetDriver());
    auto writeSession = client.CreateWriteSession(options);

    auto event = writeSession->GetEvent(true);
    UNIT_ASSERT(event.Defined() && std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event.GetRef()));
    auto token = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event.GetRef()).ContinuationToken);

    NTopic::TWriteMessage params("message");
    params.Tx(tx);

    if (invalidTxId) {
        CommitTx(tx, EStatus::SUCCESS);
    } else {
        UNIT_ASSERT(tableSession.Close().ExtractValueSync().IsSuccess());
    }

    writeSession->Write(std::move(token), std::move(params));

    while (true) {
        event = writeSession->GetEvent(true);
        UNIT_ASSERT(event.Defined());
        auto& v = event.GetRef();
        if (auto e = std::get_if<TWriteSessionEvent::TAcksEvent>(&v); e) {
            UNIT_ASSERT(false);
        } else if (auto e = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&v); e) {
            ;
        } else if (auto e = std::get_if<TSessionClosedEvent>(&v); e) {
            break;
        }
    }
}

Y_UNIT_TEST_F(SessionAbort, TFixture)
{
    {
        auto reader = CreateReader();
        auto session = CreateTableSession();
        auto tx = BeginTx(session);

        StartPartitionSession(reader, tx, 0);

        WriteMessage("message #0");
        ReadMessage(reader, tx, 0);

        WriteMessage("message #1");
        ReadMessage(reader, tx, 1);
    }

    {
        auto session = CreateTableSession();
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

    auto session1 = CreateTableSession();
    auto tx1 = BeginTx(session1);

    {
        auto reader = CreateReader();

        StartPartitionSession(reader, tx1, 0);

        ReadMessage(reader, tx1, 0);
    }

    auto session2 = CreateTableSession();
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
    TString topic[2] = {
        TEST_TOPIC,
        TEST_TOPIC + "_2"
    };

    CreateTopic(topic[1]);

    auto session = CreateTableSession();
    auto tx = BeginTx(session);

    WriteMessages({"#1", "#2", "#3"}, topic[0], TEST_MESSAGE_GROUP_ID, tx);
    WriteMessages({"#4", "#5"}, topic[1], TEST_MESSAGE_GROUP_ID, tx);

    CommitTx(tx, EStatus::ABORTED);
}

Y_UNIT_TEST_F(WriteToTopic_Invalid_Session, TFixture)
{
    WriteToTopicWithInvalidTxId(false);
}

Y_UNIT_TEST_F(WriteToTopic_Invalid_Tx, TFixture)
{
    WriteToTopicWithInvalidTxId(true);
}

Y_UNIT_TEST_F(WriteToTopic_Two_WriteSession, TFixture)
{
    TString topicPath[2] = {
        TEST_TOPIC,
        TEST_TOPIC + "_2"
    };

    CreateTopic(topicPath[1]);

    auto createWriteSession = [](NTopic::TTopicClient& client, const TString& topicPath) {
        NTopic::TWriteSessionSettings options;
        options.Path(topicPath);
        options.MessageGroupId(TEST_MESSAGE_GROUP_ID);

        return client.CreateWriteSession(options);
    };

    auto writeMessage = [](auto& ws, const TString& message, auto& tx) {
        NTopic::TWriteMessage params(message);
        params.Tx(tx);

        auto event = ws->GetEvent(true);
        UNIT_ASSERT(event.Defined() && std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event.GetRef()));
        auto token = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event.GetRef()).ContinuationToken);

        ws->Write(std::move(token), std::move(params));
    };

    auto tableSession = CreateTableSession();
    auto tx = BeginTx(tableSession);

    NTopic::TTopicClient client(GetDriver());

    auto ws0 = createWriteSession(client, topicPath[0]);
    auto ws1 = createWriteSession(client, topicPath[1]);

    writeMessage(ws0, "message-1", tx);
    writeMessage(ws1, "message-2", tx);

    size_t acks = 0;

    while (acks < 2) {
        auto event = ws0->GetEvent(false);
        if (!event) {
            event = ws1->GetEvent(false);
            if (!event) {
                Sleep(TDuration::MilliSeconds(10));
                continue;
            }
        }

        auto& v = event.GetRef();
        if (auto e = std::get_if<TWriteSessionEvent::TAcksEvent>(&v); e) {
            ++acks;
        } else if (auto e = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&v); e) {
            ;
        } else if (auto e = std::get_if<TSessionClosedEvent>(&v); e) {
            break;
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(acks, 2);
}

auto TFixture::CreateTopicWriteSession(const TString& topicPath,
                                       const TString& messageGroupId) -> TTopicWriteSessionPtr
{
    NTopic::TTopicClient client(GetDriver());
    NTopic::TWriteSessionSettings options;
    options.Path(topicPath);
    options.MessageGroupId(messageGroupId);
    return client.CreateWriteSession(options);
}

auto TFixture::GetTopicWriteSession(const TString& topicPath,
                                    const TString& messageGroupId) -> TTopicWriteSessionContext&
{
    auto i = TopicWriteSessions.find(topicPath);

    if (i == TopicWriteSessions.end()) {
        TTopicWriteSessionContext context;
        context.Session = CreateTopicWriteSession(topicPath, messageGroupId);

        TopicWriteSessions.emplace(topicPath, std::move(context));

        i = TopicWriteSessions.find(topicPath);
    }

    return i->second;
}

auto TFixture::CreateTopicReadSession(const TString& topicPath,
                                      const TString& consumerName) -> TTopicReadSessionPtr
{
    NTopic::TTopicClient client(GetDriver());
    NTopic::TReadSessionSettings options;
    options.AppendTopics(topicPath);
    options.ConsumerName(consumerName);
    return client.CreateReadSession(options);
}

auto TFixture::GetTopicReadSession(const TString& topicPath,
                                   const TString& consumerName) -> TTopicReadSessionPtr
{
    TTopicReadSessionPtr session;

    if (auto i = TopicReadSessions.find(topicPath); i == TopicReadSessions.end()) {
        session = CreateTopicReadSession(topicPath, consumerName);
        auto event = ReadEvent<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(session);
        event.Confirm();
        TopicReadSessions.emplace(topicPath, session);
    } else {
        session = i->second;
    }

    return session;
}

void TFixture::TTopicWriteSessionContext::WaitForContinuationToken()
{
    DBGTRACE("TFixture::TTopicWriteSessionContext::WaitForContinuationToken");

    while (!ContinuationToken.Defined()) {
        Session->WaitEvent().Wait();
        for (auto& event : Session->GetEvents()) {
            if (auto* e = std::get_if<NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                DBGTRACE_LOG("ReadyToAccept");
                ContinuationToken = std::move(e->ContinuationToken);
            } else if (auto* e = std::get_if<NTopic::TWriteSessionEvent::TAcksEvent>(&event)) {
                DBGTRACE_LOG("Acks");
                for (auto& ack : e->Acks) {
                    DBGTRACE_LOG("SeqNo: " << ack.SeqNo << ", State: " << (int)ack.State);
                }
            } else if (auto* e = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
                DBGTRACE_LOG("SessionClosed");
                UNIT_FAIL("");
            }
        }
    }
}

void TFixture::TTopicWriteSessionContext::Write(const TString& message)
{
    DBGTRACE("TFixture::TTopicWriteSessionContext::Write");
    NTopic::TWriteMessage params(message);

    Session->Write(std::move(*ContinuationToken),
                   std::move(params));
    ContinuationToken = Nothing();
}

void TFixture::TTopicWriteSessionContext::WaitForAck()
{
    DBGTRACE("TFixture::TTopicWriteSessionContext::WaitForAck");
    while (true) {
        Session->WaitEvent().Wait();
        for (auto& event : Session->GetEvents()) {
            if (auto* e = std::get_if<NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                DBGTRACE_LOG("ReadyToAccept");
                ContinuationToken = std::move(e->ContinuationToken);
            } else if (auto* e = std::get_if<NTopic::TWriteSessionEvent::TAcksEvent>(&event)) {
                DBGTRACE_LOG("Acks");
                for (auto& ack : e->Acks) {
                    DBGTRACE_LOG("SeqNo: " << ack.SeqNo << ", State: " << (int)ack.State);
                }
                return;
            } else if (auto* e = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
                DBGTRACE_LOG("SessionClosed");
                UNIT_FAIL("");
            }
        }
    }
}

void TFixture::WriteToTopic(const TString& topicPath,
                            const TString& messageGroupId,
                            const TString& message)
{
    DBGTRACE("TFixture::WriteToTopic");
    TTopicWriteSessionContext& context = GetTopicWriteSession(topicPath, messageGroupId);

    context.WaitForContinuationToken();
    UNIT_ASSERT(context.ContinuationToken.Defined());
    context.Write(message);
    context.WaitForAck();
}

//void TFixture::WriteToTopic(const TString& topicPath,
//                            const TString& messageGroupId,
//                            const TString& message,
//                            NTable::TTransaction& tx)
//{
//    auto session = GetTopicWriteSession(topicPath, messageGroupId);
//
//    NTopic::TWriteMessage params(message);
//    params.Tx(tx);
//
//    session->Write(std::move(params));
//}

TVector<TString> TFixture::ReadFromTopic(const TString& topicPath, const TString& consumerName, size_t count)
{
    DBGTRACE("TFixture::ReadFromTopic");
    TVector<TString> messages;

    auto session = GetTopicReadSession(topicPath, consumerName);

    while (count > 0) {
        auto event = session->GetEvent(true, count);
        UNIT_ASSERT(event);

        auto ev = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event);
        UNIT_ASSERT(ev);

        for (auto& m : ev->GetMessages()) {
            messages.push_back(m.GetData());
        }

        count -= ev->GetMessages().size();
    }

    return messages;
}

Y_UNIT_TEST_F(WriteToTopic_Demo, TFixture)
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "Лидер");
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "бодро");
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "гордо");
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "бредил");

    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "Тарту");
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "дорог");
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "как");
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "город");
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "утрат");

    TVector<TString> messages;

    messages = ReadFromTopic("topic_A", TEST_CONSUMER, 4);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 4);

    messages = ReadFromTopic("topic_B", TEST_CONSUMER, 5);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 5);
}

}

}
