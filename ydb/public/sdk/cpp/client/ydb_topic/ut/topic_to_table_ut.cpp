#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/ut_utils.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/key.h>
#include <ydb/core/persqueue/blob.h>

#include <ydb/core/tx/long_tx_service/public/events.h>

#include <library/cpp/logger/stream.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NTopic::NTests {

const auto TEST_MESSAGE_GROUP_ID_1 = TEST_MESSAGE_GROUP_ID + "_1";
const auto TEST_MESSAGE_GROUP_ID_2 = TEST_MESSAGE_GROUP_ID + "_2";

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
        size_t WriteCount = 0;
        size_t WrittenAckCount = 0;
        size_t WrittenInTxAckCount = 0;

        void WaitForContinuationToken();
        void Write(const TString& message, NTable::TTransaction* tx = nullptr);

        size_t AckCount() const { return WrittenAckCount + WrittenInTxAckCount; }

        void WaitForEvent();
    };

    void SetUp(NUnitTest::TTestContext&) override;

    NTable::TSession CreateTableSession();
    NTable::TTransaction BeginTx(NTable::TSession& session);
    void CommitTx(NTable::TTransaction& tx, EStatus status = EStatus::SUCCESS);
    void RollbackTx(NTable::TTransaction& tx, EStatus status = EStatus::SUCCESS);

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
                                                  const TString& messageGroupId,
                                                  TMaybe<ui32> partitionId);
    TTopicWriteSessionContext& GetTopicWriteSession(const TString& topicPath,
                                                    const TString& messageGroupId,
                                                    TMaybe<ui32> partitionId);

    TTopicReadSessionPtr CreateTopicReadSession(const TString& topicPath,
                                                const TString& consumerName,
                                                TMaybe<ui32> partitionId);
    TTopicReadSessionPtr GetTopicReadSession(const TString& topicPath,
                                             const TString& consumerName,
                                             TMaybe<ui32> partitionId);

    void WriteToTopic(const TString& topicPath,
                      const TString& messageGroupId,
                      const TString& message,
                      NTable::TTransaction* tx = nullptr,
                      TMaybe<ui32> partitionId = Nothing());
    TVector<TString> ReadFromTopic(const TString& topicPath,
                                   const TString& consumerName,
                                   const TDuration& duration,
                                   NTable::TTransaction* tx = nullptr,
                                   TMaybe<ui32> partitionId = Nothing());
    void WaitForAcks(const TString& topicPath,
                     const TString& messageGroupId,
                     size_t writtenInTxCount = Max<size_t>());
    void WaitForSessionClose(const TString& topicPath,
                             const TString& messageGroupId,
                             NYdb::EStatus status);
    void CloseTopicWriteSession(const TString& topicPath,
                                const TString& messageGroupId);

    enum EEndOfTransaction {
        Commit,
        Rollback,
        CloseTableSession
    };

    struct TTransactionCompletionTestDescription {
        TVector<TString> Topics;
        EEndOfTransaction EndOfTransaction = Commit;
    };

    void TestTheCompletionOfATransaction(const TTransactionCompletionTestDescription& d);
    void RestartLongTxService();
    void RestartPQTablet(const TString& topicPath, ui32 partition);
    void DumpPQTabletKeys(const TString& topicName, ui32 partition);

    void DeleteSupportivePartition(const TString& topicName,
                                   ui32 partition);

    struct TTableRecord {
        TTableRecord() = default;
        TTableRecord(const TString& key, const TString& value);

        TString Key;
        TString Value;
    };

    TVector<TTableRecord> MakeTableRecords();
    TString MakeJsonDoc(const TVector<TTableRecord>& records);

    void CreateTable(const TString& path);
    void WriteToTable(const TString& tablePath,
                      const TVector<TTableRecord>& records,
                      NTable::TTransaction* tx);
    size_t GetTableRecordsCount(const TString& tablePath);

    enum ERestartPQTabletMode {
        ERestartNo,
        ERestartBeforeCommit,
        ERestartAfterCommit,
    };

    struct TTestTxWithBigBlobsParams {
        size_t OldHeadCount = 0;
        size_t BigBlobsCount = 2;
        size_t NewHeadCount = 0;
        ERestartPQTabletMode RestartMode = ERestartNo;
    };

    void TestTxWithBigBlobs(const TTestTxWithBigBlobsParams& params);

    const TDriver& GetDriver() const;

    void CheckTabletKeys(const TString& topicName);
    void DumpPQTabletKeys(const TString& topicName);

private:
    template<class E>
    E ReadEvent(TTopicReadSessionPtr reader, NTable::TTransaction& tx);
    template<class E>
    E ReadEvent(TTopicReadSessionPtr reader);

    ui64 GetTopicTabletId(const TActorId& actorId,
                          const TString& topicPath,
                          ui32 partition);
    TVector<TString> GetTabletKeys(const TActorId& actorId,
                                   ui64 tabletId);
    NPQ::TWriteId GetTransactionWriteId(const TActorId& actorId,
                                        ui64 tabletId);
    void SendLongTxLockStatus(const TActorId& actorId,
                              ui64 tabletId,
                              const NPQ::TWriteId& writeId,
                              NKikimrLongTxService::TEvLockStatus::EStatus status);
    void WaitForTheTabletToDeleteTheWriteInfo(const TActorId& actorId,
                                              ui64 tabletId,
                                              const NPQ::TWriteId& writeId);

    std::unique_ptr<TTopicSdkTestSetup> Setup;
    std::unique_ptr<TDriver> Driver;

    THashMap<std::pair<TString, TString>, TTopicWriteSessionContext> TopicWriteSessions;
    THashMap<TString, TTopicReadSessionPtr> TopicReadSessions;
};

TFixture::TTableRecord::TTableRecord(const TString& key, const TString& value) :
    Key(key),
    Value(value)
{
}

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
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result.GetSession();
}

NTable::TTransaction TFixture::BeginTx(NTable::TSession& session)
{
    auto result = session.BeginTransaction().ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result.GetTransaction();
}

void TFixture::CommitTx(NTable::TTransaction& tx, EStatus status)
{
    auto result = tx.Commit().ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), status, result.GetIssues().ToString());
}

void TFixture::RollbackTx(NTable::TTransaction& tx, EStatus status)
{
    auto result = tx.Rollback().ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), status, result.GetIssues().ToString());
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
        auto result = tableSession.Close().ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
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
                                       const TString& messageGroupId,
                                       TMaybe<ui32> partitionId) -> TTopicWriteSessionPtr
{
    NTopic::TTopicClient client(GetDriver());
    NTopic::TWriteSessionSettings options;
    options.Path(topicPath);
    options.ProducerId(messageGroupId);
    options.MessageGroupId(messageGroupId);
    options.PartitionId(partitionId);
    options.Codec(ECodec::RAW);
    return client.CreateWriteSession(options);
}

auto TFixture::GetTopicWriteSession(const TString& topicPath,
                                    const TString& messageGroupId,
                                    TMaybe<ui32> partitionId) -> TTopicWriteSessionContext&
{
    std::pair<TString, TString> key(topicPath, messageGroupId);
    auto i = TopicWriteSessions.find(key);

    if (i == TopicWriteSessions.end()) {
        TTopicWriteSessionContext context;
        context.Session = CreateTopicWriteSession(topicPath, messageGroupId, partitionId);

        TopicWriteSessions.emplace(key, std::move(context));

        i = TopicWriteSessions.find(key);
    }

    return i->second;
}

NTopic::TTopicReadSettings MakeTopicReadSettings(const TString& topicPath,
                                                 TMaybe<ui32> partitionId)
{
    TTopicReadSettings options;
    options.Path(topicPath);
    if (partitionId.Defined()) {
        options.AppendPartitionIds(*partitionId);
    }
    return options;
}

NTopic::TReadSessionSettings MakeTopicReadSessionSettings(const TString& topicPath,
                                                          const TString& consumerName,
                                                          TMaybe<ui32> partitionId)
{
    NTopic::TReadSessionSettings options;
    options.AppendTopics(MakeTopicReadSettings(topicPath, partitionId));
    options.ConsumerName(consumerName);
    return options;
}

auto TFixture::CreateTopicReadSession(const TString& topicPath,
                                      const TString& consumerName,
                                      TMaybe<ui32> partitionId) -> TTopicReadSessionPtr
{
    NTopic::TTopicClient client(GetDriver());
    return client.CreateReadSession(MakeTopicReadSessionSettings(topicPath,
                                                                 consumerName,
                                                                 partitionId));
}

auto TFixture::GetTopicReadSession(const TString& topicPath,
                                   const TString& consumerName,
                                   TMaybe<ui32> partitionId) -> TTopicReadSessionPtr
{
    TTopicReadSessionPtr session;

    if (auto i = TopicReadSessions.find(topicPath); i == TopicReadSessions.end()) {
        session = CreateTopicReadSession(topicPath, consumerName, partitionId);
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
    while (!ContinuationToken.Defined()) {
        WaitForEvent();
    }
}

void TFixture::TTopicWriteSessionContext::WaitForEvent()
{
    Session->WaitEvent().Wait();
    for (auto& event : Session->GetEvents()) {
        if (auto* e = std::get_if<NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
            ContinuationToken = std::move(e->ContinuationToken);
        } else if (auto* e = std::get_if<NTopic::TWriteSessionEvent::TAcksEvent>(&event)) {
            for (auto& ack : e->Acks) {
                switch (ack.State) {
                case NTopic::TWriteSessionEvent::TWriteAck::EES_WRITTEN:
                    ++WrittenAckCount;
                    break;
                case NTopic::TWriteSessionEvent::TWriteAck::EES_WRITTEN_IN_TX:
                    ++WrittenInTxAckCount;
                    break;
                default:
                    break;
                }
            }
        } else if (auto* e = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
            UNIT_FAIL("");
        }
    }
}

void TFixture::TTopicWriteSessionContext::Write(const TString& message, NTable::TTransaction* tx)
{
    NTopic::TWriteMessage params(message);

    if (tx) {
        params.Tx(*tx);
    }

    Session->Write(std::move(*ContinuationToken),
                   std::move(params));

    ++WriteCount;
    ContinuationToken = Nothing();
}

void TFixture::CloseTopicWriteSession(const TString& topicPath,
                                      const TString& messageGroupId)
{
    std::pair<TString, TString> key(topicPath, messageGroupId);
    auto i = TopicWriteSessions.find(key);

    UNIT_ASSERT(i != TopicWriteSessions.end());

    TTopicWriteSessionContext& context = i->second;

    context.Session->Close();
    TopicWriteSessions.erase(key);
}

void TFixture::WriteToTopic(const TString& topicPath,
                            const TString& messageGroupId,
                            const TString& message,
                            NTable::TTransaction* tx,
                            TMaybe<ui32> partitionId)
{
    TTopicWriteSessionContext& context = GetTopicWriteSession(topicPath, messageGroupId, partitionId);
    context.WaitForContinuationToken();
    UNIT_ASSERT(context.ContinuationToken.Defined());
    context.Write(message, tx);
}

TVector<TString> TFixture::ReadFromTopic(const TString& topicPath,
                                         const TString& consumerName,
                                         const TDuration& duration,
                                         NTable::TTransaction* tx,
                                         TMaybe<ui32> partitionId)
{
    TVector<TString> messages;

    TInstant end = TInstant::Now() + duration;
    TDuration remain = duration;

    auto session = GetTopicReadSession(topicPath, consumerName, partitionId);

    while (TInstant::Now() < end) {
        if (!session->WaitEvent().Wait(remain)) {
            return messages;
        }

        NTopic::TReadSessionGetEventSettings settings;
        if (tx) {
            settings.Tx(*tx);
        }

        for (auto& event : session->GetEvents(settings)) {
            if (auto* e = std::get_if<NTopic::TReadSessionEvent::TDataReceivedEvent>(&event)) {
                Cerr << e->HasCompressedMessages() << " " << e->GetMessagesCount() << Endl;
                for (auto& m : e->GetMessages()) {
                    messages.push_back(m.GetData());
                }

                if (!tx) {
                    e->Commit();
                }
            }
        }

        remain = end - TInstant::Now();
    }

    return messages;
}

void TFixture::WaitForAcks(const TString& topicPath, const TString& messageGroupId, size_t writtenInTxCount)
{
    std::pair<TString, TString> key(topicPath, messageGroupId);
    auto i = TopicWriteSessions.find(key);
    UNIT_ASSERT(i != TopicWriteSessions.end());

    auto& context = i->second;

    UNIT_ASSERT(context.AckCount() <= context.WriteCount);

    while (context.AckCount() < context.WriteCount) {
        context.WaitForEvent();
    }

    UNIT_ASSERT((context.WrittenAckCount + context.WrittenInTxAckCount) == context.WriteCount);

    if (writtenInTxCount != Max<size_t>()) {
        UNIT_ASSERT_VALUES_EQUAL(context.WrittenInTxAckCount, writtenInTxCount);
    }
}

void TFixture::WaitForSessionClose(const TString& topicPath,
                                   const TString& messageGroupId,
                                   NYdb::EStatus status)
{
    std::pair<TString, TString> key(topicPath, messageGroupId);
    auto i = TopicWriteSessions.find(key);
    UNIT_ASSERT(i != TopicWriteSessions.end());

    auto& context = i->second;

    UNIT_ASSERT(context.AckCount() <= context.WriteCount);

    for(bool stop = false; !stop; ) {
        context.Session->WaitEvent().Wait();
        for (auto& event : context.Session->GetEvents()) {
            if (auto* e = std::get_if<NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                context.ContinuationToken = std::move(e->ContinuationToken);
            } else if (auto* e = std::get_if<NTopic::TWriteSessionEvent::TAcksEvent>(&event)) {
                for (auto& ack : e->Acks) {
                    switch (ack.State) {
                    case NTopic::TWriteSessionEvent::TWriteAck::EES_WRITTEN:
                        ++context.WrittenAckCount;
                        break;
                    case NTopic::TWriteSessionEvent::TWriteAck::EES_WRITTEN_IN_TX:
                        ++context.WrittenInTxAckCount;
                        break;
                    default:
                        break;
                    }
                }
            } else if (auto* e = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
                UNIT_ASSERT_VALUES_EQUAL(e->GetStatus(), status);
                UNIT_ASSERT_GT(e->GetIssues().Size(), 0);
                stop = true;
            }
        }
    }

    UNIT_ASSERT(context.AckCount() <= context.WriteCount);
}

ui64 TFixture::GetTopicTabletId(const TActorId& actorId, const TString& topicPath, ui32 partition)
{
    auto navigate = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
    navigate->DatabaseName = "/Root";

    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = SplitPath(topicPath);
    entry.SyncVersion = true;
    entry.ShowPrivatePath = true;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpList;

    navigate->ResultSet.push_back(std::move(entry));
    //navigate->UserToken = "root@builtin";
    navigate->Cookie = 12345;

    auto& runtime = Setup->GetRuntime();

    runtime.Send(MakeSchemeCacheID(), actorId,
                 new TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()),
                 0,
                 true);
    auto response = runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySetResult>();

    UNIT_ASSERT_VALUES_EQUAL(response->Request->Cookie, 12345);
    UNIT_ASSERT_VALUES_EQUAL(response->Request->ErrorCount, 0);

    auto& front = response->Request->ResultSet.front();
    UNIT_ASSERT(front.PQGroupInfo);
    UNIT_ASSERT_GT(front.PQGroupInfo->Description.PartitionsSize(), 0);
    UNIT_ASSERT_LT(partition, front.PQGroupInfo->Description.PartitionsSize());

    for (size_t i = 0; i < front.PQGroupInfo->Description.PartitionsSize(); ++i) {
        auto& p = front.PQGroupInfo->Description.GetPartitions(partition);
        if (p.GetPartitionId() == partition) {
            return p.GetTabletId();
        }
    }

    UNIT_FAIL("unknown partition");

    return Max<ui64>();
}

TVector<TString> TFixture::GetTabletKeys(const TActorId& actorId, ui64 tabletId)
{
    using TEvKeyValue = NKikimr::TEvKeyValue;

    auto request = std::make_unique<TEvKeyValue::TEvRequest>();
    request->Record.SetCookie(12345);

    auto cmd = request->Record.AddCmdReadRange();
    TString from(1, '\x00');
    TString to(1, '\xFF');
    auto range = cmd->MutableRange();
    range->SetFrom(from);
    range->SetIncludeFrom(true);
    range->SetTo(to);
    range->SetIncludeTo(true);

    auto& runtime = Setup->GetRuntime();

    runtime.SendToPipe(tabletId, actorId, request.release());
    auto response = runtime.GrabEdgeEvent<TEvKeyValue::TEvResponse>();

    UNIT_ASSERT(response->Record.HasCookie());
    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetCookie(), 12345);
    UNIT_ASSERT_VALUES_EQUAL(response->Record.ReadRangeResultSize(), 1);

    TVector<TString> keys;

    auto& result = response->Record.GetReadRangeResult(0);
    for (size_t i = 0; i < result.PairSize(); ++i) {
        auto& kv = result.GetPair(i);
        keys.emplace_back(kv.GetKey());
    }

    return keys;
}

void TFixture::RestartLongTxService()
{
    auto& runtime = Setup->GetRuntime();
    TActorId edge = runtime.AllocateEdgeActor();

    for (ui32 node = 0; node < runtime.GetNodeCount(); ++node) {
        runtime.Send(NKikimr::NLongTxService::MakeLongTxServiceID(runtime.GetNodeId(node)), edge,
                     new TEvents::TEvPoison(),
                     0,
                     true);
    }
}

Y_UNIT_TEST_F(WriteToTopic_Demo_1, TFixture)
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", &tx);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", &tx);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #3", &tx);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #4", &tx);

    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #5", &tx);
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #6", &tx);
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #7", &tx);
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #8", &tx);
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #9", &tx);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);
    WaitForAcks("topic_B", TEST_MESSAGE_GROUP_ID);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);
    }

    {
        auto messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);
    }

    CommitTx(tx, EStatus::SUCCESS);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages[3], "message #4");
    }

    {
        auto messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #5");
        UNIT_ASSERT_VALUES_EQUAL(messages[4], "message #9");
    }
}

Y_UNIT_TEST_F(WriteToTopic_Demo_2, TFixture)
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #1", &tx);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #2", &tx);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #3", &tx);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #4", &tx);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, "message #5");
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID_2, "message #6");

    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID_1, "message #7", &tx);
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID_1, "message #8", &tx);
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID_1, "message #9", &tx);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID_1);
    WaitForAcks("topic_B", TEST_MESSAGE_GROUP_ID_1);
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID_2);
    WaitForAcks("topic_B", TEST_MESSAGE_GROUP_ID_2);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #5");
    }

    {
        auto messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #6");
    }

    CommitTx(tx, EStatus::SUCCESS);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages[3], "message #4");
    }

    {
        auto messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #7");
        UNIT_ASSERT_VALUES_EQUAL(messages[2], "message #9");
    }
}

Y_UNIT_TEST_F(WriteToTopic_Demo_3, TFixture)
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", &tx);
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", &tx);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #3");

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);
    WaitForAcks("topic_B", TEST_MESSAGE_GROUP_ID);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #3");
    }

    CommitTx(tx, EStatus::ABORTED);

    tx = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", &tx);
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", &tx);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);
    WaitForAcks("topic_B", TEST_MESSAGE_GROUP_ID);

    CommitTx(tx, EStatus::SUCCESS);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_4, TFixture)
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx_1 = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", &tx_1);
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", &tx_1);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);
    WaitForAcks("topic_B", TEST_MESSAGE_GROUP_ID);

    NTable::TTransaction tx_2 = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #3", &tx_2);
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #4", &tx_2);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);
    WaitForAcks("topic_B", TEST_MESSAGE_GROUP_ID);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);
    }

    {
        auto messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);
    }

    CommitTx(tx_2, EStatus::SUCCESS);
    CommitTx(tx_1, EStatus::ABORTED);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #3");
    }

    {
        auto messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #4");
    }
}

Y_UNIT_TEST_F(WriteToTopic_Demo_5, TFixture)
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    NTable::TSession tableSession = CreateTableSession();

    {
        NTable::TTransaction tx_1 = BeginTx(tableSession);

        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", &tx_1);
        WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", &tx_1);

        WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);
        WaitForAcks("topic_B", TEST_MESSAGE_GROUP_ID);

        CommitTx(tx_1, EStatus::SUCCESS);
    }

    {
        NTable::TTransaction tx_2 = BeginTx(tableSession);

        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #3", &tx_2);
        WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #4", &tx_2);

        WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);
        WaitForAcks("topic_B", TEST_MESSAGE_GROUP_ID);

        CommitTx(tx_2, EStatus::SUCCESS);
    }

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #3");
    }

    {
        auto messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #2");
        UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #4");
    }
}

Y_UNIT_TEST_F(WriteToTopic_Demo_6, TFixture)
{
    CreateTopic("topic_A");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", &tx);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", &tx);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);
    }

    CommitTx(tx, EStatus::SUCCESS);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #2");
    }
}

Y_UNIT_TEST_F(WriteToTopic_Demo_7, TFixture)
{
    CreateTopic("topic_A");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #1", &tx);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #2", &tx);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, "message #3");
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, "message #4");

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #5", &tx);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #6", &tx);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID_1);
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID_2);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #3");
        UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #4");
    }

    CommitTx(tx, EStatus::SUCCESS);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages[3], "message #6");
    }
}

Y_UNIT_TEST_F(WriteToTopic_Demo_8, TFixture)
{
    CreateTopic("topic_A");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", &tx);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2");

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #2");
    }

    CommitTx(tx, EStatus::ABORTED);

    tx = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", &tx);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    CommitTx(tx, EStatus::SUCCESS);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
    }
}

Y_UNIT_TEST_F(WriteToTopic_Demo_9, TFixture)
{
    CreateTopic("topic_A");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx_1 = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", &tx_1);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    NTable::TTransaction tx_2 = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", &tx_2);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);
    }

    CommitTx(tx_2, EStatus::SUCCESS);
    CommitTx(tx_1, EStatus::ABORTED);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #2");
    }
}

Y_UNIT_TEST_F(WriteToTopic_Demo_10, TFixture)
{
    CreateTopic("topic_A");

    NTable::TSession tableSession = CreateTableSession();

    {
        NTable::TTransaction tx_1 = BeginTx(tableSession);

        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", &tx_1);

        WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

        CommitTx(tx_1, EStatus::SUCCESS);
    }

    {
        NTable::TTransaction tx_2 = BeginTx(tableSession);

        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", &tx_2);

        WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

        CommitTx(tx_2, EStatus::SUCCESS);
    }

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #2");
    }
}

NPQ::TWriteId TFixture::GetTransactionWriteId(const TActorId& actorId,
                                              ui64 tabletId)
{
    using TEvKeyValue = NKikimr::TEvKeyValue;

    auto request = std::make_unique<TEvKeyValue::TEvRequest>();
    request->Record.SetCookie(12345);
    request->Record.AddCmdRead()->SetKey("_txinfo");

    auto& runtime = Setup->GetRuntime();

    runtime.SendToPipe(tabletId, actorId, request.release());
    auto response = runtime.GrabEdgeEvent<TEvKeyValue::TEvResponse>();

    UNIT_ASSERT(response->Record.HasCookie());
    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetCookie(), 12345);
    UNIT_ASSERT_VALUES_EQUAL(response->Record.ReadResultSize(), 1);

    auto& read = response->Record.GetReadResult(0);

    NKikimrPQ::TTabletTxInfo info;
    UNIT_ASSERT(info.ParseFromString(read.GetValue()));

    UNIT_ASSERT_VALUES_EQUAL(info.TxWritesSize(), 1);

    auto& writeInfo = info.GetTxWrites(0);
    UNIT_ASSERT(writeInfo.HasWriteId());

    return NPQ::GetWriteId(writeInfo);
}

void TFixture::SendLongTxLockStatus(const TActorId& actorId,
                                    ui64 tabletId,
                                    const NPQ::TWriteId& writeId,
                                    NKikimrLongTxService::TEvLockStatus::EStatus status)
{
    auto event =
        std::make_unique<NKikimr::NLongTxService::TEvLongTxService::TEvLockStatus>(writeId.KeyId, writeId.NodeId,
                                                                                   status);
    auto& runtime = Setup->GetRuntime();
    runtime.SendToPipe(tabletId, actorId, event.release());
}

void TFixture::WaitForTheTabletToDeleteTheWriteInfo(const TActorId& actorId,
                                                    ui64 tabletId,
                                                    const NPQ::TWriteId& writeId)
{
    while (true) {
        using TEvKeyValue = NKikimr::TEvKeyValue;

        auto request = std::make_unique<TEvKeyValue::TEvRequest>();
        request->Record.SetCookie(12345);
        request->Record.AddCmdRead()->SetKey("_txinfo");

        auto& runtime = Setup->GetRuntime();

        runtime.SendToPipe(tabletId, actorId, request.release());
        auto response = runtime.GrabEdgeEvent<TEvKeyValue::TEvResponse>();

        UNIT_ASSERT(response->Record.HasCookie());
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetCookie(), 12345);
        UNIT_ASSERT_VALUES_EQUAL(response->Record.ReadResultSize(), 1);

        auto& read = response->Record.GetReadResult(0);

        NKikimrPQ::TTabletTxInfo info;
        UNIT_ASSERT(info.ParseFromString(read.GetValue()));

        bool found = false;

        for (size_t i = 0; i < info.TxWritesSize(); ++i) {
            auto& writeInfo = info.GetTxWrites(i);
            UNIT_ASSERT(writeInfo.HasWriteId());
            if (NPQ::GetWriteId(writeInfo) == writeId) {
                found = true;
                break;
            }
        }

        if (!found) {
            break;
        }

        Sleep(TDuration::MilliSeconds(100));
    }
}

void TFixture::RestartPQTablet(const TString& topicName, ui32 partition)
{
    auto& runtime = Setup->GetRuntime();
    TActorId edge = runtime.AllocateEdgeActor();
    ui64 tabletId = GetTopicTabletId(edge, "/Root/" + topicName, partition);
    runtime.SendToPipe(tabletId, edge, new TEvents::TEvPoison());

    Sleep(TDuration::Seconds(2));
}

void TFixture::DeleteSupportivePartition(const TString& topicName, ui32 partition)
{
    auto& runtime = Setup->GetRuntime();
    TActorId edge = runtime.AllocateEdgeActor();
    ui64 tabletId = GetTopicTabletId(edge, "/Root/" + topicName, partition);
    NPQ::TWriteId writeId = GetTransactionWriteId(edge, tabletId);

    SendLongTxLockStatus(edge, tabletId, writeId, NKikimrLongTxService::TEvLockStatus::STATUS_NOT_FOUND);

    WaitForTheTabletToDeleteTheWriteInfo(edge, tabletId, writeId);
}

void TFixture::CheckTabletKeys(const TString& topicName)
{
    auto& runtime = Setup->GetRuntime();
    TActorId edge = runtime.AllocateEdgeActor();
    ui64 tabletId = GetTopicTabletId(edge, "/Root/" + topicName, 0);

    const THashSet<char> types {
        NPQ::TKeyPrefix::TypeInfo,
        NPQ::TKeyPrefix::TypeData,
        NPQ::TKeyPrefix::TypeTmpData,
        NPQ::TKeyPrefix::TypeMeta,
        NPQ::TKeyPrefix::TypeTxMeta,
    };

    bool found;
    TVector<TString> keys;
    for (size_t i = 0; i < 20; ++i) {
        keys = GetTabletKeys(edge, tabletId);

        found = false;
        for (const auto& key : keys) {
            UNIT_ASSERT_GT(key.size(), 0);
            if (key[0] == '_') {
                continue;
            }

            if (types.contains(key[0])) {
                found = false;
                break;
            }
        }

        if (!found) {
            break;
        }

        Sleep(TDuration::MilliSeconds(100));
    }

    if (found) {
        Cerr << "keys for tablet " << tabletId << ":" << Endl;
        for (const auto& k : keys) {
            Cerr << k << Endl;
        }
        Cerr << "=============" << Endl;

        UNIT_FAIL("unexpected keys for tablet " << tabletId);
    }
}

void TFixture::DumpPQTabletKeys(const TString& topicName)
{
    auto& runtime = Setup->GetRuntime();
    TActorId edge = runtime.AllocateEdgeActor();
    ui64 tabletId = GetTopicTabletId(edge, "/Root/" + topicName, 0);
    auto keys = GetTabletKeys(edge, tabletId);
    for (const auto& key : keys) {
        Cerr << key << Endl;
    }
}

void TFixture::TestTheCompletionOfATransaction(const TTransactionCompletionTestDescription& d)
{
    for (auto& topic : d.Topics) {
        CreateTopic(topic);
    }

    {
        NTable::TSession tableSession = CreateTableSession();
        NTable::TTransaction tx = BeginTx(tableSession);

        for (auto& topic : d.Topics) {
            WriteToTopic(topic, TEST_MESSAGE_GROUP_ID, "message", &tx);
            WaitForAcks(topic, TEST_MESSAGE_GROUP_ID);
        }

        switch (d.EndOfTransaction) {
        case Commit:
            CommitTx(tx, EStatus::SUCCESS);
            break;
        case Rollback:
            RollbackTx(tx, EStatus::SUCCESS);
            break;
        case CloseTableSession:
            break;
        }
    }

    Sleep(TDuration::Seconds(5));

    for (auto& topic : d.Topics) {
        CheckTabletKeys(topic);
    }
}

Y_UNIT_TEST_F(WriteToTopic_Demo_11, TFixture)
{
    for (auto endOfTransaction : {Commit, Rollback, CloseTableSession}) {
        TestTheCompletionOfATransaction({.Topics={"topic_A"}, .EndOfTransaction = endOfTransaction});
        TestTheCompletionOfATransaction({.Topics={"topic_A", "topic_B"}, .EndOfTransaction = endOfTransaction});
    }
}

Y_UNIT_TEST_F(WriteToTopic_Demo_12, TFixture)
{
    CreateTopic("topic_A");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", &tx);
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    DeleteSupportivePartition("topic_A", 0);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", &tx);
    WaitForSessionClose("topic_A", TEST_MESSAGE_GROUP_ID, NYdb::EStatus::PRECONDITION_FAILED);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_13, TFixture)
{
    CreateTopic("topic_A");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message", &tx);
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    DeleteSupportivePartition("topic_A", 0);

    CommitTx(tx, EStatus::ABORTED);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_14, TFixture)
{
    CreateTopic("topic_A");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", &tx);
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    DeleteSupportivePartition("topic_A", 0);

    CloseTopicWriteSession("topic_A", TEST_MESSAGE_GROUP_ID);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", &tx);
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    CommitTx(tx, EStatus::ABORTED);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_15, TFixture)
{
    CreateTopic("topic_A");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #1", &tx);
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID_1);
    CloseTopicWriteSession("topic_A", TEST_MESSAGE_GROUP_ID_1);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, "message #2", &tx);
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID_2);
    CloseTopicWriteSession("topic_A", TEST_MESSAGE_GROUP_ID_2);

    CommitTx(tx, EStatus::SUCCESS);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_16, TFixture)
{
    CreateTopic("topic_A");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", &tx);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", &tx);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    RestartPQTablet("topic_A", 0);

    CommitTx(tx, EStatus::SUCCESS);

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
    UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #2");
}

Y_UNIT_TEST_F(WriteToTopic_Demo_17, TFixture)
{
    CreateTopic("topic_A");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(22'000'000, 'x'));
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(100, 'x'));
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(200, 'x'));
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(300, 'x'));
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(10'000'000, 'x'));

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString( 6'000'000, 'x'), &tx);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(20'000'000, 'x'), &tx);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString( 7'000'000, 'x'), &tx);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    CommitTx(tx, EStatus::SUCCESS);

    //RestartPQTablet("topic_A", 0);

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 8);
    UNIT_ASSERT_VALUES_EQUAL(messages[0].size(), 22'000'000);
    UNIT_ASSERT_VALUES_EQUAL(messages[1].size(),        100);
    UNIT_ASSERT_VALUES_EQUAL(messages[2].size(),        200);
    UNIT_ASSERT_VALUES_EQUAL(messages[3].size(),        300);
    UNIT_ASSERT_VALUES_EQUAL(messages[4].size(), 10'000'000);
    UNIT_ASSERT_VALUES_EQUAL(messages[5].size(),  6'000'000);
    UNIT_ASSERT_VALUES_EQUAL(messages[6].size(), 20'000'000);
    UNIT_ASSERT_VALUES_EQUAL(messages[7].size(),  7'000'000);
}

void TFixture::TestTxWithBigBlobs(const TTestTxWithBigBlobsParams& params)
{
    size_t oldHeadMsgCount = 0;
    size_t bigBlobMsgCount = 0;
    size_t newHeadMsgCount = 0;

    CreateTopic("topic_A");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    for (size_t i = 0; i < params.OldHeadCount; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(100'000, 'x'));
        ++oldHeadMsgCount;
    }

    for (size_t i = 0; i < params.BigBlobsCount; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(7'900'000, 'x'), &tx);
        ++bigBlobMsgCount;
    }

    for (size_t i = 0; i < params.NewHeadCount; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(100'000, 'x'), &tx);
        ++newHeadMsgCount;
    }

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    if (params.RestartMode == ERestartBeforeCommit) {
        RestartPQTablet("topic_A", 0);
    }

    CommitTx(tx, EStatus::SUCCESS);

    if (params.RestartMode == ERestartAfterCommit) {
        RestartPQTablet("topic_A", 0);
    }

    TVector<TString> messages;
    for (size_t i = 0; (i < 10) && (messages.size() < (oldHeadMsgCount + bigBlobMsgCount + newHeadMsgCount)); ++i) {
        auto block = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        for (auto& m : block) {
            messages.push_back(std::move(m));
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(messages.size(), oldHeadMsgCount + bigBlobMsgCount + newHeadMsgCount);

    size_t start = 0;

    for (size_t i = 0; i < oldHeadMsgCount; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(messages[start + i].size(), 100'000);
    }
    start += oldHeadMsgCount;

    for (size_t i = 0; i < bigBlobMsgCount; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(messages[start + i].size(), 7'900'000);
    }
    start += bigBlobMsgCount;

    for (size_t i = 0; i < newHeadMsgCount; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(messages[start + i].size(), 100'000);
    }
}

#define Y_UNIT_TEST_WITH_REBOOTS(name, oldHeadCount, bigBlobsCount, newHeadCount) \
Y_UNIT_TEST_F(name##_RestartNo, TFixture) { \
    TestTxWithBigBlobs({.OldHeadCount = oldHeadCount, .BigBlobsCount = bigBlobsCount, .NewHeadCount = newHeadCount, .RestartMode = ERestartNo}); \
} \
Y_UNIT_TEST_F(name##_RestartBeforeCommit, TFixture) { \
    TestTxWithBigBlobs({.OldHeadCount = oldHeadCount, .BigBlobsCount = bigBlobsCount, .NewHeadCount = newHeadCount, .RestartMode = ERestartBeforeCommit}); \
} \
Y_UNIT_TEST_F(name##_RestartAfterCommit, TFixture) { \
    TestTxWithBigBlobs({.OldHeadCount = oldHeadCount, .BigBlobsCount = bigBlobsCount, .NewHeadCount = newHeadCount, .RestartMode = ERestartAfterCommit}); \
}

Y_UNIT_TEST_WITH_REBOOTS(WriteToTopic_Demo_18, 10, 2, 10);
Y_UNIT_TEST_WITH_REBOOTS(WriteToTopic_Demo_19, 10, 0, 10);
Y_UNIT_TEST_WITH_REBOOTS(WriteToTopic_Demo_20, 10, 2,  0);

Y_UNIT_TEST_WITH_REBOOTS(WriteToTopic_Demo_21,  0, 2, 10);
Y_UNIT_TEST_WITH_REBOOTS(WriteToTopic_Demo_22,  0, 0, 10);
Y_UNIT_TEST_WITH_REBOOTS(WriteToTopic_Demo_23,  0, 2,  0);

void TFixture::CreateTable(const TString& tablePath)
{
    UNIT_ASSERT(!tablePath.empty());

    TString path = (tablePath[0] != '/') ? ("/Root/" + tablePath) : tablePath;

    NTable::TSession session = CreateTableSession();
    auto desc = NTable::TTableBuilder()
        .AddNonNullableColumn("key", EPrimitiveType::Utf8)
        .AddNonNullableColumn("value", EPrimitiveType::Utf8)
        .SetPrimaryKeyColumn("key")
        .Build();
    auto result = session.CreateTable(path, std::move(desc)).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

auto TFixture::MakeTableRecords() -> TVector<TTableRecord>
{
    TVector<TTableRecord> records;
    records.emplace_back("key-1", "value-1");
    records.emplace_back("key-2", "value-2");
    records.emplace_back("key-3", "value-3");
    records.emplace_back("key-4", "value-4");
    return records;
}

auto TFixture::MakeJsonDoc(const TVector<TTableRecord>& records) -> TString
{
    auto makeJsonObject = [](const TTableRecord& r) {
        return Sprintf(R"({"key":"%s", "value":"%s"})",
                       r.Key.data(),
                       r.Value.data());
    };

    if (records.empty()) {
        return "[]";
    }

    TString s = "[";

    s += makeJsonObject(records.front());
    for (auto i = records.begin() + 1; i != records.end(); ++i) {
        s += ", ";
        s += makeJsonObject(*i);
    }
    s += "]";

    return s;
}

void TFixture::WriteToTable(const TString& tablePath,
                            const TVector<TTableRecord>& records,
                            NTable::TTransaction* tx)
{
    TString query = Sprintf("DECLARE $key AS Utf8;"
                            "DECLARE $value AS Utf8;"
                            "UPSERT INTO `%s` (key, value) VALUES ($key, $value);",
                            tablePath.data());
    NTable::TSession session = tx->GetSession();

    for (const auto& r : records) {
        auto params = session.GetParamsBuilder()
            .AddParam("$key").Utf8(r.Key).Build()
            .AddParam("$value").Utf8(r.Value).Build()
            .Build();
        auto result = session.ExecuteDataQuery(query,
                                               NYdb::NTable::TTxControl::Tx(*tx),
                                               params).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
}

size_t TFixture::GetTableRecordsCount(const TString& tablePath)
{
    TString query = Sprintf(R"(SELECT COUNT(*) FROM `%s`)",
                            tablePath.data());
    NTable::TSession session = CreateTableSession();
    NTable::TTransaction tx = BeginTx(session);

    auto result = session.ExecuteDataQuery(query,
                                           NYdb::NTable::TTxControl::Tx(tx).CommitTx(true)).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    NYdb::TResultSetParser parser(result.GetResultSet(0));
    UNIT_ASSERT(parser.TryNextRow());

    return parser.ColumnParser(0).GetUint64();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_24, TFixture)
{
    //
    // the test verifies a transaction in which data is written to a topic and to a table
    //
    CreateTopic("topic_A");
    CreateTable("/Root/table_A");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    auto records = MakeTableRecords();
    WriteToTable("table_A", records, &tx);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), &tx);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    CommitTx(tx, EStatus::SUCCESS);

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], MakeJsonDoc(records));

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), records.size());

    CheckTabletKeys("topic_A");
}

Y_UNIT_TEST_F(WriteToTopic_Demo_25, TFixture)
{
    //
    // the test verifies a transaction in which data is read from one topic and written to another
    //
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1");
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2");
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #3");

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), &tx);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 3);

    for (const auto& m : messages) {
        WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, m, &tx);
    }

    WaitForAcks("topic_B", TEST_MESSAGE_GROUP_ID);

    CommitTx(tx, EStatus::SUCCESS);

    messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 3);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_26, TFixture)
{
    //
    // the test verifies a transaction in which data is read from a partition of one topic and written to
    // another partition of this topic
    //
    const ui32 PARTITION_0 = 0;
    const ui32 PARTITION_1 = 1;

    CreateTopic("topic_A", TEST_CONSUMER, 2);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", nullptr, PARTITION_0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", nullptr, PARTITION_0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #3", nullptr, PARTITION_0);

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), &tx, PARTITION_0);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 3);

    for (const auto& m : messages) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, m, &tx, PARTITION_1);
    }

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    CommitTx(tx, EStatus::SUCCESS);

    messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), nullptr, PARTITION_1);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 3);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_27, TFixture)
{
    CreateTopic("topic_A", TEST_CONSUMER);
    CreateTopic("topic_B", TEST_CONSUMER);
    CreateTopic("topic_C", TEST_CONSUMER);

    for (size_t i = 0, writtenInTx = 0; i < 2; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", nullptr, 0);
        WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", nullptr, 0);

        NTable::TSession tableSession = CreateTableSession();
        NTable::TTransaction tx = BeginTx(tableSession);

        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), &tx, 0);
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        WriteToTopic("topic_C", TEST_MESSAGE_GROUP_ID, messages[0], &tx, 0);
        ++writtenInTx;
        WaitForAcks("topic_C", TEST_MESSAGE_GROUP_ID, writtenInTx);

        messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2), &tx, 0);
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        WriteToTopic("topic_C", TEST_MESSAGE_GROUP_ID, messages[0], &tx, 0);
        ++writtenInTx;
        WaitForAcks("topic_C", TEST_MESSAGE_GROUP_ID, writtenInTx);

        CommitTx(tx, EStatus::SUCCESS);

        messages = ReadFromTopic("topic_C", TEST_CONSUMER, TDuration::Seconds(2), nullptr, 0);
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);

        DumpPQTabletKeys("topic_A");
        DumpPQTabletKeys("topic_B");
        DumpPQTabletKeys("topic_C");
    }
}

Y_UNIT_TEST_F(WriteToTopic_Demo_28, TFixture)
{
    // The test verifies that the `WriteInflightSize` is correctly considered for the main partition.
    // Writing to the service partition does not change the `WriteInflightSize` of the main one.
    CreateTopic("topic_A", TEST_CONSUMER);

    NTable::TSession tableSession = CreateTableSession();
    NTable::TTransaction tx = BeginTx(tableSession);

    TString message(16'000, 'a');

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, TString(16'000, 'a'), &tx, 0);
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID_1);

    CommitTx(tx, EStatus::SUCCESS);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, TString(20'000, 'b'), nullptr, 0);
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID_2);

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), nullptr, 0);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);
}

}

}
