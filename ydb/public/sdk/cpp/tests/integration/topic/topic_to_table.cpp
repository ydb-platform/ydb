#include "setup/fixture.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <ydb/public/sdk/cpp/src/library/issue/yql_issue_message.h>

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_query.pb.h>

#include <grpcpp/create_channel.h>

#include <util/generic/hash.h>

#include <thread>

using namespace std::chrono_literals;

namespace NYdb::inline Dev::NTopic::NTests {

const auto TEST_MESSAGE_GROUP_ID_1 = TEST_MESSAGE_GROUP_ID + "_1";
const auto TEST_MESSAGE_GROUP_ID_2 = TEST_MESSAGE_GROUP_ID + "_2";
const auto TEST_MESSAGE_GROUP_ID_3 = TEST_MESSAGE_GROUP_ID + "_3";
const auto TEST_MESSAGE_GROUP_ID_4 = TEST_MESSAGE_GROUP_ID + "_4";

class TxUsage : public TTopicTestFixture {
protected:
    using TTopicReadSession = NTopic::IReadSession;
    using TTopicReadSessionPtr = std::shared_ptr<TTopicReadSession>;
    using TTopicWriteSession = NTopic::IWriteSession;
    using TTopicWriteSessionPtr = std::shared_ptr<TTopicWriteSession>;

    struct TTopicWriteSessionContext {
        TTopicWriteSessionPtr Session;
        std::optional<NTopic::TContinuationToken> ContinuationToken;
        std::size_t WriteCount = 0;
        std::size_t WrittenAckCount = 0;
        std::size_t WrittenInTxAckCount = 0;

        void WaitForContinuationToken();
        void Write(const std::string& message, TTransactionBase* tx = nullptr);

        std::size_t AckCount() const { return WrittenAckCount + WrittenInTxAckCount; }

        void WaitForEvent();
    };

    class ISession {
    public:
        using TExecuteInTxResult = std::pair<std::vector<TResultSet>, std::unique_ptr<TTransactionBase>>;

        virtual std::vector<TResultSet> Execute(const std::string& query,
                                                TTransactionBase* tx,
                                                bool commit = true,
                                                const TParams& params = TParamsBuilder().Build()) = 0;

        virtual TExecuteInTxResult ExecuteInTx(const std::string& query,
                                               bool commit = true,
                                               const TParams& params = TParamsBuilder().Build()) = 0;

        virtual std::unique_ptr<TTransactionBase> BeginTx() = 0;
        virtual void CommitTx(TTransactionBase& tx, EStatus status = EStatus::SUCCESS) = 0;
        virtual void RollbackTx(TTransactionBase& tx, EStatus status = EStatus::SUCCESS) = 0;

        virtual void Close() = 0;

        virtual ~ISession() = default;
    };

    void SetUp() override;

    std::unique_ptr<ISession> CreateSession();

    TTopicReadSessionPtr CreateReader();

    void StartPartitionSession(TTopicReadSessionPtr reader, TTransactionBase& tx, std::uint64_t offset);
    void StartPartitionSession(TTopicReadSessionPtr reader, std::uint64_t offset);

    struct TReadMessageSettings {
        TTransactionBase& Tx;
        bool CommitOffsets = false;
        std::optional<std::uint64_t> Offset;
    };

    void ReadMessage(TTopicReadSessionPtr reader, TTransactionBase& tx, std::uint64_t offset);
    void ReadMessage(TTopicReadSessionPtr reader, const TReadMessageSettings& settings);

    void WriteMessage(const std::string& message);
    void WriteMessages(const std::vector<std::string>& messages,
                       const std::string& topic, const std::string& groupId,
                       TTransactionBase& tx);

    void AddConsumer(const std::string& topicName, const std::vector<std::string>& consumers);
    void AlterAutoPartitioning(const std::string& topicName,
                               std::uint64_t minActivePartitions,
                               std::uint64_t maxActivePartitions,
                               EAutoPartitioningStrategy strategy,
                               TDuration stabilizationWindow,
                               std::uint64_t downUtilizationPercent,
                               std::uint64_t upUtilizationPercent);

    void WriteToTopicWithInvalidTxId(bool invalidTxId);

    TTopicWriteSessionPtr CreateTopicWriteSession(const std::string& topicName,
                                                  const std::string& messageGroupId,
                                                  std::optional<std::uint32_t> partitionId);
    TTopicWriteSessionContext& GetTopicWriteSession(const std::string& topicName,
                                                    const std::string& messageGroupId,
                                                    std::optional<std::uint32_t> partitionId);

    TTopicReadSessionPtr CreateTopicReadSession(const std::string& topicName,
                                                const std::string& consumerName,
                                                std::optional<std::uint32_t> partitionId);
    TTopicReadSessionPtr GetTopicReadSession(const std::string& topicName,
                                             const std::string& consumerName,
                                             std::optional<std::uint32_t> partitionId);

    void WriteToTopic(const std::string& topicName,
                      const std::string& messageGroupId,
                      const std::string& message,
                      TTransactionBase* tx = nullptr,
                      std::optional<std::uint32_t> partitionId = std::nullopt);
    std::vector<std::string> ReadFromTopic(const std::string& topicName,
                                   const std::string& consumerName,
                                   const TDuration& duration,
                                   TTransactionBase* tx = nullptr,
                                   std::optional<std::uint32_t> partitionId = std::nullopt);
    void WaitForAcks(const std::string& topicName,
                     const std::string& messageGroupId,
                     std::size_t writtenInTxCount = std::numeric_limits<std::size_t>::max());
    void CloseTopicWriteSession(const std::string& topicName,
                                const std::string& messageGroupId,
                                bool force = false);

    struct TTableRecord {
        TTableRecord() = default;
        TTableRecord(const std::string& key, const std::string& value);

        std::string Key;
        std::string Value;
    };

    void WriteMessagesInTx(std::size_t big, size_t small);

    const TDriver& GetDriver() const;
    NTable::TTableClient& GetTableClient();

    std::vector<std::string> Read_Exactly_N_Messages_From_Topic(const std::string& topicName,
                                                                const std::string& consumerName,
                                                                std::size_t count);

    void TestSessionAbort();

    void TestTwoSessionOneConsumer();

    void TestOffsetsCannotBePromotedWhenReadingInATransaction();

    void TestWriteToTopicTwoWriteSession();

    void TestWriteToTopic1();

    void TestWriteToTopic2();

    void TestWriteToTopic3();

    void TestWriteToTopic4();

    void TestWriteToTopic5();

    void TestWriteToTopic6();

    void TestWriteToTopic7();

    void TestWriteToTopic8();

    void TestWriteToTopic9();

    void TestWriteToTopic10();

    void TestWriteToTopic15();

    void TestWriteToTopic17();

    void TestWriteToTopic25();

    void TestWriteToTopic26();

    void TestWriteToTopic28();

    void TestWriteToTopic29();

    void TestWriteToTopic30();

    void TestWriteToTopic31();

    void TestWriteToTopic32();

    void TestWriteToTopic33();

    void TestWriteToTopic34();

    void TestWriteToTopic35();

    void TestWriteToTopic36();

    void TestWriteToTopic37();

    void TestWriteToTopic39();

    void TestWriteToTopic48();

    enum class EClientType {
        Table,
        Query,
        None
    };

    virtual EClientType GetClientType() const = 0;

private:
    class TTableSession : public ISession {
    public:
        TTableSession(NTable::TTableClient& client);

        std::vector<TResultSet> Execute(const std::string& query,
                                        TTransactionBase* tx,
                                        bool commit = true,
                                        const TParams& params = TParamsBuilder().Build()) override;

        TExecuteInTxResult ExecuteInTx(const std::string& query,
                                       bool commit = true,
                                       const TParams& params = TParamsBuilder().Build()) override;

        std::unique_ptr<TTransactionBase> BeginTx() override;
        void CommitTx(TTransactionBase& tx, EStatus status = EStatus::SUCCESS) override;
        void RollbackTx(TTransactionBase& tx, EStatus status = EStatus::SUCCESS) override;

        void Close() override;

    private:
        NTable::TSession Init(NTable::TTableClient& client);

        NTable::TSession Session_;
    };

    class TQuerySession : public ISession {
    public:
        TQuerySession(NQuery::TQueryClient& client,
                      const std::string& endpoint,
                      const std::string& database);

        std::vector<TResultSet> Execute(const std::string& query,
                                        TTransactionBase* tx,
                                        bool commit = true,
                                        const TParams& params = TParamsBuilder().Build()) override;

        TExecuteInTxResult ExecuteInTx(const std::string& query,
                                       bool commit = true,
                                       const TParams& params = TParamsBuilder().Build()) override;

        std::unique_ptr<TTransactionBase> BeginTx() override;
        void CommitTx(TTransactionBase& tx, EStatus status = EStatus::SUCCESS) override;
        void RollbackTx(TTransactionBase& tx, EStatus status = EStatus::SUCCESS) override;

        void Close() override;

    private:
        NQuery::TSession Init(NQuery::TQueryClient& client);

        NQuery::TSession Session_;
        std::string Endpoint_;
        std::string Database_;
    };

    template<class E>
    E ReadEvent(TTopicReadSessionPtr reader, TTransactionBase& tx);
    template<class E>
    E ReadEvent(TTopicReadSessionPtr reader);

    TTopicReadSettings MakeTopicReadSettings(const std::string& topicName, std::optional<std::uint32_t> partitionId);
    TReadSessionSettings MakeTopicReadSessionSettings(const std::string& topicName,
                                                      const std::string& consumerName,
                                                      std::optional<std::uint32_t> partitionId);

    std::unique_ptr<TDriver> Driver;
    std::unique_ptr<NTable::TTableClient> TableClient;
    std::unique_ptr<NQuery::TQueryClient> QueryClient;

    THashMap<std::pair<std::string, std::string>, TTopicWriteSessionContext> TopicWriteSessions;
    THashMap<std::string, TTopicReadSessionPtr> TopicReadSessions;
};

class TxUsageTable : public TxUsage {
protected:
    EClientType GetClientType() const override {
        return EClientType::Table;
    }
};

class TxUsageQuery : public TxUsage {
protected:
    EClientType GetClientType() const override {
        return EClientType::Query;
    }
};

TxUsage::TTableRecord::TTableRecord(const std::string& key, const std::string& value) :
    Key(key),
    Value(value)
{
}

void TxUsage::SetUp()
{
    char* ydbVersion = std::getenv("YDB_VERSION");

    if (ydbVersion != nullptr && std::string(ydbVersion) != "trunk" && std::string(ydbVersion) <= "24.3") {
        GTEST_SKIP() << "Skipping test for YDB version " << ydbVersion;
    }

    TTopicTestFixture::SetUp();

    Driver = std::make_unique<TDriver>(MakeDriver());
    auto tableSettings = NTable::TClientSettings().SessionPoolSettings(NTable::TSessionPoolSettings()
        .MaxActiveSessions(3000)
    );

    auto querySettings = NQuery::TClientSettings().SessionPoolSettings(NQuery::TSessionPoolSettings()
        .MaxActiveSessions(3000)
    );

    TableClient = std::make_unique<NTable::TTableClient>(*Driver, tableSettings);
    QueryClient = std::make_unique<NQuery::TQueryClient>(*Driver, querySettings);
}

TxUsage::TTableSession::TTableSession(NTable::TTableClient& client)
    : Session_(Init(client))
{
}

NTable::TSession TxUsage::TTableSession::Init(NTable::TTableClient& client)
{
    auto result = client.GetSession().ExtractValueSync();
    Y_ENSURE_BT(result.IsSuccess(), ToString(static_cast<TStatus>(result)));
    return result.GetSession();
}

std::vector<TResultSet> TxUsage::TTableSession::Execute(const std::string& query,
                                                        TTransactionBase* tx,
                                                        bool commit,
                                                        const TParams& params)
{
    auto txTable = dynamic_cast<NTable::TTransaction*>(tx);
    auto txControl = NTable::TTxControl::Tx(*txTable).CommitTx(commit);

    auto result = Session_.ExecuteDataQuery(query, txControl, params).GetValueSync();
    Y_ENSURE_BT(result.IsSuccess(), ToString(static_cast<TStatus>(result)));

    return std::move(result).ExtractResultSets();
}

TxUsage::ISession::TExecuteInTxResult TxUsage::TTableSession::ExecuteInTx(const std::string& query,
                                                                          bool commit,
                                                                          const TParams& params)
{
    auto txControl = NTable::TTxControl::BeginTx().CommitTx(commit);

    auto result = Session_.ExecuteDataQuery(query, txControl, params).GetValueSync();
    Y_ENSURE_BT(result.IsSuccess(), ToString(static_cast<TStatus>(result)));

    return {std::move(result).ExtractResultSets(), std::make_unique<NTable::TTransaction>(*result.GetTransaction())};
}

std::unique_ptr<TTransactionBase> TxUsage::TTableSession::BeginTx()
{
    while (true) {
        auto result = Session_.BeginTransaction().ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            Y_ENSURE_BT(result.IsSuccess(), ToString(static_cast<TStatus>(result)));
            return std::make_unique<NTable::TTransaction>(result.GetTransaction());
        }
        std::this_thread::sleep_for(100ms);
    }
}

void TxUsage::TTableSession::CommitTx(TTransactionBase& tx, EStatus status)
{
    auto txTable = dynamic_cast<NTable::TTransaction&>(tx);
    while (true) {
        auto result = txTable.Commit().ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            Y_ENSURE_BT(result.GetStatus() == status, ToString(static_cast<TStatus>(result)));
            return;
        }
        std::this_thread::sleep_for(100ms);
    }
}

void TxUsage::TTableSession::RollbackTx(TTransactionBase& tx, EStatus status)
{
    auto txTable = dynamic_cast<NTable::TTransaction&>(tx);
    while (true) {
        auto result = txTable.Rollback().ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            Y_ENSURE_BT(result.GetStatus() == status, ToString(static_cast<TStatus>(result)));
            return;
        }
        std::this_thread::sleep_for(100ms);
    }
}

void TxUsage::TTableSession::Close()
{
    Session_.Close();
}

TxUsage::TQuerySession::TQuerySession(NQuery::TQueryClient& client,
                                      const std::string& endpoint,
                                      const std::string& database)
    : Session_(Init(client))
    , Endpoint_(endpoint)
    , Database_(database)
{
}

NQuery::TSession TxUsage::TQuerySession::Init(NQuery::TQueryClient& client)
{
    auto result = client.GetSession().ExtractValueSync();
    Y_ENSURE_BT(result.IsSuccess(), ToString(static_cast<TStatus>(result)));
    return result.GetSession();
}

std::vector<TResultSet> TxUsage::TQuerySession::Execute(const std::string& query,
                                                        TTransactionBase* tx,
                                                        bool commit,
                                                        const TParams& params)
{
    auto txQuery = dynamic_cast<NQuery::TTransaction*>(tx);
    auto txControl = NQuery::TTxControl::Tx(*txQuery).CommitTx(commit);

    auto result = Session_.ExecuteQuery(query, txControl, params).ExtractValueSync();
    Y_ENSURE_BT(result.IsSuccess(), ToString(static_cast<TStatus>(result)));

    return result.GetResultSets();
}

TxUsage::ISession::TExecuteInTxResult TxUsage::TQuerySession::ExecuteInTx(const std::string& query,
                                                                          bool commit,
                                                                          const TParams& params)
{
    auto txControl = NQuery::TTxControl::BeginTx().CommitTx(commit);

    auto result = Session_.ExecuteQuery(query, txControl, params).ExtractValueSync();
    Y_ENSURE_BT(result.IsSuccess(), ToString(static_cast<TStatus>(result)));

    return {result.GetResultSets(), std::make_unique<NQuery::TTransaction>(*result.GetTransaction())};
}

std::unique_ptr<TTransactionBase> TxUsage::TQuerySession::BeginTx()
{
    while (true) {
        auto result = Session_.BeginTransaction(NQuery::TTxSettings()).ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            Y_ENSURE_BT(result.IsSuccess(), ToString(static_cast<TStatus>(result)));
            return std::make_unique<NQuery::TTransaction>(result.GetTransaction());
        }
        std::this_thread::sleep_for(100ms);
    }
}

void TxUsage::TQuerySession::CommitTx(TTransactionBase& tx, EStatus status)
{
    auto txQuery = dynamic_cast<NQuery::TTransaction&>(tx);
    while (true) {
        auto result = txQuery.Commit().ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            Y_ENSURE_BT(result.GetStatus() == status, ToString(static_cast<TStatus>(result)));
            return;
        }
        std::this_thread::sleep_for(100ms);
    }
}

void TxUsage::TQuerySession::RollbackTx(TTransactionBase& tx, EStatus status)
{
    auto txQuery = dynamic_cast<NQuery::TTransaction&>(tx);
    while (true) {
        auto result = txQuery.Rollback().ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            Y_ENSURE_BT(result.GetStatus() == status, ToString(static_cast<TStatus>(result)));
            return;
        }
        std::this_thread::sleep_for(100ms);
    }
}

void TxUsage::TQuerySession::Close()
{
    // SDK doesn't provide a method to close the session for Query Client, so we use grpc API directly
    auto credentials = grpc::InsecureChannelCredentials();
    auto channel = grpc::CreateChannel(TStringType(Endpoint_), credentials);
    auto stub = Ydb::Query::V1::QueryService::NewStub(channel);

    grpc::ClientContext context;
    context.AddMetadata("x-ydb-database", TStringType(Database_));

    Ydb::Query::DeleteSessionRequest request;
    request.set_session_id(Session_.GetId());

    Ydb::Query::DeleteSessionResponse response;
    auto status = stub->DeleteSession(&context, request, &response);

    NIssue::TIssues issues;
    NYdb::NIssue::IssuesFromMessage(response.issues(), issues);
    Y_ENSURE_BT(status.ok(), status.error_message());
    Y_ENSURE_BT(response.status() == Ydb::StatusIds::SUCCESS, issues.ToString());
}

std::unique_ptr<TxUsage::ISession> TxUsage::CreateSession()
{
    switch (GetClientType()) {
        case EClientType::Table: {
            Y_ENSURE_BT(TableClient, "TableClient is not initialized");
            return std::make_unique<TxUsage::TTableSession>(*TableClient);
        }
        case EClientType::Query: {
            Y_ENSURE_BT(QueryClient, "QueryClient is not initialized");
            return std::make_unique<TxUsage::TQuerySession>(*QueryClient,
                                                             GetEndpoint(),
                                                             GetDatabase());
        }
        case EClientType::None: {
            Y_ENSURE_BT(false, "CreateSession is forbidden for None client type");
        }
    }

    return nullptr;
}

auto TxUsage::CreateReader() -> TTopicReadSessionPtr
{
    NTopic::TTopicClient client(GetDriver());
    TReadSessionSettings options;
    options.ConsumerName(GetConsumerName());
    options.AppendTopics(GetTopicPath());
    return client.CreateReadSession(options);
}

void TxUsage::StartPartitionSession(TTopicReadSessionPtr reader, TTransactionBase& tx, std::uint64_t offset)
{
    auto event = ReadEvent<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(reader, tx);
    Y_ENSURE_BT(event.GetCommittedOffset() == offset);
    event.Confirm();
}

void TxUsage::StartPartitionSession(TTopicReadSessionPtr reader, std::uint64_t offset)
{
    auto event = ReadEvent<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(reader);
    Y_ENSURE_BT(event.GetCommittedOffset() == offset);
    event.Confirm();
}

void TxUsage::ReadMessage(TTopicReadSessionPtr reader, TTransactionBase& tx, std::uint64_t offset)
{
    TReadMessageSettings settings {
        .Tx = tx,
        .CommitOffsets = false,
        .Offset = offset
    };
    ReadMessage(reader, settings);
}

void TxUsage::ReadMessage(TTopicReadSessionPtr reader, const TReadMessageSettings& settings)
{
    auto event = ReadEvent<NTopic::TReadSessionEvent::TDataReceivedEvent>(reader, settings.Tx);
    if (settings.Offset.has_value()) {
        Y_ENSURE_BT(event.GetMessages()[0].GetOffset() == *settings.Offset);
    }
    if (settings.CommitOffsets) {
        event.Commit();
    }
}

template<class E>
E TxUsage::ReadEvent(TTopicReadSessionPtr reader, TTransactionBase& tx)
{
    NTopic::TReadSessionGetEventSettings options;
    options.Block(true);
    options.MaxEventsCount(1);
    options.Tx(tx);

    auto event = reader->GetEvent(options);
    Y_ENSURE_BT(event);

    auto ev = std::get_if<E>(&*event);
    Y_ENSURE_BT(ev);

    return *ev;
}

template<class E>
E TxUsage::ReadEvent(TTopicReadSessionPtr reader)
{
    auto event = reader->GetEvent(true, 1);
    Y_ENSURE_BT(event);

    auto ev = std::get_if<E>(&*event);
    Y_ENSURE_BT(ev);

    return *ev;
}

void TxUsage::WriteMessage(const std::string& message)
{
    NTopic::TWriteSessionSettings options;
    options.Path(GetTopicPath());
    options.MessageGroupId(TEST_MESSAGE_GROUP_ID);

    NTopic::TTopicClient client(GetDriver());
    auto session = client.CreateSimpleBlockingWriteSession(options);
    Y_ENSURE_BT(session->Write(message));
    session->Close();
}

void TxUsage::WriteMessages(const std::vector<std::string>& messages,
                            const std::string& topic, const std::string& groupId,
                            TTransactionBase& tx)
{
    NTopic::TWriteSessionSettings options;
    options.Path(GetTopicPath(topic));
    options.MessageGroupId(groupId);

    NTopic::TTopicClient client(GetDriver());
    auto session = client.CreateSimpleBlockingWriteSession(options);

    for (auto& message : messages) {
        NTopic::TWriteMessage params(message);
        params.Tx(tx);
        Y_ENSURE_BT(session->Write(std::move(params)));
    }

    Y_ENSURE_BT(session->Close());
}

void TxUsage::AddConsumer(const std::string& topicName,
                          const std::vector<std::string>& consumers)
{
    NTopic::TTopicClient client(GetDriver());
    NTopic::TAlterTopicSettings settings;

    for (const auto& consumer : consumers) {
        settings.BeginAddConsumer(GetConsumerName(consumer));
    }

    auto result = client.AlterTopic(GetTopicPath(topicName), settings).GetValueSync();
    Y_ENSURE_BT(result.IsSuccess(), ToString(static_cast<TStatus>(result)));
}

void TxUsage::AlterAutoPartitioning(const std::string& topicName,
                                    std::uint64_t minActivePartitions,
                                    std::uint64_t maxActivePartitions,
                                    EAutoPartitioningStrategy strategy,
                                    TDuration stabilizationWindow,
                                    std::uint64_t downUtilizationPercent,
                                    std::uint64_t upUtilizationPercent)
{
    NTopic::TTopicClient client(GetDriver());
    NTopic::TAlterTopicSettings settings;

    settings
        .BeginAlterPartitioningSettings()
            .MinActivePartitions(minActivePartitions)
            .MaxActivePartitions(maxActivePartitions)
            .BeginAlterAutoPartitioningSettings()
                .Strategy(strategy)
                .StabilizationWindow(stabilizationWindow)
                .DownUtilizationPercent(downUtilizationPercent)
                .UpUtilizationPercent(upUtilizationPercent)
            .EndAlterAutoPartitioningSettings()
        .EndAlterTopicPartitioningSettings()
        ;

    auto result = client.AlterTopic(GetTopicPath(topicName), settings).GetValueSync();
    Y_ENSURE_BT(result.IsSuccess(), ToString(static_cast<TStatus>(result)));
}

const TDriver& TxUsage::GetDriver() const
{
    return *Driver;
}

NTable::TTableClient& TxUsage::GetTableClient()
{
    return *TableClient;
}

void TxUsage::WriteToTopicWithInvalidTxId(bool invalidTxId)
{
    auto session = CreateSession();
    auto tx = session->BeginTx();

    NTopic::TWriteSessionSettings options;
    options.Path(GetTopicPath());
    options.MessageGroupId(TEST_MESSAGE_GROUP_ID);

    NTopic::TTopicClient client(GetDriver());
    auto writeSession = client.CreateWriteSession(options);

    auto event = writeSession->GetEvent(true);
    ASSERT_TRUE(event && std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event.value()));
    auto token = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event.value()).ContinuationToken);

    NTopic::TWriteMessage params("message");
    params.Tx(*tx);

    if (invalidTxId) {
        session->CommitTx(*tx, EStatus::SUCCESS);
    } else {
        session->Close();
    }

    writeSession->Write(std::move(token), std::move(params));

    while (true) {
        event = writeSession->GetEvent(true);
        ASSERT_TRUE(event.has_value());
        auto& v = event.value();
        if (auto e = std::get_if<TWriteSessionEvent::TAcksEvent>(&v); e) {
            FAIL();
        } else if (auto e = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&v); e) {
            ;
        } else if (auto e = std::get_if<TSessionClosedEvent>(&v); e) {
            break;
        }
    }
}

void TxUsage::TestSessionAbort()
{
    {
        auto reader = CreateReader();
        auto session = CreateSession();
        auto tx = session->BeginTx();

        StartPartitionSession(reader, *tx, 0);

        WriteMessage("message #0");
        ReadMessage(reader, *tx, 0);

        WriteMessage("message #1");
        ReadMessage(reader, *tx, 1);
    }

    {
        auto session = CreateSession();
        auto tx = session->BeginTx();
        auto reader = CreateReader();

        StartPartitionSession(reader, *tx, 0);

        ReadMessage(reader, *tx, 0);

        session->CommitTx(*tx, EStatus::SUCCESS);
    }

    {
        auto reader = CreateReader();

        StartPartitionSession(reader, 2);
    }
}

TEST_F(TxUsageTable, TEST_NAME(SessionAbort))
{
    TestSessionAbort();
}

TEST_F(TxUsageQuery, TEST_NAME(SessionAbort))
{
    TestSessionAbort();
}

void TxUsage::TestTwoSessionOneConsumer()
{
    WriteMessage("message #0");

    auto session1 = CreateSession();
    auto tx1 = session1->BeginTx();

    {
        auto reader = CreateReader();
        StartPartitionSession(reader, *tx1, 0);
        ReadMessage(reader, *tx1, 0);
    }

    auto session2 = CreateSession();
    auto tx2 = session2->BeginTx();

    {
        auto reader = CreateReader();
        StartPartitionSession(reader, *tx2, 0);
        ReadMessage(reader, *tx2, 0);
    }

    session2->CommitTx(*tx2, EStatus::SUCCESS);
    session1->CommitTx(*tx1, EStatus::ABORTED);
}

TEST_F(TxUsageTable, TEST_NAME(TwoSessionOneConsumer))
{
    TestTwoSessionOneConsumer();
}

TEST_F(TxUsageQuery, TEST_NAME(TwoSessionOneConsumer))
{
    TestTwoSessionOneConsumer();
}

void TxUsage::TestOffsetsCannotBePromotedWhenReadingInATransaction()
{
    WriteMessage("message");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto reader = CreateReader();
    StartPartitionSession(reader, *tx, 0);

    ASSERT_THROW(ReadMessage(reader, {.Tx = *tx, .CommitOffsets = true}), yexception);
}

TEST_F(TxUsageTable, TEST_NAME(Offsets_Cannot_Be_Promoted_When_Reading_In_A_Transaction))
{
    TestOffsetsCannotBePromotedWhenReadingInATransaction();
}

TEST_F(TxUsageQuery, TEST_NAME(Offsets_Cannot_Be_Promoted_When_Reading_In_A_Transaction))
{
    TestOffsetsCannotBePromotedWhenReadingInATransaction();
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Invalid_Session))
{
    WriteToTopicWithInvalidTxId(false);
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Invalid_Session))
{
    WriteToTopicWithInvalidTxId(false);
}

//Y_UNIT_TEST_F(WriteToTopic_Invalid_Tx, TFixture)
//{
//    WriteToTopicWithInvalidTxId(true);
//}

void TxUsage::TestWriteToTopicTwoWriteSession()
{
    std::string topicName[2] = {
        "topic_A",
        "topic_B"
    };

    CreateTopic(topicName[0]);
    CreateTopic(topicName[1]);

    auto createWriteSession = [this](NTopic::TTopicClient& client, const std::string& topicName) {
        NTopic::TWriteSessionSettings options;
        options.Path(GetTopicPath(topicName));
        options.MessageGroupId(TEST_MESSAGE_GROUP_ID);

        return client.CreateWriteSession(options);
    };

    auto writeMessage = [](auto& ws, const std::string& message, auto& tx) {
        NTopic::TWriteMessage params(message);
        params.Tx(*tx);

        auto event = ws->GetEvent(true);
        Y_ENSURE_BT(event && std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event.value()));
        auto token = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event.value()).ContinuationToken);

        ws->Write(std::move(token), std::move(params));
    };

    auto session = CreateSession();
    auto tx = session->BeginTx();

    NTopic::TTopicClient client(GetDriver());

    auto ws0 = createWriteSession(client, topicName[0]);
    auto ws1 = createWriteSession(client, topicName[1]);

    writeMessage(ws0, "message-1", tx);
    writeMessage(ws1, "message-2", tx);

    std::size_t acks = 0;

    while (acks < 2) {
        auto event = ws0->GetEvent(false);
        if (!event) {
            event = ws1->GetEvent(false);
            if (!event) {
                std::this_thread::sleep_for(10ms);
                continue;
            }
        }

        auto& v = event.value();
        if (auto e = std::get_if<TWriteSessionEvent::TAcksEvent>(&v); e) {
            ++acks;
        } else if (auto e = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&v); e) {
            ;
        } else if (auto e = std::get_if<TSessionClosedEvent>(&v); e) {
            break;
        }
    }

    ASSERT_EQ(acks, 2u);
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Two_WriteSession))
{
    TestWriteToTopicTwoWriteSession();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Two_WriteSession))
{
    TestWriteToTopicTwoWriteSession();
}

auto TxUsage::CreateTopicWriteSession(const std::string& topicName,
                                      const std::string& messageGroupId,
                                      std::optional<std::uint32_t> partitionId) -> TTopicWriteSessionPtr
{
    NTopic::TTopicClient client(GetDriver());
    NTopic::TWriteSessionSettings options;
    options.Path(GetTopicPath(topicName));
    options.ProducerId(messageGroupId);
    options.MessageGroupId(messageGroupId);
    options.PartitionId(partitionId);
    options.Codec(ECodec::RAW);
    return client.CreateWriteSession(options);
}

auto TxUsage::GetTopicWriteSession(const std::string& topicName,
                                   const std::string& messageGroupId,
                                   std::optional<std::uint32_t> partitionId) -> TTopicWriteSessionContext&
{
    std::pair<std::string, std::string> key(topicName, messageGroupId);
    auto i = TopicWriteSessions.find(key);

    if (i == TopicWriteSessions.end()) {
        TTopicWriteSessionContext context;
        context.Session = CreateTopicWriteSession(topicName, messageGroupId, partitionId);

        TopicWriteSessions.emplace(key, std::move(context));

        i = TopicWriteSessions.find(key);
    }

    return i->second;
}

TTopicReadSettings TxUsage::MakeTopicReadSettings(const std::string& topicName,
                                                  std::optional<std::uint32_t> partitionId)
{
    TTopicReadSettings options;
    options.Path(GetTopicPath(topicName));
    if (partitionId) {
        options.AppendPartitionIds(*partitionId);
    }
    return options;
}

TReadSessionSettings TxUsage::MakeTopicReadSessionSettings(const std::string& topicName,
                                                           const std::string& consumerName,
                                                           std::optional<std::uint32_t> partitionId)
{
    NTopic::TReadSessionSettings options;
    options.AppendTopics(MakeTopicReadSettings(topicName, partitionId));
    options.ConsumerName(GetConsumerName(consumerName));
    return options;
}

auto TxUsage::CreateTopicReadSession(const std::string& topicName,
                                     const std::string& consumerName,
                                     std::optional<std::uint32_t> partitionId) -> TTopicReadSessionPtr
{
    NTopic::TTopicClient client(GetDriver());
    return client.CreateReadSession(MakeTopicReadSessionSettings(topicName,
                                                                 consumerName,
                                                                 partitionId));
}

auto TxUsage::GetTopicReadSession(const std::string& topicName,
                                  const std::string& consumerName,
                                  std::optional<std::uint32_t> partitionId) -> TTopicReadSessionPtr
{
    TTopicReadSessionPtr session;

    if (auto i = TopicReadSessions.find(topicName); i == TopicReadSessions.end()) {
        session = CreateTopicReadSession(topicName, consumerName, partitionId);
        auto event = ReadEvent<NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(session);
        event.Confirm();
        TopicReadSessions.emplace(topicName, session);
    } else {
        session = i->second;
    }

    return session;
}

void TxUsage::TTopicWriteSessionContext::WaitForContinuationToken()
{
    while (!ContinuationToken.has_value()) {
        WaitForEvent();
    }
}

void TxUsage::TTopicWriteSessionContext::WaitForEvent()
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
        } else if ([[maybe_unused]] auto* e = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
            ADD_FAILURE();
        }
    }
}

void TxUsage::TTopicWriteSessionContext::Write(const std::string& message, TTransactionBase* tx)
{
    NTopic::TWriteMessage params(message);

    if (tx) {
        params.Tx(*tx);
    }

    Session->Write(std::move(*ContinuationToken),
                   std::move(params));

    ++WriteCount;
    ContinuationToken = std::nullopt;
}

void TxUsage::CloseTopicWriteSession(const std::string& topicName,
                                     const std::string& messageGroupId,
                                     bool force)
{
    std::pair<std::string, std::string> key(topicName, messageGroupId);
    auto i = TopicWriteSessions.find(key);

    Y_ENSURE_BT(i != TopicWriteSessions.end());

    TTopicWriteSessionContext& context = i->second;

    context.Session->Close(force ? TDuration::MilliSeconds(0) : TDuration::Max());
    TopicWriteSessions.erase(key);
}

void TxUsage::WriteToTopic(const std::string& topicName,
                           const std::string& messageGroupId,
                           const std::string& message,
                           TTransactionBase* tx,
                           std::optional<std::uint32_t> partitionId)
{
    TTopicWriteSessionContext& context = GetTopicWriteSession(topicName, messageGroupId, partitionId);
    context.WaitForContinuationToken();
    Y_ENSURE_BT(context.ContinuationToken.has_value());
    context.Write(message, tx);
}

std::vector<std::string> TxUsage::ReadFromTopic(const std::string& topicName,
                                                const std::string& consumerName,
                                                const TDuration& duration,
                                                TTransactionBase* tx,
                                                std::optional<std::uint32_t> partitionId)
{
    std::vector<std::string> messages;

    TInstant end = TInstant::Now() + duration;
    TDuration remain = duration;

    auto session = GetTopicReadSession(topicName, consumerName, partitionId);

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
                    messages.emplace_back(m.GetData());
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

void TxUsage::WaitForAcks(const std::string& topicName, const std::string& messageGroupId, std::size_t writtenInTxCount)
{
    std::pair<std::string, std::string> key(topicName, messageGroupId);
    auto i = TopicWriteSessions.find(key);
    Y_ENSURE_BT(i != TopicWriteSessions.end());

    auto& context = i->second;

    Y_ENSURE_BT(context.AckCount() <= context.WriteCount);

    while (context.AckCount() < context.WriteCount) {
        context.WaitForEvent();
    }

    Y_ENSURE_BT((context.WrittenAckCount + context.WrittenInTxAckCount) == context.WriteCount);

    if (writtenInTxCount != std::numeric_limits<std::size_t>::max()) {
        Y_ENSURE_BT(context.WrittenInTxAckCount, writtenInTxCount);
    }
}

std::vector<std::string> TxUsage::Read_Exactly_N_Messages_From_Topic(const std::string& topicName,
                                                                     const std::string& consumerName,
                                                                     std::size_t limit)
{
    std::vector<std::string> result;

    while (result.size() < limit) {
        auto messages = ReadFromTopic(topicName, consumerName, TDuration::Seconds(2));
        for (auto& m : messages) {
            result.push_back(std::move(m));
        }
    }

    Y_ENSURE_BT(result.size(), limit);

    return result;
}

void TxUsage::TestWriteToTopic1()
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #3", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #4", tx.get());

    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #5", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #6", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #7", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #8", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #9", tx.get());

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        ASSERT_EQ(messages.size(), 0u);
    }

    {
        auto messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
        ASSERT_EQ(messages.size(), 0u);
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 4);
        ASSERT_EQ(messages[0], "message #1");
        ASSERT_EQ(messages[3], "message #4");
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, 5);
        ASSERT_EQ(messages[0], "message #5");
        ASSERT_EQ(messages[4], "message #9");
    }
}

void TxUsage::TestWriteToTopic4()
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    auto session = CreateSession();
    auto tx_1 = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx_1.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", tx_1.get());

    auto tx_2 = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #3", tx_2.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #4", tx_2.get());

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
    ASSERT_EQ(messages.size(), 0u);

    messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
    ASSERT_EQ(messages.size(), 0u);

    session->CommitTx(*tx_2, EStatus::SUCCESS);
    session->CommitTx(*tx_1, EStatus::ABORTED);

    messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
    ASSERT_EQ(messages.size(), 1u);
    ASSERT_EQ(messages[0], "message #3");

    messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
    ASSERT_EQ(messages.size(), 1u);
    ASSERT_EQ(messages[0], "message #4");
}

void TxUsage::TestWriteToTopic7()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #1", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #2", tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, "message #3");
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, "message #4");

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #5", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #6", tx.get());

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 2);
        ASSERT_EQ(messages[0], "message #3");
        ASSERT_EQ(messages[1], "message #4");
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 4);
        ASSERT_EQ(messages[0], "message #1");
        ASSERT_EQ(messages[3], "message #6");
    }
}

void TxUsage::TestWriteToTopic9()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx_1 = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx_1.get());

    auto tx_2 = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx_2.get());

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        ASSERT_EQ(messages.size(), 0u);
    }

    session->CommitTx(*tx_2, EStatus::SUCCESS);
    session->CommitTx(*tx_1, EStatus::ABORTED);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        ASSERT_EQ(messages.size(), 1u);
        ASSERT_EQ(messages[0], "message #2");
    }
}

void TxUsage::TestWriteToTopic10()
{
    CreateTopic("topic_A");

    auto session = CreateSession();

    {
        auto tx_1 = session->BeginTx();

        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx_1.get());

        session->CommitTx(*tx_1, EStatus::SUCCESS);
    }

    {
        auto tx_2 = session->BeginTx();

        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx_2.get());

        session->CommitTx(*tx_2, EStatus::SUCCESS);
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 2);
        ASSERT_EQ(messages[0], "message #1");
        ASSERT_EQ(messages[1], "message #2");
    }
}

void TxUsage::TestWriteToTopic26()
{
    //
    // the test verifies a transaction in which data is read from a partition of one topic and written to
    // another partition of this topic
    //
    const std::uint32_t PARTITION_0 = 0;
    const std::uint32_t PARTITION_1 = 1;

    CreateTopic("topic_A", TEST_CONSUMER, 2);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", nullptr, PARTITION_0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", nullptr, PARTITION_0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #3", nullptr, PARTITION_0);

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), tx.get(), PARTITION_0);
    ASSERT_EQ(messages.size(), 3u);

    for (const auto& m : messages) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, m, tx.get(), PARTITION_1);
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), nullptr, PARTITION_1);
    ASSERT_EQ(messages.size(), 3u);
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_1))
{
    TestWriteToTopic1();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_1))
{
    TestWriteToTopic1();
}

void TxUsage::TestWriteToTopic2()
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #1", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #2", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #3", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #4", tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, "message #5");
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID_2, "message #6");

    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID_1, "message #7", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID_1, "message #8", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID_1, "message #9", tx.get());

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        ASSERT_EQ(messages.size(), 1u);
        ASSERT_EQ(messages[0], "message #5");
    }

    {
        auto messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
        ASSERT_EQ(messages.size(), 1u);
        ASSERT_EQ(messages[0], "message #6");
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 4);
        ASSERT_EQ(messages[0], "message #1");
        ASSERT_EQ(messages[3], "message #4");
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, 3);
        ASSERT_EQ(messages[0], "message #7");
        ASSERT_EQ(messages[2], "message #9");
    }
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_2))
{
    TestWriteToTopic2();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_2))
{
    TestWriteToTopic2();
}

void TxUsage::TestWriteToTopic3()
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #3");

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
    ASSERT_EQ(messages.size(), 1u);
    ASSERT_EQ(messages[0], "message #3");

    session->CommitTx(*tx, EStatus::ABORTED);

    tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());

    session->CommitTx(*tx, EStatus::SUCCESS);

    messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
    ASSERT_EQ(messages.size(), 1u);
    ASSERT_EQ(messages[0], "message #1");

    messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
    ASSERT_EQ(messages.size(), 1u);
    ASSERT_EQ(messages[0], "message #2");
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_3))
{
    TestWriteToTopic3();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_3))
{
    TestWriteToTopic3();
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_4))
{
    TestWriteToTopic4();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_4))
{
    TestWriteToTopic4();
}

void TxUsage::TestWriteToTopic5()
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    auto session = CreateSession();

    {
        auto tx_1 = session->BeginTx();

        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx_1.get());
        WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", tx_1.get());

        session->CommitTx(*tx_1, EStatus::SUCCESS);
    }

    {
        auto tx_2 = session->BeginTx();

        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #3", tx_2.get());
        WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #4", tx_2.get());

        session->CommitTx(*tx_2, EStatus::SUCCESS);
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 2);
        ASSERT_EQ(messages[0], "message #1");
        ASSERT_EQ(messages[1], "message #3");
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, 2);
        ASSERT_EQ(messages[0], "message #2");
        ASSERT_EQ(messages[1], "message #4");
    }
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_5))
{
    TestWriteToTopic5();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic))
{
    TestWriteToTopic5();
}

void TxUsage::TestWriteToTopic6()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        ASSERT_EQ(messages.size(), 0u);
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 2);
        ASSERT_EQ(messages[0], "message #1");
        ASSERT_EQ(messages[1], "message #2");
    }

    DescribeTopic("topic_A");
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_6))
{
    TestWriteToTopic6();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_6))
{
    TestWriteToTopic6();
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_7))
{
    TestWriteToTopic7();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_7))
{
    TestWriteToTopic7();
}

void TxUsage::TestWriteToTopic8()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2");

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        ASSERT_EQ(messages.size(), 1u);
        ASSERT_EQ(messages[0], "message #2");
    }

    session->CommitTx(*tx, EStatus::ABORTED);

    tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        ASSERT_EQ(messages.size(), 1u);
        ASSERT_EQ(messages[0], "message #1");
    }
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_8))
{
    TestWriteToTopic8();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_8))
{
    TestWriteToTopic8();
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_9))
{
    TestWriteToTopic9();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_9))
{
    TestWriteToTopic9();
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_10))
{
    TestWriteToTopic10();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_10))
{
    TestWriteToTopic10();
}

void TxUsage::TestWriteToTopic15()
{
    // the session of writing to the topic can be closed before the commit
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, "message #1", tx.get());
    CloseTopicWriteSession("topic_A", TEST_MESSAGE_GROUP_ID_1);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, "message #2", tx.get());
    CloseTopicWriteSession("topic_A", TEST_MESSAGE_GROUP_ID_2);

    session->CommitTx(*tx, EStatus::SUCCESS);

    auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 2);
    ASSERT_EQ(messages[0], "message #1");
    ASSERT_EQ(messages[1], "message #2");
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_15))
{
    TestWriteToTopic15();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_15))
{
    TestWriteToTopic15();
}

void TxUsage::TestWriteToTopic17()
{
    // TODO(abcdef): temporarily deleted
    return;

    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(22'000'000, 'x'));
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(100, 'x'));
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(200, 'x'));
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(300, 'x'));
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(10'000'000, 'x'));

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string( 6'000'000, 'x'), tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(20'000'000, 'x'), tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string( 7'000'000, 'x'), tx.get());

    session->CommitTx(*tx, EStatus::SUCCESS);

    //RestartPQTablet("topic_A", 0);

    auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 8);
    ASSERT_EQ(messages[0].size(), 22'000'000u);
    ASSERT_EQ(messages[1].size(),        100u);
    ASSERT_EQ(messages[2].size(),        200u);
    ASSERT_EQ(messages[3].size(),        300u);
    ASSERT_EQ(messages[4].size(), 10'000'000u);
    ASSERT_EQ(messages[5].size(),  6'000'000u);
    ASSERT_EQ(messages[6].size(), 20'000'000u);
    ASSERT_EQ(messages[7].size(),  7'000'000u);
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_17))
{
    TestWriteToTopic17();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_17))
{
    TestWriteToTopic17();
}

void TxUsage::TestWriteToTopic25()
{
    //
    // the test verifies a transaction in which data is read from one topic and written to another
    //
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1");
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2");
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #3");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), tx.get());
    ASSERT_EQ(messages.size(), 3u);

    for (const auto& m : messages) {
        WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, m, tx.get());
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, 3);
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_25))
{
    TestWriteToTopic25();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_25))
{
    TestWriteToTopic25();
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_26))
{
    TestWriteToTopic26();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_26))
{
    TestWriteToTopic26();
}

void TxUsage::TestWriteToTopic28()
{
    // The test verifies that the `WriteInflightSize` is correctly considered for the main partition.
    // Writing to the service partition does not change the `WriteInflightSize` of the main one.
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    std::string message(16'000, 'a');

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, std::string(16'000, 'a'), tx.get(), 0);

    session->CommitTx(*tx, EStatus::SUCCESS);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, std::string(20'000, 'b'), nullptr, 0);

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), nullptr, 0);
    ASSERT_EQ(messages.size(), 2u);
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_28))
{
    TestWriteToTopic28();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_28))
{
    TestWriteToTopic28();
}

void TxUsage::WriteMessagesInTx(std::size_t big, std::size_t small)
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    for (std::size_t i = 0; i < big; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(7'000'000, 'x'), tx.get(), 0);
    }

    for (std::size_t i = 0; i < small; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, std::string(16'384, 'x'), tx.get(), 0);
    }

    session->CommitTx(*tx, EStatus::SUCCESS);
}

void TxUsage::TestWriteToTopic29()
{
    WriteMessagesInTx(1, 0);
    WriteMessagesInTx(1, 0);
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_29))
{
    TestWriteToTopic29();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_29))
{
    TestWriteToTopic29();
}

void TxUsage::TestWriteToTopic30()
{
    WriteMessagesInTx(1, 0);
    WriteMessagesInTx(0, 1);
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_30))
{
    TestWriteToTopic30();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_30))
{
    TestWriteToTopic30();
}

void TxUsage::TestWriteToTopic31()
{
    WriteMessagesInTx(1, 0);
    WriteMessagesInTx(1, 1);
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_31))
{
    TestWriteToTopic31();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_31))
{
    TestWriteToTopic31();
}

void TxUsage::TestWriteToTopic32()
{
    WriteMessagesInTx(0, 1);
    WriteMessagesInTx(1, 0);
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_32))
{
    TestWriteToTopic32();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_32))
{
    TestWriteToTopic32();
}

void TxUsage::TestWriteToTopic33()
{
    WriteMessagesInTx(0, 1);
    WriteMessagesInTx(0, 1);
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_33))
{
    TestWriteToTopic33();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_33))
{
    TestWriteToTopic33();
}

void TxUsage::TestWriteToTopic34()
{
    WriteMessagesInTx(0, 1);
    WriteMessagesInTx(1, 1);
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_34))
{
    TestWriteToTopic34();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_34))
{
    TestWriteToTopic34();
}

void TxUsage::TestWriteToTopic35()
{
    WriteMessagesInTx(1, 1);
    WriteMessagesInTx(1, 0);
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_35))
{
    TestWriteToTopic35();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_35))
{
    TestWriteToTopic35();
}

void TxUsage::TestWriteToTopic36()
{
    WriteMessagesInTx(1, 1);
    WriteMessagesInTx(0, 1);
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_36))
{
    TestWriteToTopic36();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_36))
{
    TestWriteToTopic36();
}

void TxUsage::TestWriteToTopic37()
{
    WriteMessagesInTx(1, 1);
    WriteMessagesInTx(1, 1);
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_37))
{
    TestWriteToTopic37();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_37))
{
    TestWriteToTopic37();
}

void TxUsage::TestWriteToTopic39()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());

    AddConsumer("topic_A", {"consumer"});

    session->CommitTx(*tx, EStatus::SUCCESS);

    Read_Exactly_N_Messages_From_Topic("topic_A", "consumer", 2);
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_39))
{
    TestWriteToTopic39();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_39))
{
    TestWriteToTopic39();
}

void TxUsage::TestWriteToTopic48()
{
    // the commit of a transaction affects the split of the partition
    CreateTopic("topic_A", TEST_CONSUMER, 2, 10);
    AlterAutoPartitioning("topic_A", 2, 10, EAutoPartitioningStrategy::ScaleUp, TDuration::Seconds(2), 1, 2);

    auto session = CreateSession();
    auto tx = session->BeginTx();

    std::string message(1_MB, 'x');

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message, tx.get(), 0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message, tx.get(), 0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_3, message, tx.get(), 0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_3, message, tx.get(), 0);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, message, tx.get(), 1);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, message, tx.get(), 1);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_4, message, tx.get(), 1);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_4, message, tx.get(), 1);

    session->CommitTx(*tx, EStatus::SUCCESS);

    std::this_thread::sleep_for(5s);

    auto topicDescription = DescribeTopic("topic_A");

    ASSERT_GT(topicDescription.GetTotalPartitionsCount(), 2u);
}

TEST_F(TxUsageTable, TEST_NAME(WriteToTopic_Demo_48))
{
    TestWriteToTopic48();
}

TEST_F(TxUsageQuery, TEST_NAME(WriteToTopic_Demo_48))
{
    TestWriteToTopic48();
}

TEST_F(TxUsageQuery, TEST_NAME(TestRetentionOnLongTxAndBigMessages))
{
    // TODO uncomment
    return;

    auto bigMessage = []() {
        std::string sb;
        sb.reserve(10_MB);
        for (std::size_t i = 0; i < sb.capacity(); ++i) {
            sb += RandomNumber<char>();
        }
        return std::move(sb);
    };

    auto msg = bigMessage();

    CreateTopic("topic_A", TEST_CONSUMER, 1, 1, TDuration::Seconds(1), true);

    auto session = CreateSession();
    auto tx0 = session->BeginTx();
    auto tx1 = session->BeginTx();

    WriteToTopic("topic_A", "grp-0", msg, tx0.get());
    WriteToTopic("topic_A", "grp-1", msg, tx1.get());

    std::this_thread::sleep_for(3s);

    WriteToTopic("topic_A", "grp-0", "short-msg", tx0.get());
    WriteToTopic("topic_A", "grp-1", "short-msg", tx1.get());

    WriteToTopic("topic_A", "grp-0", msg, tx0.get());
    WriteToTopic("topic_A", "grp-1", msg, tx1.get());

    std::this_thread::sleep_for(3s);

    WriteToTopic("topic_A", "grp-0", msg, tx0.get());
    WriteToTopic("topic_A", "grp-1", msg, tx1.get());

    std::this_thread::sleep_for(3s);

    session->CommitTx(*tx0, EStatus::SUCCESS);
    session->CommitTx(*tx1, EStatus::SUCCESS);

    //RestartPQTablet("topic_A", 0);

    auto read = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
    ASSERT_TRUE(read.size() > 0);
    ASSERT_EQ(msg, read[0]);
}

}
