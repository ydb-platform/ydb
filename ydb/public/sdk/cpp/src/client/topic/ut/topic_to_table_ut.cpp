#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/ut_utils.h>

#include <ydb/public/sdk/cpp/src/library/issue/yql_issue_message.h>

#include <ydb/core/cms/console/console.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/key.h>
#include <ydb/core/persqueue/blob.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/pq_l2_service.h>
#include <ydb/core/tx/long_tx_service/public/events.h>

#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <library/cpp/logger/stream.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/streams/bzip2/bzip2.h>
#include <grpcpp/create_channel.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_query.pb.h>

namespace NYdb::NTopic::NTests {

const auto TEST_MESSAGE_GROUP_ID_1 = TEST_MESSAGE_GROUP_ID + "_1";
const auto TEST_MESSAGE_GROUP_ID_2 = TEST_MESSAGE_GROUP_ID + "_2";
const auto TEST_MESSAGE_GROUP_ID_3 = TEST_MESSAGE_GROUP_ID + "_3";
const auto TEST_MESSAGE_GROUP_ID_4 = TEST_MESSAGE_GROUP_ID + "_4";

Y_UNIT_TEST_SUITE(TxUsage) {

class TFixture : public NUnitTest::TBaseFixture {
protected:
    using TTopicReadSession = NTopic::IReadSession;
    using TTopicReadSessionPtr = std::shared_ptr<TTopicReadSession>;
    using TTopicWriteSession = NTopic::IWriteSession;
    using TTopicWriteSessionPtr = std::shared_ptr<TTopicWriteSession>;

    struct TTopicWriteSessionContext {
        TTopicWriteSessionPtr Session;
        std::optional<NTopic::TContinuationToken> ContinuationToken;
        size_t WriteCount = 0;
        size_t WrittenAckCount = 0;
        size_t WrittenInTxAckCount = 0;

        void WaitForContinuationToken();
        void Write(const std::string& message, TTransactionBase* tx = nullptr);

        size_t AckCount() const { return WrittenAckCount + WrittenInTxAckCount; }

        void WaitForEvent();
    };

    struct TFeatureFlags {
        bool EnablePQConfigTransactionsAtSchemeShard = true;
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

        virtual TAsyncStatus AsyncCommitTx(TTransactionBase& tx) = 0;

        virtual ~ISession() = default;
    };

    void SetUp(NUnitTest::TTestContext&) override;

    void NotifySchemeShard(const TFeatureFlags& flags);

    std::unique_ptr<ISession> CreateSession();

    TTopicReadSessionPtr CreateReader();

    void StartPartitionSession(TTopicReadSessionPtr reader, TTransactionBase& tx, ui64 offset);
    void StartPartitionSession(TTopicReadSessionPtr reader, ui64 offset);

    struct TReadMessageSettings {
        TTransactionBase& Tx;
        bool CommitOffsets = false;
        std::optional<ui64> Offset;
    };

    void ReadMessage(TTopicReadSessionPtr reader, TTransactionBase& tx, ui64 offset);
    void ReadMessage(TTopicReadSessionPtr reader, const TReadMessageSettings& settings);

    void WriteMessage(const TString& message);
    void WriteMessages(const TVector<TString>& messages,
                       const TString& topic, const TString& groupId,
                       TTransactionBase& tx);

    void CreateTopic(const TString& path = TString{TEST_TOPIC},
                     const TString& consumer = TEST_CONSUMER,
                     size_t partitionCount = 1,
                     std::optional<size_t> maxPartitionCount = std::nullopt);
    TTopicDescription DescribeTopic(const TString& path);

    void AddConsumer(const TString& topicPath, const TVector<TString>& consumers);
    void AlterAutoPartitioning(const TString& topicPath,
                               ui64 minActivePartitions,
                               ui64 maxActivePartitions,
                               EAutoPartitioningStrategy strategy,
                               TDuration stabilizationWindow,
                               ui64 downUtilizationPercent,
                               ui64 upUtilizationPercent);
    void SetPartitionWriteSpeed(const std::string& topicPath,
                                size_t bytesPerSeconds);

    void WriteToTopicWithInvalidTxId(bool invalidTxId);

    TTopicWriteSessionPtr CreateTopicWriteSession(const TString& topicPath,
                                                  const TString& messageGroupId,
                                                  std::optional<ui32> partitionId);
    TTopicWriteSessionContext& GetTopicWriteSession(const TString& topicPath,
                                                    const TString& messageGroupId,
                                                    std::optional<ui32> partitionId);

    TTopicReadSessionPtr CreateTopicReadSession(const TString& topicPath,
                                                const TString& consumerName,
                                                TMaybe<ui32> partitionId);
    TTopicReadSessionPtr GetTopicReadSession(const TString& topicPath,
                                             const TString& consumerName,
                                             TMaybe<ui32> partitionId);

    void WriteToTopic(const TString& topicPath,
                      const TString& messageGroupId,
                      const TString& message,
                      TTransactionBase* tx = nullptr,
                      std::optional<ui32> partitionId = std::nullopt);
    TVector<TString> ReadFromTopic(const TString& topicPath,
                                   const TString& consumerName,
                                   const TDuration& duration,
                                   TTransactionBase* tx = nullptr,
                                   TMaybe<ui32> partitionId = Nothing());
    void WaitForAcks(const TString& topicPath,
                     const TString& messageGroupId,
                     size_t writtenInTxCount = Max<size_t>());
    void WaitForSessionClose(const TString& topicPath,
                             const TString& messageGroupId,
                             NYdb::EStatus status);
    void CloseTopicWriteSession(const TString& topicPath,
                                const TString& messageGroupId,
                                bool force = false);
    void CloseTopicReadSession(const TString& topicPath,
                               const TString& consumerName);

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
    void PQTabletPrepareFromResource(const TString& topicPath,
                                     ui32 partitionId,
                                     const TString& resourceName);

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
    void UpsertToTable(const TString& tablePath,
                      const TVector<TTableRecord>& records,
                      ISession& session,
                      TTransactionBase* tx);
    void InsertToTable(const TString& tablePath,
                      const TVector<TTableRecord>& records,
                      ISession& session,
                      TTransactionBase* tx);
    void DeleteFromTable(const TString& tablePath,
                      const TVector<TTableRecord>& records,
                      ISession& session,
                      TTransactionBase* tx);
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

    void WriteMessagesInTx(size_t big, size_t small);

    const TDriver& GetDriver() const;
    NTable::TTableClient& GetTableClient();

    void CheckTabletKeys(const TString& topicName);
    void DumpPQTabletKeys(const TString& topicName);

    TVector<TString> Read_Exactly_N_Messages_From_Topic(const TString& topicPath,
                                                        const TString& consumerName,
                                                        size_t count);

    void TestSessionAbort();

    void TestTwoSessionOneConsumer();

    void TestOffsetsCannotBePromotedWhenReadingInATransaction();

    void TestWriteToTopicTwoWriteSession();

    void TestWriteRandomSizedMessagesInWideTransactions();

    void TestWriteOnlyBigMessagesInWideTransactions();

    void TestTransactionsConflictOnSeqNo();

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

    void TestWriteToTopic11();

    void TestWriteToTopic12();

    void TestWriteToTopic13();

    void TestWriteToTopic14();

    void TestWriteToTopic15();

    void TestWriteToTopic16();

    void TestWriteToTopic17();

    void TestWriteToTopic24();

    void TestWriteToTopic25();

    void TestWriteToTopic26();

    void TestWriteToTopic27();

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

    void TestWriteToTopic38();

    void TestWriteToTopic39();

    void TestWriteToTopic40();

    void TestWriteToTopic41();

    void TestWriteToTopic42();

    void TestWriteToTopic43();

    void TestWriteToTopic44();

    void TestWriteToTopic45();

    void TestWriteToTopic46();

    void TestWriteToTopic47();

    void TestWriteToTopic48();

    void TestWriteToTopic50();

    struct TAvgWriteBytes {
        ui64 PerSec = 0;
        ui64 PerMin = 0;
        ui64 PerHour = 0;
        ui64 PerDay = 0;
    };

    TAvgWriteBytes GetAvgWriteBytes(const TString& topicPath,
                                    ui32 partitionId);

    void CheckAvgWriteBytes(const TString& topicPath,
                            ui32 partitionId,
                            size_t minSize, size_t maxSize);

    void SplitPartition(const TString& topicPath,
                        ui32 partitionId,
                        const TString& boundary);

    virtual bool GetEnableOltpSink() const;
    virtual bool GetEnableOlapSink() const;
    virtual bool GetEnableHtapTx() const;
    virtual bool GetAllowOlapDataQuery() const;

    size_t GetPQCacheRenameKeysCount();

    enum class EClientType {
        Table,
        Query,
        None
    };

    virtual EClientType GetClientType() const = 0;
    virtual ~TFixture() = default;

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

        TAsyncStatus AsyncCommitTx(TTransactionBase& tx) override;

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

        TAsyncStatus AsyncCommitTx(TTransactionBase& tx) override;

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

    ui64 GetTopicTabletId(const TActorId& actorId,
                          const TString& topicPath,
                          ui32 partition);
    std::vector<std::string> GetTabletKeys(const TActorId& actorId,
                                           ui64 tabletId);
    std::vector<std::string> GetPQTabletDataKeys(const TActorId& actorId,
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

    ui64 GetSchemeShardTabletId(const TActorId& actorId);

    std::unique_ptr<TTopicSdkTestSetup> Setup;
    std::unique_ptr<TDriver> Driver;
    std::unique_ptr<NTable::TTableClient> TableClient;
    std::unique_ptr<NQuery::TQueryClient> QueryClient;

    THashMap<std::pair<TString, TString>, TTopicWriteSessionContext> TopicWriteSessions;
    THashMap<TString, TTopicReadSessionPtr> TopicReadSessions;

    ui64 SchemaTxId = 1000;
};

class TFixtureTable : public TFixture {
protected:
    EClientType GetClientType() const override {
        return EClientType::Table;
    }
};

class TFixtureQuery : public TFixture {
protected:
    EClientType GetClientType() const override {
        return EClientType::Query;
    }
};

class TFixtureNoClient : public TFixture {
protected:
    EClientType GetClientType() const override {
        return EClientType::None;
    }
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
    settings.SetEnableTopicSplitMerge(true);
    settings.SetEnablePQConfigTransactionsAtSchemeShard(true);
    settings.SetEnableOltpSink(GetEnableOltpSink());
    settings.SetEnableOlapSink(GetEnableOlapSink());
    settings.SetEnableHtapTx(GetEnableHtapTx());
    settings.SetAllowOlapDataQuery(GetAllowOlapDataQuery());

    Setup = std::make_unique<TTopicSdkTestSetup>(TEST_CASE_NAME, settings);

    Driver = std::make_unique<TDriver>(Setup->MakeDriver());
    auto tableSettings = NTable::TClientSettings().SessionPoolSettings(NTable::TSessionPoolSettings()
        .MaxActiveSessions(3000)
    );

    auto querySettings = NQuery::TClientSettings().SessionPoolSettings(NQuery::TSessionPoolSettings()
        .MaxActiveSessions(3000)
    );

    TableClient = std::make_unique<NTable::TTableClient>(*Driver, tableSettings);
    QueryClient = std::make_unique<NQuery::TQueryClient>(*Driver, querySettings);
}

void TFixture::NotifySchemeShard(const TFeatureFlags& flags)
{
    auto request = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationRequest>();
    *request->Record.MutableConfig() = *Setup->GetServer().ServerSettings.AppConfig;
    request->Record.MutableConfig()->MutableFeatureFlags()->SetEnablePQConfigTransactionsAtSchemeShard(flags.EnablePQConfigTransactionsAtSchemeShard);

    auto& runtime = Setup->GetRuntime();
    auto actorId = runtime.AllocateEdgeActor();

    ui64 ssId = GetSchemeShardTabletId(actorId);

    runtime.SendToPipe(ssId, actorId, request.release());
    runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>();
}

TFixture::TTableSession::TTableSession(NTable::TTableClient& client)
    : Session_(Init(client))
{
}

NTable::TSession TFixture::TTableSession::Init(NTable::TTableClient& client)
{
    auto result = client.GetSession().ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result.GetSession();
}

std::vector<TResultSet> TFixture::TTableSession::Execute(const std::string& query,
                                                         TTransactionBase* tx,
                                                         bool commit,
                                                         const TParams& params)
{
    auto txTable = dynamic_cast<NTable::TTransaction*>(tx);
    auto txControl = NTable::TTxControl::Tx(*txTable).CommitTx(commit);

    auto result = Session_.ExecuteDataQuery(query, txControl, params).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    return std::move(result).ExtractResultSets();
}

TFixture::ISession::TExecuteInTxResult TFixture::TTableSession::ExecuteInTx(const std::string& query,
                                                                            bool commit,
                                                                            const TParams& params)
{
    auto txControl = NTable::TTxControl::BeginTx().CommitTx(commit);

    auto result = Session_.ExecuteDataQuery(query, txControl, params).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    return {std::move(result).ExtractResultSets(), std::make_unique<NTable::TTransaction>(*result.GetTransaction())};
}

std::unique_ptr<TTransactionBase> TFixture::TTableSession::BeginTx()
{
    while (true) {
        auto result = Session_.BeginTransaction().ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return std::make_unique<NTable::TTransaction>(result.GetTransaction());
        }
        Sleep(TDuration::MilliSeconds(100));
    }
}

void TFixture::TTableSession::CommitTx(TTransactionBase& tx, EStatus status)
{
    auto txTable = dynamic_cast<NTable::TTransaction&>(tx);
    while (true) {
        auto result = txTable.Commit().ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), status, result.GetIssues().ToString());
            return;
        }
        Sleep(TDuration::MilliSeconds(100));
    }
}

void TFixture::TTableSession::RollbackTx(TTransactionBase& tx, EStatus status)
{
    auto txTable = dynamic_cast<NTable::TTransaction&>(tx);
    while (true) {
        auto result = txTable.Rollback().ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), status, result.GetIssues().ToString());
            return;
        }
        Sleep(TDuration::MilliSeconds(100));
    }
}

void TFixture::TTableSession::Close()
{
    Session_.Close();
}

TAsyncStatus TFixture::TTableSession::AsyncCommitTx(TTransactionBase& tx)
{
    auto txTable = dynamic_cast<NTable::TTransaction&>(tx);
    return txTable.Commit().Apply([](auto result) {
        return TStatus(result.GetValue());
    });
}

TFixture::TQuerySession::TQuerySession(NQuery::TQueryClient& client,
                                       const std::string& endpoint,
                                       const std::string& database)
    : Session_(Init(client))
    , Endpoint_(endpoint)
    , Database_(database)
{
}

NQuery::TSession TFixture::TQuerySession::Init(NQuery::TQueryClient& client)
{
    auto result = client.GetSession().ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result.GetSession();
}

std::vector<TResultSet> TFixture::TQuerySession::Execute(const std::string& query,
                                                         TTransactionBase* tx,
                                                         bool commit,
                                                         const TParams& params)
{
    auto txQuery = dynamic_cast<NQuery::TTransaction*>(tx);
    auto txControl = NQuery::TTxControl::Tx(*txQuery).CommitTx(commit);

    auto result = Session_.ExecuteQuery(query, txControl, params).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    return result.GetResultSets();
}

TFixture::ISession::TExecuteInTxResult TFixture::TQuerySession::ExecuteInTx(const std::string& query,
                                                                            bool commit,
                                                                            const TParams& params)
{
    auto txControl = NQuery::TTxControl::BeginTx().CommitTx(commit);

    auto result = Session_.ExecuteQuery(query, txControl, params).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    return {result.GetResultSets(), std::make_unique<NQuery::TTransaction>(*result.GetTransaction())};
}

std::unique_ptr<TTransactionBase> TFixture::TQuerySession::BeginTx()
{
    while (true) {
        auto result = Session_.BeginTransaction(NQuery::TTxSettings()).ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return std::make_unique<NQuery::TTransaction>(result.GetTransaction());
        }
        Sleep(TDuration::MilliSeconds(100));
    }
}

void TFixture::TQuerySession::CommitTx(TTransactionBase& tx, EStatus status)
{
    auto txQuery = dynamic_cast<NQuery::TTransaction&>(tx);
    while (true) {
        auto result = txQuery.Commit().ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), status, result.GetIssues().ToString());
            return;
        }
        Sleep(TDuration::MilliSeconds(100));
    }
}

void TFixture::TQuerySession::RollbackTx(TTransactionBase& tx, EStatus status)
{
    auto txQuery = dynamic_cast<NQuery::TTransaction&>(tx);
    while (true) {
        auto result = txQuery.Rollback().ExtractValueSync();
        if (result.GetStatus() != EStatus::SESSION_BUSY) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), status, result.GetIssues().ToString());
            return;
        }
        Sleep(TDuration::MilliSeconds(100));
    }
}

void TFixture::TQuerySession::Close()
{
    // SDK doesn't provide a method to close the session for Query Client, so we use grpc API directly
    auto credentials = grpc::InsecureChannelCredentials();
    auto channel = grpc::CreateChannel(TString(Endpoint_), credentials);
    auto stub = Ydb::Query::V1::QueryService::NewStub(channel);

    grpc::ClientContext context;
    context.AddMetadata("x-ydb-database", TString(Database_));

    Ydb::Query::DeleteSessionRequest request;
    request.set_session_id(Session_.GetId());

    Ydb::Query::DeleteSessionResponse response;
    auto status = stub->DeleteSession(&context, request, &response);

    NIssue::TIssues issues;
    NYdb::NIssue::IssuesFromMessage(response.issues(), issues);
    UNIT_ASSERT_C(status.ok(), status.error_message());
    UNIT_ASSERT_VALUES_EQUAL_C(response.status(), Ydb::StatusIds::SUCCESS, issues.ToString());
}

TAsyncStatus TFixture::TQuerySession::AsyncCommitTx(TTransactionBase& tx)
{
    auto txQuery = dynamic_cast<NQuery::TTransaction&>(tx);
    return txQuery.Commit().Apply([](auto result) {
        return TStatus(result.GetValue());
    });
}

std::unique_ptr<TFixture::ISession> TFixture::CreateSession()
{
    switch (GetClientType()) {
        case EClientType::Table: {
            UNIT_ASSERT_C(TableClient, "TableClient is not initialized");
            return std::make_unique<TFixture::TTableSession>(*TableClient);
        }
        case EClientType::Query: {
            UNIT_ASSERT_C(QueryClient, "QueryClient is not initialized");
            return std::make_unique<TFixture::TQuerySession>(*QueryClient,
                                                             Setup->GetEndpoint(),
                                                             Setup->GetDatabase());
        }
        case EClientType::None: {
            UNIT_FAIL("CreateSession is forbidden for None client type");
        }
    }

    return nullptr;
}

auto TFixture::CreateReader() -> TTopicReadSessionPtr
{
    NTopic::TTopicClient client(GetDriver());
    TReadSessionSettings options;
    options.ConsumerName(TEST_CONSUMER);
    options.AppendTopics(TEST_TOPIC);
    return client.CreateReadSession(options);
}

void TFixture::StartPartitionSession(TTopicReadSessionPtr reader, TTransactionBase& tx, ui64 offset)
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

void TFixture::ReadMessage(TTopicReadSessionPtr reader, TTransactionBase& tx, ui64 offset)
{
    TReadMessageSettings settings {
        .Tx = tx,
        .CommitOffsets = false,
        .Offset = offset
    };
    ReadMessage(reader, settings);
}

void TFixture::ReadMessage(TTopicReadSessionPtr reader, const TReadMessageSettings& settings)
{
    auto event = ReadEvent<NTopic::TReadSessionEvent::TDataReceivedEvent>(reader, settings.Tx);
    if (settings.Offset.has_value()) {
        UNIT_ASSERT_VALUES_EQUAL(event.GetMessages()[0].GetOffset(), *settings.Offset);
    }
    if (settings.CommitOffsets) {
        event.Commit();
    }
}

template<class E>
E TFixture::ReadEvent(TTopicReadSessionPtr reader, TTransactionBase& tx)
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
                             TTransactionBase& tx)
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

void TFixture::AddConsumer(const TString& topicPath,
                           const TVector<TString>& consumers)
{
    NTopic::TTopicClient client(GetDriver());
    NTopic::TAlterTopicSettings settings;

    for (const auto& consumer : consumers) {
        settings.BeginAddConsumer(consumer);
    }

    auto result = client.AlterTopic(topicPath, settings).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void TFixture::AlterAutoPartitioning(const TString& topicPath,
                                     ui64 minActivePartitions,
                                     ui64 maxActivePartitions,
                                     EAutoPartitioningStrategy strategy,
                                     TDuration stabilizationWindow,
                                     ui64 downUtilizationPercent,
                                     ui64 upUtilizationPercent)
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

    auto result = client.AlterTopic(topicPath, settings).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void TFixture::SetPartitionWriteSpeed(const std::string& topicPath,
                                      size_t bytesPerSeconds)
{
    NTopic::TTopicClient client(GetDriver());
    NTopic::TAlterTopicSettings settings;

    settings.SetPartitionWriteSpeedBytesPerSecond(bytesPerSeconds);

    auto result = client.AlterTopic(topicPath, settings).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

TTopicDescription TFixture::DescribeTopic(const TString& path)
{
    return Setup->DescribeTopic(path);
}

const TDriver& TFixture::GetDriver() const
{
    return *Driver;
}

NTable::TTableClient& TFixture::GetTableClient()
{
    return *TableClient;
}

void TFixture::WriteToTopicWithInvalidTxId(bool invalidTxId)
{
    auto session = CreateSession();
    auto tx = session->BeginTx();

    NTopic::TWriteSessionSettings options;
    options.Path(TEST_TOPIC);
    options.MessageGroupId(TEST_MESSAGE_GROUP_ID);

    NTopic::TTopicClient client(GetDriver());
    auto writeSession = client.CreateWriteSession(options);

    auto event = writeSession->GetEvent(true);
    UNIT_ASSERT(event && std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event.value()));
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
        UNIT_ASSERT(event.has_value());
        auto& v = event.value();
        if (auto e = std::get_if<TWriteSessionEvent::TAcksEvent>(&v); e) {
            UNIT_ASSERT(false);
        } else if (auto e = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&v); e) {
            ;
        } else if (auto e = std::get_if<TSessionClosedEvent>(&v); e) {
            break;
        }
    }
}

void TFixture::TestSessionAbort()
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

Y_UNIT_TEST_F(SessionAbort_Table, TFixtureTable)
{
    TestSessionAbort();
}

Y_UNIT_TEST_F(SessionAbort_Query, TFixtureQuery)
{
    TestSessionAbort();
}

void TFixture::TestTwoSessionOneConsumer()
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

Y_UNIT_TEST_F(TwoSessionOneConsumer_Table, TFixtureTable)
{
    TestTwoSessionOneConsumer();
}

Y_UNIT_TEST_F(TwoSessionOneConsumer_Query, TFixtureQuery)
{
    TestTwoSessionOneConsumer();
}

void TFixture::TestOffsetsCannotBePromotedWhenReadingInATransaction()
{
    WriteMessage("message");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto reader = CreateReader();
    StartPartitionSession(reader, *tx, 0);

    UNIT_ASSERT_EXCEPTION(ReadMessage(reader, {.Tx = *tx, .CommitOffsets = true}), yexception);
}

Y_UNIT_TEST_F(Offsets_Cannot_Be_Promoted_When_Reading_In_A_Transaction_Table, TFixtureTable)
{
    TestOffsetsCannotBePromotedWhenReadingInATransaction();
}

Y_UNIT_TEST_F(Offsets_Cannot_Be_Promoted_When_Reading_In_A_Transaction_Query, TFixtureQuery)
{
    TestOffsetsCannotBePromotedWhenReadingInATransaction();
}

Y_UNIT_TEST_F(WriteToTopic_Invalid_Session_Table, TFixtureTable)
{
    WriteToTopicWithInvalidTxId(false);
}

Y_UNIT_TEST_F(WriteToTopic_Invalid_Session_Query, TFixtureQuery)
{
    WriteToTopicWithInvalidTxId(false);
}

//Y_UNIT_TEST_F(WriteToTopic_Invalid_Tx, TFixture)
//{
//    WriteToTopicWithInvalidTxId(true);
//}

void TFixture::TestWriteToTopicTwoWriteSession()
{
    TString topicPath[2] = {
        TString{TEST_TOPIC},
        TString{TEST_TOPIC} + "_2"
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
        params.Tx(*tx);

        auto event = ws->GetEvent(true);
        UNIT_ASSERT(event && std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event.value()));
        auto token = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event.value()).ContinuationToken);

        ws->Write(std::move(token), std::move(params));
    };

    auto session = CreateSession();
    auto tx = session->BeginTx();

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

        auto& v = event.value();
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

Y_UNIT_TEST_F(WriteToTopic_Two_WriteSession_Table, TFixtureTable)
{
    TestWriteToTopicTwoWriteSession();
}

Y_UNIT_TEST_F(WriteToTopic_Two_WriteSession_Query, TFixtureQuery)
{
    TestWriteToTopicTwoWriteSession();
}

auto TFixture::CreateTopicWriteSession(const TString& topicPath,
                                       const TString& messageGroupId,
                                       std::optional<ui32> partitionId) -> TTopicWriteSessionPtr
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
                                     std::optional<ui32> partitionId) -> TTopicWriteSessionContext&
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
    while (!ContinuationToken.has_value()) {
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
        } else if ([[maybe_unused]] auto* e = std::get_if<NTopic::TSessionClosedEvent>(&event)) {
            UNIT_FAIL("");
        }
    }
}

void TFixture::TTopicWriteSessionContext::Write(const std::string& message, TTransactionBase* tx)
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

void TFixture::CloseTopicWriteSession(const TString& topicPath,
                                      const TString& messageGroupId,
                                      bool force)
{
    std::pair<TString, TString> key(topicPath, messageGroupId);
    auto i = TopicWriteSessions.find(key);

    UNIT_ASSERT(i != TopicWriteSessions.end());

    TTopicWriteSessionContext& context = i->second;

    context.Session->Close(force ? TDuration::MilliSeconds(0) : TDuration::Max());
    TopicWriteSessions.erase(key);
}

void TFixture::CloseTopicReadSession(const TString& topicPath,
                                     const TString& consumerName)
{
    Y_UNUSED(consumerName);
    TopicReadSessions.erase(topicPath);
}

void TFixture::WriteToTopic(const TString& topicPath,
                            const TString& messageGroupId,
                            const TString& message,
                            TTransactionBase* tx,
                            std::optional<ui32> partitionId)
{
    TTopicWriteSessionContext& context = GetTopicWriteSession(topicPath, messageGroupId, partitionId);
    context.WaitForContinuationToken();
    UNIT_ASSERT(context.ContinuationToken.has_value());
    context.Write(message, tx);
}

TVector<TString> TFixture::ReadFromTopic(const TString& topicPath,
                                         const TString& consumerName,
                                         const TDuration& duration,
                                         TTransactionBase* tx,
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

ui64 TFixture::GetSchemeShardTabletId(const TActorId& actorId)
{
    auto navigate = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
    navigate->DatabaseName = "/Root";

    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = SplitPath("/Root");
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

    return front.Self->Info.GetSchemeshardId();
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

std::vector<std::string> TFixture::GetTabletKeys(const TActorId& actorId,
                                                 ui64 tabletId)
{
    auto request = std::make_unique<NKikimr::TEvKeyValue::TEvRequest>();
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
    auto response = runtime.GrabEdgeEvent<NKikimr::TEvKeyValue::TEvResponse>();

    UNIT_ASSERT(response->Record.HasCookie());
    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetCookie(), 12345);
    UNIT_ASSERT_VALUES_EQUAL(response->Record.ReadRangeResultSize(), 1);

    std::vector<std::string> keys;

    auto& result = response->Record.GetReadRangeResult(0);
    for (size_t i = 0; i < result.PairSize(); ++i) {
        auto& kv = result.GetPair(i);
        keys.emplace_back(kv.GetKey());
    }

    return keys;
}

std::vector<std::string> TFixture::GetPQTabletDataKeys(const TActorId& actorId,
                                                       ui64 tabletId)
{
    using namespace NKikimr::NPQ;

    std::vector<std::string> keys;

    for (const auto& key : GetTabletKeys(actorId, tabletId)) {
        if (key.empty() ||
            ((std::tolower(key.front()) != TKeyPrefix::TypeData) &&
             (std::tolower(key.front()) != TKeyPrefix::TypeTmpData))) {
            continue;
        }

        keys.push_back(key);
    }

    return keys;
}

size_t TFixture::GetPQCacheRenameKeysCount()
{
    using namespace NKikimr::NPQ;

    auto& runtime = Setup->GetRuntime();
    TActorId edge = runtime.AllocateEdgeActor();

    auto request = MakeHolder<TEvPqCache::TEvCacheKeysRequest>();

    runtime.Send(MakePersQueueL2CacheID(), edge, request.Release());

    TAutoPtr<IEventHandle> handle;
    auto* result = runtime.GrabEdgeEvent<TEvPqCache::TEvCacheKeysResponse>(handle);

    return result->RenamedKeys;
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

TVector<TString> TFixture::Read_Exactly_N_Messages_From_Topic(const TString& topicPath,
                                                              const TString& consumerName,
                                                              size_t limit)
{
    TVector<TString> result;

    while (result.size() < limit) {
        auto messages = ReadFromTopic(topicPath, consumerName, TDuration::Seconds(2));
        for (auto& m : messages) {
            result.push_back(std::move(m));
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(result.size(), limit);

    return result;
}

void TFixture::TestWriteToTopic1()
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
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);
    }

    {
        auto messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 4);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages[3], "message #4");
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, 5);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #5");
        UNIT_ASSERT_VALUES_EQUAL(messages[4], "message #9");
    }
}

void TFixture::TestWriteToTopic4()
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
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);

    messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);

    session->CommitTx(*tx_2, EStatus::SUCCESS);
    session->CommitTx(*tx_1, EStatus::ABORTED);

    messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #3");

    messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #4");
}

void TFixture::TestWriteToTopic7()
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
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #3");
        UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #4");
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 4);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages[3], "message #6");
    }
}

void TFixture::TestWriteToTopic9()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx_1 = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx_1.get());

    auto tx_2 = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx_2.get());

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);
    }

    session->CommitTx(*tx_2, EStatus::SUCCESS);
    session->CommitTx(*tx_1, EStatus::ABORTED);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #2");
    }
}

void TFixture::TestWriteToTopic10()
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
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #2");
    }
}

void TFixture::TestWriteToTopic11()
{
    for (auto endOfTransaction : {Commit, Rollback, CloseTableSession}) {
        TestTheCompletionOfATransaction({.Topics={"topic_A"}, .EndOfTransaction = endOfTransaction});
        TestTheCompletionOfATransaction({.Topics={"topic_A", "topic_B"}, .EndOfTransaction = endOfTransaction});
    }
}

void TFixture::TestWriteToTopic24()
{
    //
    // the test verifies a transaction in which data is written to a topic and to a table
    //
    CreateTopic("topic_A");
    CreateTable("/Root/table_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();
    UpsertToTable("table_A", records, *session, tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());

    session->CommitTx(*tx, EStatus::SUCCESS);

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], MakeJsonDoc(records));

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), records.size());

    CheckTabletKeys("topic_A");
}

void TFixture::TestWriteToTopic26()
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

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), tx.get(), PARTITION_0);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 3);

    for (const auto& m : messages) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, m, tx.get(), PARTITION_1);
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), nullptr, PARTITION_1);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 3);
}

void TFixture::TestWriteToTopic27()
{
    CreateTopic("topic_A", TEST_CONSUMER);
    CreateTopic("topic_B", TEST_CONSUMER);
    CreateTopic("topic_C", TEST_CONSUMER);

    for (size_t i = 0; i < 2; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", nullptr, 0);
        WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", nullptr, 0);

        auto session = CreateSession();
        auto tx = session->BeginTx();

        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), tx.get(), 0);
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);

        WriteToTopic("topic_C", TEST_MESSAGE_GROUP_ID, messages[0], tx.get(), 0);

        messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2), tx.get(), 0);
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);

        WriteToTopic("topic_C", TEST_MESSAGE_GROUP_ID, messages[0], tx.get(), 0);

        session->CommitTx(*tx, EStatus::SUCCESS);

        messages = ReadFromTopic("topic_C", TEST_CONSUMER, TDuration::Seconds(2), nullptr, 0);
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);

        DumpPQTabletKeys("topic_A");
        DumpPQTabletKeys("topic_B");
        DumpPQTabletKeys("topic_C");
    }
}

auto TFixture::GetAvgWriteBytes(const TString& topicName,
                                ui32 partitionId) -> TAvgWriteBytes
{
    auto& runtime = Setup->GetRuntime();
    TActorId edge = runtime.AllocateEdgeActor();
    ui64 tabletId = GetTopicTabletId(edge, "/Root/" + topicName, partitionId);

    runtime.SendToPipe(tabletId, edge, new NKikimr::TEvPersQueue::TEvStatus());
    auto response = runtime.GrabEdgeEvent<NKikimr::TEvPersQueue::TEvStatusResponse>();

    UNIT_ASSERT_VALUES_EQUAL(tabletId, response->Record.GetTabletId());

    TAvgWriteBytes result;

    for (size_t i = 0; i < response->Record.PartResultSize(); ++i) {
        const auto& partition = response->Record.GetPartResult(i);
        if (partition.GetPartition() == static_cast<int>(partitionId)) {
            result.PerSec = partition.GetAvgWriteSpeedPerSec();
            result.PerMin = partition.GetAvgWriteSpeedPerMin();
            result.PerHour = partition.GetAvgWriteSpeedPerHour();
            result.PerDay = partition.GetAvgWriteSpeedPerDay();
            break;
        }
    }

    return result;
}

bool TFixture::GetEnableOltpSink() const
{
    return false;
}

bool TFixture::GetEnableOlapSink() const
{
    return false;
}

bool TFixture::GetEnableHtapTx() const
{
    return false;
}

bool TFixture::GetAllowOlapDataQuery() const
{
    return false;
}

Y_UNIT_TEST_F(WriteToTopic_Demo_1_Table, TFixtureTable)
{
    TestWriteToTopic1();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_1_Query, TFixtureQuery)
{
    TestWriteToTopic1();
}

void TFixture::TestWriteToTopic2()
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
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #5");
    }

    {
        auto messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #6");
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 4);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages[3], "message #4");
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, 3);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #7");
        UNIT_ASSERT_VALUES_EQUAL(messages[2], "message #9");
    }
}

Y_UNIT_TEST_F(WriteToTopic_Demo_2_Table, TFixtureTable)
{
    TestWriteToTopic2();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_2_Query, TFixtureQuery)
{
    TestWriteToTopic2();
}

void TFixture::TestWriteToTopic3()
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #3");

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #3");

    session->CommitTx(*tx, EStatus::ABORTED);

    tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());

    session->CommitTx(*tx, EStatus::SUCCESS);

    messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");

    messages = ReadFromTopic("topic_B", TEST_CONSUMER, TDuration::Seconds(2));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #2");
}

Y_UNIT_TEST_F(WriteToTopic_Demo_3_Table, TFixtureTable)
{
    TestWriteToTopic3();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_3_Query, TFixtureQuery)
{
    TestWriteToTopic3();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_4_Table, TFixtureTable)
{
    TestWriteToTopic4();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_4_Query, TFixtureQuery)
{
    TestWriteToTopic4();
}

void TFixture::TestWriteToTopic5()
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
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #3");
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #2");
        UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #4");
    }
}

Y_UNIT_TEST_F(WriteToTopic_Demo_5_Table, TFixtureTable)
{
    TestWriteToTopic5();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_5_Query, TFixtureQuery)
{
    TestWriteToTopic5();
}

void TFixture::TestWriteToTopic6()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 2);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #2");
    }

    DescribeTopic("topic_A");
}

Y_UNIT_TEST_F(WriteToTopic_Demo_6_Table, TFixtureTable)
{
    TestWriteToTopic6();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_6_Query, TFixtureQuery)
{
    TestWriteToTopic6();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_7_Table, TFixtureTable)
{
    TestWriteToTopic7();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_7_Query, TFixtureQuery)
{
    TestWriteToTopic7();
}

void TFixture::TestWriteToTopic8()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2");

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #2");
    }

    session->CommitTx(*tx, EStatus::ABORTED);

    tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
    }
}

Y_UNIT_TEST_F(WriteToTopic_Demo_8_Table, TFixtureTable)
{
    TestWriteToTopic8();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_8_Query, TFixtureQuery)
{
    TestWriteToTopic8();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_9_Table, TFixtureTable)
{
    TestWriteToTopic9();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_9_Query, TFixtureQuery)
{
    TestWriteToTopic9();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_10_Table, TFixtureTable)
{
    TestWriteToTopic10();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_10_Query, TFixtureQuery)
{
    TestWriteToTopic10();
}

NPQ::TWriteId TFixture::GetTransactionWriteId(const TActorId& actorId,
                                              ui64 tabletId)
{
    auto request = std::make_unique<NKikimr::TEvKeyValue::TEvRequest>();
    request->Record.SetCookie(12345);
    request->Record.AddCmdRead()->SetKey("_txinfo");

    auto& runtime = Setup->GetRuntime();

    runtime.SendToPipe(tabletId, actorId, request.release());
    auto response = runtime.GrabEdgeEvent<NKikimr::TEvKeyValue::TEvResponse>();

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
        auto request = std::make_unique<NKikimr::TEvKeyValue::TEvRequest>();
        request->Record.SetCookie(12345);
        request->Record.AddCmdRead()->SetKey("_txinfo");

        auto& runtime = Setup->GetRuntime();

        runtime.SendToPipe(tabletId, actorId, request.release());
        auto response = runtime.GrabEdgeEvent<NKikimr::TEvKeyValue::TEvResponse>();

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
            if ((NPQ::GetWriteId(writeInfo) == writeId) && writeInfo.HasOriginalPartitionId()) {
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
    std::vector<std::string> keys;
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

void TFixture::PQTabletPrepareFromResource(const TString& topicPath,
                                           ui32 partitionId,
                                           const TString& resourceName)
{
    auto& runtime = Setup->GetRuntime();
    TActorId edge = runtime.AllocateEdgeActor();
    ui64 tabletId = GetTopicTabletId(edge, "/Root/" + topicPath, partitionId);

    auto request = MakeHolder<TEvKeyValue::TEvRequest>();
    size_t count = 0;

    for (TStringStream stream(NResource::Find(resourceName)); true; ++count) {
        TString key, encoded;

        if (!stream.ReadTo(key, ' ')) {
            break;
        }
        encoded = stream.ReadLine();

        auto decoded = Base64Decode(encoded);
        TStringInput decodedStream(decoded);
        TBZipDecompress decompressor(&decodedStream);

        auto* cmd = request->Record.AddCmdWrite();
        cmd->SetKey(key);
        cmd->SetValue(decompressor.ReadAll());
    }

    runtime.SendToPipe(tabletId, edge, request.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto* response = runtime.GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);
    UNIT_ASSERT(response);
    UNIT_ASSERT(response->Record.HasStatus());
    UNIT_ASSERT_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);

    UNIT_ASSERT_VALUES_EQUAL(response->Record.WriteResultSize(), count);

    for (size_t i = 0; i < response->Record.WriteResultSize(); ++i) {
        const auto &result = response->Record.GetWriteResult(i);
        UNIT_ASSERT(result.HasStatus());
        UNIT_ASSERT_EQUAL(result.GetStatus(), NKikimrProto::OK);
    }
}

void TFixture::TestTheCompletionOfATransaction(const TTransactionCompletionTestDescription& d)
{
    for (auto& topic : d.Topics) {
        CreateTopic(topic);
    }

    {
        auto session = CreateSession();
        auto tx = session->BeginTx();

        for (auto& topic : d.Topics) {
            WriteToTopic(topic, TEST_MESSAGE_GROUP_ID, "message", tx.get());
            // TODO: нужен callback для RollbakTx
            WaitForAcks(topic, TEST_MESSAGE_GROUP_ID);
        }

        switch (d.EndOfTransaction) {
        case Commit:
            session->CommitTx(*tx, EStatus::SUCCESS);
            break;
        case Rollback:
            session->RollbackTx(*tx, EStatus::SUCCESS);
            break;
        case CloseTableSession:
            break;
        }
    }

    Sleep(TDuration::Seconds(5));

    for (auto& topic : d.Topics) {
        CheckTabletKeys(topic);
    }

    for (auto& topic : d.Topics) {
        CloseTopicWriteSession(topic, TEST_MESSAGE_GROUP_ID);
    }
}

Y_UNIT_TEST_F(WriteToTopic_Demo_11_Table, TFixtureTable)
{
    TestWriteToTopic11();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_11_Query, TFixtureQuery)
{
    TestWriteToTopic11();
}

void TFixture::TestWriteToTopic12()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    DeleteSupportivePartition("topic_A", 0);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());
    WaitForSessionClose("topic_A", TEST_MESSAGE_GROUP_ID, NYdb::EStatus::PRECONDITION_FAILED);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_12_Table, TFixtureTable)
{
    TestWriteToTopic12();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_12_Query, TFixtureQuery)
{
    TestWriteToTopic12();
}

void TFixture::TestWriteToTopic13()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message", tx.get());
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    DeleteSupportivePartition("topic_A", 0);

    session->CommitTx(*tx, EStatus::ABORTED);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_13_Table, TFixtureTable)
{
    TestWriteToTopic13();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_13_Query, TFixtureQuery)
{
    TestWriteToTopic13();
}

void TFixture::TestWriteToTopic14()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    DeleteSupportivePartition("topic_A", 0);

    CloseTopicWriteSession("topic_A", TEST_MESSAGE_GROUP_ID);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());

    session->CommitTx(*tx, EStatus::ABORTED);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_14_Table, TFixtureTable)
{
    TestWriteToTopic14();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_14_Query, TFixtureQuery)
{
    TestWriteToTopic14();
}

void TFixture::TestWriteToTopic15()
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
    UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
    UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #2");
}

Y_UNIT_TEST_F(WriteToTopic_Demo_15_Table, TFixtureTable)
{
    TestWriteToTopic15();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_15_Query, TFixtureQuery)
{
    TestWriteToTopic15();
}

void TFixture::TestWriteToTopic16()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());

    RestartPQTablet("topic_A", 0);

    session->CommitTx(*tx, EStatus::SUCCESS);

    auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 2);
    UNIT_ASSERT_VALUES_EQUAL(messages[0], "message #1");
    UNIT_ASSERT_VALUES_EQUAL(messages[1], "message #2");
}

Y_UNIT_TEST_F(WriteToTopic_Demo_16_Table, TFixtureTable)
{
    TestWriteToTopic16();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_16_Query, TFixtureQuery)
{
    TestWriteToTopic16();
}

void TFixture::TestWriteToTopic17()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(22'000'000, 'x'));
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(100, 'x'));
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(200, 'x'));
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(300, 'x'));
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(10'000'000, 'x'));

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString( 6'000'000, 'x'), tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(20'000'000, 'x'), tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString( 7'000'000, 'x'), tx.get());

    session->CommitTx(*tx, EStatus::SUCCESS);

    //RestartPQTablet("topic_A", 0);

    auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 8);
    UNIT_ASSERT_VALUES_EQUAL(messages[0].size(), 22'000'000);
    UNIT_ASSERT_VALUES_EQUAL(messages[1].size(),        100);
    UNIT_ASSERT_VALUES_EQUAL(messages[2].size(),        200);
    UNIT_ASSERT_VALUES_EQUAL(messages[3].size(),        300);
    UNIT_ASSERT_VALUES_EQUAL(messages[4].size(), 10'000'000);
    UNIT_ASSERT_VALUES_EQUAL(messages[5].size(),  6'000'000);
    UNIT_ASSERT_VALUES_EQUAL(messages[6].size(), 20'000'000);
    UNIT_ASSERT_VALUES_EQUAL(messages[7].size(),  7'000'000);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_17_Table, TFixtureTable)
{
    TestWriteToTopic17();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_17_Query, TFixtureQuery)
{
    TestWriteToTopic17();
}

void TFixture::TestTxWithBigBlobs(const TTestTxWithBigBlobsParams& params)
{
    size_t oldHeadMsgCount = 0;
    size_t bigBlobMsgCount = 0;
    size_t newHeadMsgCount = 0;

    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    for (size_t i = 0; i < params.OldHeadCount; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(100'000, 'x'));
        ++oldHeadMsgCount;
    }

    for (size_t i = 0; i < params.BigBlobsCount; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(7'000'000, 'x'), tx.get());
        ++bigBlobMsgCount;
    }

    for (size_t i = 0; i < params.NewHeadCount; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(100'000, 'x'), tx.get());
        ++newHeadMsgCount;
    }

    if (params.RestartMode == ERestartBeforeCommit) {
        RestartPQTablet("topic_A", 0);
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

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
        UNIT_ASSERT_VALUES_EQUAL(messages[start + i].size(), 7'000'000);
    }
    start += bigBlobMsgCount;

    for (size_t i = 0; i < newHeadMsgCount; ++i) {
        UNIT_ASSERT_VALUES_EQUAL(messages[start + i].size(), 100'000);
    }
}

#define Y_UNIT_TEST_WITH_REBOOTS(name, oldHeadCount, bigBlobsCount, newHeadCount) \
Y_UNIT_TEST_F(name##_RestartNo_Table, TFixtureTable) { \
    TestTxWithBigBlobs({.OldHeadCount = oldHeadCount, .BigBlobsCount = bigBlobsCount, .NewHeadCount = newHeadCount, .RestartMode = ERestartNo}); \
} \
Y_UNIT_TEST_F(name##_RestartNo_Query, TFixtureQuery) { \
    TestTxWithBigBlobs({.OldHeadCount = oldHeadCount, .BigBlobsCount = bigBlobsCount, .NewHeadCount = newHeadCount, .RestartMode = ERestartNo}); \
} \
Y_UNIT_TEST_F(name##_RestartBeforeCommit_Table, TFixtureTable) { \
    TestTxWithBigBlobs({.OldHeadCount = oldHeadCount, .BigBlobsCount = bigBlobsCount, .NewHeadCount = newHeadCount, .RestartMode = ERestartBeforeCommit}); \
} \
Y_UNIT_TEST_F(name##_RestartBeforeCommit_Query, TFixtureQuery) { \
    TestTxWithBigBlobs({.OldHeadCount = oldHeadCount, .BigBlobsCount = bigBlobsCount, .NewHeadCount = newHeadCount, .RestartMode = ERestartBeforeCommit}); \
} \
Y_UNIT_TEST_F(name##_RestartAfterCommit_Table, TFixtureTable) { \
    TestTxWithBigBlobs({.OldHeadCount = oldHeadCount, .BigBlobsCount = bigBlobsCount, .NewHeadCount = newHeadCount, .RestartMode = ERestartAfterCommit}); \
} \
Y_UNIT_TEST_F(name##_RestartAfterCommit_Query, TFixtureQuery) { \
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

    auto createSessionResult = GetTableClient().CreateSession().ExtractValueSync();
    UNIT_ASSERT_C(createSessionResult.IsSuccess(), createSessionResult.GetIssues().ToString());
    auto session = createSessionResult.GetSession();

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

void TFixture::UpsertToTable(const TString& tablePath,
                            const TVector<TTableRecord>& records,
                            ISession& session,
                            TTransactionBase* tx)
{
    TString query = Sprintf("DECLARE $key AS Utf8;"
                            "DECLARE $value AS Utf8;"
                            "UPSERT INTO `%s` (key, value) VALUES ($key, $value);",
                            tablePath.data());

    for (const auto& r : records) {
        auto params = TParamsBuilder()
                .AddParam("$key").Utf8(r.Key).Build()
                .AddParam("$value").Utf8(r.Value).Build()
            .Build();

        session.Execute(query, tx, false, params);
    }
}

void TFixture::InsertToTable(const TString& tablePath,
                            const TVector<TTableRecord>& records,
                            ISession& session,
                            TTransactionBase* tx)
{
    TString query = Sprintf("DECLARE $key AS Utf8;"
                            "DECLARE $value AS Utf8;"
                            "INSERT INTO `%s` (key, value) VALUES ($key, $value);",
                            tablePath.data());

    for (const auto& r : records) {
        auto params = TParamsBuilder()
                .AddParam("$key").Utf8(r.Key).Build()
                .AddParam("$value").Utf8(r.Value).Build()
            .Build();

        session.Execute(query, tx, false, params);
    }
}

void TFixture::DeleteFromTable(const TString& tablePath,
                            const TVector<TTableRecord>& records,
                            ISession& session,
                            TTransactionBase* tx)
{
    TString query = Sprintf("DECLARE $key AS Utf8;"
                            "DECLARE $value AS Utf8;"
                            "DELETE FROM `%s` ON (key, value) VALUES ($key, $value);",
                            tablePath.data());

    for (const auto& r : records) {
        auto params = TParamsBuilder()
                .AddParam("$key").Utf8(r.Key).Build()
                .AddParam("$value").Utf8(r.Value).Build()
            .Build();

        session.Execute(query, tx, false, params);
    }
}

size_t TFixture::GetTableRecordsCount(const TString& tablePath)
{
    TString query = Sprintf(R"(SELECT COUNT(*) FROM `%s`)",
                            tablePath.data());
    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto result = session->Execute(query, tx.get());

    NYdb::TResultSetParser parser(result.at(0));
    UNIT_ASSERT(parser.TryNextRow());

    return parser.ColumnParser(0).GetUint64();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_24_Table, TFixtureTable)
{
    TestWriteToTopic24();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_24_Query, TFixtureQuery)
{
    TestWriteToTopic24();
}

void TFixture::TestWriteToTopic25()
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
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 3);

    for (const auto& m : messages) {
        WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, m, tx.get());
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, 3);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_25_Table, TFixtureTable)
{
    TestWriteToTopic25();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_25_Query, TFixtureQuery)
{
    TestWriteToTopic25();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_26_Table, TFixtureTable)
{
    TestWriteToTopic26();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_26_Query, TFixtureQuery)
{
    TestWriteToTopic26();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_27_Table, TFixtureTable)
{
    TestWriteToTopic27();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_27_Query, TFixtureQuery)
{
    TestWriteToTopic27();
}

void TFixture::TestWriteToTopic28()
{
    // The test verifies that the `WriteInflightSize` is correctly considered for the main partition.
    // Writing to the service partition does not change the `WriteInflightSize` of the main one.
    CreateTopic("topic_A", TEST_CONSUMER);

    auto session = CreateSession();
    auto tx = session->BeginTx();

    TString message(16'000, 'a');

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, TString(16'000, 'a'), tx.get(), 0);

    session->CommitTx(*tx, EStatus::SUCCESS);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, TString(20'000, 'b'), nullptr, 0);

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), nullptr, 0);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_28_Table, TFixtureTable)
{
    TestWriteToTopic28();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_28_Query, TFixtureQuery)
{
    TestWriteToTopic28();
}

void TFixture::WriteMessagesInTx(size_t big, size_t small)
{
    CreateTopic("topic_A", TEST_CONSUMER);

    auto session = CreateSession();
    auto tx = session->BeginTx();

    for (size_t i = 0; i < big; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(7'000'000, 'x'), tx.get(), 0);
    }

    for (size_t i = 0; i < small; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(16'384, 'x'), tx.get(), 0);
    }

    session->CommitTx(*tx, EStatus::SUCCESS);
}

void TFixture::TestWriteToTopic29()
{
    WriteMessagesInTx(1, 0);
    WriteMessagesInTx(1, 0);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_29_Table, TFixtureTable)
{
    TestWriteToTopic29();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_29_Query, TFixtureQuery)
{
    TestWriteToTopic29();
}

void TFixture::TestWriteToTopic30()
{
    WriteMessagesInTx(1, 0);
    WriteMessagesInTx(0, 1);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_30_Table, TFixtureTable)
{
    TestWriteToTopic30();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_30_Query, TFixtureQuery)
{
    TestWriteToTopic30();
}

void TFixture::TestWriteToTopic31()
{
    WriteMessagesInTx(1, 0);
    WriteMessagesInTx(1, 1);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_31_Table, TFixtureTable)
{
    TestWriteToTopic31();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_31_Query, TFixtureQuery)
{
    TestWriteToTopic31();
}

void TFixture::TestWriteToTopic32()
{
    WriteMessagesInTx(0, 1);
    WriteMessagesInTx(1, 0);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_32_Table, TFixtureTable)
{
    TestWriteToTopic32();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_32_Query, TFixtureQuery)
{
    TestWriteToTopic32();
}

void TFixture::TestWriteToTopic33()
{
    WriteMessagesInTx(0, 1);
    WriteMessagesInTx(0, 1);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_33_Table, TFixtureTable)
{
    TestWriteToTopic33();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_33_Query, TFixtureQuery)
{
    TestWriteToTopic33();
}

void TFixture::TestWriteToTopic34()
{
    WriteMessagesInTx(0, 1);
    WriteMessagesInTx(1, 1);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_34_Table, TFixtureTable)
{
    TestWriteToTopic34();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_34_Query, TFixtureQuery)
{
    TestWriteToTopic34();
}

void TFixture::TestWriteToTopic35()
{
    WriteMessagesInTx(1, 1);
    WriteMessagesInTx(1, 0);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_35_Table, TFixtureTable)
{
    TestWriteToTopic35();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_35_Query, TFixtureQuery)
{
    TestWriteToTopic35();
}

void TFixture::TestWriteToTopic36()
{
    WriteMessagesInTx(1, 1);
    WriteMessagesInTx(0, 1);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_36_Table, TFixtureTable)
{
    TestWriteToTopic36();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_36_Query, TFixtureQuery)
{
    TestWriteToTopic36();
}

void TFixture::TestWriteToTopic37()
{
    WriteMessagesInTx(1, 1);
    WriteMessagesInTx(1, 1);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_37_Table, TFixtureTable)
{
    TestWriteToTopic37();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_37_Query, TFixtureQuery)
{
    TestWriteToTopic37();
}

void TFixture::TestWriteToTopic38()
{
    WriteMessagesInTx(2, 202);
    WriteMessagesInTx(2, 200);
    WriteMessagesInTx(0, 1);
    WriteMessagesInTx(4, 0);
    WriteMessagesInTx(0, 1);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_38_Table, TFixtureTable)
{
    TestWriteToTopic38();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_38_Query, TFixtureQuery)
{
    TestWriteToTopic38();
}

void TFixture::TestWriteToTopic39()
{
    CreateTopic("topic_A", TEST_CONSUMER);

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());

    AddConsumer("topic_A", {"consumer"});

    session->CommitTx(*tx, EStatus::SUCCESS);

    Read_Exactly_N_Messages_From_Topic("topic_A", "consumer", 2);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_39_Table, TFixtureTable)
{
    TestWriteToTopic39();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_39_Query, TFixtureQuery)
{
    TestWriteToTopic39();
}

Y_UNIT_TEST_F(ReadRuleGeneration, TFixtureNoClient)
{
    // There was a server
    NotifySchemeShard({.EnablePQConfigTransactionsAtSchemeShard = false});

    // Users have created their own topic on it
    CreateTopic(TString{TEST_TOPIC});

    // And they wrote their messages into it
    WriteToTopic(TString{TEST_TOPIC}, TEST_MESSAGE_GROUP_ID, "message-1");
    WriteToTopic(TString{TEST_TOPIC}, TEST_MESSAGE_GROUP_ID, "message-2");
    WriteToTopic(TString{TEST_TOPIC}, TEST_MESSAGE_GROUP_ID, "message-3");

    // And he had a consumer
    AddConsumer(TString{TEST_TOPIC}, {"consumer-1"});

    // We read messages from the topic and committed offsets
    Read_Exactly_N_Messages_From_Topic(TString{TEST_TOPIC}, "consumer-1", 3);
    CloseTopicReadSession(TString{TEST_TOPIC}, "consumer-1");

    // And then the Logbroker team turned on the feature flag
    NotifySchemeShard({.EnablePQConfigTransactionsAtSchemeShard = true});

    // Users continued to write to the topic
    WriteToTopic(TString{TEST_TOPIC}, TEST_MESSAGE_GROUP_ID, "message-4");

    // Users have added new consumers
    AddConsumer(TString{TEST_TOPIC}, {"consumer-2"});

    // And they wanted to continue reading their messages
    Read_Exactly_N_Messages_From_Topic(TString{TEST_TOPIC}, "consumer-1", 1);
}

void TFixture::TestWriteToTopic40()
{
    // The recording stream will run into a quota. Before the commit, the client will receive confirmations
    // for some of the messages. The `CommitTx` call will wait for the rest.
    CreateTopic("topic_A", TEST_CONSUMER);

    auto session = CreateSession();
    auto tx = session->BeginTx();

    for (size_t k = 0; k < 100; ++k) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(1'000'000, 'a'), tx.get());
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 100);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_40_Table, TFixtureTable)
{
    TestWriteToTopic40();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_40_Query, TFixtureQuery)
{
    TestWriteToTopic40();
}

void TFixture::TestWriteToTopic41()
{
    // If the recording session does not wait for confirmations, the commit will fail
    CreateTopic("topic_A", TEST_CONSUMER);

    auto session = CreateSession();
    auto tx = session->BeginTx();

    for (size_t k = 0; k < 100; ++k) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(1'000'000, 'a'), tx.get());
    }

    CloseTopicWriteSession("topic_A", TEST_MESSAGE_GROUP_ID, true); // force close

    session->CommitTx(*tx, EStatus::SESSION_EXPIRED);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_41_Table, TFixtureTable)
{
    TestWriteToTopic41();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_41_Query, TFixtureQuery)
{
    TestWriteToTopic41();
}

void TFixture::TestWriteToTopic42()
{
    CreateTopic("topic_A", TEST_CONSUMER);

    auto session = CreateSession();
    auto tx = session->BeginTx();

    for (size_t k = 0; k < 100; ++k) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(1'000'000, 'a'), tx.get());
    }

    CloseTopicWriteSession("topic_A", TEST_MESSAGE_GROUP_ID); // gracefully close

    session->CommitTx(*tx, EStatus::SUCCESS);

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 100);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_42_Table, TFixtureTable)
{
    TestWriteToTopic42();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_42_Query, TFixtureQuery)
{
    TestWriteToTopic42();
}

void TFixture::TestWriteToTopic43()
{
    // The recording stream will run into a quota. Before the commit, the client will receive confirmations
    // for some of the messages. The `ExecuteDataQuery` call will wait for the rest.
    CreateTopic("topic_A", TEST_CONSUMER);

    auto session = CreateSession();
    auto tx = session->BeginTx();

    for (size_t k = 0; k < 100; ++k) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(1'000'000, 'a'), tx.get());
    }

    session->Execute("SELECT 1", tx.get());

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 100);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_43_Table, TFixtureTable)
{
    TestWriteToTopic43();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_43_Query, TFixtureQuery)
{
    TestWriteToTopic43();
}

void TFixture::TestWriteToTopic44()
{
    CreateTopic("topic_A", TEST_CONSUMER);

    auto session = CreateSession();

    auto [_, tx] = session->ExecuteInTx("SELECT 1", false);

    for (size_t k = 0; k < 100; ++k) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, TString(1'000'000, 'a'), tx.get());
    }

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(60));
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 0);

    session->Execute("SELECT 2", tx.get());

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 100);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_44_Table, TFixtureTable)
{
    TestWriteToTopic44();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_44_Query, TFixtureQuery)
{
    TestWriteToTopic44();
}

void TFixture::CheckAvgWriteBytes(const TString& topicPath,
                                  ui32 partitionId,
                                  size_t minSize, size_t maxSize)
{
#define UNIT_ASSERT_AVGWRITEBYTES(v, minSize, maxSize) \
    UNIT_ASSERT_LE_C(minSize, v, ", actual " << minSize << " > " << v); \
    UNIT_ASSERT_LE_C(v, maxSize, ", actual " << v << " > " << maxSize);

    auto avgWriteBytes = GetAvgWriteBytes(topicPath, partitionId);

    UNIT_ASSERT_AVGWRITEBYTES(avgWriteBytes.PerSec, minSize, maxSize);
    UNIT_ASSERT_AVGWRITEBYTES(avgWriteBytes.PerMin, minSize, maxSize);
    UNIT_ASSERT_AVGWRITEBYTES(avgWriteBytes.PerHour, minSize, maxSize);
    UNIT_ASSERT_AVGWRITEBYTES(avgWriteBytes.PerDay, minSize, maxSize);

#undef UNIT_ASSERT_AVGWRITEBYTES
}

void TFixture::SplitPartition(const TString& topicName,
                              ui32 partitionId,
                              const TString& boundary)
{
    NKikimr::NPQ::NTest::SplitPartition(Setup->GetRuntime(),
                                        ++SchemaTxId,
                                        topicName,
                                        partitionId,
                                        boundary);
}

void TFixture::TestWriteToTopic45()
{
    // Writing to a topic in a transaction affects the `AvgWriteBytes` indicator
    CreateTopic("topic_A", TEST_CONSUMER, 2);

    auto session = CreateSession();
    auto tx = session->BeginTx();

    TString message(1'000, 'x');

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message, tx.get(), 0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message, tx.get(), 0);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, message, tx.get(), 1);

    session->CommitTx(*tx, EStatus::SUCCESS);

    size_t minSize = (message.size() + TEST_MESSAGE_GROUP_ID_1.size()) * 2;
    size_t maxSize = minSize + 200;

    CheckAvgWriteBytes("topic_A", 0, minSize, maxSize);

    minSize = (message.size() + TEST_MESSAGE_GROUP_ID_2.size());
    maxSize = minSize + 200;

    CheckAvgWriteBytes("topic_A", 1, minSize, maxSize);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_45_Table, TFixtureTable)
{
    TestWriteToTopic45();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_45_Query, TFixtureQuery)
{
    TestWriteToTopic45();
}

void TFixture::TestWriteToTopic46()
{
    // The `split` operation of the topic partition affects the writing in the transaction.
    // The transaction commit should fail with an error
    CreateTopic("topic_A", TEST_CONSUMER, 2, 10);

    auto session = CreateSession();
    auto tx = session->BeginTx();

    TString message(1'000, 'x');

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message, tx.get(), 0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message, tx.get(), 0);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, message, tx.get(), 1);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID_2);

    SplitPartition("topic_A", 1, "\xC0");

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, message, tx.get(), 1);

    session->CommitTx(*tx, EStatus::ABORTED);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_46_Table, TFixtureTable)
{
    TestWriteToTopic46();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_46_Query, TFixtureQuery)
{
    TestWriteToTopic46();
}

void TFixture::TestWriteToTopic47()
{
    // The `split` operation of the topic partition does not affect the reading in the transaction.
    CreateTopic("topic_A", TEST_CONSUMER, 2, 10);

    TString message(1'000, 'x');

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message, nullptr, 0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message, nullptr, 0);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, message, nullptr, 1);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, message, nullptr, 1);

    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID_1);
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID_2);

    SplitPartition("topic_A", 1, "\xC0");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), tx.get(), 0);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);

    CloseTopicReadSession("topic_A", TEST_CONSUMER);

    messages = ReadFromTopic("topic_A", TEST_CONSUMER, TDuration::Seconds(2), tx.get(), 1);
    UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);

    session->CommitTx(*tx, EStatus::SUCCESS);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_47_Table, TFixtureTable)
{
    TestWriteToTopic47();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_47_Query, TFixtureQuery)
{
    TestWriteToTopic47();
}

void TFixture::TestWriteToTopic48()
{
    // the commit of a transaction affects the split of the partition
    CreateTopic("topic_A", TEST_CONSUMER, 2, 10);
    AlterAutoPartitioning("topic_A", 2, 10, EAutoPartitioningStrategy::ScaleUp, TDuration::Seconds(2), 1, 2);

    auto session = CreateSession();
    auto tx = session->BeginTx();

    TString message(1_MB, 'x');

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message, tx.get(), 0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message, tx.get(), 0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_3, message, tx.get(), 0);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_3, message, tx.get(), 0);

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, message, tx.get(), 1);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, message, tx.get(), 1);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_4, message, tx.get(), 1);
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_4, message, tx.get(), 1);

    session->CommitTx(*tx, EStatus::SUCCESS);

    Sleep(TDuration::Seconds(5));

    auto topicDescription = DescribeTopic("topic_A");

    UNIT_ASSERT_GT(topicDescription.GetTotalPartitionsCount(), 2);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_48_Table, TFixtureTable)
{
    TestWriteToTopic48();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_48_Query, TFixtureQuery)
{
    TestWriteToTopic48();
}

void TFixture::TestWriteToTopic50()
{
    // We write to the topic in the transaction. When a transaction is committed, the keys in the blob
    // cache are renamed.
    CreateTopic("topic_A", TEST_CONSUMER);
    CreateTopic("topic_B", TEST_CONSUMER);

    TString message(128_KB, 'x');

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_1, message);
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID_1);

    auto session = CreateSession();

    // tx #1
    // After the transaction commit, there will be no large blobs in the batches.  The number of renames
    // will not change in the cache.
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, message, tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID_3, message, tx.get());

    UNIT_ASSERT_VALUES_EQUAL(GetPQCacheRenameKeysCount(), 0);

    session->CommitTx(*tx, EStatus::SUCCESS);

    Sleep(TDuration::Seconds(5));

    UNIT_ASSERT_VALUES_EQUAL(GetPQCacheRenameKeysCount(), 0);

    // tx #2
    // After the commit, the party will rename one big blob
    tx = session->BeginTx();

    for (unsigned i = 0; i < 80; ++i) {
        WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID_2, message, tx.get());
    }

    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID_3, message, tx.get());

    UNIT_ASSERT_VALUES_EQUAL(GetPQCacheRenameKeysCount(), 0);

    session->CommitTx(*tx, EStatus::SUCCESS);

    Sleep(TDuration::Seconds(5));

    UNIT_ASSERT_VALUES_EQUAL(GetPQCacheRenameKeysCount(), 1);
}

Y_UNIT_TEST_F(WriteToTopic_Demo_50_Table, TFixtureTable)
{
    TestWriteToTopic50();
}

Y_UNIT_TEST_F(WriteToTopic_Demo_50_Query, TFixtureQuery)
{
    TestWriteToTopic50();
}

class TFixtureSinks : public TFixture {
protected:
    void CreateRowTable(const TString& path);
    void CreateColumnTable(const TString& tablePath);

    bool GetEnableOltpSink() const override;
    bool GetEnableOlapSink() const override;
    bool GetEnableHtapTx() const override;
    bool GetAllowOlapDataQuery() const override;

    void TestSinksOltpWriteToTopic5();

    void TestSinksOltpWriteToTopicAndTable2();
    void TestSinksOltpWriteToTopicAndTable3();
    void TestSinksOltpWriteToTopicAndTable4();
    void TestSinksOltpWriteToTopicAndTable5();
    void TestSinksOltpWriteToTopicAndTable6();

    void TestSinksOlapWriteToTopicAndTable1();
    void TestSinksOlapWriteToTopicAndTable2();
    void TestSinksOlapWriteToTopicAndTable3();
    void TestSinksOlapWriteToTopicAndTable4();
};

class TFixtureSinksTable : public TFixtureSinks {
protected:
    EClientType GetClientType() const override {
        return EClientType::Table;
    }
};

class TFixtureSinksQuery : public TFixtureSinks {
protected:
    EClientType GetClientType() const override {
        return EClientType::Query;
    }
};

void TFixtureSinks::CreateRowTable(const TString& path)
{
    CreateTable(path);
}

void TFixtureSinks::CreateColumnTable(const TString& tablePath)
{
    UNIT_ASSERT(!tablePath.empty());

    TString path = (tablePath[0] != '/') ? ("/Root/" + tablePath) : tablePath;

    auto createSessionResult = GetTableClient().CreateSession().ExtractValueSync();
    UNIT_ASSERT_C(createSessionResult.IsSuccess(), createSessionResult.GetIssues().ToString());
    auto session = createSessionResult.GetSession();

    auto desc = NTable::TTableBuilder()
        .SetStoreType(NTable::EStoreType::Column)
        .AddNonNullableColumn("key", EPrimitiveType::Utf8)
        .AddNonNullableColumn("value", EPrimitiveType::Utf8)
        .SetPrimaryKeyColumn("key")
        .Build();
    auto result = session.CreateTable(path, std::move(desc)).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

bool TFixtureSinks::GetEnableOltpSink() const
{
    return true;
}

bool TFixtureSinks::GetEnableOlapSink() const
{
    return true;
}

bool TFixtureSinks::GetEnableHtapTx() const
{
    return true;
}

bool TFixtureSinks::GetAllowOlapDataQuery() const
{
    return true;
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_1_Table, TFixtureSinksTable)
{
    TestWriteToTopic7();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_1_Query, TFixtureSinksQuery)
{
    TestWriteToTopic7();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_2_Table, TFixtureSinksTable)
{
    TestWriteToTopic10();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_2_Query, TFixtureSinksQuery)
{
    TestWriteToTopic10();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_3_Table, TFixtureSinksTable)
{
    TestWriteToTopic26();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_3_Query, TFixtureSinksQuery)
{
    TestWriteToTopic26();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_4_Table, TFixtureSinksTable)
{
    TestWriteToTopic9();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_4_Query, TFixtureSinksQuery)
{
    TestWriteToTopic9();
}

void TFixtureSinks::TestSinksOltpWriteToTopic5()
{
    CreateTopic("topic_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 0);

    session->RollbackTx(*tx, EStatus::SUCCESS);

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 0);
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_5_Table, TFixtureSinksTable)
{
    TestSinksOltpWriteToTopic5();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopic_5_Query, TFixtureSinksQuery)
{
    TestSinksOltpWriteToTopic5();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopics_1_Table, TFixtureSinksTable)
{
    TestWriteToTopic1();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopics_1_Query, TFixtureSinksQuery)
{
    TestWriteToTopic1();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopics_2_Table, TFixtureSinksTable)
{
    TestWriteToTopic27();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopics_2_Query, TFixtureSinksQuery)
{
    TestWriteToTopic27();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopics_3_Table, TFixtureSinksTable)
{
    TestWriteToTopic11();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopics_3_Query, TFixtureSinksQuery)
{
    TestWriteToTopic11();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopics_4_Table, TFixtureSinksTable)
{
    TestWriteToTopic4();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopics_4_Query, TFixtureSinksQuery)
{
    TestWriteToTopic4();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_1_Table, TFixtureSinksTable)
{
    TestWriteToTopic24();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_1_Query, TFixtureSinksQuery)
{
    TestWriteToTopic24();
}

void TFixtureSinks::TestSinksOltpWriteToTopicAndTable2()
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");
    CreateRowTable("/Root/table_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();
    UpsertToTable("table_A", records, *session, tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());

    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #3", tx.get());

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), MakeJsonDoc(records));
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, 3);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages.back(), "message #3");
    }

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), records.size());

    CheckTabletKeys("topic_A");
    CheckTabletKeys("topic_B");
}


Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_2_Table, TFixtureSinksTable)
{
    TestSinksOltpWriteToTopicAndTable2();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_2_Query, TFixtureSinksQuery)
{
    TestSinksOltpWriteToTopicAndTable2();
}

void TFixtureSinks::TestSinksOltpWriteToTopicAndTable3()
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    CreateRowTable("/Root/table_A");
    CreateRowTable("/Root/table_B");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();
    UpsertToTable("table_A", records, *session, tx.get());
    UpsertToTable("table_B", records, *session, tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());

    const size_t topicMsgCnt = 10;
    for (size_t i = 1; i <= topicMsgCnt; ++i) {
        WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #" + std::to_string(i), tx.get());
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), MakeJsonDoc(records));
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, topicMsgCnt);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages.back(), "message #" + std::to_string(topicMsgCnt));
    }

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), records.size());
    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_B"), records.size());

    CheckTabletKeys("topic_A");
    CheckTabletKeys("topic_B");
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_3_Table, TFixtureSinksTable)
{
    TestSinksOltpWriteToTopicAndTable3();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_3_Query, TFixtureSinksQuery)
{
    TestSinksOltpWriteToTopicAndTable3();
}

void TFixtureSinks::TestSinksOltpWriteToTopicAndTable4()
{
    CreateTopic("topic_A");
    CreateRowTable("/Root/table_A");

    auto session = CreateSession();
    auto tx1 = session->BeginTx();
    auto tx2 = session->BeginTx();

    session->Execute(R"(SELECT COUNT(*) FROM `table_A`)", tx1.get(), false);

    auto records = MakeTableRecords();
    UpsertToTable("table_A", records, *session, tx2.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx1.get());
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    session->CommitTx(*tx2, EStatus::SUCCESS);
    session->CommitTx(*tx1, EStatus::ABORTED);

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 0);

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), records.size());

    CheckTabletKeys("topic_A");
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_4_Table, TFixtureSinksTable)
{
    TestSinksOltpWriteToTopicAndTable4();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_4_Query, TFixtureSinksQuery)
{
    TestSinksOltpWriteToTopicAndTable4();
}

void TFixtureSinks::TestSinksOltpWriteToTopicAndTable5()
{
    CreateTopic("topic_A");
    CreateRowTable("/Root/table_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();
    UpsertToTable("table_A", records, *session, tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    session->RollbackTx(*tx, EStatus::SUCCESS);

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 0);

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), 0);

    CheckTabletKeys("topic_A");
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_5_Table, TFixtureSinksTable)
{
    TestSinksOltpWriteToTopicAndTable5();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_5_Query, TFixtureSinksQuery)
{
    TestSinksOltpWriteToTopicAndTable5();
}

void TFixtureSinks::TestSinksOltpWriteToTopicAndTable6()
{
    CreateTopic("topic_A");
    CreateTopic("topic_B");
    CreateRowTable("/Root/table_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();
    InsertToTable("table_A", records, *session, tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());

    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #1", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #2", tx.get());
    WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #3", tx.get());

    DeleteFromTable("table_A", records, *session, tx.get());

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), MakeJsonDoc(records));
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, 3);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages.back(), "message #3");
    }

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), 0);

    CheckTabletKeys("topic_A");
    CheckTabletKeys("topic_B");
}


Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_6_Table, TFixtureSinksTable)
{
    TestSinksOltpWriteToTopicAndTable6();
}

Y_UNIT_TEST_F(Sinks_Oltp_WriteToTopicAndTable_6_Query, TFixtureSinksQuery)
{
    TestSinksOltpWriteToTopicAndTable6();
}

void TFixtureSinks::TestSinksOlapWriteToTopicAndTable1()
{
    return; // https://github.com/ydb-platform/ydb/issues/17271
    CreateTopic("topic_A");
    CreateColumnTable("/Root/table_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();
    UpsertToTable("table_A", records, *session, tx.get());
    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());

    session->CommitTx(*tx, EStatus::SUCCESS);

    auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 1);
    UNIT_ASSERT_VALUES_EQUAL(messages.front(), MakeJsonDoc(records));

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), records.size());

    CheckTabletKeys("topic_A");
}

Y_UNIT_TEST_F(Sinks_Olap_WriteToTopicAndTable_1_Table, TFixtureSinksTable)
{
    TestSinksOlapWriteToTopicAndTable1();
}

Y_UNIT_TEST_F(Sinks_Olap_WriteToTopicAndTable_1_Query, TFixtureSinksQuery)
{
    TestSinksOlapWriteToTopicAndTable1();
}

void TFixtureSinks::TestSinksOlapWriteToTopicAndTable2()
{
    return; // https://github.com/ydb-platform/ydb/issues/17271
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    CreateRowTable("/Root/table_A");
    CreateColumnTable("/Root/table_B");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();

    UpsertToTable("table_A", records, *session, tx.get());
    UpsertToTable("table_B", records, *session, tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());

    const size_t topicMsgCnt = 10;
    for (size_t i = 1; i <= topicMsgCnt; ++i) {
        WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #" + std::to_string(i), tx.get());
    }

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), MakeJsonDoc(records));
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, topicMsgCnt);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages.back(), "message #" + std::to_string(topicMsgCnt));
    }

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), records.size());
    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_B"), records.size());

    CheckTabletKeys("topic_A");
    CheckTabletKeys("topic_B");
}

Y_UNIT_TEST_F(Sinks_Olap_WriteToTopicAndTable_2_Table, TFixtureSinksTable)
{
    TestSinksOlapWriteToTopicAndTable2();
}

Y_UNIT_TEST_F(Sinks_Olap_WriteToTopicAndTable_2_Query, TFixtureSinksQuery)
{
    TestSinksOlapWriteToTopicAndTable2();
}

void TFixtureSinks::TestSinksOlapWriteToTopicAndTable3()
{
    CreateTopic("topic_A");
    CreateColumnTable("/Root/table_A");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();
    UpsertToTable("table_A", records, *session, tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());
    WaitForAcks("topic_A", TEST_MESSAGE_GROUP_ID);

    session->RollbackTx(*tx, EStatus::SUCCESS);

    Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 0);

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), 0);

    CheckTabletKeys("topic_A");
}

Y_UNIT_TEST_F(Sinks_Olap_WriteToTopicAndTable_3_Table, TFixtureSinksTable)
{
    TestSinksOlapWriteToTopicAndTable3();
}

Y_UNIT_TEST_F(Sinks_Olap_WriteToTopicAndTable_3_Query, TFixtureSinksQuery)
{
    TestSinksOlapWriteToTopicAndTable3();
}

void TFixtureSinks::TestSinksOlapWriteToTopicAndTable4()
{
    return; // https://github.com/ydb-platform/ydb/issues/17271
    CreateTopic("topic_A");
    CreateTopic("topic_B");

    CreateRowTable("/Root/table_A");
    CreateColumnTable("/Root/table_B");
    CreateColumnTable("/Root/table_C");

    auto session = CreateSession();
    auto tx = session->BeginTx();

    auto records = MakeTableRecords();

    InsertToTable("table_A", records, *session, tx.get());
    InsertToTable("table_B", records, *session, tx.get());
    UpsertToTable("table_C", records, *session, tx.get());

    WriteToTopic("topic_A", TEST_MESSAGE_GROUP_ID, MakeJsonDoc(records), tx.get());

    const size_t topicMsgCnt = 10;
    for (size_t i = 1; i <= topicMsgCnt; ++i) {
        WriteToTopic("topic_B", TEST_MESSAGE_GROUP_ID, "message #" + std::to_string(i), tx.get());
    }

    DeleteFromTable("table_B", records, *session, tx.get());

    session->CommitTx(*tx, EStatus::SUCCESS);

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_A", TEST_CONSUMER, 1);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), MakeJsonDoc(records));
    }

    {
        auto messages = Read_Exactly_N_Messages_From_Topic("topic_B", TEST_CONSUMER, topicMsgCnt);
        UNIT_ASSERT_VALUES_EQUAL(messages.front(), "message #1");
        UNIT_ASSERT_VALUES_EQUAL(messages.back(), "message #" + std::to_string(topicMsgCnt));
    }

    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_A"), records.size());
    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_B"), 0);
    UNIT_ASSERT_VALUES_EQUAL(GetTableRecordsCount("table_C"), records.size());

    CheckTabletKeys("topic_A");
    CheckTabletKeys("topic_B");
}

Y_UNIT_TEST_F(Sinks_Olap_WriteToTopicAndTable_4_Table, TFixtureSinksTable)
{
    TestSinksOlapWriteToTopicAndTable4();
}

Y_UNIT_TEST_F(Sinks_Olap_WriteToTopicAndTable_4_Query, TFixtureSinksQuery)
{
    TestSinksOlapWriteToTopicAndTable4();
}

void TFixture::TestWriteRandomSizedMessagesInWideTransactions()
{
    // The test verifies the simultaneous execution of several transactions. There is a topic
    // with PARTITIONS_COUNT partitions. In each transaction, the test writes to all the partitions.
    // The size of the messages is random. Such that both large blobs in the body and small ones in
    // the head of the partition are obtained. Message sizes are multiples of 500 KB. This way we
    // will make sure that when committing transactions, the division into blocks is taken into account.

    const size_t PARTITIONS_COUNT = 20;
    const size_t TXS_COUNT = 10;

    CreateTopic("topic_A", TEST_CONSUMER, PARTITIONS_COUNT);

    SetPartitionWriteSpeed("topic_A", 50'000'000);

    std::vector<std::unique_ptr<TFixture::ISession>> sessions;
    std::vector<std::unique_ptr<TTransactionBase>> transactions;

    // We open TXS_COUNT transactions and write messages to the topic.
    for (size_t i = 0; i < TXS_COUNT; ++i) {
        sessions.push_back(CreateSession());
        auto& session = sessions.back();

        transactions.push_back(session->BeginTx());
        auto& tx = transactions.back();

        for (size_t j = 0; j < PARTITIONS_COUNT; ++j) {
            TString sourceId = TEST_MESSAGE_GROUP_ID;
            sourceId += "_";
            sourceId += ToString(i);
            sourceId += "_";
            sourceId += ToString(j);

            size_t count = RandomNumber<size_t>(20) + 3;
            WriteToTopic("topic_A", sourceId, TString(512 * 1000 * count, 'x'), tx.get(), j);

            WaitForAcks("topic_A", sourceId);
        }
    }

    // We are doing an asynchronous commit of transactions. They will be executed simultaneously.
    std::vector<TAsyncStatus> futures;

    for (size_t i = 0; i < TXS_COUNT; ++i) {
        futures.push_back(sessions[i]->AsyncCommitTx(*transactions[i]));
    }

    // All transactions must be completed successfully.
    for (size_t i = 0; i < TXS_COUNT; ++i) {
        futures[i].Wait();
        const auto& result = futures[i].GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
}

Y_UNIT_TEST_F(Write_Random_Sized_Messages_In_Wide_Transactions_Table, TFixtureTable)
{
    TestWriteRandomSizedMessagesInWideTransactions();
}

Y_UNIT_TEST_F(Write_Random_Sized_Messages_In_Wide_Transactions_Query, TFixtureQuery)
{
    TestWriteRandomSizedMessagesInWideTransactions();
}

void TFixture::TestWriteOnlyBigMessagesInWideTransactions()
{
    // The test verifies the simultaneous execution of several transactions. There is a topic `topic_A` and
    // it contains a `PARTITIONS_COUNT' of partitions. In each transaction, the test writes to all partitions.
    // The size of the messages is chosen so that only large blobs are recorded in the transaction and there
    // are no records in the head. Thus, we verify that transaction bundling is working correctly.

    const size_t PARTITIONS_COUNT = 20;
    const size_t TXS_COUNT = 100;

    CreateTopic("topic_A", TEST_CONSUMER, PARTITIONS_COUNT);

    SetPartitionWriteSpeed("topic_A", 50'000'000);

    std::vector<std::unique_ptr<TFixture::ISession>> sessions;
    std::vector<std::unique_ptr<TTransactionBase>> transactions;

    // We open TXS_COUNT transactions and write messages to the topic.
    for (size_t i = 0; i < TXS_COUNT; ++i) {
        sessions.push_back(CreateSession());
        auto& session = sessions.back();

        transactions.push_back(session->BeginTx());
        auto& tx = transactions.back();

        for (size_t j = 0; j < PARTITIONS_COUNT; ++j) {
            TString sourceId = TEST_MESSAGE_GROUP_ID;
            sourceId += "_";
            sourceId += ToString(i);
            sourceId += "_";
            sourceId += ToString(j);

            WriteToTopic("topic_A", sourceId, TString(6'500'000, 'x'), tx.get(), j);

            WaitForAcks("topic_A", sourceId);
        }
    }

    // We are doing an asynchronous commit of transactions. They will be executed simultaneously.
    std::vector<TAsyncStatus> futures;

    for (size_t i = 0; i < TXS_COUNT; ++i) {
        futures.push_back(sessions[i]->AsyncCommitTx(*transactions[i]));
    }

    // All transactions must be completed successfully.
    for (size_t i = 0; i < TXS_COUNT; ++i) {
        futures[i].Wait();
        const auto& result = futures[i].GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
}

Y_UNIT_TEST_F(Write_Only_Big_Messages_In_Wide_Transactions_Table, TFixtureTable)
{
    TestWriteOnlyBigMessagesInWideTransactions();
}

Y_UNIT_TEST_F(Write_Only_Big_Messages_In_Wide_Transactions_Query, TFixtureQuery)
{
    TestWriteOnlyBigMessagesInWideTransactions();
}

void TFixture::TestTransactionsConflictOnSeqNo()
{
    const ui32 PARTITIONS_COUNT = 20;
    const size_t TXS_COUNT = 100;

    CreateTopic("topic_A", TEST_CONSUMER, PARTITIONS_COUNT);

    SetPartitionWriteSpeed("topic_A", 50'000'000);

    auto session = CreateSession();
    std::vector<std::shared_ptr<NTopic::ISimpleBlockingWriteSession>> topicWriteSessions;

    for (ui32 i = 0; i < PARTITIONS_COUNT; ++i) {
        TString sourceId = TEST_MESSAGE_GROUP_ID;
        sourceId += "_";
        sourceId += ToString(i);

        NTopic::TTopicClient client(GetDriver());
        NTopic::TWriteSessionSettings options;
        options.Path("topic_A");
        options.ProducerId(sourceId);
        options.MessageGroupId(sourceId);
        options.PartitionId(i);
        options.Codec(ECodec::RAW);

        auto session = client.CreateSimpleBlockingWriteSession(options);

        topicWriteSessions.push_back(std::move(session));
    }

    std::vector<std::unique_ptr<TFixture::ISession>> sessions;
    std::vector<std::unique_ptr<TTransactionBase>> transactions;

    for (size_t i = 0; i < TXS_COUNT; ++i) {
        sessions.push_back(CreateSession());
        auto& session = sessions.back();

        transactions.push_back(session->BeginTx());
        auto& tx = transactions.back();

        for (size_t j = 0; j < PARTITIONS_COUNT; ++j) {
            TString sourceId = TEST_MESSAGE_GROUP_ID;
            sourceId += "_";
            sourceId += ToString(j);

            for (size_t k = 0, count = RandomNumber<size_t>(20) + 1; k < count; ++k) {
                const std::string data(RandomNumber<size_t>(1'000) + 100, 'x');
                NTopic::TWriteMessage params(data);
                params.Tx(*tx);

                topicWriteSessions[j]->Write(std::move(params));
            }
        }
    }

    std::vector<TAsyncStatus> futures;

    for (size_t i = 0; i < TXS_COUNT; ++i) {
        futures.push_back(sessions[i]->AsyncCommitTx(*transactions[i]));
    }

    // Some transactions should end with the error `ABORTED`
    size_t successCount = 0;

    for (size_t i = 0; i < TXS_COUNT; ++i) {
        futures[i].Wait();
        const auto& result = futures[i].GetValueSync();
        switch (result.GetStatus()) {
        case EStatus::SUCCESS:
            ++successCount;
            break;
        case EStatus::ABORTED:
            break;
        default:
            UNIT_FAIL("unexpected status: " << static_cast<const NYdb::TStatus&>(result));
            break;
        }
    }

    UNIT_ASSERT_VALUES_UNEQUAL(successCount, TXS_COUNT);
}

Y_UNIT_TEST_F(Transactions_Conflict_On_SeqNo_Table, TFixtureTable)
{
    TestTransactionsConflictOnSeqNo();
}

Y_UNIT_TEST_F(Transactions_Conflict_On_SeqNo_Query, TFixtureQuery)
{
    TestTransactionsConflictOnSeqNo();
}

Y_UNIT_TEST_F(The_Transaction_Starts_On_One_Version_And_Ends_On_The_Other, TFixtureNoClient)
{
    // In the test, we check the compatibility between versions `24-4-2` and `24-4-*/25-1-*`. To do this, the data
    // obtained on the `24-4-2` version is loaded into the PQ tablets.

    CreateTopic("topic_A", TEST_CONSUMER, 2);

    PQTabletPrepareFromResource("topic_A", 0, "topic_A_partition_0_v24-4-2.dat");
    PQTabletPrepareFromResource("topic_A", 1, "topic_A_partition_1_v24-4-2.dat");

    RestartPQTablet("topic_A", 0);
    RestartPQTablet("topic_A", 1);
}

}

}
