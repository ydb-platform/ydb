#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <ydb/core/persqueue/public/write_id.h>
#include <ydb/core/protos/long_tx_service.pb.h>
#include <ydb/library/actors/core/actorid.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NTopic::NTests::NTxUsage {

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

    struct TReadMessageSettings {
        TTransactionBase& Tx;
        bool CommitOffsets = false;
        std::optional<std::uint64_t> Offset;
    };

    void CreateTopic(const std::string& path = TEST_TOPIC,
                     const std::string& consumer = TEST_CONSUMER,
                     std::size_t partitionCount = 1,
                     std::optional<size_t> maxPartitionCount = std::nullopt,
                     const TDuration retention = TDuration::Hours(1),
                     bool important = false);

    void AddConsumer(const std::string& topicPath, const std::vector<std::string>& consumers);

    void SetPartitionWriteSpeed(const std::string& topicName, std::size_t bytesPerSeconds);

    TTopicWriteSessionPtr CreateTopicWriteSession(const std::string& topicPath,
                                                  const std::string& messageGroupId,
                                                  std::optional<std::uint32_t> partitionId);
    TTopicWriteSessionContext& GetTopicWriteSession(const std::string& topicPath,
                                                    const std::string& messageGroupId,
                                                    std::optional<std::uint32_t> partitionId);

    TTopicReadSessionPtr CreateTopicReadSession(const std::string& topicPath,
                                                const std::string& consumerName,
                                                std::optional<std::uint32_t> partitionId);
    TTopicReadSessionPtr GetTopicReadSession(const std::string& topicPath,
                                             const std::string& consumerName,
                                             std::optional<std::uint32_t> partitionId);

    void WriteToTopic(const std::string& topicPath,
                      const std::string& messageGroupId,
                      const std::string& message,
                      TTransactionBase* tx = nullptr,
                      std::optional<std::uint32_t> partitionId = std::nullopt);
    std::vector<std::string> ReadFromTopic(const std::string& topicPath,
                                   const std::string& consumerName,
                                   const TDuration& duration,
                                   TTransactionBase* tx = nullptr,
                                   std::optional<std::uint32_t> partitionId = std::nullopt);
    void WaitForAcks(const std::string& topicPath,
                     const std::string& messageGroupId,
                     std::size_t writtenInTxCount = std::numeric_limits<std::size_t>::max());
    void WaitForSessionClose(const std::string& topicPath,
                             const std::string& messageGroupId,
                             NYdb::EStatus status);
    void CloseTopicWriteSession(const std::string& topicPath,
                                const std::string& messageGroupId,
                                bool force = false);
    void CloseTopicReadSession(const std::string& topicPath,
                               const std::string& consumerName);

    enum EEndOfTransaction {
        Commit,
        Rollback,
        CloseTableSession
    };

    struct TTransactionCompletionTestDescription {
        std::vector<std::string> Topics;
        EEndOfTransaction EndOfTransaction = Commit;
    };

    void TestTheCompletionOfATransaction(const TTransactionCompletionTestDescription& d);
    void RestartPQTablet(const std::string& topicPath, std::uint32_t partition);
    void DumpPQTabletKeys(const std::string& topicName, std::uint32_t partition);
    void PQTabletPrepareFromResource(const std::string& topicPath,
                                     std::uint32_t partitionId,
                                     const std::string& resourceName);

    void DeleteSupportivePartition(const std::string& topicName,
                                   std::uint32_t partition);

    struct TTableRecord {
        TTableRecord() = default;
        TTableRecord(const std::string& key, const std::string& value);

        std::string Key;
        std::string Value;
    };

    std::vector<TTableRecord> MakeTableRecords();
    std::string MakeJsonDoc(const std::vector<TTableRecord>& records);

    void CreateTable(const std::string& path);
    void UpsertToTable(const std::string& tablePath,
                      const std::vector<TTableRecord>& records,
                      ISession& session,
                      TTransactionBase* tx);
    void InsertToTable(const std::string& tablePath,
                      const std::vector<TTableRecord>& records,
                      ISession& session,
                      TTransactionBase* tx);
    void DeleteFromTable(const std::string& tablePath,
                      const std::vector<TTableRecord>& records,
                      ISession& session,
                      TTransactionBase* tx);
    size_t GetTableRecordsCount(const std::string& tablePath);

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

    void WriteMessagesInTx(std::size_t big, size_t small);

    const TDriver& GetDriver() const;
    NTable::TTableClient& GetTableClient();

    void CheckTabletKeys(const std::string& topicName);
    void DumpPQTabletKeys(const std::string& topicName);

    std::vector<std::string> Read_Exactly_N_Messages_From_Topic(const std::string& topicPath,
                                                        const std::string& consumerName,
                                                        size_t count);

    void TestWriteRandomSizedMessagesInWideTransactions();

    void TestWriteOnlyBigMessagesInWideTransactions();

    void TestTransactionsConflictOnSeqNo();

    void TestWriteToTopic1();

    void TestWriteToTopic4();

    void TestWriteToTopic7();

    void TestWriteToTopic9();

    void TestWriteToTopic10();

    void TestWriteToTopic11();

    void TestWriteToTopic12();

    void TestWriteToTopic13();

    void TestWriteToTopic14();

    void TestWriteToTopic16();

    void TestWriteToTopic24();

    void TestWriteToTopic26();

    void TestWriteToTopic27();

    void TestWriteToTopic38();

    void TestWriteToTopic40();

    void TestWriteToTopic41();

    void TestWriteToTopic42();

    void TestWriteToTopic43();

    void TestWriteToTopic44();

    void TestWriteToTopic45();

    void TestWriteToTopic46();

    void TestWriteToTopic47();

    void TestWriteToTopic50();

    struct TAvgWriteBytes {
        std::uint64_t PerSec = 0;
        std::uint64_t PerMin = 0;
        std::uint64_t PerHour = 0;
        std::uint64_t PerDay = 0;
    };

    TAvgWriteBytes GetAvgWriteBytes(const std::string& topicPath,
                                    std::uint32_t partitionId);

    void CheckAvgWriteBytes(const std::string& topicPath,
                            std::uint32_t partitionId,
                            std::size_t minSize, std::size_t maxSize);

    void SplitPartition(const std::string& topicPath,
                        std::uint32_t partitionId,
                        const std::string& boundary);

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

    void TestWriteAndReadMessages(size_t count, size_t size, bool restart);

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

        TAsyncStatus AsyncCommitTx(TTransactionBase& tx) override;

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

        TAsyncStatus AsyncCommitTx(TTransactionBase& tx) override;

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

    std::uint64_t GetTopicTabletId(const NActors::TActorId& actorId,
                                   const std::string& topicPath,
                                   std::uint32_t partition);
    std::vector<std::string> GetTabletKeys(const NActors::TActorId& actorId,
                                           std::uint64_t tabletId);
    NKikimr::NPQ::TWriteId GetTransactionWriteId(const NActors::TActorId& actorId,
                                                 std::uint64_t tabletId);
    void SendLongTxLockStatus(const NActors::TActorId& actorId,
                              std::uint64_t tabletId,
                              const NKikimr::NPQ::TWriteId& writeId,
                              NKikimrLongTxService::TEvLockStatus::EStatus status);
    void WaitForTheTabletToDeleteTheWriteInfo(const NActors::TActorId& actorId,
                                              std::uint64_t tabletId,
                                              const NKikimr::NPQ::TWriteId& writeId);

    std::uint64_t GetSchemeShardTabletId(const NActors::TActorId& actorId);

    std::unique_ptr<TTopicSdkTestSetup> Setup;
    std::unique_ptr<TDriver> Driver;
    std::unique_ptr<NTable::TTableClient> TableClient;
    std::unique_ptr<NQuery::TQueryClient> QueryClient;

    std::unordered_map<std::pair<std::string, std::string>, TTopicWriteSessionContext> TopicWriteSessions;
    std::unordered_map<std::string, TTopicReadSessionPtr> TopicReadSessions;

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

class TFixtureSinks : public TFixture {
protected:
    void CreateRowTable(const std::string& path);
    void CreateColumnTable(const std::string& tablePath);

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

}
