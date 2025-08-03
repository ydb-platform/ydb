#pragma once

#include <util/string/join.h>
#include <util/system/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>

#include <library/cpp/threading/local_executor/local_executor.h>

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NYdb::NTopic;
using namespace NYdb::NReplication;

namespace NUnitTest::NPrivate {
    template<>
    inline bool CompareEqual<TUuidValue>(const TUuidValue& x, const TUuidValue& y) {
        return x.ToString() == y.ToString();
    }
}

template<>
inline void Out<NYdb::Dev::TUuidValue>(IOutputStream& os, const NYdb::Dev::TUuidValue& value) {
    os << value.ToString();
}

namespace NReplicationTest {

struct IChecker {
    virtual void Assert(const std::string& msg, const ::Ydb::Value& value) = 0;
    virtual ~IChecker() = default;
};


template<typename T>
struct Checker : public IChecker {
    Checker(T&& expected)
        : Expected(std::move(expected))
    {}

    void Assert(const std::string& msg, const ::Ydb::Value& value) override {
        UNIT_ASSERT_VALUES_EQUAL_C(Get(value), Expected, msg);
    }

    virtual T Get(const ::Ydb::Value& value);

    T Expected;
};

struct DateTimeChecker : public Checker<TInstant> {
    DateTimeChecker(TInstant&& expected)
        : Checker<TInstant>(std::move(expected)) {
    }

    TInstant Get(const ::Ydb::Value& value) override {
        return TInstant::Seconds(value.uint32_value());
    }
};

template<>
inline bool Checker<bool>::Get(const ::Ydb::Value& value) {
    return value.bool_value();
}

template<>
inline i8 Checker<i8>::Get(const ::Ydb::Value& value) {
    return value.int32_value();
}

template<>
inline i16 Checker<i16>::Get(const ::Ydb::Value& value) {
    return value.int32_value();
}

template<>
inline i32 Checker<i32>::Get(const ::Ydb::Value& value) {
    return value.int32_value();
}

template<>
inline i64 Checker<i64>::Get(const ::Ydb::Value& value) {
    return value.int64_value();
}

template<>
inline ui32 Checker<ui32>::Get(const ::Ydb::Value& value) {
    return value.uint32_value();
}

template<>
inline ui64 Checker<ui64>::Get(const ::Ydb::Value& value) {
    return value.uint64_value();
}

template<>
inline double Checker<double>::Get(const ::Ydb::Value& value) {
    return value.double_value();
}

template<>
inline TString Checker<TString>::Get(const ::Ydb::Value& value) {
    return value.text_value();
}

template<>
inline TInstant Checker<TInstant>::Get(const ::Ydb::Value& value) {
    return TInstant::Days(value.uint32_value());
}

template<>
inline TUuidValue Checker<TUuidValue>::Get(const ::Ydb::Value& value) {
    return TUuidValue(value);
}

template<typename T>
std::pair<TString, std::shared_ptr<IChecker>> _C(std::string&& name, T&& expected) {
    return {
        std::move(name),
        std::make_shared<Checker<T>>(std::move(expected))
    };
}

template<typename C, typename T>
std::pair<TString, std::shared_ptr<IChecker>> _T(std::string&& name, T&& expected) {
    return {
        std::move(name),
        std::make_shared<C>(std::move(expected))
    };
}

struct TMessage {
    TString Message;
    std::optional<ui32> Partition = std::nullopt;
    std::optional<std::string> ProducerId = std::nullopt;
    std::optional<std::string> MessageGroupId = std::nullopt;
    std::optional<ui64> SeqNo = std::nullopt;
};

inline TMessage _withSeqNo(ui64 seqNo) {
    return {
        .Message = TStringBuilder() << "Message-" << seqNo,
        .Partition = 0,
        .ProducerId = std::nullopt,
        .MessageGroupId = std::nullopt,
        .SeqNo = seqNo
    };
}

inline TMessage _withProducerId(const std::string& producerId) {
    return {
        .Message = TStringBuilder() << "Message-" << producerId,
        .Partition = 0,
        .ProducerId = producerId,
        .MessageGroupId = std::nullopt,
        .SeqNo = std::nullopt
    };
}

inline TMessage _withMessageGroupId(const std::string& messageGroupId) {
    return {
        .Message = TStringBuilder() << "Message-" << messageGroupId,
        .Partition = 0,
        .ProducerId = messageGroupId,
        .MessageGroupId = messageGroupId,
        .SeqNo = std::nullopt
    };
}

using TExpectations = TVector<TVector<std::pair<TString, std::shared_ptr<IChecker>>>>;

struct TConfig {
    const TString TableDDL;
    const TString Lambda;
    const TVector<TMessage> Messages;
    const TExpectations Expectations;
    const TVector<TString> AlterLambdas;
};

struct MainTestCase {

    static auto CreateDriverConfig(std::string connectionString, std::optional<std::string> user) {
        auto config = TDriverConfig(connectionString);
        if (user) {
            config.SetAuthToken(TStringBuilder() << user.value() << "@builtin");
        }
        // config.SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG).Release()));
        return config;
    }

    MainTestCase(const std::optional<std::string> user = std::nullopt, std::string tableType = "COLUMN")
        : TableType(std::move(tableType))
        , Id(RandomNumber<size_t>())
        , ConnectionString(GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE"))
        , TopicName(TStringBuilder() << "Topic_" << Id)
        , SourceTableName(TStringBuilder() << "SourceTable_" << Id)
        , ChangefeedName(TStringBuilder() << "cdc_" << Id)
        , TableName(TStringBuilder() << "Table_" << Id)
        , ReplicationName(TStringBuilder() << "Replication_" << Id)
        , TransferName(TStringBuilder() << "Transfer_" << Id)
        , Driver(CreateDriverConfig(ConnectionString, user))
        , TableClient(Driver)
        , TopicClient(Driver)
    {
    }

    ~MainTestCase() {
        Driver.Stop(true);
    }

    void CreateDirectory(const std::string& directoryName) {
        NScheme::TSchemeClient schemeClient(Driver);
        auto result = schemeClient.MakeDirectory(directoryName).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
    }

    auto Session() {
        return TableClient.GetSession().GetValueSync().GetSession();
    }

    void ExecuteDDL(const std::string& ddl, bool checkResult = true, const std::optional<std::string> expectedMessage = std::nullopt) {
        Cerr << "DDL: " << ddl << Endl << Flush;
        auto res = Session().ExecuteQuery(ddl, TTxControl::NoTx()).GetValueSync();
        if (checkResult) {
            if (expectedMessage) {
                UNIT_ASSERT(!res.IsSuccess());
                Cerr << ">>>>> ACTUAL: " << res.GetIssues().ToOneLineString() << Endl << Flush;
                Cerr << ">>>>> EXPECTED: " << expectedMessage << Endl << Flush;
                UNIT_ASSERT(res.GetIssues().ToOneLineString().contains(expectedMessage.value()));
            } else {
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }
        }
    }

    auto ExecuteQuery(const std::string& query, bool retry = true) {
        for (size_t i = 10; i--;) {
            Cerr << ">>>>> Query: " << query << Endl << Flush;
            auto res = Session().ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
            if (!res.IsSuccess()) {
                Cerr << ">>>>> Query error: " << res.GetIssues().ToString() << Endl << Flush;
            }
            if (res.IsSuccess() || !retry) {
                return res;
            }

            UNIT_ASSERT_C(i, res.GetIssues().ToString());
            Sleep(TDuration::Seconds(1));
        }

        Y_UNREACHABLE();
    }

    auto ExecuteTableQuery(const std::string& query) {
        return ExecuteQuery(Sprintf(query.data(), TableName.data()));
    }

    auto ExecuteSourceTableQuery(const std::string& query) {
        return ExecuteQuery(Sprintf(query.data(), SourceTableName.data()));
    }

    void Grant(const std::string& object, const std::string& username, const std::vector<std::string>& permissions) {
        TStringBuilder sql;
        sql << "GRANT ";
        for (size_t i = 0; i < permissions.size(); ++i) {
            if (i) {
                sql << ", ";
            }
            if ("ALL" == permissions[i]) {
                sql << "ALL";
            } else {
                sql << "'" << permissions[i] << "'";
            }
        }
        sql << " ON `/local";
        if (!object.empty()) {
            sql << "/" << object;
        }
        sql << "` TO `" << username << "@builtin`";

        ExecuteDDL(sql);
    }

    void CreateTable(const std::string& tableDDL) {
        ExecuteDDL(Sprintf(tableDDL.data(), TableName.data(), TableType.data()));
    }

    void DropTable() {
        ExecuteDDL(Sprintf("DROP TABLE `%s`", TableName.data()));
    }

    void CreateSourceTable(const std::string& tableDDL) {
        ExecuteDDL(Sprintf(tableDDL.data(), SourceTableName.data()));
    }

    void DropSourceTable() {
        ExecuteDDL(Sprintf("DROP TABLE `%s`", SourceTableName.data()));
    }

    void AddChangefeed() {
        ExecuteDDL(Sprintf(R"(
            ALTER TABLE `%s`
            ADD CHANGEFEED `%s` WITH (
                MODE = 'UPDATES',
                FORMAT = 'JSON'
            )
        )", SourceTableName.data(), ChangefeedName.data()));
    }

    struct CreatTopicSettings {
        size_t MinPartitionCount = 1;
        size_t MaxPartitionCount = 100;
        bool AutoPartitioningEnabled = false;
    };

    void CreateTopic(size_t partitionCount = 10) {
        CreateTopic({
            .MinPartitionCount = partitionCount
        });
    }

    void CreateTopic(const CreatTopicSettings& settings) {
        if (settings.AutoPartitioningEnabled) {
            ExecuteDDL(Sprintf(R"(
                CREATE TOPIC `%s`
                WITH (
                    MIN_ACTIVE_PARTITIONS = %d,
                    MAX_ACTIVE_PARTITIONS = %d,
                    AUTO_PARTITIONING_STRATEGY = 'UP',
                    auto_partitioning_down_utilization_percent = 1,
                    auto_partitioning_up_utilization_percent=2,
                    auto_partitioning_stabilization_window = Interval('PT1S'),
                    partition_write_speed_bytes_per_second = 3
                );
            )", TopicName.data(), settings.MinPartitionCount, settings.MaxPartitionCount));
        } else {
            ExecuteDDL(Sprintf(R"(
                CREATE TOPIC `%s`
                WITH (
                    MIN_ACTIVE_PARTITIONS = %d

                );
            )", TopicName.data(), settings.MinPartitionCount));
        }
    }

    void DropTopic() {
        ExecuteDDL(Sprintf("DROP TOPIC `%s`", TopicName.data()));
    }

    void CreateConsumer(const std::string& consumerName) {
        ExecuteDDL(Sprintf(R"(
            ALTER TOPIC `%s`
            ADD CONSUMER `%s`;
        )", TopicName.data(), consumerName.data()));
    }

    void DropConsumer(const std::string& consumerName) {
        ExecuteDDL(Sprintf(R"(
            ALTER TOPIC `%s`
            DROP CONSUMER `%s`;
        )", TopicName.data(), consumerName.data()));
    }

    struct CreateTransferSettings {
        std::optional<std::string> TopicName;
        bool LocalTopic = false;
        std::optional<std::string> ConsumerName;
        std::optional<TDuration> FlushInterval = TDuration::Seconds(1);
        std::optional<ui64> BatchSizeBytes = 8_MB;
        std::optional<std::string> ExpectedError;
        std::optional<std::string> Username;
        std::optional<std::string> Directory;

        CreateTransferSettings() {};

        static CreateTransferSettings WithLocalTopic(bool local) {
            CreateTransferSettings result;
            result.LocalTopic = local;
            return result;
        }

        static CreateTransferSettings WithTopic(const std::string& topicName, std::optional<TString> consumerName = std::nullopt) {
            CreateTransferSettings result;
            result.TopicName = topicName;
            result.ConsumerName = consumerName;
            return result;
        }

        static CreateTransferSettings WithConsumerName(const std::string& consumerName) {
            CreateTransferSettings result;
            result.ConsumerName = consumerName;
            return result;
        }

        static CreateTransferSettings WithBatching(const TDuration& flushInterval, const ui64 batchSize) {
            CreateTransferSettings result;
            result.FlushInterval = flushInterval;
            result.BatchSizeBytes = batchSize;
            return result;
        }

        static CreateTransferSettings WithExpectedError(const std::string& expected) {
            CreateTransferSettings result;
            result.ExpectedError = expected;
            return result;
        }

        static CreateTransferSettings WithUsername(const TString& username) {
            CreateTransferSettings result;
            result.Username = username;
            return result;
        }

        static CreateTransferSettings WithDirectory(const TString& directory) {
            CreateTransferSettings result;
            result.Directory = directory;
            return result;
        }
    };

    void CreateTransfer(const std::string& lambda, const CreateTransferSettings& settings = CreateTransferSettings()) {
        std::vector<std::string> options;
        if (!settings.LocalTopic) {
            options.push_back(TStringBuilder() << "CONNECTION_STRING = 'grpc://" << ConnectionString << "'");
        }
        if (settings.ConsumerName) {
            options.push_back(TStringBuilder() <<  "CONSUMER = '" << *settings.ConsumerName << "'");
        }
        if (settings.FlushInterval) {
            options.push_back(TStringBuilder() <<  "FLUSH_INTERVAL = Interval('PT" << settings.FlushInterval->Seconds() << "S')");
        }
        if (settings.BatchSizeBytes) {
            options.push_back(TStringBuilder() <<  "BATCH_SIZE_BYTES = " << *settings.BatchSizeBytes);
        }
        if (settings.Username) {
            options.push_back(TStringBuilder() <<  "TOKEN = '" << *settings.Username << "@builtin'");
        }
        if (settings.Directory) {
            options.push_back(TStringBuilder() <<  "DIRECTORY = '" << *settings.Directory << "'");
        }

        std::string topicName = settings.TopicName.value_or(TopicName);
        std::string optionsStr = JoinRange(",\n", options.begin(), options.end());

        auto ddl = Sprintf(R"(
            %s;

            CREATE TRANSFER `%s`
            FROM `%s` TO `%s` USING $l
            WITH (
                %s
            );
        )", lambda.data(), TransferName.data(), topicName.data(), TableName.data(), optionsStr.data());

        ExecuteDDL(ddl, true, settings.ExpectedError);
    }

    struct AlterTransferSettings {
        std::optional<std::string> TransformLambda;
        std::optional<TDuration> FlushInterval;
        std::optional<ui64> BatchSizeBytes;
        std::optional<std::string> Directory;

        AlterTransferSettings()
            : FlushInterval(std::nullopt)
            , BatchSizeBytes(std::nullopt) {}

        static AlterTransferSettings WithBatching(const TDuration& flushInterval, const ui64 batchSize) {
            AlterTransferSettings result;
            result.FlushInterval = flushInterval;
            result.BatchSizeBytes = batchSize;
            return result;
        }

        static AlterTransferSettings WithTransformLambda(const std::string& lambda) {
            AlterTransferSettings result;
            result.TransformLambda = lambda;
            return result;
        }

        static AlterTransferSettings WithDirectory(const std::string& directory) {
            AlterTransferSettings result;
            result.Directory = directory;
            return result;
        }
    };

    void AlterTransfer(const std::string& lambda) {
        AlterTransfer(AlterTransferSettings::WithTransformLambda(lambda));
    }

    void AlterTransfer(const AlterTransferSettings& settings, bool success = true) {
        std::string lambda = settings.TransformLambda ? *settings.TransformLambda : "";
        std::string setLambda = settings.TransformLambda ? "SET USING $l" : "";

        std::vector<std::string> options;
        if (settings.FlushInterval) {
            options.push_back(TStringBuilder() << "FLUSH_INTERVAL = Interval('PT" << settings.FlushInterval->Seconds() << "S')");
        }
        if (settings.BatchSizeBytes) {
            options.push_back(TStringBuilder() << "BATCH_SIZE_BYTES = " << *settings.BatchSizeBytes);
        }

        if (settings.Directory) {
            options.push_back(TStringBuilder() << "DIRECTORY = \"" << *settings.Directory << "\"");
        }

        std::string setOptions;
        if (!options.empty()) {
            setOptions = TStringBuilder() << "SET (" << JoinRange(",\n", options.begin(), options.end()) << " )";
        }

        ExecuteDDL(Sprintf(R"(
            %s;

            ALTER TRANSFER `%s`
            %s
            %s;
        )", lambda.data(), TransferName.data(), setLambda.data(), setOptions.data()), success);
    }

    void DropTransfer() {
        ExecuteDDL(Sprintf("DROP TRANSFER `%s`;", TransferName.data()));
    }

    void PauseTransfer() {
        ExecuteDDL(Sprintf(R"(
            ALTER TRANSFER `%s`
            SET (
                STATE = "Paused"
            );
        )", TransferName.data()));
    }

    void ResumeTransfer() {
        ExecuteDDL(Sprintf(R"(
            ALTER TRANSFER `%s`
            SET (
                STATE = "StandBy"
            );
        )", TransferName.data()));
    }

    auto DescribeTransfer() {
        TReplicationClient client(Driver);

        return client.DescribeTransfer(TString("/") + GetEnv("YDB_DATABASE") + "/" + TransferName).ExtractValueSync();
    }

    auto DescribeConsumer(const std::string& consumerName) {
        TDescribeConsumerSettings settings;
        settings.IncludeLocation(true);
        settings.IncludeStats(true);

        auto c = TopicClient.DescribeConsumer(TopicName, consumerName, settings).GetValueSync();
        UNIT_ASSERT_C(c.IsSuccess(), c.GetIssues().ToOneLineString());
        return c;
    }

    auto DescribeConsumer() {
        auto topic = DescribeTopic();
        auto consumers = topic.GetTopicDescription().GetConsumers();
        UNIT_ASSERT_VALUES_EQUAL(1, consumers.size());
        return DescribeConsumer(consumers[0].GetConsumerName());
    }

    void CheckCommittedOffset(size_t partitionId, size_t expectedOffset) {
        auto d = DescribeConsumer();
        UNIT_ASSERT(d.IsSuccess());
        auto s = d.GetConsumerDescription().GetPartitions().at(partitionId).GetPartitionConsumerStats();
        UNIT_ASSERT_VALUES_EQUAL(expectedOffset, s->GetCommittedOffset());
    }

    void CreateReplication() {
        auto ddl = Sprintf(R"(
            CREATE ASYNC REPLICATION `%s`
            FOR `%s` AS `%s`
            WITH (
                CONNECTION_STRING = 'grpc://%s'
            );
        )", ReplicationName.data(), SourceTableName.data(), TableName.data(), ConnectionString.data());

        ExecuteDDL(ddl);
    }

    void DropReplication() {
        ExecuteDDL(Sprintf("DROP ASYNC REPLICATION `%s`;", ReplicationName.data()));
    }

    auto DescribeReplication() {
        TReplicationClient client(Driver);

        TDescribeReplicationSettings settings;
        settings.IncludeStats(true);

        return client.DescribeReplication(TString("/") + GetEnv("YDB_DATABASE") + "/" + ReplicationName, settings).ExtractValueSync();
    }

    TReplicationDescription CheckReplicationState(TReplicationDescription::EState expected) {
        for (size_t i = 20; i--;) {
            auto result = DescribeReplication().GetReplicationDescription();
            if (expected == result.GetState()) {
                return result;
            }
    
            UNIT_ASSERT_C(i, "Unable to wait replication state. Expected: " << expected << ", actual: " << result.GetState());
            Sleep(TDuration::Seconds(1));
        }

        Y_UNREACHABLE();
    }

    void PauseReplication() {
        ExecuteDDL(Sprintf(R"(
            ALTER ASYNC REPLICATION `%s`
            SET (
                STATE = "Paused"
            );
        )", ReplicationName.data()));
    }

    void ResumeReplication() {
        ExecuteDDL(Sprintf(R"(
            ALTER ASYNC REPLICATION `%s`
            SET (
                STATE = "StandBy"
            );
        )", ReplicationName.data()));
    }

    TDescribeTopicResult DescribeTopic() {
        TDescribeTopicSettings settings;
        settings.IncludeLocation(true);
        settings.IncludeStats(true);

        auto result = TopicClient.DescribeTopic(TopicName, settings).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        return result;
    }

    void CreateUser(const std::string& username) {
        ExecuteDDL(Sprintf(R"(
            CREATE USER %s
        )", username.data()));
    }

    void Write(const TMessage& message) {
        TWriteSessionSettings writeSettings;
        writeSettings.Path(TopicName);
        writeSettings.DeduplicationEnabled(message.SeqNo);
        if (message.Partition) {
            writeSettings.PartitionId(message.Partition);
        }
        if (message.ProducerId) {
            writeSettings.ProducerId(*message.ProducerId);
        }
        if (message.MessageGroupId) {
            writeSettings.MessageGroupId(*message.MessageGroupId);
        }
        auto writeSession = TopicClient.CreateSimpleBlockingWriteSession(writeSettings);

        UNIT_ASSERT(writeSession->Write(message.Message, message.SeqNo));
        writeSession->Close(TDuration::Seconds(1));
    }

    std::pair<i64, Ydb::ResultSet> DoRead(const TExpectations& expectations) {
        return DoRead(TableName, expectations);
    }

    std::pair<i64, Ydb::ResultSet> DoRead(const std::string& tableName, const TExpectations& expectations) {
        auto& e = expectations.front();

        TStringBuilder columns;
        for (size_t i = 0; i < e.size(); ++i) {
            if (i) {
                columns << ", ";
            }
            columns << "`" << e[i].first << "`";
        }


        auto res = ExecuteQuery(Sprintf("SELECT %s FROM `%s` ORDER BY %s", columns.data(), tableName.data(), columns.data()), false);
        if (!res.IsSuccess()) {
            TResultSet r{Ydb::ResultSet()};
            return {-1, NYdb::TProtoAccessor::GetProto(r)};
        }
    
        const auto proto = NYdb::TProtoAccessor::GetProto(res.GetResultSet(0));
        return {proto.rowsSize(), proto};
    }

    void CheckResult(const std::string& tableName, const TExpectations& expectations) {
        for (size_t attempt = 20; attempt--; ) {
            auto res = DoRead(tableName, expectations);
            Cerr << "Attempt=" << attempt << " count=" << res.first << Endl << Flush;
            if (res.first == (ssize_t)expectations.size()) {
                const Ydb::ResultSet& proto = res.second;
                for (size_t i = 0; i < expectations.size(); ++i) {
                    auto& row = proto.rows(i);
                    auto& rowExpectations = expectations[i];
                    for (size_t i = 0; i < rowExpectations.size(); ++i) {
                        auto& c = rowExpectations[i];
                        TString msg = TStringBuilder() << "Row " << i << " column '" << c.first << "': ";
                        c.second->Assert(msg, row.items(i));
                    }
                }

                return;
            }

            Sleep(TDuration::Seconds(1));
        }

        CheckTransferState(TTransferDescription::EState::Running);
        UNIT_ASSERT_C(false, "Unable to wait transfer result");
    }

    void CheckResult(const TExpectations& expectations) {
        CheckResult(TableName, expectations);
    }

    TTransferDescription CheckTransferState(TTransferDescription::EState expected) {
        for (size_t i = 20; i--;) {
            auto result = DescribeTransfer().GetTransferDescription();
            if (expected == result.GetState()) {
                return result;
            }
    
            std::string issues;
            if (result.GetState() == TTransferDescription::EState::Error) {
                issues = result.GetErrorState().GetIssues().ToOneLineString();
            }
    
            UNIT_ASSERT_C(i, "Unable to wait transfer state. Expected: " << expected << ", actual: " << result.GetState() << ", " << issues);
            Sleep(TDuration::Seconds(1));
        }

        Y_UNREACHABLE();
    }

    void CheckTransferStateError(const std::string& expectedMessage) {
        auto result = CheckTransferState(TTransferDescription::EState::Error);
        Cerr << ">>>>> ACTUAL: " << result.GetErrorState().GetIssues().ToOneLineString() << Endl << Flush;
        Cerr << ">>>>> EXPECTED: " << expectedMessage << Endl << Flush;
        UNIT_ASSERT(result.GetErrorState().GetIssues().ToOneLineString().contains(expectedMessage));
    }

    void Run(const TConfig& config) {

        CreateTable(config.TableDDL);
        CreateTopic();

        TVector<TString> lambdas;
        lambdas.insert(lambdas.end(), config.AlterLambdas.begin(), config.AlterLambdas.end());
        lambdas.push_back(config.Lambda);

        for (size_t i = 0; i < lambdas.size(); ++i) {
            auto lambda = lambdas[i];
            if (!i) {
                CreateTransfer(lambda);
            } else {
                Sleep(TDuration::Seconds(1));

                AlterTransfer(lambda);

                if (i == lambdas.size() - 1) {
                    Sleep(TDuration::Seconds(1));
                }
            }
        }

        for (const auto& m : config.Messages) {
            Write(m);
        }

        CheckResult(config.Expectations);

        DropTransfer();
        DropTable();
        DropTopic();
    }

    const std::string TableType;
    const size_t Id;
    const std::string ConnectionString;

    const std::string TopicName;
    const std::string SourceTableName;
    const std::string ChangefeedName;
    const std::string TableName;
    const std::string ReplicationName;
    const std::string TransferName;

    TDriver Driver;
    TQueryClient TableClient;
    TTopicClient TopicClient;
};


} // namespace
