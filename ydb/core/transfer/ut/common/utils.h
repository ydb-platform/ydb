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

#include <library/cpp/http/io/stream.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/threading/local_executor/local_executor.h>
#include <util/network/socket.h>

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

struct Timestamp64Checker : public Checker<TInstant> {
    Timestamp64Checker(TInstant&& expected, TDuration&& precision)
        : Checker<TInstant>(std::move(expected))
        , Precision(precision) {
    }

    TInstant Get(const ::Ydb::Value& value) override {
        return TInstant::MicroSeconds(value.uint64_value());
    }

    void Assert(const std::string& msg, const ::Ydb::Value& value) override {
        auto v = Get(value);
        if (Expected - Precision > v || Expected + Precision < v) {
            UNIT_ASSERT_VALUES_EQUAL_C(v, Expected, msg);
        }
    }

    TDuration Precision;
};

struct NullChecker : public IChecker {
    bool Get(const ::Ydb::Value& value) {
        return value.has_null_flag_value();
    }

    void Assert(const std::string& msg, const ::Ydb::Value& value) override {
        UNIT_ASSERT_VALUES_EQUAL_C(Get(value), true, msg);
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
inline std::pair<ui64, i64> Checker<std::pair<ui64, i64>>::Get(const ::Ydb::Value& value) {
    return std::make_pair(value.high_128(), value.low_128());
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

template<typename C, typename... T>
std::pair<TString, std::shared_ptr<IChecker>> _T(std::string&& name, T&&... expected) {
    return {
        std::move(name),
        std::make_shared<C>(std::forward<T>(expected)...)
    };
}

struct TMessage {
    TString Message;
    std::optional<ui32> Partition = std::nullopt;
    std::optional<std::string> ProducerId = std::nullopt;
    std::optional<std::string> MessageGroupId = std::nullopt;
    std::optional<ui64> SeqNo = std::nullopt;
    std::optional<TInstant> CreateTimestamp = std::nullopt;
    std::map<std::string, std::string> Attributes = {};
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

inline TMessage _withCreateTimestamp(const TInstant& timestamp) {
    return {
        .Message = TStringBuilder() << "Message-" << timestamp,
        .Partition = 0,
        .ProducerId = std::nullopt,
        .MessageGroupId = std::nullopt,
        .SeqNo = std::nullopt,
        .CreateTimestamp = timestamp
    };
}

inline TMessage _withAttributes(std::map<std::string, std::string>&& attributes) {
    return {
        .Message = TStringBuilder() << "Message",
        .Partition = 0,
        .ProducerId = std::nullopt,
        .MessageGroupId = std::nullopt,
        .SeqNo = std::nullopt,
        .CreateTimestamp = std::nullopt,
        .Attributes = std::move(attributes)
    };
}

struct TMetricInfo {
    THashMap<TString, TString> Labels;
    ui64 Value;
};

struct TTransferMetrics {
    THashMap<TString, TMetricInfo> AggregatedMetrics;
    THashMap<TString, TVector<TMetricInfo>> DetailedMetrics;
};

class TMetricsValidator {
public:
    using TValidator = std::function<void (const TTransferMetrics&)>;
    using TValueValidator = std::function<TString (const TVector<TMetricInfo>&)>;

private:
    TVector<TValidator> Validators;
    const TVector<TString> MustHaveLabels = {"transfer_id", "database_id", "folder_id", "cloud_id"};


    void CheckMetricInfo(const TVector<TMetricInfo>& metrics, const TString& name, const TVector<TString>& extraLabels = {}) const {
        auto expectedLabels = MustHaveLabels;
        expectedLabels.insert(expectedLabels.end(), extraLabels.begin(), extraLabels.end());
        for (const auto& metricInfo : metrics) {
            for (const auto& label : expectedLabels) {
                auto labelIter = metricInfo.Labels.find(label);
                UNIT_ASSERT_C(!labelIter.IsEnd(), TStringBuilder() << "Metric " << name << " must have label " << label);
                if (label == "transfer_id") {
                    UNIT_ASSERT_C(!labelIter->second.empty(), TStringBuilder() << "Metric's " << name << " label " << label << " must not be empty");
                }
            }
        }
    }

public:
    TMetricsValidator& HasTransferMetrics() {
        Validators.emplace_back(
            [](const TTransferMetrics& metrics) {
                if (metrics.AggregatedMetrics.size() > 0){
                    return TString{};
                }
                return TString{"Transfer metrics is empty"};
            });
        return *this;
    }

    TMetricsValidator& HasDetailedMetrics() {
        Validators.emplace_back(
            [](const TTransferMetrics& metrics) {
                if (metrics.DetailedMetrics.size() > 0) {
                    return TString{};
                }
                return TString{"Transfer detailed metrics is empty"};
            });
        return *this;
    }

    TMetricsValidator& HasTransferSensor(const TString& name,
                                         TValueValidator valueValidator = [](const auto&) -> TString { return {}; }) {
        Validators.emplace_back(
            [=](const TTransferMetrics& metrics) -> void {
                auto iter = metrics.AggregatedMetrics.find(name);
                UNIT_ASSERT_C(iter != metrics.AggregatedMetrics.end(), TStringBuilder() << "Transfer sensor " << name << " not found");
                CheckMetricInfo({iter->second}, name);
                auto error = valueValidator({iter->second});
                UNIT_ASSERT_C(error.empty(), TStringBuilder() << "Error checking sensor: " << name << ": " << error);
            });
        return *this;
    };

    TMetricsValidator& HasDetailedSensor(const TString& name,
                                         TValueValidator valueValidator = [](const auto&) -> TString { return {}; })
    {
        Validators.emplace_back(
            [=](const TTransferMetrics& metrics) -> void {
                auto iter = metrics.DetailedMetrics.find(name);
                UNIT_ASSERT_C(iter != metrics.DetailedMetrics.end(), TStringBuilder() << "Transfer detailed sensor " << name << " not found");
                CheckMetricInfo(iter->second, name, {"monitoring_project_id"});
                auto error = valueValidator(iter->second);
                UNIT_ASSERT_C(error.empty(), TStringBuilder() << "Error checking sensor: " << name << ": " << error);
            });
        return *this;
    };

    TMetricsValidator& AddValidator(TValidator&& validator) {
        Validators.emplace_back(std::move(validator));
        return *this;
    }

    void Validate(const TTransferMetrics& metrics) const {
        for (const auto& validator : Validators) {
            validator(metrics);
        }
    }
};

using TExpectations = TVector<TVector<std::pair<TString, std::shared_ptr<IChecker>>>>;

struct TConfig {
    const TString TableDDL;
    const TString Lambda;
    const TVector<TMessage> Messages;
    const TExpectations Expectations;
    const TVector<TString> AlterLambdas;
    const TMaybe<TMetricsValidator> MetricsValidator;
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

    MainTestCase(const std::optional<std::string> user = std::nullopt, std::string tableType = "ROW")
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
        ChangePermissions("GRANT", "TO", object, username, permissions);
    }

    void Revoke(const std::string& object, const std::string& username, const std::vector<std::string>& permissions) {
        ChangePermissions("REVOKE", "FROM", object, username, permissions);
    }

    void ChangePermissions(const std::string& statement, const std::string& statementClause, const std::string& object, const std::string& username, const std::vector<std::string>& permissions) {
        TStringBuilder sql;
        sql << statement << " ";
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
        sql << "` " << statementClause << " `" << username << "@builtin`";

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
        std::optional<std::string> UserSecret;
        std::optional<std::string> Directory;
        ui64 MetricsLevel = 0;

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

        static CreateTransferSettings WithSecret(const TString& secret) {
            CreateTransferSettings result;
            result.UserSecret = secret;
            return result;
        }
    };

    void CreateTransfer(const std::string& lambda, const CreateTransferSettings& settings = CreateTransferSettings()) {
        CreateTransfer(TransferName, lambda, settings);
    }

    void CreateTransfer(const std::string& name, const std::string& lambda, const CreateTransferSettings& settings = CreateTransferSettings()) {
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
        if (settings.UserSecret) {
            options.push_back(TStringBuilder() <<  "TOKEN_SECRET_PATH = '" << *settings.UserSecret << "'");
        }
        if (settings.Username) {
            options.push_back(TStringBuilder() <<  "TOKEN = '" << *settings.Username << "@builtin'");
        }
        if (settings.Directory) {
            options.push_back(TStringBuilder() <<  "DIRECTORY = '" << *settings.Directory << "'");
        }
        switch (settings.MetricsLevel) {
            case 2:
                options.push_back("METRICS_LEVEL = 'OBJECT'");
                break;
            case 3:
                options.push_back("METRICS_LEVEL = 'DETAILED'");
                break;
            default:
                break;
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
        )", lambda.data(), name.data(), topicName.data(), TableName.data(), optionsStr.data());

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
        AlterTransfer(TransferName, settings, success);
    }

    void AlterTransfer(const std::string& name, const AlterTransferSettings& settings, bool success = true) {
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
        )", lambda.data(), name.data(), setLambda.data(), setOptions.data()), success);
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
                STATE = "Active"
            );
        )", TransferName.data()));
    }

    auto DescribeTransfer(bool includeStats = false) {
        TReplicationClient client(Driver);
        TDescribeTransferSettings settings;
        settings.IncludeStats(includeStats);
        return client.DescribeTransfer(TString("/") + GetEnv("YDB_DATABASE") + "/" + TransferName, settings).ExtractValueSync();
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
        const auto topic = DescribeTopic();
        const auto& consumers = topic.GetTopicDescription().GetConsumers();
        UNIT_ASSERT_VALUES_EQUAL(1, consumers.size());
        return DescribeConsumer(consumers[0].GetConsumerName());
    }

    void CheckCommittedOffset(size_t partitionId, size_t expectedOffset, TDuration timeout = TDuration::Seconds(5)) {
        auto end = TInstant::Now() + timeout;

        while(true) {
            auto d = DescribeConsumer();
            UNIT_ASSERT(d.IsSuccess());
            auto s = d.GetConsumerDescription().GetPartitions().at(partitionId).GetPartitionConsumerStats();
            if (expectedOffset == s->GetCommittedOffset()) {
                break;
            }
            if (end < TInstant::Now()) {
                UNIT_ASSERT_VALUES_EQUAL(expectedOffset, s->GetCommittedOffset());
            }

            Sleep(TDuration::Seconds(1));
        }
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

    auto DescribeReplication(const std::string& name) {
        TReplicationClient client(Driver);

        TDescribeReplicationSettings settings;
        settings.IncludeStats(true);

        return client.DescribeReplication(TString("/") + GetEnv("YDB_DATABASE") + "/" + name, settings).ExtractValueSync();
    }

    auto DescribeReplication() {
        return DescribeReplication(ReplicationName);
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

    void CreateUser(const std::string& username, const std::optional<std::string> password = std::nullopt) {
        if (password) {
            ExecuteDDL(Sprintf(R"(
                CREATE USER %s PASSWORD '%s'
            )", username.data(), password.value().data()));
        } else {
            ExecuteDDL(Sprintf(R"(
                CREATE USER %s
            )", username.data()));
        }
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

        TWriteMessage msg(message.Message);
        msg.SeqNo(message.SeqNo);
        msg.CreateTimestamp(message.CreateTimestamp);

        if (!message.Attributes.empty()) {
            TWriteMessage::TMessageMeta meta;
            for (auto& [k, v] : message.Attributes) {
                meta.push_back({k , v});
            }
            msg.MessageMeta(meta);
        }

        UNIT_ASSERT(writeSession->Write(std::move(msg)));
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
            Cerr << "Attempt=" << attempt << " count=" << res.first <<", expectations: " << expectations.size() << Endl << Flush;
            if (res.first == (ssize_t)expectations.size()) {
                const Ydb::ResultSet& proto = res.second;
                for (size_t i = 0; i < expectations.size(); ++i) {
                    auto& row = proto.rows(i);
                    auto& rowExpectations = expectations[i];
                    for (size_t j = 0; j < rowExpectations.size(); ++j) {
                        auto& c = rowExpectations[j];
                        TString msg = TStringBuilder() << "Row " << i << " column '" << c.first << "': ";
                        c.second->Assert(msg, row.items(j));
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

    void MakeTest(const TConfig& config, const CreateTransferSettings settings = {}) {
        CreateTable(config.TableDDL);
        CreateTopic();

        TVector<TString> lambdas;
        lambdas.insert(lambdas.end(), config.AlterLambdas.begin(), config.AlterLambdas.end());
        lambdas.push_back(config.Lambda);

        for (size_t i = 0; i < lambdas.size(); ++i) {
            auto lambda = lambdas[i];
            if (!i) {
                CreateTransfer(lambda, settings);
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

        if (config.MetricsValidator) {
            auto metrics = GetMetrics();
            config.MetricsValidator->Validate(metrics);
        }
    }
    void MakeCleanup() {
        DropTransfer();
        DropTable();
        DropTopic();
    }

    void Run(const TConfig& config, const CreateTransferSettings settings = {}) {
        MakeTest(config, settings);
        MakeCleanup();
    }

    TTransferMetrics GetMetrics() {
        TTransferMetrics result;
        TNetworkAddress addr("localhost", FromString<ui16>(GetEnv("YDB_MON_PORT")));
        TSocket s(addr);
        SendMinimalHttpRequest(s, "localhost", "/counters/json");
        TSocketInput si(s);
        THttpInput input(&si);
        TString firstLine = input.FirstLine();
        const auto httpCode = ParseHttpRetCode(firstLine);
        UNIT_ASSERT_VALUES_EQUAL(httpCode, 200u);
        NJson::TJsonValue value;
        bool res = NJson::ReadJsonTree(&input, &value);
        auto arr = value.GetMap().at("sensors").GetArray();
        NJson::TJsonValue transfer, detailed;
        TSet<TString> counterTypes;

        auto GetLabelsSet = [](const auto& labels) {
            THashMap<TString, TString> result;
            for (const auto& [key, value] : labels) {
                result.emplace(key, value.GetString());
            }
            return result;
        };
        for (const auto& item : arr) {
            const auto& topLevel = item.GetMap();
            const auto& labels = topLevel.at("labels").GetMap();
            auto iter = labels.find("counters");
            if (iter.IsEnd()) {
                continue;
            }
            const auto& counterType = iter->second.GetString();
            iter = labels.find("name");
            if (iter.IsEnd()) {
                continue;
            }
            auto& counterName = iter->second.GetString();
            if (counterType == "transfer") {
                result.AggregatedMetrics[counterName] = TMetricInfo{GetLabelsSet(labels), topLevel.at("value").GetUInteger()};
                Cerr << "Add transfer counter " << counterName << ", value: " << topLevel.at("value").GetUInteger() << Endl << Flush;
            } else if (counterType == "transfer_detailed") {
                result.DetailedMetrics[counterName].emplace_back(TMetricInfo{GetLabelsSet(labels), topLevel.at("value").GetUInteger()});
                Cerr << "Add transfer_detailed counter " << counterName << ", value: " << topLevel.at("value").GetUInteger() << Endl << Flush;
            }
        }
        UNIT_ASSERT(res);

        return result;
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
