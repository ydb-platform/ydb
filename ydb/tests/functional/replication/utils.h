#pragma once

#include <util/system/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
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
    virtual void Assert(const TString& msg, const ::Ydb::Value& value) = 0;
    virtual ~IChecker() = default;
};


template<typename T>
struct Checker : public IChecker {
    Checker(T&& expected)
        : Expected(std::move(expected))
    {}

    void Assert(const TString& msg, const ::Ydb::Value& value) override {
        UNIT_ASSERT_VALUES_EQUAL_C(Get(value), Expected, msg);
    }

    T Get(const ::Ydb::Value& value);

    T Expected;
};

template<>
inline bool Checker<bool>::Get(const ::Ydb::Value& value) {
    return value.bool_value();
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
std::pair<TString, std::shared_ptr<IChecker>> _C(TString&& name, T&& expected) {
    return {
        std::move(name),
        std::make_shared<Checker<T>>(std::move(expected))
    };
}

struct TMessage {
    TString Message;
    std::optional<ui32> Partition = std::nullopt;
    std::optional<TString> ProducerId = std::nullopt;
    std::optional<TString> MessageGroupId = std::nullopt;
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

inline TMessage _withProducerId(const TString& producerId) {
    return {
        .Message = TStringBuilder() << "Message-" << producerId,
        .Partition = 0,
        .ProducerId = producerId,
        .MessageGroupId = std::nullopt,
        .SeqNo = std::nullopt
    };
}

inline TMessage _withMessageGroupId(const TString& messageGroupId) {
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

    MainTestCase()
        : Id(RandomNumber<size_t>())
        , ConnectionString(GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE"))
        , TopicName(TStringBuilder() << "Topic_" << Id)
        , SourceTableName(TStringBuilder() << "SourceTable_" << Id)
        , TableName(TStringBuilder() << "Table_" << Id)
        , ReplicationName(TStringBuilder() << "Replication_" << Id)
        , TransferName(TStringBuilder() << "Transfer_" << Id)
        , Driver(TDriverConfig(ConnectionString))
        , TableClient(Driver)
        , Session(TableClient.GetSession().GetValueSync().GetSession())
        , TopicClient(Driver)
    {
    }

    ~MainTestCase() {
        Driver.Stop(true);
    }

    void ExecuteDDL(const TString& ddl, bool checkResult = true) {
        Cerr << "DDL: " << ddl << Endl << Flush;
        auto res = Session.ExecuteQuery(ddl, TTxControl::NoTx()).GetValueSync();
        if (checkResult) {
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }
    }

    auto ExecuteSourceTableQuery(const TString& query) {
        for (size_t i = 10; i--;) {
            auto q = Sprintf(query.data(), SourceTableName.data());
            Cerr << ">>>>> Query: " << q << Endl << Flush;
            auto res = Session.ExecuteQuery(q, TTxControl::NoTx()).GetValueSync();
            if (res.IsSuccess()) {
                return;
            }

            UNIT_ASSERT_C(i, res.GetIssues().ToString());
            Sleep(TDuration::Seconds(1));
        }
    }

    void CreateTable(const TString& tableDDL) {
        ExecuteDDL(Sprintf(tableDDL.data(), TableName.data()));
    }

    void DropTable() {
        ExecuteDDL(Sprintf("DROP TABLE `%s`", TableName.data()));
    }

    void CreateSourceTable(const TString& tableDDL) {
        ExecuteDDL(Sprintf(tableDDL.data(), SourceTableName.data()));
    }

    void DropSourceTable() {
        ExecuteDDL(Sprintf("DROP TABLE `%s`", SourceTableName.data()));
    }

    void CreateTopic(size_t partitionCount = 10) {
        ExecuteDDL(Sprintf(R"(
            CREATE TOPIC `%s`
            WITH (
                min_active_partitions = %d
            );
        )", TopicName.data(), partitionCount));
    }

    void DropTopic() {
        ExecuteDDL(Sprintf("DROP TOPIC `%s`", TopicName.data()));
    }

    void CreateConsumer(const TString& consumerName) {
        ExecuteDDL(Sprintf(R"(
            ALTER TOPIC `%s`
            ADD CONSUMER `%s`;
        )", TopicName.data(), consumerName.data()));
    }

    struct CreateTransferSettings {
        std::optional<TString> ConsumerName = std::nullopt;
        std::optional<TDuration> FlushInterval;
        std::optional<ui64> BatchSizeBytes;

        CreateTransferSettings()
            : ConsumerName(std::nullopt)
            , FlushInterval(TDuration::Seconds(1))
            , BatchSizeBytes(8_MB) {}

        static CreateTransferSettings WithConsumerName(const TString& consumerName) {
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
    };

    void CreateTransfer(const TString& lambda, const CreateTransferSettings& settings = CreateTransferSettings()) {
        TStringBuilder sb;
        if (settings.ConsumerName) {
            sb << ", CONSUMER = '" << *settings.ConsumerName << "'" << Endl;
        }
        if (settings.FlushInterval) {
            sb << ", FLUSH_INTERVAL = Interval('PT" << settings.FlushInterval->Seconds() << "S')" << Endl;
        }
        if (settings.BatchSizeBytes) {
            sb << ", BATCH_SIZE_BYTES = " << *settings.BatchSizeBytes << Endl;
        }

        auto ddl = Sprintf(R"(
            %s;

            CREATE TRANSFER `%s`
            FROM `%s` TO `%s` USING $l
            WITH (
                CONNECTION_STRING = 'grpc://%s'
                %s
            );
        )", lambda.data(), TransferName.data(), TopicName.data(), TableName.data(), ConnectionString.data(), sb.data());

        ExecuteDDL(ddl);
    }

    struct AlterTransferSettings {
        std::optional<TString> TransformLambda;
        std::optional<TDuration> FlushInterval;
        std::optional<ui64> BatchSizeBytes;

        AlterTransferSettings()
            : FlushInterval(std::nullopt)
            , BatchSizeBytes(std::nullopt) {}

        static AlterTransferSettings WithBatching(const TDuration& flushInterval, const ui64 batchSize) {
            AlterTransferSettings result;
            result.FlushInterval = flushInterval;
            result.BatchSizeBytes = batchSize;
            return result;
        }

        static AlterTransferSettings WithTransformLambda(const TString& lambda) {
            AlterTransferSettings result;
            result.TransformLambda = lambda;
            return result;
        }
    };

    void AlterTransfer(const TString& lambda) {
        AlterTransfer(AlterTransferSettings::WithTransformLambda(lambda));
    }

    void AlterTransfer(const AlterTransferSettings& settings, bool success = true) {
        TString lambda = settings.TransformLambda ? *settings.TransformLambda : "";
        TString setLambda = settings.TransformLambda ? "SET USING $l" : "";

        TStringBuilder sb;
        if (settings.FlushInterval) {
            sb << "FLUSH_INTERVAL = Interval('PT" << settings.FlushInterval->Seconds() << "S')" << Endl;
        }
        if (settings.BatchSizeBytes) {
            sb << ", BATCH_SIZE_BYTES = " << *settings.BatchSizeBytes << Endl;
        }

        TString setOptions;
        if (!sb.empty()) {
            setOptions = TStringBuilder() << "SET (" << sb << " )";
        }

        auto res = Session.ExecuteQuery(Sprintf(R"(
            %s;

            ALTER TRANSFER `%s`
            %s
            %s;
        )", lambda.data(), TransferName.data(), setLambda.data(), setOptions.data()), TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(success, res.IsSuccess(), res.GetIssues().ToString());
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

        TDescribeReplicationSettings settings;
        settings.IncludeStats(true);

        return client.DescribeReplication(TString("/") + GetEnv("YDB_DATABASE") + "/" + TransferName, settings).ExtractValueSync();
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

    auto DescribeTopic() {
        TDescribeTopicSettings settings;
        settings.IncludeLocation(true);
        settings.IncludeStats(true);

        return TopicClient.DescribeTopic(TopicName, settings).ExtractValueSync();
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
        auto& e = expectations.front();

        TStringBuilder columns;
        for (size_t i = 0; i < e.size(); ++i) {
            if (i) {
                columns << ", ";
            }
            columns << "`" << e[i].first << "`";
        }


        auto query = Sprintf("SELECT %s FROM `%s` ORDER BY %s", columns.data(), TableName.data(), columns.data());
        Cerr << ">>>>> Query: " << query << Endl << Flush;
        auto res = Session.ExecuteQuery(query, TTxControl::NoTx()).GetValueSync();
        if (!res.IsSuccess()) {
            Cerr << ">>>>> Query error: " << res.GetIssues().ToString() << Endl << Flush;
            TResultSet r{Ydb::ResultSet()};
            return {-1, NYdb::TProtoAccessor::GetProto(r)};
        }
    
        const auto proto = NYdb::TProtoAccessor::GetProto(res.GetResultSet(0));
        return {proto.rowsSize(), proto};
    }

    void CheckResult(const TExpectations& expectations) {
        for (size_t attempt = 20; attempt--; ) {
            auto res = DoRead(expectations);
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

                break;
            }

            UNIT_ASSERT_C(attempt, "Unable to wait transfer result");
            Sleep(TDuration::Seconds(1));
        }
    }

    TReplicationDescription CheckTransferState(TReplicationDescription::EState expected) {
        for (size_t i = 20; i--;) {
            auto result = DescribeTransfer().GetReplicationDescription();
            if (expected == result.GetState()) {
                return result;
            }
    
            UNIT_ASSERT_C(i, "Unable to wait transfer state. Expected: " << expected << ", actual: " << result.GetState());
            Sleep(TDuration::Seconds(1));
        }

        Y_UNREACHABLE();
    }

    void CheckTransferStateError(const TString& expectedMessage) {
        auto result = CheckTransferState(TReplicationDescription::EState::Error);
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

    const size_t Id;
    const TString ConnectionString;

    const TString TopicName;
    const TString SourceTableName;
    const TString TableName;
    const TString ReplicationName;
    const TString TransferName;

    TDriver Driver;
    TQueryClient TableClient;
    TSession Session;
    TTopicClient TopicClient;
};


} // namespace
