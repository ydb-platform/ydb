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

namespace {

volatile size_t TestCaseCounter = RandomNumber<size_t>();

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
bool Checker<bool>::Get(const ::Ydb::Value& value) {
    return value.bool_value();
}

template<>
ui32 Checker<ui32>::Get(const ::Ydb::Value& value) {
    return value.uint32_value();
}

template<>
ui64 Checker<ui64>::Get(const ::Ydb::Value& value) {
    return value.uint64_value();
}

template<>
double Checker<double>::Get(const ::Ydb::Value& value) {
    return value.double_value();
}

template<>
TString Checker<TString>::Get(const ::Ydb::Value& value) {
    return value.text_value();
}

template<>
TInstant Checker<TInstant>::Get(const ::Ydb::Value& value) {
    return TInstant::Days(value.uint32_value());
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

TMessage _withSeqNo(ui64 seqNo) {
    return {
        .Message = TStringBuilder() << "Message-" << seqNo,
        .Partition = 0,
        .ProducerId = std::nullopt,
        .MessageGroupId = std::nullopt,
        .SeqNo = seqNo
    };
}

TMessage _withProducerId(const TString& producerId) {
    return {
        .Message = TStringBuilder() << "Message-" << producerId,
        .Partition = 0,
        .ProducerId = producerId,
        .MessageGroupId = std::nullopt,
        .SeqNo = std::nullopt
    };
}

TMessage _withMessageGroupId(const TString& messageGroupId) {
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
        : Id(TestCaseCounter++)
        , ConnectionString(GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE"))
        , TopicName(TStringBuilder() << "Topic_" << Id)
        , TableName(TStringBuilder() << "Table_" << Id)
        , TransferName(TStringBuilder() << "Transfer_" << Id)
        , Driver(TDriverConfig(ConnectionString))
        , TableClient(Driver)
        , Session(TableClient.GetSession().GetValueSync().GetSession())
        , TopicClient(Driver)
    {
    }

    void ExecuteDDL(const TString& ddl) {
        auto res = Session.ExecuteQuery(ddl, TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    }

    void CreateTable(const TString& tableDDL) {
        ExecuteDDL(Sprintf(tableDDL.data(), TableName.data()));
    }

    void CreateTopic(size_t partitionCount = 10) {
        ExecuteDDL(Sprintf(R"(
            CREATE TOPIC `%s`
            WITH (
                min_active_partitions = %d
            );
        )", TopicName.data(), partitionCount));
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
        auto res = Session.ExecuteQuery(Sprintf(R"(
            DROP TRANSFER `%s`;
        )", TransferName.data()), TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
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

    std::pair<ui64, Ydb::ResultSet> DoRead(const TExpectations& expectations) {
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
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    
        const auto proto = NYdb::TProtoAccessor::GetProto(res.GetResultSet(0));
        return {proto.rowsSize(), proto};
    }

    void CheckResult(const TExpectations& expectations) {
        for (size_t attempt = 20; attempt--; ) {
            auto res = DoRead(expectations);
            Cerr << "Attempt=" << attempt << " count=" << res.first << Endl << Flush;
            if (res.first == expectations.size()) {
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
    }

    const size_t Id;
    const TString ConnectionString;

    const TString TopicName;
    const TString TableName;
    const TString TransferName;

    TDriver Driver;
    TQueryClient TableClient;
    TSession Session;
    TTopicClient TopicClient;

    std::vector<std::string> ColumnNames;
};


} // namespace

Y_UNIT_TEST_SUITE(Transfer)
{
    Y_UNIT_TEST(Main_ColumnTable_KeyColumnFirst)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {{"Message-1"}},

            .Expectations = {{
                _C("Key", ui64(0)),
                _C("Message", TString("Message-1")),
            }}
        });
    }

    Y_UNIT_TEST(Main_ColumnTable_KeyColumnLast)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Message Utf8 NOT NULL,
                    Key Uint64 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {{"Message-1"}},

            .Expectations = {{
                _C("Key", ui64(0)),
                _C("Message", TString("Message-1")),
            }}
        });
    }

    Y_UNIT_TEST(Main_ColumnTable_ComplexKey)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Key1 Uint64 NOT NULL,
                    Key3 Uint64 NOT NULL,
                    Value1 Utf8,
                    Key2 Uint64 NOT NULL,
                    Value2 Utf8,
                    Key4 Uint64 NOT NULL,
                    PRIMARY KEY (Key3, Key2, Key1, Key4)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key1:CAST(1 AS Uint64),
                            Key2:CAST(2 AS Uint64),
                            Value2:CAST("value-2" AS Utf8),
                            Key4:CAST(4 AS Uint64),
                            Key3:CAST(3 AS Uint64),
                            Value1:CAST("value-1" AS Utf8),
                        |>
                    ];
                };
            )",

            .Messages = {{"Message-1"}},

            .Expectations = {{
                _C("Key1", ui64(1)),
                _C("Key2", ui64(2)),
                _C("Key3", ui64(3)),
                _C("Key4", ui64(4)),
                _C("Value1", TString("value-1")),
                _C("Value2", TString("value-2")),
            }}
        });
    }

    Y_UNIT_TEST(Main_ColumnTable_JsonMessage)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Id Uint64 NOT NULL,
                    FirstName Utf8 NOT NULL,
                    LastName Utf8 NOT NULL,
                    Salary Uint64 NOT NULL,
                    PRIMARY KEY (Id)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    $input = CAST($x._data AS JSON);

                    return [
                        <|
                            Id:        Yson::ConvertToUint64($input.id),
                            FirstName: CAST(Yson::ConvertToString($input.first_name) AS Utf8),
                            LastName:  CAST(Yson::ConvertToString($input.last_name) AS Utf8),
                            Salary:    CAST(Yson::ConvertToString($input.salary) AS UInt64)
                        |>
                    ];
                };
            )",

            .Messages = {{R"({
                "id": 1,
                "first_name": "Vasya",
                "last_name": "Pupkin",
                "salary": "123"
            })"}},

            .Expectations = {{
                _C("Id", ui64(1)),
                _C("FirstName", TString("Vasya")),
                _C("LastName", TString("Pupkin")),
                _C("Salary", ui64(123)),
            }}
        });
    }

    Y_UNIT_TEST(Main_ColumnTable_NullableColumn)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {{"Message-1"}},

            .Expectations = {{
                _C("Key", ui64(0)),
                _C("Message", TString("Message-1")),
            }}
        });
    }

    Y_UNIT_TEST(Main_ColumnTable_Date)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Date,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message: CAST($x._data AS Date)
                        |>
                    ];
                };
            )",

            .Messages = {{"2025-02-21"}},

            .Expectations = {{
                _C("Key", ui64(0)),
                _C("Message", TInstant::ParseIso8601("2025-02-21")),
            }}
        });
    }

    Y_UNIT_TEST(Main_ColumnTable_Double)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Double,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message: CAST($x._data AS Double)
                        |>
                    ];
                };
            )",

            .Messages = {{"1.23"}},

            .Expectations = {{
                _C("Key", ui64(0)),
                _C("Message", 1.23),
            }}
        });
    }

    Y_UNIT_TEST(Main_ColumnTable_Utf8_Long)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {{"Message-1 long value 0 1234567890 1 1234567890 2 1234567890 3 1234567890 4 1234567890 5 1234567890 6 1234567890"}},

            .Expectations = {{
                _C("Key", ui64(0)),
                _C("Message", TString("Message-1 long value 0 1234567890 1 1234567890 2 1234567890 3 1234567890 4 1234567890 5 1234567890 6 1234567890")),
            }}
        });
    }

    Y_UNIT_TEST(Main_MessageField_Partition)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Partition Uint32 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Partition)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Partition:CAST($x._partition AS Uint32),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {{"Message-1", 7}},

            .Expectations = {{
                _C("Partition", ui32(7)),
                _C("Message", TString("Message-1")),
            }}
        });
    }

    Y_UNIT_TEST(Main_MessageField_SeqNo)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    SeqNo Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (SeqNo)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            SeqNo:CAST($x._seq_no AS Uint32),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {_withSeqNo(13)},

            .Expectations = {{
                _C("SeqNo", ui64(13)),
            }}
        });
    }

    Y_UNIT_TEST(Main_MessageField_ProducerId)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Offset Uint64 NOT NULL,
                    ProducerId Utf8,
                    PRIMARY KEY (Offset)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Offset:CAST($x._offset AS Uint64),
                            ProducerId:CAST($x._producer_id AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {_withProducerId("Producer-13")},

            .Expectations = {{
                _C("ProducerId", TString("Producer-13")),
            }}
        });
    }

    Y_UNIT_TEST(Main_MessageField_MessageGroupId)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Offset Uint64 NOT NULL,
                    MessageGroupId Utf8,
                    PRIMARY KEY (Offset)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Offset:CAST($x._offset AS Uint64),
                            MessageGroupId:CAST($x._message_group_id AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {_withMessageGroupId("MessageGroupId-13")},

            .Expectations = {{
                _C("MessageGroupId", TString("MessageGroupId-13")),
            }}
        });
    }

    Y_UNIT_TEST(AlterLambda)
    {
        MainTestCase().Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )",
    
            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data || " new lambda" AS Utf8)
                        |>
                    ];
                };
            )",
    
            .Messages = {{"Message-1"}},
    
            .Expectations = {{
                _C("Message", TString("Message-1 new lambda")),
            }},

            .AlterLambdas = {
                R"(
                    $l = ($x) -> {
                        return [
                            <|
                                Key:CAST($x._offset AS Uint64),
                                Message:CAST($x._data || " 1 lambda" AS Utf8)
                            |>
                        ];
                    };
                )",
                R"(
                    $l = ($x) -> {
                        return [
                            <|
                                Key:CAST($x._offset AS Uint64),
                                Message:CAST($x._data || " 2 lambda" AS Utf8)
                            |>
                        ];
                    };
                )",
            }
    
        });
    }

    Y_UNIT_TEST(DropTransfer)
    {
        MainTestCase testCase;
        testCase.Run({
            .TableDDL = R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )",

            .Lambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )",

            .Messages = {{"Message-1"}},

            .Expectations = {{
                _C("Key", ui64(0)),
                _C("Message", TString("Message-1")),
            }}
        });

        {
            auto result = testCase.DescribeTransfer();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
        }

        testCase.DropTransfer();

        {
            auto result = testCase.DescribeTransfer();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(EStatus::SCHEME_ERROR, result.GetStatus());
        }
    }

    Y_UNIT_TEST(CreateAndDropConsumer)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");
        
        testCase.CreateTopic();
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        for (size_t i = 20; i--; ) {
            auto result = testCase.DescribeTopic();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
            auto& consumers = result.GetTopicDescription().GetConsumers();
            if (1 == consumers.size()) {
                UNIT_ASSERT_VALUES_EQUAL(1, consumers.size());
                Cerr << "Consumer name is '" << consumers[0].GetConsumerName() << "'" << Endl << Flush;
                UNIT_ASSERT_C("replicationConsumer" != consumers[0].GetConsumerName(), "Consumer name is random uuid");
                break;
            }

            UNIT_ASSERT_C(i, "Unable to wait consumer has been created");
            Sleep(TDuration::Seconds(1));
        }

        testCase.DropTransfer();

        for (size_t i = 20; i--; ) {
            auto result = testCase.DescribeTopic();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
            auto& consumers = result.GetTopicDescription().GetConsumers();
            if (0 == consumers.size()) {
                UNIT_ASSERT_VALUES_EQUAL(0, consumers.size());
                break;
            }

            UNIT_ASSERT_C(i, "Unable to wait consumer has been removed");
            Sleep(TDuration::Seconds(1));
        }
    }

    Y_UNIT_TEST(DescribeError_OnLambdaCompilation)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");
        
        testCase.CreateTopic(1);
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return $x._unknown_field_for_lambda_compilation_error;
                };
            )");
        
        testCase.CheckTransferStateError("_unknown_field_for_lambda_compilation_error");
    }
/*
    Y_UNIT_TEST(DescribeError_OnWriteToShard)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");
        
        testCase.CreateTopic(1);
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:null,
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");
        
        testCase.Write({"message-1"});

        testCase.CheckTransferStateError("Cannot write data into shard");
    }
*/

    Y_UNIT_TEST(CustomConsumer)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");
        
        testCase.CreateTopic(1);
        testCase.CreateConsumer("PredefinedConsumer");
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithConsumerName("PredefinedConsumer"));

        Sleep(TDuration::Seconds(3));

        { // Check that consumer is reused
            auto result = testCase.DescribeTopic();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
            auto& consumers = result.GetTopicDescription().GetConsumers();
            UNIT_ASSERT_VALUES_EQUAL(1, consumers.size());
            UNIT_ASSERT_VALUES_EQUAL("PredefinedConsumer", consumers[0].GetConsumerName());
        }

        testCase.DropTransfer();

        Sleep(TDuration::Seconds(3));

        { // Check that consumer is not removed
            auto result = testCase.DescribeTopic();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToOneLineString());
            auto& consumers = result.GetTopicDescription().GetConsumers();
            UNIT_ASSERT_VALUES_EQUAL(1, consumers.size());
            UNIT_ASSERT_VALUES_EQUAL("PredefinedConsumer", consumers[0].GetConsumerName());
        }
    }

    Y_UNIT_TEST(CustomFlushInterval)
    {
        TDuration flushInterval = TDuration::Seconds(5);

        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");

        testCase.CreateTopic(1);
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithBatching(flushInterval, 13_MB));


        TInstant expectedEnd = TInstant::Now() + flushInterval;
        testCase.Write({"Message-1"});

        // check that data in the table only after flush interval
        for (size_t attempt = 20; attempt--; ) {
            auto res = testCase.DoRead({{
                _C("Key", ui64(0)),
            }});
            Cerr << "Attempt=" << attempt << " count=" << res.first << Endl << Flush;
            if (res.first == 1) {
                UNIT_ASSERT_C(expectedEnd <= TInstant::Now(), "Expected: " << expectedEnd << " Now: " << TInstant::Now());
                break;
            }

            UNIT_ASSERT_C(attempt, "Unable to wait transfer result");
            Sleep(TDuration::Seconds(1));
        }
    }

    Y_UNIT_TEST(AlterFlushInterval)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");

        testCase.CreateTopic(1);
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithBatching(TDuration::Hours(1), 1_GB));


        testCase.Write({"Message-1"});

        // check if there isn`t data in the table (flush_interval is big)
        for (size_t attempt = 5; attempt--; ) {
            auto res = testCase.DoRead({{
                _C("Key", ui64(0)),
            }});
            Cerr << "Attempt=" << attempt << " count=" << res.first << Endl << Flush;
            UNIT_ASSERT_VALUES_EQUAL_C(0, res.first, "Flush has not been happened");
            Sleep(TDuration::Seconds(1));
        }

        // flush interval is small
        testCase.AlterTransfer(MainTestCase::AlterTransferSettings::WithBatching(TDuration::MilliSeconds(1), 1_GB), false);
        // flush interval is big
        testCase.AlterTransfer(MainTestCase::AlterTransferSettings::WithBatching(TDuration::Days(1) + TDuration::Seconds(1), 1_GB), false);

        testCase.AlterTransfer(MainTestCase::AlterTransferSettings::WithBatching(TDuration::Seconds(1), 1_GB));

        // check if there is data in the table
        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});
    }

    Y_UNIT_TEST(AlterBatchSize)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");

        testCase.CreateTopic(1);
        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithBatching(TDuration::Hours(1), 512_MB));


        testCase.Write({"Message-1"});

        // batch size is big. alter is not success
        testCase.AlterTransfer(MainTestCase::AlterTransferSettings::WithBatching(TDuration::Hours(1), 1_GB + 1), false);

        // batch size is top valid value. alter is success
        testCase.AlterTransfer(MainTestCase::AlterTransferSettings::WithBatching(TDuration::Hours(1), 1_GB));

        // batch size is small. alter is success. after flush will
        testCase.AlterTransfer(MainTestCase::AlterTransferSettings::WithBatching(TDuration::Hours(1), 1));

        // check if there is data in the table
        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});
    }

    Y_UNIT_TEST(CreateTransferSourceNotExists)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        testCase.CheckTransferStateError("Discovery error: local/Topic_");
    }

    Y_UNIT_TEST(CreateTransferSourceIsNotTopic)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");
        
            testCase.ExecuteDDL(Sprintf(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    PRIMARY KEY (Key)
                );
            )", testCase.TopicName.data()));

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        testCase.CheckTransferStateError("Discovery error: local/Topic_");
    }

    Y_UNIT_TEST(CreateTransferRowTable)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8 NOT NULL,
                    PRIMARY KEY (Key)
                );
            )");
        testCase.CreateTopic();

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        testCase.CheckTransferStateError("Only column tables are supported as transfer targets");
    }

    Y_UNIT_TEST(CreateTransferTargetIsNotTable)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TOPIC `%s`;
            )");
        testCase.CreateTopic();

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        testCase.CheckTransferStateError("Only column tables are supported as transfer targets");
    }

    Y_UNIT_TEST(CreateTransferTargetNotExists)
    {
        MainTestCase testCase;
        testCase.CreateTopic();

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )");

        testCase.CheckTransferStateError(TStringBuilder() << "The target table `/local/" << testCase.TableName << "` does not exist");
    }

    Y_UNIT_TEST(PauseAndResumeTransfer)
    {
        MainTestCase testCase;
        testCase.CreateTable(R"(
                CREATE TABLE `%s` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )");
        testCase.CreateTopic(1);

        testCase.CreateTransfer(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };
            )", MainTestCase::CreateTransferSettings::WithBatching(TDuration::Seconds(1), 1));

        testCase.Write({"Message-1"});

        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        testCase.CheckTransferState(TReplicationDescription::EState::Running);

        Cerr << "State: Paused" << Endl << Flush;

        testCase.PauseTransfer();

        Sleep(TDuration::Seconds(1));
        testCase.CheckTransferState(TReplicationDescription::EState::Paused);

        testCase.Write({"Message-2"});

        // Transfer is paused. New messages aren`t added to the table.
        Sleep(TDuration::Seconds(3));
        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }});

        Cerr << "State: StandBy" << Endl << Flush;

        testCase.ResumeTransfer();

        // Transfer is resumed. New messages are added to the table.
        testCase.CheckTransferState(TReplicationDescription::EState::Running);
        testCase.CheckResult({{
            _C("Message", TString("Message-1"))
        }, {
            _C("Message", TString("Message-2")),
        }});

        // More cycles for pause/resume
        testCase.PauseTransfer();
        testCase.CheckTransferState(TReplicationDescription::EState::Paused);

        testCase.ResumeTransfer();
        testCase.CheckTransferState(TReplicationDescription::EState::Running);
    }
}

