#include <util/system/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/query/client.h>
#include <ydb-cpp-sdk/client/topic/client.h>
#include <ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb-cpp-sdk/client/draft/ydb_scripting.h>

#include <library/cpp/threading/local_executor/local_executor.h>

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NYdb::NTopic;

namespace {

volatile size_t TestCaseCounter = 0;

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

struct TConfig {
    const char* TableDDL;
    const char* Lambda;
    const TVector<TMessage> Messages;
    TVector<std::pair<TString, std::shared_ptr<IChecker>>> Expectations;
    std::optional<TString> AlterLambda = std::nullopt;
};


struct MainTestCase {

    MainTestCase(TConfig&& config)
        : Config(std::move(config)) {
        size_t id = TestCaseCounter++;

        TopicName = TStringBuilder() << "Topic_" << id;
        TableName = TStringBuilder() << "Table_" << id;
        TransferName = TStringBuilder() << "Transfer_" << id;
    }

    void Run() {
        TString connectionString = GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE");

        auto config = TDriverConfig(connectionString);
        auto driver = TDriver(config);
        auto tableClient = TQueryClient(driver);
        auto session = tableClient.GetSession().GetValueSync().GetSession();
        auto topicClient = TTopicClient(driver);

        {
            auto tableDDL = Sprintf(Config.TableDDL, TableName.data());
            auto res = session.ExecuteQuery(tableDDL, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        {
            auto res = session.ExecuteQuery(Sprintf(R"(
                CREATE TOPIC `%s`
                WITH (
                    min_active_partitions = 10
                );
            )", TopicName.data()), TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        {
            auto res = session.ExecuteQuery(Sprintf(R"(
                %s;

                CREATE TRANSFER `%s`
                FROM `%s` TO `%s` USING $l
                WITH (
                    CONNECTION_STRING = 'grpc://%s'
                    -- , TOKEN = 'user@builtin'
                );
            )", Config.Lambda, TransferName.data(), TopicName.data(), TableName.data(), connectionString.data()), TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        {
            if (Config.AlterLambda) {
                Sleep(TDuration::Seconds(1));

                auto res = session.ExecuteQuery(Sprintf(R"(
                    %s;

                    ALTER TRANSFER `%s`
                    SET USING $l;
                )", Config.AlterLambda->data(), TransferName.data()), TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

                Sleep(TDuration::Seconds(1));
            }
        }

        {
            for (const auto& m : Config.Messages) {
                TWriteSessionSettings writeSettings;
                writeSettings.Path(TopicName);
                writeSettings.DeduplicationEnabled(m.SeqNo);
                if (m.Partition) {
                    writeSettings.PartitionId(m.Partition);
                }
                if (m.ProducerId) {
                    writeSettings.ProducerId(*m.ProducerId);
                }
                if (m.MessageGroupId) {
                    writeSettings.MessageGroupId(*m.MessageGroupId);
                }
                auto writeSession = topicClient.CreateSimpleBlockingWriteSession(writeSettings);

                UNIT_ASSERT(writeSession->Write(m.Message, m.SeqNo));
                writeSession->Close(TDuration::Seconds(1));
            }
        }

        {
            for (size_t attempt = 20; attempt--; ) {
                auto res = DoRead(session);
                Cerr << "Attempt=" << attempt << " count=" << res.first << Endl << Flush;
                if (res.first == Config.Messages.size()) {
                    const Ydb::ResultSet& proto = res.second;
                    for (size_t i = 0; i < Config.Expectations.size(); ++i) {
                        auto& c = Config.Expectations[i];
                        TString msg = TStringBuilder() << "Column '" << c.first << "': ";
                        c.second->Assert(msg, proto.rows(0).items(i));
                    }

                    break;
                }

                UNIT_ASSERT_C(attempt, "Unable to wait replication result");
                Sleep(TDuration::Seconds(1));
            }
        }
    }


    std::pair<ui64, Ydb::ResultSet> DoRead(TSession& s) {
        TStringBuilder columns;
        for (size_t i = 0; i < Config.Expectations.size(); ++i) {
            if (i) {
                columns << ", ";
            }
            columns << "`" << Config.Expectations[i].first << "`";
        }


        auto res = s.ExecuteQuery(
            Sprintf("SELECT %s FROM `%s`", columns.data(), TableName.data()),
                TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    
        const auto proto = NYdb::TProtoAccessor::GetProto(res.GetResultSet(0));
        return {proto.rowsSize(), proto};
    }

    TConfig Config;

    TString TopicName;
    TString TableName;
    TString TransferName;

    std::vector<std::string> ColumnNames;
};


} // namespace

Y_UNIT_TEST_SUITE(Transfer)
{
    Y_UNIT_TEST(Main_ColumnTable_KeyColumnFirst)
    {
        MainTestCase({
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

            .Expectations = {
                _C("Key", ui64(0)),
                _C("Message", TString("Message-1")),
            }
        }).Run();
    }

    Y_UNIT_TEST(Main_ColumnTable_KeyColumnLast)
    {
        MainTestCase({
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

            .Expectations = {
                _C("Key", ui64(0)),
                _C("Message", TString("Message-1")),
            }
        }).Run();
    }

    Y_UNIT_TEST(Main_ColumnTable_ComplexKey)
    {
        MainTestCase({
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

            .Expectations = {
                _C("Key1", ui64(1)),
                _C("Key2", ui64(2)),
                _C("Key3", ui64(3)),
                _C("Key4", ui64(4)),
                _C("Value1", TString("value-1")),
                _C("Value2", TString("value-2")),
            }
        }).Run();
    }

    Y_UNIT_TEST(Main_ColumnTable_JsonMessage)
    {
        MainTestCase({
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

            .Expectations = {
                _C("Id", ui64(1)),
                _C("FirstName", TString("Vasya")),
                _C("LastName", TString("Pupkin")),
                _C("Salary", ui64(123)),
            }
        }).Run();
    }

    Y_UNIT_TEST(Main_ColumnTable_NullableColumn)
    {
        MainTestCase({
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

            .Expectations = {
                _C("Key", ui64(0)),
                _C("Message", TString("Message-1")),
            }
        }).Run();
    }

    Y_UNIT_TEST(Main_ColumnTable_Date)
    {
        MainTestCase({
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

            .Expectations = {
                _C("Key", ui64(0)),
                _C("Message", TInstant::ParseIso8601("2025-02-21")),
            }
        }).Run();
    }

    Y_UNIT_TEST(Main_ColumnTable_Double)
    {
        MainTestCase({
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

            .Expectations = {
                _C("Key", ui64(0)),
                _C("Message", 1.23),
            }
        }).Run();
    }

    Y_UNIT_TEST(Main_ColumnTable_Utf8_Long)
    {
        MainTestCase({
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

            .Expectations = {
                _C("Key", ui64(0)),
                _C("Message", TString("Message-1 long value 0 1234567890 1 1234567890 2 1234567890 3 1234567890 4 1234567890 5 1234567890 6 1234567890")),
            }
        }).Run();
    }

    Y_UNIT_TEST(Main_MessageField_Partition)
    {
        MainTestCase({
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

            .Expectations = {
                _C("Partition", ui32(7)),
                _C("Message", TString("Message-1")),
            }
        }).Run();
    }

    Y_UNIT_TEST(Main_MessageField_SeqNo)
    {
        MainTestCase({
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

            .Expectations = {
                _C("SeqNo", ui64(13)),
            }
        }).Run();
    }

    Y_UNIT_TEST(Main_MessageField_ProducerId)
    {
        MainTestCase({
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

            .Expectations = {
                _C("ProducerId", TString("Producer-13")),
            }
        }).Run();
    }

    Y_UNIT_TEST(Main_MessageField_MessageGroupId)
    {
        MainTestCase({
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

            .Expectations = {
                _C("MessageGroupId", TString("MessageGroupId-13")),
            }
        }).Run();
    }

    Y_UNIT_TEST(AlterLambda)
    {
        MainTestCase({
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
                            Message:CAST($x._data || " old lambda" AS Utf8)
                        |>
                    ];
                };
            )",
    
            .Messages = {{"Message-1"}},
    
            .Expectations = {
                _C("Message", TString("Message-1 new lambda")),
            },

            .AlterLambda = R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data || " new lambda" AS Utf8)
                        |>
                    ];
                };
            )"
    
        }).Run();
    }


}

