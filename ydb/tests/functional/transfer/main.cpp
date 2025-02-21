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
ui64 Checker<ui64>::Get(const ::Ydb::Value& value) {
    return value.uint64_value();
}

template<>
TString Checker<TString>::Get(const ::Ydb::Value& value) {
    return value.text_value();
}

template<typename T>
std::pair<TString, std::shared_ptr<IChecker>> _C(TString&& name, T&& expected) {
    return {
        std::move(name),
        std::make_shared<Checker<T>>(std::move(expected))
    };
}

struct TConfig {
    const char* TableDDL;
    const char* Lambda;
    const char* Message;
    TVector<std::pair<TString, std::shared_ptr<IChecker>>> Columns;
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
                CREATE TOPIC `%s`;
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
            TWriteSessionSettings writeSettings;
            writeSettings.Path(TopicName);
            writeSettings.DeduplicationEnabled(false);
            auto writeSession = topicClient.CreateSimpleBlockingWriteSession(writeSettings);

            UNIT_ASSERT(writeSession->Write(Config.Message));
            writeSession->Close(TDuration::Seconds(1));
        }

        {
            for (size_t attempt = 20; attempt--; ) {
                auto res = DoRead(session);
                Cerr << "Attempt=" << attempt << " count=" << res.first << Endl << Flush;
                if (res.first == 1) {
                    const Ydb::ResultSet& proto = res.second;
                    for (size_t i = 0; i < Config.Columns.size(); ++i) {
                        auto& c = Config.Columns[i];
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
        for (size_t i = 0; i < Config.Columns.size(); ++i) {
            if (i) {
                columns << ", ";
            }
            columns << "`" << Config.Columns[i].first << "`";
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

            .Message = "Message-1",

            .Columns = {
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

            .Message = "Message-1",

            .Columns = {
                _C("Key", ui64(0)),
                _C("Message", TString("Message-1")),
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

            .Message = R"({
                "id": 1,
                "first_name": "Vasya",
                "last_name": "Pupkin",
                "salary": "123"
            })",

            .Columns = {
                _C("Id", ui64(1)),
                _C("FirstName", TString("Vasya")),
                _C("LastName", TString("Pupkin")),
                _C("Salary", ui64(123)),
            }
        }).Run();
    }

}

