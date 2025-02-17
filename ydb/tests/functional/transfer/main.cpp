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

std::pair<ui64, Ydb::ResultSet> DoRead(TSession& s, const TString& table) {
    auto res = s.ExecuteQuery(
        Sprintf("SELECT Key, Message FROM `/local/%s`  ORDER BY Key", table.data()),
            TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

    const auto proto = NYdb::TProtoAccessor::GetProto(res.GetResultSet(0));

    return {proto.rowsSize(), proto};
}

} // namespace

Y_UNIT_TEST_SUITE(Transfer)
{
    void Main(const TString& transferName, const TString& tableName, const TString& tableDDL, const TString& topicName)
    {
        TString connectionString = GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE");
        Cerr << ">>>>> connectionString = " << connectionString << Endl << Flush;

        auto config = TDriverConfig(connectionString);
        auto driver = TDriver(config);
        auto tableClient = TQueryClient(driver);
        auto session = tableClient.GetSession().GetValueSync().GetSession();
        auto topicClient = TTopicClient(driver);

        {
            auto res = session.ExecuteQuery(tableDDL, TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        {
            auto res = session.ExecuteQuery(Sprintf(R"(
                CREATE TOPIC `%s`;
            )", topicName.data()), TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        {
            auto res = session.ExecuteQuery(Sprintf(R"(
                $l = ($x) -> {
                    return [
                        <|
                            Key:CAST($x._offset AS Uint64),
                            Message:CAST($x._data AS Utf8)
                        |>
                    ];
                };

                CREATE TRANSFER `%s`
                FROM `%s` TO `%s` USING $l
                WITH (
                    CONNECTION_STRING = 'grpc://%s'
                    -- , TOKEN = 'user@builtin'
                );
            )", transferName.data(), topicName.data(), tableName.data(), connectionString.data()), TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        {
            TWriteSessionSettings writeSettings;
            writeSettings.Path(topicName);
            writeSettings.DeduplicationEnabled(false);
            auto writeSession = topicClient.CreateSimpleBlockingWriteSession(writeSettings);

            UNIT_ASSERT(writeSession->Write("message-1"));
            writeSession->Close(TDuration::Seconds(1));
        }

        {
            for (size_t attempt = 20; attempt--; ) {
                auto res = DoRead(session, tableName);
                Cerr << "Attempt=" << attempt << " count=" << res.first << Endl << Flush;
                if (res.first == 1) {
                    const Ydb::ResultSet& proto = res.second;
                    UNIT_ASSERT_VALUES_EQUAL(proto.rows(0).items(0).uint64_value(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(proto.rows(0).items(1).text_value(), "message-1");

                    break;
                }

                UNIT_ASSERT_C(attempt, "Unable to wait replication result");
                Sleep(TDuration::Seconds(1));
            }
        }
    }

    Y_UNIT_TEST(Main_ColumnTable)
    {
        Main("ColumnTransfer1", "TargetColumnTable1", R"(
            CREATE TABLE `/local/TargetColumnTable1` (
                Key Uint64 NOT NULL,
                Message Utf8 NOT NULL,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = COLUMN
            );
        )", "SourceTopic1");
    }

    Y_UNIT_TEST(Main_ColumnTable_KeyColumnOrder)
    {
        Main("ColumnTransfer2", "TargetColumnTable2", R"(
            CREATE TABLE `/local/TargetColumnTable2` (
                Message Utf8 NOT NULL,
                Key Uint64 NOT NULL,
                PRIMARY KEY (Key)
            )  WITH (
                STORE = COLUMN
            );
        )", "SourceTopic2");
    }
}

