#include <util/system/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/table/table.h>
#include <ydb-cpp-sdk/client/topic/client.h>
#include <ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb-cpp-sdk/client/draft/ydb_scripting.h>

#include <library/cpp/threading/local_executor/local_executor.h>

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NTopic;

namespace {

std::pair<ui64, Ydb::ResultSet> DoRead(TSession& s, const TString& table) {
    /*
    auto resCount = s.ExecuteDataQuery(
        Sprintf("SELECT COUNT(*) AS __count FROM `/local/%s`;", table.data()),
            TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(resCount.IsSuccess(), resCount.GetIssues().ToString());
    auto rsCount = NYdb::TResultSetParser(resCount.GetResultSet(0));
    UNIT_ASSERT(rsCount.TryNextRow());
    auto count = rsCount.ColumnParser("__count").GetUint64();
    */

    auto res = s.ExecuteDataQuery(
        Sprintf("SELECT * FROM `/local/%s`  ORDER BY Key", table.data()),
            TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW())).GetValueSync();
    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

    const auto proto = NYdb::TProtoAccessor::GetProto(res.GetResultSet(0));
    return {0, proto};
}

} // namespace

Y_UNIT_TEST_SUITE(Transfer)
{
    Y_UNIT_TEST(Main_ColumnTable)
    {
        TString connectionString = GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE");
        auto config = TDriverConfig(connectionString);
        auto driver = TDriver(config);
        auto tableClient = TTableClient(driver, TClientSettings().UseQueryCache(false));
        auto session = tableClient.GetSession().GetValueSync().GetSession();
        auto topicClient = TTopicClient(driver);
        auto writeSession = topicClient.CreateSimpleBlockingWriteSession(TWriteSessionSettings("/local/SourceTopic", "producer-1", "producer-1"));

        {
            auto res = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/local/TargetColumnTable` (
                    Key Uint64 NOT NULL,
                    Message Utf8,
                    PRIMARY KEY (Key)
                )  WITH (
                    STORE = COLUMN
                );
            )").GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        {
            auto res = session.ExecuteSchemeQuery(R"(
                CREATE TOPIC `/local/SourceTopic`;
            )").GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        {
            auto res = session.ExecuteSchemeQuery(Sprintf(R"(
                $l = ($x) -> {
                    return [
                        <| Key:$x._offset, Message:$x._data |>
                    ];
                };

                CREATE TRANSFER `ColumnTransfer`
                FROM `SourceTopic` TO `TargetColumnTable` USING $l
                WITH (
                    CONNECTION_STRING = 'grpc://%s',
                    TOKEN = 'user@builtin'
                );
            )", connectionString.data())).GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        session.Close().GetValueSync();
        session = tableClient.GetSession().GetValueSync().GetSession();

        {
            writeSession->Write("message-1");
        }

        {
            for (size_t attempt = 10; attempt-- ; ) {
                auto res = DoRead(session, "TargetColumnTable");
                if (res.first == 1) {
                    const Ydb::ResultSet& proto = res.second;
                    UNIT_ASSERT_VALUES_EQUAL(proto.rows(0).items(0).uint64_value(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(proto.rows(0).items(1).text_value(), "message-1");
                }

                UNIT_ASSERT_C(attempt, "Unable to wait replication result");
                Sleep(TDuration::Seconds(1));
            }
        }
    }
}

