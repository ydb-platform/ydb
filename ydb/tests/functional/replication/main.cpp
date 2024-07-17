#include <util/system/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <library/cpp/yson/writer.h>

#include <library/cpp/threading/local_executor/local_executor.h>

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

TString ReformatYson(const TString& yson) {
    TStringStream ysonInput(yson);
    TStringStream output;
    NYson::ReformatYsonStream(&ysonInput, &output, NYson::EYsonFormat::Text);
    return output.Str();
}

void CompareYson(const TString& expected, const TString& actual) {
    UNIT_ASSERT_NO_DIFF(ReformatYson(expected), ReformatYson(actual));
}

ui64 DoRead(TSession& s, const TString& table, ui64 expectedRows, const TString& expectedContent) {
    auto res = s.ExecuteDataQuery(
        Sprintf("SELECT * FROM `/local/%s`; SELECT COUNT(*) AS __count FROM `/local/%s`;",
            table.data(), table.data()), TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    auto rs = NYdb::TResultSetParser(res.GetResultSet(1));
    UNIT_ASSERT(rs.TryNextRow());
    auto count = rs.ColumnParser("__count").GetUint64();

    if (count == expectedRows) {
        auto yson = NYdb::FormatResultSetYson(res.GetResultSet(0));

        CompareYson(expectedContent, yson);
    }

    return count;
}

} // namespace

Y_UNIT_TEST_SUITE(Replication)
{
    Y_UNIT_TEST(UuidValue)
    {
        TString connectionString = GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE");
        Cerr << connectionString << Endl;
        auto config = TDriverConfig(connectionString);
        auto driver = TDriver(config);
        auto tableClient = TTableClient(driver);
        auto session = tableClient.GetSession().GetValueSync().GetSession();

        {
            auto res = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/local/ProducerUuidValue` (
                    Key Uint32,
                    Value1 Uuid,
                    Value2 Uuid NOT NULL,
                    PRIMARY KEY (Key)
                );
            )").GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        {
            auto sessionResult = tableClient.GetSession().GetValueSync();
            UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
            auto s = sessionResult.GetSession();

            {
                const TString query = "UPSERT INTO ProducerUuidValue (Key, Value1, Value2) VALUES"
                    "(1, "
                      "CAST(\"5b99a330-04ef-4f1a-9b64-ba6d5f44ea01\" as Uuid), "
                      "UNWRAP(CAST(\"5b99a330-04ef-4f1a-9b64-ba6d5f44ea02\" as Uuid)"
                    "));";
                auto res = s.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }

            {
                const TString query = Sprintf("CREATE ASYNC REPLICATION `replication` FOR"
                    "`ProducerUuidValue` AS `ConsumerUuidValue`"
                    "WITH ("
                        "CONNECTION_STRING = 'grpc://%s',"
                        "TOKEN = 'root@builtin'"
                    ");", connectionString.data());
                auto res = s.ExecuteSchemeQuery(query).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }
            // TODO: Make CREATE ASYNC REPLICATION to be a sync call
            Sleep(TDuration::Seconds(10));
        }

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.KeepInQueryCache(true);

        auto sessionResult = tableClient.GetSession().GetValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());

        auto s = sessionResult.GetSession();
        const TString expected = R"([[[1u];["5b99a330-04ef-4f1a-9b64-ba6d5f44ea01"];"5b99a330-04ef-4f1a-9b64-ba6d5f44ea02"]])";
        ui32 attempt = 10;
        while (1 != DoRead(s, "ConsumerUuidValue", 1, expected) && --attempt) {
            Sleep(TDuration::Seconds(1));
        }
        
        UNIT_ASSERT_C(attempt, "Unable to wait replication result");
    }
}

