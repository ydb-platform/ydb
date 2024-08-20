#include <util/system/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>

#include <library/cpp/threading/local_executor/local_executor.h>

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

std::pair<ui64, Ydb::ResultSet> DoRead(TSession& s, const TString& table) {
    auto res = s.ExecuteDataQuery(
        Sprintf("SELECT * FROM `/local/%s`; SELECT COUNT(*) AS __count FROM `/local/%s`;",
            table.data(), table.data()), TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    auto rs = NYdb::TResultSetParser(res.GetResultSet(1));
    UNIT_ASSERT(rs.TryNextRow());
    auto count = rs.ColumnParser("__count").GetUint64();

    const auto proto = NYdb::TProtoAccessor::GetProto(res.GetResultSet(0));
    return {count, proto};
}

} // namespace

Y_UNIT_TEST_SUITE(Replication)
{
    Y_UNIT_TEST(Types)
    {
        TString connectionString = GetEnv("YDB_ENDPOINT") + "/?database=" + GetEnv("YDB_DATABASE");
        auto config = TDriverConfig(connectionString);
        auto driver = TDriver(config);
        auto tableClient = TTableClient(driver);
        auto session = tableClient.GetSession().GetValueSync().GetSession();

        {
            auto res = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/local/ProducerUuidValue` (
                    Key Uint32,
                    Key2 Uuid,
                    v01 Uuid,
                    v02 Uuid NOT NULL,
                    v03 Double,
                    PRIMARY KEY (Key, Key2)
                );
            )").GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        {
            auto sessionResult = tableClient.GetSession().GetValueSync();
            UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
            auto s = sessionResult.GetSession();

            {
                const TString query = "UPSERT INTO ProducerUuidValue (Key,Key2,v01,v02,v03) VALUES"
                    "(1, "
                      "CAST(\"5b99a330-04ef-4f1a-9b64-ba6d5f44ea00\" as Uuid), "
                      "CAST(\"5b99a330-04ef-4f1a-9b64-ba6d5f44ea01\" as Uuid), "
                      "UNWRAP(CAST(\"5b99a330-04ef-4f1a-9b64-ba6d5f44ea02\" as Uuid)), "
                      "CAST(\"311111111113.222222223\" as Double) "
                    ");";
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
        TUuidValue expectedKey2("5b99a330-04ef-4f1a-9b64-ba6d5f44ea00");
        TUuidValue expectedV1("5b99a330-04ef-4f1a-9b64-ba6d5f44ea01");
        TUuidValue expectedV2("5b99a330-04ef-4f1a-9b64-ba6d5f44ea02");
        double expectedV3 = 311111111113.222222223;
        ui32 attempt = 10;
        while (--attempt) {
            auto res = DoRead(s, "ConsumerUuidValue");
            if (res.first == 1) {
                const Ydb::ResultSet& proto = res.second;
                UNIT_ASSERT_VALUES_EQUAL(proto.rows(0).items(0).uint32_value(), 1);
                UNIT_ASSERT_VALUES_EQUAL(proto.rows(0).items(1).low_128(), expectedKey2.Buf_.Halfs[0]);
                UNIT_ASSERT_VALUES_EQUAL(proto.rows(0).items(1).high_128(), expectedKey2.Buf_.Halfs[1]);
                UNIT_ASSERT_VALUES_EQUAL(proto.rows(0).items(2).low_128(), expectedV1.Buf_.Halfs[0]);
                UNIT_ASSERT_VALUES_EQUAL(proto.rows(0).items(2).high_128(), expectedV1.Buf_.Halfs[1]);
                UNIT_ASSERT_VALUES_EQUAL(proto.rows(0).items(3).low_128(), expectedV2.Buf_.Halfs[0]);
                UNIT_ASSERT_VALUES_EQUAL(proto.rows(0).items(3).high_128(), expectedV2.Buf_.Halfs[1]);
                UNIT_ASSERT_DOUBLES_EQUAL(proto.rows(0).items(4).double_value(), expectedV3, 0.0001);
                break;
            }
            Sleep(TDuration::Seconds(1));
        }
        
        UNIT_ASSERT_C(attempt, "Unable to wait replication result");
    }
}

