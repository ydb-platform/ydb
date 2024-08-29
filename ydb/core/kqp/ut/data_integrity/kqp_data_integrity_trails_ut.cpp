#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {
    ui64 CountSubstr(const TString& str, const TString& substr) {
        ui64 count = 0;
        for (auto pos = str.find(substr); pos != TString::npos; pos = str.find(substr, pos + substr.size())) {
            ++count;
        }
        return count;
    }
}

Y_UNIT_TEST_SUITE(KqpDataIntegrityTrails) {
    Y_UNIT_TEST(Upsert) {
        TKikimrSettings serverSettings;
        TStringStream ss;
        serverSettings.LogStream = &ss;
        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::DATA_INTEGRITY, NLog::PRI_DEBUG);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES
                (3u, "Value3"),
                (101u, "Value101"),
                (201u, "Value201");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        // check executer logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY NOTICE: Component: Executer"), 1);
        // check session actor logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: SessionActor"), 2);
        // check grpc logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY DEBUG: Component: Grpc"), 2);
        // TODO: check datashard logs
    }

    Y_UNIT_TEST(Ddl) {
        TKikimrSettings serverSettings;
        TStringStream ss;
        serverSettings.LogStream = &ss;
        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::DATA_INTEGRITY, NLog::PRI_DEBUG);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"(
            --!syntax_v1
            
            CREATE TABLE `/Root/Tmp` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        // check executer logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY NOTICE: Component: Executer"), 0);
        // check session actor logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: SessionActor"), 0);
        // check grpc logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY DEBUG: Component: Grpc"), 0);
        // TODO: check datashard logs
    }

    Y_UNIT_TEST(Select) {
        TKikimrSettings serverSettings;
        TStringStream ss;
        serverSettings.LogStream = &ss;
        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::DATA_INTEGRITY, NLog::PRI_DEBUG);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            SELECT * FROM `/Root/KeyValue`;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        // check executer logs (should be empty, because executer logs only modification operations)
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY NOTICE: Component: Executer"), 0);
        // check session actor logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: SessionActor"), 2);
        // check grpc logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY DEBUG: Component: Grpc"), 2);
        // TODO: check datashard logs
    }
}

} // namespace NKqp
} // namespace NKikimr
