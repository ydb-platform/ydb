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
    Y_UNIT_TEST_TWIN(Upsert, LogEnabled) {
        TKikimrSettings serverSettings;
        TStringStream ss;
        serverSettings.LogStream = &ss;
        TKikimrRunner kikimr(serverSettings);

        if (LogEnabled) {
            kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::DATA_INTEGRITY, NLog::PRI_TRACE);
        }
        
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
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: Executer"), LogEnabled ? 1 : 0);
        // check session actor logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY DEBUG: Component: SessionActor"), LogEnabled ? 2 : 0);
        // check grpc logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY TRACE: Component: Grpc"), LogEnabled ? 2 : 0);
        // check datashard logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: DataShard"), LogEnabled ? 2 : 0);
    }

    Y_UNIT_TEST(UpsertEvWrite) {
        NKikimrConfig::TAppConfig AppConfig;
        AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        TKikimrSettings serverSettings = TKikimrSettings().SetAppConfig(AppConfig);
        TStringStream ss;
        serverSettings.LogStream = &ss;
        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::DATA_INTEGRITY, NLog::PRI_TRACE);
        
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
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: Executer"), 2);
        // check session actor logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY DEBUG: Component: SessionActor"), 2);
        // check grpc logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY TRACE: Component: Grpc"), 2);
        // check datashard logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: DataShard"), 4);
    }

    Y_UNIT_TEST(Ddl) {
        TKikimrSettings serverSettings;
        TStringStream ss;
        serverSettings.LogStream = &ss;
        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::DATA_INTEGRITY, NLog::PRI_TRACE);
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
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: Executer"), 0);
        // check session actor logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY DEBUG: Component: SessionActor"), 0);
        // check grpc logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY TRACE: Component: Grpc"), 0);
        // check datashard logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: DataShard"), 0);
    }

    Y_UNIT_TEST(Select) {
        TKikimrSettings serverSettings;
        TStringStream ss;
        serverSettings.LogStream = &ss;
        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::DATA_INTEGRITY, NLog::PRI_TRACE);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1

            SELECT * FROM `/Root/KeyValue`;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        // check executer logs (should be empty, because executer only logs modification operations)
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: Executer"), 0);
        // check session actor logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY DEBUG: Component: SessionActor"), 2);
        // check grpc logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY TRACE: Component: Grpc"), 2);
        // check datashard logs (should be empty, because DataShard only logs modification operations)
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: DataShard"), 0);
    }

    Y_UNIT_TEST_TWIN(UpsertViaLegacyScripting, Streaming) {
        TKikimrSettings serverSettings;
        TStringStream ss;
        serverSettings.LogStream = &ss;
        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::DATA_INTEGRITY, NLog::PRI_TRACE);
        NYdb::NScripting::TScriptingClient client(kikimr.GetDriver());


        const auto query = R"(
            --!syntax_v1

            UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES
                (3u, "Value3"),
                (101u, "Value101"),
                (201u, "Value201");
        )";

        if (Streaming) {
            auto result = client.StreamExecuteYqlScript(query).GetValueSync();        
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CollectStreamResult(result);
        } else {
            auto result = client.ExecuteYqlScript(query).GetValueSync();        
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
            
        // check executer logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: Executer"), 1);
        // check session actor logs (should contain double logs because this query was executed via worker actor)
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY DEBUG: Component: SessionActor"), 4);
        // check grpc logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY TRACE: Component: Grpc"), 2);
        // check datashard logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: DataShard"), 2);

        Cout << ss.Str() << Endl;
    }
}

} // namespace NKqp
} // namespace NKikimr
