#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <regex>

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
    Y_UNIT_TEST_QUAD(Upsert, LogEnabled, UseSink) {
        TStringStream ss;
        {
            NKikimrConfig::TAppConfig appConfig;
            appConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);
            TKikimrSettings serverSettings;
            serverSettings.SetAppConfig(appConfig);
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
        }

        if (UseSink) {
            // check write actor logs
            UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: WriteActor"), LogEnabled ? 1 : 0);
        } else {
            // check executer logs
            UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: Executer"), LogEnabled ? 2 : 0);
        }
        // check session actor logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY DEBUG: Component: SessionActor"), LogEnabled ? 2 : 0);
        // check grpc logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY TRACE: Component: Grpc"), LogEnabled ? 2 : 0);
        // check datashard logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: DataShard"), LogEnabled ? 2 : 0);
    }

    Y_UNIT_TEST_QUAD(UpsertEvWriteQueryService, isOlap, useOltpSink) {
        TStringStream ss;
        {
            NKikimrConfig::TAppConfig AppConfig;
            AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(useOltpSink);
            AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(isOlap);

            TKikimrSettings serverSettings = TKikimrSettings().SetAppConfig(AppConfig);
            serverSettings.LogStream = &ss;
            TKikimrRunner kikimr(serverSettings);
            kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::DATA_INTEGRITY, NLog::PRI_TRACE);

            auto db = kikimr.GetQueryClient();
            auto session = db.GetSession().GetValueSync().GetSession();

            {
                const TString query = Sprintf(R"(
                    CREATE TABLE `/Root/test_evwrite` (
                        Key Int64 NOT NULL,
                        Value String,
                        primary key (Key)
                    ) WITH (STORE=%s);
                )", isOlap ? "COLUMN" : "ROW");

                auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }

            ss.Clear();

            auto result = session.ExecuteQuery(R"(
                --!syntax_v1

                UPSERT INTO `/Root/test_evwrite` (Key, Value) VALUES
                    (3u, "Value3"),
                    (101u, "Value101"),
                    (201u, "Value201");
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        if (!isOlap) {
            if (useOltpSink) {
                // check write actor logs
                UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: WriteActor"), 1);
            } else {
                // check executer logs
                UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: Executer"), 2);
            }
            // check session actor logs
            UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY DEBUG: Component: SessionActor"), 2);
            // check grpc logs
            UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY TRACE: Component: Grpc"), 2);
            // check datashard logs
            UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: DataShard"), 2);
        } else {
            // check write actor logs
            UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: WriteActor"), 3);
            if (useOltpSink) {
                // check executer logs
                UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: Executer"), 1);
            } else {
                // check executer logs
                UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: Executer"), 11);
            }
            // check session actor logs
            UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY DEBUG: Component: SessionActor"), 2);
            // check grpc logs
            UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY TRACE: Component: Grpc"), 2);
            // check columnshard logs
            // ColumnShard doesn't have integrity logs.
            // UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: ColumnShard"), 6);
        }
    }

    Y_UNIT_TEST(Ddl) {
        TStringStream ss;
        {
            TKikimrSettings serverSettings;
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
        }

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
        TStringStream ss;
        {
            TKikimrSettings serverSettings;
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
        }

        // check executer logs (should be 1, because executer only logs result for read actor)
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: Executer"), 1);
        // check session actor logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY DEBUG: Component: SessionActor"), 2);
        // check grpc logs
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY TRACE: Component: Grpc"), 2);
        // check datashard logs (should be empty, because DataShard only logs modification operations)
        UNIT_ASSERT_VALUES_EQUAL(CountSubstr(ss.Str(), "DATA_INTEGRITY INFO: Component: DataShard"), 0);
    }

    Y_UNIT_TEST_TWIN(BrokenReadLock, UseSink) {
        TStringStream ss;
        {
            NKikimrConfig::TAppConfig AppConfig;
            AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);
            TKikimrSettings serverSettings;
            serverSettings.SetAppConfig(AppConfig);
            serverSettings.LogStream = &ss;
            TKikimrRunner kikimr(serverSettings);
            kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::DATA_INTEGRITY, NLog::PRI_TRACE);
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();

            std::optional<TTransaction> tx1;

            {  // tx1: read
                auto result = session.ExecuteDataQuery(R"(
                    --!syntax_v1

                    SELECT * FROM `/Root/KeyValue` WHERE Key = 1u OR Key = 2u;
                )", TTxControl::BeginTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([
                    [[1u];["One"]];
                    [[2u];["Two"]]
                ])", FormatResultSetYson(result.GetResultSet(0)));
                tx1 = result.GetTransaction();
                UNIT_ASSERT(tx1);
            }

            {  // tx2: write + commit
                auto result = session.ExecuteDataQuery(R"(
                    --!syntax_v1

                    UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES
                        (1u, "NewValue1");
                )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }

            {  // tx1: commit
                auto result = tx1->Commit().ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
        }

        auto logRows = SplitString(ss.Str(), "DATA_INTEGRITY");
        std::string readLock;
        std::string brokenLock;
        for (const auto& row : logRows) {
            // we need to find row with info about read physical tx and extract lock id
            if (row.Contains("Component: Executer,Type: InputActorResult")) {
                std::regex lockIdRegex(R"(LockId:\s*(\d+))");
                std::smatch lockIdMatch;
                UNIT_ASSERT_C(std::regex_search(row.data(), lockIdMatch, lockIdRegex) || lockIdMatch.size() != 2, "failed to extract read lock id");
                readLock = lockIdMatch[1].str();
            }

            // we need to find row with info about broken locks and extract lock id
            if (row.Contains("Component: DataShard,Type: Locks")) {
                std::regex lockIdRegex(R"(BreakLocks:\s*\[(\d+)\s*\])");
                std::smatch lockIdMatch;
                UNIT_ASSERT_C(std::regex_search(row.data(), lockIdMatch, lockIdRegex) || lockIdMatch.size() != 2, "failed to extract broken lock id");
                brokenLock = lockIdMatch[1].str();
            } 
        }

        UNIT_ASSERT_C(!readLock.empty() && readLock == brokenLock, "read lock should be broken");
    }

    Y_UNIT_TEST(BrokenReadLockAbortedTx) {
        TStringStream ss;
        {
            TKikimrSettings serverSettings;
            serverSettings.LogStream = &ss;
            TKikimrRunner kikimr(serverSettings);
            kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::DATA_INTEGRITY, NLog::PRI_TRACE);
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();

            std::optional<TTransaction> tx1;

            {  // tx1: read
                auto result = session.ExecuteDataQuery(R"(
                    --!syntax_v1

                    SELECT * FROM `/Root/KeyValue` WHERE Key = 1u OR Key = 2u;
                )", TTxControl::BeginTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([
                    [[1u];["One"]];
                    [[2u];["Two"]]
                ])", FormatResultSetYson(result.GetResultSet(0)));
                tx1 = result.GetTransaction();
                UNIT_ASSERT(tx1);
            }

            {  // tx2: write + commit
                auto result = session.ExecuteDataQuery(R"(
                    --!syntax_v1

                    UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES
                        (1u, "NewValue1");
                )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }

            {  // tx1: write + commit
                auto result = session.ExecuteDataQuery(R"(
                    --!syntax_v1

                    UPSERT INTO `/Root/KeyValue` (Key, Value) VALUES
                        (1000u, "Value1000");
                )", TTxControl::Tx(*tx1).CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
            }
        }

        auto logRows = SplitString(ss.Str(), "DATA_INTEGRITY");
        std::string readLock;
        std::string brokenLock;
        for (const auto& row : logRows) {
            // we need to find row with info about read physical tx and extract lock id
            if (row.Contains("Component: Executer,Type: InputActorResult")) {
                std::regex lockIdRegex(R"(LockId:\s*(\d+))");
                std::smatch lockIdMatch;
                UNIT_ASSERT_C(std::regex_search(row.data(), lockIdMatch, lockIdRegex) || lockIdMatch.size() != 2, "failed to extract read lock id");
                readLock = lockIdMatch[1].str();
            }

            // we need to find row with info about broken locks and extract lock id
            if (row.Contains("Component: DataShard,Type: Locks")) {
                std::regex lockIdRegex(R"(BreakLocks:\s*\[(\d+)\s*\])");
                std::smatch lockIdMatch;
                UNIT_ASSERT_C(std::regex_search(row.data(), lockIdMatch, lockIdRegex) || lockIdMatch.size() != 2, "failed to extract broken lock id");
                brokenLock = lockIdMatch[1].str();
            } 
        }

        UNIT_ASSERT_C(!readLock.empty() && readLock == brokenLock, "read lock should be broken");
    }
}

} // namespace NKqp
} // namespace NKikimr
