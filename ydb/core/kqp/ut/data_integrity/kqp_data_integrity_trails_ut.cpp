#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <regex>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

    void CheckRegexMatch(
        const TString& str,
        const TVector<std::pair<TString, ui64>>& regexToMatchCount)
    {
        for (auto& [regexString, expectedMatchCount]: regexToMatchCount) {
            std::regex expression(regexString.c_str());

            auto matchCount = std::distance(
                std::sregex_iterator(str.begin(), str.end(), expression),
                std::sregex_iterator());

            UNIT_ASSERT_VALUES_EQUAL(expectedMatchCount, matchCount);
        }
    }

    TString ConstructRegexToCheckLogs(
        const TString& logLevel,
        const TString& component)
    {
        TStringBuilder builder;

        // [\\w]+\\.[A-Za-z]+:[0-9]+ match filename and line number
        builder << "DATA_INTEGRITY " << logLevel
                << ": [\\w]+\\.[A-Za-z]+:[0-9]+: Component: " << component;
        return builder;
    }

} // namespace

Y_UNIT_TEST_SUITE(KqpDataIntegrityTrails) {
    Y_UNIT_TEST_QUAD(Upsert, LogEnabled, UseSink) {
        TStringStream ss;
        {
            TKikimrSettings serverSettings;
            serverSettings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);
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

        TVector<std::pair<TString, ui64>> regexToMatchCount{
            // check session actor logs
            {ConstructRegexToCheckLogs("DEBUG", "SessionActor"),
             LogEnabled ? 2 : 0},
            // check grpc logs
            {ConstructRegexToCheckLogs("TRACE", "Grpc"), LogEnabled ? 2 : 0},
            // check datashard logs
            {ConstructRegexToCheckLogs("INFO", "DataShard"), LogEnabled ? 2 : 0},
        };

        if (UseSink) {
            // check write actor logs
            regexToMatchCount.emplace_back(
                ConstructRegexToCheckLogs("INFO", "WriteActor"),
                LogEnabled ? 1 : 0);
        } else {
            // check executer logs
            regexToMatchCount.emplace_back(
                ConstructRegexToCheckLogs("INFO", "Executer"),
                LogEnabled ? 2 : 0);
        }

        CheckRegexMatch(ss.Str(), regexToMatchCount);
    }

    Y_UNIT_TEST_QUAD(UpsertEvWriteQueryService, isOlap, useOltpSink) {
        TStringStream ss;
        {
            TKikimrSettings serverSettings;
            serverSettings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(useOltpSink);
            serverSettings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(isOlap);
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

            auto result = session.ExecuteQuery(R"(
                --!syntax_v1

                UPSERT INTO `/Root/test_evwrite` (Key, Value) VALUES
                    (3u, "Value3"),
                    (101u, "Value101"),
                    (201u, "Value201");
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        TVector<std::pair<TString, ui64>> regexToMatchCount;
        if (!isOlap) {
            regexToMatchCount = {
                {ConstructRegexToCheckLogs("DEBUG", "SessionActor"), 2 + 2},
                {ConstructRegexToCheckLogs("TRACE", "Grpc"), 2 + 2},
                {ConstructRegexToCheckLogs("INFO", "DataShard"), 2},
            };

            if (useOltpSink) {
                // check write actor logs
                regexToMatchCount.emplace_back(
                    ConstructRegexToCheckLogs("INFO", "WriteActor"),
                    1);
            } else {
                // check executer logs
                regexToMatchCount.emplace_back(
                    ConstructRegexToCheckLogs("INFO", "Executer"),
                    2);
            }
        } else {
            regexToMatchCount = {
                {ConstructRegexToCheckLogs("INFO", "WriteActor"), 3},
                {ConstructRegexToCheckLogs("DEBUG", "SessionActor"), 2 + 2},
                {ConstructRegexToCheckLogs("TRACE", "Grpc"), 2 + 2},
                {ConstructRegexToCheckLogs("INFO", "Executer"),
                 useOltpSink ? 1 : 11}};

            // ColumnShard doesn't have integrity logs.
        }

        CheckRegexMatch(ss.Str(), regexToMatchCount);
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

        TVector<std::pair<TString, ui64>> regexToMatchCount{
            {ConstructRegexToCheckLogs("INFO", "Executer"), 0},
            {ConstructRegexToCheckLogs("DEBUG", "SessionActor"), 0},
            {ConstructRegexToCheckLogs("TRACE", "Grpc"), 0},
            {ConstructRegexToCheckLogs("INFO", "DataShard"), 0},
        };

        CheckRegexMatch(ss.Str(), regexToMatchCount);
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

        TVector<std::pair<TString, ui64>> regexToMatchCount{
            // check executer logs (should be 1, because executer only logs
            // result for read actor)
            {ConstructRegexToCheckLogs("INFO", "Executer"), 1},
            // check session actor logs
            {ConstructRegexToCheckLogs("DEBUG", "SessionActor"), 2},
            // check grpc logs
            {ConstructRegexToCheckLogs("TRACE", "Grpc"), 2},
            // check datashard logs (should be empty, because DataShard only
            // logs modification operations)
            {ConstructRegexToCheckLogs("INFO", "DataShard"), 0},
        };

        CheckRegexMatch(ss.Str(), regexToMatchCount);
    }

    Y_UNIT_TEST_TWIN(BrokenReadLock, UseSink) {
        TStringStream ss;
        {
            TKikimrSettings serverSettings;
            serverSettings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);
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
            if (row.Contains("Component: Executer, Type: InputActorResult")) {
                std::regex lockIdRegex(R"(LockId:\s*(\d+))");
                std::smatch lockIdMatch;
                UNIT_ASSERT_C(std::regex_search(row.data(), lockIdMatch, lockIdRegex) || lockIdMatch.size() != 2, "failed to extract read lock id");
                readLock = lockIdMatch[1].str();
            }

            // we need to find row with info about broken locks and extract lock id
            if (row.Contains("Component: DataShard, Type: Locks")) {
                std::regex lockIdRegex(R"(BrokenLocks:\s*\[(\d+)\s*\])");
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
            serverSettings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(false);
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

        // Verify that the abort was logged correctly
        auto logRows = SplitString(ss.Str(), "DATA_INTEGRITY");
        bool foundAbortLog = false;
        bool foundInputActorResult = false;
        for (const auto& row : logRows) {
            // Check for InputActorResult log (tx1's read acquiring a lock)
            if (row.Contains("Component: Executer, Type: InputActorResult")) {
                foundInputActorResult = true;
            }

            // Check for the abort response log
            if (row.Contains("Status: ABORTED") && row.Contains("Transaction locks invalidated")) {
                foundAbortLog = true;
            }
        }

        UNIT_ASSERT_C(foundInputActorResult, "InputActorResult log should be present for read tx");
        UNIT_ASSERT_C(foundAbortLog, "ABORTED status with lock invalidation message should be logged");
    }
}

} // namespace NKqp
} // namespace NKikimr
