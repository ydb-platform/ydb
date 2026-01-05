#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_tli.h>

#include <regex>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {
} // namespace

Y_UNIT_TEST_SUITE(KqpTli) {


    Y_UNIT_TEST_QUAD(QueryMetricsLocksBroken, LogEnabled, UseSink) {
        TStringStream ss;
        TKikimrSettings serverSettings;
        serverSettings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);
        serverSettings.LogStream = &ss;
        TKikimrRunner kikimr(serverSettings);

        if (LogEnabled) {
            kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TLI, NLog::PRI_INFO);
        }

        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        auto victimSession = client.CreateSession().GetValueSync().GetSession();

        // Create table and insert initial data
        NKqp::AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/Tenant1/TableLocks` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync());

        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"Initial\")",
            TTxControl::BeginTx().CommitTx()
        ).GetValueSync());

        // Establish locks by reading in a transaction (victim)
        std::optional<TTransaction> victimTx;
        while (!victimTx) {
            auto result = victimSession.ExecuteDataQuery(
                "SELECT * FROM `/Root/Tenant1/TableLocks` WHERE Key = 1u /* victim-query */",
                TTxControl::BeginTx()
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

            TString yson = FormatResultSetYson(result.GetResultSet(0));
            if (yson == "[]") {
                continue;  // Data not visible yet, retry
            }

            victimTx = result.GetTransaction();
            UNIT_ASSERT(victimTx);
        }

        // Breaker transaction: writes to key 1, breaking victim's read lock
        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"BreakerValue\") /* lock-breaker */",
            TTxControl::BeginTx().CommitTx()
        ).GetValueSync());

        // Victim tries to commit with write to the same key
        // This triggers lock validation, which fails because the lock on key 1 was broken
        auto commitResult = victimSession.ExecuteDataQuery(
            "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"VictimValue\") /* victim-commit */",
            TTxControl::Tx(*victimTx).CommitTx()
        ).ExtractValueSync();

        // Victim should be ABORTED because its locks were broken
        UNIT_ASSERT_VALUES_EQUAL(commitResult.GetStatus(), EStatus::ABORTED);

        TVector<std::pair<TString, ui64>> regexToMatchCount{
            // check session actor logs
            {NTestTli::ConstructRegexToCheckLogs("INFO", "SessionActor", "Query had broken other locks"), LogEnabled ? 1 : 0},
        };

        NTestTli::CheckRegexMatch(ss.Str(), regexToMatchCount);
    }

    Y_UNIT_TEST_QUAD(QueryMetricsLocksBrokenSeparateCommit, LogEnabled, UseSink) {
        TStringStream ss;
        TKikimrSettings serverSettings;
        serverSettings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);
        serverSettings.LogStream = &ss;
        TKikimrRunner kikimr(serverSettings);

        if (LogEnabled) {
            kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::TLI, NLog::PRI_INFO);
        }

        auto client = kikimr.GetTableClient();
        auto session = client.CreateSession().GetValueSync().GetSession();
        auto victimSession = client.CreateSession().GetValueSync().GetSession();

        // Create table and insert initial data
        NKqp::AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/Tenant1/TableLocks` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync());

        NKqp::AssertSuccessResult(session.ExecuteDataQuery(
            "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"Initial\")",
            TTxControl::BeginTx().CommitTx()
        ).GetValueSync());

        // Establish locks by reading in a transaction (victim)
        std::optional<TTransaction> victimTx;
        while (!victimTx) {
            auto result = victimSession.ExecuteDataQuery(
                "SELECT * FROM `/Root/Tenant1/TableLocks` WHERE Key = 1u /* victim-query */",
                TTxControl::BeginTx()
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());

            TString yson = FormatResultSetYson(result.GetResultSet(0));
            if (yson == "[]") {
                continue;  // Data not visible yet, retry
            }

            victimTx = result.GetTransaction();
            UNIT_ASSERT(victimTx);
        }

        // Breaker transaction: writes to key 1 and makes an empty final commit using separate operations
        std::optional<TTransaction> breakerTx;
        {
            auto result = session.ExecuteDataQuery(
                "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"BreakerValue\") /* lock-breaker */",
                TTxControl::BeginTx()
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
            breakerTx = result.GetTransaction();
            UNIT_ASSERT(breakerTx);
        }

        // (Optional) Non-breaker query: writes to key 2
        {
            auto result = session.ExecuteDataQuery(
                "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (2u, \"UsualValue\") /* non-breaker */",
                TTxControl::Tx(*breakerTx)
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // Separate COMMIT should break the lock
        {
            auto result = breakerTx->Commit().ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // Victim tries to commit with write to the same key
        // This triggers lock validation, which fails because the lock on key 1 was broken
        auto victimCommitResult = victimSession.ExecuteDataQuery(
            "UPSERT INTO `/Root/Tenant1/TableLocks` (Key, Value) VALUES (1u, \"VictimValue\") /* victim-commit */",
            TTxControl::Tx(*victimTx).CommitTx()
        ).ExtractValueSync();

        // Victim should be ABORTED because its locks were broken
        UNIT_ASSERT_VALUES_EQUAL(victimCommitResult.GetStatus(), EStatus::ABORTED);

        TVector<std::pair<TString, ui64>> regexToMatchCount{
            // check session actor logs
            {NTestTli::ConstructRegexToCheckLogs("INFO", "SessionActor", "Commit had broken other locks"), LogEnabled ? 1 : 0},
        };

        NTestTli::CheckRegexMatch(ss.Str(), regexToMatchCount);
    }

}

} // namespace NKqp
} // namespace NKikimr
