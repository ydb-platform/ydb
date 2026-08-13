#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

constexpr const char* TablePath = "/Root/UnsafeTruncateTable";

TKikimrRunner MakeRunner(bool enableUnsafeTruncate) {
    auto settings = TKikimrSettings().SetWithSampleTables(false);
    settings.FeatureFlags.SetEnableUnsafeTruncateTable(enableUnsafeTruncate);
    return TKikimrRunner(settings);
}

TString CountQuery() {
    return Sprintf("SELECT COUNT(*) AS cnt FROM `%s`;", TablePath);
}

TString UnsafeTruncateQuery() {
    return Sprintf("TRUNCATE TABLE `%s` WITH (unsafe = true);", TablePath);
}

ui64 ReadCount(const TExecuteQueryResult& result) {
    auto parser = result.GetResultSetParser(0);
    UNIT_ASSERT(parser.TryNextRow());
    return parser.ColumnParser("cnt").GetUint64();
}

void CreateAndFill(TSession& session) {
    auto create = session.ExecuteQuery(Sprintf(R"(
        CREATE TABLE `%s` (
            Key Uint64,
            Value String,
            PRIMARY KEY (Key)
        );
    )", TablePath), TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(create.GetStatus(), EStatus::SUCCESS, create.GetIssues().ToString());

    auto fill = session.ExecuteQuery(Sprintf(R"(
        UPSERT INTO `%s` (Key, Value) VALUES (1u, "one"), (2u, "two"), (3u, "three");
    )", TablePath), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(fill.GetStatus(), EStatus::SUCCESS, fill.GetIssues().ToString());
}

ui64 CountRows(TSession& session) {
    auto result = session.ExecuteQuery(CountQuery(), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return ReadCount(result);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpUnsafeTruncate) {

    Y_UNIT_TEST(DisabledByDefault) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ false);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS,
            "unsafe truncate must be rejected while the feature flag is off");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "disabled");

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 3u);
    }

    Y_UNIT_TEST(UnknownSettingRejected) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = session.ExecuteQuery(Sprintf(
            "TRUNCATE TABLE `%s` WITH (nonsense = true);", TablePath), TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unknown TRUNCATE TABLE setting");
    }

    // The plain statement still goes through SchemeShard exactly as before.
    Y_UNIT_TEST(PlainTruncateStillWorks) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = session.ExecuteQuery(Sprintf(
            "TRUNCATE TABLE `%s`;", TablePath), TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    Y_UNIT_TEST(WipesTable) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto result = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    // The point of the whole feature: the statement runs inside T_user without aborting it.
    Y_UNIT_TEST(InsideTransaction) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto before = session.ExecuteQuery(CountQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(before.GetStatus(), EStatus::SUCCESS, before.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReadCount(before), 3u);

        auto tx = before.GetTransaction();
        UNIT_ASSERT(tx);

        auto trunc = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto after = session.ExecuteQuery(CountQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(after.GetStatus(), EStatus::SUCCESS, after.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(ReadCount(after), 0u);

        auto commit = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    // Anomaly (a): T_trunc is committed on its own, so rolling T_user back does not bring rows back.
    Y_UNIT_TEST(SurvivesRollback) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session);

        auto before = session.ExecuteQuery(CountQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(before.GetStatus(), EStatus::SUCCESS, before.GetIssues().ToString());

        auto tx = before.GetTransaction();
        UNIT_ASSERT(tx);

        auto trunc = session.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        auto rollback = tx->Rollback().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(rollback.GetStatus(), EStatus::SUCCESS, rollback.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(CountRows(session), 0u);
    }

    // Anomaly (b): the effect is visible outside T_user before T_user commits.
    Y_UNIT_TEST(VisibleInConcurrentTransaction) {
        auto kikimr = MakeRunner(/* enableUnsafeTruncate */ true);
        auto client = kikimr.GetQueryClient();
        auto session1 = client.GetSession().GetValueSync().GetSession();
        auto session2 = client.GetSession().GetValueSync().GetSession();
        CreateAndFill(session1);

        auto before = session1.ExecuteQuery(CountQuery(), TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(before.GetStatus(), EStatus::SUCCESS, before.GetIssues().ToString());

        auto tx = before.GetTransaction();
        UNIT_ASSERT(tx);

        auto trunc = session1.ExecuteQuery(UnsafeTruncateQuery(), TTxControl::Tx(*tx)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(trunc.GetStatus(), EStatus::SUCCESS, trunc.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL_C(CountRows(session2), 0u,
            "the truncate must be visible to others while T_user is still open");

        auto commit = tx->Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), EStatus::SUCCESS, commit.GetIssues().ToString());
    }
}

} // namespace NKqp
} // namespace NKikimr
