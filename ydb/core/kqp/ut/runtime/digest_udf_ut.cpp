#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(DigestMurMurHash) {

Y_UNIT_TEST(CheckPlanContainsStrict) {
    NKikimrConfig::TAppConfig appCfg;
    TKikimrRunner kikimr(appCfg);

    auto db = kikimr.GetQueryClient();
    auto query = R"(
        SELECT Digest::MurMurHash("42");
    )";

    auto explainMode = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), explainMode).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    TString ast = *result.GetStats()->GetAst();
    Cerr << ast << Endl;

    UNIT_ASSERT_C(ast.Contains("Digest.MurMurHash"), "Digest::MurMurHash not found in AST");
    UNIT_ASSERT_C(ast.Contains("\"strict\""), "Strict flag not found in AST");
}

// TODO: Move to Join test
Y_UNIT_TEST(NoFullScanInJoin) {
    NKikimrConfig::TAppConfig appCfg;
    TKikimrRunner kikimr(appCfg);

    auto db = kikimr.GetTableClient();
    auto session = db.CreateSession().GetValueSync().GetSession();

    {
        auto res1 = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TableRight` (
                id Uint64,
                value Utf8,
                PRIMARY KEY (id)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(res1.IsSuccess(), res1.GetIssues().ToString());

        auto res2 = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TableLeft` (
                hash_key Uint64,
                ref_id Uint64,
                data Utf8,
                PRIMARY KEY (hash_key)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(res2.IsSuccess(), res2.GetIssues().ToString());
    }

    // Digest::MurMurHash(Utf8("target")) = 9488119898155926451
    const ui64 hashValue = 9488119898155926451ULL;

    {
        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            UPSERT INTO `/Root/TableRight` (id, value) VALUES
                (1, "one"), (2, "two"), (3, "three");
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        auto result2 = session.ExecuteDataQuery(Sprintf(R"(
            --!syntax_v1
            UPSERT INTO `/Root/TableLeft` (hash_key, ref_id, data) VALUES
                (%llu, 2, "match"),
                (%llu, 1, "no_match_1"),
                (%llu, 3, "no_match_2");
        )", hashValue, hashValue + 1, hashValue + 2), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result2.IsSuccess());
    }

    const TString query = R"(
        SELECT r.value FROM `/Root/TableLeft` AS l
        INNER JOIN `/Root/TableRight` AS r ON l.ref_id = r.id
        WHERE l.hash_key = Digest::MurMurHash(Utf8("target"));
    )";

    NYdb::NTable::TExecDataQuerySettings settings;
    settings.CollectQueryStats(ECollectQueryStatsMode::Profile);

    auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    const auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
    Cerr << stats.DebugString() << Endl;

    bool leftTableChecked = false;
    bool rightTableChecked = false;
    for (const auto& phase : stats.query_phases()) {
        for (const auto& access : phase.table_access()) {
            if (access.name() == "/Root/TableLeft") {
                UNIT_ASSERT_VALUES_EQUAL(access.reads().rows(), 1);
                leftTableChecked = true;
            }
            if (access.name() == "/Root/TableRight") {
                 UNIT_ASSERT_VALUES_EQUAL(access.reads().rows(), 1);
                rightTableChecked = true;
            }
        }
    }

    UNIT_ASSERT_C(leftTableChecked, "No reads found for /Root/TableLeft");
    UNIT_ASSERT_C(rightTableChecked, "No reads found for /Root/TableRight");
}

} // suite

} // namespace NKqp
} // namespace NKikimr
