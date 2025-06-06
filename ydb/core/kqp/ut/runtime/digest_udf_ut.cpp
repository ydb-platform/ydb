#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

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

// SPI-146280
Y_UNIT_TEST(JoinWithNonEquiMurMurHash) {
    NKikimrConfig::TAppConfig appCfg;
    TKikimrRunner kikimr(appCfg);

    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

    {
        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TableRight` (
                id Uint64,
                value Utf8,
                PRIMARY KEY (id)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    {
        auto result = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TableLeft` (
                hash_key Uint64,
                ref_id Uint64,
                extra Utf8,
                PRIMARY KEY (hash_key)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    auto db = kikimr.GetQueryClient();
    auto query = R"(
        SELECT r.* FROM `/Root/TableLeft` AS l
        INNER JOIN `/Root/TableRight` AS r ON l.ref_id = r.id
        WHERE l.hash_key = Digest::MurMurHash(Utf8("test"));
    )";

    auto explainMode = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), explainMode).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

    TString ast = *result.GetStats()->GetAst();
    Cerr << ast << Endl;

    UNIT_ASSERT_C(ast.Contains("Digest.MurMurHash"), "Digest::MurMurHash not found in AST");
    UNIT_ASSERT_C(!ast.Contains("EquiJoin"), "EquiJoin should not be used due to non-deterministic hash");
}

} // suite

} // namespace NKqp
} // namespace NKikimr
