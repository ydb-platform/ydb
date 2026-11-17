#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <optional>
#include <utility>

namespace NKikimr::NKqp {
namespace {

using namespace NYdb;
using namespace NYdb::NQuery;

void CheckSortedRead(bool newRbo, bool separated) {
    TKikimrSettings settings;
    settings.SetWithSampleTables(false);
    settings.SetUseRealThreads(false);
    settings.SetColumnShardReaderClassName("TRIVIAL");
    settings.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(newRbo);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(false);
    settings.FeatureFlags.SetEnableSeparationComputeActorsFromRead(separated);
    auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
    controller->SetCompactionControl(NYDBTest::EOptimizerCompactionWeightControl::Disable);
    TKikimrRunner kikimr(settings);
    auto session = kikimr.RunCall([&] { return kikimr.GetQueryClient().GetSession().GetValueSync().GetSession(); });
    auto execute = [&](const TString& query, const TTxControl& tx,
                       const TExecuteQuerySettings& querySettings = TExecuteQuerySettings()) {
        return kikimr.RunCall([&] { return session.ExecuteQuery(query, tx, querySettings).GetValueSync(); });
    };
    auto ddl = execute(R"(
        CREATE TABLE `/Root/SortedRead` (
            k Uint64 NOT NULL,
            subkey Uint64 NOT NULL,
            value Int32 NOT NULL,
            PRIMARY KEY (k, subkey)
        ) PARTITION BY HASH(k) WITH (
            STORE = COLUMN,
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4
        );
    )", TTxControl::NoTx());
    UNIT_ASSERT_C(ddl.IsSuccess(), ddl.GetIssues().ToString());

    // Interleaved keys in several portions on each shard; insertion order is not PK order.
    for (ui32 portion = 0; portion < 4; ++portion) {
        TStringBuilder query;
        query << "INSERT INTO `/Root/SortedRead` (k, subkey, value) VALUES ";
        bool first = true;
        for (ui32 k = portion; k < 32; k += 4) {
            for (ui32 subkey : {1, 0}) {
                query << (std::exchange(first, false) ? "" : ",")
                    << "(" << k << "u," << subkey << "u," << (100 - 2 * k - subkey) << ")";
            }
        }
        query << ";";
        auto result = execute(query, TTxControl::BeginTx().CommitTx());
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    std::atomic<ui32> sortedScans{0};
    std::atomic<ui32> missingDirections{0};
    auto* runtime = kikimr.GetTestServer().GetRuntime();
    auto observer = runtime->AddObserver<TEvDataShard::TEvKqpScan>([&](TEvDataShard::TEvKqpScan::TPtr& ev) {
        const auto& request = ev->Get()->Record;
        if (request.HasCSScanPolicy()) {
            ++sortedScans;
            if (!request.HasReverse()) {
                ++missingDirections;
            }
        }
    });

    auto run = [&](const TString& sql, std::optional<bool> enabled, bool expectSorted, bool blocks) {
        TStringBuilder query;
        if (enabled) {
            query << "PRAGMA kikimr.OptEnableOlapSortedRead = \"" << (*enabled ? "true" : "false") << "\";\n";
        }
        if (!blocks) {
            query << "PRAGMA ydb.DisableBlockExecution;\n";
        }
        query << sql;
        sortedScans = 0;
        missingDirections = 0;
        auto result = execute(query, TTxControl::BeginTx().CommitTx(),
            TExecuteQuerySettings().StatsMode(EStatsMode::Full));
        UNIT_ASSERT_C(result.IsSuccess(), query << "\n" << result.GetIssues().ToString());
        UNIT_ASSERT(result.GetStats() && result.GetStats()->GetAst());
        const TString ast = *result.GetStats()->GetAst();
        UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("RequireSortedRead"), expectSorted, query << "\n" << ast);
        if (expectSorted) {
            UNIT_ASSERT_C(ast.Contains("DqCnMerge"), ast);
            UNIT_ASSERT_C(!ast.Contains("TopSort"), ast);
            UNIT_ASSERT_C(sortedScans.load() > 0, query);
            UNIT_ASSERT_VALUES_EQUAL(missingDirections.load(), 0);
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(sortedScans.load(), 0, query);
        }
        return FormatResultSetYson(result.GetResultSet(0));
    };

    struct TCase {
        TString Query;
        bool Sorted;
    };
    const TVector<TCase> cases = {
        {"SELECT * FROM `/Root/SortedRead` WHERE k >= 3 AND k < 29 ORDER BY k, subkey;", true},
        {"SELECT * FROM `/Root/SortedRead` WHERE k >= 3 AND k < 29 ORDER BY k DESC, subkey DESC;", true},
        {"SELECT * FROM `/Root/SortedRead` WHERE k >= 3 AND k < 29 ORDER BY k, subkey LIMIT 7;", true},
        {"SELECT * FROM `/Root/SortedRead` WHERE k >= 3 AND k < 29 ORDER BY k DESC, subkey DESC LIMIT 7 OFFSET 5;", true},
        {"SELECT value FROM `/Root/SortedRead` WHERE k >= 3 AND k < 29 ORDER BY k, subkey LIMIT 5;", true},
        {"SELECT * FROM `/Root/SortedRead` WHERE k IN (2u, 7u, 18u, 30u) ORDER BY k, subkey;", true},
        {"SELECT * FROM `/Root/SortedRead` WHERE k >= 100 ORDER BY k, subkey;", true},
        {"SELECT * FROM `/Root/SortedRead` ORDER BY k, subkey LIMIT 7;", true},
        {"SELECT k FROM `/Root/SortedRead` ORDER BY k LIMIT 7;", true},
        {"SELECT * FROM `/Root/SortedRead` WHERE value > 60 ORDER BY k, subkey;", true},
        {"SELECT * FROM `/Root/SortedRead` WHERE k >= 3 AND value < 60 ORDER BY k, subkey LIMIT 7;", true},
        {"SELECT * FROM `/Root/SortedRead` WHERE k >= 3 OR value > 60 ORDER BY k, subkey;", true},
        {"PRAGMA kikimr.OptEnableOlapPushdown = \"false\"; SELECT value FROM `/Root/SortedRead` "
         "WHERE value % 5 = 0 ORDER BY k, subkey LIMIT 4 OFFSET 2;", true},
        {"SELECT * FROM `/Root/SortedRead` WHERE k >= 3 ORDER BY value, k, subkey;", false},
        {"SELECT * FROM `/Root/SortedRead` WHERE k >= 3 ORDER BY k, subkey DESC;", false},
        {"SELECT * FROM `/Root/SortedRead` WHERE k >= 3 AND subkey % 2 = 0 ORDER BY k, subkey;", true},
        {"SELECT k, COUNT(*) AS cnt FROM `/Root/SortedRead` GROUP BY k ORDER BY cnt, k;", false},
        {"SELECT 100u - k AS k, subkey, value FROM `/Root/SortedRead` ORDER BY k, subkey;", false},
    };
    run(cases.front().Query, std::nullopt, false, true);
    for (bool blocks : {true, false}) {
        for (const auto& test : cases) {
            const auto expected = run(test.Query, false, false, blocks);
            const auto actual = run(test.Query, true, test.Sorted, blocks);
            CompareYson(expected, actual);
        }
    }

    // Sorted reads must retain the latest versions across overlapping portions.
    auto upsert = execute(
        "UPSERT INTO `/Root/SortedRead` (k, subkey, value) VALUES (3u, 0u, -10), (18u, 1u, -20);",
        TTxControl::BeginTx().CommitTx());
    UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
    const TString versionsQuery = "SELECT * FROM `/Root/SortedRead` WHERE k >= 3 ORDER BY k DESC, subkey DESC;";
    const auto expectedVersions = run(versionsQuery, false, false, true);
    CompareYson(expectedVersions, run(versionsQuery, true, true, true));
}

} // namespace

Y_UNIT_TEST_SUITE(RequireSortedRead) {
    Y_UNIT_TEST_TWIN(LegacyOptimizer, Separated) {
        CheckSortedRead(false, Separated);
    }

    Y_UNIT_TEST_TWIN(RboOptimizer, Separated) {
        CheckSortedRead(true, Separated);
    }
}

} // namespace NKikimr::NKqp
