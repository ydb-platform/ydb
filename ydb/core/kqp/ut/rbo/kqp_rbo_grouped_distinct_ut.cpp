#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <yql/essentials/public/langver/yql_langver.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <algorithm>
#include <optional>
#include <vector>

namespace NKikimr::NKqp {
namespace {

using TInt64Row = std::vector<std::optional<i64>>;
using TInt64Bag = std::vector<TInt64Row>;

// These are SQL-result regressions: an initial/final RBO equivalence proof
// starts after RewriteSelect and cannot detect an expression already lost there.
class TGroupedDistinctFixture {
public:
    explicit TGroupedDistinctFixture(bool populate = true)
        : Kikimr(TKikimrSettings(Config()).SetWithSampleTables(false))
        , Session(Kikimr.GetQueryClient().GetSession().GetValueSync().GetSession())
    {
        auto tableClient = Kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        const auto scheme = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/GroupedDistinct` (
                Id Int64 NOT NULL,
                K Int64,
                PRIMARY KEY (Id)
            );
        )").GetValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());

        if (populate) {
            // Five NULL-safe K groups; two groups have two different primary
            // keys. +/- values also make K*K noninjective across the groups.
            const TVector<std::optional<i64>> keys{-2, -1, 1, 2, std::nullopt, 1, std::nullopt};
            NYdb::TValueBuilder rows;
            rows.BeginList();
            for (size_t index = 0; index < keys.size(); ++index) {
                rows.AddListItem().BeginStruct()
                    .AddMember("Id").Int64(index + 1)
                    .AddMember("K").OptionalInt64(keys[index])
                    .EndStruct();
            }
            const auto upsert = tableClient.BulkUpsert(
                "/Root/GroupedDistinct", rows.EndList().Build()).GetValueSync();
            UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
        }
    }

    void CheckInt64Bag(const TString& sql, const TVector<TString>& names, TInt64Bag expected) {
        const auto resultSet = Execute(sql);
        CheckColumns(resultSet, names, "Int64?", sql);
        TInt64Bag actual;
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            TInt64Row row;
            for (size_t column = 0; column < names.size(); ++column) {
                row.push_back(parser.ColumnParser(column).GetOptionalInt64());
            }
            actual.push_back(std::move(row));
        }
        // Sort only the host-side bags; the SQL has no ORDER BY or LIMIT.
        // Repeated rows remain repeated and therefore cannot pass as a set.
        std::sort(actual.begin(), actual.end());
        std::sort(expected.begin(), expected.end());
        UNIT_ASSERT_C(actual == expected,
            sql << "\nExpected bag: " << FormatBag(expected)
                << "\nActual bag: " << FormatBag(actual));
    }

    void CheckInt64Sequence(const TString& sql, const TVector<TString>& names, const TInt64Bag& expected) {
        const auto resultSet = Execute(sql);
        CheckColumns(resultSet, names, "Int64?", sql);
        TInt64Bag actual;
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            TInt64Row row;
            for (size_t column = 0; column < names.size(); ++column) {
                row.push_back(parser.ColumnParser(column).GetOptionalInt64());
            }
            actual.push_back(std::move(row));
        }
        // The returned order is the assertion. Do not sort either side here.
        UNIT_ASSERT_C(actual == expected,
            sql << "\nExpected sequence: " << FormatBag(expected)
                << "\nActual sequence: " << FormatBag(actual));
    }

    void CheckCountBag(const TString& sql, std::vector<ui64> expected) {
        const auto resultSet = Execute(sql);
        CheckColumns(resultSet, {"n"}, "Uint64", sql);
        std::vector<ui64> actual;
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            actual.push_back(parser.ColumnParser(0).GetUint64());
        }
        std::sort(actual.begin(), actual.end());
        std::sort(expected.begin(), expected.end());
        UNIT_ASSERT_VALUES_EQUAL_C(actual, expected, sql);
    }

    void CheckOptionalUint64Bag(const TString& sql, std::vector<std::optional<ui64>> expected) {
        const auto resultSet = Execute(sql);
        CheckColumns(resultSet, {"n"}, "Uint64?", sql);
        std::vector<std::optional<ui64>> actual;
        NYdb::TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            actual.push_back(parser.ColumnParser(0).GetOptionalUint64());
        }
        std::sort(actual.begin(), actual.end());
        std::sort(expected.begin(), expected.end());
        UNIT_ASSERT_C(actual == expected, sql);
    }

private:
    static NKikimrConfig::TAppConfig Config() {
        NKikimrConfig::TAppConfig config;
        auto* service = config.MutableTableServiceConfig();
        service->SetEnableNewRBO(true);
        service->SetEnableFallbackToYqlOptimizer(false);
        service->SetDefaultLangVer(NYql::GetMaxLangVersion());
        service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        return config;
    }

    NYdb::TResultSet Execute(const TString& sql) {
        const auto counters = TKqpCounters(Kikimr.GetTestServer().GetRuntime()->GetAppData().Counters)
            .GetKqpCounters();
        const auto successes = counters->GetCounter("Compilation/NewRBO/Success");
        const auto failures = counters->GetCounter("Compilation/NewRBO/Failed");
        const auto successesBefore = successes->Val();
        const auto failuresBefore = failures->Val();
        const TString query = TString("PRAGMA YqlSelect = 'force';\n") + sql;
        // Every SQL text is executed once in this fixture; a fresh compilation
        // must use the new RBO, independently of matching the result values.
        const auto result = Session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), query << "\n" << result.GetIssues().ToString());
        UNIT_ASSERT_C(successes->Val() > successesBefore, query << " did not compile with the new RBO");
        UNIT_ASSERT_VALUES_EQUAL_C(failures->Val(), failuresBefore, query);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetResultSets().size(), 1, query);
        UNIT_ASSERT_C(!result.GetResultSet(0).Truncated(), query);
        return result.GetResultSet(0);
    }

    static void CheckColumns(const NYdb::TResultSet& resultSet, const TVector<TString>& names,
        const TString& type, const TString& sql)
    {
        UNIT_ASSERT_VALUES_EQUAL_C(resultSet.GetColumnsMeta().size(), names.size(), sql);
        for (size_t column = 0; column < names.size(); ++column) {
            UNIT_ASSERT_VALUES_EQUAL_C(resultSet.GetColumnsMeta()[column].Name, names[column], sql);
            UNIT_ASSERT_VALUES_EQUAL_C(resultSet.GetColumnsMeta()[column].Type.ToString(), type, sql);
        }
    }

    static TString FormatBag(const TInt64Bag& rows) {
        TStringBuilder result;
        result << "[";
        for (const auto& row : rows) {
            result << "[";
            for (const auto& value : row) {
                if (value) {
                    result << *value;
                } else {
                    result << "NULL";
                }
                result << ";";
            }
            result << "]";
        }
        result << "]";
        return result;
    }

    TKikimrRunner Kikimr;
    NYdb::NQuery::TSession Session;
};

const TString GroupedAdd = R"(
    SELECT DISTINCT t.K + 1 AS shifted
    FROM `/Root/GroupedDistinct` AS t GROUP BY t.K;
)";
const TString HiddenKey = R"(
    SELECT DISTINCT t.K AS k
    FROM `/Root/GroupedDistinct` AS t GROUP BY t.K, t.Id;
)";
const TString GroupedCount = R"(
    SELECT DISTINCT COUNT(*) AS n
    FROM `/Root/GroupedDistinct` AS t GROUP BY t.K;
)";
const TString RepeatedGroupAliases = R"(
    SELECT DISTINCT t.K AS z, t.K AS a
    FROM `/Root/GroupedDistinct` AS t GROUP BY t.K;
)";

} // namespace

Y_UNIT_TEST_SUITE(KqpRboGroupedDistinct) {
    Y_UNIT_TEST(ExpressionsAreEvaluatedBeforeDistinct) {
        TGroupedDistinctFixture fixture;
        fixture.CheckInt64Bag(GroupedAdd, {"shifted"},
            {{-1}, {0}, {2}, {3}, {std::nullopt}});
        // Evaluating this after DISTINCT(K) would still leave duplicate 1/4
        // results; checking only the K+1 case would miss that incomplete repair.
        fixture.CheckInt64Bag(R"(
            SELECT DISTINCT t.K * t.K AS squared
            FROM `/Root/GroupedDistinct` AS t GROUP BY t.K;
        )", {"squared"}, {{1}, {4}, {std::nullopt}});
    }

    Y_UNIT_TEST(UnselectedGroupKeyDoesNotPreventDistinct) {
        TGroupedDistinctFixture fixture;
        fixture.CheckInt64Bag(HiddenKey, {"k"},
            {{-2}, {-1}, {1}, {2}, {std::nullopt}});
    }

    Y_UNIT_TEST(DistinctAggregateResultsExcludeHiddenGroupKeys) {
        TGroupedDistinctFixture fixture;
        // Group sizes are 1,1,2,1,2. DISTINCT returns each count just once.
        fixture.CheckCountBag(GroupedCount, {1, 2});
    }

    Y_UNIT_TEST(RepeatedGroupedAliasesRemainSeparateResultColumns) {
        TGroupedDistinctFixture fixture;
        // Non-lexical SELECT order checks both aliases and their positions.
        fixture.CheckInt64Bag(RepeatedGroupAliases, {"z", "a"},
            {{-2, -2}, {-1, -1}, {1, 1}, {2, 2}, {std::nullopt, std::nullopt}});
    }

    Y_UNIT_TEST(DirectAndNestedGroupExpressionControls) {
        TGroupedDistinctFixture fixture;
        const TInt64Bag expected{{-1}, {0}, {2}, {3}, {std::nullopt}};
        fixture.CheckInt64Bag(R"(
            SELECT t.K + 1 AS shifted
            FROM `/Root/GroupedDistinct` AS t GROUP BY t.K;
        )", {"shifted"}, expected);
        fixture.CheckInt64Bag(R"(
            SELECT DISTINCT t.K + 1 AS shifted
            FROM `/Root/GroupedDistinct` AS t;
        )", {"shifted"}, expected);
        fixture.CheckInt64Bag(R"(
            PRAGMA YqlSelectAllowUnnamedGroupByExpr;
            SELECT DISTINCT t.K + 1 AS shifted
            FROM `/Root/GroupedDistinct` AS t GROUP BY t.K + 1;
        )", {"shifted"}, expected);
    }

    Y_UNIT_TEST(AggregateAndGroupExpressionsRetainEveryOperand) {
        TGroupedDistinctFixture fixture;
        // K=-2,-1,1,2,NULL has group sizes 1,1,2,1,2. The two
        // distinct groups K=1 and K=2 deliberately produce the same sum.
        fixture.CheckInt64Bag(R"(
            SELECT CAST(COUNT(*) AS Int64) + t.K AS mixed
            FROM `/Root/GroupedDistinct` AS t GROUP BY t.K;
        )", {"mixed"}, {{-1}, {0}, {3}, {3}, {std::nullopt}});
    }

    Y_UNIT_TEST(DistinctAggregateAndGroupExpressionsUseTheComputedValue) {
        TGroupedDistinctFixture fixture;
        fixture.CheckInt64Bag(R"(
            SELECT DISTINCT CAST(COUNT(*) AS Int64) + t.K AS mixed
            FROM `/Root/GroupedDistinct` AS t GROUP BY t.K;
        )", {"mixed"}, {{-1}, {0}, {3}, {std::nullopt}});
    }

    Y_UNIT_TEST(DistinctTuplePreservesCorrelatedGroupExpressions) {
        TGroupedDistinctFixture fixture;
        fixture.CheckInt64Bag(R"(
            SELECT DISTINCT t.K * t.K AS z, t.K + 1 AS a
            FROM `/Root/GroupedDistinct` AS t GROUP BY t.K;
        )", {"z", "a"}, {{4, -1}, {1, 0}, {1, 2}, {4, 3},
            {std::nullopt, std::nullopt}});
    }

    Y_UNIT_TEST(UnusedWindowDeclarationDoesNotChangeGroupedDistinct) {
        TGroupedDistinctFixture fixture;
        // The named window has no calls. It must not make a supported grouped
        // DISTINCT unavailable or change the selected tuple.
        fixture.CheckInt64Bag(R"(
            SELECT DISTINCT t.K AS k
            FROM `/Root/GroupedDistinct` AS t GROUP BY t.K WINDOW unused AS ();
        )", {"k"}, {{-2}, {-1}, {1}, {2}, {std::nullopt}});
    }

    Y_UNIT_TEST(GroupedDistinctSortBindsSelectedAliasesAndExpressions) {
        TGroupedDistinctFixture fixture;
        for (const TStringBuf order : {"sq", "t.K * t.K", "sq + 1"}) {
            const TString sql = TStringBuilder()
                << "SELECT DISTINCT t.K * t.K AS sq FROM `/Root/GroupedDistinct` AS t "
                << "WHERE t.K IS NOT NULL GROUP BY t.K ORDER BY " << order << " DESC;";
            fixture.CheckInt64Sequence(sql, {"sq"}, {{4}, {1}});
        }
        // This dialect does not support ordinal ORDER BY. When this constant
        // expression is admitted, its tied rows have no specified order.
        fixture.CheckInt64Bag(R"(
            SELECT DISTINCT t.K * t.K AS sq
            FROM `/Root/GroupedDistinct` AS t WHERE t.K IS NOT NULL
            GROUP BY t.K ORDER BY 1 DESC;
        )", {"sq"}, {{1}, {4}});
        // Projection aliases have priority over an unqualified source name.
        fixture.CheckInt64Sequence(R"(
            SELECT DISTINCT t.K * t.K AS K
            FROM `/Root/GroupedDistinct` AS t WHERE t.K IS NOT NULL
            GROUP BY t.K ORDER BY K DESC;
        )", {"K"}, {{4}, {1}});
        fixture.CheckInt64Sequence(R"(
            SELECT DISTINCT t.K * t.K AS sq
            FROM `/Root/GroupedDistinct` AS t WHERE t.K IS NOT NULL
            GROUP BY t.K ORDER BY sq LIMIT 1;
        )", {"sq"}, {{1}});
    }

    Y_UNIT_TEST(ComputedGroupKeysRetainTheOuterResultExpression) {
        TGroupedDistinctFixture fixture;
        fixture.CheckInt64Bag(R"(
            PRAGMA YqlSelectAllowUnnamedGroupByExpr;
            SELECT DISTINCT t.K * t.K + 1 AS value
            FROM `/Root/GroupedDistinct` AS t GROUP BY t.K * t.K;
        )", {"value"}, {{2}, {5}, {std::nullopt}});
    }

    Y_UNIT_TEST(NestedAggregateDoesNotMaterializeTheOuterResultAlias) {
        TGroupedDistinctFixture fixture;
        // Only the scalar subquery has an aggregate. The outer result still
        // has to evaluate +K after grouping; COUNT(*) over this seed is 7.
        for (const TStringBuf distinct : {"", "DISTINCT "}) {
            const TString sql = TStringBuilder() << "SELECT " << distinct << R"(
                CAST((SELECT COUNT(*) FROM `/Root/GroupedDistinct` AS i) AS Int64) + t.K AS mixed
                FROM `/Root/GroupedDistinct` AS t GROUP BY t.K;
            )";
            fixture.CheckInt64Bag(sql, {"mixed"}, {{5}, {6}, {8}, {9}, {std::nullopt}});
        }
    }

    Y_UNIT_TEST(DistinctMixedAggregateKeepsNestedGroupScopes) {
        TGroupedDistinctFixture fixture;
        // Inner K*K groups yield 6,4,NULL, so MAX(mixed) is 6. Outer
        // COUNT(*)+K yields -1,0,3,3,NULL; DISTINCT must remove the repeated 9.
        fixture.CheckInt64Bag(R"(
            PRAGMA YqlSelectAllowUnnamedGroupByExpr;
            SELECT DISTINCT CAST(COUNT(*) AS Int64) + t.K +
                (SELECT MAX(s.mixed) FROM (
                    SELECT CAST(COUNT(*) AS Int64) + i.K * i.K AS mixed
                    FROM `/Root/GroupedDistinct` AS i GROUP BY i.K * i.K
                ) AS s) AS mixed
            FROM `/Root/GroupedDistinct` AS t GROUP BY t.K;
        )", {"mixed"}, {{5}, {6}, {9}, {std::nullopt}});
    }

    Y_UNIT_TEST(GroupedDistinctConstantAndEmptyMixedExpressionKeepCardinality) {
        {
            TGroupedDistinctFixture populated;
            populated.CheckOptionalUint64Bag(R"(
                SELECT DISTINCT CAST(1 AS Uint64) AS n
                FROM `/Root/GroupedDistinct` AS t GROUP BY t.K;
            )", {1});
        } // Release this server before creating the empty fixture.
        TGroupedDistinctFixture empty(false);
        empty.CheckOptionalUint64Bag(R"(
            SELECT DISTINCT CAST(1 AS Uint64) AS n
            FROM `/Root/GroupedDistinct` AS t GROUP BY t.K;
        )", {});
        for (const TStringBuf distinct : {"", "DISTINCT "}) {
            const TString sql = TStringBuilder() << "SELECT " << distinct << R"(
                CAST(COUNT(*) AS Int64) + t.K AS mixed
                FROM `/Root/GroupedDistinct` AS t GROUP BY t.K;
            )";
            empty.CheckInt64Bag(sql, {"mixed"}, {});
        }
    }

    Y_UNIT_TEST(EmptyGroupedDistinctHasNoRows) {
        TGroupedDistinctFixture fixture(false);
        fixture.CheckInt64Bag(GroupedAdd, {"shifted"}, {});
        fixture.CheckInt64Bag(HiddenKey, {"k"}, {});
        fixture.CheckCountBag(GroupedCount, {});
        fixture.CheckInt64Bag(RepeatedGroupAliases, {"z", "a"}, {});
    }
}

} // namespace NKikimr::NKqp
