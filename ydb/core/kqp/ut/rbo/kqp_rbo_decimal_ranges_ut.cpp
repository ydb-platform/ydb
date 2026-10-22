#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <yql/essentials/public/langver/yql_langver.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/output.h>
#include <util/string/builder.h>

#include <algorithm>
#include <utility>
#include <vector>

namespace NKikimr::NKqp {
namespace {

class TDecimalRangeFixture {
public:
    explicit TDecimalRangeFixture(bool parameter)
        : Parameter(parameter)
        , Kikimr(TKikimrSettings(Config()).SetWithSampleTables(false))
        , Session(Kikimr.GetQueryClient().GetSession().GetValueSync().GetSession())
    {
        auto tableClient = Kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        const auto created = tableSession.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/DecimalRanges` (
                K Decimal(10,0) NOT NULL, Id Int32 NOT NULL, PRIMARY KEY (K));
        )").GetValueSync();
        UNIT_ASSERT_C(created.IsSuccess(), "Required Decimal PK creation failed; this is not an unsupported pass:\n"
            << created.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        // Stable independent row identities: -Inf=1, -1=2, 0=3, 1=4, +Inf=5, NaN=6.
        const std::vector<std::pair<const char*, i32>> seed{
            {"-inf", 1}, {"-1", 2}, {"0", 3}, {"1", 4}, {"inf", 5}, {"nan", 6}};
        for (const auto& [value, id] : seed) {
            rows.AddListItem().BeginStruct()
                .AddMember("K").Decimal(NYdb::TDecimalValue(value, 10, 0))
                .AddMember("Id").Int32(id)
                .EndStruct();
        }
        const auto seeded = tableClient.BulkUpsert("/Root/DecimalRanges", rows.EndList().Build()).GetValueSync();
        UNIT_ASSERT_C(seeded.IsSuccess(), "Decimal special-value PK storage rejected the fixture; this is not an unsupported pass:\n"
            << seeded.GetIssues().ToString());
    }

    void Check(const char* op, const char* rhs, std::vector<i32> expected) {
        NYdb::TParamsBuilder paramsBuilder;
        TStringBuilder sql;
        sql << "PRAGMA YqlSelect = 'force';\n";
        if (Parameter) {
            sql << "DECLARE $p AS Decimal(10,0);\n";
            paramsBuilder.AddParam("$p").Decimal(NYdb::TDecimalValue(rhs, 10, 0)).Build();
        }
        sql << "SELECT Id FROM `/Root/DecimalRanges` WHERE K " << op << " ";
        if (Parameter) {
            sql << "$p";
        } else {
            sql << "Decimal(\"" << rhs << "\", 10, 0)";
        }
        sql << ";";
        const TString query = sql;
        const auto params = paramsBuilder.Build();
        const auto counters = TKqpCounters(Kikimr.GetTestServer().GetRuntime()->GetAppData().Counters).GetKqpCounters();
        const auto successes = counters->GetCounter("Compilation/NewRBO/Success");
        const auto failures = counters->GetCounter("Compilation/NewRBO/Failed");
        const auto beforeSuccess = successes->Val();
        const auto beforeFailure = failures->Val();

        const auto explained = Session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), params,
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)).ExtractValueSync();
        UNIT_ASSERT_C(explained.IsSuccess(), query << "\n" << explained.GetIssues().ToString());
        // Later parameter values may reuse the same compiled query. The first
        // successful compilation must use new RBO, and fallback is disabled.
        if (!ObservedNewRbo) {
            UNIT_ASSERT_C(successes->Val() > beforeSuccess, query << ": no successful new RBO compilation");
            ObservedNewRbo = true;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(failures->Val(), beforeFailure, query);
        UNIT_ASSERT_C(explained.GetStats() && explained.GetStats()->GetAst(), query << ": missing Explain AST");
        const TString ast{*explained.GetStats()->GetAst()};
        // Full AST, not marker presence alone, establishes whether a filter
        // was retained and whether a literal range is attached to this source.
        // Explain AST does not expose the compiled RangeIsPoint protobuf bit.
        Cerr << "[DECIMAL-RANGE-AST-BEGIN]\n" << query
             << "\nrhs=" << rhs << " parameter=" << Parameter
             << " rows_source=" << ast.Contains("KqpRowsSourceSettings")
             << " key_inc=" << ast.Contains("KqlKeyInc")
             << " key_exc=" << ast.Contains("KqlKeyExc")
             << " computed=" << ast.Contains("RangeFinalize")
             << " filter=" << ast.Contains("Filter")
             << " optional_if=" << ast.Contains("OptionalIf")
             << " flat_map=" << ast.Contains("FlatMap")
             << "\n" << ast << "\n[DECIMAL-RANGE-AST-END]" << Endl;

        const auto result = Session.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), query << "\n" << result.GetIssues().ToString() << "\n" << ast);
        UNIT_ASSERT_VALUES_EQUAL_C(failures->Val(), beforeFailure, query);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetResultSets().size(), 1, query);
        const auto& resultSet = result.GetResultSet(0);
        UNIT_ASSERT_C(!resultSet.Truncated(), query);
        UNIT_ASSERT_VALUES_EQUAL_C(resultSet.GetColumnsMeta().size(), 1, query);
        UNIT_ASSERT_VALUES_EQUAL_C(resultSet.GetColumnsMeta()[0].Name, "Id", query);
        UNIT_ASSERT_VALUES_EQUAL_C(resultSet.GetColumnsMeta()[0].Type.ToString(), "Int32", query);
        NYdb::TResultSetParser parser(resultSet);
        std::vector<i32> actual;
        while (parser.TryNextRow()) {
            actual.push_back(parser.ColumnParser(0).GetInt32());
        }
        // Host-side bag normalization only; SQL has no ORDER BY.
        std::sort(actual.begin(), actual.end());
        std::sort(expected.begin(), expected.end());
        if (actual != expected) {
            TStringBuilder failure;
            failure << query << "\nrhs=" << rhs << "\nExpected Id bag:";
            for (i32 id : expected) {
                failure << " " << id;
            }
            failure << "\nActual: " << FormatResultSetYson(resultSet) << "\nExplain AST:\n" << ast;
            Mismatches.emplace_back(failure);
        }
    }

    void Finish() const {
        TStringBuilder failures;
        for (const auto& mismatch : Mismatches) {
            failures << mismatch << "\n\n";
        }
        UNIT_ASSERT_C(Mismatches.empty(), failures);
    }

private:
    static NKikimrConfig::TAppConfig Config() {
        NKikimrConfig::TAppConfig config;
        auto* service = config.MutableTableServiceConfig();
        service->SetEnableNewRBO(true);
        service->SetEnableFallbackToYqlOptimizer(false);
        service->SetEnableFallbackOnMultipleStatements(false);
        service->SetEnableFallbackOnDML(false);
        service->SetDefaultLangVer(NYql::GetMaxLangVersion());
        service->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
        return config;
    }

    const bool Parameter;
    TKikimrRunner Kikimr;
    NYdb::NQuery::TSession Session;
    bool ObservedNewRbo = false;
    std::vector<TString> Mismatches;
};

} // namespace

Y_UNIT_TEST_SUITE(KqpRboDecimalRanges) {
    Y_UNIT_TEST_TWIN(EqualityIncludesInfinitiesButNeverNaN, Parameter) {
        TDecimalRangeFixture fixture(Parameter);
        fixture.Check("=", "0", {3});
        fixture.Check("=", "inf", {5});
        fixture.Check("=", "-inf", {1});
        fixture.Check("=", "nan", {});
        fixture.Finish();
    }

    Y_UNIT_TEST_TWIN(FiniteBoundComparisonsExcludeNaNExceptNotEqual, Parameter) {
        TDecimalRangeFixture fixture(Parameter);
        fixture.Check(">", "0", {4, 5});
        fixture.Check(">=", "0", {3, 4, 5});
        fixture.Check("<", "0", {1, 2});
        fixture.Check("<=", "0", {1, 2, 3});
        fixture.Check("!=", "0", {1, 2, 4, 5, 6});
        fixture.Finish();
    }

    Y_UNIT_TEST_TWIN(NaNBoundIsUnorderedButNotEqual, Parameter) {
        TDecimalRangeFixture fixture(Parameter);
        fixture.Check(">", "nan", {});
        fixture.Check(">=", "nan", {});
        fixture.Check("<", "nan", {});
        fixture.Check("<=", "nan", {});
        fixture.Check("!=", "nan", {1, 2, 3, 4, 5, 6});
        fixture.Finish();
    }
}

} // namespace NKikimr::NKqp
