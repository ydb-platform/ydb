#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <yql/essentials/public/langver/yql_langver.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/output.h>
#include <util/string/builder.h>

#include <algorithm>
#include <initializer_list>
#include <utility>
#include <vector>

namespace NKikimr::NKqp {
namespace {

// Keep real SQL result expectations independent of the semantic exporter:
// equal exclusive endpoints denote an empty interval, even when a reader
// mistakes identical serialized bounds for a point.
class TExclusiveRangeFixture {
public:
    explicit TExclusiveRangeFixture(bool int32, bool composite = false)
        : Int32(int32)
        , Composite(composite)
        , Kikimr(TKikimrSettings(Config()).SetWithSampleTables(false))
        , Session(Kikimr.GetQueryClient().GetSession().GetValueSync().GetSession())
    {
        auto tableClient = Kikimr.GetTableClient();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();
        const TString ddl = TStringBuilder()
            << "CREATE TABLE `/Root/ExclusiveRange` (k " << Type() << " NOT NULL, "
            << (Composite ? "subkey Int32 NOT NULL, " : "")
            << "PRIMARY KEY (k" << (Composite ? ", subkey" : "") << "));";
        const auto scheme = tableSession.ExecuteSchemeQuery(ddl).GetValueSync();
        UNIT_ASSERT_C(scheme.IsSuccess(), scheme.GetIssues().ToString());

        NYdb::TValueBuilder rows;
        rows.BeginList();
        for (i64 key : {41, 42, 43}) {
            const i32 copies = Composite && key == 42 ? 2 : 1;
            for (i32 subkey = 0; subkey < copies; ++subkey) {
                rows.AddListItem().BeginStruct();
                if (Int32) {
                    rows.AddMember("k").Int32(static_cast<i32>(key));
                } else {
                    rows.AddMember("k").Int64(key);
                }
                if (Composite) {
                    rows.AddMember("subkey").Int32(subkey);
                }
                rows.EndStruct();
            }
        }
        const auto upsert = tableClient.BulkUpsert(
            "/Root/ExclusiveRange", rows.EndList().Build()).GetValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
    }

    TString Literal(i64 value) const {
        return TStringBuilder() << Type() << "(\"" << value << "\")";
    }

    void Check(const TString& label, const TString& predicate, std::vector<i64> expected,
        std::initializer_list<std::pair<TString, i64>> bindings = {})
    {
        NYdb::TParamsBuilder builder;
        TStringBuilder sql;
        sql << "PRAGMA YqlSelect = 'force';\n";
        for (const auto& [name, value] : bindings) {
            sql << "DECLARE " << name << " AS " << Type() << ";\n";
            if (Int32) {
                builder.AddParam(name).Int32(static_cast<i32>(value)).Build();
            } else {
                builder.AddParam(name).Int64(value).Build();
            }
        }
        sql << "SELECT k FROM `/Root/ExclusiveRange` WHERE " << predicate << ";";
        const TString query = sql;
        const auto params = builder.Build();
        const auto counters = TKqpCounters(Kikimr.GetTestServer().GetRuntime()->GetAppData().Counters)
            .GetKqpCounters();
        const auto successes = counters->GetCounter("Compilation/NewRBO/Success");
        const auto failures = counters->GetCounter("Compilation/NewRBO/Failed");
        const auto successesBefore = successes->Val();
        const auto failuresBefore = failures->Val();

        const auto explained = Session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), params,
            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain))
            .ExtractValueSync();
        UNIT_ASSERT_C(explained.IsSuccess(), label << ": " << query << "\n" << explained.GetIssues().ToString());
        if (!ObservedNewRbo) {
            UNIT_ASSERT_C(successes->Val() > successesBefore, label << ": no successful new RBO compilation");
            ObservedNewRbo = true;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(failures->Val(), failuresBefore, query);
        UNIT_ASSERT_C(explained.GetStats() && explained.GetStats()->GetAst(), label << ": missing Explain AST");
        const TString ast{*explained.GetStats()->GetAst()};
        TStringBuilder boundValues;
        for (const auto& [name, value] : bindings) {
            boundValues << name << "=" << value << "; ";
        }
        // A successful empty result may be constant-folded before a read.
        // Retain the complete AST and flags; do not call that range-path coverage.
        // ASTs do not expose the compiled RangeIsPoint protobuf bit itself.
        Cerr << "[EXCLUSIVE-RANGE-AST-BEGIN] " << label << " " << Type()
             << " composite=" << Composite << "\n" << query
             << "\nparameters: " << boundValues
             << "\nrows_source=" << ast.Contains("KqpRowsSourceSettings")
             << " key_exc=" << ast.Contains("KqlKeyExc")
             << " key_inc=" << ast.Contains("KqlKeyInc")
             << " computed=" << ast.Contains("RangeFinalize")
             << " binding=" << ast.Contains("kqp_physical_tx_param_binding")
             << "\n" << ast << "\n[EXCLUSIVE-RANGE-AST-END]" << Endl;

        const auto result = Session.ExecuteQuery(query,
            NYdb::NQuery::TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), label << ": " << query << "\n" << result.GetIssues().ToString()
            << "\nExplain AST:\n" << ast);
        UNIT_ASSERT_VALUES_EQUAL_C(failures->Val(), failuresBefore, query);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetResultSets().size(), 1, query);
        const auto& resultSet = result.GetResultSet(0);
        UNIT_ASSERT_C(!resultSet.Truncated(), query);
        UNIT_ASSERT_VALUES_EQUAL_C(resultSet.GetColumnsMeta().size(), 1, query);
        UNIT_ASSERT_VALUES_EQUAL_C(resultSet.GetColumnsMeta()[0].Name, "k", query);
        UNIT_ASSERT_VALUES_EQUAL_C(resultSet.GetColumnsMeta()[0].Type.ToString(), Type(), query);
        NYdb::TResultSetParser parser(resultSet);
        std::vector<i64> actual;
        while (parser.TryNextRow()) {
            actual.push_back(Int32 ? static_cast<i64>(parser.ColumnParser(0).GetInt32())
                                  : parser.ColumnParser(0).GetInt64());
        }
        // Sort only host-side bags. SQL contains no ORDER BY, and duplicated
        // projected prefix keys remain duplicated in the composite control.
        std::sort(actual.begin(), actual.end());
        std::sort(expected.begin(), expected.end());
        UNIT_ASSERT_C(actual == expected, label << ": " << query
            << "\nExpected bag: " << FormatBag(expected)
            << "\nActual result: " << FormatResultSetYson(resultSet)
            << "\nExplain AST:\n" << ast);
    }

private:
    const char* Type() const {
        return Int32 ? "Int32" : "Int64";
    }

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

    static TString FormatBag(const std::vector<i64>& bag) {
        TStringBuilder result;
        result << "[";
        for (i64 value : bag) {
            result << "[" << value << "];";
        }
        result << "]";
        return result;
    }

    const bool Int32;
    const bool Composite;
    TKikimrRunner Kikimr;
    NYdb::NQuery::TSession Session;
    bool ObservedNewRbo = false;
};

} // namespace

Y_UNIT_TEST_SUITE(KqpRboExclusiveRanges) {
    Y_UNIT_TEST_TWIN(EqualExclusiveLiteralIsEmpty, Int32) {
        TExclusiveRangeFixture fixture(Int32);
        fixture.Check("literal-equal-exclusive",
            TStringBuilder() << "k > " << fixture.Literal(42) << " AND k < " << fixture.Literal(42), {});
    }

    Y_UNIT_TEST_TWIN(EqualExclusiveSharedParameterIsEmpty, Int32) {
        TExclusiveRangeFixture fixture(Int32);
        fixture.Check("shared-parameter-equal-exclusive", "k > $p AND k < $p", {}, {{"$p", 42}});
    }

    Y_UNIT_TEST_TWIN(EqualInclusiveAndMixedControls, Int32) {
        TExclusiveRangeFixture fixture(Int32);
        fixture.Check("shared-parameter-point", "k >= $p AND k <= $p", {42}, {{"$p", 42}});
        fixture.Check("shared-parameter-inclusive-exclusive", "k >= $p AND k < $p", {}, {{"$p", 42}});
        fixture.Check("shared-parameter-exclusive-inclusive", "k > $p AND k <= $p", {}, {{"$p", 42}});
        fixture.Check("literal-point",
            TStringBuilder() << "k >= " << fixture.Literal(42) << " AND k <= " << fixture.Literal(42), {42});
        fixture.Check("literal-inclusive-exclusive",
            TStringBuilder() << "k >= " << fixture.Literal(42) << " AND k < " << fixture.Literal(42), {});
        fixture.Check("literal-exclusive-inclusive",
            TStringBuilder() << "k > " << fixture.Literal(42) << " AND k <= " << fixture.Literal(42), {});
    }

    Y_UNIT_TEST_TWIN(DistinctParameterNamesUseRuntimeValues, Int32) {
        TExclusiveRangeFixture fixture(Int32);
        // Same runtime values do not imply identical serialized parameter names.
        fixture.Check("distinct-parameters-equal-exclusive", "k > $lo AND k < $hi", {},
            {{"$lo", 42}, {"$hi", 42}});
        fixture.Check("distinct-parameters-nonempty-exclusive", "k > $lo AND k < $hi", {42},
            {{"$lo", 41}, {"$hi", 43}});
        fixture.Check("distinct-parameters-reversed-exclusive", "k > $lo AND k < $hi", {},
            {{"$lo", 43}, {"$hi", 41}});
        fixture.Check("distinct-parameters-point", "k >= $lo AND k <= $hi", {42},
            {{"$lo", 42}, {"$hi", 42}});
        fixture.Check("distinct-parameters-nonempty-inclusive", "k >= $lo AND k <= $hi", {41, 42, 43},
            {{"$lo", 41}, {"$hi", 43}});
    }

    Y_UNIT_TEST_TWIN(ReversedLiteralRangesAreEmpty, Int32) {
        TExclusiveRangeFixture fixture(Int32);
        fixture.Check("literal-reversed-exclusive",
            TStringBuilder() << "k > " << fixture.Literal(43) << " AND k < " << fixture.Literal(41), {});
        fixture.Check("literal-reversed-inclusive",
            TStringBuilder() << "k >= " << fixture.Literal(43) << " AND k <= " << fixture.Literal(41), {});
    }

    Y_UNIT_TEST_TWIN(CompositePrefixKeepsAllRows, Int32) {
        TExclusiveRangeFixture fixture(Int32, true);
        fixture.Check("composite-inclusive-prefix", "k >= $p AND k <= $p", {42, 42}, {{"$p", 42}});
        fixture.Check("composite-exclusive-prefix", "k > $p AND k < $p", {}, {{"$p", 42}});
        fixture.Check("composite-complete-point", "k = $p AND subkey >= 0 AND subkey <= 0", {42}, {{"$p", 42}});
    }

    Y_UNIT_TEST_TWIN(CompositeCompleteExclusiveRangeIsEmpty, Int32) {
        TExclusiveRangeFixture fixture(Int32, true);
        fixture.Check("composite-complete-exclusive", "k = $p AND subkey > 0 AND subkey < 0", {}, {{"$p", 42}});
    }
}

} // namespace NKikimr::NKqp
