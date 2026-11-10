#include <ydb/core/kqp/host/new_parser/kqp_type_annotation.h>
#include <ydb/core/kqp/host/new_parser/kqp_new_parser.h>

#include <yql/essentials/core/yql_type_annotation.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

namespace NKikimr::NKqp {
namespace {

using namespace NNewYql;
using namespace NYql;

// Run parse, resolution and annotation directly with nullable/non-nullable
// catalog columns. Ownership order keeps the AST keys and context-owned types
// alive for assertions on the separate annotation result.
struct TFixture {
    TExprContext Ctx;
    TTypeAnnotationContext Types;
    TAstTypeAnnotationSettings Settings;
    TVector<TColumnMetadata> CatalogColumns = {
        {"id", 1, "Int64", true}, {"value", 2, "String", false}, {"score", 3, "Int32", false},
        {"amount", 4, "Decimal(12,2)", true}};
    std::unique_ptr<TScript> Ast;
    std::unique_ptr<TResolvedNames> Names;
    std::unique_ptr<TAnnotatedAst> Result;

    void Run(const TString& sql) {
        auto parsed = ParseQueryWithNewParser(sql, {}, false);
        UNIT_ASSERT_C(parsed.IsOk(), parsed.Issues.ToString());
        Ast = std::move(parsed.Ast);
        auto table = std::make_shared<TTableMetadata>();
        table->Columns = CatalogColumns;
        auto names = ResolveNames(*Ast, {"local", "/Root", {}}, [&](const TString&, const TString&) { return table; });
        UNIT_ASSERT_C(names.IsOk(), names.Issues.ToString());
        Names = std::move(names.Names);
        Result = AnnotateAst(*Ast, *Names, Ctx, Types, Settings);
    }

    // Every successful scalar root must be a closed lambda before RBO lowering
    // substitutes its arguments. Checking all roots also covers hidden clauses.
    void Ok(const TString& sql) {
        Run(sql);
        UNIT_ASSERT_C(Result, Ctx.IssueManager.GetIssues().ToString());
        for (const auto& [expression, annotated] : Result->Expressions) {
            Y_UNUSED(expression);
            CheckArguments(*annotated.Lambda);
        }
    }

    const TQuery& Query(size_t index = 0) const {
        return *std::get<TQueryStatement>(Ast->Statements[index].Value).Query;
    }

    const TSelect& Select(size_t index = 0) const { return std::get<TSelect>(Query(index).Body); }

    TString Output(size_t index, size_t statement = 0) const {
        return FormatType(Result->QueryOutputs.at(&Query(statement)).at(index));
    }
};

} // namespace

Y_UNIT_TEST_SUITE(KqpNewYqlTypeAnnotation) {
    Y_UNIT_TEST(ScalarInputsContainOnlyReferencedColumns) {
        TFixture f;
        f.Ok("SELECT id + id, 42, value FROM t WHERE score > 0 ORDER BY id LIMIT 5;");
        const auto& select = f.Select();
        const auto& repeated = f.Result->Expressions.at(select.Columns[0].Expression.get());
        UNIT_ASSERT_VALUES_EQUAL(repeated.Inputs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Expressions.at(select.Columns[1].Expression.get()).Inputs.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Expressions.at(select.Columns[2].Expression.get()).Inputs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Expressions.at(select.Where.get()).Inputs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Expressions.at(select.OrderBy[0].Expression.get()).Inputs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Expressions.at(f.Query().Limit.get()).Inputs.size(), 0);
    }

    Y_UNIT_TEST(ScalarInputsKeepDistinctJoinColumnsAndLambdaArguments) {
        TFixture f;
        f.Ok("$add = ($x, $y) -> ($x + $y); "
            "SELECT $add(l.id, r.id), l.* FROM t AS l JOIN t AS r ON l.id = r.id;");
        const auto& select = f.Select(1);
        const auto& sum = f.Result->Expressions.at(select.Columns[0].Expression.get());
        UNIT_ASSERT_VALUES_EQUAL(sum.Inputs.size(), 2);
        UNIT_ASSERT(sum.Inputs[0] != sum.Inputs[1]);
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Expressions.at(select.Columns[1].Expression.get()).Inputs.size(), 4);
    }

    Y_UNIT_TEST(ScalarInputsExcludeSiblingSubqueries) {
        TFixture f;
        f.Ok("SELECT l.id, (SELECT MAX(r.id) FROM t AS r WHERE r.id = l.id), "
            "(SELECT COUNT(*) FROM t AS s WHERE s.score = l.score) FROM t AS l;");
        const auto& select = f.Select();
        for (const auto& projection : select.Columns) {
            const auto& annotated = f.Result->Expressions.at(projection.Expression.get());
            UNIT_ASSERT_VALUES_EQUAL(annotated.Inputs.size(), 1);
        }
        UNIT_ASSERT(f.Result->Expressions.at(select.Columns[1].Expression.get()).Inputs
            != f.Result->Expressions.at(select.Columns[2].Expression.get()).Inputs);
        for (const auto& [id, subquery] : f.Result->Subqueries) {
            Y_UNUSED(id);
            UNIT_ASSERT_VALUES_EQUAL(f.Result->Captures.at(subquery.Query).size(), 1);
        }
    }

    Y_UNIT_TEST(LazyInputsPreserveGrandparentCapturesAndNegatedIn) {
        TFixture f;
        f.Ok("SELECT l.id, (SELECT (SELECT MAX(z.id) FROM t AS z WHERE z.id = l.id) "
            "FROM t AS r LIMIT 1) FROM t AS l;");
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Subqueries.size(), 2);
        for (const auto& [id, subquery] : f.Result->Subqueries) {
            Y_UNUSED(id);
            UNIT_ASSERT_VALUES_EQUAL(f.Result->Captures.at(subquery.Query).size(), 1);
        }
        TFixture negated;
        negated.Ok("SELECT l.id NOT IN (SELECT id FROM t) FROM t AS l;");
        const auto& expression = *negated.Select().Columns[0].Expression;
        const auto& annotated = negated.Result->Expressions.at(&expression);
        UNIT_ASSERT_VALUES_EQUAL(annotated.Inputs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(annotated.Inputs.front(), negated.Result->Values.at(&expression));
        UNIT_ASSERT(annotated.Lambda->Tail().IsCallable("Not"));
        const auto& comparison = *std::get<TIn>(expression.Value).Value;
        UNIT_ASSERT_VALUES_EQUAL(negated.Result->Expressions.at(&comparison).Inputs.size(), 1);
    }

    Y_UNIT_TEST(ScalarSubqueryInputsStayLocalToEachExpression) {
        TFixture f;
        f.Ok("SELECT l.id + (SELECT MAX(r.id) FROM t AS r WHERE r.id = l.id) "
            "+ (SELECT MAX(s.id) FROM t AS s WHERE s.score = l.score), l.id FROM t AS l;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0), "Optional<Int64>");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1), "Int64");
        const auto& columns = f.Select().Columns;
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Expressions.at(columns[0].Expression.get()).Inputs.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Expressions.at(columns[1].Expression.get()).Inputs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Subqueries.size(), 2);
        for (const auto& [id, subquery] : f.Result->Subqueries) {
            Y_UNUSED(id);
            const auto& captures = f.Result->Captures.at(subquery.Query);
            UNIT_ASSERT_VALUES_EQUAL(captures.size(), 1);
            UNIT_ASSERT(captures.begin()->first < f.Names->Columns.size());
        }
    }

    Y_UNIT_TEST(SetMatchingColumnsPreserveMixedCoercionPositions) {
        for (const TString op : {"UNION ALL", "UNION", "INTERSECT", "EXCEPT"}) {
            TFixture f;
            f.Ok("SELECT id AS same, id AS widened, value AS nullable, amount AS decimal_same, "
                "amount AS decimal_widened FROM t " + op + " SELECT id, score, value, amount, "
                "CAST(amount AS Decimal(15,2)) FROM t ORDER BY same;");
            UNIT_ASSERT_VALUES_EQUAL(f.Output(0), "Int64");
            UNIT_ASSERT_VALUES_EQUAL(f.Output(1), "Optional<Int64>");
            UNIT_ASSERT_VALUES_EQUAL(f.Output(2), "Optional<String>");
            UNIT_ASSERT_VALUES_EQUAL(f.Output(3), "Decimal(12,2)");
            UNIT_ASSERT_VALUES_EQUAL(f.Output(4), "Decimal(15,2)");
            const auto& conversions = f.Result->SetConversions.at(&f.Query());
            for (const size_t i : {1, 4}) {
                CheckArguments(*conversions[i].first.Lambda);
                CheckArguments(*conversions[i].second.Lambda);
                UNIT_ASSERT_VALUES_EQUAL(FormatType(conversions[i].first.GetType()), f.Output(i));
                UNIT_ASSERT_VALUES_EQUAL(FormatType(conversions[i].second.GetType()), f.Output(i));
            }
        }
    }

    Y_UNIT_TEST(SetOrderingUsesCoercedPositionalTypes) {
        TFixture f;
        f.Ok("SELECT value AS z, id AS a FROM t UNION ALL SELECT 'text' AS z, score AS a FROM t "
            "ORDER BY a + 1, z LIMIT 2 OFFSET 1;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0), "Optional<String>");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1), "Optional<Int64>");
        const auto& query = f.Query();
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Result->Expressions.at(query.OrderBy[0].Expression.get()).GetType()), "Optional<Int64>");
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Result->Expressions.at(query.OrderBy[1].Expression.get()).GetType()), "Optional<String>");
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Result->Expressions.at(query.Limit.get()).GetType()), "Uint64");
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Result->Expressions.at(query.Offset.get()).GetType()), "Uint64");
    }

    // The annotation table must keep coercions and access rewrites, not merely
    // attach a result type to the old unrewritten expression.
    Y_UNIT_TEST(AnnotatesScalarQueryAndRetainsRewrites) {
        TFixture f;
        f.Ok("SELECT id + 1 AS next, value ?? 'none', CAST(id AS Double), amount, [1, 2][0u] "
            "FROM t WHERE score > 0 ORDER BY next;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1), "String");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(2), "Double");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(3), "Decimal(12,2)");
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Result->Expressions.at(f.Select().Where.get()).GetType()), "Bool");
        const auto& sum = f.Result->Expressions.at(f.Select().Columns[0].Expression.get());
        UNIT_ASSERT(sum.Lambda->IsLambda());
        UNIT_ASSERT(sum.Lambda->Tail().GetTypeAnn());
        UNIT_ASSERT_VALUES_EQUAL(sum.Lambda->Head().ChildrenSize(), sum.Inputs.size());
        // Retain the rewritten root: SqlAccess becomes Lookup(ToIndexDict(...)).
        const auto& access = f.Result->Expressions.at(f.Select().Columns[4].Expression.get());
        UNIT_ASSERT(access.Lambda->Tail().IsCallable("Lookup"));
        UNIT_ASSERT_VALUES_EQUAL(f.Output(4), "Optional<Int32>");
        const auto aliasId = f.Names->ColumnReferences.at(f.Select().OrderBy[0].Expression.get());
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Result->Columns[aliasId]), "Int64");
    }

    // Tuple expansion preserves catalog order and each column's stored nullability.
    Y_UNIT_TEST(StarAndCatalogNullability) {
        TFixture f;
        f.Ok("SELECT t.* FROM t;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1), "Optional<String>");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(2), "Optional<Int32>");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(3), "Decimal(12,2)");
        const auto& star = f.Result->Expressions.at(f.Select().Columns[0].Expression.get());
        UNIT_ASSERT_VALUES_EQUAL(star.Lambda->Tail().ChildrenSize(), 4);
    }

    Y_UNIT_TEST(StoredTypesKeepColumnNullabilityAndDecimalParameters) {
        TFixture f;
        f.CatalogColumns = {
            {"id", 1, "Int64", true}, {"optional_id", 2, "Int64", false},
            {"amount", 3, "Decimal(12,2)", true}, {"optional_amount", 4, "Decimal(12,2)", false},
            {"wide_amount", 5, "Decimal(35,2)", true}};
        f.Ok("SELECT * FROM t;");
        const TVector<TString> expected = {
            "Int64", "Optional<Int64>", "Decimal(12,2)", "Optional<Decimal(12,2)>", "Decimal(35,2)"};
        for (size_t i = 0; i < expected.size(); ++i) { UNIT_ASSERT_VALUES_EQUAL(f.Output(i), expected[i]); }
    }

    // Producer output types must be available before following consumer alias IDs.
    Y_UNIT_TEST(CteAndDerivedOutputsAreTypedBeforeConsumers) {
        TFixture f;
        f.Ok("WITH c(key, label) AS (SELECT id + 1, value ?? 'none' FROM t) "
            "SELECT d.key + 1, d.label FROM (SELECT key, label FROM c) AS d;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1), "String");
        UNIT_ASSERT_VALUES_EQUAL(f.Result->QueryOutputs.size(), 3);
        for (const auto type : f.Result->Columns) { UNIT_ASSERT(type); }
    }

    // Null extension applies to join outputs; ON and child reads retain input types.
    Y_UNIT_TEST(LeftJoinNullabilityDoesNotAffectOnOrInputSchemas) {
        TFixture f;
        f.Ok("SELECT l.id, r.id, r.score FROM t AS l LEFT JOIN t AS r ON l.id = r.id;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1), "Optional<Int64>");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(2), "Optional<Int32>");
        const auto& join = std::get<TJoin>(f.Select().From->Value);
        const auto& rightExpr = *f.Select().Columns[1].Expression;
        const auto id = f.Names->ColumnReferences.at(&rightExpr);
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Result->Sources.at(join.Right.get()).at(id)), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Result->Sources.at(f.Select().From.get()).at(id)), "Optional<Int64>");
        const auto& on = f.Result->Expressions.at(join.On.get());
        const auto it = std::find(on.Inputs.begin(), on.Inputs.end(), id);
        UNIT_ASSERT(it != on.Inputs.end());
        UNIT_ASSERT_VALUES_EQUAL(FormatType(on.Lambda->Head().Child(it - on.Inputs.begin())->GetTypeAnn()), "Int64");
    }

    // Check which sides become nullable or disappear for each output-scope rule.
    Y_UNIT_TEST(RightFullAndSemiJoins) {
        TFixture right;
        right.Ok("SELECT l.id, r.id FROM t AS l RIGHT JOIN t AS r ON l.id = r.id;");
        UNIT_ASSERT_VALUES_EQUAL(right.Output(0), "Optional<Int64>");
        UNIT_ASSERT_VALUES_EQUAL(right.Output(1), "Int64");
        TFixture full;
        full.Ok("SELECT l.id, r.id FROM t AS l FULL JOIN t AS r ON l.id = r.id;");
        UNIT_ASSERT_VALUES_EQUAL(full.Output(0), "Optional<Int64>");
        UNIT_ASSERT_VALUES_EQUAL(full.Output(1), "Optional<Int64>");
        TFixture semi;
        semi.Ok("SELECT l.id FROM t AS l LEFT SEMI JOIN t AS r ON l.id = r.id;");
        UNIT_ASSERT_VALUES_EQUAL(semi.Output(0), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(semi.Result->Sources.at(semi.Select().From.get()).size(), 4);
    }

    // Making one CTE consumer nullable must not contaminate the other consumer.
    Y_UNIT_TEST(CteSelfJoinHasIndependentNullability) {
        TFixture f;
        f.Ok("WITH c AS (SELECT id FROM t) SELECT l.id, r.id "
            "FROM c AS l LEFT JOIN c AS r ON l.id = r.id;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1), "Optional<Int64>");
        const auto& cte = *f.Query().With[0].Query;
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Result->QueryOutputs.at(&cte)[0]), "Int64");
    }

    // Counts are query-wide typed expressions and reuse the existing Uint64 coercion.
    Y_UNIT_TEST(LimitOffsetAndParameters) {
        TFixture f;
        f.Settings.Parameters["$p"] = f.Ctx.MakeType<TDataExprType>(EDataSlot::Int64);
        f.Ok("SELECT id + $p FROM t LIMIT 10 OFFSET NULL;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Result->Expressions.at(f.Query().Limit.get()).GetType()), "Uint64");
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Result->Expressions.at(f.Query().Offset.get()).GetType()), "Uint64");
    }

    // Separate queries share settings but must retain their own annotation entries.
    Y_UNIT_TEST(MultipleStatementsAndExpressionSettings) {
        TFixture f;
        f.Settings.Expressions.CheckedOps = true;
        f.Settings.Expressions.UnicodeLiterals = true;
        f.Ok("SELECT id + 1 FROM t; SELECT 'text', -9223372036854775808l;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0), "Optional<Int64>");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0, 1), "Utf8");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1, 1), "Int64");
    }

    // A late scalar/clause error invalidates the complete annotation result.
    Y_UNIT_TEST(ErrorsDoNotReturnPartialAnnotations) {
        for (const TString sql : {"SELECT id, id + 'bad' FROM t;", "SELECT id FROM t WHERE 1;",
            "SELECT l.id FROM t AS l JOIN t AS r ON l.id;", "SELECT id FROM t LIMIT 'bad';",
            "SELECT $missing;"}) {
            TFixture f;
            f.Run(sql);
            UNIT_ASSERT_C(!f.Result, sql);
            UNIT_ASSERT_C(!f.Ctx.IssueManager.GetIssues().Empty(), sql);
        }
    }

    // The wrapper issue scope must retain the source location of the SQL expression.
    Y_UNIT_TEST(DiagnosticsKeepSqlPositions) {
        TFixture f;
        f.Run("SELECT\n  id + 'bad' FROM t;");
        UNIT_ASSERT(!f.Result);
        const auto issues = f.Ctx.IssueManager.GetIssues();
        UNIT_ASSERT_VALUES_EQUAL(issues.begin()->Position.Row, 2);
        UNIT_ASSERT_VALUES_EQUAL(issues.begin()->Position.Column, 3);
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "New YQL type annotation");
    }

    // Embedding scalar annotation inside the host must preserve completed stages
    // and the surrounding transform repeat counter.
    Y_UNIT_TEST(DoesNotResetCallerCompilationStages) {
        TFixture f;
        f.Ctx.Step.Done(TExprStep::LoadTablesMetadata);
        f.Ctx.Step.Done(TExprStep::ExpandApplyForLambdas);
        f.Ctx.RepeatTransformCounter = 17;
        f.Ok("SELECT id + 1, value ?? 'none' FROM t WHERE score > 1;");
        UNIT_ASSERT(f.Ctx.Step.IsDone(TExprStep::LoadTablesMetadata));
        UNIT_ASSERT(f.Ctx.Step.IsDone(TExprStep::ExpandApplyForLambdas));
        UNIT_ASSERT(!f.Ctx.Step.IsDone(TExprStep::Configure));
        UNIT_ASSERT_VALUES_EQUAL(f.Ctx.RepeatTransformCounter, 17);
    }

    // These checks exercise the typed relational boundary without host fallback.
    Y_UNIT_TEST(TemporalConstructorsUseSqlEncoding) {
        TFixture f;
        f.Ok("SELECT Date('1998-12-01') - Interval('P90D'), DateTime64('2024-01-01T00:00:00Z');");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0), "Optional<Date>");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1), "Datetime64");
    }

    Y_UNIT_TEST(AggregationGroupingAndSetTypes) {
        TFixture f;
        f.Ok("SELECT value, SUM(id) AS s, COUNT(DISTINCT score), AVG(id) FROM t GROUP BY value HAVING s > 0 ORDER BY s;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(2), "Uint64");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(3), "Double");
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Groupings.at(&f.Query()).Aggregates.size(), 3);
        TFixture scalar;
        scalar.Ok("SELECT SUM(id), COUNT(*) FROM t;");
        UNIT_ASSERT_VALUES_EQUAL(scalar.Output(0), "Optional<Int64>");
        UNIT_ASSERT_VALUES_EQUAL(scalar.Output(1), "Uint64");
        TFixture set;
        set.Ok("SELECT id FROM t UNION ALL SELECT score FROM t;");
        UNIT_ASSERT_VALUES_EQUAL(set.Output(0), "Optional<Int64>");
    }

    Y_UNIT_TEST(GroupingSetsAndWindows) {
        TFixture f;
        f.Ok("SELECT id, GROUPING(id), SUM(SUM(score)) OVER (), RANK() OVER (ORDER BY SUM(score)) "
            "FROM t GROUP BY ROLLUP(id);");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0), "Optional<Int64>");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1), "Uint64");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(3), "Uint64");
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Groupings.at(&f.Query()).Sets.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Windows.at(&f.Query()).size(), 2);
    }

    Y_UNIT_TEST(RepeatedAggregatesShareResults) {
        TFixture f;
        f.Ok("SELECT COUNT(DISTINCT t.id), SUM(score), SUM(id) FROM t "
            "HAVING SUM(score) > 0 ORDER BY COUNT(DISTINCT id);");
        const auto& grouping = f.Result->Groupings.at(&f.Query());
        UNIT_ASSERT_VALUES_EQUAL(grouping.Aggregates.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Values.at(f.Select().Columns[0].Expression.get()),
            f.Result->Values.at(f.Select().OrderBy[0].Expression.get()));
    }

    Y_UNIT_TEST(RepeatedComputedGroupsAndAggregates) {
        TFixture f;
        f.Ok("SELECT t.id + 1, SUM(t.score + 1), SUM(score + 1), SUM(score + 2) FROM t "
            "GROUP BY id + 1 HAVING SUM(score + 1) > 0 ORDER BY id + 1, SUM(t.score + 1);");
        const auto& grouping = f.Result->Groupings.at(&f.Query());
        UNIT_ASSERT_VALUES_EQUAL(grouping.Keys.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(grouping.Aggregates.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Values.at(f.Select().Columns[0].Expression.get()),
            f.Result->Values.at(f.Select().OrderBy[0].Expression.get()));
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Values.at(f.Select().Columns[1].Expression.get()),
            f.Result->Values.at(f.Select().Columns[2].Expression.get()));
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Values.at(f.Select().Columns[1].Expression.get()),
            f.Result->Values.at(f.Select().OrderBy[1].Expression.get()));
    }

    Y_UNIT_TEST(ComputedAggregatesKeepCteSourceIdentity) {
        TFixture f;
        f.Ok("WITH c AS (SELECT id, score FROM t) "
            "SELECT SUM(l.score + 1), SUM(r.score + 1), SUM(l.score + 1) "
            "FROM c AS l JOIN c AS r ON l.id = r.id;");
        const auto& grouping = f.Result->Groupings.at(&f.Query());
        UNIT_ASSERT_VALUES_EQUAL(grouping.Aggregates.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Values.at(f.Select().Columns[0].Expression.get()),
            f.Result->Values.at(f.Select().Columns[2].Expression.get()));
        UNIT_ASSERT(f.Result->Values.at(f.Select().Columns[0].Expression.get())
            != f.Result->Values.at(f.Select().Columns[1].Expression.get()));
    }

    Y_UNIT_TEST(CorrelatedSubqueriesHaveExplicitCaptures) {
        TFixture f;
        f.Ok("SELECT l.id, (SELECT MAX(r.id) FROM t AS r WHERE r.id = l.id) FROM t AS l "
            "WHERE EXISTS (SELECT r.id FROM t AS r WHERE r.id = l.id) AND l.id IN (SELECT id FROM t);");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1), "Optional<Int64>");
        UNIT_ASSERT_VALUES_EQUAL(f.Result->Subqueries.size(), 3);
    }

    Y_UNIT_TEST(UngroupedColumnsAreRejected) {
        TFixture f;
        f.Run("SELECT id, SUM(score) FROM t;");
        UNIT_ASSERT(!f.Result);
        UNIT_ASSERT(!f.Ctx.IssueManager.GetIssues().Empty());
    }

    // Each use specializes a fresh lambda. Local bindings and nested closures
    // must retain lexical argument ownership after the shared Apply expansion.
    Y_UNIT_TEST(PreambleLambdasArePolymorphicAndCaptureLexically) {
        TFixture f;
        f.Ctx.Step.Done(TExprStep::ExpandApplyForLambdas);
        f.Ctx.RepeatTransformCounter = 17;
        f.Ok("$identity = ($x) -> ($x); $make = ($x) -> { $n = $x + 1; "
            "return ($y) -> ($n + $y); }; $zero = () -> (42); "
            "SELECT $identity(id), $identity(value), $make(id)(2), $zero() FROM t;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0, 3), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1, 3), "Optional<String>");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(2, 3), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(3, 3), "Int32");
        UNIT_ASSERT(f.Ctx.Step.IsDone(TExprStep::ExpandApplyForLambdas));
        UNIT_ASSERT_VALUES_EQUAL(f.Ctx.RepeatTransformCounter, 17);
    }

    Y_UNIT_TEST(PreambleQueriesUseTypedLambdasAndOuterJoinSchemas) {
        TFixture f;
        f.Ok("$inc = ($x) -> ($x + 1); $q = SELECT $inc(id) AS k FROM t; "
            "$r = (WITH c AS (SELECT k FROM $q) SELECT * FROM c); "
            "SELECT l.k, r.k FROM $q AS l LEFT JOIN $r AS r ON l.k = r.k;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0, 3), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1, 3), "Optional<Int64>");
    }

    Y_UNIT_TEST(PreambleLambdaCapturesSdkParameter) {
        TFixture f;
        f.Settings.Parameters["$offset"] = f.Ctx.MakeType<TDataExprType>(EDataSlot::Int64);
        f.Ok("$f = ($x) -> ($x + $offset); SELECT $f(id) FROM t;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0, 1), "Int64");
    }

    Y_UNIT_TEST(PreambleLambdaErrorsUseYqlDiagnostics) {
        for (const TString sql : {"$f = ($x) -> ($x + 1); SELECT $f();",
            "$f = ($x) -> ($x + 1); SELECT $f(1, 2);",
            "$f = ($x) -> ($x + 1); SELECT $f('text');",
            "$f = ($x) -> ($missing + $x); SELECT $f(1);"}) {
            TFixture f;
            f.Ctx.Step.Done(TExprStep::ExpandApplyForLambdas);
            f.Run(sql);
            UNIT_ASSERT_C(!f.Result, sql);
            UNIT_ASSERT_C(!f.Ctx.IssueManager.GetIssues().Empty(), sql);
            UNIT_ASSERT(f.Ctx.Step.IsDone(TExprStep::ExpandApplyForLambdas));
        }
    }

    Y_UNIT_TEST(CostBasedOptimizationLevelDoesNotChangeTypes) {
        TFixture f;
        f.Ok("PRAGMA ydb.CostBasedOptimizationLevel = '0'; SELECT id + 1, value FROM t;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0, 1), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1, 1), "Optional<String>");
    }

    // This is the pragma used by TPCH/TPCDS preambles. It enables the comma
    // spelling of Cross without changing scalar or output nullability rules.
    Y_UNIT_TEST(PreambleImplicitCrossJoinPragma) {
        TFixture f;
        f.Ok("PRAGMA AnsiImplicitCrossJoin; $q = (SELECT id FROM t); "
            "SELECT l.id, r.id FROM $q AS l, $q AS r;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0, 2), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1, 2), "Int64");
    }

}

} // namespace NKikimr::NKqp
