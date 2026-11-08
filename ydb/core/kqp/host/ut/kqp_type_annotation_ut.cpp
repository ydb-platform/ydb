#include <ydb/core/kqp/host/new_parser/kqp_type_annotation.h>
#include <ydb/core/kqp/host/new_parser/kqp_new_parser.h>

#include <yql/essentials/core/yql_type_annotation.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>

namespace NKikimr::NKqp {
namespace {

using namespace NNewYql;
using namespace NYql;

struct TFixture {
    TExprContext Ctx;
    TTypeAnnotationContext Types;
    TAstTypeAnnotationSettings Settings;
    std::unique_ptr<TScript> Ast;
    std::unique_ptr<TResolvedNames> Names;
    std::unique_ptr<TAnnotatedAst> Result;

    void Run(const TString& sql) {
        auto parsed = ParseQueryWithNewParser(sql, {}, false);
        UNIT_ASSERT_C(parsed.IsOk(), parsed.Issues.ToString());
        Ast = std::move(parsed.Ast);
        auto table = std::make_shared<TTableMetadata>();
        table->Columns = {{"id", 1, "Int64", true}, {"value", 2, "String", false}, {"score", 3, "Int32", false},
            {"amount", 4, "Decimal(12,2)", true}};
        auto names = ResolveNames(*Ast, {"local", "/Root", {}}, [&](const TString&, const TString&) { return table; });
        UNIT_ASSERT_C(names.IsOk(), names.Issues.ToString());
        Names = std::move(names.Names);
        Result = AnnotateAst(*Ast, *Names, Ctx, Types, Settings);
    }

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

    Y_UNIT_TEST(CteAndDerivedOutputsAreTypedBeforeConsumers) {
        TFixture f;
        f.Ok("WITH c(key, label) AS (SELECT id + 1, value ?? 'none' FROM t) "
            "SELECT d.key + 1, d.label FROM (SELECT key, label FROM c) AS d;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1), "String");
        UNIT_ASSERT_VALUES_EQUAL(f.Result->QueryOutputs.size(), 3);
        for (const auto type : f.Result->Columns) { UNIT_ASSERT(type); }
    }

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

    Y_UNIT_TEST(CteSelfJoinHasIndependentNullability) {
        TFixture f;
        f.Ok("WITH c AS (SELECT id FROM t) SELECT l.id, r.id "
            "FROM c AS l LEFT JOIN c AS r ON l.id = r.id;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1), "Optional<Int64>");
        const auto& cte = *f.Query().With[0].Query;
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Result->QueryOutputs.at(&cte)[0]), "Int64");
    }

    Y_UNIT_TEST(LimitOffsetAndParameters) {
        TFixture f;
        f.Settings.Parameters["$p"] = f.Ctx.MakeType<TDataExprType>(EDataSlot::Int64);
        f.Ok("SELECT id + $p FROM t LIMIT 10 OFFSET NULL;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0), "Int64");
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Result->Expressions.at(f.Query().Limit.get()).GetType()), "Uint64");
        UNIT_ASSERT_VALUES_EQUAL(FormatType(f.Result->Expressions.at(f.Query().Offset.get()).GetType()), "Uint64");
    }

    Y_UNIT_TEST(MultipleStatementsAndExpressionSettings) {
        TFixture f;
        f.Settings.Expressions.CheckedOps = true;
        f.Settings.Expressions.UnicodeLiterals = true;
        f.Ok("SELECT id + 1 FROM t; SELECT 'text', -9223372036854775808l;");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0), "Optional<Int64>");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(0, 1), "Utf8");
        UNIT_ASSERT_VALUES_EQUAL(f.Output(1, 1), "Int64");
    }

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

    Y_UNIT_TEST(DiagnosticsKeepSqlPositions) {
        TFixture f;
        f.Run("SELECT\n  id + 'bad' FROM t;");
        UNIT_ASSERT(!f.Result);
        const auto issues = f.Ctx.IssueManager.GetIssues();
        UNIT_ASSERT_VALUES_EQUAL(issues.begin()->Position.Row, 2);
        UNIT_ASSERT_VALUES_EQUAL(issues.begin()->Position.Column, 3);
        UNIT_ASSERT_STRING_CONTAINS(issues.ToString(), "New YQL type annotation");
    }

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

    Y_UNIT_TEST(UnsupportedRelationalSemanticsFallBack) {
        for (const TString sql : {"SELECT COUNT(*) FROM t;", "SELECT id FROM t GROUP BY id;",
            "SELECT SUM(id) OVER () FROM t;", "SELECT (SELECT id FROM t);", "SELECT value LIKE 'a%' FROM t;"}) {
            TFixture f;
            UNIT_ASSERT_EXCEPTION(f.Run(sql), TNewYqlParserFallback);
            UNIT_ASSERT(!f.Result);
            UNIT_ASSERT(f.Ctx.IssueManager.GetIssues().Empty());
        }
    }
}

} // namespace NKikimr::NKqp
