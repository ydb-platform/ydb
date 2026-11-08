#include <ydb/core/kqp/host/new_parser/rbo/kqp_rbo_lowering.h>
#include <ydb/core/kqp/host/new_parser/kqp_new_parser.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_test_helpers.h>

#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

namespace NKikimr::NKqp {
namespace {

// This fixture bypasses host fallback and imports the new AST directly. Its
// common checks validate IU definition/use, lambda argument ownership and
// PlanProps lifetime for every operator, not just the visible result columns.
struct TFixture : NTests::TIdTestContext {
    std::unique_ptr<NNewYql::TScript> Ast;
    std::unique_ptr<NNewYql::TResolvedNames> Names;
    std::unique_ptr<NNewYql::TAnnotatedAst> Annotations;

    TIntrusivePtr<TOpRoot> Run(const TString& sql, bool orderedColumns = false) {
        auto parsed = ParseQueryWithNewParser(sql, {}, false);
        UNIT_ASSERT_C(parsed.IsOk(), parsed.Issues.ToString());
        Ast = std::move(parsed.Ast);
        auto table = std::make_shared<NNewYql::TTableMetadata>();
        table->Path = "/Root/t";
        table->Columns = {{"id", 1, "Int64", true}, {"value", 2, "String", false}, {"a", 3, "Int32", true}};
        // RBO type recomputation consults the provider catalog independently of
        // the frontend snapshot. Register the same mock schema in both places.
        auto& description = Tables->GetOrAddTable(KqpCtx.Cluster, "/Root", table->Path);
        description.Metadata = MakeIntrusive<TKikimrTableMetadata>();
        description.Metadata->DoesExist = true;
        for (const auto& column : table->Columns) {
            description.Metadata->Columns.emplace(column.Name,
                TKikimrColumnMetadata(column.Name, column.Id, column.Type, column.NotNull));
        }
        UNIT_ASSERT(description.Load(ExprCtx));
        auto resolved = NNewYql::ResolveNames(*Ast, {"ut", "/Root", {}},
            [&](const TString&, const TString&) { return table; });
        UNIT_ASSERT_C(resolved.IsOk(), resolved.Issues.ToString());
        Names = std::move(resolved.Names);
        Annotations = NNewYql::AnnotateAst(*Ast, *Names, ExprCtx, TypeCtx);
        UNIT_ASSERT_C(Annotations, ExprCtx.IssueManager.GetIssues().ToString());
        return Lower(orderedColumns);
    }

    // Keep this separate from Run so a test can lower the same AST/annotation
    // result twice and verify that importing a plan did not consume or mutate it.
    TIntrusivePtr<TOpRoot> Lower(bool orderedColumns = false) {
        const auto& query = *std::get<NNewYql::TQueryStatement>(Ast->Statements.back().Value).Query;
        auto root = NNewYql::LowerToRbo(query, *Ast, *Names, *Annotations, ExprCtx,
            [&](const NNewYql::TTableMetadata& metadata) {
                const auto table = NNodes::Build<NNodes::TKqpTable>(ExprCtx, Pos)
                    .Path().Build(metadata.Path).PathId().Build("1:1")
                    .SysView().Build("").Version().Build("1").Done().Ptr();
                return NNewYql::TRboTable{table, NYql::EStorageType::RowStorage};
            }, orderedColumns);
        NTests::AssertIdInvariants(*root, root->PlanProps);
        for (const auto& item : IterateSubtreeWithSubplans(root.Get(), root->PlanProps)) {
            for (const auto& expression : item.Current->GetExpressions()) {
                CheckArguments(*expression.get().GetLambda());
                UNIT_ASSERT_EQUAL(expression.get().PlanProps, &root->PlanProps);
            }
        }
        return root;
    }

    TVector<IOperator*> Find(TOpRoot& root, EOperator kind) {
        TVector<IOperator*> result;
        for (const auto& item : root) {
            if (item.Current->Kind == kind) { result.push_back(item.Current); }
        }
        return result;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(KqpNewYqlRboLowering) {
    // Recompute every operator type as well as validating IU ownership. This
    // catches mismatches between scalar annotations and relational result types.
    Y_UNIT_TEST(RelationalExpressionsLowerAndReannotate) {
        for (const TString sql : {
            "SELECT value, SUM(id) AS s, COUNT(DISTINCT a) FROM t GROUP BY value HAVING s > 0 ORDER BY s;",
            "SELECT a + 1 AS k, SUM(id) FROM t GROUP BY a + 1;",
            "SELECT a, GROUPING(a), SUM(id) FROM t GROUP BY ROLLUP(a);",
            "SELECT a, SUM(SUM(id)) OVER (), RANK() OVER (ORDER BY SUM(id)) FROM t GROUP BY a;",
            "SELECT id FROM t UNION ALL SELECT a FROM t;",
            "SELECT value FROM t INTERSECT SELECT value FROM t;",
            "SELECT l.id, (SELECT MAX(r.id) FROM t AS r WHERE r.id = l.id) FROM t AS l;",
            "SELECT l.id FROM t AS l WHERE EXISTS (SELECT r.id FROM t AS r WHERE r.id = l.id);",
            "SELECT COUNT(*) FROM t AS l WHERE EXISTS (SELECT * FROM t AS r WHERE r.id = l.id AND r.a != l.a) "
                "AND NOT EXISTS (SELECT * FROM t AS r WHERE r.id = l.id);",
            "SELECT id FROM t WHERE id IN (SELECT id FROM t);"}) {
            TFixture f;
            auto root = f.Run(sql);
            auto annotator = CreateTypeAnnotationTransformer(CreateExtCallableTypeAnnotationTransformer(f.TypeCtx, true), f.TypeCtx);
            TRBOContext ctx(f.KqpCtx, f.ExprCtx, f.TypeCtx, *annotator, *f.FuncRegistry);
            UNIT_ASSERT_C(root->ComputeTypes(ctx) == IGraphTransformer::TStatus::Ok,
                sql << ": " << f.ExprCtx.IssueManager.GetIssues().ToString());
            f.Lower();
        }
    }

    // ORDER BY uses an unprojected computed value; the map must retain its input
    // and count literals must already be visible to later pushdown rules.
    Y_UNIT_TEST(ProjectionSortAndLimitKeepHiddenDependencies) {
        TFixture f;
        auto root = f.Run("PRAGMA YqlSelect = 'force'; SELECT id + 1 AS next FROM t WHERE id > 0 ORDER BY a + 2 DESC LIMIT 5 OFFSET 1;");
        UNIT_ASSERT_VALUES_EQUAL(root->GetColumns().Items().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(root->GetColumns().Items()[0].second, "next");
        UNIT_ASSERT_VALUES_EQUAL(f.Find(*root, EOperator::Filter).size(), 1);
        auto* sort = static_cast<TOpSort*>(f.Find(*root, EOperator::Sort).at(0));
        UNIT_ASSERT(!sort->GetSortElements().Unordered().HasAny(root->GetOutputIUs()));
        auto* limit = static_cast<TOpLimit*>(f.Find(*root, EOperator::Limit).at(0));
        UNIT_ASSERT(limit->HasOffset());
        UNIT_ASSERT(limit->GetLimitCond().GetRawInputIUs().Empty());
        const auto count = limit->GetLimitCond().GetExpressionBody();
        UNIT_ASSERT_C(count->IsCallable("Uint64"), count->Content());
        UNIT_ASSERT_VALUES_EQUAL(count->Head().Content(), "5");
        const auto offset = limit->GetOffsetCond()->GetExpressionBody();
        UNIT_ASSERT_C(offset->IsCallable("Uint64"), offset->Content());
        UNIT_ASSERT_VALUES_EQUAL(offset->Head().Content(), "1");
    }

    // Check both RBO type recomputation and a second import from the same inputs
    // to detect consumed AST nodes or mutated annotation lambdas.
    Y_UNIT_TEST(SelectWithoutSourceCanBeReannotated) {
        TFixture f;
        auto root = f.Run("SELECT [1, 2][0u] AS v, CAST(3 AS Int64) + 2 AS n LIMIT 1;");
        UNIT_ASSERT_VALUES_EQUAL(f.Find(*root, EOperator::EmptySource).size(), 1);
        auto annotator = CreateTypeAnnotationTransformer(CreateExtCallableTypeAnnotationTransformer(f.TypeCtx, true), f.TypeCtx);
        TRBOContext ctx(f.KqpCtx, f.ExprCtx, f.TypeCtx, *annotator, *f.FuncRegistry);
        UNIT_ASSERT_C(root->ComputeTypes(ctx) == IGraphTransformer::TStatus::Ok, f.ExprCtx.IssueManager.GetIssues().ToString());
        auto again = f.Lower();
        UNIT_ASSERT_VALUES_EQUAL(again->GetColumns().Items().size(), 2);
        for (const auto& [expression, annotation] : f.Annotations->Expressions) {
            Y_UNUSED(expression);
            CheckArguments(*annotation.Lambda);
            UNIT_ASSERT(annotation.GetType());
        }
    }

    // Reversed equality operands still form a key, while the remaining outer-join
    // condition must stay on the join rather than disappear or move to WHERE.
    Y_UNIT_TEST(SelfJoinExtractsKeysAndRetainsResidual) {
        TFixture f;
        auto root = f.Run("SELECT l.id, r.value FROM t AS l LEFT JOIN t AS r ON r.id = l.id AND r.a > 1;");
        auto* join = static_cast<TOpJoin*>(f.Find(*root, EOperator::Join).at(0));
        UNIT_ASSERT_VALUES_EQUAL(join->JoinKind, "Left");
        UNIT_ASSERT_VALUES_EQUAL(join->JoinKeys.Items().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(join->JoinFilters.size(), 1);
        UNIT_ASSERT(!join->GetLeftInput()->GetOutputIUs().HasAny(join->GetRightInput()->GetOutputIUs()));
    }

    // Shared computation and independent consumer identity are separate invariants:
    // one read should feed two ports with disjoint IU sets.
    Y_UNIT_TEST(CteReferencesShareProducerAndHaveDisjointIds) {
        TFixture f;
        auto root = f.Run("WITH c(k) AS (SELECT id + 1 FROM t) "
            "SELECT l.k AS a, r.k AS b FROM c AS l JOIN c AS r ON l.k = r.k;");
        UNIT_ASSERT_VALUES_EQUAL(f.Find(*root, EOperator::Source).size(), 1);
        auto ports = f.Find(*root, EOperator::Replicate);
        UNIT_ASSERT_VALUES_EQUAL(ports.size(), 2);
        UNIT_ASSERT(!ports[0]->GetOutputIUs().HasAny(ports[1]->GetOutputIUs()));
        auto* a = static_cast<TOpReplicate*>(ports[0]);
        auto* b = static_cast<TOpReplicate*>(ports[1]);
        UNIT_ASSERT_EQUAL(&a->GetReplicate(), &b->GetReplicate());
    }

    // Visible renames must not replace the stored column names used by the read.
    Y_UNIT_TEST(DerivedAndPhysicalColumnAliases) {
        TFixture f;
        auto root = f.Run("SELECT d.key FROM (SELECT key FROM t AS x(key, label, count)) AS d;");
        auto* read = static_cast<TOpRead*>(f.Find(*root, EOperator::Source).at(0));
        UNIT_ASSERT_VALUES_EQUAL(read->GetColumns().Size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(root->PlanProps.InfoUnitRegistry.Get(*read->GetColumns().begin()).GetColumnName(), "id");
    }

    // Presentation ordering is separate from catalog order; DISTINCT must emit
    // only its projected key IUs and exclude hidden input dependencies.
    Y_UNIT_TEST(StarPresentationAndDistinctKeys) {
        TFixture f;
        auto root = f.Run("SELECT * FROM t;");
        UNIT_ASSERT_VALUES_EQUAL(root->GetColumns().Items()[0].second, "a");
        auto ordered = f.Lower(true);
        UNIT_ASSERT_VALUES_EQUAL(ordered->GetColumns().Items()[0].second, "id");
        auto distinct = f.Run("SELECT DISTINCT value AS v FROM t ORDER BY v;");
        auto* agg = static_cast<TOpAggregate*>(f.Find(*distinct, EOperator::Aggregate).at(0));
        UNIT_ASSERT(agg->GetOutputIUs() == distinct->GetOutputIUs());
    }

    // Exercise differing output-side rules through the same ID/argument invariants.
    Y_UNIT_TEST(SemiCrossAndOuterJoinKinds) {
        for (const TString kind : {"LEFT SEMI", "RIGHT ONLY", "FULL", "EXCLUSION", "CROSS"}) {
            TFixture f;
            const auto projection = kind == "RIGHT ONLY" ? "r.id" : "l.id";
            auto root = f.Run(TStringBuilder() << "SELECT " << projection << " FROM t AS l " << kind
                << " JOIN t AS r" << (kind == "CROSS" ? ";" : " ON l.id = r.id;"));
            UNIT_ASSERT_VALUES_EQUAL(f.Find(*root, EOperator::Join).size(), 1);
        }
    }

    // Reject unsupported modifiers and unavailable DISTINCT sort dependencies
    // without producing either a partial plan or a spurious type-error issue.
    Y_UNIT_TEST(UnsupportedSemanticsDoNotSilentlyDisappear) {
        for (const TString sql : {"SELECT l.id FROM t AS l JOIN ANY t AS r ON l.id = r.id;",
            "SELECT DISTINCT id FROM t ORDER BY value;", "SELECT id FROM t ASSUME ORDER BY id;"}) {
            TFixture f;
            UNIT_ASSERT_EXCEPTION(f.Run(sql), TNewYqlParserFallback);
            UNIT_ASSERT(f.ExprCtx.IssueManager.GetIssues().Empty());
        }
    }

    // Named query aliases must share a producer while their self-join ports
    // define disjoint IUs. Reimport also checks annotation/AST immutability.
    Y_UNIT_TEST(PreambleLambdasAndNamedQueriesLowerDirectly) {
        TFixture f;
        auto root = f.Run("$offset = 1; $inc = ($x) -> ($x + $offset); "
            "$q = (SELECT $inc(id) AS k FROM t); $alias = $q; "
            "$offset = 100; SELECT l.k, r.k FROM $q AS l JOIN $alias AS r ON l.k = r.k ORDER BY l.k LIMIT 2;");
        UNIT_ASSERT_VALUES_EQUAL(f.Find(*root, EOperator::Source).size(), 1);
        const auto ports = f.Find(*root, EOperator::Replicate);
        UNIT_ASSERT_VALUES_EQUAL(ports.size(), 2);
        UNIT_ASSERT(!ports[0]->GetOutputIUs().HasAny(ports[1]->GetOutputIUs()));
        auto annotator = CreateTypeAnnotationTransformer(CreateExtCallableTypeAnnotationTransformer(f.TypeCtx, true), f.TypeCtx);
        TRBOContext ctx(f.KqpCtx, f.ExprCtx, f.TypeCtx, *annotator, *f.FuncRegistry);
        UNIT_ASSERT_C(root->ComputeTypes(ctx) == IGraphTransformer::TStatus::Ok, f.ExprCtx.IssueManager.GetIssues().ToString());
        auto again = f.Lower();
        UNIT_ASSERT_VALUES_EQUAL(f.Find(*again, EOperator::Source).size(), 1);
    }

    Y_UNIT_TEST(PreambleQueriesComposeWithWithCtesAndDerivedTables) {
        TFixture f;
        auto root = f.Run("$q = SELECT id FROM t; $r = (SELECT id + 1 AS k FROM $q); "
            "WITH c AS (SELECT k FROM $r) SELECT d.k FROM (SELECT k FROM c) AS d WHERE d.k > 0;");
        UNIT_ASSERT_VALUES_EQUAL(f.Find(*root, EOperator::Source).size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(root->GetColumns().Items()[0].second, "k");
        UNIT_ASSERT_VALUES_EQUAL(f.Find(*root, EOperator::Filter).size(), 1);
    }

}

} // namespace NKikimr::NKqp
