#include "yql_kikimr_provider_impl.h"
#include "yql_kikimr_settings.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/logical/kqp_opt_log.h>
#include <ydb/core/kqp/query_compiler/kqp_mkql_compiler.h>
#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/library/testlib/helpers.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/core/yql_expr_csee.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node_visitor.h>
#include <yql/essentials/minikql/mkql_terminator.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>
#include <yql/essentials/sql/v1/translation/context.h>
#include <yql/essentials/sql/v1/translation/source.h>

namespace NYql {

namespace {

using namespace NSQLTranslationV1;

TContext CreateDefaultParserContext(NYql::TIssues& issues) {
    NSQLTranslation::TTranslationSettings settings;
    settings.DefaultCluster = "/Cluster";
    settings.AssumeYdbOnClusterWithSlash = true;

    NSQLTranslationV1::TLexers lexers;
    NSQLTranslationV1::TParsers parsers;
    return TContext(lexers, parsers, settings, { /* hints */ }, issues, { /* query */ });
}

TAstNode* CreateAlterTable(TContext& parserContext, const TString& tableName, const TAlterTableParameters& params) {
    TTableRef tableRef(tableName, parserContext.Scoped->CurrService, parserContext.Scoped->CurrCluster, {});
    {
        TDeferredAtom tableAtom(parserContext.Pos(), tableName);
        tableRef.Keys = BuildTableKey(parserContext.Pos(), tableRef.Service, tableRef.Cluster, tableAtom, {});
    }

    auto alterTableNode = BuildAlterTable(parserContext.Pos(), tableRef, params, parserContext.Scoped);
    UNIT_ASSERT_C(alterTableNode, parserContext.Issues.ToString());
    UNIT_ASSERT_C(alterTableNode->Init(parserContext, nullptr), parserContext.Issues.ToString());
    TAstNode* alterTableAst = alterTableNode->Translate(parserContext);
    UNIT_ASSERT_C(alterTableAst, parserContext.Issues.ToString());

    UNIT_ASSERT_C(alterTableAst->IsList()
        && alterTableAst->GetChildrenCount() > 1
        && alterTableAst->GetChild(1)->IsList()
        && alterTableAst->GetChild(1)->GetChildrenCount() > 1
        && alterTableAst->GetChild(1)->GetChild(1),
        alterTableAst->ToString()
    );
    // this child represents the world
    return alterTableAst->GetChild(1)->GetChild(1);
}

void Find(const TAstNode* node, const std::function<bool(const TAstNode*)>& predicate) {
    if (predicate(node)) {
        return;
    }
    if (node->IsList()) {
        for (auto* child : node->GetChildren()) {
            Find(child, predicate);
        }
    }
}

struct TStreamingAggregationTypeAnnTest {
    TExprContext Ctx;
    TTypeAnnotationContext Types;
    const TKikimrConfiguration::TPtr Config = MakeIntrusive<TKikimrConfiguration>();

    TExprNode::TPtr Atom(TStringBuf value) {
        return Ctx.NewAtom(TPositionHandle(), value);
    }

    TExprNode::TPtr List(TExprNodeList items) {
        return Ctx.NewList(TPositionHandle(), std::move(items));
    }

    TExprNode::TPtr Traits(TStringBuf finish, TStringBuf defaultValue = "(Null)",
        TStringBuf itemType = "(StructType '('key (DataType 'String)))")
    {
        const TString program = TStringBuilder() << R"((
            (return (AggregationTraits )" << itemType << R"(
                (lambda '(item) (Int64 '0))
                (lambda '(item state) state)
                (lambda '(state) state)
                (lambda '(state) state)
                (lambda '(left right) left)
                (lambda '(state) )" << finish << ") " << defaultValue << ")) )";
        auto traits = ParseAndAnnotate(program, Ctx, /*instant=*/false, /*wholeProgram=*/false, Types);
        UNIT_ASSERT_C(traits, Ctx.IssueManager.GetIssues().ToString());
        UNIT_ASSERT(NNodes::TCoAggregationTraits::Match(traits.Get()));
        return traits;
    }

    TExprNode::TPtr Aggregation(ETypeAnnotationKind kind, TExprNodeList keys, TExprNodeList handlers,
        TExprNodeList settings = {})
    {
        const auto* rowType = Ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
            Ctx.MakeType<TItemExprType>("key", Ctx.MakeType<TDataExprType>(EDataSlot::String))});
        auto input = Ctx.NewArgument(TPositionHandle(), "input");
        input->SetTypeAnn(MakeSequenceType(kind, *rowType, Ctx));
        return Ctx.NewCallable(input->Pos(), NNodes::TKqpStreamingAggregation::CallableName(),
            {input, List(std::move(keys)), List(std::move(handlers)), List(std::move(settings))});
    }

    IGraphTransformer::TStatus Annotate(TExprNode::TPtr& node) {
        auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        const auto session = MakeIntrusive<TKikimrSessionContext>(registry.Get(), Config,
            CreateDefaultTimeProvider(), CreateDeterministicRandomProvider(1), nullptr);
        session->SetInternalTypeAnnTransformer(NKikimr::NKqp::NOpt::CreateKqpTypeAnnotationTransformer(
            session->GetCluster(), session->TablesPtr(), session->ConfigPtr()));
        auto sinkTypeAnn = CreateKiSinkTypeAnnotationTransformer(nullptr, session, Types);
        auto coreTypeAnn = CreateExtCallableTypeAnnotationTransformer(Types);
        auto callableTypeAnn = CreateFunctorTransformer([&](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            return (NNodes::TKqpStreamingAggregation::Match(input.Get()) ? sinkTypeAnn : coreTypeAnn)->Transform(input, output, ctx);
        });
        auto typeAnn = CreateTypeAnnotationTransformer(std::move(callableTypeAnn), Types);
        return SyncTransform(*typeAnn, node, Ctx);
    }

    const TStructExprType* CheckType(TExprNode::TPtr& node) {
        UNIT_ASSERT_VALUES_EQUAL_C(Annotate(node), IGraphTransformer::TStatus::Ok, Ctx.IssueManager.GetIssues().ToString());
        return GetSeqItemType(*node->GetTypeAnn()).Cast<TStructExprType>();
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(KikimrProvider) {
    Y_UNIT_TEST_OCTET(StreamingAggregationPureInputDoesNotBuildStages, Enabled, Keyed, UseStateTable) {
        auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        for (const TStringBuf input : {"input", "(Iterator input)", "(ToFlow input)",
                "(PartitionsByKeys input (lambda '(row) (Member row 'key)) (Void) (Void) (lambda '(rows) rows))"}) {
            TExprContext ctx;
            TTypeAnnotationContext types;
            const TString program = TStringBuilder() << R"((
                (let input (AsList (AsStruct '('key (String 'a)))))
                (return (Aggregate )" << input << " " << (Keyed ? "'('key)" : "'()")
                << " '() '('('compact) " << (Keyed ? "'('output_columns '())" : "") << "))))";
            auto node = ParseAndAnnotate(program, ctx, /*instant=*/false, /*wholeProgram=*/false, types);
            UNIT_ASSERT_C(node, ctx.IssueManager.GetIssues().ToString());
            auto constraints = CreateConstraintTransformer(types);
            UNIT_ASSERT_VALUES_EQUAL_C(SyncTransform(*constraints, node, ctx), IGraphTransformer::TStatus::Ok,
                ctx.IssueManager.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(UpdateCompletness(node, node, ctx), IGraphTransformer::TStatus::Ok);
            const auto original = node;
            const auto config = MakeIntrusive<TKikimrConfiguration>();
            config->FeatureFlags.SetEnableStreamingAggregation(Enabled);
            config->EnableStreamingAggregation = true;
            config->OptValidateStreamingCheckpoints = false;
            if (UseStateTable) {
                config->StreamingAggregationStateTablePath = "/Root/state";
            }
            const auto session = MakeIntrusive<TKikimrSessionContext>(registry.Get(), config,
                CreateDefaultTimeProvider(), CreateDeterministicRandomProvider(1), nullptr);
            auto optCtx = MakeIntrusive<NKikimr::NKqp::NOpt::TKqpOptimizeContext>(session->GetCluster(),
                config, session->QueryPtr(), session->TablesPtr(), nullptr);
            auto optimizer = NKikimr::NKqp::NOpt::CreateKqpLogOptTransformer(optCtx, types, config);
            UNIT_ASSERT_VALUES_EQUAL_C(SyncTransform(*optimizer, node, ctx), IGraphTransformer::TStatus::Ok,
                ctx.IssueManager.GetIssues().ToString());
            if (!Enabled) {
                UNIT_ASSERT(!FindNode(node, [](const TExprNode::TPtr& expr) {
                    return NNodes::TKqpStreamingAggregation::Match(expr.Get());
                }));
                continue;
            }
            const auto aggregation = node;
            UNIT_ASSERT(NNodes::TKqpStreamingAggregation::Match(aggregation.Get()));
            UNIT_ASSERT_C(aggregation->HeadPtr() == original->HeadPtr(),
                "Pure input must be preserved without wrapping it in a stage or connection");
            UNIT_ASSERT(aggregation->ChildPtr(1) == original->ChildPtr(1));
            UNIT_ASSERT(aggregation->ChildPtr(2) == original->ChildPtr(2));
            const auto& settings = aggregation->Child(NNodes::TKqpStreamingAggregation::idx_Settings);
            UNIT_ASSERT_VALUES_EQUAL(settings->ChildrenSize(), ui32(Keyed) + ui32(UseStateTable));
            UNIT_ASSERT(GetSetting(*settings, "output_columns") == GetSetting(original->Tail(), "output_columns"));
            if (UseStateTable) {
                const auto stateTablePath = GetSetting(*settings, "state_table_path");
                UNIT_ASSERT(stateTablePath);
                UNIT_ASSERT_VALUES_EQUAL(stateTablePath->Tail().Content(), "/Root/state");
            }
        }
    }

    Y_UNIT_TEST(KqpPureExprExcludesReads) {
        TExprContext ctx;
        for (const TStringBuf callable : {"KqlReadTable", "KqlReadTableIndex", "KqpReadTable",
                "KqlReadTableRanges", "KqpReadOlapTableRanges", "KqpBlockReadOlapTableRanges",
                "KqlReadTableFullTextIndex", "KqpReadTableFullTextIndex", "KqlReadTableVectorIndex",
                "KqlStreamLookupTable", "KqlStreamLookupIndex", "KqpLookupTable", "DataSource", "DqSource",
                "DqReadWrap", "DqReadWideWrap", "DqReadBlockWideWrap"}) {
            const auto read = ctx.NewCallable(TPositionHandle(), callable, {});
            UNIT_ASSERT_C(!NKikimr::NKqp::NOpt::IsKqpPureExpr(NNodes::TExprBase(read),
                /* checkDqSources */ true, /* checkIndexReads */ true), callable);
            const auto nested = ctx.NewList(TPositionHandle(), {read});
            UNIT_ASSERT_C(!NKikimr::NKqp::NOpt::IsKqpPureExpr(NNodes::TExprBase(nested),
                /* checkDqSources */ true, /* checkIndexReads */ true), callable);
        }
    }

    Y_UNIT_TEST_QUAD(KqpPureExprSourceFlags, CheckDqSources, CheckIndexReads) {
        TExprContext ctx;
        const auto check = [&](TStringBuf callable, bool hasDqSource, bool hasIndexRead) {
            const auto body = ctx.NewCallable(TPositionHandle(), callable, {});
            for (const auto& expr : {body, ctx.NewList(TPositionHandle(), {body})}) {
                const bool expectedPure = !(CheckDqSources && hasDqSource) && !(CheckIndexReads && hasIndexRead);
                UNIT_ASSERT_VALUES_EQUAL_C(NKikimr::NKqp::NOpt::IsKqpPureExpr(NNodes::TExprBase(expr),
                    CheckDqSources, CheckIndexReads), expectedPure, callable);
            }
        };

        check("AsList", false, false);
        for (const TStringBuf callable : {"DataSource", "DqSource", "DqReadWrap", "DqReadWideWrap", "DqReadBlockWideWrap"}) {
            check(callable, true, false);
        }
        for (const TStringBuf callable : {"KqlReadTableFullTextIndex", "KqpReadTableFullTextIndex", "KqlReadTableVectorIndex"}) {
            check(callable, false, true);
        }
    }

    Y_UNIT_TEST(KqpPureLambdaPreservesClassification) {
        TExprContext ctx;
        const auto check = [&](TStringBuf callable, bool expectedPure) {
            const auto body = ctx.NewCallable(TPositionHandle(), callable, {});
            for (const auto& expr : {body, ctx.NewList(TPositionHandle(), {body})}) {
                UNIT_ASSERT_VALUES_EQUAL_C(NKikimr::NKqp::NOpt::IsKqpPureExpr(NNodes::TExprBase(expr)), expectedPure, callable);
                const auto lambda = NNodes::Build<NNodes::TCoLambda>(ctx, TPositionHandle())
                    .Args({})
                    .Body(expr)
                    .Done();
                UNIT_ASSERT_VALUES_EQUAL_C(NKikimr::NKqp::NOpt::IsKqpPureLambda(lambda), expectedPure, callable);
            }
        };

        // Preserve historical transaction purity, including reads rejected by the streaming rewrite.
        for (const TStringBuf callable : {"AsList", "DataSource", "DqSource", "DqReadWrap", "DqReadWideWrap",
                "DqReadBlockWideWrap", "KqlReadTableFullTextIndex", "KqpReadTableFullTextIndex",
                "KqlReadTableVectorIndex"}) {
            check(callable, true);
        }
        for (const TStringBuf callable : {"KqlReadTable", "KqlReadTableIndex", "KqpReadTable",
                "KqlReadTableRanges", "KqpReadOlapTableRanges", "KqpBlockReadOlapTableRanges",
                "KqlStreamLookupTable", "KqlStreamLookupIndex", "KqpLookupTable", "KqlUpsertRows", "KqlDeleteRows"}) {
            check(callable, false);
        }
    }

    Y_UNIT_TEST_QUAD(StreamingAggregationTupleResultTypes, Keyed, Optional) {
        // Unlike fused SQL percentiles, these tuple elements have different optionality.
        TStreamingAggregationTypeAnnTest test;
        auto traits = test.Traits(Optional
            ? "(Just '((Int64 '1) (Just (Int64 '2))))"
            : "'((Int64 '1) (Just (Int64 '2)))");
        auto node = test.Aggregation(ETypeAnnotationKind::Flow,
            Keyed ? TExprNodeList{test.Atom("key")} : TExprNodeList{},
            {test.List({test.List({test.Atom("first"), test.Atom("second")}), traits})});
        const auto* result = test.CheckType(node);
        const auto* intType = test.Ctx.MakeType<TDataExprType>(EDataSlot::Int64);
        const auto* optionalType = test.Ctx.MakeType<TOptionalExprType>(intType);
        UNIT_ASSERT(IsSameAnnotation(*result->FindItemType("first"),
            *(Optional || !Keyed ? static_cast<const TTypeAnnotationNode*>(optionalType) : intType)));
        UNIT_ASSERT(IsSameAnnotation(*result->FindItemType("second"), *optionalType));
    }

    Y_UNIT_TEST(StreamingAggregationTraitInputTypes) {
        const TStringBuf rowType = "(StructType '('key (DataType 'String)))";
        const TStringBuf optionalRowType = "(StructType '('key (OptionalType (DataType 'String))))";
        const TStringBuf nestedRowType = "(StructType '('key (StructType '('nested (DataType 'String)))))";
        struct TCase {
            TStringBuf InputType;
            TStringBuf TraitType;
            TStringBuf ExpectedError;
        };
        const TCase cases[] = {
            {rowType, "(DataType 'String)", "Expected struct type"},
            {rowType, "(OptionalType (StructType '('key (DataType 'String))))", "Expected struct type"},
            {rowType, "(StructType '('missing (DataType 'String)))", "must be a subset"},
            {rowType, "(StructType '('key (DataType 'Uint64)))", "must be a subset"},
            {rowType, optionalRowType, "must be a subset"},
            {optionalRowType, rowType, "must be a subset"},
            {nestedRowType, "(StructType '('key (StructType '('nested (DataType 'Uint64)))))", "must be a subset"},
            {rowType, rowType, {}},
            {rowType, "(StructType)", {}},
            {optionalRowType, optionalRowType, {}},
            {nestedRowType, nestedRowType, {}},
        };
        for (const auto& testCase : cases) {
            TStreamingAggregationTypeAnnTest test;
            const auto inputType = ParseAndAnnotate(TStringBuilder() << "((return " << testCase.InputType << "))",
                test.Ctx, false, false, test.Types);
            UNIT_ASSERT_C(inputType, test.Ctx.IssueManager.GetIssues().ToString());
            auto node = test.Aggregation(ETypeAnnotationKind::Flow, {},
                {test.List({test.Atom("value"), test.Traits("(Int64 '1)", "(Null)", testCase.TraitType)})},
                {test.List({test.Atom("output_columns"), test.List({})})});
            node->HeadPtr()->SetTypeAnn(test.Ctx.MakeType<TFlowExprType>(inputType->GetTypeAnn()->Cast<TTypeExprType>()->GetType()));
            // The handler still consumes input when its result is projected away.
            UNIT_ASSERT_VALUES_EQUAL_C(test.Annotate(node), testCase.ExpectedError.empty()
                ? IGraphTransformer::TStatus::Ok : IGraphTransformer::TStatus::Error,
                test.Ctx.IssueManager.GetIssues().ToString());
            if (!testCase.ExpectedError.empty()) {
                UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), testCase.ExpectedError);
            }
        }
    }

    Y_UNIT_TEST(StreamingAggregationRejectsNonComputableInput) {
        TStreamingAggregationTypeAnnTest test;
        auto node = test.Aggregation(ETypeAnnotationKind::Flow, {}, {});
        const auto* type = test.Ctx.MakeType<TTypeExprType>(test.Ctx.MakeType<TDataExprType>(EDataSlot::Int64));
        const auto* rowType = test.Ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
            test.Ctx.MakeType<TItemExprType>("type", type)});
        node->HeadPtr()->SetTypeAnn(test.Ctx.MakeType<TFlowExprType>(rowType));
        UNIT_ASSERT_VALUES_EQUAL(test.Annotate(node), IGraphTransformer::TStatus::Error);
        UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), "Expected computable data");
    }

    Y_UNIT_TEST_TWIN(StreamingAggregationRejectsInvalidTupleResults, OptionalFinish) {
        for (const ui32 testCase : {0, 1, 2}) {
            TStreamingAggregationTypeAnnTest test;
            const TStringBuf body = testCase == 0 ? "(Int64 '1)" : testCase == 1
                ? "'((Int64 '1))" : "'((Int64 '1) (Int64 '2))";
            const TString finish = OptionalFinish ? TStringBuilder() << "(Just " << body << ")" : TString(body);
            auto node = test.Aggregation(ETypeAnnotationKind::Flow, {},
                {test.List({test.List({test.Atom("first"), test.Atom(testCase == 2 ? "first" : "second")}),
                    test.Traits(finish)})},
                {test.List({test.Atom("output_columns"), test.List({})})});
            UNIT_ASSERT_VALUES_EQUAL(test.Annotate(node), IGraphTransformer::TStatus::Error);
            UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), testCase == 2
                ? "Duplicated member" : testCase == 1 ? "Expected tuple type of size: 2" : "Expected tuple type");
        }
    }

    Y_UNIT_TEST(StreamingAggregationInvalidSettings) {
        for (const ui32 settingCase : {0, 1, 2, 3, 4, 5, 6, 7}) {
            TStreamingAggregationTypeAnnTest test;
            TExprNodeList setting;
            switch (settingCase) {
                case 0: setting = {test.Atom("state_table_path")}; break;
                case 1: setting = {test.Atom("state_table_path"), test.List({})}; break;
                case 2: setting = {test.Atom("state_table_path"), test.Atom("/Root/state"), test.Atom("extra")}; break;
                case 3: setting = {test.Atom("unsupported")}; break;
                case 4: setting = {test.Atom("compact")}; break;
                case 5: setting = {test.Atom("output_columns")}; break;
                case 6: setting = {test.Atom("output_columns"), test.List({test.List({})})}; break;
                case 7: setting = {test.Atom("output_columns"), test.List({test.Atom("missing")})}; break;
            }
            auto node = test.Aggregation(ETypeAnnotationKind::List, {test.Atom("key")}, {}, {test.List(std::move(setting))});
            UNIT_ASSERT_VALUES_EQUAL_C(test.Annotate(node), IGraphTransformer::TStatus::Error,
                test.Ctx.IssueManager.GetIssues().ToString());
            if (settingCase == 4) {
                UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), "Unexpected setting: compact");
            } else if (settingCase == 7) {
                UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), "Unknown output column missing");
            }
        }
    }

    Y_UNIT_TEST_TWIN(StreamingAggregationOutputColumns, EmptyOutput) {
        // SQL prunes unused handlers and tuple elements before the streaming rewrite.
        // Keep this internal shape to test projection without changing the stored state.
        TStreamingAggregationTypeAnnTest test;
        auto node = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.List({test.Atom("first"), test.Atom("second")}),
                test.Traits("(Just '((Int64 '1) (Just (Int64 '2))))")}),
             test.List({test.Atom("scalar"), test.Traits("(Int64 '3)")})},
            {test.List({test.Atom("output_columns"), test.List(EmptyOutput ? TExprNodeList{}
                : TExprNodeList{test.Atom("first"), test.Atom("scalar")})})});
        const auto* result = test.CheckType(node);
        UNIT_ASSERT_VALUES_EQUAL(result->GetSize(), EmptyOutput ? 0 : 2);
        UNIT_ASSERT_VALUES_EQUAL(node->Child(1)->ChildrenSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(node->Child(2)->ChildrenSize(), 2);
        if (!EmptyOutput) {
            const auto* intType = test.Ctx.MakeType<TDataExprType>(EDataSlot::Int64);
            UNIT_ASSERT(IsSameAnnotation(*result->FindItemType("first"),
                *test.Ctx.MakeType<TOptionalExprType>(intType)));
            UNIT_ASSERT(IsSameAnnotation(*result->FindItemType("scalar"), *intType));
        }
    }

    Y_UNIT_TEST_TWIN(StreamingAggregationInvalidColumns, Duplicate) {
        TStreamingAggregationTypeAnnTest test;
        auto node = test.Aggregation(ETypeAnnotationKind::List, {test.Atom(Duplicate ? "key" : "missing")},
            {test.List({test.Atom("key"), test.Traits("(Int64 '1)")})});
        UNIT_ASSERT_VALUES_EQUAL_C(test.Annotate(node), IGraphTransformer::TStatus::Error,
            test.Ctx.IssueManager.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(StreamingAggregationProjectedStateMustBePersistable, UseStateTable) {
        const TStringBuf stateTablePath = UseStateTable ? "/Root/state" : "";
        TStreamingAggregationTypeAnnTest test;
        test.Config->DisableCheckpoints = UseStateTable;
        const auto traits = ParseAndAnnotate(R"((
            (return (AggregationTraits
                (StructType '('key (DataType 'String)))
                (lambda '(item) (InstanceOf (ResourceType 'TestState)))
                (lambda '(item state) state)
                (lambda '(state) state)
                (lambda '(saved) saved)
                (lambda '(left right) (Void))
                (lambda '(state) (Int64 '1))
                (Null)))
        ))", test.Ctx, false, false, test.Types);
        UNIT_ASSERT_C(traits, test.Ctx.IssueManager.GetIssues().ToString());
        auto node = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), traits})},
            {test.List({test.Atom("state_table_path"), test.Atom(stateTablePath)}),
             test.List({test.Atom("output_columns"), test.List({})})});
        // SQL optimization removes unused handlers before the streaming rewrite. Construct one directly
        // to check that projecting away its result does not bypass validation of its saved state.
        UNIT_ASSERT_VALUES_EQUAL_C(test.Annotate(node), IGraphTransformer::TStatus::Error,
            test.Ctx.IssueManager.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), "Expected persistable data, but got:");
    }

    Y_UNIT_TEST(StreamingAggregationRejectsMissingUpdate) {
        TStreamingAggregationTypeAnnTest test;
        const auto traits = ParseAndAnnotate(R"((
            (return (AggregationTraits
                (StructType '('key (DataType 'String)))
                (lambda '(item) (Int64 '0))
                (lambda '(item state) (Void))
                (lambda '(state) state)
                (lambda '(state) state)
                (lambda '(left right) left)
                (lambda '(state) state)
                (Null)))
        ))", test.Ctx, false, false, test.Types);
        UNIT_ASSERT_C(traits, test.Ctx.IssueManager.GetIssues().ToString());
        auto node = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), traits})});
        UNIT_ASSERT_VALUES_EQUAL(test.Annotate(node), IGraphTransformer::TStatus::Error);
        UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(),
            "Update handler must be specified for streaming aggregation");
    }

    Y_UNIT_TEST(LegacySyntaxVersionIsNormalized) {
        NYql::NProto::TTranslationSettings serializedSettings;
        serializedSettings.SetSyntaxVersion(0);

        NSQLTranslation::TTranslationSettings settings;
        NSQLTranslation::Deserialize(serializedSettings, settings);

        UNIT_ASSERT_VALUES_EQUAL(settings.SyntaxVersion, 1);
    }

    Y_UNIT_TEST(SystemColumnsMatchCoreScheme) {
        const auto& schemeColumns = NKikimr::GetSystemColumns();
        const auto& kikimrColumns = KikimrSystemColumns();

        UNIT_ASSERT_VALUES_EQUAL(kikimrColumns.size(), schemeColumns.size());
        for (const auto& [name, schemeColumn] : schemeColumns) {
            const auto* kikimrType = kikimrColumns.FindPtr(name);
            UNIT_ASSERT_C(kikimrType, "Missing KQP system column: " << name);
            UNIT_ASSERT_VALUES_EQUAL(*kikimrType, NKikimr::NUdf::GetDataSlot(schemeColumn.TypeId));
        }

        const auto* partitionColumn = schemeColumns.FindPtr(NKikimr::YqlPartitionColumnName);
        UNIT_ASSERT(partitionColumn);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(partitionColumn->ColumnId),
            static_cast<ui32>(NKikimr::TKeyDesc::EColumnIdDataShard));
    }

    Y_UNIT_TEST(TestFillAuthPropertiesNone) {
        THashMap<TString, TString> properties;
        TExternalSource source;
        source.DataSourceAuth.MutableNone();
        FillAuthProperties(properties, source);
        UNIT_ASSERT_VALUES_EQUAL(properties.size(), 1);
        auto it = properties.find("authMethod");
        UNIT_ASSERT(it != properties.end());
        UNIT_ASSERT_VALUES_EQUAL(it->second, "NONE");
    }

    Y_UNIT_TEST(TestFillAuthPropertiesServiceAccount) {
        THashMap<TString, TString> properties;
        TExternalSource source;
        auto& sa = *source.DataSourceAuth.MutableServiceAccount();
        sa.SetId("saId");
        sa.SetSecretName("secretName");
        source.ServiceAccountIdSignature = "saSignature";
        FillAuthProperties(properties, source);
        UNIT_ASSERT_VALUES_EQUAL(properties.size(), 4);
        {
            auto it = properties.find("authMethod");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "SERVICE_ACCOUNT");
        }
        {
            auto it = properties.find("serviceAccountId");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "saId");
        }
        {
            auto it = properties.find("serviceAccountIdSignature");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "saSignature");
        }
        {
            auto it = properties.find("serviceAccountIdSignatureReference");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "secretName");
        }
    }

    Y_UNIT_TEST(TestFillAuthPropertiesBasic) {
        THashMap<TString, TString> properties;
        TExternalSource source;
        auto& sa = *source.DataSourceAuth.MutableBasic();
        sa.SetLogin("login");
        sa.SetPasswordSecretName("passwordSecretName");
        source.Password = "password";
        FillAuthProperties(properties, source);
        UNIT_ASSERT_VALUES_EQUAL(properties.size(), 4);
        {
            auto it = properties.find("authMethod");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "BASIC");
        }
        {
            auto it = properties.find("login");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "login");
        }
        {
            auto it = properties.find("password");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "password");
        }
        {
            auto it = properties.find("passwordReference");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "passwordSecretName");
        }
    }

    Y_UNIT_TEST(TestFillAuthPropertiesMdbBasic) {
        THashMap<TString, TString> properties;
        TExternalSource source;
        auto& sa = *source.DataSourceAuth.MutableMdbBasic();
        sa.SetServiceAccountId("saId");
        sa.SetServiceAccountSecretName("secretName");
        source.ServiceAccountIdSignature = "saSignature";
        sa.SetLogin("login");
        sa.SetPasswordSecretName("passwordSecretName");
        source.Password = "password";
        FillAuthProperties(properties, source);
        UNIT_ASSERT_VALUES_EQUAL(properties.size(), 7);
        {
            auto it = properties.find("authMethod");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "MDB_BASIC");
        }
        {
            auto it = properties.find("login");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "login");
        }
        {
            auto it = properties.find("password");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "password");
        }
        {
            auto it = properties.find("passwordReference");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "passwordSecretName");
        }
        {
            auto it = properties.find("serviceAccountId");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "saId");
        }
        {
            auto it = properties.find("serviceAccountIdSignature");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "saSignature");
        }
        {
            auto it = properties.find("serviceAccountIdSignatureReference");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "secretName");
        }
    }

    Y_UNIT_TEST(TestFillAuthPropertiesAws) {
        THashMap<TString, TString> properties;
        TExternalSource source;
        auto& sa = *source.DataSourceAuth.MutableAws();
        sa.SetAwsAccessKeyIdSecretName("accessIdName");
        sa.SetAwsSecretAccessKeySecretName("accessSecretName");
        sa.SetAwsRegion("region");
        source.AwsAccessKeyId = "accessId";
        source.AwsSecretAccessKey = "accessSecret";
        FillAuthProperties(properties, source);
        UNIT_ASSERT_VALUES_EQUAL(properties.size(), 6);
        {
            auto it = properties.find("authMethod");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "AWS");
        }
        {
            auto it = properties.find("awsAccessKeyId");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "accessId");
        }
        {
            auto it = properties.find("awsSecretAccessKey");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "accessSecret");
        }
        {
            auto it = properties.find("awsAccessKeyIdReference");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "accessIdName");
        }
        {
            auto it = properties.find("awsSecretAccessKeyReference");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "accessSecretName");
        }
        {
            auto it = properties.find("awsRegion");
            UNIT_ASSERT(it != properties.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, "region");
        }
    }

    // test the YQL pipeline from the moment of the alter table AST node creation to the TKiAlterTable TExprNode
    Y_UNIT_TEST(AlterTableAddIndexWithTableSettings) {
        NYql::TIssues issues;
        auto parserContext = CreateDefaultParserContext(issues);

        TAlterTableParameters params;
        {
            NSQLTranslationV1::TIndexDescription indexDescription(TIdentifier(parserContext.Pos(), "index"));
            indexDescription.TableSettings.MinPartitions = new TLiteralNumberNode<i32>(parserContext.Pos(), "Int32", "12345");
            indexDescription.TableSettings.MaxPartitions = new TLiteralNumberNode<i32>(parserContext.Pos(), "Int32", "54321");
            params.AddIndexes.emplace_back(std::move(indexDescription));
        }

        TString tableName = "table";
        auto* alterTableAst = CreateAlterTable(parserContext, tableName, params);

        TString tableSettingsAst;
        Find(alterTableAst, [&tableSettingsAst](const TAstNode* node) {
            if (node->IsList()
                && node->GetChildrenCount() == 2
                && node->GetChild(0)->ToString() == "'tableSettings"
            ) {
                tableSettingsAst = node->GetChild(1)->ToString();
                return true;
            }
            return false;
        });
        UNIT_ASSERT_STRINGS_EQUAL_C(
            tableSettingsAst, R"('('('minPartitions (Int32 '"12345")) '('maxPartitions (Int32 '"54321"))))",
            alterTableAst->ToString()
        );

        TExprContext exprContext;
        TExprNode::TPtr alterTableExpr;
        UNIT_ASSERT_C(CompileExpr(*alterTableAst, alterTableExpr, exprContext, nullptr, nullptr),
            exprContext.IssueManager.GetIssues().ToString()
        );

        UNIT_ASSERT_GT_C(alterTableExpr->ChildrenSize(), 4, alterTableExpr->Dump());
        const auto* writeSettingsNode = alterTableExpr->Child(4);
        std::optional<NCommon::TWriteTableSettings> writeSettings;
        try {
            writeSettings = NCommon::ParseWriteTableSettings(NNodes::TExprList(writeSettingsNode), exprContext);
        } catch (...) {
            UNIT_FAIL(CurrentExceptionMessage());
        }

        TKikimrKey key(exprContext);
        UNIT_ASSERT_C(key.Extract(*alterTableExpr->Child(2)), alterTableExpr->Child(2)->Dump());
        UNIT_ASSERT_STRINGS_EQUAL(key.GetTablePath(), tableName);
        UNIT_ASSERT(writeSettings->AlterActions);
        UNIT_ASSERT(!writeSettings->AlterActions.Cast().Empty());

        auto alterActions = TExprNode::TPtr(writeSettings->AlterActions.MutableRaw());
        TString tableSettingsExpr;
        VisitExpr(alterActions, [&](const TExprNode::TPtr& node) {
            if (node->IsList()
                && node->ChildrenSize() == 2
                && node->Child(0)->IsAtom("tableSettings")
            ) {
                tableSettingsExpr = NCommon::SerializeExpr(exprContext, *node->Child(1));
                return false;
            }
            return true;
        });
        UNIT_ASSERT_STRING_CONTAINS_C(
            tableSettingsExpr, R"('('('minPartitions (Int32 '"12345")) '('maxPartitions (Int32 '"54321"))))",
            NCommon::SerializeExpr(exprContext, *alterActions)
        );

        // the main result of the test is the TKiAlterTable TExprNode built from the initial alter table AST node
        UNIT_ASSERT_C(NNodes::Build<NNodes::TKiAlterTable>(exprContext, alterTableExpr->Pos())
            .World(alterTableExpr->Child(0))
            .DataSink(alterTableExpr->Child(1))
            .Table().Build(key.GetTablePath())
            .Actions(writeSettings->AlterActions.Cast())
            .TableType().Build("table")
            .Done()
            .Ptr(),
            alterTableExpr->Dump()
        );
    }
}

} // namespace NYql
