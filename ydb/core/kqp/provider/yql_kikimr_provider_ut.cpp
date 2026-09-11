#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/logical/kqp_opt_log.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>

#include <ydb/core/scheme/scheme_tabledefs.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/core/yql_expr_csee.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>
#include <yql/essentials/sql/v1/translation/context.h>
#include <yql/essentials/sql/v1/translation/source.h>

#include <library/cpp/testing/unittest/registar.h>

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

    TExprNode::TPtr Atom(TStringBuf value) {
        return Ctx.NewAtom(TPositionHandle(), value);
    }

    TExprNode::TPtr List(TExprNodeList items) {
        return Ctx.NewList(TPositionHandle(), std::move(items));
    }

    TExprNode::TPtr Traits(TStringBuf finish, TStringBuf defaultValue = "(Null)") {
        const TString program = TStringBuilder() << R"((
            (return (AggregationTraits
                (StructType '('key (DataType 'String)))
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
        const auto session = MakeIntrusive<TKikimrSessionContext>(registry.Get(), MakeIntrusive<TKikimrConfiguration>(),
            CreateDefaultTimeProvider(), CreateDeterministicRandomProvider(1), nullptr);
        session->SetInternalTypeAnnTransformer(NKikimr::NKqp::NOpt::CreateKqpTypeAnnotationTransformer(
            session->GetCluster(), session->TablesPtr(), session->ConfigPtr()));
        UNIT_ASSERT(session->GetInternalTypeAnnTransformer()->CanParse(*node));
        auto typeAnn = CreateKiSinkTypeAnnotationTransformer(nullptr, session, Types);
        return SyncTransform(*typeAnn, node, Ctx);
    }

    const TStructExprType* CheckType(TExprNode::TPtr& node) {
        UNIT_ASSERT_VALUES_EQUAL_C(Annotate(node), IGraphTransformer::TStatus::Ok, Ctx.IssueManager.GetIssues().ToString());
        return GetSeqItemType(*node->GetTypeAnn()).Cast<TStructExprType>();
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(KikimrProvider) {
    Y_UNIT_TEST(AggregateStreamingInputIsRejectedAtLowering) {
        auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        for (const bool streamingInput : {false, true}) {
            for (const bool keyed : {false, true}) {
                TExprContext ctx;
                TTypeAnnotationContext types;
                const TString program = TStringBuilder() << R"((
                    (let input (AsList (AsStruct '('key (String 'a)))))
                )" << (streamingInput ? R"((let input (AssumeConstraints input '"{\"Streaming\" = #}")))" : "")
                    << "(return (Aggregate input " << (keyed ? "'('key)" : "'()") << " '() '())))";
                auto node = ParseAndAnnotate(program, ctx, /*instant=*/false, /*wholeProgram=*/false, types);
                UNIT_ASSERT_C(node, ctx.IssueManager.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(UpdateCompletness(node, node, ctx), IGraphTransformer::TStatus::Ok);

                auto constraints = CreateConstraintTransformer(types);
                UNIT_ASSERT_VALUES_EQUAL_C(SyncTransform(*constraints, node, ctx), IGraphTransformer::TStatus::Ok,
                    ctx.IssueManager.GetIssues().ToString());
                UNIT_ASSERT(NNodes::TCoAggregate::Match(node.Get()));
                UNIT_ASSERT_VALUES_EQUAL(node->Head().GetConstraint<TStreamingConstraintNode>() != nullptr, streamingInput);
                UNIT_ASSERT_VALUES_EQUAL(node->GetConstraint<TStreamingConstraintNode>() != nullptr, streamingInput);

                const auto config = MakeIntrusive<TKikimrConfiguration>();
                const auto session = MakeIntrusive<TKikimrSessionContext>(registry.Get(), config,
                    CreateDefaultTimeProvider(), CreateDeterministicRandomProvider(1), nullptr);
                auto optCtx = MakeIntrusive<NKikimr::NKqp::NOpt::TKqpOptimizeContext>(session->GetCluster(),
                    config, session->QueryPtr(), session->TablesPtr(), nullptr);
                auto optimizer = NKikimr::NKqp::NOpt::CreateKqpLogOptTransformer(optCtx, types, config);
                const auto status = SyncTransform(*optimizer, node, ctx);
                UNIT_ASSERT_VALUES_EQUAL_C(status,
                    streamingInput ? IGraphTransformer::TStatus::Error : IGraphTransformer::TStatus::Ok,
                    ctx.IssueManager.GetIssues().ToString());
                if (streamingInput) {
                    UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(),
                        "Aggregation of streaming input without windows is not supported");
                }
            }
        }
    }

    Y_UNIT_TEST(StreamingAggregationPureInputDoesNotBuildStages) {
        auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        for (const TStringBuf input : {"input", "(Iterator input)", "(ToFlow input)",
                "(PartitionsByKeys input (lambda '(row) (Member row 'key)) (Void) (Void) (lambda '(rows) rows))"}) {
            for (const bool keyed : {false, true}) {
                for (const bool useStateTable : {false, true}) {
                    TExprContext ctx;
                    TTypeAnnotationContext types;
                    const TString program = TStringBuilder() << R"((
                        (let input (AsList (AsStruct '('key (String 'a)))))
                        (return (Aggregate )" << input << " " << (keyed ? "'('key)" : "'()")
                        << " '() '('('streaming '(" << (useStateTable ? "'/Root/state" : "") << "))"
                        << (keyed ? " '('output_columns '())" : "") << "))) )";
                    auto node = ParseAndAnnotate(program, ctx, /*instant=*/false, /*wholeProgram=*/false, types);
                    UNIT_ASSERT_C(node, ctx.IssueManager.GetIssues().ToString());
                    UNIT_ASSERT_VALUES_EQUAL(UpdateCompletness(node, node, ctx), IGraphTransformer::TStatus::Ok);
                    const auto original = node;
                    const auto config = MakeIntrusive<TKikimrConfiguration>();
                    const auto session = MakeIntrusive<TKikimrSessionContext>(registry.Get(), config,
                        CreateDefaultTimeProvider(), CreateDeterministicRandomProvider(1), nullptr);
                    auto optCtx = MakeIntrusive<NKikimr::NKqp::NOpt::TKqpOptimizeContext>(session->GetCluster(),
                        config, session->QueryPtr(), session->TablesPtr(), nullptr);
                    auto optimizer = NKikimr::NKqp::NOpt::CreateKqpLogOptTransformer(optCtx, types, config);
                    UNIT_ASSERT_VALUES_EQUAL_C(SyncTransform(*optimizer, node, ctx), IGraphTransformer::TStatus::Ok,
                        ctx.IssueManager.GetIssues().ToString());
                    if (keyed) {
                        UNIT_ASSERT(NNodes::TCoExtractMembers::Match(node.Get()));
                        UNIT_ASSERT_VALUES_EQUAL(node->Child(1)->ChildrenSize(), 0);
                    }
                    const auto result = keyed ? node->HeadPtr() : node;
                    const auto inputKind = original->Head().GetTypeAnn()->GetKind();
                    if (inputKind == ETypeAnnotationKind::List) {
                        UNIT_ASSERT(NNodes::TCoForwardList::Match(result.Get()));
                    } else if (inputKind == ETypeAnnotationKind::Stream) {
                        UNIT_ASSERT(NNodes::TCoFromFlow::Match(result.Get()));
                    }
                    const auto aggregation = inputKind == ETypeAnnotationKind::Flow ? result : result->HeadPtr();
                    UNIT_ASSERT(NNodes::TKqpStreamingAggregation::Match(aggregation.Get()));
                    if (inputKind != ETypeAnnotationKind::Flow) {
                        UNIT_ASSERT(NNodes::TCoToFlow::Match(aggregation->HeadPtr().Get()));
                    }
                    const auto rewrittenInput = inputKind == ETypeAnnotationKind::Flow
                        ? aggregation->HeadPtr() : aggregation->Head().HeadPtr();
                    UNIT_ASSERT_C(rewrittenInput == original->HeadPtr(),
                        "Pure input must be preserved without wrapping it in a stage or connection");
                    UNIT_ASSERT(aggregation->ChildPtr(1) == original->ChildPtr(1));
                    UNIT_ASSERT(aggregation->ChildPtr(2) == original->ChildPtr(2));
                    UNIT_ASSERT_VALUES_EQUAL(aggregation->Child(3)->ChildrenSize(), 1);
                }
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

    Y_UNIT_TEST(KqpPureExprSourceFlags) {
        TExprContext ctx;
        const auto check = [&](TStringBuf callable, bool hasDqSource, bool hasIndexRead) {
            const auto body = ctx.NewCallable(TPositionHandle(), callable, {});
            for (const auto& expr : {body, ctx.NewList(TPositionHandle(), {body})}) {
                for (const bool checkDqSources : {false, true}) {
                    for (const bool checkIndexReads : {false, true}) {
                        const bool expectedPure = !(checkDqSources && hasDqSource) && !(checkIndexReads && hasIndexRead);
                        UNIT_ASSERT_VALUES_EQUAL_C(NKikimr::NKqp::NOpt::IsKqpPureExpr(NNodes::TExprBase(expr),
                            checkDqSources, checkIndexReads), expectedPure, TStringBuilder() << callable
                            << ", checkDqSources=" << checkDqSources << ", checkIndexReads=" << checkIndexReads);
                    }
                }
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

    Y_UNIT_TEST(StreamingSettingsAcceptAtomLists) {
        for (const TStringBuf options : {"", "'first", "'first 'second", "'first 'second 'third"}) {
            TExprContext ctx;
            TTypeAnnotationContext typesCtx;
            const TString program = TStringBuilder() << R"((
                (let input (AsList (AsStruct '('key (String 'a)))))
                (return (Aggregate input '('key) '() '('('streaming '()" << options << "))))) )";
            auto node = ParseAndAnnotate(program, ctx, /*instant=*/false, /*wholeProgram=*/false, typesCtx);
            UNIT_ASSERT_C(node, ctx.IssueManager.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(node->Content(), "Aggregate");
            UNIT_ASSERT_VALUES_EQUAL(node->GetTypeAnn()->GetKind(), ETypeAnnotationKind::List);
        }
    }

    Y_UNIT_TEST(StreamingSettingsRejectNonAtoms) {
        for (const TStringBuf options : {"(Int64 '1)", "'first (Int64 '1)", "'first '()"}) {
            TExprContext ctx;
            TTypeAnnotationContext typesCtx;
            const TString program = TStringBuilder() << R"((
                (let input (AsList (AsStruct '('key (String 'a)))))
                (return (Aggregate input '('key) '() '('('streaming '()" << options << "))))) )";
            UNIT_ASSERT(!ParseAndAnnotate(program, ctx, /*instant=*/false, /*wholeProgram=*/false, typesCtx));
            UNIT_ASSERT_STRING_CONTAINS(ctx.IssueManager.GetIssues().ToString(), "Expected atom");
        }
    }

    Y_UNIT_TEST(StreamingAggregationResultTypes) {
        for (const auto kind : {ETypeAnnotationKind::List, ETypeAnnotationKind::Stream, ETypeAnnotationKind::Flow}) {
            for (const bool keyed : {false, true}) {
                for (const bool optional : {false, true}) {
                    for (const bool hasDefault : {false, true}) {
                        TStreamingAggregationTypeAnnTest test;
                        auto traits = test.Traits(optional ? "(Just (Int64 '1))" : "(Int64 '1)",
                            hasDefault ? "(Int64 '0)" : "(Null)");
                        auto node = test.Aggregation(kind, keyed ? TExprNodeList{test.Atom("key")} : TExprNodeList{},
                            {test.List({test.Atom("value"), traits})},
                            {test.List({test.Atom("state_table_path"), test.Atom("/Root/state")}),
                             test.List({test.Atom("compact")})});
                        const auto* result = test.CheckType(node);
                        UNIT_ASSERT_VALUES_EQUAL(node->GetTypeAnn()->GetKind(), kind);
                        UNIT_ASSERT_VALUES_EQUAL(result->GetSize(), keyed ? 2 : 1);
                        const TTypeAnnotationNode* expected = test.Ctx.MakeType<TDataExprType>(EDataSlot::Int64);
                        if (keyed ? optional : !hasDefault) {
                            expected = test.Ctx.MakeType<TOptionalExprType>(expected);
                        }
                        UNIT_ASSERT(IsSameAnnotation(*result->FindItemType("value"), *expected));
                        if (keyed) {
                            UNIT_ASSERT(IsSameAnnotation(*result->FindItemType("key"),
                                *test.Ctx.MakeType<TDataExprType>(EDataSlot::String)));
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(StreamingAggregationTupleResultTypes) {
        for (const bool keyed : {false, true}) {
            for (const bool optional : {false, true}) {
                TStreamingAggregationTypeAnnTest test;
                auto traits = test.Traits(optional
                    ? "(Just '((Int64 '1) (Just (Int64 '2))))"
                    : "'((Int64 '1) (Just (Int64 '2)))");
                auto node = test.Aggregation(ETypeAnnotationKind::Flow,
                    keyed ? TExprNodeList{test.Atom("key")} : TExprNodeList{},
                    {test.List({test.List({test.Atom("first"), test.Atom("second")}), traits})});
                const auto* result = test.CheckType(node);
                const auto* intType = test.Ctx.MakeType<TDataExprType>(EDataSlot::Int64);
                const auto* optionalType = test.Ctx.MakeType<TOptionalExprType>(intType);
                UNIT_ASSERT(IsSameAnnotation(*result->FindItemType("first"),
                    *(optional || !keyed ? static_cast<const TTypeAnnotationNode*>(optionalType) : intType)));
                UNIT_ASSERT(IsSameAnnotation(*result->FindItemType("second"), *optionalType));
            }
        }
    }

    Y_UNIT_TEST(StreamingAggregationNormalizesStreamingSettings) {
        for (const bool hasStateTable : {false, true}) {
            TStreamingAggregationTypeAnnTest test;
            auto options = test.List(hasStateTable ? TExprNodeList{test.Atom("/Root/state")} : TExprNodeList{});
            auto node = test.Aggregation(ETypeAnnotationKind::List, {test.Atom("key")}, {},
                {test.List({test.Atom("streaming"), std::move(options)})});
            test.CheckType(node);
            const auto* settings = node->Child(NNodes::TKqpStreamingAggregation::idx_Settings);
            UNIT_ASSERT_VALUES_EQUAL(settings->ChildrenSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(settings->Head().ChildrenSize(), 2);
            UNIT_ASSERT_VALUES_EQUAL(settings->Head().Head().Content(), "state_table_path");
            UNIT_ASSERT(settings->Head().Tail().IsAtom());
            UNIT_ASSERT_VALUES_EQUAL(settings->Head().Tail().Content(), hasStateTable ? "/Root/state" : "");
        }
    }

    Y_UNIT_TEST(StreamingAggregationRejectsMultipleStateTablePaths) {
        for (const ui32 pathsCount : {2, 3}) {
            TStreamingAggregationTypeAnnTest test;
            TExprNodeList paths;
            for (ui32 i = 0; i < pathsCount; ++i) {
                paths.push_back(test.Atom(TStringBuilder() << "/Root/state" << i));
            }
            auto node = test.Aggregation(ETypeAnnotationKind::List, {test.Atom("key")}, {},
                {test.List({test.Atom("streaming"), test.List(std::move(paths))})});
            UNIT_ASSERT_VALUES_EQUAL_C(test.Annotate(node), IGraphTransformer::TStatus::Error,
                test.Ctx.IssueManager.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(),
                "Streaming aggregation accepts at most one state table path");
        }
    }

    Y_UNIT_TEST(StreamingAggregationInvalidSettings) {
        for (const ui32 settingCase : {0, 1, 2, 3, 4, 5, 6}) {
            TStreamingAggregationTypeAnnTest test;
            TExprNodeList setting;
            switch (settingCase) {
                case 0: setting = {test.Atom("state_table_path")}; break;
                case 1: setting = {test.Atom("state_table_path"), test.List({})}; break;
                case 2: setting = {test.Atom("state_table_path"), test.Atom("/Root/state"), test.Atom("extra")}; break;
                case 3: setting = {test.Atom("unsupported")}; break;
                case 4: setting = {test.Atom("streaming")}; break;
                case 5: setting = {test.Atom("streaming"), test.Atom("/Root/state")}; break;
                case 6: setting = {test.Atom("streaming"), test.List({test.Atom("/Root/state"), test.List({})})}; break;
            }
            auto node = test.Aggregation(ETypeAnnotationKind::List, {test.Atom("key")}, {}, {test.List(std::move(setting))});
            UNIT_ASSERT_VALUES_EQUAL_C(test.Annotate(node), IGraphTransformer::TStatus::Error,
                test.Ctx.IssueManager.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(StreamingAggregationInvalidColumns) {
        for (const bool duplicate : {false, true}) {
            TStreamingAggregationTypeAnnTest test;
            auto node = test.Aggregation(ETypeAnnotationKind::List, {test.Atom(duplicate ? "key" : "missing")},
                {test.List({test.Atom("key"), test.Traits("(Int64 '1)")})});
            UNIT_ASSERT_VALUES_EQUAL_C(test.Annotate(node), IGraphTransformer::TStatus::Error,
                test.Ctx.IssueManager.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(StreamingAggregationRejectsDistinct) {
        TStreamingAggregationTypeAnnTest test;
        auto node = test.Aggregation(ETypeAnnotationKind::List, {test.Atom("key")},
            {test.List({test.Atom("value"), test.Traits("(Int64 '1)"), test.Atom("key")})});
        UNIT_ASSERT_VALUES_EQUAL(test.Annotate(node), IGraphTransformer::TStatus::Error);
        UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(),
            "DISTINCT aggregation is not supported for mode: StreamingAggregation");
    }

    Y_UNIT_TEST(StreamingAggregationConstraints) {
        using namespace NNodes;
        auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        for (const bool constraintsEnabled : {false, true}) {
            for (const bool useStateTable : {false, true}) {
                for (const bool empty : {false, true}) {
                    TExprContext ctx;
                    TTypeAnnotationContext types;
                    const auto config = MakeIntrusive<TKikimrConfiguration>();
                    config->_KqpYqlConstraintsTransformerEnabled = constraintsEnabled;
                    const auto session = MakeIntrusive<TKikimrSessionContext>(functionRegistry.Get(), config,
                        CreateDefaultTimeProvider(), CreateDeterministicRandomProvider(1), nullptr);
                    session->SetInternalTypeAnnTransformer(NKikimr::NKqp::NOpt::CreateKqpTypeAnnotationTransformer(
                        session->GetCluster(), session->TablesPtr(), config));
                    const auto* rowType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
                        ctx.MakeType<TItemExprType>("key", ctx.MakeType<TDataExprType>(EDataSlot::String))});
                    const auto input = ctx.NewArgument(TPositionHandle(), "input");
                    input->SetTypeAnn(ctx.MakeType<TListExprType>(rowType));
                    input->AddConstraint(ctx.MakeConstraint<TStreamingConstraintNode>());
                    if (empty) {
                        input->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());
                    }
                    auto aggregation = Build<TKqpStreamingAggregation>(ctx, input->Pos())
                        .Input(input)
                        .Keys().Add().Value("key").Build().Build()
                        .Handlers().Build()
                        .Settings().Add()
                            .Name().Build("state_table_path")
                            .Value<TCoAtom>().Value(useStateTable ? "/Root/state" : "").Build()
                            .Build().Build()
                        .Done().Ptr();
                    UNIT_ASSERT(!KikimrDataSinkFunctions().contains(TKqpStreamingAggregation::CallableName()));
                    UNIT_ASSERT(session->GetInternalTypeAnnTransformer()->CanParse(*aggregation));
                    auto typeAnn = CreateKiSinkTypeAnnotationTransformer(nullptr, session, types);
                    UNIT_ASSERT_VALUES_EQUAL_C(SyncTransform(*typeAnn, aggregation, ctx), IGraphTransformer::TStatus::Ok,
                        ctx.IssueManager.GetIssues().ToString());
                    UNIT_ASSERT(aggregation->GetTypeAnn() == input->GetTypeAnn());

                    auto constraints = CreateKiSinkConstraintsTransformer(session);
                    UNIT_ASSERT_VALUES_EQUAL_C(SyncTransform(*constraints, aggregation, ctx), IGraphTransformer::TStatus::Ok,
                        ctx.IssueManager.GetIssues().ToString());
                    // Normally done by the outer constraint transformer after the provider returns Ok.
                    aggregation->SetState(TExprNode::EState::ConstrComplete);
                    UNIT_ASSERT_VALUES_EQUAL(aggregation->GetConstraint<TStreamingConstraintNode>() != nullptr, constraintsEnabled);
                    UNIT_ASSERT_VALUES_EQUAL(aggregation->GetConstraint<TEmptyConstraintNode>() != nullptr, constraintsEnabled && empty);
                    UNIT_ASSERT(!aggregation->GetConstraint<TUniqueConstraintNode>());
                    UNIT_ASSERT(!aggregation->GetConstraint<TDistinctConstraintNode>());
                }
            }
        }
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
