#include "yql_kikimr_provider_impl.h"
#include "yql_kikimr_settings.h"

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/opt/logical/kqp_opt_log.h>
#include <ydb/core/kqp/opt/peephole/kqp_opt_peephole_rules.h>
#include <ydb/core/kqp/query_compiler/kqp_mkql_compiler.h>
#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/library/yql/dq/constraints/dq_constraints.h>

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
#include <yql/essentials/providers/common/mkql/yql_type_mkql.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

namespace NYql {

namespace {

struct TStreamingAggregationTypeAnnTest {
    TExprContext Ctx;
    TTypeAnnotationContext Types;
    const TKikimrConfiguration::TPtr Config = MakeIntrusive<TKikimrConfiguration>();
    TIntrusivePtr<NKikimr::NKqp::TUserRequestContext> UserRequestContext = MakeIntrusive<NKikimr::NKqp::TUserRequestContext>();

    TStreamingAggregationTypeAnnTest() {
        UserRequestContext->IsStreamingQuery = true;
    }

    TExprNode::TPtr Atom(TStringBuf value) {
        return Ctx.NewAtom(TPositionHandle(), value);
    }

    TExprNode::TPtr List(TExprNodeList items) {
        return Ctx.NewList(TPositionHandle(), std::move(items));
    }

    TExprNode::TPtr Traits(TStringBuf finish, TStringBuf defaultValue = "(Null)",
        TStringBuf itemType = "(StructType '('key (DataType 'String)))",
        TStringBuf save = "state", TStringBuf load = "state", TStringBuf init = "(Int64 '0)", TStringBuf merge = "left")
    {
        const TString program = TStringBuilder() << R"((
            (return (AggregationTraits )" << itemType << R"(
                (lambda '(item) )" << init << R"()
                (lambda '(item state) state)
                (lambda '(state) )" << save << R"()
                (lambda '(state) )" << load << R"()
                (lambda '(left right) )" << merge << R"()
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

    TExprNode::TPtr TupleTraits(bool optional, bool serialized) {
        const auto wrap = [optional](TStringBuf value) -> TString {
            return optional ? TStringBuilder() << "(Just " << value << ")" : TString(value);
        };
        const TString init = serialized ? "(Int64 '0)" : wrap("'((Just (Int64 '0)) (Just (Int64 '0)))");
        const TString save = serialized ? wrap("'((Just state) (Just state))") : "state";
        const TStringBuf load = serialized ? "(Unwrap (Nth state '0))" : "state";
        const TString program = TStringBuilder() << R"((
            (return (AggregationTraits
                (StructType '('key (DataType 'String)))
                (lambda '(item) )" << init << R"()
                (lambda '(item state) state)
                (lambda '(state) )" << save << R"()
                (lambda '(state) )" << load << R"()
                (lambda '(left right) left)
                (lambda '(state) )" << save << R"()
                (Null)))
        ))";
        auto traits = ParseAndAnnotate(program, Ctx, false, false, Types);
        UNIT_ASSERT_C(traits, Ctx.IssueManager.GetIssues().ToString());
        return traits;
    }

    TExprNode::TPtr TupleOutputStateSetting() {
        return List({Atom("output_state_table"), List({Atom("/Root/result"), List({
            List({Atom("key"), Atom("key")}),
            List({Atom("first"), Atom("first")}),
            List({Atom("second"), Atom("second")})})})});
    }

    TExprNode::TPtr TupleAggregation(bool optional, bool serialized) {
        return Aggregation(ETypeAnnotationKind::Flow, {Atom("key")}, {
            List({List({Atom("first"), Atom("second")}), TupleTraits(optional, serialized)})});
    }

    void MarkStreaming(const TExprNode::TPtr& node) {
        node->SetState(TExprNode::EState::TypeComplete);
        node->AddConstraint(Ctx.MakeConstraint<TStreamingConstraintNode>());
    }

    static void CopyConstraints(const TExprNode::TPtr& node, const TExprNode& source) {
        node->SetState(TExprNode::EState::TypeComplete);
        node->CopyConstraints(source);
    }

    TExprNode::TPtr TableSinkStage(TStringBuf path, const TExprNode::TPtr& rows, TKikimrTablesData& tables) {
        using namespace NNodes;
        auto& table = tables.GetOrAddTable("db", "/Root", TString(path));
        table.Metadata = MakeIntrusive<TKikimrTableMetadata>();
        table.Metadata->DoesExist = true;
        table.Metadata->KeyColumnNames = {"key"};
        const auto pos = rows->Pos();
        const auto sink = Ctx.NewCallable(pos, TDqSink::CallableName(), {
            Atom("0"), Ctx.NewCallable(pos, "DataSink", {Atom("kikimr"), Atom("db")}),
            Ctx.NewCallable(pos, TKqpTableSinkSettings::CallableName(), {
                Ctx.NewCallable(pos, TKqpTable::CallableName(), {
                    Atom(path), Atom("1"), Atom(""), Atom("1")}),
                Atom("false"), Atom("upsert"), Atom("0"), Atom("true"),
                Atom("false"), Atom("false"), List({}), List({}), List({})})});
        auto stage = Ctx.NewCallable(pos, TDqPhyStage::CallableName(), {List({}),
            Ctx.NewLambda(pos, Ctx.NewArguments(pos, {}), TExprNode::TPtr(rows)),
            List({}), List({sink})});
        if (rows->GetConstraint<TStreamingConstraintNode>()) {
            MarkStreaming(stage);
            MarkStreaming(stage->ChildPtr(TDqPhyStage::idx_Program));
        }
        return stage;
    }

    IGraphTransformer::TStatus BuildStreamingFlow(const TExprNode::TPtr& tx, TExprNode::TPtr& output,
        THashSet<std::pair<ui64, ui64>>& streamingResults, const TKikimrTablesData& tables)
    {
        VisitExpr(tx, [&](const TExprNode::TPtr& node) {
            node->SetState(TExprNode::EState::ConstrComplete);
            return true;
        });
        return NKikimr::NKqp::NOpt::KqpBuildStreamingFlow(0, NNodes::TKqpPhysicalTx(tx), output,
            streamingResults, *Config, tables, "db", UserRequestContext.Get(), Ctx);
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

    template <typename TCheck>
    void Run(const TExprNode& node, TCheck&& check) {
        using namespace NKikimr::NMiniKQL;
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TKqpComputeContextBase computeCtx;
        const auto registry = CreateFunctionRegistry(CreateBuiltinRegistry());
        const auto randomProvider = CreateDeterministicRandomProvider(1);
        const auto timeProvider = CreateDeterministicTimeProvider(10000000);
        const NKikimr::NKqp::TKqlCompileContext compileCtx("", MakeIntrusive<TKikimrTablesData>(), env, *registry);
        const auto compiler = NKikimr::NKqp::CreateKqlCompiler(compileCtx, Types);
        NCommon::TMkqlBuildContext buildCtx(*compiler, compileCtx.PgmBuilder(), Ctx);
        const auto compiled = NCommon::MkqlBuildExpr(node, buildCtx);
        const auto* expectedType = NCommon::BuildType(node, *node.GetTypeAnn(), compileCtx.PgmBuilder());
        UNIT_ASSERT(compiled.GetStaticType()->IsSameType(*expectedType));
        const auto programNode = compileCtx.PgmBuilder().Collect(compiled);
        TExploringNodeVisitor explorer;
        explorer.Walk(programNode.GetNode(), env.GetNodeStack());
        const TComputationPatternOpts options(alloc.Ref(), env, GetKqpBaseComputeFactory(&computeCtx),
            registry.Get(), NUdf::EValidateMode::Greedy, NUdf::EValidatePolicy::Exception, "OFF", EGraphPerProcess::Multi);
        const auto pattern = MakeComputationPattern(explorer, programNode, {}, options);
        const auto graph = pattern->Clone(options.ToComputationOptions(*randomProvider, *timeProvider));
        const TBindTerminator bindTerminator(graph->GetTerminator());
        check(graph->GetValue());
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(KikimrProviderStreaming) {
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

    Y_UNIT_TEST_QUAD(StreamingAggregationRewritePreservesInputAndSettings, Keyed, UseStateTable) {
        auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        for (const TStringBuf input : {"input", "(Iterator input)", "(ToFlow input)",
                "(PartitionsByKeys input (lambda '(row) (Member row 'key)) (Void) (Void) (lambda '(rows) rows))"}) {
            TExprContext ctx;
            TTypeAnnotationContext types;
            const TString program = TStringBuilder() << R"((
                (let input (AsList (AsStruct '('key (String 'a)))))
                (return (Aggregate )" << input << " " << (Keyed ? "'('key)" : "'()")
                << " '() '('('compact) " << (Keyed ? "'('output_columns '())" : "") << "))))";
            auto node = ParseAndAnnotate(program, ctx, false, false, types);
            UNIT_ASSERT_C(node, ctx.IssueManager.GetIssues().ToString());
            auto constraints = CreateConstraintTransformer(types);
            UNIT_ASSERT_VALUES_EQUAL_C(SyncTransform(*constraints, node, ctx), IGraphTransformer::TStatus::Ok,
                ctx.IssueManager.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(UpdateCompletness(node, node, ctx), IGraphTransformer::TStatus::Ok);
            node->AddConstraint(ctx.MakeConstraint<TStreamingConstraintNode>());
            const auto original = node;
            const auto config = MakeIntrusive<TKikimrConfiguration>();
            config->FeatureFlags.SetEnableStreamingAggregation(true);
            config->FeatureFlags.SetEnableStreamingAggregationAdvanced(UseStateTable);
            config->DisableCheckpoints = true;
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
            UNIT_ASSERT(NNodes::TKqpStreamingAggregation::Match(node.Get()));
            UNIT_ASSERT(node->HeadPtr() == original->HeadPtr());
            UNIT_ASSERT(node->ChildPtr(1) == original->ChildPtr(1));
            UNIT_ASSERT(node->ChildPtr(2) == original->ChildPtr(2));
            const auto& settings = node->Child(NNodes::TKqpStreamingAggregation::idx_Settings);
            UNIT_ASSERT_VALUES_EQUAL(settings->ChildrenSize(), ui32(Keyed) + ui32(UseStateTable));
            UNIT_ASSERT(GetSetting(*settings, "output_columns") == GetSetting(original->Tail(), "output_columns"));
            if (UseStateTable) {
                const auto path = GetSetting(*settings, "state_table_path");
                UNIT_ASSERT(path);
                UNIT_ASSERT_VALUES_EQUAL(path->Tail().Content(), "/Root/state");
            }
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

    Y_UNIT_TEST_TWIN(StreamingAggregationProjectsHandlerInputs, InitReturnsItem) {
        for (const auto kind : {ETypeAnnotationKind::List, ETypeAnnotationKind::Stream, ETypeAnnotationKind::Flow}) {
            TStreamingAggregationTypeAnnTest test;
            const TString program = TStringBuilder() << R"((
                (return (AggregationTraits
                    (StructType '('value (DataType 'Int64)))
                    (lambda '(item) )" << (InitReturnsItem ? "item" : "(AsStruct '('value (Member item 'value)))") << R"()
                    (lambda '(item state) )" << (InitReturnsItem
                        ? "(AsStruct '('value (Add (Member state 'value) (Member item 'value))))" : "item") << R"()
                    (lambda '(state) state)
                    (lambda '(state) state)
                    (lambda '(left right) left)
                    (lambda '(state) state)
                    (Null)))
            ))";
            const auto traits = ParseAndAnnotate(program, test.Ctx, false, false, test.Types);
            UNIT_ASSERT_C(traits, test.Ctx.IssueManager.GetIssues().ToString());
            const auto keyTraits = ParseAndAnnotate(R"((
                (return (AggregationTraits
                    (StructType '('key (DataType 'String)))
                    (lambda '(item) item)
                    (lambda '(item state) item)
                    (lambda '(state) state)
                    (lambda '(state) state)
                    (lambda '(left right) left)
                    (lambda '(state) (Member state 'key))
                    (Null)))
            ))", test.Ctx, false, false, test.Types);
            UNIT_ASSERT_C(keyTraits, test.Ctx.IssueManager.GetIssues().ToString());
            auto input = ParseAndAnnotate(R"((
                (return (AsList
                    (AsStruct '('key (String 'a)) '('value (Int64 '1)))
                    (AsStruct '('key (String 'a)) '('value (Int64 '2)))
                    (AsStruct '('key (String 'b)) '('value (Int64 '10)))
                    (AsStruct '('key (String 'a)) '('value (Int64 '4)))
                    (AsStruct '('key (String 'b)) '('value (Int64 '20)))))
            ))", test.Ctx, false, false, test.Types);
            UNIT_ASSERT_C(input, test.Ctx.IssueManager.GetIssues().ToString());
            if (kind != ETypeAnnotationKind::List) {
                const auto inputPos = input->Pos();
                input = test.Ctx.NewCallable(inputPos, kind == ETypeAnnotationKind::Stream ? "Iterator" : "ToFlow",
                    {std::move(input)});
            }
            auto node = test.Aggregation(kind, {test.Atom("key")}, {
                test.List({test.Atom("value_state"), traits}), test.List({test.Atom("key_state"), keyTraits})});
            const auto originalInput = input;
            node = test.Ctx.ChangeChild(*node, NNodes::TKqpStreamingAggregation::idx_Input, std::move(input));
            const auto* resultType = test.CheckType(node);
            UNIT_ASSERT_VALUES_EQUAL(node->GetTypeAnn()->GetKind(), kind);
            const auto aggregation = kind == ETypeAnnotationKind::Flow ? node : node->HeadPtr();
            UNIT_ASSERT(NNodes::TKqpStreamingAggregation::Match(aggregation.Get()));
            UNIT_ASSERT_VALUES_EQUAL(aggregation->GetTypeAnn()->GetKind(), ETypeAnnotationKind::Flow);
            if (kind != ETypeAnnotationKind::Flow) {
                UNIT_ASSERT(node->IsCallable(kind == ETypeAnnotationKind::List ? "ForwardList" : "FromFlow"));
                UNIT_ASSERT(aggregation->Head().IsCallable("ToFlow"));
                UNIT_ASSERT(aggregation->Head().HeadPtr() == originalInput);
            }
            UNIT_ASSERT_VALUES_EQUAL(resultType->FindItemType("value_state")->Cast<TStructExprType>()->GetSize(), 1);
            test.Run(*node, [&](const auto& value) {
                const auto rows = value.GetListIterator();
                const TVector<TStringBuf> expectedKeys = {"a", "a", "b", "a", "b"};
                const TVector<i64> expectedValues = InitReturnsItem
                    ? TVector<i64>{1, 3, 10, 7, 30} : TVector<i64>{1, 2, 10, 4, 20};
                NUdf::TUnboxedValue row;
                for (ui32 i = 0; i < expectedKeys.size(); ++i) {
                    UNIT_ASSERT(rows.Next(row));
                    const auto key = row.GetElement(*resultType->FindItem("key"));
                    const auto keyState = row.GetElement(*resultType->FindItem("key_state"));
                    const auto valueState = row.GetElement(*resultType->FindItem("value_state"));
                    UNIT_ASSERT_VALUES_EQUAL(TString(key.AsStringRef()), expectedKeys[i]);
                    UNIT_ASSERT_VALUES_EQUAL(TString(keyState.AsStringRef()), expectedKeys[i]);
                    UNIT_ASSERT_VALUES_EQUAL(valueState.GetElement(0).Get<i64>(), expectedValues[i]);
                }
                UNIT_ASSERT(!rows.Next(row));
            });
        }
    }

    Y_UNIT_TEST(StreamingAggregationInvalidSettings) {
        const TStringBuf expectedErrors[] = {
            "Expected tuple size: 2, but got: 1",
            "Expected atom, but got:",
            "Expected tuple size: 2, but got: 3",
            "Unexpected setting: unsupported",
            "Unexpected setting: compact",
            "Expected tuple size: 2, but got: 1",
            "Expected atom, but got:",
            "Unknown output column missing",
        };
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
            UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), expectedErrors[settingCase]);
        }
    }

    Y_UNIT_TEST_TWIN(StreamingAggregationStateTablePathCharacters, AbsolutePath) {
        constexpr TStringBuf allowed = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789/-_.";
        for (ui32 byte = 0; byte < 256; ++byte) {
            TStreamingAggregationTypeAnnTest test;
            test.Config->FeatureFlags.SetEnableStreamingAggregationAdvanced(true);
            TString path = AbsolutePath ? "/Root/state" : "state";
            path.push_back(static_cast<char>(byte));
            path += "table";
            auto node = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")}, {},
                {test.List({test.Atom("state_table_path"), test.Atom(path)})});
            const bool valid = allowed.Contains(static_cast<char>(byte));
            UNIT_ASSERT_VALUES_EQUAL_C(test.Annotate(node),
                valid ? IGraphTransformer::TStatus::Ok : IGraphTransformer::TStatus::Error,
                "Path byte " << byte << ": " << test.Ctx.IssueManager.GetIssues().ToString());
            if (!valid) {
                UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(),
                    "Invalid streaming aggregation state table path");
            }
        }
    }

    Y_UNIT_TEST(StreamingAggregationEmptyStateTableSetting) {
        TStreamingAggregationTypeAnnTest test;
        test.Config->FeatureFlags.SetEnableStreamingAggregationAdvanced(false);
        // SQL normalizes an empty pragma away; exercise an actual empty setting here.
        auto node = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")}, {},
            {test.List({test.Atom("state_table_path"), test.Atom("")})});
        test.CheckType(node);
    }

    Y_UNIT_TEST(StreamingAggregationOutputStateSettings) {
        const TStringBuf expectedErrors[] = {
            {},
            "Expected tuple size: 2, but got: 1",
            "Expected tuple size: 2, but got: 3",
            "Invalid streaming aggregation output state table path",
            "Expected tuple, but got:",
            "Invalid streaming aggregation output state column mapping",
            "Invalid streaming aggregation output state column mapping",
            "Invalid streaming aggregation output state column mapping",
            "Invalid streaming aggregation output state table path",
            "Invalid streaming aggregation output state column mapping",
            "Expected tuple size: 2, but got: 1",
            "Expected atom, but got:",
            "Expected atom, but got:",
        };
        for (ui32 variant = 0; variant < 13; ++variant) {
            TStreamingAggregationTypeAnnTest test;
            auto mapping = test.List({
                test.List({test.Atom("key"), test.Atom("key")}),
                test.List({test.Atom("value"), test.Atom("renamed_value")})});
            auto value = test.List({test.Atom("/Root/result"), mapping});
            if (variant == 1) {
                value = test.List({test.Atom("/Root/result")});
            } else if (variant == 2) {
                value = test.List({test.Atom("/Root/result"), mapping, test.List({})});
            } else if (variant == 3) {
                value = test.List({test.Atom(""), mapping});
            } else if (variant == 4) {
                value = test.List({test.Atom("/Root/result"), test.Atom("mapping")});
            } else if (variant == 5) {
                value = test.List({test.Atom("/Root/result"), test.List({mapping->HeadPtr(),
                    test.List({test.Atom("unknown"), test.Atom("renamed_value")})})});
            } else if (variant == 6) {
                value = test.List({test.Atom("/Root/result"), test.List({mapping->HeadPtr(),
                    test.List({test.Atom("value"), test.Atom("key")})})});
            } else if (variant == 7) {
                value = test.List({test.Atom("/Root/result"), test.List({mapping->HeadPtr(),
                    test.List({test.Atom("value"), test.Atom("")})})});
            } else if (variant == 8) {
                value = test.List({test.Atom("/Root/result!"), mapping});
            } else if (variant == 9) {
                value = test.List({test.Atom("/Root/result"), test.List({mapping->HeadPtr(),
                    test.List({test.Atom("key"), test.Atom("another_key")}), mapping->TailPtr()})});
            } else if (variant == 10) {
                value = test.List({test.Atom("/Root/result"), test.List({test.List({test.Atom("key")}), mapping->TailPtr()})});
            } else if (variant == 11) {
                value = test.List({test.Atom("/Root/result"), test.List({
                    test.List({test.List({}), test.Atom("key")}), mapping->TailPtr()})});
            } else if (variant == 12) {
                value = test.List({test.Atom("/Root/result"), test.List({
                    test.List({test.Atom("key"), test.List({})}), mapping->TailPtr()})});
            }
            auto node = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
                {test.List({test.Atom("value"), test.Traits("state")})}, {
                    test.List({test.Atom("output_state_table"), value})});
            UNIT_ASSERT_VALUES_EQUAL_C(test.Annotate(node), variant == 0
                ? IGraphTransformer::TStatus::Ok : IGraphTransformer::TStatus::Error,
                TStringBuilder() << "Variant " << variant << ": " << test.Ctx.IssueManager.GetIssues().ToString());
            if (variant != 0) {
                UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), expectedErrors[variant]);
            }
        }
    }

    Y_UNIT_TEST(StreamingAggregationOutputStateKeyTypes) {
        struct TCase {
            TString Type;
            bool Supported;
        };
        TVector<TCase> cases;
        for (const TStringBuf name : {"Bool", "Int8", "Uint8", "Int16", "Uint16", "Int32", "Uint32", "Int64", "Uint64",
                "Float", "Double", "String", "Utf8", "Yson", "Json", "JsonDocument", "Uuid", "DyNumber",
                "Date", "Datetime", "Timestamp", "Interval", "Date32", "Datetime64", "Timestamp64", "Interval64",
                "TzDate", "TzDatetime", "TzTimestamp"}) {
            cases.push_back({TStringBuilder() << "(DataType '" << name << ")", true});
        }
        for (const TStringBuf type : {
                "(DataType 'Decimal '22 '9)",
                "(OptionalType (DataType 'Int64))",
                "(ListType (DataType 'Utf8))",
                "(TupleType)",
                "(TupleType (DataType 'String) (DataType 'Uint64))",
                "(DictType (DataType 'String) (DataType 'Uint64))",
                "(OptionalType (ListType (TupleType (DataType 'String) (DictType (DataType 'Int64) (OptionalType (DataType 'Decimal '22 '9))))))",
                "(VoidType)", "(NullType)", "(EmptyListType)", "(EmptyDictType)"}) {
            cases.push_back({TString(type), true});
        }
        for (const TStringBuf type : {
                "(PgType 'int4)",
                "(MultiType (DataType 'Int64))",
                "(StructType '('member (DataType 'String)))",
                "(TaggedType (DataType 'String) 'tag)",
                "(VariantType (TupleType (DataType 'String) (DataType 'Int64)))",
                "(VariantType (StructType '('member (DataType 'String))))",
                "(DataType 'TzDate32)", "(DataType 'TzDatetime64)", "(DataType 'TzTimestamp64)"}) {
            // Every container must validate its contents, including both sides of a dict.
            for (const TStringBuf wrapper : {"", "OptionalType", "ListType", "TupleType",
                    "DictType (DataType 'String)", "DictType"}) {
                const TString wrapped = wrapper.empty() ? TString(type) : TStringBuilder()
                    << '(' << wrapper << ' ' << type << (wrapper == "DictType" ? " (DataType 'String)" : "") << ')';
                cases.push_back({wrapped, false});
            }
        }
        cases.push_back({"(OptionalType (ListType (TupleType (DataType 'String) (DictType (DataType 'Int64) (OptionalType (PgType 'int4))))))", false});

        for (const auto& testCase : cases) {
            for (const TStringBuf stateSetting : {"", "state_table_path", "output_state_table"}) {
                TStreamingAggregationTypeAnnTest test;
                test.Config->FeatureFlags.SetEnableStreamingAggregationAdvanced(true);
                const auto typeNode = ParseAndAnnotate(TStringBuilder() << "((return " << testCase.Type << "))",
                    test.Ctx, false, false, test.Types);
                UNIT_ASSERT_C(typeNode, testCase.Type << ": " << test.Ctx.IssueManager.GetIssues().ToString());
                const auto* const keyType = typeNode->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                TExprNodeList settings;
                if (stateSetting == "output_state_table") {
                    settings.push_back(test.List({test.Atom(stateSetting), test.List({test.Atom("/Root/result"), test.List({
                        test.List({test.Atom("key"), test.Atom("key")}),
                        test.List({test.Atom("typed_key"), test.Atom("typed_key")})})})}));
                } else if (!stateSetting.empty()) {
                    settings.push_back(test.List({test.Atom(stateSetting), test.Atom("/Root/state")}));
                }
                auto node = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key"), test.Atom("typed_key")}, {}, std::move(settings));
                const auto* const rowType = test.Ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
                    test.Ctx.MakeType<TItemExprType>("key", test.Ctx.MakeType<TDataExprType>(EDataSlot::String)),
                    test.Ctx.MakeType<TItemExprType>("typed_key", keyType)});
                node->HeadPtr()->SetTypeAnn(test.Ctx.MakeType<TFlowExprType>(rowType));

                TStringBuf expectedError;
                if (!keyType->IsHashable() || !keyType->IsEquatable()) {
                    // The allowlist does not relax existing grouping key requirements (e.g. Json/Yson).
                    expectedError = "Expected hashable and equatable type for key column: typed_key";
                } else if (!testCase.Supported && stateSetting == "output_state_table") {
                    expectedError = "Unsupported key type for streaming aggregation output state table, column: typed_key";
                }
                UNIT_ASSERT_VALUES_EQUAL_C(test.Annotate(node), expectedError.empty()
                    ? IGraphTransformer::TStatus::Ok : IGraphTransformer::TStatus::Error,
                    testCase.Type << ", setting: " << stateSetting << ": " << test.Ctx.IssueManager.GetIssues().ToString());
                if (!expectedError.empty()) {
                    UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), expectedError);
                }
            }
        }
    }

    Y_UNIT_TEST_QUAD(StreamingAggregationOutputStateValueTypes, DisableCheckpoints, TiedTable) {
        struct TCase {
            TString Type;
            bool Supported;
        };
        TVector<TCase> cases;
        for (const TStringBuf name : {"Bool", "Int8", "Uint8", "Int16", "Uint16", "Int32", "Uint32", "Int64", "Uint64",
                "Float", "Double", "String", "Utf8", "Yson", "Json", "JsonDocument", "Uuid", "DyNumber",
                "Date", "Datetime", "Timestamp", "Interval", "Date32", "Datetime64", "Timestamp64", "Interval64",
                "TzDate", "TzDatetime", "TzTimestamp", "TzDate32", "TzDatetime64", "TzTimestamp64"}) {
            cases.push_back({TStringBuilder() << "(DataType '" << name << ")", true});
        }
        for (const TStringBuf type : {
                "(DataType 'Decimal '22 '9)", "(PgType 'int4)", "(PgType '_int4)",
                "(OptionalType (DataType 'Json))", "(ListType (DataType 'JsonDocument))",
                "(TupleType)", "(TupleType (DataType 'String) (DataType 'Uint64))",
                "(StructType)", "(StructType '('member (DataType 'String)))",
                "(DictType (DataType 'String) (DataType 'Uint64))",
                "(TaggedType (DataType 'String) 'tag)",
                "(VariantType (TupleType (DataType 'String) (DataType 'Int64)))",
                "(VariantType (StructType '('member (DataType 'String))))",
                "(OptionalType (ListType (StructType '('member (TaggedType (DictType (DataType 'String) (PgType 'int4)) 'tag)))))",
                "(VoidType)", "(NullType)", "(EmptyListType)", "(EmptyDictType)"}) {
            cases.push_back({TString(type), true});
        }
        for (const TStringBuf type : {
                "(ResourceType 'TestState)", "(StreamType (DataType 'Int64))", "(FlowType (DataType 'Int64))",
                "(CallableType '() '((DataType 'Int64)))", "(BlockType (DataType 'Int64))", "(ScalarType (DataType 'Int64))",
                "(MultiType (DataType 'Int64))", "(LinearType (DataType 'Int64))", "(DynamicLinearType (DataType 'Int64))",
                "(OptionalType (ResourceType 'TestState))", "(ListType (ResourceType 'TestState))",
                "(TupleType (DataType 'String) (ResourceType 'TestState))",
                "(StructType '('first (DataType 'String)) '('last (ResourceType 'TestState)))",
                "(DictType (DataType 'String) (ResourceType 'TestState))",
                "(DictType (MultiType (DataType 'Int64)) (DataType 'String))",
                "(TaggedType (ResourceType 'TestState) 'tag)",
                "(VariantType (TupleType (DataType 'String) (ResourceType 'TestState)))",
                "(VariantType (StructType '('first (DataType 'String)) '('last (ResourceType 'TestState))))",
                "(OptionalType (ListType (StructType '('member (TaggedType (DictType (DataType 'String) (MultiType (DataType 'Int64))) 'tag)))))"}) {
            cases.push_back({TString(type), false});
        }

        for (const auto& testCase : cases) {
            TStreamingAggregationTypeAnnTest test;
            test.Config->DisableCheckpoints = DisableCheckpoints;
            test.Types.LangVer = MakeLangVersion(2025, 4);
            const auto traits = test.Traits("state", "(Null)", "(StructType '('key (DataType 'String)))",
                "state", "state", TStringBuilder() << "(InstanceOf " << testCase.Type << ')');
            // Projection must not hide a saved field that the runtime exports.
            TExprNodeList settings = {test.List({test.Atom("output_columns"), test.List({test.Atom("key")})})};
            if constexpr (TiedTable) {
                settings.push_back(test.List({test.Atom("output_state_table"), test.List({test.Atom("/Root/result"), test.List({
                    test.List({test.Atom("key"), test.Atom("key")}),
                    test.List({test.Atom("value"), test.Atom("value")})})})}));
            }
            auto node = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
                {test.List({test.Atom("value"), traits})}, std::move(settings));
            TStringBuf expectedError;
            if ((TiedTable || !DisableCheckpoints) && !traits->Child(NNodes::TCoAggregationTraits::idx_SaveHandler)->GetTypeAnn()->IsPersistable()) {
                expectedError = "Expected persistable data, but got:";
            } else if (TiedTable && !testCase.Supported) {
                expectedError = "Unsupported saved state type for streaming aggregation output state table, column: value";
            }
            UNIT_ASSERT_VALUES_EQUAL_C(test.Annotate(node), expectedError.empty()
                ? IGraphTransformer::TStatus::Ok : IGraphTransformer::TStatus::Error,
                testCase.Type << ": " << test.Ctx.IssueManager.GetIssues().ToString());
            if (!expectedError.empty()) {
                UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), expectedError);
            }
        }
    }

    Y_UNIT_TEST_QUAD(StreamingAggregationOutputStateSavedTypeNormalization, ResourceState, IdentityFinish) {
        TStreamingAggregationTypeAnnTest test;
        test.Config->DisableCheckpoints = true;
        const TStringBuf resource = "(InstanceOf (ResourceType 'TestState))";
        const TStringBuf integer = "(Int64 '0)";
        const TStringBuf init = ResourceState ? resource : integer;
        const TStringBuf save = ResourceState ? integer : resource;
        const auto traits = test.Traits(IdentityFinish ? "state" : save, "(Null)",
            "(StructType '('key (DataType 'String)))", save, init, init);
        auto node = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), traits})}, {
                test.List({test.Atom("output_state_table"), test.List({test.Atom("/Root/result"), test.List({
                    test.List({test.Atom("key"), test.Atom("key")}),
                    test.List({test.Atom("value"), test.Atom("value")})})})})});
        const bool supported = IdentityFinish ? !ResourceState : ResourceState;
        UNIT_ASSERT_VALUES_EQUAL_C(test.Annotate(node), supported
            ? IGraphTransformer::TStatus::Ok : IGraphTransformer::TStatus::Error,
            test.Ctx.IssueManager.GetIssues().ToString());
        if (!supported) {
            UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(),
                "Expected persistable data, but got:");
        }
    }

    Y_UNIT_TEST_QUAD(StreamingAggregationOutputStateRequiresMerge, TiedTable, HasMerge) {
        TStreamingAggregationTypeAnnTest test;
        const auto traits = test.Traits("state", "(Null)", "(StructType '('key (DataType 'String)))",
            "state", "state", "(Int64 '0)", HasMerge ? "left" : "(Void)");
        auto node = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), traits})}, TiedTable ? TExprNodeList{
                test.List({test.Atom("output_state_table"), test.List({test.Atom("/Root/result"), test.List({
                    test.List({test.Atom("key"), test.Atom("key")}),
                    test.List({test.Atom("value"), test.Atom("value")})})})})} : TExprNodeList{});
        UNIT_ASSERT_VALUES_EQUAL_C(test.Annotate(node), TiedTable && !HasMerge
            ? IGraphTransformer::TStatus::Error : IGraphTransformer::TStatus::Ok,
            test.Ctx.IssueManager.GetIssues().ToString());
        if constexpr (TiedTable && !HasMerge) {
            UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(),
                "Merge handler must be specified for streaming aggregation tied to an output state table");
        }
    }

    Y_UNIT_TEST_TWIN(StreamingAggregationPendingInputMayBeNonPersistable, TiedTable) {
        TStreamingAggregationTypeAnnTest test;
        const auto traits = test.Traits("state", "(Null)",
            "(StructType '('key (DataType 'String)) '('payload (ResourceType 'TestInput)))");
        auto node = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), traits})}, TiedTable ? TExprNodeList{
                test.List({test.Atom("output_state_table"), test.List({test.Atom("/Root/result"), test.List({
                    test.List({test.Atom("key"), test.Atom("key")}),
                    test.List({test.Atom("value"), test.Atom("value")})})})})} : TExprNodeList{});
        const auto* rowType = test.Ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
            test.Ctx.MakeType<TItemExprType>("key", test.Ctx.MakeType<TDataExprType>(EDataSlot::String)),
            test.Ctx.MakeType<TItemExprType>("payload", test.Ctx.MakeType<TResourceExprType>("TestInput"))});
        node->HeadPtr()->SetTypeAnn(test.Ctx.MakeType<TFlowExprType>(rowType));
        // Pending lookups retain saved aggregate contributions, not input resources.
        test.CheckType(node);
    }

    Y_UNIT_TEST(StreamingAggregationOutputStateSerializationDoesNotMutateSharedTraits) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        const auto traits = test.Traits("state", "(Null)",
            "(StructType '('key (DataType 'String)))", "(Just state)", "(Unwrap state)");
        auto node = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), traits})}, {
                test.List({test.Atom("output_state_table"), test.List({test.Atom("/Root/result"), test.List({
                    test.List({test.Atom("key"), test.Atom("key")}),
                    test.List({test.Atom("value"), test.Atom("renamed_value")})})})})});
        test.CheckType(node);
        const auto normalized = TKqpStreamingAggregation(node).Handlers().Item(0).Trait().Cast<TCoAggregationTraits>();
        UNIT_ASSERT(IsIdentityLambda(normalized.SaveHandler().Ref()));
        UNIT_ASSERT(IsIdentityLambda(normalized.LoadHandler().Ref()));
        UNIT_ASSERT_VALUES_EQUAL(normalized.SaveHandler().Ref().GetTypeAnn()->GetKind(), ETypeAnnotationKind::Data);
        UNIT_ASSERT(normalized.Raw() != traits.Get());
        // Other users of the original trait keep its serialization contract.
        UNIT_ASSERT(!IsIdentityLambda(*traits->Child(TCoAggregationTraits::idx_SaveHandler)));
        UNIT_ASSERT(!IsIdentityLambda(*traits->Child(TCoAggregationTraits::idx_LoadHandler)));
        UNIT_ASSERT_VALUES_EQUAL(traits->Child(TCoAggregationTraits::idx_SaveHandler)->GetTypeAnn()->GetKind(), ETypeAnnotationKind::Optional);
        const auto annotated = node;
        test.CheckType(node);
        UNIT_ASSERT(node == annotated);
    }

    Y_UNIT_TEST(StreamingAggregationOutputStateRejectsLossyFinalizer) {
        TStreamingAggregationTypeAnnTest test;
        auto node = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), test.Traits("(Div state (Int64 '2))")})}, {
                test.List({test.Atom("output_state_table"), test.List({test.Atom("/Root/result"), test.List({
                    test.List({test.Atom("key"), test.Atom("key")}),
                    test.List({test.Atom("value"), test.Atom("value")})})})})});
        UNIT_ASSERT_VALUES_EQUAL(test.Annotate(node), IGraphTransformer::TStatus::Error);
        UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(),
            "require an identity finalizer or a finalizer equal to serialization");
    }

    Y_UNIT_TEST_QUAD(StreamingAggregationOutputStateFinalizerValidation, HasSink, ExplicitState) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        test.Config->FeatureFlags.SetEnableStreamingAggregationAdvanced(ExplicitState);
        auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), test.Traits("(Div state (Int64 '2))")})}, ExplicitState
                ? TExprNodeList{test.List({test.Atom("state_table_path"), test.Atom("/Root/state")})}
                : TExprNodeList{});
        test.CheckType(aggregation);
        aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
        test.MarkStreaming(aggregation);
        test.MarkStreaming(aggregation->HeadPtr());

        TKikimrTablesData tables;
        auto stage = test.TableSinkStage("/Root/result", aggregation, tables);
        if (!HasSink) {
            stage = test.Ctx.ChangeChild(*stage, TDqPhyStage::idx_Outputs, test.List({}));
            test.MarkStreaming(stage);
        }

        const auto tx = test.Ctx.NewCallable(aggregation->Pos(), TKqpPhysicalTx::CallableName(), {
            test.List({stage}), test.List({}), test.List({}), test.List({})});
        THashSet<std::pair<ui64, ui64>> streamingResults;
        TExprNode::TPtr output = tx;
        const auto status = test.BuildStreamingFlow(tx, output, streamingResults, tables);
        UNIT_ASSERT_VALUES_EQUAL_C(status, ExplicitState ? IGraphTransformer::TStatus::Ok : IGraphTransformer::TStatus::Error,
            test.Ctx.IssueManager.GetIssues().ToString());

        if (!ExplicitState) {
            UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(),
                "require an identity finalizer or a finalizer equal to serialization");
        }
    }

    Y_UNIT_TEST(StreamingAggregationOutputStateRequiresEveryColumn) {
        for (const TStringBuf missing : {"", "key", "scalar", "first", "second"}) {
            TStreamingAggregationTypeAnnTest test;
            TExprNodeList mappings;
            for (const TStringBuf column : {"key", "scalar", "first", "second"}) {
                if (column != missing) {
                    mappings.push_back(test.List({test.Atom(column), test.Atom(column)}));
                }
            }
            auto node = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")}, {
                test.List({test.Atom("scalar"), test.Traits("state")}),
                test.List({test.Atom("first"), test.Traits("state")}),
                test.List({test.Atom("second"), test.Traits("state")})}, {
                    test.List({test.Atom("output_state_table"), test.List({test.Atom("/Root/result"),
                        test.List(std::move(mappings))})})});
            UNIT_ASSERT_VALUES_EQUAL_C(test.Annotate(node), missing.empty()
                ? IGraphTransformer::TStatus::Ok : IGraphTransformer::TStatus::Error,
                test.Ctx.IssueManager.GetIssues().ToString());
            if (!missing.empty()) {
                UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), TStringBuilder()
                    << "Missing streaming aggregation output state column mapping for: " << missing);
            }
        }
    }

    Y_UNIT_TEST_QUAD(StreamingAggregationOutputStateRejectsTupleTypes, OptionalTuple, Serialized) {
        TStreamingAggregationTypeAnnTest test;
        auto aggregation = test.TupleAggregation(OptionalTuple, Serialized);
        // Tuple results remain supported when not used as output-table state.
        test.CheckType(aggregation);
        aggregation = test.Ctx.ChangeChild(*aggregation, NNodes::TKqpStreamingAggregation::idx_Settings,
            test.List({test.TupleOutputStateSetting()}));
        UNIT_ASSERT_VALUES_EQUAL_C(test.Annotate(aggregation), IGraphTransformer::TStatus::Error,
            test.Ctx.IssueManager.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), "tuple splitting is not supported");
    }

    Y_UNIT_TEST_QUAD(StreamingAggregationOutputStateRejectsTupleSplitting, OptionalTuple, Serialized) {
        using namespace NNodes;
        for (const bool assigned : {false, true}) {
            TStreamingAggregationTypeAnnTest test;
            auto aggregation = test.TupleAggregation(OptionalTuple, Serialized);
            test.CheckType(aggregation);
            if (assigned) {
                // Also validate metadata already present in a typed physical plan.
                const auto type = aggregation->GetTypeAnn();
                aggregation = test.Ctx.ChangeChild(*aggregation, TKqpStreamingAggregation::idx_Settings,
                    test.List({test.TupleOutputStateSetting()}));
                aggregation->SetTypeAnn(type);
            }
            aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
            test.MarkStreaming(aggregation);
            test.MarkStreaming(aggregation->HeadPtr());
            TKikimrTablesData tables;
            const auto tx = test.Ctx.NewCallable(aggregation->Pos(), TKqpPhysicalTx::CallableName(), {
                test.List({test.TableSinkStage("/Root/result", aggregation, tables)}),
                test.List({}), test.List({}), test.List({})});
            VisitExpr(tx, [](const TExprNode::TPtr& node) {
                node->SetState(TExprNode::EState::ConstrComplete);
                return true;
            });
            THashSet<std::pair<ui64, ui64>> streamingResults;
            TExprNode::TPtr output = tx;
            const auto status = test.BuildStreamingFlow(tx, output, streamingResults, tables);
            UNIT_ASSERT_VALUES_EQUAL(status, IGraphTransformer::TStatus::Error);
            UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), "tuple splitting is not supported");
            if (!assigned) {
                test.Config->UseInMemoryStreamingAggregation = true;
                UNIT_ASSERT_VALUES_EQUAL(test.BuildStreamingFlow(tx, output, streamingResults, tables), IGraphTransformer::TStatus::Ok);
            }
        }
    }

    Y_UNIT_TEST_QUAD(StreamingAggregationOutputStateEligibility, StreamingQuery, DisableCheckpoints) {
        using namespace NNodes;
        for (const bool withRequestContext : {false, true}) {
            for (const bool identityFinish : {false, true}) {
                TStreamingAggregationTypeAnnTest test;
                test.Config->DisableCheckpoints = DisableCheckpoints;
                test.UserRequestContext->IsStreamingQuery = StreamingQuery;
                if (!withRequestContext) {
                    test.UserRequestContext.Reset();
                }
                auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
                    {test.List({test.Atom("value"), test.Traits(identityFinish ? "state" : "(Add state (Int64 '1))")})});
                test.CheckType(aggregation);
                aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
                test.MarkStreaming(aggregation);
                test.MarkStreaming(aggregation->HeadPtr());
                TKikimrTablesData tables;
                const auto tx = test.Ctx.NewCallable(aggregation->Pos(), TKqpPhysicalTx::CallableName(), {
                    test.List({test.TableSinkStage("/Root/result", aggregation, tables)}),
                    test.List({}), test.List({}), test.List({})});
                THashSet<std::pair<ui64, ui64>> streamingResults;
                TExprNode::TPtr output;
                const auto status = test.BuildStreamingFlow(tx, output, streamingResults, tables);
                if (!withRequestContext || !StreamingQuery || DisableCheckpoints) {
                    UNIT_ASSERT_VALUES_EQUAL_C(status, IGraphTransformer::TStatus::Ok, test.Ctx.IssueManager.GetIssues().ToString());
                    UNIT_ASSERT(output == tx);
                    UNIT_ASSERT(!GetSetting(TKqpStreamingAggregation(aggregation).Settings().Ref(), "output_state_table"));
                } else if (!identityFinish) {
                    UNIT_ASSERT_VALUES_EQUAL(status, IGraphTransformer::TStatus::Error);
                    UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), "require an identity finalizer");
                } else {
                    UNIT_ASSERT_VALUES_EQUAL_C(status, IGraphTransformer::TStatus::Repeat, test.Ctx.IssueManager.GetIssues().ToString());
                    const auto rewritten = FindNode(output, [](const TExprNode::TPtr& node) { return TKqpStreamingAggregation::Match(node.Get()); });
                    UNIT_ASSERT(rewritten);
                    UNIT_ASSERT(GetSetting(TKqpStreamingAggregation(rewritten).Settings().Ref(), "output_state_table"));
                }
            }
        }
    }

    Y_UNIT_TEST_QUAD(StreamingAggregationOutputStateWrites, SameTable, Assigned) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), test.Traits("state")})}, Assigned ? TExprNodeList{
                test.List({test.Atom("output_state_table"), test.List({test.Atom("/Root/result"), test.List({
                    test.List({test.Atom("key"), test.Atom("key")}),
                    test.List({test.Atom("value"), test.Atom("value")})})})})} : TExprNodeList{});
        test.CheckType(aggregation);
        aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
        test.MarkStreaming(aggregation);
        test.MarkStreaming(aggregation->HeadPtr());

        TKikimrTablesData tables;
        // SQL planning rejects intermediate writes earlier; construct a single physical transaction
        // to check that a second sink cannot overwrite the selected aggregation state.
        const auto tx = test.Ctx.NewCallable(aggregation->Pos(), TKqpPhysicalTx::CallableName(), {
            test.List({test.TableSinkStage("/Root/result", aggregation, tables),
                test.TableSinkStage(SameTable ? "/Root/result" : "/Root/other", aggregation->HeadPtr(), tables)}),
            test.List({}), test.List({}), test.List({})});
        VisitExpr(tx, [](const TExprNode::TPtr& node) {
            node->SetState(TExprNode::EState::ConstrComplete);
            return true;
        });
        THashSet<std::pair<ui64, ui64>> streamingResults;
        TExprNode::TPtr output;
        const auto status = test.BuildStreamingFlow(tx, output, streamingResults, tables);
        if (SameTable) {
            UNIT_ASSERT_VALUES_EQUAL(status, IGraphTransformer::TStatus::Error);
            UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), Assigned
                ? "Assigned streaming aggregation output state table is no longer eligible"
                : "output state table has other writes in the same transaction");
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(status, Assigned ? IGraphTransformer::TStatus::Ok : IGraphTransformer::TStatus::Repeat,
                test.Ctx.IssueManager.GetIssues().ToString());
            if (!Assigned) {
                ui32 assigned = 0;
                VisitExpr(output, [&](const TExprNode::TPtr& node) {
                    if (const auto agg = TMaybeNode<TKqpStreamingAggregation>(node)) {
                        const auto setting = GetSetting(agg.Cast().Settings().Ref(), "output_state_table");
                        UNIT_ASSERT(setting);
                        UNIT_ASSERT_VALUES_EQUAL(setting->Tail().Head().Content(), "/Root/result");
                        ++assigned;
                    }
                    return true;
                });
                UNIT_ASSERT_VALUES_EQUAL(assigned, 1);
            }
        }
    }

    Y_UNIT_TEST_TWIN(StreamingAggregationTableSinkModes, InMemory) {
        using namespace NNodes;

        for (const TStringBuf mode : {"", "upsert", "replace", "insert", "update", "delete"}) {
            TStreamingAggregationTypeAnnTest test;
            test.Config->UseInMemoryStreamingAggregation = InMemory;
            auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
                {test.List({test.Atom("value"), test.Traits("state")})});
            test.CheckType(aggregation);
            aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
            test.MarkStreaming(aggregation);
            test.MarkStreaming(aggregation->HeadPtr());

            TKikimrTablesData tables;
            auto stage = test.TableSinkStage("/Root/state", aggregation, tables);
            const auto resultStage = test.TableSinkStage("/Root/result", aggregation, tables);
            const auto sink = TDqPhyStage(resultStage).Outputs().Cast().Item(0).Cast<TDqSink>();
            auto settings = test.Ctx.ChangeChild(sink.Settings().Ref(), TKqpTableSinkSettings::idx_Mode, test.Atom(mode));
            auto resultSink = test.Ctx.ChangeChild(sink.Ref(), TDqSink::idx_Settings, std::move(settings));
            // An eligible state sink must not hide an invalid mode on another aggregation output.
            stage = test.Ctx.ChangeChild(*stage, TDqPhyStage::idx_Outputs,
                test.List({TDqPhyStage(stage).Outputs().Cast().Item(0).Ptr(), std::move(resultSink)}));
            test.MarkStreaming(stage);

            const auto tx = test.Ctx.NewCallable(aggregation->Pos(), TKqpPhysicalTx::CallableName(), {
                test.List({stage}), test.List({}), test.List({}), test.List({})});
            THashSet<std::pair<ui64, ui64>> streamingResults;
            TExprNode::TPtr output = tx;
            const auto status = test.BuildStreamingFlow(tx, output, streamingResults, tables);
            const bool allowed = mode.empty() || mode == "upsert" || mode == "replace";
            UNIT_ASSERT_VALUES_EQUAL_C(status, allowed
                ? (InMemory ? IGraphTransformer::TStatus::Ok : IGraphTransformer::TStatus::Repeat)
                : IGraphTransformer::TStatus::Error, TStringBuilder() << mode << ": " << test.Ctx.IssueManager.GetIssues().ToString());

            if (!allowed) {
                UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(),
                    "Streaming aggregation results can only be written with UPSERT or REPLACE");
            }
        }
    }

    Y_UNIT_TEST_TWIN(StreamingConstraintsResultBindingWithoutLocalStreamingNodes, StreamingProducer) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        TKikimrTablesData tables;
        auto rows = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")}, {})->HeadPtr();
        if (StreamingProducer) {
            test.MarkStreaming(rows);
        }
        auto producer = test.Ctx.NewCallable(rows->Pos(), TKqpPhysicalTx::CallableName(), {
            test.List({}), test.List({rows}), test.List({}), test.List({})});
        THashSet<std::pair<ui64, ui64>> streamingResults;
        auto output = producer;
        UNIT_ASSERT_VALUES_EQUAL(test.BuildStreamingFlow(producer, output, streamingResults, tables), IGraphTransformer::TStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(streamingResults.contains(std::make_pair(ui64{0}, ui64{0})), StreamingProducer);

        const auto* listType = test.Ctx.MakeType<TListExprType>(GetSeqItemType(rows->GetTypeAnn()));
        const auto type = ExpandType(rows->Pos(), *listType, test.Ctx);
        const auto binding = test.Ctx.NewCallable(rows->Pos(), TKqpTxResultBinding::CallableName(), {
            type, test.Atom("0"), test.Atom("0")});
        auto parameter = test.Ctx.NewCallable(rows->Pos(), "Parameter", {test.Atom("input"), type});
        parameter->SetTypeAnn(listType);
        auto input = test.Ctx.NewCallable(rows->Pos(), "ToFlow", {parameter});
        input->SetTypeAnn(rows->GetTypeAnn());
        input->SetState(TExprNode::EState::ConstrComplete);
        auto stage = test.TableSinkStage("/Root/result", input, tables);
        auto consumer = test.Ctx.NewCallable(rows->Pos(), TKqpPhysicalTx::CallableName(), {
            test.List({stage}), test.List({}), test.List({test.List({test.Atom("input"), binding})}), test.List({})});
        VisitExpr(consumer, [](const TExprNode::TPtr& node) {
            node->SetState(TExprNode::EState::ConstrComplete);
            return true;
        });
        output = consumer;
        const auto status = NKikimr::NKqp::NOpt::KqpBuildStreamingFlow(1, TKqpPhysicalTx(consumer), output,
            streamingResults, *test.Config, tables, "db", test.UserRequestContext.Get(), test.Ctx);
        UNIT_ASSERT_VALUES_EQUAL_C(status, StreamingProducer ? IGraphTransformer::TStatus::Error : IGraphTransformer::TStatus::Ok,
            test.Ctx.IssueManager.GetIssues().ToString());
        if (StreamingProducer) {
            UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(),
                "Streaming result binding input is materializing into tx precompute for transaction 1");
        }
    }

    Y_UNIT_TEST_QUAD(StreamingAggregationConditionalVariantOutputs, InMemory, Conditional) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        test.Config->UseInMemoryStreamingAggregation = InMemory;
        auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), test.Traits("state")})});
        const auto* rowType = test.CheckType(aggregation);
        test.MarkStreaming(aggregation);
        aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
        test.MarkStreaming(aggregation->HeadPtr());
        const auto pos = aggregation->Pos();
        const auto* variantType = test.Ctx.MakeType<TVariantExprType>(test.Ctx.MakeType<TTupleExprType>(
            TVector<const TTypeAnnotationNode*>{rowType, rowType, rowType}));
        auto row = test.Ctx.NewArgument(pos, "row");
        row->SetTypeAnn(rowType);
        auto value = test.Ctx.NewCallable(pos, "Member", {row, test.Atom("value")});
        value->SetTypeAnn(rowType->FindItemType("value"));
        auto zero = test.Ctx.NewCallable(pos, "Int64", {test.Atom("0")});
        zero->SetTypeAnn(value->GetTypeAnn());
        auto positive = test.Ctx.NewCallable(pos, "AggrGreater", {value, zero});
        positive->SetTypeAnn(test.Ctx.MakeType<TDataExprType>(EDataSlot::Bool));
        const auto variant = [&](TStringBuf index) {
            auto result = test.Ctx.NewCallable(pos, "Variant", {row, test.Atom(index), ExpandType(pos, *variantType, test.Ctx)});
            result->SetTypeAnn(variantType);
            return result;
        };
        auto conditional = test.Ctx.NewCallable(pos, "If", {positive, variant("0"), variant("1")});
        conditional->SetTypeAnn(variantType);
        auto lambda = test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {row}),
            Conditional ? TExprNodeList{conditional, variant("2")} : TExprNodeList{variant("0"), variant("1"), variant("2")});
        auto rows = test.Ctx.NewCallable(pos, "MultiMap", {aggregation, lambda});
        rows->SetTypeAnn(test.Ctx.MakeType<TFlowExprType>(variantType));
        UNIT_ASSERT_VALUES_EQUAL_C(test.Annotate(rows), IGraphTransformer::TStatus::Ok,
            test.Ctx.IssueManager.GetIssues().ToString());
        test.MarkStreaming(aggregation);
        test.MarkStreaming(aggregation->HeadPtr());
        aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
        test.MarkStreaming(rows);
        // Each output emits at most one unchanged row per input, so it preserves
        // the input's per-output distinct constraint, including conditional ports.
        TMultiConstraintNode::TMapType constraints;
        for (ui32 i = 0; i < 3; ++i) {
            constraints[i] = aggregation->GetConstraintSet();
        }
        rows->AddConstraint(test.Ctx.MakeConstraint<TMultiConstraintNode>(std::move(constraints)));
        TKikimrTablesData tables;
        auto stage = test.TableSinkStage("/Root/positive", rows, tables);
        TExprNodeList sinks{TDqPhyStage(stage).Outputs().Cast().Item(0).Ptr()};
        ui32 index = 1;
        for (const TStringBuf path : {"/Root/nonpositive", "/Root/state"}) {
            const auto other = test.TableSinkStage(path, aggregation, tables);
            sinks.emplace_back(test.Ctx.ChangeChild(TDqPhyStage(other).Outputs().Cast().Item(0).Ref(),
                TDqSink::idx_Index, test.Atom(ToString(index++))));
        }
        stage = test.Ctx.ChangeChild(*stage, TDqPhyStage::idx_Outputs, test.List(std::move(sinks)));
        test.MarkStreaming(stage);
        const auto tx = test.Ctx.NewCallable(pos, TKqpPhysicalTx::CallableName(), {
            test.List({stage}), test.List({}), test.List({}), test.List({})});
        THashSet<std::pair<ui64, ui64>> streamingResults;
        auto output = tx;
        const auto status = test.BuildStreamingFlow(tx, output, streamingResults, tables);
        UNIT_ASSERT_VALUES_EQUAL_C(status, Conditional ? IGraphTransformer::TStatus::Error :
            (InMemory ? IGraphTransformer::TStatus::Ok : IGraphTransformer::TStatus::Repeat),
            test.Ctx.IssueManager.GetIssues().ToString());
        if (Conditional) {
            UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(),
                "Filtering over streaming aggregation results is not supported");
        }
    }

    Y_UNIT_TEST(StreamingAggregationBuildFlowRepeatedValidation) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), test.Traits("state", "(Null)",
                "(StructType '('key (DataType 'String)))", "(Just state)", "(Unwrap state)")})});
        test.CheckType(aggregation);
        const auto distinct = test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"});
        aggregation->AddConstraint(distinct);
        test.MarkStreaming(aggregation);
        test.MarkStreaming(aggregation->HeadPtr());
        TKikimrTablesData tables;
        auto tx = test.Ctx.NewCallable(aggregation->Pos(), TKqpPhysicalTx::CallableName(), {
            test.List({test.TableSinkStage("/Root/result", aggregation, tables)}),
            test.List({aggregation->HeadPtr()}), test.List({}), test.List({})});
        THashSet<std::pair<ui64, ui64>> streamingResults;
        TExprNode::TPtr output;
        UNIT_ASSERT_VALUES_EQUAL(test.BuildStreamingFlow(tx, output, streamingResults, tables), IGraphTransformer::TStatus::Repeat);
        UNIT_ASSERT(output != tx);
        auto rewritten = FindNode(output, [](const TExprNode::TPtr& node) { return TKqpStreamingAggregation::Match(node.Get()); });
        UNIT_ASSERT(rewritten);
        auto annotated = rewritten;
        // BuildStreamingFlow marks the synthetic graph constraint-complete. Newly inserted
        // settings still need their types inferred before running the annotation pipeline.
        VisitExpr(annotated, [](const TExprNode::TPtr& node) {
            if (!node->GetTypeAnn()) {
                node->SetState(TExprNode::EState::Initial);
            }
            return true;
        });
        test.CheckType(annotated);
        const auto traits = TKqpStreamingAggregation(annotated).Handlers().Item(0).Trait().Cast<TCoAggregationTraits>();
        UNIT_ASSERT(IsIdentityLambda(traits.SaveHandler().Ref()));
        UNIT_ASSERT(IsIdentityLambda(traits.LoadHandler().Ref()));
        annotated->AddConstraint(distinct);
        test.MarkStreaming(annotated);
        tx = test.Ctx.ReplaceNode(std::move(output), *rewritten, annotated);
        // The production pipeline recomputes enclosing stage constraints after annotation.
        for (const auto& stage : TKqpPhysicalTx(tx).Stages()) {
            test.MarkStreaming(stage.Program().Ptr());
            test.MarkStreaming(stage.Ptr());
        }
        for (ui32 i = 0; i < 2; ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(test.BuildStreamingFlow(tx, output, streamingResults, tables), IGraphTransformer::TStatus::Ok,
                test.Ctx.IssueManager.GetIssues().ToString());
            UNIT_ASSERT(output == tx);
            UNIT_ASSERT_VALUES_EQUAL(streamingResults.size(), 1);
        }
    }

    Y_UNIT_TEST(StreamingAggregationRejectsUnsupportedFlowOperators) {
        using namespace NNodes;
        for (const TStringBuf callable : {"LMap", "OrderedLMap", "Unordered", "ExtractMembers", "Skip"}) {
            TStreamingAggregationTypeAnnTest test;
            auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
                {test.List({test.Atom("value"), test.Traits("state")})});
            test.CheckType(aggregation);
            test.MarkStreaming(aggregation);
            aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
            test.MarkStreaming(aggregation->HeadPtr());
            TExprNodeList children = {aggregation};
            if (callable == "LMap" || callable == "OrderedLMap") {
                auto arg = test.Ctx.NewArgument(aggregation->Pos(), "rows");
                arg->SetTypeAnn(aggregation->GetTypeAnn());
                test.CopyConstraints(arg, *aggregation);
                auto lambda = test.Ctx.NewLambda(aggregation->Pos(), test.Ctx.NewArguments(aggregation->Pos(), {arg}), TExprNode::TPtr(arg));
                test.MarkStreaming(lambda);
                children.push_back(std::move(lambda));
            } else if (callable == "ExtractMembers") {
                children.push_back(test.List({test.Atom("key"), test.Atom("value")}));
            } else if (callable == "Skip") {
                // SQL LIMIT ... OFFSET ... fails at LIMIT before reaching this check.
                // Bypass the independent checkpoint restriction to reach aggregation validation.
                test.Config->DisableCheckpoints = true;
                children.push_back(test.Ctx.NewCallable(aggregation->Pos(), "Uint64", {test.Atom("1")}));
            }
            auto rows = test.Ctx.NewCallable(aggregation->Pos(), callable, std::move(children));
            rows->SetTypeAnn(aggregation->GetTypeAnn());
            test.CopyConstraints(rows, *aggregation);
            TKikimrTablesData tables;
            const auto tx = test.Ctx.NewCallable(rows->Pos(), TKqpPhysicalTx::CallableName(), {
                test.List({test.TableSinkStage("/Root/result", rows, tables)}), test.List({}), test.List({}), test.List({})});
            THashSet<std::pair<ui64, ui64>> streamingResults;
            TExprNode::TPtr output = tx;
            UNIT_ASSERT_VALUES_EQUAL_C(test.BuildStreamingFlow(tx, output, streamingResults, tables), IGraphTransformer::TStatus::Error, callable);
            if (callable == "Skip") {
                UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(),
                    "OFFSET operator is not supported over streaming aggregation results");
            } else {
                UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), TStringBuilder()
                    << "Unsupported callable for processing streaming aggregation results: '" << callable << "'");
            }
        }
    }

    Y_UNIT_TEST_TWIN(StreamingAggregationSwitchOutputs, InMemory) {
        using namespace NNodes;
        for (const TStringBuf mode : {"preserve", "reorder", "filter", "discard", "omit", "map"}) {
            TStreamingAggregationTypeAnnTest test;
            test.Config->UseInMemoryStreamingAggregation = InMemory;
            auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
                {test.List({test.Atom("value"), test.Traits("state")})});
            const auto* rowType = test.CheckType(aggregation);
            test.MarkStreaming(aggregation);
            aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
            test.MarkStreaming(aggregation->HeadPtr());
            const auto pos = aggregation->Pos();

            auto firstArg = test.Ctx.NewArgument(pos, "first");
            firstArg->SetTypeAnn(aggregation->GetTypeAnn());
            test.CopyConstraints(firstArg, *aggregation);
            auto first = test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {firstArg}), TExprNode::TPtr(firstArg));
            test.CopyConstraints(first, *aggregation);

            auto secondArg = test.Ctx.NewArgument(pos, "second");
            secondArg->SetTypeAnn(aggregation->GetTypeAnn());
            test.CopyConstraints(secondArg, *aggregation);
            auto row = test.Ctx.NewArgument(pos, "row");
            row->SetTypeAnn(rowType);
            auto key = test.Ctx.NewCallable(pos, "Member", {row, test.Atom("key")});
            key->SetTypeAnn(rowType->FindItemType("key"));
            auto value = test.Ctx.NewCallable(pos, "Member", {row, test.Atom("value")});
            value->SetTypeAnn(rowType->FindItemType("value"));
            const auto* storedType = test.Ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
                test.Ctx.MakeType<TItemExprType>("key", key->GetTypeAnn()),
                test.Ctx.MakeType<TItemExprType>("stored", value->GetTypeAnn())});
            auto renamed = test.Ctx.NewCallable(pos, "AsStruct", {
                test.List({test.Atom("key"), key}), test.List({test.Atom("stored"), value})});
            renamed->SetTypeAnn(storedType);
            auto mapLambda = test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {row}), std::move(renamed));
            auto secondBody = test.Ctx.NewCallable(pos, "Map", {secondArg, mapLambda});
            const auto* storedFlow = test.Ctx.MakeType<TFlowExprType>(storedType);
            secondBody->SetTypeAnn(storedFlow);
            test.CopyConstraints(secondBody, *aggregation);
            if (mode == "filter") {
                auto filterArg = test.Ctx.NewArgument(pos, "item");
                filterArg->SetTypeAnn(storedType);
                auto predicate = test.Ctx.NewCallable(pos, "Bool", {test.Atom("true")});
                predicate->SetTypeAnn(test.Ctx.MakeType<TDataExprType>(EDataSlot::Bool));
                auto lambda = test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {filterArg}), std::move(predicate));
                secondBody = test.Ctx.NewCallable(pos, "Filter", {secondBody, lambda});
                secondBody->SetTypeAnn(storedFlow);
                test.CopyConstraints(secondBody, *aggregation);
            } else if (mode == "discard") {
                secondBody = test.Ctx.NewArgument(pos, "unrelatedStream");
                secondBody->SetTypeAnn(storedFlow);
                test.CopyConstraints(secondBody, *aggregation);
            }
            auto second = test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {secondArg}), TExprNode::TPtr(secondBody));
            test.CopyConstraints(second, *aggregation);
            auto rows = test.Ctx.NewCallable(pos, "Switch", {
                aggregation, test.Atom("1000"), test.List({test.Atom("0")}), first,
                test.List({test.Atom("0")}), second});
            rows->SetTypeAnn(test.Ctx.MakeType<TFlowExprType>(test.Ctx.MakeType<TVariantExprType>(
                test.Ctx.MakeType<TTupleExprType>(TVector<const TTypeAnnotationNode*>{rowType, storedType}))));
            test.MarkStreaming(rows);
            TMultiConstraintNode::TMapType constraints;
            constraints[0] = firstArg->GetConstraintSet();
            constraints[1] = secondBody->GetConstraintSet();
            rows->AddConstraint(test.Ctx.MakeConstraint<TMultiConstraintNode>(std::move(constraints)));

            if (mode == "map") {
                auto item = test.Ctx.NewArgument(pos, "variant");
                item->SetTypeAnn(GetSeqItemType(rows->GetTypeAnn()));
                auto lambda = test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {item}), TExprNode::TPtr(item));
                auto mapped = test.Ctx.NewCallable(pos, "Map", {rows, lambda});
                mapped->SetTypeAnn(rows->GetTypeAnn());
                test.CopyConstraints(mapped, *rows);
                rows = std::move(mapped);
            }

            if (mode == "reorder" || mode == "omit") {
                TExprNodeList children = {rows, test.Atom("1000")};
                for (const ui32 index : {1u, 0u}) {
                    if (mode == "omit" && index == 0) {
                        continue;
                    }
                    auto arg = test.Ctx.NewArgument(pos, "selected");
                    arg->SetTypeAnn(index == 1 ? storedFlow : aggregation->GetTypeAnn());
                    test.CopyConstraints(arg, *aggregation);
                    auto handler = test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {arg}), TExprNode::TPtr(arg));
                    test.CopyConstraints(handler, *aggregation);
                    children.push_back(test.List({test.Atom(ToString(index))}));
                    children.push_back(std::move(handler));
                }
                const auto routed = test.Ctx.NewCallable(pos, "Switch", std::move(children));
                if (mode == "reorder") {
                    routed->SetTypeAnn(test.Ctx.MakeType<TFlowExprType>(test.Ctx.MakeType<TVariantExprType>(
                        test.Ctx.MakeType<TTupleExprType>(TVector<const TTypeAnnotationNode*>{storedType, rowType}))));
                    test.CopyConstraints(routed, *rows);
                } else {
                    routed->SetTypeAnn(storedFlow);
                    test.CopyConstraints(routed, *aggregation);
                }
                rows = routed;
            }

            TKikimrTablesData tables;
            auto stage = test.TableSinkStage("/Root/z_other", rows, tables);
            const auto stateStage = test.TableSinkStage("/Root/a_state", secondBody, tables);
            auto stateSink = test.Ctx.ChangeChild(TDqPhyStage(stateStage).Outputs().Cast().Item(0).Ref(),
                TDqSink::idx_Index, test.Atom(mode == "reorder" || mode == "omit" ? "0" : "1"));
            const auto otherSink = test.Ctx.ChangeChild(TDqPhyStage(stage).Outputs().Cast().Item(0).Ref(),
                TDqSink::idx_Index, test.Atom(mode == "reorder" ? "1" : "0"));
            stage = test.Ctx.ChangeChild(*stage, TDqPhyStage::idx_Outputs,
                test.List(mode == "omit" ? TExprNodeList{stateSink} : TExprNodeList{otherSink, stateSink}));
            test.MarkStreaming(stage);
            const auto tx = test.Ctx.NewCallable(pos, TKqpPhysicalTx::CallableName(), {
                test.List({stage}), test.List({}), test.List({}), test.List({})});
            THashSet<std::pair<ui64, ui64>> streamingResults;
            TExprNode::TPtr output = tx;
            const auto status = test.BuildStreamingFlow(tx, output, streamingResults, tables);
            if (mode != "preserve" && mode != "reorder") {
                UNIT_ASSERT_VALUES_EQUAL_C(status, IGraphTransformer::TStatus::Error, mode);
                UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), mode == "filter"
                    ? "Filtering over streaming aggregation results is not supported"
                    : mode == "discard" ? "Switch handler discards the streaming aggregation output"
                    : mode == "map" ? "Mapping over multiple input variants is not supported for streaming aggregation results"
                    : "Switch discards the streaming aggregation output");
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(status, InMemory ? IGraphTransformer::TStatus::Ok : IGraphTransformer::TStatus::Repeat,
                test.Ctx.IssueManager.GetIssues().ToString());

            if (InMemory) {
                continue;
            }

            const auto rewritten = FindNode(output, [](const TExprNode::TPtr& node) { return TKqpStreamingAggregation::Match(node.Get()); });
            UNIT_ASSERT(rewritten);
            const auto setting = GetSetting(TKqpStreamingAggregation(rewritten).Settings().Ref(), "output_state_table");
            UNIT_ASSERT(setting);
            UNIT_ASSERT_VALUES_EQUAL(setting->Tail().Head().Content(), "/Root/a_state");
            UNIT_ASSERT_VALUES_EQUAL(setting->Tail().Tail().Child(1)->Head().Content(), "value");
            UNIT_ASSERT_VALUES_EQUAL(setting->Tail().Tail().Child(1)->Tail().Content(), "stored");
        }
    }

    Y_UNIT_TEST(StreamingAggregationRejectsAggregationInsideSwitchHandler) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), test.Traits("state")})});
        test.CheckType(aggregation);
        test.MarkStreaming(aggregation);
        aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
        test.MarkStreaming(aggregation->HeadPtr());
        auto input = test.Ctx.NewArgument(aggregation->Pos(), "stream");
        input->SetTypeAnn(aggregation->Head().GetTypeAnn());
        test.MarkStreaming(input);
        auto handler = test.Ctx.NewLambda(aggregation->Pos(),
            test.Ctx.NewArguments(aggregation->Pos(), {aggregation->HeadPtr()}), TExprNode::TPtr(aggregation));
        test.MarkStreaming(handler);
        auto rows = test.Ctx.NewCallable(aggregation->Pos(), "Switch", {
            input, test.Atom("1000"), test.List({test.Atom("0")}), handler});
        rows->SetTypeAnn(aggregation->GetTypeAnn());
        test.MarkStreaming(rows);
        TKikimrTablesData tables;
        const auto tx = test.Ctx.NewCallable(rows->Pos(), TKqpPhysicalTx::CallableName(), {
            test.List({test.TableSinkStage("/Root/result", rows, tables)}), test.List({}), test.List({}), test.List({})});
        THashSet<std::pair<ui64, ui64>> streamingResults;
        TExprNode::TPtr output = tx;
        UNIT_ASSERT_VALUES_EQUAL_C(test.BuildStreamingFlow(tx, output, streamingResults, tables), IGraphTransformer::TStatus::Error,
            test.Ctx.IssueManager.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(),
            "Streaming aggregation inside switch handler is not supported");
    }

    Y_UNIT_TEST_TWIN(StreamingAggregationRejectsSwitchCapturedStream, InMemory) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        test.Config->UseInMemoryStreamingAggregation = InMemory;
        auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), test.Traits("state")})});
        test.CheckType(aggregation);
        test.MarkStreaming(aggregation);
        aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
        test.MarkStreaming(aggregation->HeadPtr());
        const auto pos = aggregation->Pos();
        TKikimrTablesData tables;
        auto producer = test.TableSinkStage("/Root/producer", aggregation, tables);
        producer = test.Ctx.ChangeChild(*producer, TDqPhyStage::idx_Outputs, test.List({}));
        test.MarkStreaming(producer);
        auto output = test.Ctx.NewCallable(pos, TDqOutput::CallableName(), {producer, test.Atom("0")});
        output->SetTypeAnn(aggregation->GetTypeAnn());
        test.CopyConstraints(output, *aggregation);
        auto connection = test.Ctx.NewCallable(pos, TDqCnUnionAll::CallableName(), {output});
        connection->SetTypeAnn(aggregation->GetTypeAnn());
        test.CopyConstraints(connection, *aggregation);
        auto captured = test.Ctx.NewArgument(pos, "captured");
        captured->SetTypeAnn(aggregation->GetTypeAnn());
        test.CopyConstraints(captured, *aggregation);
        const auto ordinary = [&](TStringBuf name) {
            auto arg = test.Ctx.NewArgument(pos, name);
            arg->SetTypeAnn(aggregation->Head().GetTypeAnn());
            test.MarkStreaming(arg);
            return arg;
        };
        auto source = ordinary("source");
        auto raw = ordinary("raw");
        auto handler = test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {ordinary("items")}), TExprNode::TPtr(captured));
        test.CopyConstraints(handler, *aggregation);
        auto rows = test.Ctx.NewCallable(pos, "Switch", {raw, test.Atom("1000"), test.List({test.Atom("0")}), handler});
        rows->SetTypeAnn(aggregation->GetTypeAnn());
        test.CopyConstraints(rows, *aggregation);
        auto program = test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {captured, raw}), TExprNode::TPtr(rows));
        test.MarkStreaming(program);
        auto consumer = test.TableSinkStage("/Root/result", rows, tables);
        consumer = test.Ctx.ChangeChild(*consumer, TDqPhyStage::idx_Inputs, test.List({connection, source}));
        consumer = test.Ctx.ChangeChild(*consumer, TDqPhyStage::idx_Program, std::move(program));
        test.MarkStreaming(consumer);
        const auto tx = test.Ctx.NewCallable(pos, TKqpPhysicalTx::CallableName(), {
            test.List({producer, consumer}), test.List({}), test.List({}), test.List({})});
        THashSet<std::pair<ui64, ui64>> streamingResults;
        output = tx;
        UNIT_ASSERT_VALUES_EQUAL(test.BuildStreamingFlow(tx, output, streamingResults, tables), IGraphTransformer::TStatus::Error);
        UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), "Streaming aggregation inside switch handler is not supported");
    }

    Y_UNIT_TEST(StreamingAggregationRejectsFlatteningStreamingInput) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), test.Traits("state")})});
        test.CheckType(aggregation);
        test.MarkStreaming(aggregation);
        aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
        test.MarkStreaming(aggregation->HeadPtr());
        const auto* rowType = GetSeqItemType(aggregation->GetTypeAnn());
        auto arg = test.Ctx.NewArgument(aggregation->Pos(), "row");
        arg->SetTypeAnn(rowType);
        auto input = aggregation;
        auto body = test.Ctx.NewCallable(aggregation->Pos(), "Just", {arg});
        body->SetTypeAnn(test.Ctx.MakeType<TOptionalExprType>(rowType));
        auto lambda = test.Ctx.NewLambda(aggregation->Pos(), test.Ctx.NewArguments(aggregation->Pos(), {arg}), std::move(body));
        auto rows = test.Ctx.NewCallable(aggregation->Pos(), "FlatMap", {input, lambda});
        rows->SetTypeAnn(aggregation->GetTypeAnn());
        test.CopyConstraints(rows, *aggregation);
        TKikimrTablesData tables;
        const auto tx = test.Ctx.NewCallable(rows->Pos(), TKqpPhysicalTx::CallableName(), {
            test.List({test.TableSinkStage("/Root/result", rows, tables)}), test.List({}), test.List({}), test.List({})});
        THashSet<std::pair<ui64, ui64>> streamingResults;
        TExprNode::TPtr output = tx;
        UNIT_ASSERT_VALUES_EQUAL_C(test.BuildStreamingFlow(tx, output, streamingResults, tables),
            IGraphTransformer::TStatus::Error,
            test.Ctx.IssueManager.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), "Flattening streaming aggregation results is not supported");
    }

    Y_UNIT_TEST_TWIN(StreamingAggregationSharedLambdaDag, InMemory) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        test.Config->UseInMemoryStreamingAggregation = InMemory;
        auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), test.Traits("state")})});
        test.CheckType(aggregation);
        test.MarkStreaming(aggregation);
        aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
        test.MarkStreaming(aggregation->HeadPtr());
        const auto pos = aggregation->Pos();
        const auto argument = [&] {
            auto arg = test.Ctx.NewArgument(pos, "rows");
            arg->SetTypeAnn(aggregation->GetTypeAnn());
            test.CopyConstraints(arg, *aggregation);
            return arg;
        };
        const auto route = [&](const TExprNode::TPtr& input, const TExprNode::TPtr& handler) {
            auto rows = test.Ctx.NewCallable(pos, "Switch", {input, test.Atom("1000"), test.List({test.Atom("0")}), handler});
            rows->SetTypeAnn(aggregation->GetTypeAnn());
            test.CopyConstraints(rows, *aggregation);
            return rows;
        };
        auto arg = argument();
        auto handler = test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {arg}), TExprNode::TPtr(arg));
        test.MarkStreaming(handler);
        // Linear-size DAG with more than 2^24 invocation paths. Each shared lambda
        // must be summarized once, independently of the caller's argument bindings.
        for (ui32 depth = 0; depth < 24; ++depth) {
            arg = argument();
            auto body = route(route(arg, handler), handler);
            handler = test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {arg}), std::move(body));
            test.MarkStreaming(handler);
        }
        const auto* rowType = GetSeqItemType(aggregation->GetTypeAnn())->Cast<TStructExprType>();
        const auto* valueType = rowType->FindItemType("value");
        const auto* optionalType = test.Ctx.MakeType<TOptionalExprType>(valueType);
        const auto scalarArgument = [&] {
            auto value = test.Ctx.NewArgument(pos, "value");
            value->SetTypeAnn(valueType);
            return value;
        };
        const auto just = [&](const TExprNode::TPtr& value) {
            auto result = test.Ctx.NewCallable(pos, "Just", {value});
            result->SetTypeAnn(optionalType);
            return result;
        };
        const auto flatMap = [&](const TExprNode::TPtr& input, const TExprNode::TPtr& lambda) {
            auto result = test.Ctx.NewCallable(pos, "FlatMap", {input, lambda});
            result->SetTypeAnn(optionalType);
            return result;
        };
        auto valueArg = scalarArgument();
        auto scalarHandler = test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {valueArg}), just(valueArg));
        for (ui32 depth = 0; depth < 24; ++depth) {
            valueArg = scalarArgument();
            auto body = flatMap(flatMap(just(valueArg), scalarHandler), scalarHandler);
            scalarHandler = test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {valueArg}), std::move(body));
        }
        auto row = test.Ctx.NewArgument(pos, "row");
        row->SetTypeAnn(rowType);
        auto key = test.Ctx.NewCallable(pos, "Member", {row, test.Atom("key")});
        key->SetTypeAnn(rowType->FindItemType("key"));
        auto value = test.Ctx.NewCallable(pos, "Member", {row, test.Atom("value")});
        value->SetTypeAnn(valueType);
        auto restored = test.Ctx.NewCallable(pos, "Unwrap", {flatMap(just(value), scalarHandler)});
        restored->SetTypeAnn(valueType);
        auto projected = test.Ctx.NewCallable(pos, "AsStruct", {
            test.List({test.Atom("key"), key}), test.List({test.Atom("value"), restored})});
        projected->SetTypeAnn(rowType);
        auto rows = test.Ctx.NewCallable(pos, "Map", {route(aggregation, handler),
            test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {row}), std::move(projected))});
        rows->SetTypeAnn(aggregation->GetTypeAnn());
        test.CopyConstraints(rows, *aggregation);
        TKikimrTablesData tables;
        const auto tx = test.Ctx.NewCallable(pos, TKqpPhysicalTx::CallableName(), {
            test.List({test.TableSinkStage("/Root/result", rows, tables)}),
            test.List({}), test.List({}), test.List({})});
        THashSet<std::pair<ui64, ui64>> streamingResults;
        TExprNode::TPtr output = tx;
        UNIT_ASSERT_VALUES_EQUAL_C(test.BuildStreamingFlow(tx, output, streamingResults, tables),
            InMemory ? IGraphTransformer::TStatus::Ok : IGraphTransformer::TStatus::Repeat,
            test.Ctx.IssueManager.GetIssues().ToString());
        const auto rewritten = FindNode(output, [](const TExprNode::TPtr& node) { return TKqpStreamingAggregation::Match(node.Get()); });
        UNIT_ASSERT(rewritten);
        const auto setting = GetSetting(TKqpStreamingAggregation(rewritten).Settings().Ref(), "output_state_table");
        UNIT_ASSERT_VALUES_EQUAL(bool(setting), !InMemory);
        if (setting) {
            UNIT_ASSERT_VALUES_EQUAL(setting->Tail().Head().Content(), "/Root/result");
        }
    }

    Y_UNIT_TEST_TWIN(StreamingAggregationStageInputOrigins, InMemory) {
        using namespace NNodes;

        for (const TStringBuf mode : {"same_origin", "different_origins", "shared_program", "local", "ordinary"}) {
            TStreamingAggregationTypeAnnTest test;
            test.Config->UseInMemoryStreamingAggregation = InMemory;
            const auto makeAggregation = [&] {
                auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
                    {test.List({test.Atom("value"), test.Traits("state")})});
                test.CheckType(aggregation);
                test.MarkStreaming(aggregation);
                aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
                test.MarkStreaming(aggregation->HeadPtr());
                return aggregation;
            };
            const auto aggregation = makeAggregation();
            const auto pos = aggregation->Pos();
            TKikimrTablesData tables;
            auto producer = test.TableSinkStage("/Root/first", aggregation, tables);
            producer = test.Ctx.ChangeChild(*producer, TDqPhyStage::idx_Outputs, test.List({}));
            test.MarkStreaming(producer);
            TExprNodeList stages = {producer};
            TExprNodeList inputs;
            TExprNodeList arguments;
            const auto connect = [&](const TExprNode::TPtr& stage, const TExprNode::TPtr& rows) {
                auto output = test.Ctx.NewCallable(pos, TDqOutput::CallableName(), {stage, test.Atom("0")});
                output->SetTypeAnn(rows->GetTypeAnn());
                test.CopyConstraints(output, *rows);
                auto connection = test.Ctx.NewCallable(pos, TDqCnUnionAll::CallableName(), {output});
                connection->SetTypeAnn(rows->GetTypeAnn());
                test.CopyConstraints(connection, *rows);
                inputs.push_back(connection);
                auto arg = test.Ctx.NewArgument(pos, "rows");
                arg->SetTypeAnn(rows->GetTypeAnn());
                test.CopyConstraints(arg, *rows);
                arguments.push_back(arg);
            };
            connect(producer, aggregation);
            TExprNode::TPtr body;

            if (mode == "local") {
                body = test.Ctx.ChangeChild(*makeAggregation(), TKqpStreamingAggregation::idx_Input, TExprNode::TPtr(arguments.front()));
                body->SetTypeAnn(aggregation->GetTypeAnn());
                test.CopyConstraints(body, *aggregation);
            } else {
                auto rows = aggregation;

                if (mode != "same_origin") {
                    rows = mode == "ordinary" ? aggregation->HeadPtr() : mode == "shared_program" ? aggregation : makeAggregation();
                    auto second = test.TableSinkStage("/Root/second", rows, tables);
                    second = test.Ctx.ChangeChild(*second, TDqPhyStage::idx_Outputs, test.List({}));

                    if (mode == "shared_program") {
                        second = test.Ctx.ChangeChild(*second, TDqPhyStage::idx_Program, producer->ChildPtr(TDqPhyStage::idx_Program));
                    }

                    test.MarkStreaming(second);
                    stages.push_back(second);
                    producer = second;
                }

                connect(producer, rows);
                body = mode == "ordinary" ? arguments.front() : arguments.back();
            }

            auto consumer = test.TableSinkStage("/Root/result", body, tables);
            auto program = test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, std::move(arguments)), std::move(body));
            test.MarkStreaming(program);
            consumer = test.Ctx.ChangeChild(*consumer, TDqPhyStage::idx_Inputs, test.List(std::move(inputs)));
            consumer = test.Ctx.ChangeChild(*consumer, TDqPhyStage::idx_Program, std::move(program));
            test.MarkStreaming(consumer);
            stages.push_back(consumer);
            const auto tx = test.Ctx.NewCallable(pos, TKqpPhysicalTx::CallableName(), {
                test.List(std::move(stages)), test.List({}), test.List({}), test.List({})});
            THashSet<std::pair<ui64, ui64>> streamingResults;
            TExprNode::TPtr output = tx;
            const auto status = test.BuildStreamingFlow(tx, output, streamingResults, tables);
            const auto issues = test.Ctx.IssueManager.GetIssues().ToString();

            if (mode == "ordinary") {
                UNIT_ASSERT_VALUES_EQUAL_C(status, InMemory ? IGraphTransformer::TStatus::Ok : IGraphTransformer::TStatus::Repeat, issues);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(status, IGraphTransformer::TStatus::Error, mode);
                UNIT_ASSERT_STRING_CONTAINS(issues, mode == "same_origin"
                    ? "Stage program discards the streaming aggregation output"
                    : "A physical stage may process results from at most one streaming aggregation origin");
            }
        }
    }

    Y_UNIT_TEST_QUAD(StreamingAggregationLocalOrigins, InMemory, SharedNode) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        test.Config->UseInMemoryStreamingAggregation = InMemory;
        TExprNodeList aggregations;

        for (ui32 i = 0; i < 2; ++i) {
            if (SharedNode && i) {
                aggregations.push_back(aggregations.front());
                continue;
            }

            auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
                {test.List({test.Atom("value"), test.Traits("state")})});
            test.CheckType(aggregation);
            test.MarkStreaming(aggregation);
            aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
            test.MarkStreaming(aggregation->HeadPtr());
            aggregations.push_back(aggregation);
        }

        const auto aggregation = aggregations.front();
        auto rows = test.Ctx.NewCallable(aggregation->Pos(), "Extend", std::move(aggregations));
        rows->SetTypeAnn(aggregation->GetTypeAnn());
        test.CopyConstraints(rows, *aggregation);
        TKikimrTablesData tables;
        const auto tx = test.Ctx.NewCallable(rows->Pos(), TKqpPhysicalTx::CallableName(), {
            test.List({test.TableSinkStage("/Root/result", rows, tables)}), test.List({}), test.List({}), test.List({})});
        THashSet<std::pair<ui64, ui64>> streamingResults;
        TExprNode::TPtr output = tx;
        UNIT_ASSERT_VALUES_EQUAL(test.BuildStreamingFlow(tx, output, streamingResults, tables), IGraphTransformer::TStatus::Error);
        UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), SharedNode
            ? "Union of streaming aggregation results with another data is not supported"
            : "A physical stage may process results from at most one streaming aggregation origin");
    }

    Y_UNIT_TEST_TWIN(StreamingAggregationRejectsResultBinding, InMemory) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        test.Config->UseInMemoryStreamingAggregation = InMemory;
        auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), test.Traits("state")})});
        test.CheckType(aggregation);
        test.MarkStreaming(aggregation);
        aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
        test.MarkStreaming(aggregation->HeadPtr());
        TKikimrTablesData tables;
        auto stage = test.TableSinkStage("/Root/result", aggregation, tables);
        stage = test.Ctx.ChangeChild(*stage, TDqPhyStage::idx_Outputs, test.List({}));
        test.MarkStreaming(stage);
        auto output = test.Ctx.NewCallable(aggregation->Pos(), TDqOutput::CallableName(), {stage, test.Atom("0")});
        output->SetTypeAnn(aggregation->GetTypeAnn());
        test.CopyConstraints(output, *aggregation);
        auto result = test.Ctx.NewCallable(aggregation->Pos(), TDqCnResult::CallableName(), {output, test.List({})});
        result->SetTypeAnn(aggregation->GetTypeAnn());
        test.CopyConstraints(result, *aggregation);
        const auto tx = test.Ctx.NewCallable(aggregation->Pos(), TKqpPhysicalTx::CallableName(), {
            test.List({stage}), test.List({result}), test.List({}), test.List({})});
        THashSet<std::pair<ui64, ui64>> streamingResults;
        output = tx;
        UNIT_ASSERT_VALUES_EQUAL(test.BuildStreamingFlow(tx, output, streamingResults, tables), IGraphTransformer::TStatus::Error);
        UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), "materialization into query results or precompute is not supported");
    }

    Y_UNIT_TEST(StreamingAggregationOutputStateSharedStageLambda) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        TKikimrTablesData tables;
        TExprNodeList stages;
        THashMap<const TExprNode*, TString> expectedTables;
        TExprNode::TPtr sharedProgram;
        const auto distinct = test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"});
        for (const TStringBuf path : {"/Root/first", "/Root/second"}) {
            auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
                {test.List({test.Atom("value"), test.Traits("state")})});
            test.CheckType(aggregation);
            test.MarkStreaming(aggregation);
            aggregation->AddConstraint(distinct);
            test.MarkStreaming(aggregation->HeadPtr());
            expectedTables.emplace(&aggregation->Head(), TString(path));
            auto producer = test.TableSinkStage(path, aggregation, tables);
            producer = test.Ctx.ChangeChild(*producer, TDqPhyStage::idx_Outputs, test.List({}));
            test.MarkStreaming(producer);
            auto connectionOutput = test.Ctx.NewCallable(aggregation->Pos(), TDqOutput::CallableName(), {producer, test.Atom("0")});
            connectionOutput->SetTypeAnn(aggregation->GetTypeAnn());
            test.CopyConstraints(connectionOutput, *aggregation);
            auto connection = test.Ctx.NewCallable(aggregation->Pos(), TDqCnUnionAll::CallableName(), {connectionOutput});
            connection->SetTypeAnn(aggregation->GetTypeAnn());
            test.CopyConstraints(connection, *aggregation);
            if (!sharedProgram) {
                auto arg = test.Ctx.NewArgument(aggregation->Pos(), "rows");
                arg->SetTypeAnn(aggregation->GetTypeAnn());
                test.CopyConstraints(arg, *aggregation);
                sharedProgram = test.Ctx.NewLambda(aggregation->Pos(), test.Ctx.NewArguments(aggregation->Pos(), {arg}), TExprNode::TPtr(arg));
                test.MarkStreaming(sharedProgram);
            }
            auto consumer = test.TableSinkStage(path, sharedProgram->TailPtr(), tables);
            consumer = test.Ctx.ChangeChild(*consumer, TDqPhyStage::idx_Inputs, test.List({connection}));
            consumer = test.Ctx.ChangeChild(*consumer, TDqPhyStage::idx_Program, TExprNode::TPtr(sharedProgram));
            test.MarkStreaming(consumer);
            stages.push_back(producer);
            stages.push_back(consumer);
        }
        const auto tx = test.Ctx.NewCallable(sharedProgram->Pos(), TKqpPhysicalTx::CallableName(), {
            test.List(std::move(stages)), test.List({}), test.List({}), test.List({})});
        THashSet<std::pair<ui64, ui64>> streamingResults;
        TExprNode::TPtr output = tx;
        UNIT_ASSERT_VALUES_EQUAL_C(test.BuildStreamingFlow(tx, output, streamingResults, tables), IGraphTransformer::TStatus::Repeat,
            test.Ctx.IssueManager.GetIssues().ToString());
        ui32 assigned = 0;
        VisitExpr(output, [&](const TExprNode::TPtr& node) {
            if (const auto aggregation = TMaybeNode<TKqpStreamingAggregation>(node)) {
                const auto setting = GetSetting(aggregation.Cast().Settings().Ref(), "output_state_table");
                UNIT_ASSERT(setting);
                UNIT_ASSERT_VALUES_EQUAL(setting->Tail().Head().Content(), expectedTables.at(&node->Head()));
                ++assigned;
            }
            return true;
        });
        UNIT_ASSERT_VALUES_EQUAL(assigned, 2);
    }

    Y_UNIT_TEST_QUAD(StreamingAggregationOutputStateSharedAggregationProgram, Renamed, Captured) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), test.Traits("state")})});
        const auto* rowType = test.CheckType(aggregation);
        const auto pos = aggregation->Pos();
        test.MarkStreaming(aggregation);
        aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
        test.MarkStreaming(aggregation->HeadPtr());
        auto rows = aggregation;
        if (Renamed) {
            auto row = test.Ctx.NewArgument(pos, "row");
            row->SetTypeAnn(rowType);
            auto key = test.Ctx.NewCallable(pos, "Member", {row, test.Atom("key")});
            key->SetTypeAnn(rowType->FindItemType("key"));
            auto value = test.Ctx.NewCallable(pos, "Member", {row, test.Atom("value")});
            value->SetTypeAnn(rowType->FindItemType("value"));
            const auto* storedType = test.Ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
                test.Ctx.MakeType<TItemExprType>("key", key->GetTypeAnn()),
                test.Ctx.MakeType<TItemExprType>("stored", value->GetTypeAnn())});
            auto body = test.Ctx.NewCallable(pos, "AsStruct", {
                test.List({test.Atom("key"), key}), test.List({test.Atom("stored"), value})});
            body->SetTypeAnn(storedType);
            auto lambda = test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {row}), std::move(body));
            rows = test.Ctx.NewCallable(pos, "Map", {aggregation, lambda});
            rows->SetTypeAnn(test.Ctx.MakeType<TFlowExprType>(storedType));
            test.CopyConstraints(rows, *aggregation);
        }
        if (Captured) {
            const auto* itemType = test.Ctx.MakeType<TDataExprType>(EDataSlot::Int32);
            auto item = test.Ctx.NewCallable(pos, "Int32", {test.Atom("1")});
            item->SetTypeAnn(itemType);
            auto finiteInput = test.Ctx.NewCallable(pos, "AsList", {item});
            finiteInput->SetTypeAnn(test.Ctx.MakeType<TListExprType>(itemType));
            auto argument = test.Ctx.NewArgument(pos, "item");
            argument->SetTypeAnn(itemType);
            auto lambda = test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {argument}), TExprNode::TPtr(rows));
            test.MarkStreaming(lambda);
            auto captured = test.Ctx.NewCallable(pos, "FlatMap", {finiteInput, lambda});
            captured->SetTypeAnn(rows->GetTypeAnn());
            test.CopyConstraints(captured, *rows);
            rows = captured;
        }
        const auto program = test.Ctx.NewLambda(pos,
            test.Ctx.NewArguments(pos, {aggregation->HeadPtr()}), TExprNode::TPtr(rows));
        test.MarkStreaming(program);

        TKikimrTablesData tables;
        TExprNodeList sources;
        TExprNodeList stages;
        for (const TStringBuf path : {"/Root/first", "/Root/second"}) {
            auto source = test.Ctx.NewArgument(pos, "source");
            source->SetTypeAnn(aggregation->Head().GetTypeAnn());
            test.MarkStreaming(source);
            sources.push_back(source);
            auto stage = test.TableSinkStage(path, rows, tables);
            stage = test.Ctx.ChangeChild(*stage, TDqPhyStage::idx_Inputs, test.List({source}));
            stage = test.Ctx.ChangeChild(*stage, TDqPhyStage::idx_Program, TExprNode::TPtr(program));
            test.MarkStreaming(stage);
            stages.push_back(stage);
        }
        const auto tx = test.Ctx.NewCallable(pos, TKqpPhysicalTx::CallableName(), {
            test.List(std::move(stages)), test.List({}), test.List({}), test.List({})});
        THashSet<std::pair<ui64, ui64>> streamingResults;
        TExprNode::TPtr output = tx;
        UNIT_ASSERT_VALUES_EQUAL_C(test.BuildStreamingFlow(tx, output, streamingResults, tables), IGraphTransformer::TStatus::Repeat,
            test.Ctx.IssueManager.GetIssues().ToString());
        const auto rewrittenStages = TKqpPhysicalTx(output).Stages();
        UNIT_ASSERT(rewrittenStages.Item(0).Program().Raw() != rewrittenStages.Item(1).Program().Raw());
        UNIT_ASSERT(rewrittenStages.Item(0).Program().Args().Arg(0).Raw() != rewrittenStages.Item(1).Program().Args().Arg(0).Raw());
        for (const auto& stage : rewrittenStages) {
            const auto rewritten = FindNode(stage.Program().Ptr(), [](const TExprNode::TPtr& node) {
                return TKqpStreamingAggregation::Match(node.Get());
            });
            UNIT_ASSERT(rewritten);
            const auto setting = GetSetting(TKqpStreamingAggregation(rewritten).Settings().Ref(), "output_state_table");
            const auto sink = stage.Outputs().Cast().Item(0).Cast<TDqSink>().Settings().Cast<TKqpTableSinkSettings>();
            UNIT_ASSERT(setting);
            UNIT_ASSERT_VALUES_EQUAL(setting->Tail().Head().Content(), sink.Table().Path().Value());
            UNIT_ASSERT_VALUES_EQUAL(setting->Tail().Tail().Child(1)->Tail().Content(), Renamed ? "stored" : "value");
            UNIT_ASSERT(&rewritten->Head() == stage.Program().Args().Arg(0).Raw());
        }
        // Rewriting one binding must neither mutate the shared program nor reuse its arguments.
        UNIT_ASSERT(!GetSetting(TKqpStreamingAggregation(aggregation).Settings().Ref(), "output_state_table"));
        CheckArguments(*test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, std::move(sources)), std::move(output)));
    }

    Y_UNIT_TEST(StreamingAggregationRejectsConflictingStateSettings) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        test.Config->DisableCheckpoints = true;
        test.Config->FeatureFlags.SetEnableStreamingAggregationAdvanced(true);
        auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")}, {}, {
            test.List({test.Atom("state_table_path"), test.Atom("/Root/state")}),
            test.List({test.Atom("output_state_table"), test.List({test.Atom("/Root/result"),
                test.List({test.List({test.Atom("key"), test.Atom("key")})})})})});
        test.CheckType(aggregation);
        // Enable table tying when validating conflicting metadata in this pre-annotated plan.
        test.Config->DisableCheckpoints = false;
        aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
        test.MarkStreaming(aggregation);
        test.MarkStreaming(aggregation->HeadPtr());
        TKikimrTablesData tables;
        const auto tx = test.Ctx.NewCallable(aggregation->Pos(), TKqpPhysicalTx::CallableName(), {
            test.List({test.TableSinkStage("/Root/result", aggregation, tables)}),
            test.List({}), test.List({}), test.List({})});
        THashSet<std::pair<ui64, ui64>> streamingResults;
        TExprNode::TPtr output;
        UNIT_ASSERT_VALUES_EQUAL(test.BuildStreamingFlow(tx, output, streamingResults, tables), IGraphTransformer::TStatus::Error);
        UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(), "Explicit and output state tables cannot be used together");
    }

    Y_UNIT_TEST_TWIN(StreamingAggregationOutputStateCandidateSelection, ReverseSinks) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), test.Traits("state")})});
        const auto* rowType = test.CheckType(aggregation);
        aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
        test.MarkStreaming(aggregation);
        test.MarkStreaming(aggregation->HeadPtr());
        const auto pos = aggregation->Pos();
        auto arg = test.Ctx.NewArgument(pos, "row");
        arg->SetTypeAnn(rowType);
        TExprNodeList members;
        TVector<const TItemExprType*> types;
        for (const auto& [name, source] : std::vector<std::pair<TStringBuf, TStringBuf>>{
                {"a_alias", "key"}, {"z_pk", "key"}, {"a_value", "value"}, {"z_value", "value"}}) {
            const auto* type = rowType->FindItemType(source);
            auto member = test.Ctx.NewCallable(pos, "Member", {arg, test.Atom(source)});
            member->SetTypeAnn(type);
            members.push_back(test.List({test.Atom(name), member}));
            types.push_back(test.Ctx.MakeType<TItemExprType>(name, type));
        }
        const auto* projectedType = test.Ctx.MakeType<TStructExprType>(types);
        auto body = test.Ctx.NewCallable(pos, "AsStruct", std::move(members));
        body->SetTypeAnn(projectedType);
        auto rows = test.Ctx.NewCallable(pos, "Map", {aggregation,
            test.Ctx.NewLambda(pos, test.Ctx.NewArguments(pos, {arg}), std::move(body))});
        rows->SetTypeAnn(test.Ctx.MakeType<TFlowExprType>(projectedType));
        rows->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"z_pk"}));
        test.MarkStreaming(rows);
        TKikimrTablesData tables;
        auto stage = test.TableSinkStage("/Root/z_result", rows, tables);
        const auto second = test.TableSinkStage("/Root/a_result", rows, tables);
        TExprNodeList sinks = {TDqPhyStage(stage).Outputs().Cast().Item(0).Ptr(), TDqPhyStage(second).Outputs().Cast().Item(0).Ptr()};
        if (ReverseSinks) {
            std::swap(sinks.front(), sinks.back());
        }
        stage = test.Ctx.ChangeChild(*stage, TDqPhyStage::idx_Outputs, test.List(std::move(sinks)));
        test.MarkStreaming(stage);
        for (const TStringBuf path : {"/Root/z_result", "/Root/a_result"}) {
            tables.GetOrAddTable("db", "/Root", TString(path)).Metadata->KeyColumnNames = {"z_pk"};
        }
        const auto tx = test.Ctx.NewCallable(pos, TKqpPhysicalTx::CallableName(), {
            test.List({stage}), test.List({}), test.List({}), test.List({})});
        THashSet<std::pair<ui64, ui64>> streamingResults;
        TExprNode::TPtr output;
        UNIT_ASSERT_VALUES_EQUAL_C(test.BuildStreamingFlow(tx, output, streamingResults, tables), IGraphTransformer::TStatus::Repeat,
            test.Ctx.IssueManager.GetIssues().ToString());
        const auto rewritten = FindNode(output, [](const TExprNode::TPtr& node) { return TKqpStreamingAggregation::Match(node.Get()); });
        UNIT_ASSERT(rewritten);
        const auto setting = GetSetting(TKqpStreamingAggregation(rewritten).Settings().Ref(), "output_state_table");
        UNIT_ASSERT(setting);
        UNIT_ASSERT_VALUES_EQUAL(setting->Tail().Head().Content(), "/Root/a_result");
        THashMap<TStringBuf, TStringBuf> mapping;
        for (const auto& pair : setting->Tail().Tail().Children()) {
            mapping.emplace(pair->Head().Content(), pair->Tail().Content());
        }
        UNIT_ASSERT_VALUES_EQUAL(mapping.at("key"), "z_pk");
        UNIT_ASSERT_VALUES_EQUAL(mapping.at("value"), "a_value");
    }

    Y_UNIT_TEST_QUAD(StreamLookupConstraints, Labeled, Streaming) {
        using namespace NNodes;
        for (const TStringBuf multiMatches : {"", "false", "true"}) {
            TStreamingAggregationTypeAnnTest test;
            const auto rows = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")}, {})->HeadPtr();
            rows->SetState(TExprNode::EState::ConstrComplete);
            TKikimrTablesData tables;
            const auto stage = test.TableSinkStage("/Root/result", rows, tables);
            const auto output = test.Ctx.NewCallable(rows->Pos(), TDqOutput::CallableName(), {stage, test.Atom("0")});
            output->SetState(TExprNode::EState::TypeComplete);
            output->AddConstraint(test.Ctx.MakeConstraint<TUniqueConstraintNode>(std::vector<std::string_view>{"key"}));
            output->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
            output->AddConstraint(test.Ctx.MakeConstraint<TEmptyConstraintNode>());
            if (Streaming) {
                test.MarkStreaming(output);
            }
            TExprNodeList children = {output, test.Atom(Labeled ? "left" : ""), rows, test.Atom("right"),
                test.Atom("Left"), test.List({}), test.List({}), test.List({}),
                test.Atom("1"), test.Atom("100"), test.Atom("100")};
            if (!multiMatches.empty()) {
                children.push_back(test.Atom("false"));
                children.push_back(test.Atom(multiMatches));
            }
            const auto connection = test.Ctx.NewCallable(rows->Pos(), TDqCnStreamLookup::CallableName(), std::move(children));
            connection->SetState(TExprNode::EState::TypeComplete);
            UNIT_ASSERT_VALUES_EQUAL(NDq::ConstraintDqConnection(connection, test.Ctx), IGraphTransformer::TStatus::Ok);
            UNIT_ASSERT_VALUES_EQUAL(!!connection->GetConstraint<TStreamingConstraintNode>(), Streaming);
            UNIT_ASSERT(connection->GetConstraint<TEmptyConstraintNode>());
            UNIT_ASSERT_VALUES_EQUAL(!!connection->GetConstraint<TUniqueConstraintNode>(), multiMatches != "true");
            UNIT_ASSERT_VALUES_EQUAL(!!connection->GetConstraint<TDistinctConstraintNode>(), multiMatches != "true");
            if (multiMatches != "true") {
                const std::vector<std::string_view> keys = {Labeled ? "left.key" : "key"};
                UNIT_ASSERT(connection->GetConstraint<TDistinctConstraintNode>()->ContainsCompleteSet(keys));
                UNIT_ASSERT(connection->GetConstraint<TUniqueConstraintNode>()->ContainsCompleteSet(keys));
            }
        }
    }

    Y_UNIT_TEST_TWIN(StreamingAggregationOutputStateRejectsStaleMapping, WrongPath) {
        using namespace NNodes;
        TStreamingAggregationTypeAnnTest test;
        auto aggregation = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")},
            {test.List({test.Atom("value"), test.Traits("state")})}, {
                test.List({test.Atom("output_state_table"), test.List({test.Atom(WrongPath ? "/Root/other" : "/Root/result"), test.List({
                    test.List({test.Atom("key"), test.Atom("key")}),
                    test.List({test.Atom("value"), test.Atom(WrongPath ? "value" : "other")})})})})});
        test.CheckType(aggregation);
        aggregation->AddConstraint(test.Ctx.MakeConstraint<TDistinctConstraintNode>(std::vector<std::string_view>{"key"}));
        test.MarkStreaming(aggregation);
        test.MarkStreaming(aggregation->HeadPtr());
        TKikimrTablesData tables;
        const auto tx = test.Ctx.NewCallable(aggregation->Pos(), TKqpPhysicalTx::CallableName(), {
            test.List({test.TableSinkStage("/Root/result", aggregation, tables)}), test.List({}), test.List({}), test.List({})});
        THashSet<std::pair<ui64, ui64>> streamingResults;
        TExprNode::TPtr output = tx;
        UNIT_ASSERT_VALUES_EQUAL(test.BuildStreamingFlow(tx, output, streamingResults, tables), IGraphTransformer::TStatus::Error);
        UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(),
            "Assigned streaming aggregation output state table is no longer eligible");
    }

    Y_UNIT_TEST_QUAD(StreamingAggregationConstraints, Streaming, Empty) {
        for (const TStringBuf mode : {"keys", "projected_key", "keyless"}) {
            TStreamingAggregationTypeAnnTest test;
            test.Config->_KqpYqlConstraintsTransformerEnabled = true;
            auto node = test.Aggregation(ETypeAnnotationKind::Flow,
                mode == "keyless" ? TExprNodeList{} : TExprNodeList{test.Atom("key")}, {},
                mode == "projected_key" ? TExprNodeList{test.List({test.Atom("output_columns"), test.List({})})} : TExprNodeList{});
            test.CheckType(node);
            if (Streaming) {
                node->HeadPtr()->AddConstraint(test.Ctx.MakeConstraint<TStreamingConstraintNode>());
            }
            if (Empty) {
                node->HeadPtr()->AddConstraint(test.Ctx.MakeConstraint<TEmptyConstraintNode>());
            }
            node->HeadPtr()->SetState(TExprNode::EState::ConstrComplete);
            // Aggregation must generate uniqueness without inheriting it from the input.
            UNIT_ASSERT(!node->Head().GetConstraint<TDistinctConstraintNode>());
            UNIT_ASSERT(!node->Head().GetConstraint<TUniqueConstraintNode>());
            const auto registry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
            const auto session = MakeIntrusive<TKikimrSessionContext>(registry.Get(), test.Config,
                CreateDefaultTimeProvider(), CreateDeterministicRandomProvider(1), nullptr);
            auto constraints = CreateKiSinkConstraintsTransformer(session);
            TExprNode::TPtr output;
            UNIT_ASSERT_VALUES_EQUAL(constraints->Transform(node, output, test.Ctx), IGraphTransformer::TStatus::Ok);
            node->SetState(TExprNode::EState::ConstrComplete);
            UNIT_ASSERT_VALUES_EQUAL(!!node->GetConstraint<TStreamingConstraintNode>(), Streaming);
            UNIT_ASSERT_VALUES_EQUAL(!!node->GetConstraint<TEmptyConstraintNode>(), Empty && mode != "keyless");
            UNIT_ASSERT_VALUES_EQUAL(!!node->GetConstraint<TUniqueConstraintNode>(), mode == "keys");
            UNIT_ASSERT_VALUES_EQUAL(!!node->GetConstraint<TDistinctConstraintNode>(), mode == "keys");
            if (mode == "keys") {
                UNIT_ASSERT(node->GetConstraint<TDistinctConstraintNode>()->ContainsCompleteSet(std::vector<std::string_view>{"key"}));
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
        UNIT_ASSERT_STRING_CONTAINS(test.Ctx.IssueManager.GetIssues().ToString(),
            Duplicate ? "Duplicated member: key" : "Member not found: missing");
    }

    Y_UNIT_TEST_QUAD(StreamingAggregationParentIndicesAndHandlerArgumentCounts, InitWithParent, UpdateWithParent) {
        using namespace NKikimr::NMiniKQL;
        TStreamingAggregationTypeAnnTest test;
        const TString program = TStringBuilder() << R"((
            (return (AggregationTraits
                (StructType '('key (DataType 'String)))
                (lambda '(item)" << (InitWithParent ? " parent" : "") << ") '("
                << (InitWithParent ? "parent" : "(Uint32 '99)") << R"( (Uint32 '99) (Uint32 '0)))
                (lambda '(item state)" << (UpdateWithParent ? " parent" : "") << ") '((Nth state '0) "
                << (UpdateWithParent ? "parent" : "(Uint32 '99)") << R"( (Add (Nth state '2) (Uint32 '1))))
                (lambda '(state) state)
                (lambda '(state) state)
                (lambda '(left right) left)
                (lambda '(state) state)
                (Null)))
        ))";
        const auto traits = ParseAndAnnotate(program, test.Ctx, false, false, test.Types);
        UNIT_ASSERT_C(traits, test.Ctx.IssueManager.GetIssues().ToString());
        // Reverse alphabetical order so handler indices cannot be confused with result member indices.
        auto node = test.Aggregation(ETypeAnnotationKind::Flow, {test.Atom("key")}, {
            test.List({test.Atom("z_first"), traits}), test.List({test.Atom("a_second"), traits})});
        auto input = ParseAndAnnotate(R"((
            (return (ToFlow (AsList
                (AsStruct '('key (String 'a)))
                (AsStruct '('key (String 'a)))
                (AsStruct '('key (String 'b)))
                (AsStruct '('key (String 'a)))
                (AsStruct '('key (String 'b))))))
        ))", test.Ctx, false, false, test.Types);
        UNIT_ASSERT_C(input, test.Ctx.IssueManager.GetIssues().ToString());
        node = test.Ctx.ChangeChild(*node, NNodes::TKqpStreamingAggregation::idx_Input, std::move(input));
        const auto* resultType = test.CheckType(node);

        test.Run(*node, [&](const auto& value) {
            const auto rows = value.GetListIterator();
            const TVector<TStringBuf> expectedKeys = {"a", "a", "b", "a", "b"};
            const TVector<ui32> expectedUpdates = {0, 1, 0, 2, 1};
            NUdf::TUnboxedValue row;
            for (ui32 i = 0; i < expectedKeys.size(); ++i) {
                UNIT_ASSERT(rows.Next(row));
                const auto key = row.GetElement(*resultType->FindItem("key"));
                UNIT_ASSERT_VALUES_EQUAL(TString(key.AsStringRef()), expectedKeys[i]);
                ui32 parent = 0;
                for (const TStringBuf name : {"z_first", "a_second"}) {
                    const auto state = row.GetElement(*resultType->FindItem(name));
                    UNIT_ASSERT_VALUES_EQUAL(state.GetElement(0).Get<ui32>(), InitWithParent ? parent : 99);
                    UNIT_ASSERT_VALUES_EQUAL(state.GetElement(1).Get<ui32>(), UpdateWithParent && expectedUpdates[i] ? parent : 99);
                    UNIT_ASSERT_VALUES_EQUAL(state.GetElement(2).Get<ui32>(), expectedUpdates[i]);
                    ++parent;
                }
            }
            UNIT_ASSERT(!rows.Next(row));
        });
    }

    Y_UNIT_TEST_TWIN(StreamingAggregationProjectedStateMustBePersistable, UseStateTable) {
        const TStringBuf stateTablePath = UseStateTable ? "/Root/state" : "";
        TStreamingAggregationTypeAnnTest test;
        test.Config->DisableCheckpoints = UseStateTable;
        test.Config->FeatureFlags.SetEnableStreamingAggregationAdvanced(UseStateTable);
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

    Y_UNIT_TEST(TestLocalTopicDataSourceWithoutLocation) {
        const auto source = TExternalDataSource::CreateForLocalTopic("cluster", "database", "token");
        UNIT_ASSERT(source.IsMessageStream());
        UNIT_ASSERT_VALUES_EQUAL(source.GetDataSourcePath(), "cluster");
        const auto properties = source.BuildConnectorProperties();
        UNIT_ASSERT_VALUES_EQUAL(properties.at("database_name"), "database");
        UNIT_ASSERT_VALUES_EQUAL(properties.at("transient_token"), "token");
    }
}

} // namespace NYql
