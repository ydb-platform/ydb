#include "yql_opt_proposed_by_data.h"

#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/ast/yql_ast_annotation.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/ut_common/yql_ut_common.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/core/services/yql_eval_expr.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yson/writer.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TDiscoverYqlExpr) {

    static TString Discover(const TString& ast) {
        TAstParseResult astRes = ParseAst(ast);
        UNIT_ASSERT(astRes.IsOk());
        TExprContext exprCtx;
        TExprNode::TPtr exprRoot;
        UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));

        auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        TTestTablesMapping testTables;
        auto yqlNativeServices = NFile::TYtFileServices::Make(functionRegistry.Get(), testTables);
        auto ytGateway = CreateYtFileGateway(yqlNativeServices);
        auto typeAnnotationContext = MakeIntrusive<TTypeAnnotationContext>();
        typeAnnotationContext->DiscoveryMode = true;
        auto ytState = MakeIntrusive<TYtState>();
        ytState->Gateway = ytGateway;
        ytState->Types = typeAnnotationContext.Get();

        InitializeYtGateway(ytGateway, ytState);
        auto randomProvider = CreateDeterministicRandomProvider(1);
        typeAnnotationContext->AddDataSink(YtProviderName, CreateYtDataSink(ytState));
        auto datasource = CreateYtDataSource(ytState);
        typeAnnotationContext->AddDataSource(YtProviderName, datasource);
        auto intentTransformer = CreateIntentDeterminationTransformer(*typeAnnotationContext);
        TVector<TTransformStage> transformers;
        const auto issueCode = TIssuesIds::DEFAULT_ERROR;
        transformers.push_back(TTransformStage(CreateFunctorTransformer(
            [=](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                return EvaluateExpression(input, output, *typeAnnotationContext, ctx, *functionRegistry, nullptr);
            }),
            "EvaluateExpression",
            issueCode));
        transformers.push_back(TTransformStage(
            CreateIODiscoveryTransformer(*typeAnnotationContext),
            "IODiscovery",
            issueCode));
        transformers.push_back(TTransformStage(
            CreateEpochsTransformer(*typeAnnotationContext),
            "Epochs",
            issueCode));
        transformers.push_back(TTransformStage(
            intentTransformer,
            "IntentDetermination",
            issueCode));
        auto fullTransformer = CreateCompositeGraphTransformer(transformers, true);

        TStringStream str;
        if (SyncTransform(*fullTransformer, exprRoot, exprCtx) == IGraphTransformer::TStatus::Ok) {
            NYson::TYsonWriter writer(&str, NYson::EYsonFormat::Text);
            datasource->CollectDiscoveredData(writer);
        } else {
            exprCtx.IssueManager.GetIssues().PrintTo(str);
        }
        return str.Str();
    }

    Y_UNIT_TEST(DiscoverYt) {
        auto s = R"((
(let mr_source (DataSource 'yt 'plato))
(let x (Read! world mr_source
    (Key '('table (String 'Input)))
    '('key 'subkey 'value) '()))
(let world (Left! x))
(let table (Right! x))

(let mr_sink (DataSink 'yt 'plato))
(let world (Write! world mr_sink
    (Key '('table (String 'Output)))
    table '('('mode 'append))))

(let world (Commit! world mr_sink))
(return world)
))";

        auto res = Discover(s);
        UNIT_ASSERT_VALUES_EQUAL(res, "[[\"plato\";\"Input\";[\"read\"]];[\"plato\";\"Output\";[\"modify\"]]]");
    }

    Y_UNIT_TEST(ErrorOnRange) {
        auto s = R"((
(let mr_source (DataSource 'yt 'plato))
(let range (MrTableRange '"" (lambda '($i) (And (>= $i (String '"Input1")) (<= $i (String '"Input2")))) '""))
(let x (Read! world mr_source
    (Key '('table range))
    '('key 'subkey 'value) '()))
(let world (Left! x))
(let table (Right! x))

(let mr_sink (DataSink 'yt 'plato))
(let world (Write! world mr_sink
    (Key '('table (String 'Output)))
    table '('('mode 'append))))

(let world (Commit! world mr_sink))
(return world)
))";

        auto res = Discover(s);
        UNIT_ASSERT_VALUES_EQUAL(res, R"(<main>: Error: Default error
    <main>:5:6: Error: MrTableRange/MrTableRangeStrict is not allowed in Discovery mode, code: 4600
)");
    }

    Y_UNIT_TEST(ErrorOnTime) {
        auto s = R"((
(let mr_source (DataSource 'yt 'plato))
(let x (Read! world mr_source
    (Key '('table (String (EvaluateAtom (SafeCast (CurrentUtcDate) (DataType 'String))))))
    '('key 'subkey 'value) '()))
(let world (Left! x))
(let table (Right! x))

(let mr_sink (DataSink 'yt 'plato))
(let world (Write! world mr_sink
    (Key '('table (String 'Output)))
    table '('('mode 'append))))

(let world (Commit! world mr_sink))
(return world)
))";

        auto res = Discover(s);
        UNIT_ASSERT_VALUES_EQUAL(res, R"(<main>: Error: Default error
    <main>:4:28: Error: At function: EvaluateAtom
        <main>: Error: Type annotation, code: 1030
            <main>:4:42: Error: At function: SafeCast
                <main>:4:52: Error: At function: CurrentUtcDate
                    <main>:4:52: Error: CurrentUtcDate is not allowed in Discovery mode, code: 4600
)");
    }
}

} // namespace NYql
