#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>
#include <ydb/library/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <ydb/library/yql/core/ut_common/yql_ut_common.h>

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/config/yql_setting.h>
#include <ydb/library/yql/providers/common/config/yql_dispatch.h>
#include <ydb/library/yql/providers/config/yql_config_provider.h>
#include <ydb/library/yql/providers/result/provider/yql_result_provider.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_state.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_datasink.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_datasource.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/sql/sql.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/cast.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TYqlEpoch) {

    static TString ParseAndOptimize(const TString& program) {
        google::protobuf::Arena arena;
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &arena;
        settings.ClusterMapping["plato"] = YtProviderName;

        TAstParseResult astRes = NSQLTranslation::SqlToYql(program, settings);
        UNIT_ASSERT(astRes.IsOk());
        TExprContext exprCtx;
        TExprNode::TPtr exprRoot;
        UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));

        auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        TTestTablesMapping testTables;
        auto yqlNativeServices = NFile::TYtFileServices::Make(functionRegistry.Get(), testTables);
        auto ytGateway = CreateYtFileGateway(yqlNativeServices);
        auto typeAnnotationContext = MakeIntrusive<TTypeAnnotationContext>();
        typeAnnotationContext->RandomProvider = CreateDeterministicRandomProvider(1);
        auto ytState = MakeIntrusive<TYtState>();
        ytState->Gateway = ytGateway;
        ytState->Types = typeAnnotationContext.Get();
        ytState->Configuration->Dispatch(NCommon::ALL_CLUSTERS, "_EnableWriteReorder", "true", NCommon::TSettingDispatcher::EStage::CONFIG, NCommon::TSettingDispatcher::GetDefaultErrorCallback());

        InitializeYtGateway(ytGateway, ytState);
        typeAnnotationContext->AddDataSink(YtProviderName, CreateYtDataSink(ytState));
        typeAnnotationContext->AddDataSource(YtProviderName, CreateYtDataSource(ytState));

        TDqStatePtr dqState = new TDqState(nullptr, {}, functionRegistry.Get(), {}, {}, {}, typeAnnotationContext.Get(), {}, {}, {}, nullptr, {}, {}, {}, false, {});
        typeAnnotationContext->AddDataSink(DqProviderName, CreateDqDataSink(dqState));
        typeAnnotationContext->AddDataSource(DqProviderName, CreateDqDataSource(dqState, [](const TDqStatePtr&) { return new TNullTransformer; }));

        typeAnnotationContext->AddDataSource(ConfigProviderName, CreateConfigProvider(*typeAnnotationContext, nullptr, ""));

        auto writerFactory = [] () { return CreateYsonResultWriter(NYson::EYsonFormat::Binary); };
        auto resultConfig = MakeIntrusive<TResultProviderConfig>(*typeAnnotationContext,
            *functionRegistry, IDataProvider::EResultFormat::Yson, ToString((ui32)NYson::EYsonFormat::Binary), writerFactory);
        resultConfig->SupportsResultPosition = true;
        auto resultProvider = CreateResultProvider(resultConfig);
        typeAnnotationContext->AddDataSink(ResultProviderName, resultProvider);

        auto transformer = TTransformationPipeline(typeAnnotationContext)
            .AddServiceTransformers()
            .AddPreTypeAnnotation()
            .AddPreIOAnnotation()
            .Add(
                CreateFunctorTransformer(
                    [&](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                        TOptimizeExprSettings settings{nullptr};
                        settings.VisitChanges = true;
                        return OptimizeExpr(input, output,
                            [&](const TExprNode::TPtr& node, TExprContext& /*ctx*/) -> TExprNode::TPtr {
                                if (node->IsCallable("PgSelect")) {
                                    TExprNode::TListType sources;
                                    for (auto child: node->Head().Children()) {
                                        if (child->Head().Content() == "set_items") {
                                            for (auto setItem: child->Tail().Children()) {
                                                for (auto subChild: setItem->Children()) {
                                                    if (subChild->Head().Content() == "from") {
                                                        for (auto from: subChild->Tail().Children()) {
                                                            sources.push_back(from->HeadPtr());
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    if (sources.size()) {
                                        return ctx.NewList(node->Pos(), std::move(sources));
                                    }
                                }
                                if (!node->IsWorld() && !node->Content().EndsWith('!') && node->Content() != "YtTable") {
                                    if (node->ChildrenSize() > 0) {
                                        return node->HeadPtr();
                                    }
                                }
                                return node;
                            },
                            ctx, settings);
                    }),
                "Opt"
            )
            .Build();

        exprCtx.Step.Done(TExprStep::ExprEval);
        const auto status = SyncTransform(*transformer, exprRoot, exprCtx);
        if (status == IGraphTransformer::TStatus::Error)
            Cerr << exprCtx.IssueManager.GetIssues().ToString() << Endl;
        UNIT_ASSERT(status == IGraphTransformer::TStatus::Ok);

        auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, true);
        return ast.Root->ToString(TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);
    }

    static struct TTestCase {
        const char* Name;
        const char* SQL;
        const char* Expected;
    } TESTCASES[] = {
        {
"Write1Write2",
R"(
use plato;
insert into @out1 select * from Input;
insert into @out2 select * from Input;
)",
R"((
(let $1 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $2 (Write! world '"yt" $1 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $3 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $4 (Write! world '"yt" $3 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(return (Commit! (Sync! $2 $4) '"yt" '('('"epoch" '"1"))))
)
)"
        },
        {
"ConfWrite1Write2",
R"(
use plato;
pragma yt.Pool="1";
insert into @out1 select * from Input;
insert into @out2 select * from Input;
)",
R"((
(let $1 (YtConfigure! world '"yt" '"Attr" '"pool" '"1"))
(let $2 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $3 (Write! $1 '"yt" $2 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $4 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $5 (Write! $1 '"yt" $4 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(return (Commit! (Sync! $3 $5) '"yt" '('('"epoch" '"1"))))
)
)"
        },
        {
"Write1Write2Write1Write2",
R"(
use plato;
insert into @out1 select * from Input;
insert into @out2 select * from Input;
insert into @out1 select * from Input;
insert into @out2 select * from Input;
)",
R"((
(let $1 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $2 (Write! world '"yt" $1 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $3 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $4 (Write! $2 '"yt" $3 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(let $5 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $6 (Write! world '"yt" $5 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $7 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $8 (Write! $6 '"yt" $7 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(return (Commit! (Sync! $4 $8) '"yt" '('('"epoch" '"1"))))
)
)"
        },
        {
"Write1Write1Write2Write2",
R"(
use plato;
insert into @out1 select * from Input;
insert into @out1 select * from Input;
insert into @out2 select * from Input;
insert into @out2 select * from Input;
)",
R"((
(let $1 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $2 (Write! world '"yt" $1 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $3 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $4 (Write! $2 '"yt" $3 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(let $5 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $6 (Write! world '"yt" $5 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $7 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $8 (Write! $6 '"yt" $7 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(return (Commit! (Sync! $4 $8) '"yt" '('('"epoch" '"1"))))
)
)"
        },
        {
"Write1Write1ConfWrite2Write2",
R"(
use plato;
insert into @out1 select * from Input;
insert into @out1 select * from Input;
pragma yt.Pool="1";
insert into @out2 select * from Input;
insert into @out2 select * from Input;
)",
R"((
(let $1 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $2 (Write! world '"yt" $1 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $3 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $4 (Write! $2 '"yt" $3 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(let $5 (YtConfigure! world '"yt" '"Attr" '"pool" '"1"))
(let $6 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $7 (Write! $5 '"yt" $6 (Right! (Read! $5 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $8 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $9 (Write! $7 '"yt" $8 (Right! (Read! $5 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(return (Commit! (Sync! $4 $9) '"yt" '('('"epoch" '"1"))))
)
)"
        },
        {
"ConfWrite1Write2Write1Write2",
R"(
use plato;
pragma yt.Pool="1";
insert into @out1 select * from Input;
insert into @out2 select * from Input;
insert into @out1 select * from Input;
insert into @out2 select * from Input;
)",
R"((
(let $1 (YtConfigure! world '"yt" '"Attr" '"pool" '"1"))
(let $2 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $3 (Write! $1 '"yt" $2 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $4 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $5 (Write! $3 '"yt" $4 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(let $6 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $7 (Write! $1 '"yt" $6 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $8 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $9 (Write! $7 '"yt" $8 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(return (Commit! (Sync! $5 $9) '"yt" '('('"epoch" '"1"))))
)
)"
        },
        {
"Write1ConfWrite2Write1Write2",
R"(
use plato;
insert into @out1 select * from Input;
pragma yt.Pool="1";
insert into @out2 select * from Input;
insert into @out1 select * from Input;
insert into @out2 select * from Input;
)",
R"((
(let $1 (YtConfigure! world '"yt" '"Attr" '"pool" '"1"))
(let $2 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $3 (Write! world '"yt" $2 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $4 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $5 (Write! (Sync! $1 $3) '"yt" $4 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(let $6 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $7 (Write! $1 '"yt" $6 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $8 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $9 (Write! $7 '"yt" $8 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(return (Commit! (Sync! $5 $9) '"yt" '('('"epoch" '"1"))))
)
)"
        },
        {
"ConfWrite1ConfWrite2Write1Write2",
R"(
use plato;
pragma yt.Pool="1";
insert into @out1 select * from Input;
pragma yt.Pool="2";
insert into @out2 select * from Input;
insert into @out1 select * from Input;
insert into @out2 select * from Input;
)",
R"((
(let $1 (YtConfigure! world '"yt" '"Attr" '"pool" '"1"))
(let $2 (YtConfigure! $1 '"yt" '"Attr" '"pool" '"2"))
(let $3 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $4 (Write! $1 '"yt" $3 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $5 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $6 (Write! (Sync! $2 $4) '"yt" $5 (Right! (Read! $2 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(let $7 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $8 (Write! $2 '"yt" $7 (Right! (Read! $2 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $9 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $10 (Write! $8 '"yt" $9 (Right! (Read! $2 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(return (Commit! (Sync! $6 $10) '"yt" '('('"epoch" '"1"))))
)
)"
        },
        {
"Write1ConfWrite1Write2ConfWrite2",
R"(
use plato;
insert into @out1 select * from Input;
pragma yt.Pool="1";
insert into @out1 select * from Input;
insert into @out2 select * from Input;
pragma yt.Pool="2";
insert into @out2 select * from Input;
)",
R"((
(let $1 (YtConfigure! world '"yt" '"Attr" '"pool" '"1"))
(let $2 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $3 (Write! world '"yt" $2 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $4 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $5 (Write! (Sync! $1 $3) '"yt" $4 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(let $6 (YtConfigure! $1 '"yt" '"Attr" '"pool" '"2"))
(let $7 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $8 (Write! $1 '"yt" $7 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $9 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $10 (Write! (Sync! $6 $8) '"yt" $9 (Right! (Read! $6 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(return (Commit! (Sync! $5 $10) '"yt" '('('"epoch" '"1"))))
)
)"
        },
        {
"Write1Write2Conf",
R"(
use plato;
insert into @out1 select * from Input;
insert into @out2 select * from Input;
pragma yt.Pool="1";
)",
R"((
(let $1 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $2 (Write! world '"yt" $1 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $3 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $4 (Write! world '"yt" $3 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(return (Commit! (Sync! $2 $4) '"yt" '('('"epoch" '"1"))))
)
)"
        },
        {
"WriteResultWrite",
R"(
use plato;
insert into @out1 select * from Input;
select * from Input;
insert into @out1 select * from Input;
)",
R"((
(let $1 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $2 (Write! world '"yt" $1 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $3 (Write! world 'result (Key) (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('type) '('autoref))))
(let $4 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $5 (Write! (Commit! (Sync! $2 $3) 'result) '"yt" $4 (Right! (Read! world '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(return (Commit! $5 '"yt" '('('"epoch" '"1"))))
)
)"
        },
        {
"OtherConfMix",
R"(
use plato;
pragma yt.Pool="1";
pragma dq.AnalyzeQuery="1";
pragma yt.Pool="2";
select * from Input;
pragma dq.AnalyzeQuery="1";
pragma yt.Pool="3";
select * from Input;
)",
R"((
(let $1 (YtConfigure! world '"yt" '"Attr" '"pool" '"1"))
(let $2 (YtConfigure! $1 '"yt" '"Attr" '"pool" '"2"))
(let $3 (YtConfigure! $2 '"yt" '"Attr" '"pool" '"3"))
(let $4 (Configure! world '"dq" '"Attr" '"analyzequery" '"1"))
(let $5 (Write! (Sync! $2 $4) 'result (Key) (Right! (Read! $2 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('type) '('autoref))))
(let $6 (Configure! (Commit! $5 'result) '"dq" '"Attr" '"analyzequery" '"1"))
(let $7 (Write! (Sync! $3 $6) 'result (Key) (Right! (Read! $3 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('type) '('autoref))))
(return (Commit! (Commit! $7 'result) '"yt" '('('"epoch" '"1"))))
)
)"
        },
        {
"ReadAfterCommit",
R"(
use plato;
pragma yt.Pool = "1";
insert into @temp
select * from Input;
commit;
select * from @temp;
)",
R"((
(let $1 (YtConfigure! world '"yt" '"Attr" '"pool" '"1"))
(let $2 (YtTable '"@temp" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $3 (Write! $1 '"yt" $2 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $4 (Commit! $3 '"yt" '('('"epoch" '"1"))))
(let $5 (Write! (Sync! $1 $4) 'result (Key) (Right! (Read! (Sync! $1 $4) '"yt" '((YtTable '"@temp" (Void) (Void) (Void) '('('"anonymous")) '1 (Void) '"plato")) (Void) '())) '('('type) '('autoref))))
(return (Commit! (Commit! $5 'result) '"yt" '('('"epoch" '"2"))))
)
)"
        },
        {
"ConfWrite1CommitConfWrite2CommitResult1CommitConfResult2",
R"(
use plato;
pragma yt.Pool = "1";
insert into @out1
select * from Input;
commit;
pragma yt.Pool = "2";
insert into @out2
select * from @out1;
select * from @out1;
commit;
pragma yt.Pool = "3";
select * from @out2;
)",
R"((
(let $1 (YtConfigure! world '"yt" '"Attr" '"pool" '"1"))
(let $2 (YtConfigure! $1 '"yt" '"Attr" '"pool" '"2"))
(let $3 (YtConfigure! $2 '"yt" '"Attr" '"pool" '"3"))
(let $4 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $5 (Write! $1 '"yt" $4 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $6 (Commit! $5 '"yt" '('('"epoch" '"1"))))
(let $7 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '2 '"plato"))
(let $8 (Write! (Sync! $2 $6) '"yt" $7 (Right! (Read! (Sync! $2 $6) '"yt" '((YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) '1 (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $9 (Write! (Sync! $2 $6) 'result (Key) (Right! (Read! (Sync! $2 $6) '"yt" '((YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) '1 (Void) '"plato")) (Void) '())) '('('type) '('autoref))))
(let $10 (Commit! (Commit! (Sync! $8 $9) 'result) '"yt" '('('"epoch" '"2"))))
(let $11 (Write! (Sync! $3 $10) 'result (Key) (Right! (Read! (Sync! $3 $10) '"yt" '((YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) '2 (Void) '"plato")) (Void) '())) '('('type) '('autoref))))
(return (Commit! (Commit! $11 'result) '"yt" '('('"epoch" '"3"))))
)
)"
        },
        {
"ManyResults",
R"(
use plato;
pragma yt.Pool = "1";
select * from Input;
select * from Input;
select * from Input;
)",
R"((
(let $1 (YtConfigure! world '"yt" '"Attr" '"pool" '"1"))
(let $2 (Write! $1 'result (Key) (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('type) '('autoref))))
(let $3 (Write! (Sync! $1 (Commit! $2 'result)) 'result (Key) (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('type) '('autoref))))
(let $4 (Write! (Sync! $1 (Commit! $3 'result)) 'result (Key) (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('type) '('autoref))))
(return (Commit! (Commit! $4 'result) '"yt" '('('"epoch" '"1"))))
)
)"
        },
        {
"ConfPlusSeveralAppends",
R"(
use plato;
pragma DqEngine="auto";
pragma yt.Pool = "1";
insert into @out
select * from Input;
insert into @out
select * from Input;
insert into @out
select * from Input;
insert into @out
select * from Input;
insert into @out
select * from Input;
)",
R"((
(let $1 (YtConfigure! world '"yt" '"Attr" '"pool" '"1"))
(let $2 (Configure! world '"config" '"DqEngine" '"auto"))
(let $3 (YtTable '"@out" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $4 (Write! (Sync! $1 $2) '"yt" $3 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $5 (YtTable '"@out" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $6 (Write! $4 '"yt" $5 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(let $7 (YtTable '"@out" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $8 (Write! $6 '"yt" $7 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(let $9 (YtTable '"@out" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $10 (Write! $8 '"yt" $9 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(let $11 (YtTable '"@out" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $12 (Write! $10 '"yt" $11 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(return (Commit! $12 '"yt" '('('"epoch" '"1"))))
)
)"
        },
        {
"SecondAppendAfterResWrite",
R"(
use plato;
pragma yt.Pool = "1";
insert into @out1
select * from Input;
select * from Input;
insert into @out2
select * from Input;
insert into @out1
select * from Input;
)",
R"((
(let $1 (YtConfigure! world '"yt" '"Attr" '"pool" '"1"))
(let $2 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $3 (Write! $1 '"yt" $2 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $4 (Write! $1 'result (Key) (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('type) '('autoref))))
(let $5 (Commit! (Sync! $3 $4) 'result))
(let $6 (YtTable '"@out2" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $7 (Write! (Sync! $1 $5) '"yt" $6 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append) '('"initial"))))
(let $8 (YtTable '"@out1" (Void) (Void) (Void) '('('"anonymous")) (Void) '1 '"plato"))
(let $9 (Write! (Sync! $1 $5) '"yt" $8 (Right! (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '())) '('('mode 'append))))
(return (Commit! (Sync! $7 $9) '"yt" '('('"epoch" '"1"))))
)
)"
        },
        {
"PgWithSelfJoin",
R"(
--!syntax_pg
set yt.Pool = "1";

select 1;

with inp as
(
    select key from plato."Input"
)
select i1.key as k1, i2.key as k2
from inp i1, inp i2;

)",
R"((
(let $1 (YtConfigure! world 'yt 'Attr '"pool" '"1"))
(let $2 (Configure! world 'config 'OrderedColumns))
(let $3 (Write! (Sync! $1 $2) 'result (Key) '('('set_items '('('('result '('"column0"))))) '('set_ops '('push))) '('('type) '('autoref))))
(let $4 (Read! $1 '"yt" '((YtTable '"Input" (Void) (Void) (Void) '() (Void) (Void) '"plato")) (Void) '()))
(let $5 (Write! (Sync! $1 (Commit! $3 'result)) 'result (Key) '('((Right! $4)) '((Right! $4))) '('('type) '('autoref))))
(return (Commit! (Commit! $5 'result) '"yt" '('('"epoch" '"1"))))
)
)"
        },
    };

    struct TTestRegistration {
        TTestRegistration() {
            for (size_t i = 0; i < sizeof(TESTCASES) / sizeof(TTestCase); ++i) {
                TCurrentTest::AddTest(TESTCASES[i].Name,
                    [i](NUnitTest::TTestContext&) {
                        auto res = NTestSuiteTYqlEpoch::ParseAndOptimize(TString{TESTCASES[i].SQL});
                        UNIT_ASSERT_STRINGS_EQUAL(res, TESTCASES[i].Expected);
                    },
                    false
                );
            }
        }
    };
    static const NTestSuiteTYqlEpoch::TTestRegistration testRegistration;

}

} // namespace NYql
