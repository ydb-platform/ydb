#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

#include <ydb/library/yql/providers/dq/provider/exec/yql_dq_exectransformer.h>

#include <ydb/library/yql/providers/dq/provider/yql_dq_datasink.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_datasource.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_state.h>

#include <yql/essentials/core/services/yql_transform_pipeline.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <yql/essentials/providers/common/dq/yql_dq_integration_impl.h>

#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>

#include <yql/essentials/providers/result/expr_nodes/yql_res_expr_nodes.h>

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/yson/writer.h>

namespace NYql {
namespace {

class THybridTimeoutGateway final
    : public IDqGateway
{
public:
    struct TExecution
    {
        bool Precompute = false;
        ui64 AllocationTimeoutMs = 0;
    };

    TVector<TExecution> Executions;
    bool Fallback = false;
    bool Timeout = false;

    NThreading::TFuture<void> OpenSession(const TString&, const TString&) override
    {
        return NThreading::MakeFuture();
    }

    NThreading::TFuture<TResult> ExecutePlan(
        const TString&,
        NDqs::TPlan&&,
        const TVector<TString>&,
        const THashMap<TString, TString>&,
        const THashMap<TString, TString>& graphParams,
        const TDqSettings::TPtr& settings,
        const TDqProgressWriter&,
        const THashMap<TString, TString>&,
        bool,
        ui64) override
    {
        const bool precompute = graphParams.contains("Precompute");
        Executions.push_back({
            .Precompute = precompute,
            .AllocationTimeoutMs = settings->WorkersAllocationTimeout.Get().GetOrElse(0),
        });

        TResult result;
        result.SetSuccess();
        result.Fallback = Fallback;
        result.Timeout = Timeout;
        result.Data = precompute ? "[\"1\"]" : "[[\"1\"]]";
        result.RowsCount = 1;
        return NThreading::MakeFuture(std::move(result));
    }
};

class TTimeoutObservingProvider final
    : public TDataProviderBase
{
public:
    class TIntegration final
        : public TDqIntegrationBase
    {
    public:
        int TimeoutNotifications = 0;

        void NotifyDqTimeout() override
        {
            ++TimeoutNotifications;
        }
    };

    TIntegration Integration;

    TStringBuf GetName() const override
    {
        return "timeout_observer";
    }

    IDqIntegration* GetDqIntegration() override
    {
        return &Integration;
    }
};

class THybridTimeoutFixture
{
public:
    TExprContext Context;
    const TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> FunctionRegistry =
        NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
    const TIntrusivePtr<TTypeAnnotationContext> Types = MakeIntrusive<TTypeAnnotationContext>();
    const TIntrusivePtr<THybridTimeoutGateway> Gateway = MakeIntrusive<THybridTimeoutGateway>();
    const TDqStatePtr State;
    const THolder<IGraphTransformer> Transformer;

    THybridTimeoutFixture()
        : State(MakeIntrusive<TDqState>(
            Gateway,
            /*gatewaysConfig*/ nullptr,
            FunctionRegistry.Get(),
            NKikimr::NMiniKQL::TComputationNodeFactory{},
            TTaskTransformFactory{},
            CreateDeterministicRandomProvider(1),
            Types.Get(),
            [] (const TOperationProgress&) { },
            TYqlOperationOptions{},
            "hybrid-timeout-test",
            IMetricsRegistryPtr{},
            TFileStoragePtr{},
            /*vanillaJobPath*/ TString{},
            /*vanillaJobMd5*/ TString{},
            /*externalUser*/ false,
            [] { }))
        , Transformer(CreateDqExecTransformer(State))
    {
        Types->RandomProvider = State->RandomProvider;
        Types->AddDataSource("dq", CreateDqDataSource(State, CreateDqExecTransformer));
        Types->AddDataSink("dq", CreateDqDataSink(State));
        State->Settings->WorkersAllocationTimeout = 100;
        State->Settings->_LongWorkersAllocationFailTimeout = 1'000;
        State->Settings->UseWideChannels = false;
        State->Settings->UseBlockReader = false;
        State->Settings->EnableFullResultWrite = false;
    }

    void ExecutePull(
        TStringBuf localFallback = {},
        bool withPrecompute = false,
        bool expectFallback = false)
    {
        const TPositionHandle pos;
        auto value = Context.NewCallable(pos, "Uint64", {Context.NewAtom(pos, "1")});
        TExprNode::TListType inputs;
        TExprNode::TListType args;
        if (withPrecompute) {
            auto precomputeStage = MakeStage({}, {}, value);
            inputs.push_back(Context.NewCallable(pos, "DqPhyPrecompute", {
                Context.NewCallable(pos, "DqCnValue", {MakeOutput(precomputeStage)}),
            }));
            value = Context.NewArgument(pos, "value");
            args.push_back(value);
        }

        auto row = Context.NewCallable(pos, "AsStruct", {
            Context.NewList(pos, {Context.NewAtom(pos, "value"), value}),
        });
        auto stage = MakeStage(std::move(inputs), std::move(args), row);
        auto graph = Context.NewCallable(pos, "DqCnResult", {
            MakeOutput(stage),
            Context.NewList(pos, {}),
        });
        const auto annotation = TTransformationPipeline(Types)
            .AddTypeAnnotationTransformer()
            .AddPostTypeAnnotation(/*forSubGraph*/ true)
            .Build();
        auto status = SyncTransform(*annotation, graph, Context);
        UNIT_ASSERT_VALUES_EQUAL_C(status.Level, IGraphTransformer::TStatus::Ok,
            Context.IssueManager.GetIssues().ToString());

        TExprNode::TListType settings;
        if (localFallback) {
            settings.push_back(Context.NewList(pos, {
                Context.NewAtom(pos, "dq_can_fallback_locally"),
                Context.NewAtom(pos, localFallback),
            }));
        }
        auto pull = NNodes::Build<NNodes::TPull>(Context, pos)
            .Input(graph)
            .BytesLimit().Value(TString()).Build()
            .RowsLimit().Value(0U).Build()
            .FormatDetails().Value(static_cast<ui32>(NYson::EYsonFormat::Binary)).Build()
            .Settings(Context.NewList(pos, std::move(settings)))
            .Format().Value(0U).Build()
            .PublicId().Value(TString()).Build()
            .Discard().Value("true").Build()
            .Origin(graph)
            .Done().Ptr();
        pull->SetTypeAnn(graph->GetTypeAnn());
        pull->SetState(TExprNode::EState::ExecutionRequired);

        status = SyncTransform(*Transformer, pull, Context);
        if (expectFallback) {
            UNIT_ASSERT_VALUES_EQUAL(status.Level, IGraphTransformer::TStatus::Error);
            const auto issuesIt = Context.AssociativeIssues.find(pull.Get());
            UNIT_ASSERT(issuesIt != Context.AssociativeIssues.end());
            bool hasFallbackIssue = false;
            for (const auto& issue : issuesIt->second) {
                hasFallbackIssue |= issue.GetCode() == TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR;
            }
            UNIT_ASSERT_C(hasFallbackIssue, issuesIt->second.ToString());
        } else {
            UNIT_ASSERT_VALUES_EQUAL_C(status.Level, IGraphTransformer::TStatus::Ok,
                Context.IssueManager.GetIssues().ToString());
            UNIT_ASSERT(pull->HasResult());
        }
        UNIT_ASSERT(!Types->DqCaptured);
        UNIT_ASSERT(!State->WholeQueryFallbackConfirmed);
    }

private:
    TExprNode::TPtr MakeStage(
        TExprNode::TListType inputs,
        TExprNode::TListType args,
        const TExprNode::TPtr& value)
    {
        const TPositionHandle pos;
        return Context.NewCallable(pos, "DqStage", {
            Context.NewList(pos, std::move(inputs)),
            Context.NewLambda(pos, Context.NewArguments(pos, std::move(args)), {
                Context.NewCallable(pos, "ToStream", {Context.NewCallable(pos, "AsList", {value})}),
            }),
            Context.NewList(pos, {}),
        });
    }

    TExprNode::TPtr MakeOutput(const TExprNode::TPtr& stage)
    {
        return Context.NewCallable(TPositionHandle{}, NNodes::TDqOutput::CallableName(), {
            stage,
            Context.NewAtom(TPositionHandle{}, "0"),
        });
    }
};

} // namespace

Y_UNIT_TEST_SUITE(HybridAllocationTimeoutExecTest)
{
    Y_UNIT_TEST(LocalPermissionDoesNotLeakIntoPrecomputeOrNextPull)
    {
        THybridTimeoutFixture fixture;
        fixture.ExecutePull("true", /*withPrecompute*/ true);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Gateway->Executions.size(), 2);
        UNIT_ASSERT(fixture.Gateway->Executions[0].Precompute);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Gateway->Executions[0].AllocationTimeoutMs, 0);
        UNIT_ASSERT(!fixture.Gateway->Executions[1].Precompute);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Gateway->Executions[1].AllocationTimeoutMs, 100);

        fixture.ExecutePull();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Gateway->Executions.size(), 3);
        UNIT_ASSERT(!fixture.Gateway->Executions[2].Precompute);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Gateway->Executions[2].AllocationTimeoutMs, 0);
    }

    Y_UNIT_TEST(OnlyTimeoutFallbackNotifiesIntegration)
    {
        struct TTimeoutCase {
            TStringBuf Name;
            TMaybe<ui64> AllocationTimeoutMs;
            bool GatewayTimeout;
            ui64 ExpectedTimeoutMs;
            int ExpectedNotifications;
        };
        const TTimeoutCase cases[] = {
            {"absent pragma, ordinary fallback", {}, false, 0, 0},
            {"absent pragma, timeout fallback", {}, true, 0, 1},
            {"enabled pragma, ordinary fallback", 100, false, 100, 0},
            {"enabled pragma, timeout fallback", 100, true, 100, 1},
        };
        for (const auto& testCase : cases) {
            THybridTimeoutFixture fixture;
            fixture.State->Settings->WorkersAllocationTimeout.Clear();
            if (testCase.AllocationTimeoutMs) {
                fixture.State->Settings->WorkersAllocationTimeout = *testCase.AllocationTimeoutMs;
            }
            const auto provider = MakeIntrusive<TTimeoutObservingProvider>();
            fixture.Types->AddDataSource(provider->GetName(), provider);
            fixture.Gateway->Fallback = true;
            fixture.Gateway->Timeout = testCase.GatewayTimeout;

            UNIT_ASSERT_NO_EXCEPTION_C(
                fixture.ExecutePull("true", /*withPrecompute*/ false, /*expectFallback*/ true), testCase.Name);

            UNIT_ASSERT_VALUES_EQUAL_C(fixture.Gateway->Executions.size(), 1, testCase.Name);
            UNIT_ASSERT_VALUES_EQUAL_C(fixture.Gateway->Executions[0].AllocationTimeoutMs,
                testCase.ExpectedTimeoutMs, testCase.Name);
            UNIT_ASSERT_VALUES_EQUAL_C(provider->Integration.TimeoutNotifications,
                testCase.ExpectedNotifications, testCase.Name);
        }
    }
}

} // namespace NYql
