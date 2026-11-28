#include "actor_helpers.h"
#include "executer_actor.h"
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_recapture.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_state.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/events.h>
#include "events.h"

#include <yql/essentials/core/yql_expr_optimize.h>

#include <yql/essentials/providers/common/dq/yql_dq_integration_impl.h>

#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>

#include "result_actor_base.h"
#include "result_receiver.h"

#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NYql;
using namespace NYql::NDqs;

namespace {

class TCapturingDqGateway final
    : public IDqGateway
{
public:
    TDqSettings::TPtr ExecutedSettings;

    NThreading::TFuture<void> OpenSession(const TString&, const TString&) override
    {
        return NThreading::MakeFuture();
    }

    NThreading::TFuture<TResult> ExecutePlan(
        const TString&,
        TPlan&&,
        const TVector<TString>&,
        const THashMap<TString, TString>&,
        const THashMap<TString, TString>&,
        const TDqSettings::TPtr& settings,
        const TDqProgressWriter&,
        const THashMap<TString, TString>&,
        bool,
        ui64) override
    {
        ExecutedSettings = settings;

        TResult result;
        result.SetSuccess();
        return NThreading::MakeFuture(std::move(result));
    }
};

class TFallbackTestIntegration final
    : public TDqIntegrationBase
{
public:
    explicit TFallbackTestIntegration(bool canFallback)
        : CanFallback_(canFallback)
    { }

    bool CanFallback() override
    {
        return CanFallback_;
    }

    bool CheckPragmas(const TExprNode&, TExprContext&, bool) override
    {
        return true;
    }

    bool CanRead(const TExprNode&, TExprContext&, bool) override
    {
        return true;
    }

    TExprNode::TPtr WrapRead(const TExprNode::TPtr& read, TExprContext& ctx, const TWrapReadSettings&) override
    {
        return ctx.NewWorld(read->Pos());
    }

private:
    const bool CanFallback_;
};

class TFallbackTestProvider final
    : public TDataProviderBase
{
public:
    explicit TFallbackTestProvider(bool canFallback)
        : Integration_(canFallback)
    { }

    TStringBuf GetName() const override
    {
        return "fallback_test";
    }

    IDqIntegration* GetDqIntegration() override
    {
        return &Integration_;
    }

private:
    TFallbackTestIntegration Integration_;
};

struct TEvaluationTestState
{
    ui32 DuringRecapture = 0;
    ui32 DuringExecution = 0;
};

struct TFallbackProviderConfig
{
    TString Name;
    bool CanFallback = false;
    bool IsSink = false;
};

struct TExecutePlanOptions
{
    bool ForceDq = false;
    EFallbackPolicy QueryFallbackPolicy = EFallbackPolicy::Default;
    TVector<TFallbackProviderConfig> Providers = {{"yt", true}};
    bool AnalyzeQuery = true;
    bool RewindAfterRecapture = false;
    TEvaluationTestState Evaluation;
    bool CanFallbackLocally = false;
};

TDqSettings::TPtr ExecutePlanAndCaptureSettings(
    const TDqSettings::TPtr& settings,
    const TExecutePlanOptions& options = {})
{
    TTypeAnnotationContext typeContext;
    typeContext.PureResultDataSource = "dq";
    typeContext.EvaluationInProgress = options.Evaluation.DuringRecapture;

    auto gateway = MakeIntrusive<TCapturingDqGateway>();
    auto state = MakeIntrusive<TDqState>(
        gateway,
        /*gatewaysConfig*/ nullptr,
        /*functionRegistry*/ nullptr,
        NKikimr::NMiniKQL::TComputationNodeFactory{},
        TTaskTransformFactory{},
        TIntrusivePtr<IRandomProvider>{},
        &typeContext,
        TOperationProgressWriter{},
        TYqlOperationOptions{},
        "session-id",
        IMetricsRegistryPtr{},
        TFileStoragePtr{},
        /*vanillaJobPath*/ TString{},
        /*vanillaJobMd5*/ TString{},
        /*externalUser*/ false,
        [] { });

    static_cast<TDqSettings&>(*state->Settings) = *settings;
    state->Settings->AnalyzeQuery = options.AnalyzeQuery;

    TExprContext ctx;
    const TPositionHandle pos;
    TExprNode::TListType reads;
    reads.push_back(ctx.NewCallable(pos, "Configure!", {
        ctx.NewWorld(pos),
        ctx.NewCallable(pos, "DataSource", {ctx.NewAtom(pos, "dq")}),
        ctx.NewAtom(pos, "WorkersAllocationTimeout"),
        ctx.NewAtom(pos, "100"),
    }));
    for (const auto& provider : options.Providers) {
        const auto integration = MakeIntrusive<TFallbackTestProvider>(provider.CanFallback);
        if (provider.IsSink) {
            typeContext.AddDataSink(provider.Name, integration);
            reads.push_back(ctx.NewCallable(pos, "Commit!", {
                ctx.NewWorld(pos),
                ctx.NewCallable(pos, "DataSink", {ctx.NewAtom(pos, provider.Name)}),
                ctx.NewList(pos, {}),
            }));
        } else {
            typeContext.AddDataSource(provider.Name, integration);
            reads.push_back(ctx.NewCallable(pos, NNodes::TCoRight::CallableName(), {
                ctx.NewCallable(pos, "Read!", {
                    ctx.NewWorld(pos),
                    ctx.NewCallable(pos, "DataSource", {ctx.NewAtom(pos, provider.Name)}),
                }),
            }));
        }
    }
    auto input = ctx.NewList(pos, std::move(reads));
    VisitExpr(input, [&] (const TExprNode::TPtr& node) {
        node->SetTypeAnn(ctx.MakeType<TWorldExprType>());
        return true;
    });

    TExprNode::TPtr output;
    const auto recapture = CreateDqsRecaptureTransformer(state);
    const auto status = recapture->Transform(input, output, ctx);
    UNIT_ASSERT_C(status.Level != IGraphTransformer::TStatus::Error, ctx.IssueManager.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(typeContext.DqCaptured, options.AnalyzeQuery);

    if (options.RewindAfterRecapture) {
        recapture->Rewind();
    }
    typeContext.EvaluationInProgress = options.Evaluation.DuringExecution;
    typeContext.ForceDq = options.ForceDq;
    typeContext.DqFallbackPolicy = options.QueryFallbackPolicy;

    state->ExecutePlan(
        "session-id",
        TPlan{},
        /*columns*/ {},
        /*secureParams*/ {},
        /*graphParams*/ {},
        settings,
        IDqGateway::TDqProgressWriter{},
        /*modulesMapping*/ {},
        /*discard*/ false,
        /*executionTimeout*/ 0,
        options.CanFallbackLocally).GetValueSync();

    return gateway->ExecutedSettings;
}

struct TAllocationErrorResult
{
    ui64 AllocationTimeoutMs = 0;
    NDqProto::TQueryResponse QueryResponse;
};

TAllocationErrorResult ExecuteWithAllocationError(
    const TDqSettings::TPtr& settings,
    NDqProto::StatusIds::StatusCode status,
    NDqProto::EAllocationErrorReason reason,
    const TString& message)
{
    TTestActorRuntimeBase runtime;
    runtime.Initialize();

    const auto gwmId = runtime.AllocateEdgeActor();
    const auto controlId = runtime.AllocateEdgeActor();
    const auto printerId = runtime.AllocateEdgeActor();

    TDqConfiguration::TPtr configuration = new TDqConfiguration;
    static_cast<TDqSettings&>(*configuration) = *settings;
    const auto executerId = runtime.Register(NDq::MakeDqExecuter(
        gwmId,
        printerId,
        "trace-id",
        "user",
        configuration,
        new NMonitoring::TDynamicCounters));

    Yql::DqsProto::ExecuteGraphRequest graphRequest;
    Yql::DqsProto::TTaskMeta taskMeta;
    graphRequest.AddTask()->MutableMeta()->PackFrom(taskMeta);
    runtime.Send(new IEventHandle(
        executerId,
        controlId,
        new TEvGraphRequest(graphRequest, controlId, controlId)));

    const auto allocationRequest = runtime.GrabEdgeEvent<TEvAllocateWorkersRequest>(gwmId);
    const auto allocationTimeoutMs =
        allocationRequest->Get()->Record.GetAllocationTimeoutMs();

    auto allocationResponse = MakeHolder<TEvAllocateWorkersResponse>(message, status);
    allocationResponse->Record.MutableError()->SetAllocationErrorReason(reason);
    runtime.Send(new IEventHandle(
        allocationRequest->Sender,
        gwmId,
        allocationResponse.Release()));

    auto queryResponse = runtime.GrabEdgeEvent<TEvQueryResponse>(controlId);
    return {
        allocationTimeoutMs,
        std::move(queryResponse->Get()->Record),
    };
}

} // namespace

Y_UNIT_TEST_SUITE(ResultReceiver) {

auto ResultReceiver(TActorId executerId = {}) {
    TDqConfiguration::TPtr settings = new TDqConfiguration();
    auto receiver = MakeResultReceiver(
        {}, // columns,
        executerId,
        "traceId",
        settings,
        {}, // secureParams
        "", // resultType ?
        {}, // graphExecutionEventsId ?
        false // discard
    );

    return receiver;
}

Y_UNIT_TEST(ReceiveStatus) {
    TTestActorRuntimeBase runtime;
    runtime.Initialize();

    auto sender = runtime.AllocateEdgeActor();
    auto receiverId = runtime.Register(ResultReceiver().Release());
    runtime.Send(new IEventHandle(receiverId, sender, new TEvReadyState(), 0, true));
}

Y_UNIT_TEST(ReceiveError) {
    TTestActorRuntimeBase runtime;
    runtime.Initialize();

    auto executerId = runtime.AllocateEdgeActor();
    auto receiverId = runtime.Register(ResultReceiver(executerId).Release());
    runtime.Send(new IEventHandle(receiverId, {}, new NActors::TEvents::TEvUndelivered(0,0), 0, true));

    auto response = runtime.GrabEdgeEvent<TEvDqFailure>();
    UNIT_ASSERT_EQUAL(response->Record.GetStatusCode(), NYql::NDqProto::StatusIds::UNAVAILABLE);
}

Y_UNIT_TEST(WriteQueue) {
    NYql::NDqs::NExecutionHelpers::TWriteQueue q;
    UNIT_ASSERT(q.empty());

    NYql::NDqs::NExecutionHelpers::TQueueItem item({}, ""); item.Size = 1000;
    q.emplace(item);
    UNIT_ASSERT_EQUAL(q.ByteSize, 1000);

    item.Size = 11;
    q.emplace(item);
    UNIT_ASSERT_EQUAL(q.ByteSize, 1011);

    q.pop();
    UNIT_ASSERT_EQUAL(q.ByteSize, 11);

    q.pop();
    UNIT_ASSERT_EQUAL(q.ByteSize, 0);
}

} // Y_UNIT_TEST_SUITE(ResultReceiver)

Y_UNIT_TEST_SUITE(Settings) {

Y_UNIT_TEST(TimeoutRequiresOptInAndShorterDeadline) {
    TDqConfiguration configuration;
    UNIT_ASSERT_VALUES_EQUAL(configuration.WorkersAllocationTimeout.Get().GetOrElse(0), 0);

    bool publiclyExposed = false;
    configuration.Enumerate([&] (std::string_view name) {
        publiclyExposed |= name == "WorkersAllocationTimeout";
    });
    UNIT_ASSERT(publiclyExposed);

    UNIT_ASSERT(configuration.Dispatch(
        NCommon::ALL_CLUSTERS,
        "WorkersAllocationTimeout",
        TString("12345"),
        NCommon::ISettingDispatcher::EStage::STATIC,
        NCommon::GetDefaultErrorCallback()));
    UNIT_ASSERT_VALUES_EQUAL(*configuration.WorkersAllocationTimeout.Get(), 12345);

    struct TTimeoutCase {
        TStringBuf Name;
        TMaybe<ui64> TimeoutMs;
        bool CanFallback = false;
        ui64 ExpectedTimeoutMs = 0;
    };
    const TTimeoutCase cases[] = {
        {"absent", {}, true, 0},
        {"explicit zero", 0, true, 0},
        {"below broad limit", 12345, true, 12345},
        {"equal to broad limit", 20000, true, 0},
        {"above broad limit", 20001, true, 0},
        {"fallback forbidden", 12345, false, 0},
    };
    for (const auto& testCase : cases) {
        auto settings = std::make_shared<TDqSettings>();
        settings->_LongWorkersAllocationFailTimeout = 20000;
        if (testCase.TimeoutMs) {
            settings->WorkersAllocationTimeout = *testCase.TimeoutMs;
        }
        const auto effective = TDqSettings::WithEffectiveWorkersAllocationTimeout(settings, testCase.CanFallback);
        UNIT_ASSERT_VALUES_EQUAL_C(effective->WorkersAllocationTimeout.Get().GetOrElse(0),
            testCase.ExpectedTimeoutMs, testCase.Name);
        UNIT_ASSERT_C(settings->WorkersAllocationTimeout.Get() == testCase.TimeoutMs, testCase.Name);
        UNIT_ASSERT_VALUES_EQUAL_C(*settings->_LongWorkersAllocationFailTimeout.Get(), 20000, testCase.Name);
    }
}

Y_UNIT_TEST(OriginalQueryProvidersMustSupportFallback) {
    auto settings = std::make_shared<TDqSettings>();
    settings->WorkersAllocationTimeout = 100;

    struct TProviderCase {
        TStringBuf Name;
        TVector<TFallbackProviderConfig> Providers;
    };
    const TProviderCase cases[] = {
        {"DQ-only sibling last", {{"yt", true}, {"solomon", false}}},
        {"DQ-only sibling first", {{"solomon", false}, {"yt", true}}},
        {"native DQ", {{"yt", true}, {"dq", false}}},
        {"same-name sink forbids fallback", {{"yt", true}, {"yt", false, /*IsSink*/ true}}},
    };
    for (const auto& testCase : cases) {
        TDqSettings::TPtr effective;
        UNIT_ASSERT_NO_EXCEPTION_C(effective = ExecutePlanAndCaptureSettings(
            settings, {.Providers = testCase.Providers}), testCase.Name);
        UNIT_ASSERT_VALUES_EQUAL_C(effective->WorkersAllocationTimeout.Get().GetOrElse(0), 0, testCase.Name);
        UNIT_ASSERT_VALUES_EQUAL_C(*settings->WorkersAllocationTimeout.Get(), 100, testCase.Name);
    }
}

Y_UNIT_TEST(WorkersAllocationTimeoutRequiresConfirmedFallback) {
    auto settings = std::make_shared<TDqSettings>();
    settings->WorkersAllocationTimeout = 100;

    const auto effective = ExecutePlanAndCaptureSettings(settings, {.AnalyzeQuery = false});
    UNIT_ASSERT_VALUES_EQUAL(effective->WorkersAllocationTimeout.Get().GetOrElse(0), 0);
    UNIT_ASSERT_VALUES_EQUAL(*settings->WorkersAllocationTimeout.Get(), 100);
}

Y_UNIT_TEST(FallbackGuardsDisableTimeout) {
    struct TGuardCase {
        TStringBuf Name;
        TExecutePlanOptions Options;
        EFallbackPolicy PlanFallbackPolicy;
        ui64 ExpectedTimeoutMs;
    };
    const TGuardCase cases[] = {
        {"whole-query allowed", {}, EFallbackPolicy::Default, 100},
        {"whole-query ForceDq", {.ForceDq = true}, EFallbackPolicy::Default, 0},
        {"whole-query query Never", {.QueryFallbackPolicy = EFallbackPolicy::Never}, EFallbackPolicy::Default, 0},
        {"whole-query plan Never", {}, EFallbackPolicy::Never, 0},
        {"local allowed", {.AnalyzeQuery = false, .CanFallbackLocally = true}, EFallbackPolicy::Default, 100},
        {"local ForceDq", {.ForceDq = true, .AnalyzeQuery = false, .CanFallbackLocally = true}, EFallbackPolicy::Default, 0},
        {"local query Never", {.QueryFallbackPolicy = EFallbackPolicy::Never, .AnalyzeQuery = false, .CanFallbackLocally = true}, EFallbackPolicy::Default, 0},
        {"local plan Never", {.AnalyzeQuery = false, .CanFallbackLocally = true}, EFallbackPolicy::Never, 0},
    };
    for (const auto& testCase : cases) {
        auto settings = std::make_shared<TDqSettings>();
        settings->WorkersAllocationTimeout = 100;
        settings->_LongWorkersAllocationFailTimeout = 20000;
        settings->FallbackPolicy = testCase.PlanFallbackPolicy;

        TDqSettings::TPtr effective;
        UNIT_ASSERT_NO_EXCEPTION_C(effective = ExecutePlanAndCaptureSettings(settings, testCase.Options), testCase.Name);
        UNIT_ASSERT_VALUES_EQUAL_C(effective->WorkersAllocationTimeout.Get().GetOrElse(0),
            testCase.ExpectedTimeoutMs, testCase.Name);
        UNIT_ASSERT_VALUES_EQUAL_C(*settings->WorkersAllocationTimeout.Get(), 100, testCase.Name);
    }
}

Y_UNIT_TEST(EvaluationDoesNotGrantFallbackPermission) {
    auto settings = std::make_shared<TDqSettings>();
    settings->WorkersAllocationTimeout = 100;

    struct TEvaluationCase {
        TStringBuf Name;
        TEvaluationTestState Evaluation;
        bool LocalFallback = false;
    };
    const TEvaluationCase cases[] = {
        {"evaluated subgraph", {.DuringRecapture = 1, .DuringExecution = 0}, false},
        {"whole-query fallback", {.DuringExecution = 1}, false},
        {"local fallback", {.DuringExecution = 1}, true},
    };
    for (const auto& testCase : cases) {
        TDqSettings::TPtr effective;
        UNIT_ASSERT_NO_EXCEPTION_C(effective = ExecutePlanAndCaptureSettings(
            settings, {
                .AnalyzeQuery = !testCase.LocalFallback,
                .Evaluation = testCase.Evaluation,
                .CanFallbackLocally = testCase.LocalFallback,
            }), testCase.Name);
        UNIT_ASSERT_VALUES_EQUAL_C(effective->WorkersAllocationTimeout.Get().GetOrElse(0), 0, testCase.Name);
        UNIT_ASSERT_VALUES_EQUAL_C(*settings->WorkersAllocationTimeout.Get(), 100, testCase.Name);
    }
}

Y_UNIT_TEST(RewindClearsFallbackPermission) {
    auto settings = std::make_shared<TDqSettings>();
    settings->WorkersAllocationTimeout = 100;

    const auto effective = ExecutePlanAndCaptureSettings(settings, {.RewindAfterRecapture = true});
    UNIT_ASSERT_VALUES_EQUAL(effective->WorkersAllocationTimeout.Get().GetOrElse(0), 0);
    UNIT_ASSERT_VALUES_EQUAL(*settings->WorkersAllocationTimeout.Get(), 100);
}

} // Y_UNIT_TEST_SUITE(Settings)

Y_UNIT_TEST_SUITE(Executer) {

Y_UNIT_TEST(AllocationTimeoutIsForwardedToQueryResponse) {
    auto settings = std::make_shared<TDqSettings>();
    settings->WorkersAllocationTimeout = 1234;
    settings->_LongWorkersAllocationFailTimeout = 2000;
    const auto result = ExecuteWithAllocationError(
        settings,
        NDqProto::StatusIds::LIMIT_EXCEEDED,
        NDqProto::ALLOCATION_TIMEOUT,
        "allocation timeout");

    UNIT_ASSERT_VALUES_EQUAL(result.AllocationTimeoutMs, 1234);
    UNIT_ASSERT(result.QueryResponse.GetStatusCode() == NDqProto::StatusIds::LIMIT_EXCEEDED);
    UNIT_ASSERT(result.QueryResponse.GetTimeout());

    TIssues issues;
    IssuesFromMessage(result.QueryResponse.GetIssues(), issues);
    bool hasFallbackIssue = false;
    for (const auto& issue : issues) {
        hasFallbackIssue |= issue.GetCode() == TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR;
    }
    UNIT_ASSERT_C(hasFallbackIssue, issues.ToString());
}

Y_UNIT_TEST(UntypedAllocationErrorIsNotTimeout) {
    auto settings = std::make_shared<TDqSettings>();
    settings->WorkersAllocationTimeout = 1234;
    settings->_LongWorkersAllocationFailTimeout = 2000;
    const auto result = ExecuteWithAllocationError(
        settings,
        NDqProto::StatusIds::LIMIT_EXCEEDED,
        NDqProto::ALLOCATION_ERROR_REASON_UNSPECIFIED,
        "allocation timeout");

    UNIT_ASSERT_VALUES_EQUAL(result.AllocationTimeoutMs, 1234);
    UNIT_ASSERT(result.QueryResponse.GetStatusCode() == NDqProto::StatusIds::LIMIT_EXCEEDED);
    UNIT_ASSERT(!result.QueryResponse.GetTimeout());
}

Y_UNIT_TEST(AllocationTimeoutIsNotSentWhenDisabled) {
    const auto result = ExecuteWithAllocationError(
        std::make_shared<TDqSettings>(),
        NDqProto::StatusIds::OVERLOADED,
        NDqProto::ALLOCATION_ERROR_REASON_UNSPECIFIED,
        "allocation overloaded");
    UNIT_ASSERT_VALUES_EQUAL(result.AllocationTimeoutMs, 0);
    UNIT_ASSERT(result.QueryResponse.GetStatusCode() == NDqProto::StatusIds::OVERLOADED);
    UNIT_ASSERT(!result.QueryResponse.GetTimeout());
}

} // Y_UNIT_TEST_SUITE(Executer)
