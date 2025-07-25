#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_async_compute_actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_channels.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_log.h>
#include <ydb/library/yql/dq/actors/compute/ut/proto/mock.pb.h>
#include <ydb/library/yql/dq/actors/input_transforms/dq_input_transform_lookup_factory.h>
#include <ydb/library/yql/dq/actors/task_runner/task_runner_actor.h>
#include <ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <ydb/library/yql/dq/tasks/dq_task_program.h>
#include <ydb/library/yql/dq/transform/yql_common_dq_transform.h>
#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_local.h>
#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_proxy.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_printer.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/providers/common/comp_nodes/yql_factory.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include "mock_lookup_factory.h"


using namespace NActors;

namespace NYql::NDq {

namespace {
static const bool TESTS_VERBOSE = getenv("TESTS_VERBOSE") != nullptr;
#define LOG_D(stream) LOG_DEBUG_S(*ActorSystem.SingleSys(), NKikimrServices::KQP_COMPUTE, LogPrefix << stream);
#define LOG_E(stream) LOG_ERROR_S(*ActorSystem.SingleSys(), NKikimrServices::KQP_COMPUTE, LogPrefix << stream);
struct TMockHttpRequest : NMonitoring::IMonHttpRequest {
    TStringStream Out;
    TCgiParameters Params;
    THttpHeaders Headers;
    IOutputStream& Output() override {
        return Out;
    }
    HTTP_METHOD GetMethod() const override {
        return HTTP_METHOD_GET;
    }
    TStringBuf GetPath() const override {
        return "";
    }
    TStringBuf GetPathInfo() const override {
        return "";
    }
    TStringBuf GetUri() const override {
        return "";
    }
    const TCgiParameters& GetParams() const override {
        return Params;
    }
    const TCgiParameters& GetPostParams() const override {
        return Params;
    }
    TStringBuf GetPostContent() const override {
        return "";
    }
    const THttpHeaders& GetHeaders() const override {
        return Headers;
    }
    TStringBuf GetHeader(TStringBuf name) const override {
        const auto* header = Headers.FindHeader(name);
        return header ? header->Value() : TStringBuf();
    }
    TStringBuf GetCookie(TStringBuf) const override {
        return "";
    }
    TString GetRemoteAddr() const override {
        return "::";
    }
    TString GetServiceTitle() const override {
        return "";
    }

    NMonitoring::IMonPage* GetPage() const override {
        return nullptr;
    }

    IMonHttpRequest* MakeChild(NMonitoring::IMonPage*, const TString&) const override {
        return nullptr;
    }
};

struct TActorSystem: NActors::TTestActorRuntimeBase {
    TActorSystem()
        : NActors::TTestActorRuntimeBase(1, true)
    {}

    void Start()
    {
        SetDispatchTimeout(TDuration::Seconds(20));
        InitNodes();
        SetLogBackend(CreateStderrBackend());
        AppendToLogSettings(
                NKikimrServices::EServiceKikimr_MIN,
                NKikimrServices::EServiceKikimr_MAX,
                NKikimrServices::EServiceKikimr_Name<NLog::EComponent>
                );

        if (TESTS_VERBOSE) {
            SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::EPriority::PRI_TRACE);
        }
    }
};

using namespace NKikimr::NMiniKQL;

NDq::IDqAsyncIoFactory::TPtr CreateAsyncIoFactory() {
    auto factory = MakeIntrusive<NYql::NDq::TDqAsyncIoFactory>();
    RegisterMockProviderFactories(*factory);
    RegisterDqInputTransformLookupActorFactory(*factory);
    return factory;
}

struct TAsyncCATestFixture: public NUnitTest::TBaseFixture {
    static constexpr ui64 InputChannelId = 1;
    static constexpr ui64 OutputChannelId = 2;
    static constexpr ui32 InputStageId = 123;
    static constexpr ui32 ThisStageId = 456;
    static constexpr ui32 OutputStageId = 789;
    static constexpr ui32 InputTaskId = 1;
    static constexpr ui32 ThisTaskId = 2;
    static constexpr ui32 OutputTaskId = 3;
    static constexpr i32 MinTransformedValue = 1;
    static constexpr i32 MaxTransformedValue = 10;
    TActorSystem ActorSystem;
    TActorId EdgeActor;
    TActorId SrcEdgeActor;
    TActorId DstEdgeActor;

    TScopedAlloc Alloc;
    TTypeEnvironment TypeEnv;
    TMemoryUsageInfo MemInfo;
    TIntrusivePtr<IFunctionRegistry> FunctionRegistry;
    THolderFactory HolderFactory;
    TDefaultValueBuilder Vb;
    bool IsWide; // BEWARE Wide tests are partially unimplemented
    NDqProto::EDataTransportVersion TransportVersion;
    TStructType* RowType = nullptr;
    TMultiType* WideRowType = nullptr;
    TStructType* RowTransformedType = nullptr;
    TMultiType* WideRowTransformedType = nullptr;
    TString LogPrefix;

    TAsyncCATestFixture(
            NDqProto::EDataTransportVersion transportVersion = NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0,
            bool isWide = false
        )
        : Alloc(__LOCATION__)
        , TypeEnv(Alloc)
        , MemInfo("Mem")
        , FunctionRegistry(NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry()))
        , HolderFactory(Alloc.Ref(), MemInfo, FunctionRegistry.Get())
        , Vb(HolderFactory)
        , IsWide(isWide)
        , TransportVersion(transportVersion)
    {
        auto keyType = TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv);
        auto tsType = TDataType::Create(NUdf::TDataType<ui64>::Id, TypeEnv);
        RowType = TStructTypeBuilder(TypeEnv)
                .Add("id", keyType)
                .Add("ts", tsType)
                .Build();
        TVector<TType*> inputTypes(Reserve(RowType->GetMembersCount()));
        for (ui32 i = 0; i < RowType->GetMembersCount(); ++i) {
            inputTypes.emplace_back(RowType->GetMemberType(i));
        }
        WideRowType = TMultiType::Create(inputTypes.size(), inputTypes.data(), TypeEnv);

        RowTransformedType = TStructTypeBuilder(TypeEnv)
                .Add("e.id", keyType)
                .Add("e.ts", tsType)
                .Add("u.data", TOptionalType::Create(TDataType::Create(NUdf::TDataType<char*>::Id, TypeEnv), TypeEnv))
                .Add("u.key", TOptionalType::Create(keyType, TypeEnv))
                .Build();
        TVector<TType*> outputTypes(Reserve(RowTransformedType->GetMembersCount()));
        for (ui32 i = 0; i < RowTransformedType->GetMembersCount(); ++i) {
            outputTypes.emplace_back(RowTransformedType->GetMemberType(i));
        }
        WideRowTransformedType = TMultiType::Create(outputTypes.size(), outputTypes.data(), TypeEnv);
    }

    void SetUp(NUnitTest::TTestContext& /* context */) override {
        ActorSystem.Start();

        EdgeActor = ActorSystem.AllocateEdgeActor();
        SrcEdgeActor = ActorSystem.AllocateEdgeActor();
        DstEdgeActor = ActorSystem.AllocateEdgeActor();
    }

    // Generates program that squares `id` column and passes `ts` column as is
    // ExprType for id column is generated by `typeMaker(ctx)`
    // ts has type Uint64
    void GenerateSquareProgram(NDqProto::TDqTask& task, auto typeMaker) {
        // TODO: parse sexpr from text and use automated type annotation
        auto& program = *task.MutableProgram();
        using namespace NNodes;
        TExprContext ctx;
        TPositionHandle pos;
        auto lambda = Build<TCoLambda>(ctx, pos)
            .Args({"in_stream"})
            .Body<TCoMap>()
                .Input({"in_stream"})
                .Lambda()
                    .Args({"val"})
                    .Body<TCoAsStruct>()
                        .Add<TCoNameValueTuple>()
                            .Name().Build("id")
                            .Value<TCoMul>()
                                .Left<TCoMember>()
                                    .Name().Build("id")
                                    .Struct("val")
                                .Build()
                                .Right<TCoMember>()
                                    .Name().Build("id")
                                    .Struct("val")
                                .Build()
                            .Build()
                        .Build()
                        .Add<TCoNameValueTuple>()
                            .Name().Build("ts")
                            .Value<TCoMember>()
                                .Name().Build("ts")
                                .Struct("val")
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Done();
        auto type = typeMaker(ctx);
        auto tsType = ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
        auto inStructType = ctx.MakeType<TStructExprType>(
            TVector<const TItemExprType*> {
                ctx.MakeType<TItemExprType>("id", type),
                ctx.MakeType<TItemExprType>("ts", tsType),
            }
        );
        auto inStreamType = ctx.MakeType<TStreamExprType>(inStructType);
        auto outStructType = inStructType;
        auto outStreamType = inStreamType;
        lambda.Ptr()->SetTypeAnn(outStreamType);
        lambda.Args().Arg(0).Ptr()->SetTypeAnn(inStreamType);
        {
            const auto& coMap = lambda.Body().Cast<TCoMap>();
            coMap.Ptr()->SetTypeAnn(inStreamType);
            coMap.Input().Ptr()->SetTypeAnn(inStreamType);
            {
                const auto& coMapLambda = coMap.Lambda();
                coMapLambda.Ptr()->SetTypeAnn(outStructType);
                coMapLambda.Args().Arg(0).Ptr()->SetTypeAnn(inStructType);
                {
                    const auto& asStruct = coMap.Lambda().Body().Cast<TCoAsStruct>();
                    asStruct.Ptr()->SetTypeAnn(outStructType);
                    {
                        const auto& coMul = asStruct.Arg(0).Cast<TCoNameValueTuple>().Value().Cast<TCoMul>();
                        coMul.Ptr()->SetTypeAnn(type);
                        coMul.Left().Ptr()->SetTypeAnn(type);
                        coMul.Left().Cast<TCoMember>().Struct().Ptr()->SetTypeAnn(inStructType);
                        coMul.Right().Ptr()->SetTypeAnn(type);
                        coMul.Right().Cast<TCoMember>().Struct().Ptr()->SetTypeAnn(inStructType);
                    }
                    {
                        const auto& coMember = asStruct.Arg(1).Cast<TCoNameValueTuple>().Value().Cast<TCoMember>();
                        coMember.Ptr()->SetTypeAnn(tsType);
                        coMember.Struct().Ptr()->SetTypeAnn(inStructType);
                    }
                }
            }
        }
        NCommon::TMkqlCommonCallableCompiler compiler;
        program.SetRaw(NDq::BuildProgram(
                    lambda,
                    *ctx.MakeType<TStructExprType>(TVector<const TItemExprType*> {}),
                    compiler,
                    TypeEnv,
                    *FunctionRegistry,
                    ctx,
                    /* reads */ {},
                    TSpillingSettings {}
                    ));
        program.SetRuntimeVersion(NYql::NDqProto::ERuntimeVersion::RUNTIME_VERSION_YQL_1_0);
        // Settings
        // LangVer
    }

    // Generates dummy empty program that passes stream-of-structures as is
    // ExprType for structures is generated by typeMaker(ctx)
    void GenerateEmptyProgram(NDqProto::TDqTask& task, auto typeMaker) {
        auto& program = *task.MutableProgram();
        using namespace NNodes;
        TExprContext ctx;
        TPositionHandle pos;
        auto lambda = Build<TCoLambda>(ctx, pos)
            .Args({"in_stream"})
            .Body<TCoMap>()
                .Input({"in_stream"})
                .Lambda()
                    .Args({"val"})
                    .Body({"val"})
                .Build()
            .Build()
        .Done();
        auto type = typeMaker(ctx);
        auto inStreamType = ctx.MakeType<TStreamExprType>(type);
        auto outStreamType = inStreamType;
        lambda.Ptr()->SetTypeAnn(outStreamType);
        lambda.Args().Arg(0).Ptr()->SetTypeAnn(inStreamType);
        {
            const auto& coMap = lambda.Body().Cast<TCoMap>();
            coMap.Ptr()->SetTypeAnn(inStreamType);
            coMap.Input().Ptr()->SetTypeAnn(inStreamType);
            {
                const auto& coMapLambda = coMap.Lambda();
                coMapLambda.Ptr()->SetTypeAnn(type);
                coMapLambda.Args().Arg(0).Ptr()->SetTypeAnn(type);
            }
        }
        NCommon::TMkqlCommonCallableCompiler compiler;
        program.SetRaw(NDq::BuildProgram(
                    lambda,
                    *ctx.MakeType<TStructExprType>(TVector<const TItemExprType*> {}),
                    compiler,
                    TypeEnv,
                    *FunctionRegistry,
                    ctx,
                    /* reads */ {},
                    TSpillingSettings {}
                    ));
        program.SetRuntimeVersion(NYql::NDqProto::ERuntimeVersion::RUNTIME_VERSION_YQL_1_0);
        // Settings
        // LangVer
    }

    // Set input transform for first input (must be already present)
    void SetInputTransform(NDqProto::TDqTask& task, TType* keyType, TType* valueType) {
        Y_ENSURE(task.MutableInputs()->size() >= 1);
        auto& input = *task.MutableInputs()->Mutable(0);
        auto& transform = *input.MutableTransform();
        transform.SetType("StreamLookupInputTransform");

        auto narrowInputType = RowType;
        auto narrowOutputType = RowTransformedType;

        TType* inputType = IsWide ? static_cast<TType*>(WideRowType) : RowType;
        transform.SetInputType(SerializeNode(inputType, TypeEnv));

        TType* outputType = IsWide ? static_cast<TType*>(WideRowTransformedType) : RowTransformedType;
        transform.SetOutputType(SerializeNode(outputType, TypeEnv));

        NDqProto::TDqInputTransformLookupSettings settings;
        settings.SetLeftLabel("e");
        settings.SetRightLabel("u");

        auto& rightSource = *settings.MutableRightSource();
        rightSource.SetProviderName("MockLookup");
        auto rightType = TStructTypeBuilder(TypeEnv)
            .Add("key", keyType)
            .Add("data", valueType)
            .Build();
        rightSource.SetSerializedRowType(SerializeNode(rightType, TypeEnv));
        Mock::TLookupSource lookupSource;
        lookupSource.SetMinValue(MinTransformedValue);
        lookupSource.SetMaxValue(MaxTransformedValue);
        rightSource.MutableLookupSource()->PackFrom(lookupSource);
        settings.SetJoinType("Left");
        settings.AddLeftJoinKeyNames("id");
        settings.AddRightJoinKeyNames("key");
        settings.SetNarrowInputRowType(SerializeNode(narrowInputType, TypeEnv));
        settings.SetNarrowOutputRowType(SerializeNode(narrowOutputType, TypeEnv));
        settings.SetCacheLimit(10);
        settings.SetCacheTtlSeconds(1);
        settings.SetMaxDelayedRows(5);
        transform.MutableSettings()->PackFrom(settings);
    }

    // Adds dummy input channel with channelId
    // returns IDqOutputChannel::TPtr that can be used to inject data/checkpoints/watermarks into channel
    auto AddDummyInputChannel(NDqProto::TDqTask& task, ui64 channelId) {
        auto& input = *task.AddInputs();
        auto& channel = *input.AddChannels();
        input.MutableUnionAll(); // for side-effect
        channel.SetId(channelId);
        auto& chEndpoint = *channel.MutableSrcEndpoint();
        ActorIdToProto(SrcEdgeActor, chEndpoint.MutableActorId());
        channel.SetWatermarksMode(NDqProto::WATERMARKS_MODE_DEFAULT);
        channel.SetCheckpointingMode(NDqProto::CHECKPOINTING_MODE_DEFAULT);
        channel.SetInMemory(true);
        channel.SetSrcStageId(InputStageId);
        channel.SetDstStageId(ThisStageId);
        channel.SetSrcTaskId(InputTaskId);
        channel.SetDstTaskId(ThisTaskId);
        // DstEndpoint
        // IsPersistent
        // EnableSpilling
        TLogFunc logFunc = [this](const TString& msg) {
            LOG_D(msg);
        };
        // DqOutputChannel is used for simulating input on CA under the test
        TDqOutputChannelSettings settings;
        settings.TransportVersion = TransportVersion;
        settings.MutableSettings.IsLocalChannel = true;
        return CreateDqOutputChannel(channelId, ThisStageId,
                (IsWide ? static_cast<TType*>(WideRowType) : RowType), HolderFactory,
                settings,
                logFunc);
    }

    // Adds dummy output channel with channelId
    // returns IDqInputChannel::TPtr that can be used to simulating reading from this channel
    auto AddDummyOutputChannel(NDqProto::TDqTask& task, ui64 channelId, TType* type) {
        auto& output = *task.AddOutputs();
        output.MutableBroadcast(); // for side-effect
        auto& channel = *output.AddChannels();
        channel.SetId(channelId);
        auto& chEndpoint = *channel.MutableDstEndpoint();
        ActorIdToProto(DstEdgeActor, chEndpoint.MutableActorId());
        channel.SetWatermarksMode(NDqProto::WATERMARKS_MODE_DEFAULT);
        channel.SetCheckpointingMode(NDqProto::CHECKPOINTING_MODE_DEFAULT);
        channel.SetInMemory(true);
        channel.SetDstStageId(OutputStageId);
        channel.SetSrcStageId(ThisStageId);
        channel.SetDstTaskId(OutputTaskId);
        channel.SetSrcTaskId(ThisTaskId);
        channel.SetTransportVersion(TransportVersion);
        // SrcEndpoint
        // DstEndpoint
        // IsPersistent
        // EnableSpilling
        return CreateDqInputChannel(channelId, ThisStageId, type, 10_MB, TCollectStatsLevel::None, TypeEnv, HolderFactory, TransportVersion);
    }

    auto CreateTestAsyncCA(NDqProto::TDqTask& task) {
        TVector<NKikimr::NMiniKQL::TComputationNodeFactory> compNodeFactories = {
            NYql::GetCommonDqFactory(),
            NKikimr::NMiniKQL::GetYqlFactory()
        };

        NKikimr::NMiniKQL::TComputationNodeFactory dqCompFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory(std::move(compNodeFactories));

        NYql::TTaskTransformFactory dqTaskTransformFactory = NYql::CreateCompositeTaskTransformFactory({
                NYql::CreateCommonDqTaskTransformFactory()
                });
        auto patternCache = std::make_shared<NKikimr::NMiniKQL::TComputationPatternLRUCache>(NKikimr::NMiniKQL::TComputationPatternLRUCache::Config(200_MB, 200_MB));
        auto factory = NTaskRunnerProxy::CreateFactory(
                FunctionRegistry.Get(),
                dqCompFactory,
                dqTaskTransformFactory,
                patternCache, false);
        auto taskRunnerActorFactory = NDq::NTaskRunnerActor::CreateLocalTaskRunnerActorFactory(
            [factory=factory](std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, const NDq::TDqTaskSettings& task, NDqProto::EDqStatsMode statsMode, const NDq::TLogFunc& )
                {
                    return factory->Get(alloc, task, statsMode);
                });
        TComputeMemoryLimits memoryLimits;
        memoryLimits.ChannelBufferSize = 1_MB;
        memoryLimits.MkqlLightProgramMemoryLimit = 10_MB;
        memoryLimits.MkqlHeavyProgramMemoryLimit = 20_MB;
        memoryLimits.MkqlProgramHardMemoryLimit = 30_MB;
        memoryLimits.MemoryQuotaManager = std::make_shared<TGuaranteeQuotaManager>(64_MB, 32_MB);
        auto actor = CreateDqAsyncComputeActor(
                EdgeActor, // executerId,
                LogPrefix,
                &task, // NYql::NDqProto::TDqTask* task,
                CreateAsyncIoFactory(),
                FunctionRegistry.Get(),
                {}, // TComputeRuntimeSettings& settings,
                memoryLimits,
                taskRunnerActorFactory,
                {}, // ::NMonitoring::TDynamicCounterPtr taskCounters,
                {}, // const TActorId& quoterServiceActorId,
                false // ownCounters
                );
        UNIT_ASSERT(actor);
        return ActorSystem.Register(actor);
    }

    TUnboxedValueBatch CreateRow(ui32 value, ui64 ts) {
        if (IsWide) {
            TUnboxedValueBatch result(WideRowType);
            result.PushRow([&](ui32 idx) {
                return RowType->GetMemberName(idx) == "id" ? NUdf::TUnboxedValuePod(value) : NUdf::TUnboxedValuePod(ts);
            });
            return result;
        }
        NUdf::TUnboxedValue* items;
        auto row = Vb.NewArray(RowType->GetMembersCount(), items);
        items[0] = NUdf::TUnboxedValuePod(value);
        items[1] = NUdf::TUnboxedValuePod(ts);
        TUnboxedValueBatch result(RowType);
        result.emplace_back(std::move(row));
        return result;
    }

    void PushRow(TUnboxedValueBatch&& row, const IDqOutputChannel::TPtr& ch) {
        auto* values = row.Head();
        if (IsWide) {
            ch->WidePush(values, *row.Width());
        } else {
            ch->Push(std::move(*values));
        }
    }

    bool ReceiveData(auto&& cb, auto&& cbWatermark, auto dqInputChannel) {
        auto ev = ActorSystem.GrabEdgeEvent<TEvDqCompute::TEvChannelData>({DstEdgeActor});
        LOG_D("Got " << ev->Get()->Record.DebugString());
        TDqSerializedBatch sbatch;
        sbatch.Proto = ev->Get()->Record.GetChannelData().GetData();
        dqInputChannel->Push(std::move(sbatch));
        if (ev->Get()->Record.GetChannelData().GetFinished()) {
            dqInputChannel->Finish();
        }
        TUnboxedValueBatch batch;
        const auto columns = IsWide ? static_cast<TMultiType*>(dqInputChannel->GetInputType())->GetElementsCount() : static_cast<TStructType*>(dqInputChannel->GetInputType())->GetMembersCount();
        while (dqInputChannel->Pop(batch)) {
            if (IsWide) {
                if (!batch.ForEachRowWide([this, cb, columns](const NUdf::TUnboxedValue row[], ui32 width) {
                    LOG_D("WideRow:");
                    if (row) {
                        UNIT_ASSERT_EQUAL(width, columns);
                        for (ui32 col = 0; col < width; ++col) {
                            const auto& item = row[col];
                            if (!cb(item, col)) {
                               return false;
                            }
                        }
                    } else {
                        LOG_D("null");
                        UNIT_ASSERT(false);
                    }
                    return true;
                })) {
                    return false;
                }
            } else {
                if (!batch.ForEachRow([this, cb, columns](const NUdf::TUnboxedValue& row) {
                    LOG_D("Row:");
                    if (row) {
                        for (ui32 col = 0; col < columns; ++col) {
                            const auto& item = row.GetElement(col);
                            if (!cb(item, col)) {
                               return false;
                            }
                        }
                    } else {
                        LOG_D("null");
                        UNIT_ASSERT(false);
                    }
                    return true;
                })) {
                    return false;
                }
            }
        }
        if (ev->Get()->Record.GetChannelData().HasWatermark()) {
            auto watermark = TInstant::MicroSeconds(ev->Get()->Record.GetChannelData().GetWatermark().GetTimestampUs());
            cbWatermark(watermark);
        }
        return !dqInputChannel->IsFinished();
    }

    void WaitForChannelDataAck(auto channelId, auto seqNo) {
        for (;;) {
            auto ev = ActorSystem.GrabEdgeEvent<TEvDqCompute::TEvChannelDataAck>({SrcEdgeActor});
            LOG_D("Got ack " << ev->Get()->Record);
            UNIT_ASSERT_EQUAL(ev->Get()->Record.GetChannelId(), channelId);
            if (ev->Get()->Record.GetSeqNo() == seqNo) {
                break;
            }
            LOG_D("...but waiting for " << seqNo);
        }
    }

    void DumpMonPage(auto asyncCA, auto hook) {
        {
            TMockHttpRequest request;
            auto evHttpInfo = MakeHolder<NActors::NMon::TEvHttpInfo>(request);
            ActorSystem.Send(asyncCA, EdgeActor, evHttpInfo.Release());
        }
        {
            auto ev = ActorSystem.GrabEdgeEvent<NActors::NMon::TEvHttpInfoRes>({EdgeActor});
            UNIT_ASSERT_EQUAL(ev->Get()->GetContentType(), NActors::NMon::IEvHttpInfoRes::EContentType::Html);
            TStringStream out;
            ev->Get()->Output(out);
            hook(out.Str());
        }
    }

    void BasicTests(ui32 packets, bool doWatermark, bool waitIntermediateAcks) {
        LogPrefix = TStringBuilder() << "Square Test for:"
           << " packets=" << packets
           << " doWatermark=" << doWatermark
           << " waitIntermediateAcks=" << waitIntermediateAcks
           << " ";
        NDqProto::TDqTask task;
        GenerateSquareProgram(task, [](auto& ctx) {
            return ctx.template MakeType<TDataExprType>(EDataSlot::Int32);
        });
        auto dqOutputChannel = AddDummyInputChannel(task, InputChannelId);
        auto dqInputChannel = AddDummyOutputChannel(task, OutputChannelId, (IsWide ? static_cast<TType*>(WideRowType) : RowType));

        auto asyncCA = CreateTestAsyncCA(task);
        ActorSystem.EnableScheduleForActor(asyncCA, true);
        ui32 seqNo = 0;
        ui32 val = 0;
        for (ui32 packet = 1; packet <= packets; ++packet) {
            bool isFinal = packet == packets;
            bool noAck = (packet % 2) == 0; // set noAck on even packets

            PushRow(CreateRow(++val, packet), dqOutputChannel);
            PushRow(CreateRow(++val, packet), dqOutputChannel);
            PushRow(CreateRow(++val, packet), dqOutputChannel);
            if (doWatermark) {
                NDqProto::TWatermark watermark;
                watermark.SetTimestampUs(TInstant::Seconds(packet).MicroSeconds());
                dqOutputChannel->Push(std::move(watermark));
            }
            if (isFinal) {
                dqOutputChannel->Finish();
            }

            auto evInputChannelData = MakeHolder<TEvDqCompute::TEvChannelData>();
            evInputChannelData->Record.SetSeqNo(++seqNo);
            evInputChannelData->Record.SetNoAck(noAck);
            auto& chData = *evInputChannelData->Record.MutableChannelData();
            if (TDqSerializedBatch serializedBatch; dqOutputChannel->Pop(serializedBatch)) {
                *chData.MutableData() = serializedBatch.Proto;
                Y_ENSURE(serializedBatch.Payload.Empty()); // TODO
            }
            if (NDqProto::TWatermark watermark; dqOutputChannel->Pop(watermark)) {
                *chData.MutableWatermark() = watermark;
            }
            if (NDqProto::TCheckpoint checkpoint; dqOutputChannel->Pop(checkpoint)) {
                *chData.MutableCheckpoint() = checkpoint;
            }
            chData.SetChannelId(InputChannelId);
            chData.SetFinished(dqOutputChannel->IsFinished());
            LOG_D("Sending " << packet << "/" << packets << " "  << chData);
            ActorSystem.Send(asyncCA, SrcEdgeActor, evInputChannelData.Release());
            if ((isFinal || waitIntermediateAcks) && !noAck) {
                WaitForChannelDataAck(InputChannelId, seqNo);
            }
        }

        TMap<ui32, ui32> receivedData;
        TMaybe<TInstant> watermark;
        while (ReceiveData(
                [this, &receivedData, &watermark](const NUdf::TUnboxedValue& val, ui32 column) {
                    UNIT_ASSERT(!!val);
                    UNIT_ASSERT(val.IsEmbedded());
                    if (RowType->GetMemberName(column) == "ts") {
                        auto ts = val.Get<ui64>();
                        if (watermark) {
                            UNIT_ASSERT_GT(ts, watermark->Seconds());
                        }
                        return true;
                    }
                    UNIT_ASSERT_EQUAL(RowType->GetMemberName(column), "id");
                    auto data = val.Get<i32>();
                    LOG_D(data);
                    ++receivedData[data];
                    return true;
                },
                [this, &watermark](const auto& receivedWatermark) {
                    watermark = receivedWatermark;
                    LOG_D("Got watermark " << *watermark);
                },
                dqInputChannel))
        {}
        DumpMonPage(asyncCA, [this](auto&& str) {
            UNIT_ASSERT_STRING_CONTAINS(str, "<h3>Sources</h3>");
            UNIT_ASSERT_STRING_CONTAINS(str, LogPrefix);
            // TODO add validation
            LOG_D(str);
        });
        UNIT_ASSERT_EQUAL(receivedData.size(), val);
        for (; val > 0; --val) {
            UNIT_ASSERT_EQUAL_C(receivedData[val * val], 1, "expected count for " << (val * val));
        }
    }

#if 0 // TODO: switch when inputtransform will be fixed; just log for now
#define WEAK_UNIT_ASSERT_GT_C UNIT_ASSERT_GT_C
#define WEAK_UNIT_ASSERT UNIT_ASSERT
#else
#define WEAK_UNIT_ASSERT_GT_C(A, B, C) do { if (!((A) > (B))) LOG_E("Assert " #A " > " #B " failed " << C); } while(0)
#define WEAK_UNIT_ASSERT(A) do { if (!(A)) LOG_E("Assert " #A " failed "); } while(0)
#endif
    void InputTransformTests(ui32 packets, bool doWatermark, bool waitIntermediateAcks) {
        LogPrefix = TStringBuilder() << "InputTransform Test for:"
           << " packets=" << packets
           << " doWatermark=" << doWatermark
           << " waitIntermediateAcks=" << waitIntermediateAcks
           << " ";
        NDqProto::TDqTask task;
        GenerateEmptyProgram(task, [](auto& ctx) {
            auto keyType = ctx.template MakeType<TDataExprType>(EDataSlot::Int32);
            auto tsType = ctx.template MakeType<TDataExprType>(EDataSlot::Uint64);
            auto valueType = ctx.template MakeType<TDataExprType>(EDataSlot::String);
            auto structType = ctx.template MakeType<TStructExprType>(
                    TVector<const TItemExprType*> {
                        ctx.template MakeType<TItemExprType>("e.id", keyType),
                        ctx.template MakeType<TItemExprType>("e.ts", tsType),
                        ctx.template MakeType<TItemExprType>("u.data", ctx.template MakeType<TOptionalExprType>(valueType)),
                        ctx.template MakeType<TItemExprType>("u.key", ctx.template MakeType<TOptionalExprType>(keyType)),
                    }
            );
            return structType;
        });
        TMap<i32, ui32> expectedData;
        auto dqOutputChannel = AddDummyInputChannel(task, InputChannelId);
        auto dqInputChannel = AddDummyOutputChannel(task, OutputChannelId, (IsWide ? static_cast<TType*>(WideRowTransformedType) : RowTransformedType));
        SetInputTransform(task,
                TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv),
                TDataType::Create(NUdf::TDataType<char*>::Id, TypeEnv)
                );

        auto asyncCA = CreateTestAsyncCA(task);
        ActorSystem.EnableScheduleForActor(asyncCA, true);
        ui32 seqNo = 0;
        ui32 val = 0;
        for (ui32 packet = 1; packet <= packets; ++packet) {
            bool isFinal = packet == packets;
            bool noAck = (packet % 2) == 0; // set noAck on even packets

            PushRow(CreateRow(++val, packet), dqOutputChannel);
            ++expectedData[val];
            PushRow(CreateRow(++val, packet), dqOutputChannel);
            ++expectedData[val];
            PushRow(CreateRow(++val, packet), dqOutputChannel);
            ++expectedData[val];
            // below row may be served from cache
            PushRow(CreateRow(++val % (MaxTransformedValue * 2), packet), dqOutputChannel);
            ++expectedData[val % (MaxTransformedValue * 2)];
            PushRow(CreateRow(++val, packet), dqOutputChannel);
            ++expectedData[val];
            PushRow(CreateRow(++val, packet), dqOutputChannel);
            ++expectedData[val];
            if (doWatermark) {
                NDqProto::TWatermark watermark;
                watermark.SetTimestampUs(TInstant::Seconds(packet).MicroSeconds());
                dqOutputChannel->Push(std::move(watermark));
            }
            if (isFinal) {
                dqOutputChannel->Finish();
            }

            auto evInputChannelData = MakeHolder<TEvDqCompute::TEvChannelData>();
            evInputChannelData->Record.SetSeqNo(++seqNo);
            evInputChannelData->Record.SetNoAck(noAck);
            auto& chData = *evInputChannelData->Record.MutableChannelData();
            if (TDqSerializedBatch serializedBatch; dqOutputChannel->Pop(serializedBatch)) {
                *chData.MutableData() = serializedBatch.Proto;
                Y_ENSURE(serializedBatch.Payload.Empty()); // TODO
            }
            if (NDqProto::TWatermark watermark; dqOutputChannel->Pop(watermark)) {
                *chData.MutableWatermark() = watermark;
            }
            if (NDqProto::TCheckpoint checkpoint; dqOutputChannel->Pop(checkpoint)) {
                *chData.MutableCheckpoint() = checkpoint;
            }
            chData.SetChannelId(InputChannelId);
            chData.SetFinished(dqOutputChannel->IsFinished());
            LOG_D("Sending " << packet << "/" << packets << " "  << chData);
            ActorSystem.Send(asyncCA, SrcEdgeActor, evInputChannelData.Release());
            if ((isFinal || waitIntermediateAcks) && !noAck) {
                WaitForChannelDataAck(InputChannelId, seqNo);
            }
        }

        TMap<i32, ui32> receivedData;

        i32 col0 = ~0;
        TMaybe<TInstant> watermark;
        while (ReceiveData(
                [this, &receivedData, &watermark, &col0](const NUdf::TUnboxedValue& val, ui32 column) {
                    UNIT_ASSERT_LT(column, RowTransformedType->GetMembersCount());
                    auto columnName = RowTransformedType->GetMemberName(column);
                    if (columnName == "e.id") {
                        UNIT_ASSERT(!!val);
                        UNIT_ASSERT(val.IsEmbedded());
                        LOG_D(column << "id = " << val.Get<i32>());
                        col0 = val.Get<i32>();
                        ++receivedData[val.Get<i32>()];
                    } else if (columnName == "e.ts") {
                        UNIT_ASSERT(!!val);
                        UNIT_ASSERT(val.IsEmbedded());
                        auto ts = val.Get<ui64>();
                        LOG_D(column << "ts = " << ts);
                        if (watermark) {
                            WEAK_UNIT_ASSERT_GT_C(ts, watermark->Seconds(), "Timestamp " << ts << " before watermark: " << watermark->Seconds());
                        }
                    } else if (columnName == "u.key") {
                        if (col0 >= MinTransformedValue && col0 <= MaxTransformedValue) {
                            UNIT_ASSERT(!!val);
                            auto cval = val.GetOptionalValue();
                            UNIT_ASSERT(!!cval);
                            UNIT_ASSERT(cval.IsEmbedded());
                            auto data = cval.Get<i32>();
                            LOG_D(column << "key = " << data);
                            UNIT_ASSERT_EQUAL_C(data, col0, data << "!=" << col0);
                        } else {
                            UNIT_ASSERT_C(!val, "null (1) expected for " << col0);
                        }
                    } else if (columnName == "u.data") {
                        if (col0 >= MinTransformedValue && col0 <= MaxTransformedValue) {
                            UNIT_ASSERT(!!val);
                            const auto cval = val.GetOptionalValue();
                            UNIT_ASSERT(!!cval);
                            auto ref = TString(cval.AsStringRef());
                            LOG_D(column << "data = '" << ref << "'");
                            UNIT_ASSERT_EQUAL(ref, ToString(col0));
                        } else {
                            UNIT_ASSERT_C(!val, "null (2) expected for " << col0);
                        }
                    } else {
                        UNIT_ASSERT_C(false, "Unexpected column " << column << " name " << columnName);
                    }
                    return true;
                },
                [this, &watermark](const auto& receivedWatermark) {
                    watermark = receivedWatermark;
                    LOG_D("Got watermark " << *watermark);
                },
                dqInputChannel))
        {}
        DumpMonPage(asyncCA, [this](auto&& str) {
            UNIT_ASSERT_STRING_CONTAINS(str, "<h3>Sources</h3>");
            UNIT_ASSERT_STRING_CONTAINS(str, LogPrefix);
            // TODO add validation
            LOG_D(str);
        });
        UNIT_ASSERT_EQUAL(receivedData.size(), expectedData.size());
        for (auto [receivedVal, receivedCnt] : receivedData) {
            UNIT_ASSERT_EQUAL_C(receivedCnt, expectedData[receivedVal], "expected count for " << receivedVal << ": " << receivedCnt << " != " << expectedData[receivedVal]);
        }
        if (doWatermark) {
            WEAK_UNIT_ASSERT(!!watermark);
            if (watermark) {
                LOG_D("Last watermark " << *watermark);
            } else {
                LOG_E("NO WATERMARK");
            }
        }
    }
};

} //namespace anonymous

Y_UNIT_TEST_SUITE(TAsyncComputeActorTest) {
    Y_UNIT_TEST_F(Empty, TAsyncCATestFixture) { }

    Y_UNIT_TEST_F(Basic, TAsyncCATestFixture) {
        for (bool waitIntermediateAcks : { false, true }) {
            for (bool doWatermark : { false, true }) {
                for (ui32 packets : { 1, 2, 3, 4, 5 }) {
                    BasicTests(packets, doWatermark, waitIntermediateAcks);
                }
            }
        }
    }

    Y_UNIT_TEST_F(InputTransform, TAsyncCATestFixture) {
        for (bool waitIntermediateAcks : { false, true }) {
            for (bool doWatermark : { false, true }) {
                for (ui32 packets : { 1, 2, 3, 4, 5, 111 }) {
                    InputTransformTests(packets, doWatermark, waitIntermediateAcks);
                }
            }
        }
    }
}

} //namespace NYql::NDq

