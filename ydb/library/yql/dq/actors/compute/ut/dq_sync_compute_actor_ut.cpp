#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/string/cast.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

#include <ydb/core/base/backtrace.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
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


using namespace NYql::NNodes;

namespace NYql::NDq {

namespace {

static const bool TESTS_VERBOSE = getenv("TESTS_VERBOSE") != nullptr;
static const bool TESTS_LARGE = getenv("TESTS_LARGE") != nullptr;

#define LOG_D(stream) LOG_DEBUG_S(*ActorSystem.SingleSys(), NKikimrServices::KQP_COMPUTE, LogPrefix << stream)
#define LOG_E(stream) LOG_ERROR_S(*ActorSystem.SingleSys(), NKikimrServices::KQP_COMPUTE, LogPrefix << stream)

void SegmentationFaultHandler(int) {
    Cerr << "segmentation fault call stack:" << Endl;
    FormatBackTrace(&Cerr);
    abort();
}

struct TMockHttpRequest : NMonitoring::IMonHttpRequest {
    TStringStream Out;
    TCgiParameters Params;
    THttpHeaders Headers;
    TMockHttpRequest() {
        Params.Scan("view=dump");
    }
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

    void Start() {
        signal(SIGSEGV, &SegmentationFaultHandler);
        SetDispatchTimeout(TDuration::Seconds(20));
        InitNodes();
        SetLogBackend(NActors::CreateStderrBackend());
        AppendToLogSettings(
                NKikimrServices::EServiceKikimr_MIN,
                NKikimrServices::EServiceKikimr_MAX,
                NKikimrServices::EServiceKikimr_Name<NActors::NLog::EComponent>
                );

        if (TESTS_VERBOSE) {
            SetLogPriority(NKikimrServices::KQP_COMPUTE, NActors::NLog::EPriority::PRI_TRACE);
            SetLogPriority(NKikimrServices::DQ_TASK_RUNNER, NActors::NLog::EPriority::PRI_TRACE);
        }
    }
};

using namespace NKikimr::NMiniKQL;

static const TString MockSinkType = "MockSink";

struct TEvMockSinkPrivate {
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

        EvAckCommitState = EvBegin,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvAckCommitState: public NActors::TEventLocal<TEvAckCommitState, EvAckCommitState> {};
};

struct TMockSinkState: public TThrRefBase {
    using TPtr = TIntrusivePtr<TMockSinkState>;

    std::atomic<ui64> SendDataCalls = 0;
    std::atomic<ui64> Rows = 0;
    std::atomic<bool> Finished = false;

    // Checkpointing.
    std::atomic<bool> DeferCommit = false;
    std::atomic<ui64> CommitStateCalls = 0;

    void OnCommitState(NActors::TActorId sinkActor) {
        {
            TGuard<TMutex> guard(Lock);
            SinkActors.insert(sinkActor);
        }
        ++CommitStateCalls;
    }

    TVector<NActors::TActorId> GetSinkActors() const {
        TGuard<TMutex> guard(Lock);
        return TVector<NActors::TActorId>(SinkActors.begin(), SinkActors.end());
    }

private:
    TMutex Lock;
    THashSet<NActors::TActorId> SinkActors;
};

class TMockSinkActor: public IDqComputeActorAsyncOutput, public NActors::TActor<TMockSinkActor> {
public:
    TMockSinkActor(ui64 outputIndex, IDqComputeActorAsyncOutput::ICallbacks* callbacks, TMockSinkState::TPtr state)
        : NActors::TActor<TMockSinkActor>(&TMockSinkActor::StateFunc)
        , OutputIndex(outputIndex)
        , Callbacks(callbacks)
        , State(std::move(state))
    {}

private:
    STRICT_STFUNC(StateFunc,
                  hFunc(NActors::TEvents::TEvPoison, Handle);
                  hFunc(TEvMockSinkPrivate::TEvAckCommitState, Handle);)

    void Handle(NActors::TEvents::TEvPoison::TPtr) {
        PassAway();
    }

    void Handle(TEvMockSinkPrivate::TEvAckCommitState::TPtr) {
        Y_ABORT_UNLESS(!DeferredCheckpoints.empty(), "sink[%lu] has no deferred commit to acknowledge", OutputIndex);

        for (const auto& checkpoint : DeferredCheckpoints) {
            Callbacks->OnAsyncOutputStateCommitted(OutputIndex, checkpoint);
        }

        DeferredCheckpoints.clear();
    }

private:
    ui64 GetOutputIndex() const override {
        return OutputIndex;
    }

    i64 GetFreeSpace() const override {
        return 1_MB;
    }

    const TDqAsyncStats& GetEgressStats() const override {
        return EgressStats;
    }

    void SendData(
        TUnboxedValueBatch&& batch,
        i64 /* dataSize */,
        const TMaybe<NDqProto::TCheckpoint>& /* checkpoint */,
        bool finished
    ) override {
        ++State->SendDataCalls;
        State->Rows += batch.RowCount();
        batch.clear();
        if (finished) {
            State->Finished = true;
            Callbacks->OnAsyncOutputFinished(OutputIndex);
        }
    }

    void CommitState(const NDqProto::TCheckpoint& checkpoint) override {
        if (State->DeferCommit.load()) {
            DeferredCheckpoints.push_back(checkpoint);
        } else {
            Callbacks->OnAsyncOutputStateCommitted(OutputIndex, checkpoint);
        }
        State->OnCommitState(SelfId());
    }

    void LoadState(const TSinkState&, const NDqProto::TCheckpoint&) override {
    }

    void PassAway() override {
        NActors::TActor<TMockSinkActor>::PassAway();
    }

private:
    const ui64 OutputIndex;
    IDqComputeActorAsyncOutput::ICallbacks* const Callbacks;
    const TMockSinkState::TPtr State;
    TDqAsyncStats EgressStats;
    std::vector<NDqProto::TCheckpoint> DeferredCheckpoints;
};

struct TCheckpointCommit {
    ui64 CheckpointId = 0;
    ui64 Generation = 0;
    ui64 TaskId = 0;
};

struct TMockCoordinatorState: public TThrRefBase {
    using TPtr = TIntrusivePtr<TMockCoordinatorState>;

    std::atomic<ui64> Acks = 0;
    std::atomic<ui64> Commits = 0;

    void OnCommit(TCheckpointCommit commit) {
        {
            TGuard<TMutex> guard(Lock);
            CommitLog.push_back(commit);
        }
        ++Commits;
    }

    TVector<TCheckpointCommit> GetCommitLog() const {
        TGuard<TMutex> guard(Lock);
        return CommitLog;
    }

private:
    mutable TMutex Lock;
    TVector<TCheckpointCommit> CommitLog;
};

class TMockCheckpointCoordinator: public NActors::TActor<TMockCheckpointCoordinator> {
public:
    explicit TMockCheckpointCoordinator(TMockCoordinatorState::TPtr state)
        : NActors::TActor<TMockCheckpointCoordinator>(&TMockCheckpointCoordinator::StateFunc)
        , State(std::move(state))
    {}

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDqCompute::TEvNewCheckpointCoordinatorAck, Handle);
            hFunc(TEvDqCompute::TEvStateCommitted, Handle);
            default:
                break;
        }
    }

    void Handle(TEvDqCompute::TEvNewCheckpointCoordinatorAck::TPtr&) {
        ++State->Acks;
    }

    void Handle(TEvDqCompute::TEvStateCommitted::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        State->OnCommit({
            .CheckpointId = record.GetCheckpoint().GetId(),
            .Generation = record.GetCheckpoint().GetGeneration(),
            .TaskId = record.GetTaskId(),
        });
    }

private:
    const TMockCoordinatorState::TPtr State;
};

NDq::IDqAsyncIoFactory::TPtr CreateAsyncIoFactory(TMockSinkState::TPtr sinkState = nullptr) {
    auto factory = MakeIntrusive<NYql::NDq::TDqAsyncIoFactory>();
    RegisterMockProviderFactories(*factory);
    RegisterDqInputTransformLookupActorFactory(*factory);
    factory->RegisterSink(MockSinkType, [sinkState = std::move(sinkState)](IDqAsyncIoFactory::TSinkArguments&& args) {
        Y_ENSURE(sinkState, "MockSink is used by the task, but no sink state was provided");
        auto* actor = new TMockSinkActor(args.OutputIndex, args.Callback, sinkState);
        return std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> { actor, actor };
    });
    return factory;
}

struct TSyncComputeActorTestFixture: public NUnitTest::TBaseFixture {
    static constexpr ui64 InputChannelId = 1000;
    static constexpr ui64 OutputChannelId = 2000;
    static constexpr ui32 InputStageId = 123;
    static constexpr ui32 ThisStageId = 456;
    static constexpr ui32 OutputStageId = 789;
    static constexpr ui32 InputTaskId = 1;
    static constexpr ui32 ThisTaskId = 2;
    static constexpr ui32 OutputTaskId = 3;
    static constexpr i32 MinTransformedValue = 1;
    static constexpr i32 MaxTransformedValue = 10;
    static constexpr ui64 CoordinatorGeneration = 42;
    static constexpr ui64 CheckpointId = 7;
    static constexpr TStringBuf GraphId = "test-graph";
    TActorSystem ActorSystem;
    NActors::TActorId EdgeActor;
    std::unordered_map<ui64, NActors::TActorId> SrcEdgeActor; // ChannelId -> actor
    NActors::TActorId DstEdgeActor;
    NActors::TActorId CheckpointCoordinator;
    TMockCoordinatorState::TPtr CoordinatorState = MakeIntrusive<TMockCoordinatorState>();
    ui64 ConsumedCommits = 0;

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
    TMockSinkState::TPtr SinkState = MakeIntrusive<TMockSinkState>();

    TSyncComputeActorTestFixture(
            NDqProto::EDataTransportVersion transportVersion = NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0,
            bool isWide = false
        )
        : Alloc(__LOCATION__)
        , TypeEnv(Alloc)
        , MemInfo("Mem")
        , FunctionRegistry(NKikimr::NMiniKQL::CreateFunctionRegistry(&PrintBackTrace, NKikimr::NMiniKQL::CreateBuiltinRegistry(), false, {}))
        , HolderFactory(Alloc.Ref(), MemInfo, FunctionRegistry.Get())
        , Vb(HolderFactory)
        , IsWide(isWide)
        , TransportVersion(transportVersion)
    {
        NKikimr::EnableYDBBacktraceFormat();

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
        DstEdgeActor = ActorSystem.AllocateEdgeActor();
    }

    // Generates program that squares `id` column and passes `ts` column as is
    // ExprType for id column is generated by `typeMaker(ctx)`
    // ts has type Uint64
    void GenerateSquareProgram(NDqProto::TDqTask& task, std::function<const NYql::TTypeAnnotationNode*(TExprContext&)> typeMaker) {
        // TODO: parse sexpr from text and use automated type annotation
        auto& program = *task.MutableProgram();
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
    void GenerateEmptyProgram(NDqProto::TDqTask& task, std::function<const NYql::TTypeAnnotationNode*(TExprContext&)> typeMaker) {
        auto& program = *task.MutableProgram();
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
    auto AddDummyInputChannel(NDqProto::TTaskInput& input, ui64 channelId) {
        auto& channel = *input.AddChannels();
        input.MutableUnionAll(); // for side-effect
        channel.SetId(channelId);
        const auto& [srcEdgeActor, inserted] = SrcEdgeActor.try_emplace(channelId);
        if (inserted) {
            srcEdgeActor->second = ActorSystem.AllocateEdgeActor();
        }
        auto& chEndpoint = *channel.MutableSrcEndpoint();
        ActorIdToProto(srcEdgeActor->second, chEndpoint.MutableActorId());
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
        TDqChannelSettings settings = {
            .RowType = (IsWide ? static_cast<TType*>(WideRowType) : RowType),
            .HolderFactory = &HolderFactory,
            .ChannelId = channelId,
            .DstStageId = ThisStageId,
            .Level = TCollectStatsLevel::Profile,
            .TransportVersion = TransportVersion,
            .MaxStoredBytes = 100,
            .MaxChunkBytes = 100
        };

        return CreateDqOutputChannel(settings, logFunc);
    }

    auto AddDummyInputChannel(NDqProto::TDqTask& task, ui64 channelId) {
        auto& input = *task.AddInputs();
        return AddDummyInputChannel(input, channelId);
    }

    auto AddDummyInputChannels(NDqProto::TDqTask& task, ui64 baseChannelId, ui64 numChannels) {
        auto& input = *task.AddInputs();
        TVector<IDqOutputChannel::TPtr> fakeOutputs;

        for (; numChannels--; ++baseChannelId) {
            fakeOutputs.push_back(AddDummyInputChannel(input, baseChannelId));
        }

        return fakeOutputs;
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
        TDqChannelSettings settings = {
            .RowType = type,
            .HolderFactory = &HolderFactory,
            .ChannelId = channelId,
            .SrcStageId = ThisStageId,
            .Level = TCollectStatsLevel::Profile,
            .TransportVersion = TransportVersion,
            .MaxStoredBytes = 10_MB
        };

        return CreateDqInputChannel(settings, TypeEnv);
    }

    void AddMockSinkOutput(NDqProto::TDqTask& task) {
        auto& output = *task.AddOutputs();
        output.MutableSink()->SetType(MockSinkType);
    }

    auto CreateTaskRunnerActorFactory() {
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
        return [factory=factory](std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, const NDq::TDqTaskSettings& task, NDqProto::EDqStatsMode statsMode, const NDq::TLogFunc&) {
            return factory->Get(alloc, task, statsMode);
        };
    }

    auto CreateTestSyncComputeActor(NDqProto::TDqTask& task, TComputeMemoryLimits memoryLimits, NDqProto::EDqStatsMode statsMode = NDqProto::DQ_STATS_MODE_PROFILE) {
        TComputeRuntimeSettings runtimeSettings;
        runtimeSettings.StatsMode = statsMode;
        runtimeSettings.ReportStatsSettings = TReportStatsSettings{TDuration::Seconds(1), TDuration::Seconds(1)};
        auto actor = CreateDqComputeActor(
                EdgeActor,
                LogPrefix,
                &task,
                CreateAsyncIoFactory(SinkState),
                FunctionRegistry.Get(),
                runtimeSettings,
                memoryLimits,
                CreateTaskRunnerActorFactory(),
                {}
                );
        UNIT_ASSERT(actor);
        return ActorSystem.Register(actor);
    }

    auto CreateTestSyncComputeActor(NDqProto::TDqTask& task, NDqProto::EDqStatsMode statsMode = NDqProto::DQ_STATS_MODE_PROFILE) {
        TComputeMemoryLimits memoryLimits;
        memoryLimits.ChannelBufferSize = 1_MB;
        memoryLimits.MkqlLightProgramMemoryLimit = 40_MB;
        memoryLimits.MkqlHeavyProgramMemoryLimit = 60_MB;
        memoryLimits.MkqlProgramHardMemoryLimit = 80_MB;
        memoryLimits.MemoryQuotaManager = std::make_shared<TGuaranteeQuotaManager>(64_MB, 40_MB);
        return CreateTestSyncComputeActor(task, memoryLimits, statsMode);
    }

    TUnboxedValueBatch CreateRow(ui32 value, ui64 ts) {
        LOG_D("create " << value << " " << ts);
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

    // cb(TUnboxedValue& value, ui32 column) is called for each value in a row
    // cbWatermark(TInstant watermark) is called for each received watermark
    // beforeFinalAck() is called before sending final ack (when CA is still definitely alive)
    bool ReceiveData(
        std::function<bool(const NUdf::TUnboxedValue& val, ui32 column)> cb,
        std::function<void(TInstant)> cbWatermark,
        std::function<void()> beforeFinalAck,
        IDqInputChannel::TPtr dqInputChannel
    ) {
        auto ev = ActorSystem.GrabEdgeEvent<TEvDqCompute::TEvChannelData>({DstEdgeActor}, TDuration::Seconds(20));
        if (!ev) {
            throw yexception() << "Failed";
        }
        LOG_D("Got " << ev->Get()->Record.DebugString());
        auto& channelData = *ev->Get()->Record.MutableChannelData();

        TDqSerializedBatch data;
        data.Proto = std::move(*channelData.MutableData());
        dqInputChannel->Push(std::move(data));

        if (channelData.HasWatermark()) {
            const auto& watermarkRequest = channelData.GetWatermark();
            const auto watermark = TInstant::MicroSeconds(watermarkRequest.GetTimestampUs());
            dqInputChannel->Push(watermark);
        }

        if (channelData.GetFinished()) {
            dqInputChannel->Finish();
        }

        TUnboxedValueBatch batch;
        TMaybe<TInstant> watermark;
        const auto columns = IsWide ? static_cast<TMultiType*>(dqInputChannel->GetInputType())->GetElementsCount() : static_cast<TStructType*>(dqInputChannel->GetInputType())->GetMembersCount();
        while (dqInputChannel->Pop(batch, watermark)) {
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
                    LOG_D("/");
                    return true;
                })) {
                    return false;
                }
            }
            if (watermark) {
                cbWatermark(*watermark);
            }
        }
        if (dqInputChannel->IsFinished()) {
            beforeFinalAck();
        }
        if (!ev->Get()->Record.GetNoAck()) {
            auto ack = new TEvDqCompute::TEvChannelDataAck;
            ack->Record.SetChannelId(channelData.GetChannelId());
            ack->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
            ack->Record.SetFreeSpace(3123); // XXX simulates limited channel size
            ack->Record.SetFinish(channelData.GetFinished());
            ActorSystem.Send(ev->Sender, ev->Recipient, ack);
        }
        return !dqInputChannel->IsFinished();
    }

    void WaitForChannelDataAck(ui64 channelId, ui32 seqNo) {
        for (;;) {
            auto ev = ActorSystem.GrabEdgeEvent<TEvDqCompute::TEvChannelDataAck>(SrcEdgeActor[channelId]);
            LOG_D("Got ack " << ev->Get()->Record);
            UNIT_ASSERT_EQUAL(ev->Get()->Record.GetChannelId(), channelId);
            if (ev->Get()->Record.GetSeqNo() == seqNo) {
                break;
            }
            LOG_D("...but waiting for " << seqNo);
        }
    }

    void DumpMonPage(auto syncCA, auto hook) {
        TMockHttpRequest request;
        {
            auto evHttpInfo = MakeHolder<NActors::NMon::TEvHttpInfo>(request);
            ActorSystem.Send(syncCA, EdgeActor, evHttpInfo.Release());
        }
        {
            auto ev = ActorSystem.GrabEdgeEvent<NActors::NMon::TEvHttpInfoRes>({EdgeActor});
            UNIT_ASSERT_EQUAL(ev->Get()->GetContentType(), NActors::NMon::IEvHttpInfoRes::EContentType::Html);
            TStringStream out;
            ev->Get()->Output(out);
            hook(out.Str());
        }
    }

    static constexpr ui32 NoAckPeriod = 2;

    void SendData(
        std::function<std::tuple<IDqOutputChannel::TPtr, ui32*>(ui32, bool)> generator,
        NActors::TActorId syncCA,
        ui32 packets,
        bool waitIntermediateAcks
    ) {
        for (ui32 packet = 1; packet <= packets; ++packet) {
            bool isFinal = packet == packets;
            bool noAck = (packet % NoAckPeriod) == 0; // set noAck on even packets

            auto [dqOutputChannel, seqNo] = generator(packet, isFinal);
            if (isFinal) {
                dqOutputChannel->Finish();
            }

            auto evInputChannelData = MakeHolder<TEvDqCompute::TEvChannelData>();
            evInputChannelData->Record.SetSeqNo(++*seqNo);
            auto& chData = *evInputChannelData->Record.MutableChannelData();
            auto channelId = dqOutputChannel->GetChannelId();
            chData.SetChannelId(channelId);
            if (TDqSerializedBatch serializedBatch; dqOutputChannel->Pop(serializedBatch)) {
                *chData.MutableData() = serializedBatch.Proto;
                Y_ENSURE(serializedBatch.Payload.Empty()); // TODO
            }
            if (NDqProto::TWatermark watermark; dqOutputChannel->Pop(watermark)) {
                *chData.MutableWatermark() = watermark;
                noAck = false; // packet containing watermark must be acked
            }
            if (NDqProto::TCheckpoint checkpoint; dqOutputChannel->Pop(checkpoint)) {
                *chData.MutableCheckpoint() = checkpoint;
                noAck = false; // packet containing checkpoint must be acked
            }
            if (dqOutputChannel->IsFinished()) {
                chData.SetFinished(true);
                noAck = false; // final packet must be acked
            }
            evInputChannelData->Record.SetNoAck(noAck);
            LOG_D("Sending " << packet << "/" << packets << " "  << chData);
            ActorSystem.Send(syncCA, SrcEdgeActor[channelId], evInputChannelData.Release());
            if ((dqOutputChannel->IsFinished() || waitIntermediateAcks) && !noAck) {
                WaitForChannelDataAck(dqOutputChannel->GetChannelId(), *seqNo);
            }
        }
    }

    void SendWatermark(NActors::TActorId syncCA, ui64 channelId, TInstant watermark, bool finish, ui32* seqNo) {
        auto evInputChannelData = MakeHolder<TEvDqCompute::TEvChannelData>();
        evInputChannelData->Record.SetSeqNo(++*seqNo);
        auto& chData = *evInputChannelData->Record.MutableChannelData();
        chData.SetChannelId(channelId);
        chData.MutableWatermark()->SetTimestampUs(watermark.MicroSeconds());
        chData.SetFinished(finish);
        evInputChannelData->Record.SetNoAck(false);
        LOG_D("Sending WATERMARK " << chData);
        ActorSystem.Send(syncCA, SrcEdgeActor[channelId], evInputChannelData.Release());
        WaitForChannelDataAck(channelId, *seqNo);
    }

    void WaitForSinkFinished(TDuration timeout = TDuration::Seconds(10)) {
        const auto deadline = TInstant::Now() + timeout;
        while (!SinkState->Finished.load()) {
            UNIT_ASSERT_C(TInstant::Now() < deadline,
                "Compute actor has not finished sink in " << timeout << ": it is stuck");
            Sleep(TDuration::MilliSeconds(10));
        }
    }

    //
    // Checkpoint commit helpers.
    //

    NActors::TActorId StartCAForCheckpointing(NDqProto::TDqTask& task, bool withSink) {
        GenerateSquareProgram(task, [](TExprContext& ctx) {
            return ctx.MakeType<TDataExprType>(EDataSlot::Int32);
        });
        task.SetId(ThisTaskId);
        AddDummyInputChannel(task, InputChannelId);
        if (withSink) {
            AddMockSinkOutput(task);
        } else {
            AddDummyOutputChannel(task, OutputChannelId, RowType);
        }

        auto syncCA = CreateTestSyncComputeActor(task);
        ActorSystem.EnableScheduleForActor(syncCA, true);
        ActorSystem.GrabEdgeEvent<TEvDqCompute::TEvState>(EdgeActor); // SayHelloOnBootstrap
        return syncCA;
    }

    void WaitFor(std::function<bool()> predicate, const TString& what, TDuration timeout = TDuration::Seconds(10)) {
        const auto deadline = TInstant::Now() + timeout;
        while (!predicate()) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, what << " in " << timeout);
            Sleep(TDuration::MilliSeconds(10));
        }
    }

    void RegisterCheckpointCoordinator(NActors::TActorId syncCA, ui64 generation) {
        if (!CheckpointCoordinator) {
            CheckpointCoordinator = ActorSystem.Register(new TMockCheckpointCoordinator(CoordinatorState));
        }

        const ui64 acksBefore = CoordinatorState->Acks.load();
        ActorSystem.Send(syncCA, CheckpointCoordinator,
            new TEvDqCompute::TEvNewCheckpointCoordinator(generation, TString(GraphId)));
        WaitFor([this, acksBefore] { return CoordinatorState->Acks.load() > acksBefore; },
            TStringBuilder() << "compute actor did not acknowledge checkpoint coordinator generation " << generation);
    }

    void SendCommitState(NActors::TActorId syncCA, ui64 checkpointId, ui64 generation, std::optional<ui64> checkpointGeneration = std::nullopt) {
        LOG_D("Sending TEvCommitState " << generation << "." << checkpointId);
        ActorSystem.Send(syncCA, CheckpointCoordinator,
            new TEvDqCompute::TEvCommitState(checkpointId, checkpointGeneration.value_or(generation), generation));
    }

    void ExpectStateCommitted(ui64 checkpointId, ui64 generation) {
        WaitFor([this] { return CoordinatorState->Commits.load() > ConsumedCommits; },
            TStringBuilder() << "compute actor did not report checkpoint " << generation << "." << checkpointId
                << " as committed");

        const auto commitLog = CoordinatorState->GetCommitLog();
        UNIT_ASSERT_VALUES_EQUAL(commitLog.size(), ConsumedCommits + 1);
        const auto& commit = commitLog.back();
        UNIT_ASSERT_VALUES_EQUAL(commit.CheckpointId, checkpointId);
        UNIT_ASSERT_VALUES_EQUAL(commit.Generation, generation);
        UNIT_ASSERT_VALUES_EQUAL(commit.TaskId, ThisTaskId);
        ++ConsumedCommits;
    }

    void ExpectNoStateCommitted(TDuration settleTime = TDuration::Seconds(1)) {
        Sleep(settleTime);
        UNIT_ASSERT_VALUES_EQUAL_C(CoordinatorState->Commits.load(), ConsumedCommits,
            "compute actor reported a checkpoint as committed before all sinks acknowledged it");
    }

    void WaitForSinkCommitCalls(ui64 expected) {
        WaitFor([this, expected] { return SinkState->CommitStateCalls.load() >= expected; },
            TStringBuilder() << "compute actor did not call sink CommitState() " << expected << " time(s)");
    }

    void AckDeferredSinkCommits() {
        const auto sinkActors = SinkState->GetSinkActors();
        UNIT_ASSERT_C(!sinkActors.empty(), "no sink received CommitState()");
        for (const auto& sinkActor : sinkActors) {
            ActorSystem.Send(sinkActor, CheckpointCoordinator, new TEvMockSinkPrivate::TEvAckCommitState());
        }
    }

    void SendFinish(NActors::TActorId syncCA, IDqOutputChannel::TPtr dqOutputChannel, ui32* seqNo) {
        auto evInputChannelData = MakeHolder<TEvDqCompute::TEvChannelData>();
        evInputChannelData->Record.SetSeqNo(++*seqNo);
        auto& chData = *evInputChannelData->Record.MutableChannelData();
        auto channelId = dqOutputChannel->GetChannelId();
        chData.SetChannelId(channelId);
        chData.SetFinished(true);
        evInputChannelData->Record.SetNoAck(false);
        LOG_D("Sending FINISH " << chData);
        ActorSystem.Send(syncCA, SrcEdgeActor[channelId], evInputChannelData.Release());
        WaitForChannelDataAck(dqOutputChannel->GetChannelId(), *seqNo);
    }

#if 0 // TODO: switch when inputtransform will be fixed; just log for now
#define WEAK_UNIT_ASSERT_GT_C UNIT_ASSERT_GT_C
#define WEAK_UNIT_ASSERT_LE_C UNIT_ASSERT_LE_C
#define WEAK_UNIT_ASSERT_EQUAL_C UNIT_ASSERT_EQUAL_C
#define WEAK_UNIT_ASSERT UNIT_ASSERT
#else
#define WEAK_UNIT_ASSERT_GT_C(A, B, C) do { if (!((A) > (B))) LOG_E("Assert " #A " > " #B " failed " << C); } while(0)
#define WEAK_UNIT_ASSERT_LE_C(A, B, C) do { if (!((A) <= (B))) LOG_E("Assert " #A " <= " #B " failed " << C); } while(0)
#define WEAK_UNIT_ASSERT_EQUAL_C(A, B, C) do { if (!((A) == (B))) LOG_E("Assert " #A " == " #B " failed " << C); } while(0)
#define WEAK_UNIT_ASSERT(A) do { if (!(A)) LOG_E("Assert " #A " failed "); } while(0)
#endif
    struct TStartCAResult {
        NActors::TActorId SyncCA;
        TVector<IDqOutputChannel::TPtr> DqOutputChannels;
        IDqInputChannel::TPtr DqInputChannel;
    };

    TStartCAResult StartCA(
        ui32 packets,
        ui32 watermarkPeriod,
        bool waitIntermediateAcks,
        ui32 numChannels,
        NDqProto::EDqStatsMode statsMode = NDqProto::DQ_STATS_MODE_PROFILE,
        bool createSuspended = false
    ) {
        LogPrefix = TStringBuilder() << "Square Test for:"
           << " packets=" << packets
           << " watermarkPeriod=" << watermarkPeriod
           << " waitIntermediateAcks=" << waitIntermediateAcks
           << " channels=" << numChannels
           << " ";
        NDqProto::TDqTask task;
        GenerateSquareProgram(task, [](TExprContext& ctx) {
            return ctx.MakeType<TDataExprType>(EDataSlot::Int32);
        });
        auto dqOutputChannels = AddDummyInputChannels(task, InputChannelId, numChannels);
        auto dqInputChannel = AddDummyOutputChannel(task, OutputChannelId, (IsWide ? static_cast<TType*>(WideRowType) : RowType));
        task.SetCreateSuspended(createSuspended);

        auto syncCA = CreateTestSyncComputeActor(task, statsMode);
        ActorSystem.EnableScheduleForActor(syncCA, true);
        ActorSystem.GrabEdgeEvent<TEvDqCompute::TEvState>(EdgeActor);   // SayHelloOnBootstrap

        return {syncCA, std::move(dqOutputChannels), std::move(dqInputChannel)};
    }

    void BasicMultichannelTests(
        ui32 packets,
        ui32 watermarkPeriod,
        bool waitIntermediateAcks,
        ui32 numChannels,
        auto& rng,
        NDqProto::EDqStatsMode statsMode = NDqProto::DQ_STATS_MODE_PROFILE,
        bool createSuspended = false
    ) {
        auto [syncCA, dqOutputChannels, dqInputChannel] = StartCA(packets, watermarkPeriod, waitIntermediateAcks, numChannels, statsMode, createSuspended);

        ui32 val = 0;
        TMaybe<TInstant> expectedWatermark;
        TVector<ui32> seqNo(numChannels);
        TVector<ui64> activeChannels(numChannels);
        std::iota(activeChannels.begin(), activeChannels.end(), 0);
        SendData([&](ui32 packet, bool isFinal) {
            auto channelIdxIdx = rng() % activeChannels.size();
            std::swap(activeChannels[channelIdxIdx], activeChannels.back());
            auto channelIdx = activeChannels.back();
            auto dqOutputChannel = dqOutputChannels[channelIdx];
            PushRow(CreateRow(++val, packet), dqOutputChannel);
            PushRow(CreateRow(++val, packet), dqOutputChannel);
            PushRow(CreateRow(++val, packet), dqOutputChannel);
            if (watermarkPeriod && packet % watermarkPeriod == 0) {
                LOG_D("push watermark " << packet);
                NDqProto::TWatermark watermark;
                watermark.SetTimestampUs(TInstant::Seconds(packet).MicroSeconds());
                dqOutputChannel->Push(std::move(watermark));
                expectedWatermark = std::max(expectedWatermark, TMaybe<TInstant>(TInstant::Seconds(packet)));
            }
            if (isFinal || activeChannels.size() > 1 && rng() % std::max(packets/numChannels, ui32{1}) == 0) {
                // when we have more than one active channels left, we may randomly finish it midway
                dqOutputChannel->Finish();
                activeChannels.pop_back();
            }
            return std::pair { dqOutputChannel, &seqNo[channelIdx] };
        },
        syncCA, packets, waitIntermediateAcks);
        // Finish all unfinished channels (when we have more than one channel left, only one is forcibly finished on final packet)
        for (ui32 channelIdx = 0; channelIdx < numChannels; ++channelIdx) {
            auto dqOutputChannel = dqOutputChannels[channelIdx];
            if (dqOutputChannel->IsFinished()) {
                continue;
            }
            SendFinish(syncCA, dqOutputChannel, &seqNo[channelIdx]);
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
                            UNIT_ASSERT_GT_C(ts, watermark->Seconds(), ts << " >= " << watermark->Seconds());
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
                [this, &syncCA]() {
                    DumpMonPage(syncCA, [this](auto&& str) {
                        UNIT_ASSERT_STRING_CONTAINS(str, "<h3>Sources</h3>");
                        UNIT_ASSERT_STRING_CONTAINS(str, LogPrefix);
                        // TODO add validation
                        LOG_D(str);
                    });
                },
                dqInputChannel))
        {}

        UNIT_ASSERT_EQUAL_C(receivedData.size(), val, "expected size " << val << " != " << receivedData.size());
        for (; val > 0; --val) {
            UNIT_ASSERT_EQUAL_C(receivedData[val * val], 1, "expected count for " << (val * val));
        }
        if (expectedWatermark) {
            WEAK_UNIT_ASSERT(!!watermark);
            if (watermark) {
                UNIT_ASSERT_LE_C(*watermark, expectedWatermark, "Expected " << (*watermark) << " <= " << expectedWatermark);
                WEAK_UNIT_ASSERT_EQUAL_C(*watermark, expectedWatermark, "Expected " << (*watermark) << " == " << expectedWatermark << ", Watermark Delay is " << (*expectedWatermark - *watermark));
                LOG_D("Last watermark " << *watermark);
            } else {
                LOG_E("NO WATERMARK");
            }
        } else {
            UNIT_ASSERT(!watermark);
        }
    }

    void InputTransformMultichannelTests(ui32 packets, ui32 watermarkPeriod, bool waitIntermediateAcks, ui32 numChannels, auto& rng) {
        LogPrefix = TStringBuilder() << "InputTransform Test for:"
           << " packets=" << packets
           << " watermarkPeriod=" << watermarkPeriod
           << " waitIntermediateAcks=" << waitIntermediateAcks
           << " channels=" << numChannels
           << " ";
        NDqProto::TDqTask task;
        GenerateEmptyProgram(task, [](TExprContext& ctx) {
            auto keyType = ctx.MakeType<TDataExprType>(EDataSlot::Int32);
            auto tsType = ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
            auto valueType = ctx.MakeType<TDataExprType>(EDataSlot::String);
            auto structType = ctx.MakeType<TStructExprType>(
                    TVector<const TItemExprType*> {
                        ctx.MakeType<TItemExprType>("e.id", keyType),
                        ctx.MakeType<TItemExprType>("e.ts", tsType),
                        ctx.MakeType<TItemExprType>("u.data", ctx.MakeType<TOptionalExprType>(valueType)),
                        ctx.MakeType<TItemExprType>("u.key", ctx.MakeType<TOptionalExprType>(keyType)),
                    }
            );
            return structType;
        });
        TMap<i32, ui32> expectedData;
        auto dqOutputChannels = AddDummyInputChannels(task, InputChannelId, numChannels);
        auto dqInputChannel = AddDummyOutputChannel(task, OutputChannelId, (IsWide ? static_cast<TType*>(WideRowTransformedType) : RowTransformedType));
        SetInputTransform(task,
                TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv),
                TDataType::Create(NUdf::TDataType<char*>::Id, TypeEnv)
                );

        auto syncCA = CreateTestSyncComputeActor(task);
        ActorSystem.EnableScheduleForActor(syncCA, true);
        ActorSystem.GrabEdgeEvent<TEvDqCompute::TEvState>(EdgeActor);

        ui32 val = 0;
        TMaybe<TInstant> expectedWatermark;
        TVector<ui32> seqNo(numChannels);
        TVector<ui64> activeChannels(numChannels);
        std::iota(activeChannels.begin(), activeChannels.end(), 0);

        SendData([&](ui32 packet, bool isFinal) {
            auto channelIdxIdx = rng() % activeChannels.size();
            std::swap(activeChannels[channelIdxIdx], activeChannels.back());
            auto channelIdx = activeChannels.back();
            auto dqOutputChannel = dqOutputChannels[channelIdx];
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
            if (watermarkPeriod && packet % watermarkPeriod == 0) {
                NDqProto::TWatermark watermark;
                watermark.SetTimestampUs(TInstant::Seconds(packet).MicroSeconds());
                dqOutputChannel->Push(std::move(watermark));
                expectedWatermark = std::max(expectedWatermark, TMaybe<TInstant>(TInstant::Seconds(packet)));
            }
            if (isFinal || activeChannels.size() > 1 && rng() % std::max(packets/numChannels, ui32{1}) == 0) {
                // when we have more than one active channels left, we may randomly finish it midway
                dqOutputChannel->Finish();
                activeChannels.pop_back();
            }
            return std::pair { dqOutputChannel, &seqNo[channelIdx] };
        },
        syncCA, packets, waitIntermediateAcks);
        // Finish all unfinished channels (when we have more than one channel left, only one is forcibly finished on final packet)
        for (ui32 channelIdx = 0; channelIdx < numChannels; ++channelIdx) {
            auto dqOutputChannel = dqOutputChannels[channelIdx];
            if (dqOutputChannel->IsFinished()) {
                continue;
            }
            SendFinish(syncCA, dqOutputChannel, &seqNo[channelIdx]);
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
                        LOG_D(column << " id = " << val.Get<i32>());
                        col0 = val.Get<i32>();
                        ++receivedData[val.Get<i32>()];
                    } else if (columnName == "e.ts") {
                        UNIT_ASSERT(!!val);
                        UNIT_ASSERT(val.IsEmbedded());
                        auto ts = val.Get<ui64>();
                        LOG_D(column << " ts = " << ts);
                        if (watermark) {
                            UNIT_ASSERT_GT_C(ts, watermark->Seconds(), "Timestamp " << ts << " before watermark: " << watermark->Seconds());
                        }
                    } else if (columnName == "u.key") {
                        if (col0 >= MinTransformedValue && col0 <= MaxTransformedValue) {
                            UNIT_ASSERT(!!val);
                            auto cval = val.GetOptionalValue();
                            UNIT_ASSERT(!!cval);
                            UNIT_ASSERT(cval.IsEmbedded());
                            auto data = cval.Get<i32>();
                            LOG_D(column << " key = " << data);
                            UNIT_ASSERT_EQUAL_C(data, col0, data << "!=" << col0);
                        } else {
                            UNIT_ASSERT_C(!val, "null (1) expected for " << col0);
                            LOG_D(column << " key IS NULL");
                        }
                    } else if (columnName == "u.data") {
                        if (col0 >= MinTransformedValue && col0 <= MaxTransformedValue) {
                            UNIT_ASSERT(!!val);
                            const auto cval = val.GetOptionalValue();
                            UNIT_ASSERT(!!cval);
                            auto ref = TString(cval.AsStringRef());
                            LOG_D(column << " data = '" << ref << "'");
                            UNIT_ASSERT_EQUAL(ref, ToString(col0));
                        } else {
                            UNIT_ASSERT_C(!val, "null (2) expected for " << col0);
                            LOG_D(column << " data IS NULL");
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
                [this, &syncCA]() {
                    DumpMonPage(syncCA, [this](auto&& str) {
                        UNIT_ASSERT_STRING_CONTAINS(str, "<h3>Sources</h3>");
                        UNIT_ASSERT_STRING_CONTAINS(str, LogPrefix);
                        // TODO add validation
                        LOG_D(str);
                    });
                },
                dqInputChannel))
        {}
        UNIT_ASSERT_EQUAL_C(receivedData.size(), expectedData.size(), "received " << receivedData.size() << " != expected " << expectedData.size());
        for (auto [receivedVal, receivedCnt] : receivedData) {
            UNIT_ASSERT_EQUAL_C(receivedCnt, expectedData[receivedVal], "expected count for " << receivedVal << ": " << receivedCnt << " != " << expectedData[receivedVal]);
        }
        if (expectedWatermark) {
            WEAK_UNIT_ASSERT(!!watermark);
            if (watermark) {
                UNIT_ASSERT_LE_C(*watermark, expectedWatermark, "Expected " << (*watermark) << " <= " << expectedWatermark);
                WEAK_UNIT_ASSERT_EQUAL_C(*watermark, expectedWatermark, "Expected " << (*watermark) << " == " << expectedWatermark << ", Watermark Delay is " << (*expectedWatermark - *watermark));
                LOG_D("Last watermark " << *watermark);
            } else {
                LOG_E("NO WATERMARK");
            }
        } else {
            UNIT_ASSERT(!watermark);
        }
    }

    auto GetRandomSeed() {
        uint32_t seed = 0; // by default tests are reproducible (fixed-seed PRNG)
        if (auto env = getenv("RANDOM_SEED")) {
            if (*env) {
                // with non-empty $RANDOM_SEED use it as seed (to reproduce random test failures)
                seed = ::FromString<uint32_t>(env);
            } else {
                // with empty $RANDOM_SEED make tests truly random
                seed = (std::random_device {})();
                Cerr << "RANDOM_SEED=" << seed << Endl;
            }
        }
        return seed;
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TSyncComputeActorTest) {
    Y_UNIT_TEST_F(Empty, TSyncComputeActorTestFixture) { }

    Y_UNIT_TEST_F(Basic, TSyncComputeActorTestFixture) {
        TVector<ui32> sizes{ 1, 2, 3, 4, 5, 51, 128, 251 };
        auto seed = GetRandomSeed();
        std::mt19937 rng(seed);
        for (ui32 t = 0; t < (TESTS_LARGE ? 32 : 16); ++t) sizes.push_back(1 + rng() % 734);
        for (bool waitIntermediateAcks : { false, true }) {
            for (ui32 watermarkPeriod : { 0, 1, 3 }) {
                for (ui32 packets : sizes) {
                    for (ui32 numChannels : { 1, 3, 7, 16 }) {
                        std::mt19937 trng(seed);
                        BasicMultichannelTests(packets, watermarkPeriod, waitIntermediateAcks, numChannels, trng);
                    }
                }
            }
        }
    }

    Y_UNIT_TEST_F(StatsMode, TSyncComputeActorTestFixture) {
        for (auto statsMode : {
                NDqProto::DQ_STATS_MODE_NONE,
                NDqProto::DQ_STATS_MODE_BASIC,
                NDqProto::DQ_STATS_MODE_FULL,
                NDqProto::DQ_STATS_MODE_PROFILE,
                }) {
            std::mt19937 rng;
            BasicMultichannelTests(5, 1, true, 1, rng, statsMode);
        }
    }

    Y_UNIT_TEST_F(InputTransformMultichannel, TSyncComputeActorTestFixture) {
        TVector<ui32> sizes{ 1, 2, 3, 4, 5, 51, 128, 251 };
        std::mt19937 rng(GetRandomSeed());
        for (ui32 t = 0; t < (TESTS_LARGE ? 32 : 8) ; ++t) sizes.push_back(1 + rng() % 734);
        for (ui32 numChannels: { 1, 2, 11 }) {
            for (bool waitIntermediateAcks : { false, true }) {
                for (ui32 watermarkPeriod : { 0, 1, 3 }) {
                    for (ui32 packets : sizes) {
                        InputTransformMultichannelTests(packets, watermarkPeriod, waitIntermediateAcks, numChannels, rng);
                    }
                }
            }
        }
    }

    // Reproduces hang of compute actor after sending a watermark into a sink (async output).
    //
    // Watermark with no rows behind it is the only thing in the sink buffer, so
    // SendDataChunkToAsyncOutput() pops it, finds neither data nor checkpoint and bails out early.
    // Task runner has returned PendingOutput for that run (it emits a watermark and asks CA to
    // send it before continuing with the input), and CheckRunStatus() only reschedules execution
    // on PendingOutput when ProcessOutputsState.DataWasSent is set. When the popped watermark is
    // not accounted as sent data, nothing reschedules the run: input is already fully buffered in
    // the CA, so no external event is expected either.
    //
    // Before the fix: CA never runs again, so it never finishes and the test times out.
    // After the fix: watermark counts as sent data, CA resumes, sees finished input and finishes.
    Y_UNIT_TEST_F(WatermarkToSinkResumesExecution, TSyncComputeActorTestFixture) {
        LogPrefix = "Watermark to sink test: ";
        NDqProto::TDqTask task;
        GenerateSquareProgram(task, [](TExprContext& ctx) {
            return ctx.MakeType<TDataExprType>(EDataSlot::Int32);
        });
        AddDummyInputChannel(task, InputChannelId);
        AddMockSinkOutput(task);

        auto syncCA = CreateTestSyncComputeActor(task);
        ActorSystem.EnableScheduleForActor(syncCA, true);
        ActorSystem.GrabEdgeEvent<TEvDqCompute::TEvState>(EdgeActor); // SayHelloOnBootstrap

        ui32 seqNo = 0;
        SendWatermark(syncCA, InputChannelId, TInstant::Seconds(1), /* finish */ true, &seqNo);

        WaitForSinkFinished();
        UNIT_ASSERT_EQUAL_C(SinkState->Rows.load(), 0, "no rows were sent to the task, got " << SinkState->Rows.load());
        // the only expected send is the finishing one: watermark itself is not passed to the sink
        UNIT_ASSERT_EQUAL_C(SinkState->SendDataCalls.load(), 1, "unexpected number of sends: " << SinkState->SendDataCalls.load());
    }

    Y_UNIT_TEST_F(DataWithWatermarkToSink, TSyncComputeActorTestFixture) {
        LogPrefix = "Data with watermark to sink test: ";
        NDqProto::TDqTask task;
        GenerateSquareProgram(task, [](TExprContext& ctx) {
            return ctx.MakeType<TDataExprType>(EDataSlot::Int32);
        });
        auto dqOutputChannel = AddDummyInputChannel(task, InputChannelId);
        AddMockSinkOutput(task);

        auto syncCA = CreateTestSyncComputeActor(task);
        ActorSystem.EnableScheduleForActor(syncCA, true);
        ActorSystem.GrabEdgeEvent<TEvDqCompute::TEvState>(EdgeActor); // SayHelloOnBootstrap

        constexpr ui32 rows = 3;
        ui32 seqNo = 0;
        SendData([&](ui32 packet, bool /* isFinal */) {
            for (ui32 row = 1; row <= rows; ++row) {
                PushRow(CreateRow(row, packet), dqOutputChannel);
            }
            NDqProto::TWatermark watermark;
            watermark.SetTimestampUs(TInstant::Seconds(packet).MicroSeconds());
            dqOutputChannel->Push(std::move(watermark));
            return std::pair { dqOutputChannel, &seqNo };
        },
        syncCA, /* packets */ 1, /* waitIntermediateAcks */ true);

        WaitForSinkFinished();
        UNIT_ASSERT_EQUAL_C(SinkState->Rows.load(), rows, "expected " << rows << " rows, got " << SinkState->Rows.load());
    }

    Y_UNIT_TEST_F(StreamingQuerySendStatsWithCreateSuspended, TSyncComputeActorTestFixture) {
        auto [syncCA, dqOutputChannels, dqInputChannel] = StartCA(5, 1, true, 1, NDqProto::DQ_STATS_MODE_PROFILE, true);
        auto ev = ActorSystem.GrabEdgeEvent<TEvDqCompute::TEvState>(EdgeActor, TDuration::Seconds(5));
        UNIT_ASSERT(ev);
        UNIT_ASSERT(ev->Get()->Record.HasStats());
    }

    // Reproduces crash: kqp_query_control_plane.cpp always creates TMemoryQuotaManager with
    // MkqlLightProgramMemoryLimit, but map-join tasks request MkqlHeavyProgramMemoryLimit
    // in CalcMkqlMemoryLimit(). When the RM is under pressure and AllocateExtraQuota returns
    // false, TDqMemoryQuota constructor hits Y_ABORT_UNLESS -> process crash.
    //
    // Before the fix: this test aborts the process.
    // After the fix: the actor is created successfully and sends TEvState.
    Y_UNIT_TEST_F(MapJoinTaskWithExhaustedQuotaManager, TSyncComputeActorTestFixture) {
        NDqProto::TDqTask task;
        GenerateSquareProgram(task, [](TExprContext& ctx) {
            return ctx.MakeType<TDataExprType>(EDataSlot::Int32);
        });
        // HasMapJoin=true makes CalcMkqlMemoryLimit() return MkqlHeavyProgramMemoryLimit
        task.MutableProgram()->MutableSettings()->SetHasMapJoin(true);
        AddDummyInputChannels(task, InputChannelId, 1);
        AddDummyOutputChannel(task, OutputChannelId, RowType);

        TComputeMemoryLimits memoryLimits;
        memoryLimits.ChannelBufferSize = 1_MB;
        memoryLimits.MkqlLightProgramMemoryLimit = 40_MB;
        memoryLimits.MkqlHeavyProgramMemoryLimit = 60_MB;
        memoryLimits.MkqlProgramHardMemoryLimit = 80_MB;
        // Quota manager funded with lightLimit only — simulates kqp_query_control_plane.cpp:315.
        // AllocateExtraQuota(heavyLimit - lightLimit) returns false -> Y_ABORT_UNLESS fires.
        memoryLimits.MemoryQuotaManager = std::make_shared<TGuaranteeQuotaManager>(40_MB, 40_MB);

        auto syncCA = CreateTestSyncComputeActor(task, memoryLimits);
        ActorSystem.EnableScheduleForActor(syncCA, true);
        ActorSystem.GrabEdgeEvent<TEvDqCompute::TEvState>(EdgeActor);
    }
}

Y_UNIT_TEST_SUITE(TSyncComputeActorCheckpointsTest) {
    Y_UNIT_TEST_F(CheckpointCommitAcknowledgedBySink, TSyncComputeActorTestFixture) {
        LogPrefix = "Checkpoint commit (sync sink): ";
        NDqProto::TDqTask task;
        auto syncCA = StartCAForCheckpointing(task, /* withSink */ true);

        RegisterCheckpointCoordinator(syncCA, CoordinatorGeneration);
        SendCommitState(syncCA, CheckpointId, CoordinatorGeneration);

        ExpectStateCommitted(CheckpointId, CoordinatorGeneration);
        UNIT_ASSERT_VALUES_EQUAL_C(SinkState->CommitStateCalls.load(), 1,
            "sink must be asked to commit exactly once per checkpoint");
    }

    Y_UNIT_TEST_F(CheckpointCommitWaitsForAsyncSink, TSyncComputeActorTestFixture) {
        LogPrefix = "Checkpoint commit (async sink): ";
        SinkState->DeferCommit.store(true);

        NDqProto::TDqTask task;
        auto syncCA = StartCAForCheckpointing(task, /* withSink */ true);

        RegisterCheckpointCoordinator(syncCA, CoordinatorGeneration);
        SendCommitState(syncCA, CheckpointId, CoordinatorGeneration);

        WaitForSinkCommitCalls(1);
        ExpectNoStateCommitted();

        AckDeferredSinkCommits();
        ExpectStateCommitted(CheckpointId, CoordinatorGeneration);
    }

    Y_UNIT_TEST_F(CheckpointCommitWithoutSinks, TSyncComputeActorTestFixture) {
        LogPrefix = "Checkpoint commit (no sinks): ";
        NDqProto::TDqTask task;
        auto syncCA = StartCAForCheckpointing(task, /* withSink */ false);

        RegisterCheckpointCoordinator(syncCA, CoordinatorGeneration);
        SendCommitState(syncCA, CheckpointId, CoordinatorGeneration);

        ExpectStateCommitted(CheckpointId, CoordinatorGeneration);
        UNIT_ASSERT_VALUES_EQUAL(SinkState->CommitStateCalls.load(), 0);
    }

    Y_UNIT_TEST_F(CheckpointCommitTwiceInARow, TSyncComputeActorTestFixture) {
        LogPrefix = "Checkpoint commit (two checkpoints): ";
        SinkState->DeferCommit.store(true);

        NDqProto::TDqTask task;
        auto syncCA = StartCAForCheckpointing(task, /* withSink */ true);

        RegisterCheckpointCoordinator(syncCA, CoordinatorGeneration);

        SendCommitState(syncCA, CheckpointId, CoordinatorGeneration);
        WaitForSinkCommitCalls(1);
        AckDeferredSinkCommits();
        ExpectStateCommitted(CheckpointId, CoordinatorGeneration);

        SendCommitState(syncCA, CheckpointId + 1, CoordinatorGeneration);
        WaitForSinkCommitCalls(2);
        ExpectNoStateCommitted();
        AckDeferredSinkCommits();
        ExpectStateCommitted(CheckpointId + 1, CoordinatorGeneration);
    }

    Y_UNIT_TEST_F(CheckpointCommitDroppedOnNewCoordinator, TSyncComputeActorTestFixture) {
        LogPrefix = "Checkpoint commit (stale coordinator): ";
        SinkState->DeferCommit.store(true);

        NDqProto::TDqTask task;
        auto syncCA = StartCAForCheckpointing(task, /* withSink */ true);

        RegisterCheckpointCoordinator(syncCA, CoordinatorGeneration);
        SendCommitState(syncCA, CheckpointId, CoordinatorGeneration);
        WaitForSinkCommitCalls(1);

        // New coordinator takes over while the commit is still pending.
        RegisterCheckpointCoordinator(syncCA, CoordinatorGeneration + 1);

        // Acknowledgement of the dropped commit: stale generation, must not be answered.
        AckDeferredSinkCommits();
        ExpectNoStateCommitted();

        // The new coordinator commits from scratch.
        SinkState->DeferCommit.store(false);
        SendCommitState(syncCA, CheckpointId + 1, CoordinatorGeneration + 1);
        WaitForSinkCommitCalls(2);
        ExpectStateCommitted(CheckpointId + 1, CoordinatorGeneration + 1);
    }

    Y_UNIT_TEST_F(ChangeCheckpointCoordinatorAndStartNewCommit, TSyncComputeActorTestFixture) {
        LogPrefix = "Change coordinator + commit during stale checkpoint commit (async sink): ";
        SinkState->DeferCommit.store(true);

        NDqProto::TDqTask task;
        auto syncCA = StartCAForCheckpointing(task, /* withSink */ true);

        RegisterCheckpointCoordinator(syncCA, CoordinatorGeneration);
        SendCommitState(syncCA, CheckpointId, CoordinatorGeneration);

        WaitForSinkCommitCalls(1);
        ExpectNoStateCommitted();

        RegisterCheckpointCoordinator(syncCA, CoordinatorGeneration + 1);

        // Test that state commit under new coordinator (while previous is inflight) works
        SendCommitState(syncCA, CheckpointId, CoordinatorGeneration + 1);
        WaitForSinkCommitCalls(2);

        AckDeferredSinkCommits();
        ExpectStateCommitted(CheckpointId, CoordinatorGeneration + 1);
    }

    Y_UNIT_TEST_F(StaleCheckpointCommitForAsyncSink, TSyncComputeActorTestFixture) {
        LogPrefix = "Stale checkpoint commit (async sink): ";
        SinkState->DeferCommit.store(true);

        NDqProto::TDqTask task;
        auto syncCA = StartCAForCheckpointing(task, /* withSink */ true);

        RegisterCheckpointCoordinator(syncCA, CoordinatorGeneration);
        SendCommitState(syncCA, CheckpointId, CoordinatorGeneration, CoordinatorGeneration - 1); // Emulate restoring from pending commit checkpoint

        WaitForSinkCommitCalls(1);
        ExpectNoStateCommitted();

        AckDeferredSinkCommits();
        ExpectStateCommitted(CheckpointId, CoordinatorGeneration - 1);
    }
}

} //namespace NYql::NDq
