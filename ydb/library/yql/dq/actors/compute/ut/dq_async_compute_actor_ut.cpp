#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_async_compute_actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_channels.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_log.h>
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


using namespace NActors;

namespace NYql::NDq {

namespace {
static const bool TESTS_VERBOSE = getenv("TESTS_VERBOSE") != nullptr;
#define LOG_D(stream) LOG_DEBUG_S(*ActorSystem.SingleSys(), NKikimrServices::KQP_COMPUTE, LogPrefix << stream);

struct TActorSystem: NActors::TTestActorRuntimeBase {
    TActorSystem()
        : NActors::TTestActorRuntimeBase(1, true)
    {}

    void Start()
    {
        SetDispatchTimeout(TDuration::Seconds(5));
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

struct TDummyMemoryQuotaManager: IMemoryQuotaManager {
    bool AllocateQuota(ui64 /*memorySize*/) override { return true; }
    void FreeQuota(ui64 /*memorySize*/) override { }
    ui64 GetCurrentQuota() const override { return ~0u; }
    ui64 GetMaxMemorySize() const override { return ~0u; }
    bool IsReasonableToUseSpilling() const override { return false; }
    TString MemoryConsumptionDetails() const override { return "No details"; }
};

struct TAsyncCATestFixture: public NUnitTest::TBaseFixture {
    static constexpr ui64 InputChannelId = 1;
    static constexpr ui64 OutputChannelId = 2;
    static constexpr ui32 InputStageId = 123;
    static constexpr ui32 ThisStageId = 456;
    static constexpr ui32 OutputStageId = 789;
    static constexpr ui32 InputTaskId = 1;
    static constexpr ui32 ThisTaskId = 2;
    static constexpr ui32 OutputTaskId = 3;
    static constexpr ui32 Columns = 1;
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
        TVector<TStructMember> members;
        members.emplace_back("key", TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv));
        RowType = TStructType::Create(members.size(), members.data(), TypeEnv);
        TVector<TType*> components;
        for (ui32 i = 0; i < RowType->GetMembersCount(); ++i) {
            components.push_back(RowType->GetMemberType(i));
        }
        WideRowType = TMultiType::Create(components.size(), components.data(), TypeEnv);
    }

    void SetUp(NUnitTest::TTestContext& /* context */) override {
        ActorSystem.Start();

        EdgeActor = ActorSystem.AllocateEdgeActor();
        SrcEdgeActor = ActorSystem.AllocateEdgeActor();
        DstEdgeActor = ActorSystem.AllocateEdgeActor();
    }

    void GenerateProgram(NDqProto::TDqTask& task) {
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
                            .Name().Build("key")
                            .Value<TCoMul>()
                                .Left<TCoMember>()
                                    .Name().Build("key")
                                    .Struct("val")
                                .Build()
                                .Right<TCoMember>()
                                    .Name().Build("key")
                                    .Struct("val")
                                .Build()
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Done();
        auto int32Type = ctx.MakeType<TDataExprType>(EDataSlot::Int32);
        auto inStructType = ctx.MakeType<TStructExprType>(
            TVector<const TItemExprType*> {
                ctx.MakeType<TItemExprType>("key", int32Type)
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
                        coMul.Ptr()->SetTypeAnn(int32Type);
                        coMul.Left().Ptr()->SetTypeAnn(int32Type);
                        coMul.Left().Cast<TCoMember>().Struct().Ptr()->SetTypeAnn(inStructType);
                        coMul.Right().Ptr()->SetTypeAnn(int32Type);
                        coMul.Right().Cast<TCoMember>().Struct().Ptr()->SetTypeAnn(inStructType);
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
        // InMemory
        // EnableSpilling
        TLogFunc logFunc = [this](const TString& msg) {
            LOG_D(msg);
        };
        // DqOutputChannel is used for simulating input on CA under the test
        TDqOutputChannelSettings settings;
        settings.TransportVersion = TransportVersion;
        settings.MutableSettings.IsLocalChannel = true;
        return CreateDqOutputChannel(channelId, ThisStageId,
                (IsWide ? (TType*)WideRowType : (TType *)RowType), HolderFactory,
                settings,
                logFunc);
    }

    auto AddDummyOutputChannel(NDqProto::TDqTask& task, ui64 channelId) {
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
        // TransportVersion
        // SrcEndpoint
        // DstEndpoint
        // IsPersistent
        // EnableSpilling
        return CreateDqInputChannel(channelId, ThisStageId, (IsWide ? (TType*)WideRowType : (TType *)RowType), 10_MB, TCollectStatsLevel::None, TypeEnv, HolderFactory, TransportVersion);
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
        memoryLimits.MemoryQuotaManager = std::make_shared<TDummyMemoryQuotaManager>();
        auto actor = CreateDqAsyncComputeActor(
                EdgeActor, // executerId,
                LogPrefix,
                &task, // NYql::NDqProto::TDqTask* task,
                {}, // IDqAsyncIoFactory::TPtr asyncIoFactory,
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

    TUnboxedValueBatch CreateRow(ui32 value) {
        if (IsWide) {
            TUnboxedValueBatch result(WideRowType);
            result.PushRow([&]([[maybe_unused]] ui32 idx) {
                return NUdf::TUnboxedValuePod(value);
            });
            return result;
        }
        NUdf::TUnboxedValue* items;
        auto row = Vb.NewArray(RowType->GetMembersCount(), items);
        items[0] = NUdf::TUnboxedValuePod(value);
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

    bool ReceiveData(auto&& cb, auto dqInputChannel) {
        auto ev = ActorSystem.GrabEdgeEvent<TEvDqCompute::TEvChannelData>({DstEdgeActor});
        LOG_D("Got " << ev->Get()->Record.DebugString());
        TDqSerializedBatch sbatch;
        sbatch.Proto = ev->Get()->Record.GetChannelData().GetData();
        dqInputChannel->Push(std::move(sbatch));
        if (ev->Get()->Record.GetChannelData().GetFinished()) {
            dqInputChannel->Finish();
        }
        TUnboxedValueBatch batch;
        while (dqInputChannel->Pop(batch)) {
            if (IsWide) {
                if (!batch.ForEachRowWide([this, cb](const NUdf::TUnboxedValue row[], ui32 width) {
                    LOG_D("WideRow:");
                    if (row) {
                        UNIT_ASSERT_EQUAL(width, Columns);
                        for(ui32 col = 0; col < width; ++col) {
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
                if (!batch.ForEachRow([this, cb](const NUdf::TUnboxedValue& row) {
                    LOG_D("Row:");
                    if (row) {
                        for(ui32 col = 0; col < Columns; ++col) {
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

    void BasicTests(ui32 packets, bool doWatermark, bool waitIntermediateAcks) {
        LogPrefix = TStringBuilder() << "Test for:"
           << " packets=" << packets
           << " doWatermark=" << doWatermark
           << " waitIntermediateAcks=" << waitIntermediateAcks
           << " ";
        NDqProto::TDqTask task;
        GenerateProgram(task);
        auto dqOutputChannel = AddDummyInputChannel(task, InputChannelId);
        auto dqInputChannel = AddDummyOutputChannel(task, OutputChannelId);

        auto asyncCA = CreateTestAsyncCA(task);
        ActorSystem.EnableScheduleForActor(asyncCA, true);
        ui32 seqNo = 0;
        ui32 val = 0;
        for (ui32 packet = 1; packet <= packets; ++packet) {
            bool isFinal = packet == packets;
            bool noAck = (packet % 2) == 0; // set noAck on even packets

            PushRow(CreateRow(++val), dqOutputChannel);
            PushRow(CreateRow(++val), dqOutputChannel);
            PushRow(CreateRow(++val), dqOutputChannel);
            if (doWatermark) {
                NDqProto::TWatermark watermark;
                watermark.SetTimestampUs((ui64)1'000'000 * packet);
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
        while (ReceiveData(
                [this, &receivedData](const NUdf::TUnboxedValue& val, ui32 column) {
                    UNIT_ASSERT_EQUAL(column, 0);
                    UNIT_ASSERT(!!val);
                    UNIT_ASSERT(val.IsEmbedded());
                    LOG_D(val.Get<ui32>());
                    ++receivedData[val.Get<ui32>()];
                    return true;
                },
                dqInputChannel))
        {}
        UNIT_ASSERT_EQUAL(receivedData.size(), val);
        for (; val > 0; --val) {
            UNIT_ASSERT_EQUAL_C(receivedData[val * val], 1, "expected count for " << (val * val));
        }
    }
};

} //namespace anonymous

Y_UNIT_TEST_SUITE(TAsyncComputeActorTest) {
    Y_UNIT_TEST_F(Empty, TAsyncCATestFixture) { }

    Y_UNIT_TEST_F(Basic, TAsyncCATestFixture) {
        for (bool waitIntermediateAcks: { false, true }) {
            for (bool doWatermark: { false, true }) {
                for (ui32 packets: { 1, 2, 3, 4, 5 }) {
                    BasicTests(packets, doWatermark, waitIntermediateAcks);
                }
            }
        }
    }
}

} //namespace NYql::NDq

