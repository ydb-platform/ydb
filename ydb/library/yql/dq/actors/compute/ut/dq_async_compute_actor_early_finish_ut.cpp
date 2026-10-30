#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_async_compute_actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_channels.h>
#include <ydb/library/yql/providers/dq/task_runner_actor/task_runner_actor.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NYql::NDq {
namespace {

using namespace NActors;
using namespace NKikimr::NMiniKQL;
using namespace NTaskRunnerActor;

// A producer blocked on output until its consumer explicitly closes the channel.
class TOutputChannelMock : public NTaskRunnerProxy::IOutputChannel {
public:
    NDqProto::TPopResponse Pop(TDqSerializedBatch& data) override {
        NDqProto::TPopResponse response;
        if (!Finished && !DataSent) {
            data.Proto.SetTransportVersion(NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0);
            data.Proto.SetRows(1);
            data.Proto.SetRaw("data");
            response.SetResult(true);
            DataSent = true;
        }
        return response;
    }

    bool IsFinished() const override { return Finished; }
    void Finish() override { Finished = true; ++FinishCalls; }

    bool Finished = false;
    bool DataSent = false;
    ui32 FinishCalls = 0;
};

class TTaskRunnerMock : public NTaskRunnerProxy::ITaskRunner {
public:
    TTaskRunnerMock(TScopedAlloc& alloc, TTypeEnvironment& env, THolderFactory& holders)
        : Alloc(alloc), Env(env), Holders(holders)
    {}

    ui64 GetTaskId() const override { return 1; }
    NDqProto::TPrepareResponse Prepare(const TDqTaskRunnerMemoryLimits&) override { return {}; }
    NDqProto::TRunResponse Run() override {
        NDqProto::TRunResponse response;
        response.SetResult(static_cast<ui32>(Output->Finished ? ERunStatus::Finished : ERunStatus::PendingOutput));
        return response;
    }
    NTaskRunnerProxy::IInputChannel::TPtr GetInputChannel(ui64) override { Y_ABORT("Unexpected input"); }
    NTaskRunnerProxy::IOutputChannel::TPtr GetOutputChannel(ui64) override { return Output; }
    IDqAsyncInputBuffer* GetSource(ui64) override { Y_ABORT("Unexpected source"); }
    IDqAsyncOutputBuffer::TPtr GetSink(ui64) override { Y_ABORT("Unexpected sink"); }
    const THashMap<TString, TString>& GetTaskParams() const override { return Params; }
    const TVector<TString>& GetReadRanges() const override { return ReadRanges; }
    const THashMap<TString, TString>& GetSecureParams() const override { return Params; }
    const TTypeEnvironment& GetTypeEnv() const override { return Env; }
    const THolderFactory& GetHolderFactory() const override { return Holders; }
    TGuard<TScopedAlloc> BindAllocator(TMaybe<ui64>) override { return TGuard<TScopedAlloc>(Alloc); }
    bool IsAllocatorAttached() override { return false; }
    i32 GetProtocolVersion() override { return 1; }
    TStatus GetStatus() override { return {0, {}}; }

    TIntrusivePtr<TOutputChannelMock> Output = MakeIntrusive<TOutputChannelMock>();

private:
    TScopedAlloc& Alloc;
    TTypeEnvironment& Env;
    THolderFactory& Holders;
    THashMap<TString, TString> Params;
    TVector<TString> ReadRanges;
};

class TProxyFactoryMock : public NTaskRunnerProxy::IProxyFactory {
public:
    explicit TProxyFactoryMock(TIntrusivePtr<TTaskRunnerMock> runner)
        : Runner(std::move(runner))
    {}

    NTaskRunnerProxy::ITaskRunner::TPtr GetOld(std::shared_ptr<TScopedAlloc>, const TDqTaskSettings&, const TString&) override {
        return Runner;
    }
    TIntrusivePtr<IDqTaskRunner> Get(std::shared_ptr<TScopedAlloc>, const TDqTaskSettings&, NDqProto::EDqStatsMode, const TString&) override {
        Y_ABORT("Unexpected direct task runner");
    }

private:
    TIntrusivePtr<TTaskRunnerMock> Runner;
};

class TTestRuntime : public TTestActorRuntimeBase {
public:
    using TTestActorRuntimeBase::InitNodes;
    using TTestActorRuntimeBase::CleanupNodes;
};

struct TEarlyFinishFixture : NUnitTest::TBaseFixture {
    static constexpr ui64 ChannelId = 10;
    TScopedAlloc Alloc{__LOCATION__};
    TTypeEnvironment Env{Alloc};
    TMemoryUsageInfo MemInfo{"Test"};
    TIntrusivePtr<IFunctionRegistry> Registry{CreateFunctionRegistry(CreateBuiltinRegistry())};
    THolderFactory Holders{Alloc.Ref(), MemInfo, Registry.Get()};
    TIntrusivePtr<TTaskRunnerMock> Runner{MakeIntrusive<TTaskRunnerMock>(Alloc, Env, Holders)};
    TTestRuntime Runtime;
    TActorId Edge;
    TActorId Consumer;

    void SetUp(NUnitTest::TTestContext&) override {
        Runtime.SetDispatchTimeout(TDuration::Seconds(5));
        Runtime.InitNodes();
        Runtime.AppendToLogSettings(
            NKikimrServices::EServiceKikimr_MIN,
            NKikimrServices::EServiceKikimr_MAX,
            NKikimrServices::EServiceKikimr_Name<NActors::NLog::EComponent>);
        Edge = Runtime.AllocateEdgeActor();
        Consumer = Runtime.AllocateEdgeActor();
    }

    ~TEarlyFinishFixture() {
        Runtime.CleanupNodes();
    }

    TActorId StartComputeActor() {
        NDqProto::TDqTask task;
        task.SetId(1);
        auto* output = task.AddOutputs();
        output->MutableBroadcast();
        auto* channel = output->AddChannels();
        channel->SetId(ChannelId);
        channel->SetSrcTaskId(1);
        channel->SetDstTaskId(2);
        channel->SetCheckpointingMode(NDqProto::CHECKPOINTING_MODE_DISABLED);
        channel->SetWatermarksMode(NDqProto::WATERMARKS_MODE_DISABLED);
        channel->SetTransportVersion(NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0);
        ActorIdToProto(Consumer, channel->MutableDstEndpoint()->MutableActorId());

        auto factory = CreateTaskRunnerActorFactory(
            MakeIntrusive<TProxyFactoryMock>(Runner), MakeIntrusive<NDqs::TTaskRunnerInvokerFactory>());
        TComputeRuntimeSettings settings;
        settings.StatsMode = NDqProto::DQ_STATS_MODE_NONE;
        TComputeMemoryLimits limits;
        limits.MemoryQuotaManager = std::make_shared<TGuaranteeQuotaManager>(1024 * 1024, 1024 * 1024);
        return Runtime.Register(CreateDqAsyncComputeActor(
            Edge, "early-finish", &task, MakeIntrusive<TDqAsyncIoFactory>(), Registry.Get(), settings, limits, factory, {}));
    }

    void SendFinish(TActorId actor, ui64 seqNo) {
        auto ack = MakeHolder<TEvDqCompute::TEvChannelDataAck>();
        ack->Record.SetChannelId(ChannelId);
        ack->Record.SetSeqNo(seqNo);
        ack->Record.SetFinish(true);
        Runtime.Send(actor, Consumer, ack.Release());
    }

    void ExpectFinished() {
        auto state = Runtime.GrabEdgeEventIf<TEvDqCompute::TEvState>(Edge, [](const auto& event) {
            return event->Get()->Record.GetState() != NDqProto::COMPUTE_STATE_EXECUTING;
        }, TDuration::Seconds(5));
        UNIT_ASSERT(state);
        UNIT_ASSERT_VALUES_EQUAL(state->Get()->Record.GetState(), static_cast<ui32>(NDqProto::COMPUTE_STATE_FINISHED));
        UNIT_ASSERT_VALUES_EQUAL(Runner->Output->FinishCalls, 1);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TAsyncComputeActorEarlyFinish) {
    Y_UNIT_TEST_F(StopsProxyProducer, TEarlyFinishFixture) {
        auto actor = StartComputeActor();
        auto data = Runtime.GrabEdgeEvent<TEvDqCompute::TEvChannelData>(Consumer, TDuration::Seconds(5));
        UNIT_ASSERT(data);
        SendFinish(actor, data->Get()->Record.GetSeqNo());
        ExpectFinished();
    }

    Y_UNIT_TEST_F(FinishesAfterInflightRead, TEarlyFinishFixture) {
        TAutoPtr<IEventHandle> delayed;
        auto observer = Runtime.AddObserver<TEvOutputChannelData>([&](TEvOutputChannelData::TPtr& event) {
            UNIT_ASSERT(!delayed);
            delayed = event.Release();
        });
        auto actor = StartComputeActor();
        TDispatchOptions waitRead;
        waitRead.CustomFinalCondition = [&] { return bool(delayed); };
        Runtime.DispatchEvents(waitRead);
        UNIT_ASSERT(delayed);
        observer.Remove();

        // Direct sends in this runtime are delivered synchronously.
        SendFinish(actor, 0);

        // Deliver the old read only after PeerFinished has seen PopStarted=true.
        Runtime.Send(delayed.Release());
        ExpectFinished();
    }
}

} // namespace NYql::NDq
