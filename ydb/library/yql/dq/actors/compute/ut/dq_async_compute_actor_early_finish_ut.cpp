#include <library/cpp/testing/unittest/registar.h>

#include <utility>

#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_async_compute_actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_channels.h>
#include <ydb/library/yql/dq/actors/task_runner/task_runner_actor.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_watermark.h>

namespace NYql::NDq {
namespace {

using namespace NActors;
using namespace NKikimr::NMiniKQL;
using namespace NTaskRunnerActor;

// Closing output stops new production but leaves buffered chunks until Pop/Drop.
class TOutputChannelMock : public IDqOutputChannel {
public:
    bool Pop(TDqSerializedBatch& data) override {
        if (BufferedChunks == 0) {
            return false;
        }
        data.Proto.SetTransportVersion(NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0);
        data.Proto.SetRows(1);
        data.Proto.SetRaw("data");
        --BufferedChunks;
        return true;
    }

    bool Pop(NDqProto::TWatermark&) override { return false; }
    bool Pop(NDqProto::TCheckpoint&) override { return false; }
    bool PopAll(TDqSerializedBatch& data) override { return Pop(data); }
    bool IsFinished() const override { return Finished && BufferedChunks == 0; }
    bool IsEarlyFinished() const override { return Finished; }
    bool HasData() const override { return BufferedChunks != 0; }
    void Finish() override { Finished = true; ++FinishCalls; }

    ui64 Drop() override {
        const auto dropped = std::exchange(BufferedChunks, 0);
        DroppedChunks += dropped;
        ++DropCalls;
        return dropped;
    }

    ui64 GetChannelId() const override { return 10; }
    ui64 GetValuesCount() const override { return BufferedChunks; }
    const TDqOutputChannelStats& GetPopStats() const override { return Stats; }
    const TDqOutputStats& GetPushStats() const override { return Stats; }
    EDqFillLevel GetFillLevel() const override { return SoftLimit; }
    EDqFillLevel UpdateFillLevel() override { return GetFillLevel(); }
    void SetFillAggregator(std::shared_ptr<TDqFillAggregator>) override {}
    void Push(NUdf::TUnboxedValue&&) override { Y_ABORT("Unexpected push"); }
    void WidePush(NUdf::TUnboxedValue*, ui32) override { Y_ABORT("Unexpected push"); }
    void Push(NDqProto::TWatermark&&) override { Y_ABORT("Unexpected watermark"); }
    void Push(NDqProto::TCheckpoint&&) override { Y_ABORT("Unexpected checkpoint"); }
    void Flush() override {}
    TType* GetOutputType() const override { return nullptr; }
    void Terminate() override {}
    void Bind(TActorId, TActorId) override {}
    bool IsLocal() const override { return false; }

    bool Finished = false;
    ui32 BufferedChunks = 1;
    ui32 DroppedChunks = 0;
    ui32 DropCalls = 0;
    ui32 FinishCalls = 0;
    TDqOutputChannelStats Stats{};
};

class TTaskRunnerMock : public IDqTaskRunner {
public:
    TTaskRunnerMock(TScopedAlloc& alloc, TTypeEnvironment& env, THolderFactory& holders)
        : Alloc(alloc), Env(env), Holders(holders)
    {}

    ui64 GetTaskId() const override { return 1; }
    void Prepare(const TDqTaskSettings&, const TDqTaskRunnerMemoryLimits&,
        const IDqTaskRunnerExecutionContext&, TDqComputeActorWatermarks*, TDqWatermarkGeneratorTracker*) override {}
    ERunStatus Run() override {
        return Output->IsFinished() ? ERunStatus::Finished : ERunStatus::PendingOutput;
    }
    bool HasEffects() const override { return false; }
    IDqInputChannel::TPtr GetInputChannel(ui64) override { Y_ABORT("Unexpected input"); }
    IDqOutputChannel::TPtr GetOutputChannel(ui64) override { return Output; }
    IDqAsyncInputBuffer::TPtr GetSource(ui64) override { Y_ABORT("Unexpected source"); }
    IDqAsyncOutputBuffer::TPtr GetSink(ui64) override { Y_ABORT("Unexpected sink"); }
    std::optional<std::pair<NUdf::TUnboxedValue, IDqAsyncInputBuffer::TPtr>> GetInputTransform(ui64) override { return {}; }
    TDqComputeActorWatermarks* GetInputTransformWatermarksTracker(ui64) override { return nullptr; }
    std::pair<IDqAsyncOutputBuffer::TPtr, IDqOutputConsumer::TPtr> GetOutputTransform(ui64) override { Y_ABORT("Unexpected output transform"); }
    IRandomProvider* GetRandomProvider() const override { return nullptr; }
    const THashMap<TString, TString>& GetTaskParams() const override { return Params; }
    const TVector<TString>& GetReadRanges() const override { return ReadRanges; }
    const THashMap<TString, TString>& GetSecureParams() const override { return Params; }
    const TTypeEnvironment& GetTypeEnv() const override { return Env; }
    const THolderFactory& GetHolderFactory() const override { return Holders; }
    TScopedAlloc& GetAllocator() const override { return Alloc; }
    TGuard<TScopedAlloc> BindAllocator(TMaybe<ui64>) override { return TGuard<TScopedAlloc>(Alloc); }
    bool IsAllocatorAttached() override { return false; }
    const TDqTaskRunnerStats* GetStats() const override { return &Stats; }
    const TDqMeteringStats* GetMeteringStats() const override { return &MeteringStats; }
    TString Save() const override { Y_ABORT("Unexpected save"); }
    void Load(TStringBuf) override { Y_ABORT("Unexpected load"); }
    void SetWatermarkIn(TInstant) override { Y_ABORT("Unexpected watermark"); }
    const TWatermark& GetWatermark() const override { return Watermark; }
    void SetSpillerFactory(std::shared_ptr<ISpillerFactory>) override { Y_ABORT("Unexpected spilling"); }
    TString GetOutputDebugString() override { return {}; }

    TIntrusivePtr<TOutputChannelMock> Output = MakeIntrusive<TOutputChannelMock>();

private:
    TScopedAlloc& Alloc;
    TTypeEnvironment& Env;
    THolderFactory& Holders;
    THashMap<TString, TString> Params;
    TVector<TString> ReadRanges;
    TDqTaskRunnerStats Stats{};
    TDqMeteringStats MeteringStats{};
    TWatermark Watermark;
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

    TActorId StartComputeActor(ui64 channelBufferSize = 1024 * 1024) {
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

        auto factory = CreateLocalTaskRunnerActorFactory(
            [runner = Runner](std::shared_ptr<TScopedAlloc>, const TDqTaskSettings&, NDqProto::EDqStatsMode, const TLogFunc&) {
                return runner;
            });
        TComputeRuntimeSettings settings;
        settings.StatsMode = NDqProto::DQ_STATS_MODE_NONE;
        TComputeMemoryLimits limits;
        limits.ChannelBufferSize = channelBufferSize;
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
        UNIT_ASSERT_VALUES_EQUAL(Runner->Output->DropCalls, 1);
        UNIT_ASSERT_VALUES_EQUAL(Runner->Output->BufferedChunks, 0);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TAsyncComputeActorEarlyFinish) {
    Y_UNIT_TEST_F(StopsLocalProducer, TEarlyFinishFixture) {
        auto actor = StartComputeActor();
        auto data = Runtime.GrabEdgeEvent<TEvDqCompute::TEvChannelData>(Consumer, TDuration::Seconds(5));
        UNIT_ASSERT(data);
        SendFinish(actor, data->Get()->Record.GetSeqNo());
        ExpectFinished();
    }

    Y_UNIT_TEST_F(DropsAllBufferedChunks, TEarlyFinishFixture) {
        Runner->Output->BufferedChunks = 4;
        // One chunk exceeds the consumer's free space: each ordinary read pops
        // one chunk. Finishing must discard the entire remaining buffer.
        auto actor = StartComputeActor(/*channelBufferSize=*/1);
        auto data = Runtime.GrabEdgeEvent<TEvDqCompute::TEvChannelData>(Consumer, TDuration::Seconds(5));
        UNIT_ASSERT(data);
        UNIT_ASSERT_C(Runner->Output->BufferedChunks > 1, "Expected multiple queued chunks before early finish");
        SendFinish(actor, data->Get()->Record.GetSeqNo());
        ExpectFinished();
        UNIT_ASSERT_C(Runner->Output->DroppedChunks > 1, "Expected all remaining chunks to be discarded");
    }

    Y_UNIT_TEST_F(FinishesAfterInflightRead, TEarlyFinishFixture) {
        Runner->Output->BufferedChunks = 4;
        TAutoPtr<IEventHandle> delayed;
        auto observer = Runtime.AddObserver<TEvOutputChannelData>([&](TEvOutputChannelData::TPtr& event) {
            UNIT_ASSERT(!delayed);
            delayed = event.Release();
        });
        auto actor = StartComputeActor(/*channelBufferSize=*/1);
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
        UNIT_ASSERT(Runner->Output->DroppedChunks > 1);
    }
}

} // namespace NYql::NDq
