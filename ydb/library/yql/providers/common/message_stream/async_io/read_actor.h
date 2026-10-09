#pragma once

#include <ydb/library/yql/providers/common/message_stream/partition.h>
#include <ydb/library/yql/providers/abstract/message_stream/message_stream_client.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/runtime/streaming/partition_key.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/invoke.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <functional>
#include <ydb/library/actors/util/datetime.h>
#include <ydb/library/yql/dq/actors/compute/dq_schedulable.h>
#include <util/generic/scope.h>
#include <util/system/hp_timer.h>

namespace NFq::NMessageStream {

struct TMessageStreamReadState {
    THashMap<NYql::NDq::TPartitionKey, TPartitionProgress> Partitions;
    TInstant StartingMessageTimestamp;
    NYql::NDq::TDqAsyncStats IngressStats;
};

// Source-specific checkpoint encoding and consumer initialization. Neither
// implementation owns the reading loop or buffered/acknowledged records.
class IMessageStreamReadActorState {
public:
    virtual ~IMessageStreamReadActorState() = default;
    virtual TMessageStreamReadState& GetReadState() = 0;
    virtual void SaveState(const NYql::NDqProto::TCheckpoint&, NYql::NDq::TSourceState&) = 0;
    virtual void LoadState(const NYql::NDq::TSourceState&) = 0;
    virtual void InitConsumerOffsets(NActors::TActorId, ui32,
        std::shared_ptr<NFq::IMessageStreamClient>, ui32) {}
    virtual bool ConsumerOffsetsInitialized() const { return true; }
    virtual void HandleConsumerOffsets(NActors::TEvents::TEvInvokeResult::TPtr&) {}
    virtual void StopConsumerOffsetInitialization() {}
};

struct TEvExecuteMessageStreamCallback : NActors::TEventLocal<TEvExecuteMessageStreamCallback,
    EventSpaceBegin(NActors::TEvents::ES_PRIVATE) + 20> {
    explicit TEvExecuteMessageStreamCallback(std::function<void()> function)
        : Function(std::move(function)) {}
    void Execute() { Function(); }
    std::function<void()> Function;
};

struct TMessageStreamReadCluster {
    TString Name;
    ui32 PartitionsCount = 0;
    std::vector<ui64> Partitions;
    std::function<std::shared_ptr<NFq::IMessageStreamClient>(const NActors::TActorContext&)> CreateClient;
    // Factories may complete asynchronously; callbacks never call the actor directly.
    std::function<NThreading::TFuture<std::shared_ptr<NFq::IMessageStreamReadSession>>(
        const NActors::TActorContext&, NFq::IMessageStreamClient&,
        const NFq::TMessageStreamReadSessionSettings&)> CreateSession;
    std::function<void(ui64, TInstant)> AdvancePartitionTime;
};

struct TMessageStreamReadActorSettings {
    NYql::NDq::IDqSchedulableWorkFactoryPtr WorkFactory;
    ui64 InputIndex = 0;
    ui64 TaskId = 0;
    NYql::NDq::TTxId TxId;
    NActors::TActorId ComputeActorId;
    TString Stream;
    TString Consumer;
    bool StopAtCurrentEndOffsets = false;
    bool EnableStreamingAutopartitioning = false;
    bool RequireWriteTime = false;
    i64 BufferSize = 16ULL << 20;
    TDuration ReconnectPeriod;
    TDuration CheckPartitionCountPeriod;
    TMaybe<ui64> BeginOffset, EndOffset;
    TMaybe<TInstant> BeginWriteTime, EndWriteTime;
    bool WatermarksEnabled = false;
    bool IdlePartitionsEnabled = false;
    TDuration WatermarkGranularity, LateArrivalDelay, IdleTimeout;
    TString MetricsSource;
    TVector<std::pair<TString, TString>> SensorLabels;
    bool EnableStreamingQueriesCounters = false;
    NYql::NDq::TCollectStatsLevel StatsLevel = NYql::NDq::TCollectStatsLevel::None;
    ::NMonitoring::TDynamicCounterPtr Counters;
    const NKikimr::NMiniKQL::THolderFactory* HolderFactory = nullptr;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    using TMetaExtractor = std::function<std::pair<NYql::NUdf::TUnboxedValuePod, i64>(
        const NFq::TMessageStreamRecord&, const TString&)>;
    std::vector<TMetaExtractor> MetadataFields;
    std::function<void(const NFq::TMessageStreamRecord&)> TraceRecord;
    std::vector<TMessageStreamReadCluster> Clusters;
};

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateMessageStreamReadActor(
    TMessageStreamReadActorSettings settings, std::unique_ptr<IMessageStreamReadActorState> state);

} // namespace NFq::NMessageStream

namespace NFq::NMessageStream::NInternal {

// Only SDK callbacks are measured here. GetAsyncInputData runs under the
// compute actor's quota and must not acquire or charge it a second time.
class TMessageStreamCpuQuota {
public:
    explicit TMessageStreamCpuQuota(NYql::NDq::IDqSchedulableWorkFactoryPtr factory)
        : Work(factory ? factory->CreateSchedulableWork() : nullptr)
    {}

    ~TMessageStreamCpuQuota() {
        Cancel();
    }

    bool HasWork() const {
        return !!Work;
    }

    bool IsWaiting() const {
        return Waiting;
    }

    void RegisterForResume(const NActors::TActorId& actorId) {
        if (Work) {
            Work->RegisterForResume(actorId);
        }
    }

    void NotifyResumed(bool byScheduler) {
        if (Work && Waiting) {
            Work->NotifyResumed(byScheduler);
        }
    }

    template <typename TCallback>
    std::optional<TDuration> Execute(TCallback&& callback) {
        if (Work) {
            if (auto delay = Work->TryStartExecution(TMonotonic::Now())) {
                Waiting = true;
                return delay;
            }
        }
        Waiting = false;
        const auto start = GetCycleCountFast();
        Y_DEFER {
            CpuTime += TDuration::Seconds(NHPTimer::GetSeconds(GetCycleCountFast() - start));
            if (Work) {
                Work->StopExecution();
            }
        };
        callback();
        return std::nullopt;
    }

    void Cancel() {
        if (Waiting) {
            Work->StopExecution();
            Waiting = false;
        }
    }

    TDuration GetCpuTime() const {
        return CpuTime;
    }

private:
    std::unique_ptr<NYql::NDq::IDqSchedulableWork> Work;
    bool Waiting = false;
    TDuration CpuTime;
};

} // namespace NFq::NMessageStream::NInternal
