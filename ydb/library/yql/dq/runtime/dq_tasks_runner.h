#pragma once

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/dq/runtime/dq_async_output.h>
#include <ydb/library/yql/dq/runtime/dq_compute.h>
#include <ydb/library/yql/dq/runtime/dq_input_channel.h>
#include <ydb/library/yql/dq/runtime/dq_input_producer.h>
#include <ydb/library/yql/dq/runtime/dq_output_channel.h>
#include <ydb/library/yql/dq/runtime/dq_output_consumer.h>
#include <ydb/library/yql/dq/runtime/dq_async_input.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_pattern_cache.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_node_visitor.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_watermark.h>

#include <library/cpp/monlib/metrics/histogram_collector.h>

#include <util/generic/size_literals.h>
#include <util/system/types.h>

namespace NActors {
    class TActorSystem;
};

namespace NYql::NDq {

enum class ERunStatus : ui32 {
    Finished,
    PendingInput,
    PendingOutput
};

struct TMkqlStat {
    NKikimr::NMiniKQL::TStatKey Key;
    i64 Value = 0;
};

struct TTaskRunnerStatsBase {
    // basic stats
    TDuration BuildCpuTime;
    TInstant FinishTs;
    TInstant StartTs;

    TDuration ComputeCpuTime;
    TDuration WaitInputTime;
    TDuration WaitOutputTime;

    // profile stats
    NMonitoring::IHistogramCollectorPtr ComputeCpuTimeByRun; // in millis

    THashMap<ui32, THashMap<ui64, IDqInputChannel::TPtr>> InputChannels;   // SrcStageId => {ChannelId => Channel}
    THashMap<ui64, IDqAsyncInputBuffer::TPtr> Sources;                     // InputIndex => Source
    THashMap<ui32, THashMap<ui64, IDqOutputChannel::TPtr>> OutputChannels; // DstStageId => {ChannelId => Channel}

    TVector<TMkqlStat> MkqlStats;

    TTaskRunnerStatsBase() = default;
    TTaskRunnerStatsBase(TTaskRunnerStatsBase&&) = default;
    TTaskRunnerStatsBase& operator=(TTaskRunnerStatsBase&&) = default;

    virtual ~TTaskRunnerStatsBase() = default;
};

struct TDqTaskRunnerStats : public TTaskRunnerStatsBase {
};

// Provides read access to TTaskRunnerStatsBase
// May or may not own the underlying object
class TDqTaskRunnerStatsView {
public:
    TDqTaskRunnerStatsView() : IsDefined(false) {}

    TDqTaskRunnerStatsView(const TDqTaskRunnerStats* stats)   // used in TLocalTaskRunnerActor, cause it holds this stats, and does not modify it asyncronously from TDqAsyncComputeActor
        : StatsPtr(stats)
        , IsDefined(true) {
    }

    TDqTaskRunnerStatsView(const TDqTaskRunnerStats* stats, THashMap<ui32, const IDqAsyncOutputBuffer*>&& sinks,
        THashMap<ui32, const IDqAsyncInputBuffer*>&& inputTransforms)
        : StatsPtr(stats)
        , IsDefined(true)
        , Sinks(std::move(sinks))
        , InputTransforms(std::move(inputTransforms)) {
    }

    const TTaskRunnerStatsBase* Get() {
        if (!IsDefined) {
            return nullptr;
        }
        return StatsPtr;
    }

    operator bool() const {
        return IsDefined;
    }

    const IDqAsyncOutputBuffer* GetSink(ui32 sinkId) const {
        return Sinks.at(sinkId);
    }

    const IDqAsyncInputBuffer* GetInputTransform(ui32 inputTransformId) const {
        return InputTransforms.at(inputTransformId);
    }

private:
    const TDqTaskRunnerStats* StatsPtr;
    bool IsDefined;
    THashMap<ui32, const IDqAsyncOutputBuffer*> Sinks;
    THashMap<ui32, const IDqAsyncInputBuffer*> InputTransforms;
};

struct TDqTaskRunnerContext {
    const NKikimr::NMiniKQL::IFunctionRegistry* FuncRegistry = nullptr;
    IRandomProvider* RandomProvider = nullptr;
    ITimeProvider* TimeProvider = nullptr;
    TDqComputeContextBase* ComputeCtx = nullptr;
    NKikimr::NMiniKQL::TComputationNodeFactory ComputationFactory;
    NUdf::IApplyContext* ApplyCtx = nullptr;
    NKikimr::NMiniKQL::TCallableVisitFuncProvider FuncProvider;
    NKikimr::NMiniKQL::TTypeEnvironment* TypeEnv = nullptr;
    std::shared_ptr<NKikimr::NMiniKQL::TComputationPatternLRUCache> PatternCache;
};

class IDqTaskRunnerExecutionContext {
public:
    virtual ~IDqTaskRunnerExecutionContext() = default;

    virtual IDqOutputConsumer::TPtr CreateOutputConsumer(const NDqProto::TTaskOutput& outputDesc,
        const NKikimr::NMiniKQL::TType* type, NUdf::IApplyContext* applyCtx,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        TVector<IDqOutput::TPtr>&& outputs) const = 0;

    virtual IDqChannelStorage::TPtr CreateChannelStorage(ui64 channelId, bool withSpilling) const = 0;
    virtual IDqChannelStorage::TPtr CreateChannelStorage(ui64 channelId, bool withSpilling, NActors::TActorSystem* actorSystem) const = 0;

    virtual std::function<void()> GetWakeupCallback() const = 0;
    virtual std::function<void(const TString& error)> GetErrorCallback() const = 0;
    virtual TTxId GetTxId() const = 0;
};

class TDqTaskRunnerExecutionContextBase : public IDqTaskRunnerExecutionContext {
public:
    IDqOutputConsumer::TPtr CreateOutputConsumer(const NDqProto::TTaskOutput& outputDesc,
        const NKikimr::NMiniKQL::TType* type, NUdf::IApplyContext* applyCtx,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        TVector<IDqOutput::TPtr>&& outputs) const override;
};

class TDqTaskRunnerExecutionContextDefault : public TDqTaskRunnerExecutionContextBase {
public:
    IDqChannelStorage::TPtr CreateChannelStorage(ui64 /*channelId*/, bool /*withSpilling*/) const override {
        return {};
    };

    IDqChannelStorage::TPtr CreateChannelStorage(ui64 /*channelId*/, bool /*withSpilling*/, NActors::TActorSystem* /*actorSystem*/) const override {
        return {};
    };

    std::function<void()> GetWakeupCallback() const override {
        return {};
    }

    std::function<void(const TString&)> GetErrorCallback() const override {
        return {};
    }

    TTxId GetTxId() const override {
        return {};
    }

};

struct TDqTaskRunnerSettings {
    NDqProto::EDqStatsMode StatsMode = NDqProto::DQ_STATS_MODE_NONE;
    bool TerminateOnError = false;
    bool UseCacheForLLVM = true;
    TString OptLLVM = "";
    THashMap<TString, TString> SecureParams;
    THashMap<TString, TString> TaskParams;
    TVector<TString> ReadRanges;
};

struct TDqTaskRunnerMemoryLimits {
    ui32 ChannelBufferSize = 0;
    ui32 OutputChunkMaxSize = 0;
    ui32 ChunkSizeLimit = 48_MB;
};

NUdf::TUnboxedValue DqBuildInputValue(const NDqProto::TTaskInput& inputDesc, const NKikimr::NMiniKQL::TType* type,
    TVector<IDqInputChannel::TPtr>&& channels, const NKikimr::NMiniKQL::THolderFactory& holderFactory);

IDqOutputConsumer::TPtr DqBuildOutputConsumer(const NDqProto::TTaskOutput& outputDesc, const NKikimr::NMiniKQL::TType* type,
    const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv, const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    TVector<IDqOutput::TPtr>&& channels);

using TDqTaskRunnerParameterProvider = std::function<
    bool(std::string_view name, NKikimr::NMiniKQL::TType* type, const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
         const NKikimr::NMiniKQL::THolderFactory& holderFactory, NUdf::TUnboxedValue& value)
>;


/// TDqTaskSettings class that holds all the settings of the DqTask.
/// It accepts pointer and accepts ownership.
class TDqTaskSettings {
public:
    explicit TDqTaskSettings(NDqProto::TDqTask* task, TIntrusivePtr<NActors::TProtoArenaHolder> arena = nullptr)
        : Task_(nullptr)
        , Arena(std::move(arena))
    {
        if (!task->GetArena()) {
            HeapTask_ = std::make_unique<NDqProto::TDqTask>();
            HeapTask_->Swap(task);
            Task_ = HeapTask_.get();
            Y_ABORT_UNLESS(!Arena);
        } else {
            Task_ = task;
            Y_ABORT_UNLESS(Arena);
            Y_ABORT_UNLESS(task->GetArena() == Arena->Get());
        }
    }

    TDqTaskSettings(const TDqTaskSettings& task) {
        if (Y_LIKELY(task.HeapTask_)) {
            HeapTask_ = std::make_unique<NDqProto::TDqTask>();
            HeapTask_->CopyFrom(*task.Task_);
            Task_ = HeapTask_.get();
            Y_ABORT_UNLESS(!task.Arena);
        } else {
            Y_ABORT("not allowed to copy dq settings for arena allocated messages.");
        }
    }

    ui64 GetId() const {
        return Task_->GetId();
    }

    bool GetCreateSuspended() const {
        return Task_->GetCreateSuspended();
    }

    const NDqProto::TDqTask& GetSerializedTask() const {
        Y_ABORT_UNLESS(!ParamProvider, "GetSerialized isn't supported if external ParamProvider callback is specified!");
        return *Task_;
    }

    const ::NYql::NDqProto::TTaskInput& GetInputs(size_t index) const {
        return Task_->GetInputs(index);
    }

    const ::NYql::NDqProto::TTaskOutput& GetOutputs(size_t index) const {
        return Task_->GetOutputs(index);
    }

    const ::google::protobuf::RepeatedPtrField<::NYql::NDqProto::TTaskInput>& GetInputs() const {
        return Task_->GetInputs();
    }

    size_t InputsSize() const {
        return Task_->InputsSize();
    }

    size_t OutputsSize() const {
        return Task_->OutputsSize();
    }

    void SetParamsProvider(TDqTaskRunnerParameterProvider&& provider) {
        ParamProvider = std::move(provider);
    }

    void GetParameterValue(std::string_view name, NKikimr::NMiniKQL::TType* type, const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const NKikimr::NMiniKQL::THolderFactory& holderFactory, NUdf::TUnboxedValue& value) const
    {
        if (ParamProvider && ParamProvider(name, type, typeEnv, holderFactory, value)) {
#ifndef NDEBUG
            YQL_ENSURE(!Task_->GetParameters().contains(name), "param: " << name);
#endif
        } else {
            auto it = Task_->GetParameters().find(name);
            YQL_ENSURE(it != Task_->GetParameters().end());

            auto guard = typeEnv.BindAllocator();
            TDqDataSerializer::DeserializeParam(it->second, type, holderFactory, value);
        }
    }

    ui64 GetStageId() const {
        return Task_->GetStageId();
    }

    const ::NYql::NDqProto::TProgram& GetProgram() const {
        return Task_->GetProgram();
    }

    const TProtoStringType & GetRateLimiterResource() const {
        return Task_->GetRateLimiterResource();
    }

    const TProtoStringType& GetRateLimiter() const {
        return Task_->GetRateLimiter();
    }

    const ::google::protobuf::Map<TProtoStringType, ::NYql::NDqProto::TData>& GetParameters() const {
        return Task_->GetParameters();
    }

    const ::google::protobuf::Map<TProtoStringType, TProtoStringType>& GetTaskParams() const {
        return Task_->GetTaskParams();
    }

    const ::google::protobuf::RepeatedPtrField<TString>& GetReadRanges() const {
        return Task_->GetReadRanges();
    }

    const ::google::protobuf::Map<TProtoStringType, TProtoStringType>& GetSecureParams() const {
        return Task_->GetSecureParams();
    }

    const ::google::protobuf::RepeatedPtrField<::NYql::NDqProto::TTaskOutput>& GetOutputs() const {
        return Task_->GetOutputs();
    }

    const ::google::protobuf::Any& GetMeta() const {
        return Task_->GetMeta();
    }

    bool GetUseLlvm() const {
        return Task_->GetUseLlvm();
    }

    bool HasUseLlvm() const {
        return Task_->HasUseLlvm();
    }

    bool IsLLVMDisabled() const {
        return HasUseLlvm() && !GetUseLlvm();
    }

    const TVector<google::protobuf::Message*>& GetSourceSettings() const {
        return SourceSettings;
    }

    TVector<google::protobuf::Message*>& MutableSourceSettings() {
        return SourceSettings;
    }

    const TIntrusivePtr<NActors::TProtoArenaHolder>& GetArena() const {
        return Arena;
    }

    const google::protobuf::Map<TProtoStringType, TProtoStringType>& GetRequestContext() const {
        return Task_->GetRequestContext();
    }

    bool GetEnableSpilling() const {
        return Task_->HasEnableSpilling() && Task_->GetEnableSpilling();
    }

private:

    // external callback to retrieve parameter value.
    TDqTaskRunnerParameterProvider ParamProvider;
    NDqProto::TDqTask* Task_ = nullptr;
    std::unique_ptr<NDqProto::TDqTask> HeapTask_;
    TIntrusivePtr<NActors::TProtoArenaHolder> Arena;
    TVector<google::protobuf::Message*> SourceSettings;  // used only in case if we execute compute actor locally
};

class IDqTaskRunner : public TSimpleRefCount<IDqTaskRunner>, private TNonCopyable {
public:
    virtual ~IDqTaskRunner() = default;

    virtual ui64 GetTaskId() const = 0;

    virtual void Prepare(const TDqTaskSettings& task, const TDqTaskRunnerMemoryLimits& memoryLimits,
        const IDqTaskRunnerExecutionContext& execCtx) = 0;
    virtual ERunStatus Run() = 0;

    virtual bool HasEffects() const = 0;

    virtual IDqInputChannel::TPtr GetInputChannel(ui64 channelId) = 0;
    virtual IDqAsyncInputBuffer::TPtr GetSource(ui64 inputIndex) = 0;
    virtual IDqOutputChannel::TPtr GetOutputChannel(ui64 channelId) = 0;
    virtual IDqAsyncOutputBuffer::TPtr GetSink(ui64 outputIndex) = 0;
    virtual std::optional<std::pair<NUdf::TUnboxedValue, IDqAsyncInputBuffer::TPtr>> GetInputTransform(ui64 inputIndex) = 0;
    virtual std::pair<IDqAsyncOutputBuffer::TPtr, IDqOutputConsumer::TPtr> GetOutputTransform(ui64 outputIndex) = 0;

    virtual IRandomProvider* GetRandomProvider() const = 0;

    // if memoryLimit = Nothing()  then don't set memory limit, use existing one (if any)
    // if memoryLimit = 0          then set unlimited
    // otherwise use particular memory limit
    virtual TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator(TMaybe<ui64> memoryLimit = Nothing()) = 0;
    virtual bool IsAllocatorAttached() = 0;
    virtual const NKikimr::NMiniKQL::TTypeEnvironment& GetTypeEnv() const = 0;
    virtual const NKikimr::NMiniKQL::THolderFactory& GetHolderFactory() const = 0;
    virtual NKikimr::NMiniKQL::TScopedAlloc& GetAllocator() const = 0;

    virtual const THashMap<TString, TString>& GetSecureParams() const = 0;
    virtual const THashMap<TString, TString>& GetTaskParams() const = 0;
    virtual const TVector<TString>& GetReadRanges() const = 0;

    virtual const TDqTaskRunnerStats* GetStats() const = 0;
    virtual const TDqMeteringStats* GetMeteringStats() const = 0;

    [[nodiscard]]
    virtual TString Save() const = 0;
    virtual void Load(TStringBuf in) = 0;

    virtual void SetWatermarkIn(TInstant time) = 0;
    virtual const NKikimr::NMiniKQL::TWatermark& GetWatermark() const = 0;

    virtual void SetSpillerFactory(std::shared_ptr<NKikimr::NMiniKQL::ISpillerFactory> spillerFactory) = 0;
};

TIntrusivePtr<IDqTaskRunner> MakeDqTaskRunner(
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, 
    const TDqTaskRunnerContext& ctx, 
    const TDqTaskRunnerSettings& settings,
    const TLogFunc& logFunc
);

} // namespace NYql::NDq

template <>
inline void Out<NYql::NDq::TTaskRunnerStatsBase>(IOutputStream& os, TTypeTraits<NYql::NDq::TTaskRunnerStatsBase>::TFuncParam stats) {
    os << "TTaskRunnerStatsBase:" << Endl
       << "\tBuildCpuTime: " << stats.BuildCpuTime << Endl
       << "\tStartTs: " << stats.StartTs << Endl
       << "\tFinishTs: " << stats.FinishTs << Endl
       << "\tComputeCpuTime: " << stats.ComputeCpuTime << Endl
       << "\tWaitInputTime: " << stats.WaitInputTime << Endl
       << "\tWaitOutputTime: " << stats.WaitOutputTime << Endl
       << "\tsize of InputChannels: " << stats.InputChannels.size() << Endl
       << "\tsize of Sources: " << stats.Sources.size() << Endl
       << "\tsize of OutputChannels: " << stats.OutputChannels.size();
}
