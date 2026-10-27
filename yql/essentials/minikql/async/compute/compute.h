#pragma once

#include <yql/essentials/minikql/async/async.h>
#include <yql/essentials/minikql/computation/mkql_async_checkpoint.h>
#include <yql/essentials/minikql/async/scheduler.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <util/generic/hash.h>

namespace NKikimr::NMiniKQL::NAsync {

// Compiler metadata, independent of runtime provider instances and the scalar UDF ABI.
// All async occurrences are effectful: no CSE, constant evaluation or duplication.
struct TCallableDescriptor {
    TString Name;
    TType* InputType = nullptr;
    TType* ResultType = nullptr;
    TString Provider;
    TString Operation;
    ui64 MaxResponseBytes = 1024 * 1024;
};

class TCallableRegistry {
public:
    void Register(TCallableDescriptor descriptor);
    const TCallableDescriptor& Resolve(TStringBuf name) const;
    TRuntimeNode Call(TProgramBuilder& builder, TStringBuf name, TRuntimeNode argument) const;

private:
    THashMap<TString, TCallableDescriptor> Descriptors_;
};

// Typed internal IR, not a SQL syntax or a scalar UDF wrapper. Only explicitly
// supported row expressions are lowered; unknown/lazy/higher-order contexts fail
// preparation. CASE lowers to nested If. Flow is adapted through Stream/Yield.
TRuntimeNode BuildAsyncMap(TProgramBuilder& builder, TRuntimeNode input,
                           TRuntimeNode rowArgument, TRuntimeNode body,
                           const TCallableRegistry& registry, ui32 maxFrames = 16);
IComputationNode* WrapAsyncMap(TCallable& callable, const TComputationNodeFactoryContext& ctx);

// Per graph execution, never stored in a cached computation pattern. All methods
// except notification callbacks run on the task owner with its allocator bound.
// The owner MUST call BeginTurn once at the START of each compute event, before
// fetching any stream, including on initial execution and source/timer wakeups.
enum class ECheckpointMode {
    Disabled,
    Drain,
    Replay,
};

// Explicit replay permission, independent of batching and async capability.
// Encode/Decode are pure logical codecs: no transport handles, identity, secret,
// deadline clock or I/O. Contract must change when descriptor semantics change.
class IReplayCodec {
public:
    virtual ~IReplayCodec() = default;
    virtual TString Contract() const = 0;
    virtual TString Encode(const TRequest& request) const = 0;
    virtual TRequest Decode(TStringBuf descriptor) const = 0;
};
struct TSnapshotId {
    ui64 Generation = 0;
    ui64 Checkpoint = 0;
    ui64 Revision = 0;
    bool operator==(const TSnapshotId&) const = default;
};
struct TCheckpointSettings {
    ECheckpointMode Mode = ECheckpointMode::Disabled;
    // Includes typed graph, source/operator codecs and their compatibility versions.
    TString PlanFingerprint;
    ui64 MaxSnapshotBytes = 1024 * 1024;
};
struct TExecutionSettings {
    TCheckpointSettings Checkpoint;
    TSchedulerSettings Scheduler;
    // Cooperative thread CPU time, in microseconds; zero disables this bound.
    ui64 CpuBudgetUs = 1000;
    std::function<ui64()> CpuClock;
};
struct TComputeStats {
    ui64 Turns = 0;
    ui64 Steps = 0;
    ui64 CpuBudgetYields = 0;
    ui64 CpuTimeUs = 0;
};
class TExecutionServices: public IAsyncCheckpoint {
public:
    explicit TExecutionServices(TWakeup enqueue, size_t stepBudget = 1024, TLimits limits = {}, TExecutionSettings settings = {});
    ~TExecutionServices();
    void RegisterProvider(TString name, std::shared_ptr<IProvider> provider);
    bool HasProvider(TStringBuf name) const;
    void BeginTurn();
    void EndTurn();
    bool TryStep();
    void RequestRerun();
    void Flush();
    bool CanWaitForMemory() const;
    std::shared_ptr<TMemoryReservation> TryReserveFrame(ui64 bytes);
    void AccountAllocatorBytes(ui64 bytes);
    std::optional<TTimePoint> NextDeadline() const;
    ui64 GetTurn() const;
    TSubmission Submit(TStringBuf provider, TRequest request);
    bool IsReady(TOperationId id) const;
    std::optional<TResult> TakeResult(TOperationId id);
    void CancelAndForget(TOperationId id);
    void Shutdown();
    TStats GetStats() const;
    TSchedulerStats GetSchedulerStats() const;
    TComputeStats GetComputeStats() const;

    void RegisterReplayCodec(TString provider, std::shared_ptr<const IReplayCodec> codec);
    const IReplayCodec& GetReplayCodec(TStringBuf provider) const;
    ECheckpointMode GetCheckpointMode() const;
    ui64 GetGeneration() const;
    const TResult* PeekResult(TOperationId id) const;
    // Participants live on the owner; callbacks must not outlive their stream.
    void RegisterParticipant(ui32 id, std::function<bool()> drained);
    void UnregisterParticipant(ui32 id);
    void BeginCheckpoint(ui64 id);
    void EstablishInputCut(ui64 id);
    bool IsInputPaused() const;
    ui64 OutputProduced();
    void OutputAcknowledged(ui64 generation, ui64 sequence);
    bool CanSaveCheckpoint() const;
    TSnapshotId GetSnapshotId() const;
    bool SnapshotStored(TSnapshotId id);
    bool CanForwardBoundary() const;
    bool CanForwardWatermark() const;
    void ReleaseBoundary();
    // Called after graph.LoadGraphState + graph.GetValue, before any execution.
    void CompleteRestore();
    void EnsureCanExecute() const;
    void CheckpointProgress();

    TString BeforeSave() override;
    void CheckSnapshotSize(ui64 bytes) const override;
    void AfterSave() override;
    void AbortSave() noexcept override;
    void BeforeLoad(TStringBuf header, ui64 bytes) override;
    void AfterLoad() override;

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NKikimr::NMiniKQL::NAsync
