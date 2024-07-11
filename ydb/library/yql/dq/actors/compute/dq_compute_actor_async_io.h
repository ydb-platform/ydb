#pragma once
#include <ydb/library/yql/dq/actors/dq_events_ids.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/runtime/dq_output_consumer.h>
#include <ydb/library/yql/dq/runtime/dq_async_input.h>
#include <ydb/library/yql/dq/runtime/dq_input_producer.h>
#include <ydb/library/yql/dq/runtime/dq_async_output.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <util/generic/ptr.h>

#include <memory>
#include <utility>

namespace NYql::NDqProto {
class TCheckpoint;
class TTaskInput;
class TTaskOutput;
} // namespace NYql::NDqProto

namespace NYql::NDq {
struct TSourceState;
struct TSinkState;
} // namespace NYql::NDq

namespace NActors {
class IActor;
} // namespace NActors

namespace NKikimr::NMiniKQL {
class TProgramBuilder;
} // namespace NKikimr::NMiniKQL

namespace NYql::NDq {

enum class EResumeSource : ui32 {
    Default,
    ChannelsHandleWork,
    ChannelsHandleUndeliveredData,
    ChannelsHandleUndeliveredAck,
    AsyncPopFinished,
    CheckpointRegister,
    CheckpointInject,
    CABootstrap,
    CABootstrapWakeup,
    CAPendingInput,
    CATakeInput,
    CASinkFinished,
    CATransformFinished,
    CAStart,
    CAPollAsync,
    CAPollAsyncNoSpace,
    CANewAsyncInput,
    CADataSent,
    CAPendingOutput,
    CATaskRunnerCreated,

    Last,
};

struct IMemoryQuotaManager {
    using TPtr = std::shared_ptr<IMemoryQuotaManager>;
    using TWeakPtr = std::weak_ptr<IMemoryQuotaManager>;
    virtual ~IMemoryQuotaManager() = default;
    virtual bool AllocateQuota(ui64 memorySize) = 0;
    virtual void FreeQuota(ui64 memorySize) = 0;
    virtual ui64 GetCurrentQuota() const = 0;
    virtual ui64 GetMaxMemorySize() const = 0;
    virtual bool IsReasonableToUseSpilling() const = 0;
    virtual TString MemoryConsumptionDetails() const = 0;
};

// Source/transform.
// Must be IActor.
//
// Protocol:
// 1. CA starts source/transform.
// 2. CA calls IDqComputeActorAsyncInput::GetAsyncInputData(batch, FreeSpace).
// 3. Source/transform sends TEvNewAsyncInputDataArrived when it has data to process.
// 4. CA calls IDqComputeActorAsyncInput::GetAsyncInputData(batch, FreeSpace) to get data when it is ready to process it.
//
// In case of error source/transform sends TEvAsyncInputError
//
// Checkpointing:
// 1. InjectCheckpoint event arrives to CA.
// 2. ...
// 3. CA calls IDqComputeActorAsyncInput::SaveState() and IDqTaskRunner::SaveGraphState() and uses this pair as state for CA.
// 3. ...
// 5. CA calls IDqComputeActorAsyncInput::CommitState() to apply all side effects.
struct IDqComputeActorAsyncInput {
    struct TEvNewAsyncInputDataArrived : public NActors::TEventLocal<TEvNewAsyncInputDataArrived, TDqComputeEvents::EvNewAsyncInputDataArrived> {
        const ui64 InputIndex;
        explicit TEvNewAsyncInputDataArrived(ui64 inputIndex)
            : InputIndex(inputIndex)
        {}
    };

    struct TEvAsyncInputError : public NActors::TEventLocal<TEvAsyncInputError, TDqComputeEvents::EvAsyncInputError> {
        TEvAsyncInputError(ui64 inputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode)
            : InputIndex(inputIndex)
            , Issues(issues)
            , FatalCode(fatalCode)
        {}

        const ui64 InputIndex;
        const TIssues Issues;
        const NYql::NDqProto::StatusIds::StatusCode FatalCode;
    };

    virtual ui64 GetInputIndex() const = 0;

    virtual const TDqAsyncStats& GetIngressStats() const = 0;

    // Gets data and returns space used by filled data batch.
    // Watermark will be returned if source watermark was moved forward. Watermark should be handled AFTER data.
    // Method should be called under bound mkql allocator.
    // Could throw YQL errors.
    virtual i64 GetAsyncInputData(
        NKikimr::NMiniKQL::TUnboxedValueBatch& batch,
        TMaybe<TInstant>& watermark,
        bool& finished,
        i64 freeSpace) = 0;

    // Checkpointing.
    virtual void SaveState(const NDqProto::TCheckpoint& checkpoint, TSourceState& state) = 0;
    virtual void CommitState(const NDqProto::TCheckpoint& checkpoint) = 0; // Apply side effects related to this checkpoint.
    virtual void LoadState(const TSourceState& state) = 0;

    virtual TDuration GetCpuTime() {
        return TDuration::Zero();
    }

    virtual TMaybe<google::protobuf::Any> ExtraData() { return {}; }

    virtual void FillExtraStats(NDqProto::TDqTaskStats* /* stats */, bool /* finalized stats */, const NYql::NDq::TDqMeteringStats*) { }

    // The same signature as IActor::PassAway().
    // It is guaranted that this method will be called with bound MKQL allocator.
    // So, it is the right place to destroy all internal UnboxedValues.
    virtual void PassAway() = 0;

    // Do not destroy UnboxedValues inside destructor!!!
    // It is called from actor system thread, and MKQL allocator is not bound in this case.
    virtual ~IDqComputeActorAsyncInput() = default;
};

// Sink/transform.
// Must be IActor.
//
// Protocol:
// 1. CA starts sink/transform.
// 2. CA runs program and gets results.
// 3. CA calls IDqComputeActorAsyncOutput::SendData().
// 4. If SendData() returns value less than 0, loop stops running until free space appears.
// 5. When free space appears, sink/transform calls ICallbacks::ResumeExecution() to start processing again.
//
// Checkpointing:
// 1. InjectCheckpoint event arrives to CA.
// 2. CA saves its state and injects special checkpoint event to all outputs (TDqComputeActorCheckpoints::ICallbacks::InjectBarrierToOutputs()).
// 3. Sink/transform writes all data before checkpoint.
// 4. Sink/transform waits all external sink's acks for written data.
// 5. Sink/transform gathers its state and passes it into callback ICallbacks::OnAsyncOutputStateSaved(state, outputIndex).
// 6. Checkpoints actor builds state for all task node as sum of the state of CA and all its sinks and saves it.
// 7. ...
// 8. When checkpoint is written into database, checkpoints actor calls IDqComputeActorAsyncOutput::CommitState() to apply all side effects.
struct IDqComputeActorAsyncOutput {
    struct ICallbacks { // Compute actor
        virtual void ResumeExecution(EResumeSource source = EResumeSource::Default) = 0;
        virtual void OnAsyncOutputError(ui64 outputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) = 0;

        // Checkpointing
        virtual void OnAsyncOutputStateSaved(TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) = 0;

        // Finishing
        virtual void OnAsyncOutputFinished(ui64 outputIndex) = 0; // Signal that async output has successfully written its finish flag and so compute actor is ready to finish.

        virtual ~ICallbacks() = default;
    };

    virtual ui64 GetOutputIndex() const = 0;

    virtual i64 GetFreeSpace() const = 0;

    virtual const TDqAsyncStats& GetEgressStats() const = 0;

    // Sends data.
    // Method shoud be called under bound mkql allocator.
    // Could throw YQL errors.
    // Checkpoint (if any) is supposed to be ordered after batch,
    // and finished flag is supposed to be ordered after checkpoint.
    virtual void SendData(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, i64 dataSize,
        const TMaybe<NDqProto::TCheckpoint>& checkpoint, bool finished) = 0;

    // Checkpointing.
    virtual void CommitState(const NDqProto::TCheckpoint& checkpoint) = 0; // Apply side effects related to this checkpoint.
    virtual void LoadState(const TSinkState& state) = 0;

    virtual TMaybe<google::protobuf::Any> ExtraData() { return {}; }

    virtual void PassAway() = 0; // The same signature as IActor::PassAway()

    virtual ~IDqComputeActorAsyncOutput() = default;
};

struct IDqAsyncLookupSource {
    using TKeyTypeHelper = NKikimr::NMiniKQL::TKeyTypeContanerHelper<true, true, false>;
    using TUnboxedValueMap = THashMap<
            NUdf::TUnboxedValue,
            NUdf::TUnboxedValue,
            NKikimr::NMiniKQL::TValueHasher,
            NKikimr::NMiniKQL::TValueEqual,
            NKikimr::NMiniKQL::TMKQLAllocator<std::pair<const NUdf::TUnboxedValue, NUdf::TUnboxedValue>>
    >;
    struct TEvLookupRequest: NActors::TEventLocal<TEvLookupRequest, TDqComputeEvents::EvLookupRequest> {
        TEvLookupRequest(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, TUnboxedValueMap&& request)
            : Alloc(alloc)
            , Request(std::move(request))
        {
        }
        ~TEvLookupRequest() {
            auto guard = Guard(*Alloc);
            TKeyTypeHelper empty;
            Request = TUnboxedValueMap{0, empty.GetValueHash(), empty.GetValueEqual()};
        }
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
        TUnboxedValueMap Request;
    };

    struct TEvLookupResult: NActors::TEventLocal<TEvLookupResult, TDqComputeEvents::EvLookupResult> {
        TEvLookupResult(std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc, TUnboxedValueMap&& result)
            : Alloc(alloc)
            , Result(std::move(result))
        {
        }
        ~TEvLookupResult() {
            auto guard = Guard(*Alloc.get());
            TKeyTypeHelper empty;
            Result = TUnboxedValueMap{0, empty.GetValueHash(), empty.GetValueEqual()};
        }

        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
        TUnboxedValueMap Result;
    };

    virtual size_t GetMaxSupportedKeysInRequest() const = 0;
    //Initiate lookup for requested keys
    //Only one request at a time is allowed. Request must contain no more than GetMaxSupportedKeysInRequest() keys
    //Upon completion, results are sent in TEvLookupResult event to the preconfigured actor
    virtual void AsyncLookup(TUnboxedValueMap&& request) = 0;
protected:
    ~IDqAsyncLookupSource() {}
};

struct IDqAsyncIoFactory : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IDqAsyncIoFactory>;

    struct TSourceArguments {
        const NDqProto::TTaskInput& InputDesc;
        ui64 InputIndex;
        TCollectStatsLevel StatsLevel;
        TTxId TxId;
        ui64 TaskId;
        const THashMap<TString, TString>& SecureParams;
        const THashMap<TString, TString>& TaskParams;
        const TVector<TString>& ReadRanges;
        const NActors::TActorId& ComputeActorId;
        const NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv;
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
        NKikimr::NMiniKQL::TProgramBuilder& ProgramBuilder;
        ::NMonitoring::TDynamicCounterPtr TaskCounters;
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
        IMemoryQuotaManager::TPtr MemoryQuotaManager;
        const google::protobuf::Message* SourceSettings = nullptr;  // used only in case if we execute compute actor locally
        TIntrusivePtr<NActors::TProtoArenaHolder> Arena;  // Arena for SourceSettings
        NWilson::TTraceId TraceId;
    };

    struct TLookupSourceArguments {
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
        std::shared_ptr<IDqAsyncLookupSource::TKeyTypeHelper> KeyTypeHelper;
        NActors::TActorId ParentId;
        google::protobuf::Any LookupSource; //provider specific data source
        const NKikimr::NMiniKQL::TStructType* KeyType;
        const NKikimr::NMiniKQL::TStructType* PayloadType;
        const NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv;
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
        size_t MaxKeysInRequest;
    };

    struct TSinkArguments {
        const NDqProto::TTaskOutput& OutputDesc;
        ui64 OutputIndex;
        TCollectStatsLevel StatsLevel;
        TTxId TxId;
        ui64 TaskId;
        IDqComputeActorAsyncOutput::ICallbacks* Callback;
        const THashMap<TString, TString>& SecureParams;
        const THashMap<TString, TString>& TaskParams;
        const NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv;
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
        IRandomProvider *const RandomProvider;
    };

    struct TInputTransformArguments {
        const NDqProto::TTaskInput& InputDesc;
        const ui64 InputIndex;
        TCollectStatsLevel StatsLevel;
        TTxId TxId;
        ui64 TaskId;
        const NUdf::TUnboxedValue TransformInput;
        const THashMap<TString, TString>& SecureParams;
        const THashMap<TString, TString>& TaskParams;
        const NActors::TActorId& ComputeActorId;
        const NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv;
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
        std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
        NWilson::TTraceId TraceId;
    };

    struct TOutputTransformArguments {
        const NDqProto::TTaskOutput& OutputDesc;
        const ui64 OutputIndex;
        TCollectStatsLevel StatsLevel;
        TTxId TxId;
        ui64 TaskId;
        const IDqOutputConsumer::TPtr TransformOutput;
        IDqComputeActorAsyncOutput::ICallbacks* Callback;
        const THashMap<TString, TString>& SecureParams;
        const THashMap<TString, TString>& TaskParams;
        const NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv;
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
    };

    // Creates source.
    // Could throw YQL errors.
    // IActor* and IDqComputeActorAsyncInput* returned by method must point to the objects with consistent lifetime.
    virtual std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> CreateDqSource(TSourceArguments&& args) const = 0;

    // Creates Lookup source.
    // Could throw YQL errors.
    // IActor* and IDqAsyncLookupSource* returned by method must point to the objects with consistent lifetime.
    virtual std::pair<IDqAsyncLookupSource*, NActors::IActor*> CreateDqLookupSource(TStringBuf type, TLookupSourceArguments&& args) const = 0;

    // Creates sink.
    // Could throw YQL errors.
    // IActor* and IDqComputeActorAsyncOutput* returned by method must point to the objects with consistent lifetime.
    virtual std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> CreateDqSink(TSinkArguments&& args) const = 0;

    // Creates input transform.
    // Could throw YQL errors.
    // IActor* and IDqComputeActorAsyncInput* returned by method must point to the objects with consistent lifetime.
    virtual std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> CreateDqInputTransform(TInputTransformArguments&& args) = 0;

    // Creates output transform.
    // Could throw YQL errors.
    // IActor* and IDqComputeActorAsyncOutput* returned by method must point to the objects with consistent lifetime.
    virtual std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> CreateDqOutputTransform(TOutputTransformArguments&& args) = 0;
};

} // namespace NYql::NDq
