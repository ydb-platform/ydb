#pragma once
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/runtime/dq_output_consumer.h>
#include <ydb/library/yql/dq/runtime/dq_async_output.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <util/generic/ptr.h>

#include <memory>
#include <utility>

namespace NYql::NDqProto {
class TCheckpoint;
class TTaskOutput;
class TSinkState;
} // namespace NYql::NDqProto

namespace NActors {
class IActor;
} // namespace NActors

namespace NKikimr::NMiniKQL {
class TProgramBuilder;
} // namespace NKikimr::NMiniKQL

namespace NYql::NDq {

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
        virtual void ResumeExecution() = 0;
        virtual void OnAsyncOutputError(ui64 outputIndex, const TIssues& issues, bool isFatal) = 0;

        // Checkpointing
        virtual void OnAsyncOutputStateSaved(NDqProto::TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) = 0;

        virtual ~ICallbacks() = default;
    };

    virtual ui64 GetOutputIndex() const = 0;

    virtual i64 GetFreeSpace() const = 0;

    // Sends data.
    // Method shoud be called under bound mkql allocator.
    // Could throw YQL errors.
    // Checkpoint (if any) is supposed to be ordered after batch,
    // and finished flag is supposed to be ordered after checkpoint.
    virtual void SendData(NKikimr::NMiniKQL::TUnboxedValueVector&& batch, i64 dataSize,
        const TMaybe<NDqProto::TCheckpoint>& checkpoint, bool finished) = 0;

    // Checkpointing.
    virtual void CommitState(const NDqProto::TCheckpoint& checkpoint) = 0; // Apply side effects related to this checkpoint.
    virtual void LoadState(const NDqProto::TSinkState& state) = 0;

    virtual void PassAway() = 0; // The same signature as IActor::PassAway()

    virtual ~IDqComputeActorAsyncOutput() = default;
};

struct IDqSinkFactory : public TThrRefBase {
    using TPtr = TIntrusivePtr<IDqSinkFactory>;

    struct TArguments {
        const NDqProto::TTaskOutput& OutputDesc;
        ui64 OutputIndex;
        TTxId TxId;
        IDqComputeActorAsyncOutput::ICallbacks* Callback;
        const THashMap<TString, TString>& SecureParams;
        const NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv;
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
    };

    // Creates sink.
    // Could throw YQL errors.
    // IActor* and IDqComputeActorAsyncOutput* returned by method must point to the objects with consistent lifetime.
    virtual std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> CreateDqSink(TArguments&& args) const = 0;
};

struct IDqOutputTransformFactory : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IDqOutputTransformFactory>;

    struct TArguments {
        const NDqProto::TTaskOutput& OutputDesc;
        const ui64 OutputIndex;
        TTxId TxId;
        const IDqOutputConsumer::TPtr TransformOutput;
        IDqComputeActorAsyncOutput::ICallbacks* Callback;
        const THashMap<TString, TString>& SecureParams;
        const NKikimr::NMiniKQL::TTypeEnvironment& TypeEnv;
        const NKikimr::NMiniKQL::THolderFactory& HolderFactory;
        NKikimr::NMiniKQL::TProgramBuilder& ProgramBuilder;
    };

    virtual std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> CreateDqOutputTransform(TArguments&& args) = 0;
};

} // namespace NYql::NDq
