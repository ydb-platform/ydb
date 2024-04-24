#pragma once

#include <ydb/core/fq/libs/checkpointing_common/defs.h>

#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/ptr.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

class IStateStorage : public virtual TThrRefBase {
public:
    using TGetStateResult = std::pair<std::vector<NYql::NDq::TComputeActorState>, NYql::TIssues>;
    using TSaveStateResult = std::pair<size_t, NYql::TIssues>;
    using TCountStatesResult = std::pair<size_t, NYql::TIssues>;

    virtual NThreading::TFuture<NYql::TIssues> Init() = 0;

    virtual NThreading::TFuture<TSaveStateResult> SaveState(
        ui64 taskId,
        const TString& graphId,
        const TCheckpointId& checkpointId,
        const NYql::NDq::TComputeActorState& state) = 0;

    virtual NThreading::TFuture<TGetStateResult> GetState(
        const std::vector<ui64>& taskIds,
        const TString& graphId,
        const TCheckpointId& checkpointId) = 0;

    virtual NThreading::TFuture<TCountStatesResult> CountStates(
        const TString& graphId,
        const TCheckpointId& checkpointId) = 0;

    // GC interface

    virtual NThreading::TFuture<NYql::TIssues> DeleteGraph(
        const TString& graphId) = 0;

    virtual NThreading::TFuture<NYql::TIssues> DeleteCheckpoints(
        const TString& graphId,
        const TCheckpointId& checkpointId) = 0;
};

using TStateStoragePtr = TIntrusivePtr<IStateStorage>;

} // namespace NFq
