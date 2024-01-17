#pragma once

#include <ydb/core/fq/libs/checkpointing_common/defs.h>
#include <ydb/core/fq/libs/checkpoint_storage/proto/graph_description.pb.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/ptr.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

class ICheckpointStorage : public virtual TThrRefBase {
public:
    using TGetCheckpointsResult = std::pair<TCheckpoints, NYql::TIssues>;
    using TGetCoordinatorsResult = std::pair<TCoordinators, NYql::TIssues>;
    using TGetTotalCheckpointsStateSizeResult = std::pair<ui64, NYql::TIssues>;
    using TCreateCheckpointResult = std::pair<TString, NYql::TIssues>; // graphDescId for subsequent usage.

    virtual NThreading::TFuture<NYql::TIssues> Init() = 0;

    virtual NThreading::TFuture<NYql::TIssues> RegisterGraphCoordinator(
        const TCoordinatorId& coordinator) = 0;

    virtual NThreading::TFuture<TGetCoordinatorsResult> GetCoordinators() = 0;

    virtual NThreading::TFuture<TCreateCheckpointResult> CreateCheckpoint(
        const TCoordinatorId& coordinator,
        const TCheckpointId& checkpointId,
        const TString& graphDescId,
        ECheckpointStatus status) = 0;

    virtual NThreading::TFuture<TCreateCheckpointResult> CreateCheckpoint(
        const TCoordinatorId& coordinator,
        const TCheckpointId& checkpointId,
        const NProto::TCheckpointGraphDescription& graphDesc,
        ECheckpointStatus status) = 0;

    virtual NThreading::TFuture<NYql::TIssues> UpdateCheckpointStatus(
        const TCoordinatorId& coordinator,
        const TCheckpointId& checkpointId,
        ECheckpointStatus newStatus,
        ECheckpointStatus prevStatus,
        ui64 stateSizeBytes) = 0;

    virtual NThreading::TFuture<NYql::TIssues> AbortCheckpoint(
        const TCoordinatorId& coordinator,
        const TCheckpointId& checkpointId) = 0;

    virtual NThreading::TFuture<TGetCheckpointsResult> GetCheckpoints(const TString& graph) = 0;
    virtual NThreading::TFuture<TGetCheckpointsResult> GetCheckpoints(
        const TString& graph, const TVector<ECheckpointStatus>& statuses, ui64 limit, bool loadGraphDescription = false) = 0;

    // GC interface
    // Note that no coordinator check required
    // Also there is no check of current checkpoint state, it's
    // up to caller to check at first

    virtual NThreading::TFuture<NYql::TIssues> DeleteGraph(
        const TString& graphId) = 0;

    virtual NThreading::TFuture<NYql::TIssues> MarkCheckpointsGC(
        const TString& graphId,
        const TCheckpointId& checkpointUpperBound) = 0;

    // will only delete checkpoints marked as GC
    virtual NThreading::TFuture<NYql::TIssues> DeleteMarkedCheckpoints(
        const TString& graphId,
        const TCheckpointId& checkpointUpperBound) = 0;

    virtual NThreading::TFuture<ICheckpointStorage::TGetTotalCheckpointsStateSizeResult> GetTotalCheckpointsStateSize(const TString& graphId) = 0;
};

using TCheckpointStoragePtr = TIntrusivePtr<ICheckpointStorage>;

} // namespace NFq
