#pragma once

#include "object.h"

namespace NKikimr::NKqp {

class IStreamingQueryOperationController : public NMetadata::NModifications::IAlterController {
public:
    using TPtr = std::shared_ptr<IStreamingQueryOperationController>;

    virtual void OnAlteringFinishedWithStatus(const TStreamingQueryConfig::TStatus& status) = 0;
};

// In case of an intermediate failure, all operations on streaming queries will be continued only on the next operation.

// Current assumptions on successfully restarting a previously running streaming query:
// - After the restart, the previous query execution may still commit task states into
//   the `.metadata/streaming/checkpoints/states` table for checkpoints in the `Pending` status
// - The previous execution may continue working and read from / write into external systems
//
// Also, the row for the streaming query in the `.metadata/streaming/queries` table may be allocated even when the query is not actually created in SS
void DoCreateStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, IStreamingQueryOperationController::TPtr controller, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context);

// Current assumptions after a successful alter that stops a previously running streaming query:
// - The previous query execution may perform all checkpoint actions:
//   - Register a new coordinator in the `.metadata/streaming/coordinators_sync` table
//   - Register checkpoints and change the status of checkpoints in the `.metadata/streaming/checkpoints_metadata` table
//   - Save new graph descriptions in the `.metadata/streaming/checkpoints_graphs_description` table
//   - Save task states into the `.metadata/streaming/checkpoints/states` table
// - The previous execution may continue working and read from / write into external systems
//
// Also, in case of concurrent creates / alters / drops, the streaming query SS state may be unsynchronized with the actual runtime state
// (synchronization will be performed before the next operation on this query)
void DoAlterStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, IStreamingQueryOperationController::TPtr controller, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context);

// Current assumptions after a successful drop of a streaming query:
// - The previous query execution may perform all checkpoint actions, and all query checkpoint data
//   is not cleaned during the drop operation (all tables in `.metadata/streaming/checkpoints/` stay the same)
// - The previous execution may continue working and read from / write into external systems
// - The row for the streaming query in the `.metadata/streaming/queries` table may not be dropped after removing the query from SS
void DoDropStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, IStreamingQueryOperationController::TPtr controller, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context);

}  // namespace NKikimr::NKqp
