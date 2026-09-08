#pragma once

#include "object.h"

namespace NKikimr::NKqp {

class IStreamingQueryOperationController : public NMetadata::NModifications::IAlterController {
public:
    using TPtr = std::shared_ptr<IStreamingQueryOperationController>;

    virtual void OnAlteringFinishedWithStatus(const TStreamingQueryConfig::TStatus& status) = 0;
};

// Common operation execution pipeline:
// 1. Register operation in SS
// 2. Take operation lock via one row in metadata table
// 3. Perform actions while lock still valid
// 4. Remove lock
// 5. Finish operation in SS

// Current assumptions on successfully restarting a previously running streaming query:
// - After the restart, the previous query execution may still commit task states into
//   the `.metadata/streaming/checkpoints/states` table for checkpoints in the `Pending` status
// - The previous execution may continue working and read from / write into external systems
void DoCreateStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, IStreamingQueryOperationController::TPtr controller, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context);

// Current assumptions after a successful alter that stops a previously running streaming query:
// - The previous query execution may perform all checkpoint actions:
//   - Register a new coordinator in the `.metadata/streaming/coordinators_sync` table
//   - Register checkpoints and change the status of checkpoints in the `.metadata/streaming/checkpoints_metadata` table
//   - Save new graph descriptions in the `.metadata/streaming/checkpoints_graphs_description` table
//   - Save task states into the `.metadata/streaming/checkpoints/states` table
// - The previous execution may continue working and read from / write into external systems
void DoAlterStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, IStreamingQueryOperationController::TPtr controller, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context);

// Current assumptions after a successful drop of a streaming query:
// - The previous query execution may perform all checkpoint actions, and all query checkpoint data
//   is not cleaned during the drop operation (all tables in `.metadata/streaming/checkpoints/` stay the same)
// - The previous execution may continue working and read from / write into external systems
void DoDropStreamingQuery(const NKikimrSchemeOp::TModifyScheme& schemeTx, IStreamingQueryOperationController::TPtr controller, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context);

void DoTrackStreamingQueryOperation(const TString& queryName, IStreamingQueryOperationController::TPtr controller, const NMetadata::NModifications::IOperationsManager::TOperationTrackContext& context);

}  // namespace NKikimr::NKqp
