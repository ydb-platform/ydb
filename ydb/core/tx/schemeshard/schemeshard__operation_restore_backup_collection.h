#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

// Forward declarations for restore backup collection operations

/**
 * Creates incremental backup path state operations for each table in each incremental backup.
 * This is used to set up the proper path states for incremental restore operations.
 * 
 * @param opId Operation ID
 * @param tx Transaction containing the restore backup collection operation
 * @param bc Backup collection information
 * @param bcPath Path to the backup collection
 * @param incrBackupNames List of incremental backup names
 * @param context Operation context
 * @param result Vector to store the created sub-operations
 * @return true if all operations were created successfully, false otherwise
 */
bool CreateIncrementalBackupPathStateOps(
    TOperationId opId,
    const TTxTransaction& tx,
    const TBackupCollectionInfo::TPtr& bc,
    const TPath& bcPath,
    const TVector<TString>& incrBackupNames,
    TOperationContext& context,
    TVector<ISubOperation::TPtr>& result);

/**
 * Factory function to create a long incremental restore operation control plane.
 * 
 * @param opId Operation ID
 * @param tx Transaction containing the operation
 * @return Sub-operation pointer
 */
ISubOperation::TPtr CreateLongIncrementalRestoreOpControlPlane(TOperationId opId, const TTxTransaction& tx);

/**
 * Factory function to create a long incremental restore operation control plane from a state.
 * 
 * @param opId Operation ID
 * @param state Transaction state
 * @return Sub-operation pointer
 */
ISubOperation::TPtr CreateLongIncrementalRestoreOpControlPlane(TOperationId opId, TTxState::ETxState state);

/**
 * Creates the restore backup collection operations.
 * This is the main entry point for restore backup collection operations.
 * 
 * @param opId Operation ID
 * @param tx Transaction containing the restore backup collection operation
 * @param context Operation context
 * @return Vector of sub-operations
 */
TVector<ISubOperation::TPtr> CreateRestoreBackupCollection(TOperationId opId, const TTxTransaction& tx, TOperationContext& context);

} // namespace NKikimr::NSchemeShard
