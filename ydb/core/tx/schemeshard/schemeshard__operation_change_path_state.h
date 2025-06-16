#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

/**
 * Creates a change path state operation for modifying path states during operations.
 * This operation is used to change the state of a path element in the schema shard.
 * !!WARNING!! The state of the path element is not persisted in the database.
 * It must be only used inside long (async) operations.
 * Correct path state must be restored on startup of schemeshard based on local db.
 * This code MUST be written by developer.
 * 
 * @param opId Operation ID
 * @param tx Transaction containing the change path state operation
 * @param context Operation context
 * @param result Vector to store the created sub-operations
 * @return true if the operation was created successfully, false otherwise
 */
bool CreateChangePathState(TOperationId opId, const TTxTransaction& tx, TOperationContext& context, TVector<ISubOperation::TPtr>& result);

/**
 * Factory function to create a change path state sub-operation from a transaction.
 * 
 * @param opId Operation ID
 * @param tx Transaction containing the change path state operation
 * @return Sub-operation pointer
 */
ISubOperation::TPtr CreateChangePathState(TOperationId opId, const TTxTransaction& tx);

/**
 * Factory function to create a change path state sub-operation from a state.
 * 
 * @param opId Operation ID
 * @param state Transaction state
 * @return Sub-operation pointer
 */
ISubOperation::TPtr CreateChangePathState(TOperationId opId, TTxState::ETxState state);

/**
 * Creates a vector of change path state operations from a transaction.
 * 
 * @param opId Operation ID
 * @param tx Transaction containing the change path state operation
 * @param context Operation context
 * @return Vector of sub-operations
 */
TVector<ISubOperation::TPtr> CreateChangePathState(TOperationId opId, const TTxTransaction& tx, TOperationContext& context);

} // namespace NKikimr::NSchemeShard
