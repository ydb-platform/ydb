#pragma once

#include "schemeshard__operation_part.h"
#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

/**
 * Creates an incremental restore finalization operation that properly handles
 * path state cleanup after all incremental backups have been processed.
 * This operation releases all affected paths to EPathStateNoChanges and
 * performs final cleanup of the incremental restore process.
 * 
 * @param opId Operation ID
 * @param tx Transaction containing the finalization operation
 * @return Sub-operation pointer
 */
ISubOperation::TPtr CreateIncrementalRestoreFinalize(TOperationId opId, const TTxTransaction& tx);

/**
 * Factory function to create an incremental restore finalization sub-operation from a state.
 * 
 * @param opId Operation ID
 * @param state Transaction state
 * @return Sub-operation pointer
 */
ISubOperation::TPtr CreateIncrementalRestoreFinalize(TOperationId opId, TTxState::ETxState state);

}
