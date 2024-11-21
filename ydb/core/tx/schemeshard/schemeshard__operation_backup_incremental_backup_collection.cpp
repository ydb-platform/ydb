#include "schemeshard__operation_common.h"
#include "schemeshard__operation_create_cdc_stream.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/schemeshard/backup/constants.h>

namespace NKikimr::NSchemeShard {

TVector<ISubOperation::TPtr> CreateBackupIncrementalBackupCollection(TOperationId opId, const TTxTransaction& tx, TOperationContext& context) {
    Y_UNUSED(opId, tx, context);
    return {};
}

} // namespace NKikimr::NSchemeShard
