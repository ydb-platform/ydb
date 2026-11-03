#pragma once

#include "datashard_impl.h"

namespace NKikimr::NDataShard {

// Persist one complete schema event per schema-enabled CDC stream in the current transaction.
void PersistCdcSchemaChange(TDataShard& dataShard, NTabletFlatExecutor::TTransactionContext& txc,
    TOperation::TPtr op, const TPathId& tableId, const TUserTable& table);

}
