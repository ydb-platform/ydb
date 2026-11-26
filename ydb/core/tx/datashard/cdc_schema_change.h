#pragma once

#include "datashard_impl.h"

namespace NKikimr::NDataShard {

// The synthetic row ID is sent by schema-aware CDC once its column is added,
// before the backfill writes the assigned IDs. It remains hidden from SQL and
// ordinary CDC while the column build is running.
bool IsCdcSchemaColumnVisible(const TUserTable::TUserColumn& column);

// Persist one complete schema event per schema-enabled CDC stream in the current transaction.
void PersistCdcSchemaChange(TDataShard& dataShard, NTabletFlatExecutor::TTransactionContext& txc,
    TOperation::TPtr op, const TPathId& tableId, const TUserTable& table, const TPathId& onlyStream = {});

}
