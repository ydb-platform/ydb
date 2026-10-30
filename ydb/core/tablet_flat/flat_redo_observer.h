#pragma once

#include "flat_update_op.h"
#include "flat_sausage_solid.h"
#include <ydb/core/base/row_version.h>

namespace NKikimr::NTable {

// Optional follower-only observer. Called after each accepted redo operation,
// in the executor transaction zone, with external values resolved by Annex.
// Truncate notifications precede redo updates from the same transaction;
// the corresponding physical part switch may be applied later.
struct IRedoObserver {
    virtual ~IRedoObserver() = default;
    virtual void OnRedoUpdate(ui32 table, ERowOp op, TArrayRef<const TRawTypeValue> key,
        TArrayRef<const TUpdateOp> updates, TArrayRef<const NPageCollection::TMemGlob> annex,
        TRowVersion version, ui64 txId, ui32 savepoint) = 0;
    virtual void OnRedoCommit(ui32 table, ui64 txId, TRowVersion version) = 0;
    virtual void OnRedoRemove(ui32 table, ui64 txId) = 0;
    virtual void OnRedoTruncate(ui32 /*table*/) {}
};

} // namespace NKikimr::NTable
