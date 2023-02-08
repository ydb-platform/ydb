#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_database.h>

namespace NKikimr {
namespace NMiniKQL {

class IChangeCollector {
public:
    virtual ~IChangeCollector() = default;

    virtual bool OnUpdate(const TTableId& tableId, ui32 localTid, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TUpdateOp> updates,
        const TRowVersion& writeVersion) = 0;

    virtual bool OnUpdateTx(const TTableId& tableId, ui32 localTid, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TUpdateOp> updates,
        ui64 writeTxId) = 0;
};

} // NMiniKQL
} // NKikimr
