#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_database.h>

namespace NACLib {
    class TUserContext;
}

namespace NKikimr {
namespace NMiniKQL {

class IChangeCollector {
public:
    virtual ~IChangeCollector() = default;

    virtual bool OnUpdate(const TTableId& tableId, ui32 localTid, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TUpdateOp> updates,
        const TRowVersion& writeVersion, TIntrusivePtr<NACLib::TUserContext> userCtx) = 0;

    virtual bool OnUpdateTx(const TTableId& tableId, ui32 localTid, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TUpdateOp> updates,
        ui64 writeTxId, TIntrusivePtr<NACLib::TUserContext> userCtx) = 0;
};

} // NMiniKQL
} // NKikimr
