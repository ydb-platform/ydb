#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_database.h>

namespace NKikimr {
namespace NMiniKQL {

class IChangeCollector {
public:
    // basic change record's info
    struct TChange {
        ui64 Order;
        ui64 Group;
        ui64 Step;
        ui64 TxId;
        TPathId PathId;
        ui64 BodySize;
        TPathId TableId;
        ui64 SchemaVersion;
        ui64 LockId = 0;
        ui64 LockOffset = 0;
    };

public:
    virtual ~IChangeCollector() = default;

    virtual bool NeedToReadKeys() const = 0;
    virtual void SetReadVersion(const TRowVersion& readVersion) = 0;
    virtual void SetWriteVersion(const TRowVersion& writeVersion) = 0;
    virtual void SetWriteTxId(ui64 txId) = 0;

    virtual bool Collect(const TTableId& tableId, NTable::ERowOp rop,
        TArrayRef<const TRawTypeValue> key, TArrayRef<const NTable::TUpdateOp> updates) = 0;

    virtual const TVector<TChange>& GetCollected() const = 0;
    virtual TVector<TChange>&& GetCollected() = 0;
    virtual void Reset() = 0;
};

} // NMiniKQL
} // NKikimr

Y_DECLARE_OUT_SPEC(inline, NKikimr::NMiniKQL::IChangeCollector::TChange, o, x) {
    o << "{"
      << " Order: " << x.Order
      << " PathId: " << x.PathId
      << " BodySize: " << x.BodySize
      << " TableId: " << x.TableId
      << " SchemaVersion: " << x.SchemaVersion
    << " }";
}
