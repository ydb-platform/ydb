#pragma once

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_database.h>

namespace NKikimr {
namespace NMiniKQL {

class IChangeCollector {
public:
    // basic change record's info
    struct TChange: public std::tuple<ui64, TPathId, ui64, TPathId, ui64> {
        using std::tuple<ui64, TPathId, ui64, TPathId, ui64>::tuple;

        ui64 Order() const { return std::get<0>(*this); }
        const TPathId& PathId() const { return std::get<1>(*this); }
        ui64 BodySize() const { return std::get<2>(*this); }
        const TPathId& TableId() const { return std::get<3>(*this); }
        ui64 SchemaVersion() const { return std::get<4>(*this); }

        void SetPathId(const TPathId& value) { std::get<1>(*this) = value; }
    };

public:
    virtual ~IChangeCollector() = default;

    virtual bool NeedToReadKeys() const = 0;
    virtual void SetReadVersion(const TRowVersion& readVersion) = 0;
    virtual void SetWriteVersion(const TRowVersion& writeVersion) = 0;

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
      << " Order: " << x.Order()
      << " PathId: " << x.PathId()
      << " BodySize: " << x.BodySize()
      << " TableId: " << x.TableId()
      << " SchemaVersion: " << x.SchemaVersion()
    << " }";
}
