#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

ui64 TSchemeShard::AllocateSchemeChangeOrder(NIceDb::TNiceDb& db) {
    ui64 id = ++NextSchemeChangeOrder;
    PersistUpdateNextSchemeChangeOrder(db);
    return id;
}

void TSchemeShard::PersistSchemeChangeRecord(NIceDb::TNiceDb& db, const TSchemeChangeRecordData& entry) {
    using T = Schema::SchemeChangeRecords;
    db.Table<T>().Key(entry.Order).Update(
        NIceDb::TUpdate<T::TxId>(ui64(entry.TxId)),
        NIceDb::TUpdate<T::OperationType>(ui32(entry.TxType)),
        NIceDb::TUpdate<T::PathOwnerId>(entry.PathId.OwnerId),
        NIceDb::TUpdate<T::PathLocalId>(entry.PathId.LocalPathId),
        NIceDb::TUpdate<T::Path>(entry.Path),
        NIceDb::TUpdate<T::ObjectType>(ui32(entry.ObjectType)),
        NIceDb::TUpdate<T::Status>(ui32(entry.Status)),
        NIceDb::TUpdate<T::UserSID>(entry.UserSid),
        NIceDb::TUpdate<T::SchemaVersion>(entry.SchemaVersion),
        NIceDb::TUpdate<T::CompletedAtUs>(entry.CompletedAtUs.MicroSeconds()),
        NIceDb::TUpdate<T::PlanStep>(ui64(entry.PlanStep)),
        NIceDb::TUpdate<T::BodySize>(entry.Body.size())
    );
    if (!entry.Body.empty()) {
        db.Table<Schema::SchemeChangeRecordDetails>().Key(entry.Order).Update(
            NIceDb::TUpdate<Schema::SchemeChangeRecordDetails::Body>(entry.Body)
        );
    }
}

bool TSchemeShard::DeleteAckedSchemeChangeRecords(NIceDb::TNiceDb& db, ui64 oldMinOrder, ui64 newMinOrder) {
    if (newMinOrder <= oldMinOrder) {
        return true;
    }
    auto logRowset = db.Table<Schema::SchemeChangeRecords>()
        .GreaterOrEqual(oldMinOrder + 1)
        .Select();
    if (!logRowset.IsReady()) {
        return false;
    }
    while (!logRowset.EndOfSet()) {
        ui64 order = logRowset.GetValue<Schema::SchemeChangeRecords::Order>();
        if (order > newMinOrder) {
            break;
        }
        db.Table<Schema::SchemeChangeRecords>().Key(order).Delete();
        db.Table<Schema::SchemeChangeRecordDetails>().Key(order).Delete();
        if (!logRowset.Next()) {
            return false;
        }
    }
    return true;
}

} // namespace NKikimr::NSchemeShard
