#include "keyvalue_simple_db_flat.h"
#include "keyvalue_scheme_flat.h"

#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/tablet_flat/flat_database.h>

namespace NKikimr {
namespace NKeyValue {


TSimpleDbFlat::TSimpleDbFlat(NTable::TDatabase &db, TVector<TLogoBlobID>& trashBeingCommitted)
    : Db(db)
    , TrashBeingCommitted(trashBeingCommitted)
{}

void TSimpleDbFlat::Erase(const TString &key, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    const auto keyStr = NScheme::TSmallBoundedString::TInstance(key);
    const TRawTypeValue rawTypeValue = (TRawTypeValue)keyStr;
    Db.Update(TABLE_ID, NTable::ERowOp::Erase, {&rawTypeValue, 1}, { });
}

void TSimpleDbFlat::Update(const TString &key, const TString &value, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    const auto keyStr = NScheme::TSmallBoundedString::TInstance(key);
    const TRawTypeValue rawTypeValue = (TRawTypeValue)keyStr;
    const auto valueStr = NScheme::TString::TInstance(value);
    NTable::TUpdateOp ops(VALUE_TAG, NTable::ECellOp::Set, valueStr);
    Db.Update(TABLE_ID, NTable::ERowOp::Upsert, { rawTypeValue }, { ops });
}

void TSimpleDbFlat::AddTrash(const TLogoBlobID& id) {
    TrashBeingCommitted.push_back(id);
}

} // NKeyValue
} // NKikimr
