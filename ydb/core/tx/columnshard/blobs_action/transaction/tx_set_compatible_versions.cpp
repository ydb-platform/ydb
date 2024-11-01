#include "tx_set_compatible_versions.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

bool TTxSetCompatibleSchemaVersions::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    NOlap::TDbWrapper db(txc.DB, nullptr);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "TTxSetCompatibleSchemaVersions::Execute")("tablet_id", Self->TabletID());
    Self->ChangeSchemaVersionsToLastCompatible(db);
    return true;
}

void TTxSetCompatibleSchemaVersions::Complete(const TActorContext& /*ctx*/) {
}

}
