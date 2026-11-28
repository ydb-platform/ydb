#include "global_write_txs_repository.h"
#include "schema.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NReplication::NController {

using Schema = TControllerSchema;

std::optional<TMap<TRowVersion, ui64>> TGlobalWriteTxsRepository::Load(NIceDb::TNiceDb& db) {
    auto rowset = db.Table<Schema::TxIds>().Select();
    if (!rowset.IsReady()) {
        return std::nullopt;
    }

    TMap<TRowVersion, ui64> versionToWriteTxId;
    while (!rowset.EndOfSet()) {
        const auto writeTxId = rowset.GetValue<Schema::TxIds::WriteTxId>();
        const auto version = TRowVersion(
            rowset.GetValue<Schema::TxIds::VersionStep>(),
            rowset.GetValue<Schema::TxIds::VersionTxId>()
        );

        auto res = versionToWriteTxId.emplace(version, writeTxId);
        Y_VERIFY_S(res.second, "Duplicate version: " << version);

        if (!rowset.Next()) {
            return std::nullopt;
        }
    }

    return versionToWriteTxId;
}

void TGlobalWriteTxsRepository::SaveChanges(NIceDb::TNiceDb& db, TGlobalWriteTxs& writeTxs) {
    const auto changes = writeTxs.TakeChanges();
    for (const auto& version : changes.Deleted) {
        db.Table<Schema::TxIds>().Key(version.Step, version.TxId).Delete();
    }
    for (const auto& [version, writeTxId] : changes.Inserted) {
        db.Table<Schema::TxIds>().Key(version.Step, version.TxId).Update(
            NIceDb::TUpdate<Schema::TxIds::WriteTxId>(writeTxId)
        );
    }
}

}
