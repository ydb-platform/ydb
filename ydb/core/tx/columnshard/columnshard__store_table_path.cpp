#include "columnshard_impl.h"
#include "columnshard_schema.h"

#include <ydb/core/tx/columnshard/operations/write.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

namespace NKikimr::NColumnShard {


class TColumnShard::TTxStoreTablePath: public TTransactionBase<TColumnShard> {
private:
    ui64 PathId;
    TString Path;

public:
    TTxStoreTablePath(TColumnShard* self, ui64 pathId, const TString &path)
        : TTransactionBase(self)
        , PathId(pathId)
        , Path(path) {
    }

    TTxType GetTxType() const override {
        return TXTYPE_STORE_TABLE_PATH;
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)
            ("event", "TTxStoreTablePath::Execute")
            ("tablet_id", Self->TabletID());

        Y_ABORT_UNLESS(Self->TablesManager.HasTable(PathId));

        txc.DB.NoMoreReadsForTx();

        TTableInfo tableInfo = Self->TablesManager.GetTable(PathId);
        tableInfo.Path = Path;

        NIceDb::TNiceDb db(txc.DB);
        Self->TablesManager.UpdateTable(std::move(tableInfo), db);

        return true;
    }

    void Complete(const TActorContext&) override {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)
            ("event", "TTxStoreTablePath::Complete")
            ("tablet_id", Self->TabletID());
    }
};


void TColumnShard::Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr ev, const TActorContext &ctx) {
    const auto &rec = ev->Get()->GetRecord();

    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD_TX)
        ("event", "DescribeSchemeResult")
        ("tablet_id", TabletID())
        ("record", rec.ShortDebugString());

    ui64 pathId = rec.GetPathId();
    if (!TablesManager.HasTable(pathId)) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_TX)
            ("event", "DescribeSchemeResult")
            ("tablet_id", TabletID())
            ("path_id", pathId)
            ("error", "Shard got describe result for unknown table");
        return;
    }

    if (!rec.GetPath()) {
        AFL_CRIT(NKikimrServices::TX_COLUMNSHARD_TX)
            ("event", "DescribeSchemeResult")
            ("tablet_id", TabletID())
            ("path_id", pathId)
            ("status", rec.GetStatus())
            ("error", "Shard couldn't get path for table");
        return;
    }
    Execute(new TTxStoreTablePath(this, pathId, rec.GetPath()), ctx);
}

}   // namespace NKikimr::NColumnShard
