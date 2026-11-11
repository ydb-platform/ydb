#include "columnshard_impl.h"

#include "maintenance/move_data/move_data.h"

namespace NKikimr::NColumnShard {

void TColumnShard::Handle(TEvTablet::TEvMoveData::TPtr& ev, const TActorContext& ctx) {
    if (!HasAppData() || !AppData()->FeatureFlags.GetEnableColumnshardMoveData()) {
        TTabletExecutedFlat::Handle(ev);
        return;
    }
    TMoveDataDriver::StartOrUpdateMigration(*this, ev->Get()->Record, ev->Sender, ctx);
}

void TColumnShard::MoveDataCompleted(const TActorContext& ctx) {
    if (MoveDataDriver != nullptr) {
        MoveDataDriver->OnVacuumCompleted(ctx);
    }
}

}   // namespace NKikimr::NColumnShard
