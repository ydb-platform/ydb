#include "columnshard_impl.h"

#include <ydb/core/tx/data_events/backup_events.h>

namespace NKikimr::NColumnShard {

void TColumnShard::Handle(NEvents::TBackupEvents::TEvBackupShardPropose::TPtr& ev, const TActorContext& ctx) {
    NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletID())("event", "TBackupEvents");

    const auto source = ev->Sender;
    auto result = std::make_unique<NEvents::TBackupEvents::TEvBackupShardProposeResult>();

    ctx.Send(source, result.release());
}

} // namespace NKikimr::NColumnShard