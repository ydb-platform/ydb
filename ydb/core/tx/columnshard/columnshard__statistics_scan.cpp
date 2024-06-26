#include "columnshard.h"
#include "columnshard_impl.h"

#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr::NColumnShard {

void TColumnShard::Handle(TEvColumnShard::TEvStatisticsScanRequest::TPtr& ev, const TActorContext& ctx) {
    auto response = std::make_unique<TEvDataShard::TEvStatisticsScanResponse>();
    auto& record = response->Record;
    record.SetShardTabletId(TabletID());

    record.SetStatus(NKikimrStat::TEvStatisticsScanResponse::SUCCESS);

    ctx.Send(new IEventHandle(ev->Sender, TActorId(), response.release(), 0, ev->Cookie));
}

}
