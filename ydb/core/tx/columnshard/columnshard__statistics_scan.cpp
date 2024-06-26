#include "columnshard.h"
#include "columnshard_impl.h"

#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr::NColumnShard {

void TColumnShard::Handle(TEvColumnShard::TEvStatisticsRequest::TPtr& ev, const TActorContext& ctx) {
    auto response = std::make_unique<TEvDataShard::TEvStatisticsResponse>();
    auto& record = response->Record;
    record.SetShardTabletId(TabletID());

    record.SetStatus(NKikimrStat::TEvStatisticsResponse::SUCCESS);

    Send(new IEventHandle(ev->Sender, response.release(), 0, ev->Cookie));
}

}
