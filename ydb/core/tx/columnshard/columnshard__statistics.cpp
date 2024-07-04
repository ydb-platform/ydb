#include "columnshard.h"
#include "columnshard_impl.h"

#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr::NColumnShard {

void TColumnShard::Handle(NStat::TEvStatistics::TEvStatisticsRequest::TPtr& ev, const TActorContext&) {
    auto response = std::make_unique<NStat::TEvStatistics::TEvStatisticsResponse>();
    auto& record = response->Record;
    record.SetShardTabletId(TabletID());

    record.SetStatus(NKikimrStat::TEvStatisticsResponse::SUCCESS);

    Send(ev->Sender, response.release(), 0, ev->Cookie);
}

}
