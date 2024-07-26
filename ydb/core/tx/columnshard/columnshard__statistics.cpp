#include "columnshard.h"
#include "columnshard_impl.h"

#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr::NColumnShard {

void TColumnShard::Handle(NStat::TEvStatistics::TEvStatisticsRequest::TPtr& ev, const TActorContext&) {
    auto response = std::make_unique<NStat::TEvStatistics::TEvStatisticsResponse>();
    auto& record = response->Record;
    record.SetShardTabletId(TabletID());

    record.SetStatus(NKikimrStat::TEvStatisticsResponse::STATUS_SUCCESS);

    std::unique_ptr<TCountMinSketch> sketch(TCountMinSketch::Create());
    ui32 value = 1;
    sketch->Count((const char*)&value, sizeof(value));
    TString strSketch(sketch->AsStringBuf());

    auto* column = record.AddColumns();
    column->SetTag(1);

    auto* statistic = column->AddStatistics();
    statistic->SetType(NStat::COUNT_MIN_SKETCH);
    statistic->SetData(std::move(strSketch));

    Send(ev->Sender, response.release(), 0, ev->Cookie);
}

}
