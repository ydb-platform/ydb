#include "selector.h"
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr::NOlap::NExport {

std::unique_ptr<NKikimr::TEvDataShard::TEvKqpScan> TBackupSelector::DoBuildRequestInitiator(const TCursor& cursor) const {
    auto ev = std::make_unique<TEvDataShard::TEvKqpScan>();
    ev->Record.SetLocalPathId(TablePathId);

    auto protoRanges = ev->Record.MutableRanges();

    if (cursor.HasLastKey()) {
        auto* newRange = protoRanges->Add();
        TSerializedTableRange(TSerializedCellVec::Serialize(*cursor.GetLastKey()), {}, false, false).Serialize(*newRange);
    }

    ev->Record.MutableSnapshot()->SetStep(Snapshot.GetPlanStep());
    ev->Record.MutableSnapshot()->SetTxId(Snapshot.GetTxId());
    ev->Record.SetStatsMode(NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_NONE);
    ev->Record.SetScanId(TablePathId);
    ev->Record.SetTxId(TablePathId);
    ev->Record.SetTablePath(TableName);
    ev->Record.SetSchemaVersion(0);

    ev->Record.SetReverse(false);
    ev->Record.SetDataFormat(NKikimrDataEvents::EDataFormat::FORMAT_ARROW);
    return ev;
}

}
