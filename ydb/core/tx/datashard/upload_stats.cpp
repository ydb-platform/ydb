#include "upload_stats.h"

#include <ydb/core/base/appdata.h>

#include <util/string/builder.h>

NKikimr::NDataShard::TUploadMonStats::TUploadMonStats(const TString service, const TString subsystem) {
    auto counters = GetServiceCounters(AppData()->Counters, service)->GetSubgroup("subsystem", subsystem);

    MonBytes = counters->GetCounter("Bytes", true);
    MonRows = counters->GetCounter("Records", true);
}

void NKikimr::NDataShard::TUploadMonStats::Aggr(ui64 rows, ui64 bytes) {
    if (MonRows && MonBytes) {
        *MonRows += rows;
        *MonBytes += bytes;
    }
    RowsSent += rows;
    BytesSent += bytes;
}

void NKikimr::NDataShard::TUploadMonStats::Aggr(const NKikimr::NDataShard::IStatHolder *other) {
    Aggr(other->GetRows(), other->GetBytes());
}

TString NKikimr::NDataShard::TUploadMonStats::ToString() const {
    return TStringBuilder()
            << "Stats { "
            << " RowsSent: " << RowsSent
            << " BytesSent: " << BytesSent
            << " }";
}
