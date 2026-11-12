#include "events.h"

#include <util/string/builder.h>

namespace NKikimr::NCdcAggregator {

TString TEvCdcAggregatorService::TEvWriteHeartbeat::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " RecipientTabletId: " << RecipientTabletId
        << " SourceDataShardId: " << SourceDataShardId
        << " Version: " << Heartbeat.Version
        << " DataSize: " << Heartbeat.Data.size()
    << " }";
}

TString TEvCdcAggregatorService::TEvWriteHeartbeatResult::ToString() const {
    return TStringBuilder() << ToStringHeader() << " {"
        << " Status: " << Status
        << " WrittenVersion: " << WrittenVersion
        << " Error: " << Error
    << " }";
}

} // namespace NKikimr::NCdcAggregator
