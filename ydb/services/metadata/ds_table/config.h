#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/protos/config.pb.h>
#include <util/datetime/base.h>

namespace NKikimr::NMetadataProvider {

class TConfig {
private:
    YDB_READONLY(TDuration, RefreshPeriod, TDuration::Seconds(10));
    TDuration RetryPeriodStart = TDuration::Seconds(3);
    TDuration RetryPeriodFinish = TDuration::Seconds(30);
    YDB_READONLY_FLAG(Enabled, true);
public:
    TConfig() = default;

    TDuration GetRetryPeriod(const ui32 retry) const;
    bool DeserializeFromProto(const NKikimrConfig::TMetadataProviderConfig& config);
};
}
