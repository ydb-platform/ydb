#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/protos/config.pb.h>
#include <util/datetime/base.h>

namespace NKikimr::NInternal::NRequest {

class TConfig {
private:
    TDuration RetryPeriodStart = TDuration::Seconds(3);
    TDuration RetryPeriodFinish = TDuration::Seconds(30);
public:
    TConfig() = default;

    TDuration GetRetryPeriod(const ui32 retry) const;
    bool DeserializeFromProto(const NKikimrConfig::TInternalRequestConfig& config);
};
}
