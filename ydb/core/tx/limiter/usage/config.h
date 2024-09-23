#pragma once
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/accessor/accessor.h>
#include <util/datetime/base.h>

namespace NKikimr::NLimiter {

class TConfig {
private:
    YDB_READONLY(TDuration, Period, TDuration::Seconds(1));
    YDB_READONLY(ui64, Limit, 0);
    YDB_READONLY_FLAG(Enabled, true);
public:
    template <class TPolicy>
    bool DeserializeFromProto(const NKikimrConfig::TLimiterConfig& config) {
        if (config.HasPeriodMilliSeconds()) {
            Period = TDuration::MilliSeconds(config.GetPeriodMilliSeconds());
        } else {
            Period = TPolicy::DefaultPeriod;
        }
        if (config.HasLimit()) {
            Limit = config.GetLimit();
        } else {
            Limit = TPolicy::DefaultLimit;
        }
        if (config.HasEnabled()) {
            EnabledFlag = config.GetEnabled();
        } else {
            EnabledFlag = TPolicy::DefaultEnabled;
        }
        return true;
    }

    TString DebugString() const;
};

}
