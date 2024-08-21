#include "abstract.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NYDBTest {

const NKikimrConfig::TColumnShardConfig ICSController::DefaultAppDataConfig = {};

TDuration ICSController::GetGuaranteeIndexationInterval() const {
    const TDuration defaultValue = NColumnShard::TSettings::GuaranteeIndexationInterval;
    return DoGetGuaranteeIndexationInterval(defaultValue);
}

TDuration ICSController::GetPeriodicWakeupActivationPeriod() const {
    const TDuration defaultValue = NColumnShard::TSettings::DefaultPeriodicWakeupActivationPeriod;
    return DoGetPeriodicWakeupActivationPeriod(defaultValue);
}

TDuration ICSController::GetStatsReportInterval() const {
    const TDuration defaultValue = NColumnShard::TSettings::DefaultStatsReportInterval;
    return DoGetStatsReportInterval(defaultValue);
}

ui64 ICSController::GetGuaranteeIndexationStartBytesLimit() const {
    const ui64 defaultValue = NColumnShard::TSettings::GuaranteeIndexationStartBytesLimit;
    return DoGetGuaranteeIndexationStartBytesLimit(defaultValue);
}
}
