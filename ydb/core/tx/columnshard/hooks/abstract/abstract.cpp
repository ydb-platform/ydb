#include "abstract.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NYDBTest {

TDuration ICSController::GetGuaranteeIndexationInterval() const {
    const TDuration defaultValue = NColumnShard::TSettings::GuaranteeIndexationInterval;
    return DoGetGuaranteeIndexationInterval(defaultValue);
}

TDuration ICSController::GetPeriodicWakeupActivationPeriod() const {
    const TDuration defaultValue = TDuration::MilliSeconds(GetConfig().GetPeriodicWakeupActivationPeriodMs());
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

bool ICSController::CheckPortionForEvict(const NOlap::TPortionInfo& portion) const {
    return portion.HasRuntimeFeature(NOlap::TPortionInfo::ERuntimeFeature::Optimized) && portion.GetPortionType() == NOlap::EPortionType::Compacted;
}

bool ICSController::CheckPortionsToMergeOnCompaction(const ui64 memoryAfterAdd, const ui32 /*currentSubsetsCount*/) {
    return memoryAfterAdd > GetConfig().GetMemoryLimitMergeOnCompactionRawData();
}

}
