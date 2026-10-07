#include "abstract.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>

#include <library/cpp/threading/atomic_shared_ptr/atomic_shared_ptr.h>

namespace NKikimr::NYDBTest {

namespace {

// Tablets read the controller from actor threads while tests replace it. A reader keeps the holder
// alive while it copies the pointer out. Kept out of abstract.h, which is included too widely.
TTrueAtomicSharedPtr<ICSController::TPtr>& CSControllerHolder() {
    static TTrueAtomicSharedPtr<ICSController::TPtr> holder = MakeTrueAtomicShared<ICSController::TPtr>(std::make_shared<ICSController>());
    return holder;
}

}   // namespace

void TControllers::ReplaceCSController(const ICSController::TPtr& newController) {
    CSControllerHolder().atomic_store(MakeTrueAtomicShared<ICSController::TPtr>(newController));
}

ICSController::TPtr TControllers::GetColumnShardController() {
    return *CSControllerHolder().atomic_load();
}

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
    return portion.HasRuntimeFeature(NOlap::TPortionInfo::ERuntimeFeature::Optimized);
}

bool ICSController::CheckPortionsToMergeOnCompaction(const ui64 memoryAfterAdd, const ui32 /*currentSubsetsCount*/) {
    return memoryAfterAdd > GetConfig().GetMemoryLimitMergeOnCompactionRawData();
}

}   // namespace NKikimr::NYDBTest
