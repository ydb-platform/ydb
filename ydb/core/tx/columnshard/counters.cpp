#include "counters.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr::NColumnShard {

TScanCounters::TScanCounters(const TString& module) {
    ::NMonitoring::TDynamicCounterPtr subGroup = GetServiceCounters(AppData()->Counters, "tablets")->GetSubgroup("subsystem", "columnshard");

    PortionBytes = subGroup->GetCounter(module + "/PortionBytes", true);
    FilterBytes = subGroup->GetCounter(module + "/FilterBytes", true);
    PostFilterBytes = subGroup->GetCounter(module + "/PostFilterBytes", true);

    AssembleFilterCount = subGroup->GetCounter(module + "/AssembleFilterCount", true);

    FilterOnlyCount = subGroup->GetCounter(module + "/FilterOnlyCount", true);
    FilterOnlyFetchedBytes = subGroup->GetCounter(module + "/FilterOnlyFetchedBytes", true);
    FilterOnlyUsefulBytes = subGroup->GetCounter(module + "/FilterOnlyUsefulBytes", true);

    EmptyFilterCount = subGroup->GetCounter(module + "/EmptyFilterCount", true);
    EmptyFilterFetchedBytes = subGroup->GetCounter(module + "/EmptyFilterFetchedBytes", true);

    OriginalRowsCount = subGroup->GetCounter(module + "/OriginalRowsCount", true);
    FilteredRowsCount = subGroup->GetCounter(module + "/FilteredRowsCount", true);
    SkippedBytes = subGroup->GetCounter(module + "/SkippedBytes", true);

    TwoPhasesCount = subGroup->GetCounter(module + "/TwoPhasesCount", true);
    TwoPhasesFilterFetchedBytes = subGroup->GetCounter(module + "/TwoPhasesFilterFetchedBytes", true);
    TwoPhasesFilterUsefulBytes = subGroup->GetCounter(module + "/TwoPhasesFilterUsefulBytes", true);
    TwoPhasesPostFilterFetchedBytes = subGroup->GetCounter(module + "/TwoPhasesPostFilterFetchedBytes", true);
    TwoPhasesPostFilterUsefulBytes = subGroup->GetCounter(module + "/TwoPhasesPostFilterUsefulBytes", true);
}

}
