#include "context.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

TFilterAccumulator::TFilterAccumulator(const TEvRequestFilter::TPtr& request, ui64 recordsCount, std::shared_ptr<NColumnShard::TDuplicateFilteringCounters> counters)
    : OriginalRequest(request)
    , RecordsCount(recordsCount)
    , Counters(counters)
    , StartTime(TInstant::Now())
{
    AFL_VERIFY(!!OriginalRequest);
    Counters->OnRequestStart();
}

TFilterBuildingGuard::TFilterBuildingGuard()
    : ProcessGuard(NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildProcessGuard(GetStageFeatures()))
    , ScopeGuard(ProcessGuard->BuildScopeGuard(1))
    , GroupGuard(ScopeGuard->BuildGroupGuard())
{
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
