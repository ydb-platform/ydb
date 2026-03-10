#include "context.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

TFilterAccumulator::TFilterAccumulator(const TEvRequestFilter::TPtr& request, ui64 recordsCount)
    : OriginalRequest(request)
    , RecordsCount(recordsCount)
{
    AFL_VERIFY(!!OriginalRequest);
}

TFilterBuildingGuard::TFilterBuildingGuard()
    : ProcessGuard(NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildProcessGuard(GetStageFeatures()))
    , ScopeGuard(ProcessGuard->BuildScopeGuard(1))
    , GroupGuard(ScopeGuard->BuildGroupGuard())
{
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
