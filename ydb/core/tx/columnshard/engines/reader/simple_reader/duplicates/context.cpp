#include "context.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

TFilterAccumulator::TFilterAccumulator(const TEvRequestFilter::TPtr& request)
    : OriginalRequest(request)
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
