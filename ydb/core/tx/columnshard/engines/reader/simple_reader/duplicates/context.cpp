#include "context.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {
<<<<<<< HEAD
    
=======

TFilterAccumulator::TFilterAccumulator(const TEvRequestFilter::TPtr& request)
    : OriginalRequest(request)
{
    AFL_VERIFY(!!OriginalRequest);
}

>>>>>>> af473aa4b23 (trivial reader has been introduced (#38377))
TFilterBuildingGuard::TFilterBuildingGuard()
    : ProcessGuard(NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildProcessGuard(GetStageFeatures()))
    , ScopeGuard(ProcessGuard->BuildScopeGuard(1))
    , GroupGuard(ScopeGuard->BuildGroupGuard())
{
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
