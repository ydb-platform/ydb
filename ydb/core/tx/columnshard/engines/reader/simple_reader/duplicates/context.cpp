#include "context.h"

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {
    
TFilterBuildingGuard::TFilterBuildingGuard()
    : ProcessGuard(NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildProcessGuard(GetStageFeatures()))
    , ScopeGuard(ProcessGuard->BuildScopeGuard(1))
    , GroupGuard(ScopeGuard->BuildGroupGuard())
{
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
