#include "context.h"

#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

TInternalFilterConstructor::TInternalFilterConstructor(const TEvRequestFilter::TPtr& request)
    : OriginalRequest(request)
    , ProcessGuard(NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildProcessGuard({}))
    , ScopeGuard(ProcessGuard->BuildScopeGuard(1))
    , GroupGuard(ScopeGuard->BuildGroupGuard())
{
    AFL_VERIFY(!!OriginalRequest);
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
