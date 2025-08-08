#include "context.h"

#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

    namespace {

        static std::shared_ptr<NOlap::NGroupedMemoryManager::TStageFeatures> DeduplicationStageFeatures =
            NOlap::NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildStageFeatures("DEFAULT", 1000000000);

    }

TInternalFilterConstructor::TInternalFilterConstructor(const TEvRequestFilter::TPtr& request)
    : OriginalRequest(request)
    , ProcessGuard(NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildProcessGuard({ DeduplicationStageFeatures }))
    , ScopeGuard(ProcessGuard->BuildScopeGuard(1))
    , GroupGuard(ScopeGuard->BuildGroupGuard())
{
    AFL_VERIFY(!!OriginalRequest);
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
