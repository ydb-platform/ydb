#include "context.h"

#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering {

namespace {

static std::shared_ptr<NOlap::NGroupedMemoryManager::TStageFeatures> DeduplicationStageFeatures =
    NOlap::NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildStageFeatures("DEFAULT", 1000000000);
}

TInternalFilterConstructor::TInternalFilterConstructor(const TEvRequestFilter::TPtr& request, const TPortionInfo& portionInfo,
    const std::shared_ptr<NColumnShard::TDuplicateFilteringCounters>& counters, const std::shared_ptr<arrow::Schema>& pkSchema)
    : OriginalRequest(request)
    , ProcessGuard(NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::BuildProcessGuard({ DeduplicationStageFeatures }))
    , ScopeGuard(ProcessGuard->BuildScopeGuard(1))
    , GroupGuard(ScopeGuard->BuildGroupGuard())
    , Counters(counters)
    , PKSchema(pkSchema)
    , MinPK(portionInfo.IndexKeyStart())
    , MaxPK(portionInfo.IndexKeyEnd())
{
    AFL_VERIFY(!!OriginalRequest);
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NDuplicateFiltering
