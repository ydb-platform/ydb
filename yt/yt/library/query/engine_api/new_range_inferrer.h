#pragma once

#include "public.h"

#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/query.h>

#include <functional>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TConstraintExtractorMap
    : public TRefCounted
    , public std::unordered_map<TString, TConstraintExtractor>
{ };

DEFINE_REFCOUNTED_TYPE(TConstraintExtractorMap)

////////////////////////////////////////////////////////////////////////////////

using TRangeInferrer = std::function<std::vector<TMutableRowRange>(
    const TRowRange& keyRange,
    const TRowBufferPtr& rowBuffer)>;

TRangeInferrer CreateNewRangeInferrer(
    TConstExpressionPtr predicate,
    const TTableSchemaPtr& schema,
    const TKeyColumns& keyColumns,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstConstraintExtractorMapPtr& constraintExtractors,
    const TQueryOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
