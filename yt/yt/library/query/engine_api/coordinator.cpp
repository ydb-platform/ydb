#include "coordinator.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

Y_WEAK std::pair<TConstFrontQueryPtr, std::vector<TConstQueryPtr>> CoordinateQuery(
    const TConstQueryPtr& /*query*/,
    const std::vector<TRefiner>& /*refiners*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/coordinator.cpp.
    YT_ABORT();
}

Y_WEAK TRowRanges GetPrunedRanges(
    const TConstExpressionPtr& /*predicate*/,
    const TTableSchemaPtr& /*tableSchema*/,
    const TKeyColumns& /*keyColumns*/,
    NObjectClient::TObjectId /*tableId*/,
    const TSharedRange<TRowRange>& /*ranges*/,
    const TRowBufferPtr& /*rowBuffer*/,
    const IColumnEvaluatorCachePtr& /*evaluatorCache*/,
    const TConstRangeExtractorMapPtr& /*rangeExtractors*/,
    const TQueryOptions& /*options*/,
    TGuid /*queryId*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/coordinator.cpp.
    YT_ABORT();
}

Y_WEAK TRowRanges GetPrunedRanges(
    const TConstQueryPtr& /*query*/,
    NObjectClient::TObjectId /*tableId*/,
    const TSharedRange<TRowRange>& /*ranges*/,
    const TRowBufferPtr& /*rowBuffer*/,
    const IColumnEvaluatorCachePtr& /*evaluatorCache*/,
    const TConstRangeExtractorMapPtr& /*rangeExtractors*/,
    const TQueryOptions& /*options*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/coordinator.cpp.
    YT_ABORT();
}

Y_WEAK TQueryStatistics CoordinateAndExecute(
    const TConstQueryPtr& /*query*/,
    const IUnversionedRowsetWriterPtr& /*writer*/,
    const std::vector<TRefiner>& /*ranges*/,
    std::function<TEvaluateResult(const TConstQueryPtr&, int)> /*evaluateSubquery*/,
    std::function<TQueryStatistics(const TConstFrontQueryPtr&, const ISchemafulUnversionedReaderPtr&, const IUnversionedRowsetWriterPtr&)> /*evaluateTop*/)
{
    // Proper implementation resides in yt/yt/library/query/engine/coordinator.cpp.
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
