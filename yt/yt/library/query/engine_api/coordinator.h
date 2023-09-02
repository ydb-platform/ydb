#pragma once

#include "public.h"

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TRefiner = std::function<TConstExpressionPtr(
    const TConstExpressionPtr& expr,
    const TKeyColumns& keyColumns)>;

std::pair<TConstFrontQueryPtr, std::vector<TConstQueryPtr>> CoordinateQuery(
    const TConstQueryPtr& query,
    const std::vector<TRefiner>& refiners);

TRowRanges GetPrunedRanges(
    const TConstExpressionPtr& predicate,
    const TTableSchemaPtr& tableSchema,
    const TKeyColumns& keyColumns,
    NObjectClient::TObjectId tableId,
    const TSharedRange<TRowRange>& ranges,
    const TRowBufferPtr& rowBuffer,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options,
    TGuid queryId = {});

TRowRanges GetPrunedRanges(
    const TConstQueryPtr& query,
    NObjectClient::TObjectId tableId,
    const TSharedRange<TRowRange>& ranges,
    const TRowBufferPtr& rowBuffer,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options);

using TEvaluateResult = std::pair<
    ISchemafulUnversionedReaderPtr,
    TFuture<TQueryStatistics>>;

TQueryStatistics CoordinateAndExecute(
    const TConstQueryPtr& query,
    const IUnversionedRowsetWriterPtr& writer,
    const std::vector<TRefiner>& ranges,
    std::function<TEvaluateResult(const TConstQueryPtr&, int)> evaluateSubquery,
    std::function<TQueryStatistics(const TConstFrontQueryPtr&, const ISchemafulUnversionedReaderPtr&, const IUnversionedRowsetWriterPtr&)> evaluateTop);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
