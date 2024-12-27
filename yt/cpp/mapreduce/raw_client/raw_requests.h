#pragma once

#include "raw_batch_request.h"

#include <yt/cpp/mapreduce/common/fwd.h>
#include <yt/cpp/mapreduce/http/context.h>
#include <yt/cpp/mapreduce/interface/client_method_options.h>
#include <yt/cpp/mapreduce/interface/operation.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class IRequestRetryPolicy;
struct TClientContext;
struct TExecuteBatchOptions;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail::NRawClient {

////////////////////////////////////////////////////////////////////////////////

TOperationAttributes ParseOperationAttributes(const TNode& node);

TJobAttributes ParseJobAttributes(const TNode& node);

TCheckPermissionResponse ParseCheckPermissionResponse(const TNode& node);

////////////////////////////////////////////////////////////////////////////////

// marks `batchRequest' as executed
void ExecuteBatch(
    IRequestRetryPolicyPtr retryPolicy,
    const TClientContext& context,
    TRawBatchRequest& batchRequest,
    const TExecuteBatchOptions& options = {});

// Misc

TRichYPath CanonizeYPath(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TRichYPath& path);

TVector<TRichYPath> CanonizeYPaths(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TVector<TRichYPath>& paths);

////////////////////////////////////////////////////////////////////////////////

template<typename TSrc, typename TBatchAdder>
auto BatchTransform(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TClientContext& context,
    const TSrc& src,
    TBatchAdder batchAdder,
    const TExecuteBatchOptions& executeBatchOptions = {})
{
    TRawBatchRequest batch(context.Config);
    using TFuture = decltype(batchAdder(batch, *std::begin(src)));
    TVector<TFuture> futures;
    for (const auto& el : src) {
        futures.push_back(batchAdder(batch, el));
    }
    ExecuteBatch(retryPolicy, context, batch, executeBatchOptions);
    using TDst = decltype(futures[0].ExtractValueSync());
    TVector<TDst> result;
    result.reserve(std::size(src));
    for (auto& future : futures) {
        result.push_back(future.ExtractValueSync());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail::NRawClient
} // namespace NYT
