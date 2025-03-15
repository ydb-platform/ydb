#pragma once

#include "raw_batch_request.h"

#include <yt/cpp/mapreduce/common/fwd.h>

#include <yt/cpp/mapreduce/client/client.h>

#include <yt/cpp/mapreduce/http/context.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/client_method_options.h>
#include <yt/cpp/mapreduce/interface/operation.h>
#include <yt/cpp/mapreduce/interface/raw_client.h>

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

TRichYPath CanonizeYPath(
    const IRawClientPtr& rawClient,
    const TRichYPath& path);

TVector<TRichYPath> CanonizeYPaths(
    const IRawClientPtr& rawClient,
    const TVector<TRichYPath>& paths);

NHttpClient::IHttpResponsePtr SkyShareTable(
    const TClientContext& context,
    const std::vector<TYPath>& tablePaths,
    const TSkyShareTableOptions& options);

std::unique_ptr<IOutputStream> WriteTable(
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TRichYPath& path,
    const TMaybe<TFormat>& format,
    const TTableWriterOptions& options);

void InsertRows(
    const TClientContext& context,
    const TYPath& path,
    const TNode::TListType& rows,
    const TInsertRowsOptions& options);

TNode::TListType LookupRows(
    const TClientContext& context,
    const TYPath& path,
    const TNode::TListType& keys,
    const TLookupRowsOptions& options);

void DeleteRows(
    const TClientContext& context,
    const TYPath& path,
    const TNode::TListType& keys,
    const TDeleteRowsOptions& options);

TAuthorizationInfo WhoAmI(const TClientContext& context);

////////////////////////////////////////////////////////////////////////////////

template<typename TSrc, typename TBatchAdder>
auto BatchTransform(
    const IRawClientPtr& rawClient,
    const TSrc& src,
    TBatchAdder batchAdder,
    const TExecuteBatchOptions& executeBatchOptions = {})
{
    auto batch = rawClient->CreateRawBatchRequest();
    using TFuture = decltype(batchAdder(batch, *std::begin(src)));
    TVector<TFuture> futures;
    for (const auto& el : src) {
        futures.push_back(batchAdder(batch, el));
    }
    batch->ExecuteBatch(executeBatchOptions);
    using TDst = decltype(futures[0].ExtractValueSync());
    TVector<TDst> result;
    result.reserve(std::size(src));
    for (auto& future : futures) {
        result.push_back(future.ExtractValueSync());
    }
    return result;
}

template <typename TBatchAdder>
auto RemoteClustersBatchTransform(
    const IRawClientPtr& rawClient,
    const TClientContext& context,
    const TVector<TRichYPath>& paths,
    TBatchAdder batchAdder,
    const TExecuteBatchOptions& options = {})
{
    // Given inputs from multiple clusters, we need to categorize them based on their cluster names.
    // We assume the current cluster name is empty. Within each cluster,
    // gather all related inputs to perform a batch request.
    std::unordered_map<TString, std::vector<int>> clusterToPathsIndexes;
    for (ssize_t index = 0; index < std::ssize(paths); ++index) {
        const auto& path = paths[index];
        auto clusterName = path.Cluster_.GetOrElse("");
        clusterToPathsIndexes[clusterName].push_back(index);
    }

    std::vector<TNode> result(paths.size());
    for (const auto& [clusterName, pathsIndexes] : clusterToPathsIndexes) {
        auto newContext = context;
        if (!clusterName.empty()) {
            // It is not a current cluster, we switch the cluster context.
            SetupClusterContext(newContext, clusterName);
        }
        auto newRawClient = rawClient->Clone(newContext);

        TVector<TRichYPath> pathsBatch;
        pathsBatch.reserve(pathsIndexes.size());
        for (int index : pathsIndexes) {
            pathsBatch.push_back(paths[index]);
        }

        auto batchResult = NRawClient::BatchTransform(
            newRawClient,
            NRawClient::CanonizeYPaths(newRawClient, pathsBatch),
            batchAdder,
            options);

        for (ssize_t index = 0; index < std::ssize(pathsIndexes); ++index) {
            result[pathsIndexes[index]] = batchResult[index];
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail::NRawClient
} // namespace NYT
