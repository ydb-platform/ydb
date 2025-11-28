#include "raw_batch_request.h"

#include "wrap_rpc_error.h"

#include <yt/cpp/mapreduce/common/helpers.h>

#include <yt/cpp/mapreduce/common/retry_request.h>

#include <yt/cpp/mapreduce/interface/raw_client.h>

#include <yt/yt/client/ypath/rich.h>

#include <util/generic/scope.h>

namespace NYT::NDetail {

using ::NThreading::TFuture;
using ::NThreading::TPromise;

////////////////////////////////////////////////////////////////////////////////

template <typename TResultType>
TRpcRawBatchRequest::TSingleRequest<TResultType>::TSingleRequest(
    IRequestRetryPolicyPtr retryPolicy,
    std::function<TResultType(TMutationId&)> request)
    : RequestRetryPolicy_(retryPolicy)
    , Result_(::NThreading::NewPromise<TResultType>())
    , Request_(std::move(request))
{ }

template <typename TResultType>
TFuture<TResultType> TRpcRawBatchRequest::TSingleRequest<TResultType>::GetFuture()
{
    return WrapRpcError(Result_.GetFuture());
}

template <typename TResultType>
void TRpcRawBatchRequest::TSingleRequest<TResultType>::Invoke()
{
    try {
        if constexpr (std::is_same_v<TResultType, void>) {
            RequestWithRetry<void>(RequestRetryPolicy_, Request_);
            Result_.SetValue();
        } else {
            auto value = RequestWithRetry<TResultType>(RequestRetryPolicy_, Request_);
            Result_.SetValue(std::move(value));
        }
    } catch (...) {
        Result_.SetException(std::current_exception());
    }
}

////////////////////////////////////////////////////////////////////////////////

TRpcRawBatchRequest::TRpcRawBatchRequest(
    IRawClientPtr rawClient,
    const TConfigPtr& config)
    : RawClient_(std::move(rawClient))
    , Config_(config)
{ }

void TRpcRawBatchRequest::ExecuteBatch(const TExecuteBatchOptions& /*options*/)
{
    if (Executed_) {
        ythrow yexception() << "Cannot execute batch request since it is already executed";
    }
    Y_DEFER {
        Executed_ = true;
    };

    while (!Requests_.empty()) {
        auto& request = Requests_.front();
        request->Invoke();
        Requests_.pop();
    }
}

TFuture<TNodeId> TRpcRawBatchRequest::Create(
    const TTransactionId& transactionId,
    const TYPath& path,
    ENodeType type,
    const TCreateOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<TNodeId>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& mutationId) {
            return RawClient_->Create(mutationId, transactionId, path, type, options);
        });
    auto future = request->GetFuture();
    Requests_.emplace(std::move(request));
    return future;
}

TFuture<void> TRpcRawBatchRequest::Remove(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TRemoveOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<void>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& mutationId) {
            RawClient_->Remove(mutationId, transactionId, path, options);
        });
    return AddRequest(std::move(request));
}

TFuture<bool> TRpcRawBatchRequest::Exists(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TExistsOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<bool>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& /*mutationId*/) {
            return RawClient_->Exists(transactionId, path, options);
        });
    return AddRequest(std::move(request));
}

TFuture<TNode> TRpcRawBatchRequest::Get(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<TNode>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& /*mutationId*/) {
            return RawClient_->Get(transactionId, path, options);
        });
    return AddRequest(std::move(request));
}

TFuture<void> TRpcRawBatchRequest::Set(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<void>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& mutationId) {
            RawClient_->Set(mutationId, transactionId, path, value, options);
        });
    return AddRequest(std::move(request));
}

TFuture<TNode::TListType> TRpcRawBatchRequest::List(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TListOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<TNode::TListType>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& /*mutationId*/) {
            return RawClient_->List(transactionId, path, options);
        });
    return AddRequest(std::move(request));
}

TFuture<TNodeId> TRpcRawBatchRequest::Copy(
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<TNodeId>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& /*mutationId*/) {
            return RawClient_->CopyWithoutRetries(transactionId, sourcePath, destinationPath, options);
        });
    return AddRequest(std::move(request));
}

TFuture<TNodeId> TRpcRawBatchRequest::Move(
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<TNodeId>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& /*mutationId*/) {
            return RawClient_->MoveWithoutRetries(transactionId, sourcePath, destinationPath, options);
        });
    return AddRequest(std::move(request));
}

TFuture<TNodeId> TRpcRawBatchRequest::Link(
    const TTransactionId& transactionId,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<TNodeId>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& mutationId) {
            return RawClient_->Link(mutationId, transactionId, targetPath, linkPath, options);
        });
    return AddRequest(std::move(request));
}

TFuture<TLockId> TRpcRawBatchRequest::Lock(
    const TTransactionId& transactionId,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<TLockId>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& mutationId) {
            return RawClient_->Lock(mutationId, transactionId, path, mode, options);
        });
    return AddRequest(std::move(request));
}

TFuture<void> TRpcRawBatchRequest::Unlock(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TUnlockOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<void>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& mutationId) {
            RawClient_->Unlock(mutationId, transactionId, path, options);
        });
    return AddRequest(std::move(request));
}

TFuture<TMaybe<TYPath>> TRpcRawBatchRequest::GetFileFromCache(
    const TTransactionId& transactionId,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<TMaybe<TYPath>>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& /*mutationId*/) {
            return RawClient_->GetFileFromCache(transactionId, md5Signature, cachePath, options);
        });
    return AddRequest(std::move(request));
}

TFuture<TYPath> TRpcRawBatchRequest::PutFileToCache(
    const TTransactionId& transactionId,
    const TYPath& filePath,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<TYPath>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& /*mutationId*/) {
            return RawClient_->PutFileToCache(transactionId, filePath, md5Signature, cachePath, options);
        });
    return AddRequest(std::move(request));
}

TFuture<TCheckPermissionResponse> TRpcRawBatchRequest::CheckPermission(
    const TString& user,
    EPermission permission,
    const TYPath& path,
    const TCheckPermissionOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<TCheckPermissionResponse>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& /*mutationId*/) {
            return RawClient_->CheckPermission(user, permission, path, options);
        });
    return AddRequest(std::move(request));
}

TFuture<TOperationAttributes> TRpcRawBatchRequest::GetOperation(
    const TOperationId& operationId,
    const TGetOperationOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<TOperationAttributes>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& /*mutationId*/) {
            return RawClient_->GetOperation(operationId, options);
        });
    return AddRequest(std::move(request));
}

TFuture<void> TRpcRawBatchRequest::AbortOperation(const TOperationId& operationId)
{
    auto request = MakeIntrusive<TSingleRequest<void>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& mutationId) {
            RawClient_->AbortOperation(mutationId, operationId);
        });
    return AddRequest(std::move(request));
}

TFuture<void> TRpcRawBatchRequest::CompleteOperation(const TOperationId& operationId)
{
    auto request = MakeIntrusive<TSingleRequest<void>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& mutationId) {
            RawClient_->CompleteOperation(mutationId, operationId);
        });
    return AddRequest(std::move(request));
}

TFuture<void> TRpcRawBatchRequest::SuspendOperation(
    const TOperationId& operationId,
    const TSuspendOperationOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<void>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& mutationId) {
            RawClient_->SuspendOperation(mutationId, operationId, options);
        });
    return AddRequest(std::move(request));
}

TFuture<void> TRpcRawBatchRequest::ResumeOperation(
    const TOperationId& operationId,
    const TResumeOperationOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<void>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& mutationId) {
            RawClient_->ResumeOperation(mutationId, operationId, options);
        });
    return AddRequest(std::move(request));
}

TFuture<void> TRpcRawBatchRequest::UpdateOperationParameters(
    const TOperationId& operationId,
    const TUpdateOperationParametersOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<void>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& /*mutationId*/) {
            RawClient_->UpdateOperationParameters(operationId, options);
        });
    return AddRequest(std::move(request));
}

TFuture<TRichYPath> TRpcRawBatchRequest::CanonizeYPath(const TRichYPath& path)
{
    TRichYPath result = path;
    // Out of the symbols in the canonization branch below, only '<' can appear in the beggining of a valid rich YPath.
    if (!result.Path_.StartsWith("<")) {
        result.Path_ = AddPathPrefix(result.Path_, Config_->Prefix);
    }

    if (result.Path_.find_first_of("<>{}[]:") != TString::npos) {
        auto request = MakeIntrusive<TSingleRequest<TRichYPath>>(
            MakeIntrusive<TAttemptLimitedRetryPolicy>(/*attemptLimit*/ 1u, Config_),
            [=, this] (TMutationId& /*mutationId*/) {
                auto richPath = NYPath::TRichYPath::Parse(result.Path_);

                TNode pathNode;
                TNodeBuilder builder(&pathNode);
                Serialize(richPath, &builder);

                auto originalPathNode = PathToNode(result);
                for (const auto& [key, value] : originalPathNode.GetAttributes().AsMap()) {
                    pathNode.Attributes()[key] = value;
                }

                TRichYPath canonizedPath;
                Deserialize(canonizedPath, pathNode);
                canonizedPath.Path_ = AddPathPrefix(canonizedPath.Path_, Config_->Prefix);
                return canonizedPath;
            });

        return AddRequest(std::move(request));
    }

    return ::NThreading::MakeFuture(result);
}

TFuture<TVector<TTableColumnarStatistics>> TRpcRawBatchRequest::GetTableColumnarStatistics(
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& paths,
    const TGetTableColumnarStatisticsOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<TVector<TTableColumnarStatistics>>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& /*mutationId*/) {
            return RawClient_->GetTableColumnarStatistics(transactionId, paths, options);
        });
    return AddRequest(std::move(request));
}

TFuture<TMultiTablePartitions> TRpcRawBatchRequest::GetTablePartitions(
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& paths,
    const TGetTablePartitionsOptions& options)
{
    auto request = MakeIntrusive<TSingleRequest<TMultiTablePartitions>>(
        CreateDefaultRequestRetryPolicy(Config_),
        [=, this] (TMutationId& /*mutationId*/) {
            return RawClient_->GetTablePartitions(transactionId, paths, options);
        });
    return AddRequest(std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
