#pragma once

#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <yt/cpp/mapreduce/interface/raw_batch_request.h>

#include <queue>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

class TRpcRawBatchRequest
    : public IRawBatchRequest
{
public:
    TRpcRawBatchRequest(
        IRawClientPtr rawClient,
        const TConfigPtr& config);

    void ExecuteBatch(const TExecuteBatchOptions& options = {}) override;

    ::NThreading::TFuture<TNodeId> Create(
        const TTransactionId& transactionId,
        const TYPath& path,
        ENodeType type,
        const TCreateOptions& options = {}) override;

    ::NThreading::TFuture<void> Remove(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TRemoveOptions& options = {}) override;

    ::NThreading::TFuture<bool> Exists(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TExistsOptions& options = {}) override;

    ::NThreading::TFuture<TNode> Get(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TGetOptions& options = {}) override;

    ::NThreading::TFuture<void> Set(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TNode& value,
        const TSetOptions& options = {}) override;

    ::NThreading::TFuture<TNode::TListType> List(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TListOptions& options = {}) override;

    ::NThreading::TFuture<TNodeId> Copy(
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = {}) override;

    ::NThreading::TFuture<TNodeId> Move(
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = {}) override;

    ::NThreading::TFuture<TNodeId> Link(
        const TTransactionId& transactionId,
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options = {}) override;

    ::NThreading::TFuture<TLockId> Lock(
        const TTransactionId& transactionId,
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options = {}) override;

    ::NThreading::TFuture<void> Unlock(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TUnlockOptions& options = {}) override;

    ::NThreading::TFuture<TMaybe<TYPath>> GetFileFromCache(
        const TTransactionId& transactionId,
        const TString& md5Signature,
        const TYPath& cachePath,
        const TGetFileFromCacheOptions& options = {}) override;

    ::NThreading::TFuture<TYPath> PutFileToCache(
        const TTransactionId& transactionId,
        const TYPath& filePath,
        const TString& md5Signature,
        const TYPath& cachePath,
        const TPutFileToCacheOptions& options = {}) override;

    ::NThreading::TFuture<TCheckPermissionResponse> CheckPermission(
        const TString& user,
        EPermission permission,
        const TYPath& path,
        const TCheckPermissionOptions& options = {}) override;

    ::NThreading::TFuture<TOperationAttributes> GetOperation(
        const TOperationId& operationId,
        const TGetOperationOptions& options = {}) override;

    ::NThreading::TFuture<void> AbortOperation(const TOperationId& operationId) override;

    ::NThreading::TFuture<void> CompleteOperation(const TOperationId& operationId) override;

    ::NThreading::TFuture<void> SuspendOperation(
        const TOperationId& operationId,
        const TSuspendOperationOptions& options = {}) override;

    ::NThreading::TFuture<void> ResumeOperation(
        const TOperationId& operationId,
        const TResumeOperationOptions& options = {}) override;

    ::NThreading::TFuture<void> UpdateOperationParameters(
        const TOperationId& operationId,
        const TUpdateOperationParametersOptions& options = {}) override;

    ::NThreading::TFuture<TRichYPath> CanonizeYPath(const TRichYPath& path) override;

    ::NThreading::TFuture<TVector<TTableColumnarStatistics>> GetTableColumnarStatistics(
        const TTransactionId& transactionId,
        const TVector<TRichYPath>& paths,
        const TGetTableColumnarStatisticsOptions& options = {}) override;

    ::NThreading::TFuture<TMultiTablePartitions> GetTablePartitions(
        const TTransactionId& transactionId,
        const TVector<TRichYPath>& paths,
        const TGetTablePartitionsOptions& options = {}) override;

private:
    struct ISingleRequest
        : public TThrRefBase
    {
        virtual void Invoke() = 0;
    };

    template <typename TResultType>
    class TSingleRequest
        : public ISingleRequest
    {
    public:
        TSingleRequest(
            IRequestRetryPolicyPtr retryPolicy,
            std::function<TResultType(TMutationId&)> request);

        ::NThreading::TFuture<TResultType> GetFuture();

        void Invoke() override;

    private:
        const IRequestRetryPolicyPtr RequestRetryPolicy_;

        ::NThreading::TPromise<TResultType> Result_;

        std::function<TResultType(TMutationId&)> Request_;
    };

private:
    const IRawClientPtr RawClient_;
    const TConfigPtr Config_;

    std::queue<TIntrusivePtr<ISingleRequest>> Requests_;

    bool Executed_ = false;

    template<typename TRequest>
    auto AddRequest(TRequest&& request) -> decltype(request->GetFuture())
    {
        Y_ENSURE(!Executed_, "Cannot add request: batch request is already executed");
        auto future = request->GetFuture();
        Requests_.emplace(std::forward<decltype(request)>(request));
        return future;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
