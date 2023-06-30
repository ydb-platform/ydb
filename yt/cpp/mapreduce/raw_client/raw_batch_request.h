#pragma once

#include <yt/cpp/mapreduce/common/fwd.h>

#include <yt/cpp/mapreduce/interface/batch_request.h>
#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/cpp/mapreduce/interface/node.h>
#include <yt/cpp/mapreduce/interface/retry_policy.h>

#include <yt/cpp/mapreduce/http/requests.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/generic/deque.h>

#include <exception>

namespace NYT::NDetail {
    struct TResponseInfo;
}

namespace NYT::NDetail::NRawClient {

////////////////////////////////////////////////////////////////////////////////

class TRawBatchRequest
    : public TThrRefBase
{
public:
    struct IResponseItemParser
        : public TThrRefBase
    {
        ~IResponseItemParser() = default;

        virtual void SetResponse(TMaybe<TNode> node) = 0;
        virtual void SetException(std::exception_ptr e) = 0;
    };

public:
    TRawBatchRequest(const TConfigPtr& config);
    ~TRawBatchRequest();

    bool IsExecuted() const;
    void MarkExecuted();

    void FillParameterList(size_t maxSize, TNode* result, TInstant* nextTry) const;

    size_t BatchSize() const;

    void ParseResponse(
        const TResponseInfo& requestResult,
        const IRequestRetryPolicyPtr& retryPolicy,
        TRawBatchRequest* retryBatch,
        TInstant now = TInstant::Now());
    void ParseResponse(
        TNode response,
        const TString& requestId,
        const IRequestRetryPolicyPtr& retryPolicy,
        TRawBatchRequest* retryBatch,
        TInstant now = TInstant::Now());
    void SetErrorResult(std::exception_ptr e) const;

    ::NThreading::TFuture<TNodeId> Create(
        const TTransactionId& transaction,
        const TYPath& path,
        ENodeType type,
        const TCreateOptions& options);
    ::NThreading::TFuture<void> Remove(
        const TTransactionId& transaction,
        const TYPath& path,
        const TRemoveOptions& options);
    ::NThreading::TFuture<bool> Exists(
        const TTransactionId& transaction,
        const TYPath& path,
        const TExistsOptions& options);
    ::NThreading::TFuture<TNode> Get(
        const TTransactionId& transaction,
        const TYPath& path,
        const TGetOptions& options);
    ::NThreading::TFuture<void> Set(
        const TTransactionId& transaction,
        const TYPath& path,
        const TNode& value,
        const TSetOptions& options);
    ::NThreading::TFuture<TNode::TListType> List(
        const TTransactionId& transaction,
        const TYPath& path,
        const TListOptions& options);
    ::NThreading::TFuture<TNodeId> Copy(
        const TTransactionId& transaction,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options);
    ::NThreading::TFuture<TNodeId> Move(
        const TTransactionId& transaction,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options);
    ::NThreading::TFuture<TNodeId> Link(
        const TTransactionId& transaction,
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options);
    ::NThreading::TFuture<TLockId> Lock(
        const TTransactionId& transaction,
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options);
    ::NThreading::TFuture<void> Unlock(
        const TTransactionId& transaction,
        const TYPath& path,
        const TUnlockOptions& options);
    ::NThreading::TFuture<TMaybe<TYPath>> GetFileFromCache(
        const TTransactionId& transactionId,
        const TString& md5Signature,
        const TYPath& cachePath,
        const TGetFileFromCacheOptions& options);
    ::NThreading::TFuture<TYPath> PutFileToCache(
        const TTransactionId& transactionId,
        const TYPath& filePath,
        const TString& md5Signature,
        const TYPath& cachePath,
        const TPutFileToCacheOptions& options);
    ::NThreading::TFuture<TCheckPermissionResponse> CheckPermission(
        const TString& user,
        EPermission permission,
        const TYPath& path,
        const TCheckPermissionOptions& options);
    ::NThreading::TFuture<TOperationAttributes> GetOperation(
        const TOperationId& operationId,
        const TGetOperationOptions& options);
    ::NThreading::TFuture<void> AbortOperation(const TOperationId& operationId);
    ::NThreading::TFuture<void> CompleteOperation(const TOperationId& operationId);
    ::NThreading::TFuture<void> SuspendOperation(
        const TOperationId& operationId,
        const TSuspendOperationOptions& options);
    ::NThreading::TFuture<void> ResumeOperation(
        const TOperationId& operationId,
        const TResumeOperationOptions& options);
    ::NThreading::TFuture<void> UpdateOperationParameters(
        const TOperationId& operationId,
        const TUpdateOperationParametersOptions& options);
    ::NThreading::TFuture<TRichYPath> CanonizeYPath(const TRichYPath& path);
    ::NThreading::TFuture<TVector<TTableColumnarStatistics>> GetTableColumnarStatistics(
        const TTransactionId& transaction,
        const TVector<TRichYPath>& paths,
        const TGetTableColumnarStatisticsOptions& options);
    ::NThreading::TFuture<TMultiTablePartitions> GetTablePartitions(
        const TTransactionId& transaction,
        const TVector<TRichYPath>& paths,
        const TGetTablePartitionsOptions& options);

private:
    struct TBatchItem {
        TNode Parameters;
        ::TIntrusivePtr<IResponseItemParser> ResponseParser;
        TInstant NextTry;

        TBatchItem(TNode parameters, ::TIntrusivePtr<IResponseItemParser> responseParser);

        TBatchItem(const TBatchItem& batchItem, TInstant nextTry);
    };

private:
    template <typename TResponseParser>
    typename TResponseParser::TFutureResult AddRequest(
        const TString& command,
        TNode parameters,
        TMaybe<TNode> input);

    template <typename TResponseParser>
    typename TResponseParser::TFutureResult AddRequest(
        const TString& command,
        TNode parameters,
        TMaybe<TNode> input,
        ::TIntrusivePtr<TResponseParser> parser);

    void AddRequest(TBatchItem batchItem);

private:
    TConfigPtr Config_;

    TDeque<TBatchItem> BatchItemList_;
    bool Executed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail::NRawClient
