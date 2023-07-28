#pragma once

#include <yt/cpp/mapreduce/interface/batch_request.h>
#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/cpp/mapreduce/interface/node.h>

#include <yt/cpp/mapreduce/http/requests.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/generic/deque.h>

#include <exception>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TResponseInfo;
class TClient;
using TClientPtr = ::TIntrusivePtr<TClient>;

namespace NRawClient {
    class TRawBatchRequest;
}

////////////////////////////////////////////////////////////////////////////////

class TBatchRequest
    : public IBatchRequest
{
public:
    TBatchRequest(const TTransactionId& defaultTransaction, ::TIntrusivePtr<TClient> client);

    ~TBatchRequest();

    virtual IBatchRequestBase& WithTransaction(const TTransactionId& transactionId) override;

    virtual ::NThreading::TFuture<TLockId> Create(
        const TYPath& path,
        ENodeType type,
        const TCreateOptions& options = TCreateOptions()) override;

    virtual ::NThreading::TFuture<void> Remove(
        const TYPath& path,
        const TRemoveOptions& options = TRemoveOptions()) override;

    virtual ::NThreading::TFuture<bool> Exists(
        const TYPath& path,
        const TExistsOptions& options = TExistsOptions()) override;

    virtual ::NThreading::TFuture<TNode> Get(
        const TYPath& path,
        const TGetOptions& options = TGetOptions()) override;

    virtual ::NThreading::TFuture<void> Set(
        const TYPath& path,
        const TNode& node,
        const TSetOptions& options = TSetOptions()) override;

    virtual ::NThreading::TFuture<TNode::TListType> List(
        const TYPath& path,
        const TListOptions& options = TListOptions()) override;

    virtual ::NThreading::TFuture<TNodeId> Copy(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = TCopyOptions()) override;

    virtual ::NThreading::TFuture<TNodeId> Move(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = TMoveOptions()) override;

    virtual ::NThreading::TFuture<TNodeId> Link(
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options = TLinkOptions()) override;

    virtual ::NThreading::TFuture<ILockPtr> Lock(
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options) override;

    virtual ::NThreading::TFuture<void> Unlock(
        const TYPath& path,
        const TUnlockOptions& options) override;

    virtual ::NThreading::TFuture<void> AbortOperation(const TOperationId& operationId) override;

    virtual ::NThreading::TFuture<void> CompleteOperation(const TOperationId& operationId) override;

    ::NThreading::TFuture<void> SuspendOperation(
        const TOperationId& operationId,
        const TSuspendOperationOptions& options) override;

    ::NThreading::TFuture<void> ResumeOperation(
        const TOperationId& operationId,
        const TResumeOperationOptions& options) override;

    virtual ::NThreading::TFuture<void> UpdateOperationParameters(
        const TOperationId& operationId,
        const TUpdateOperationParametersOptions& options) override;

    virtual ::NThreading::TFuture<TRichYPath> CanonizeYPath(const TRichYPath& path) override;

    virtual ::NThreading::TFuture<TVector<TTableColumnarStatistics>> GetTableColumnarStatistics(
        const TVector<TRichYPath>& paths,
        const TGetTableColumnarStatisticsOptions& options) override;

    ::NThreading::TFuture<TCheckPermissionResponse> CheckPermission(
        const TString& user,
        EPermission permission,
        const TYPath& path,
        const TCheckPermissionOptions& options) override;

    virtual void ExecuteBatch(const TExecuteBatchOptions& executeBatch) override;

private:
    TBatchRequest(NDetail::NRawClient::TRawBatchRequest* impl, ::TIntrusivePtr<TClient> client);

private:
    TTransactionId DefaultTransaction_;
    ::TIntrusivePtr<NDetail::NRawClient::TRawBatchRequest> Impl_;
    THolder<TBatchRequest> TmpWithTransaction_;
    ::TIntrusivePtr<TClient> Client_;

private:
    friend class NYT::NDetail::TClient;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
