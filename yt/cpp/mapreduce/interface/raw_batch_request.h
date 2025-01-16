#pragma once

#include "client_method_options.h"
#include "fwd.h"
#include "operation.h"

#include <library/cpp/threading/future/core/future.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class IRawBatchRequest
    : public virtual TThrRefBase
{
public:
    virtual void ExecuteBatch(const TExecuteBatchOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<TNodeId> Create(
        const TTransactionId& transaction,
        const TYPath& path,
        ENodeType type,
        const TCreateOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<void> Remove(
        const TTransactionId& transaction,
        const TYPath& path,
        const TRemoveOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<bool> Exists(
        const TTransactionId& transaction,
        const TYPath& path,
        const TExistsOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<TNode> Get(
        const TTransactionId& transaction,
        const TYPath& path,
        const TGetOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<void> Set(
        const TTransactionId& transaction,
        const TYPath& path,
        const TNode& value,
        const TSetOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<TNode::TListType> List(
        const TTransactionId& transaction,
        const TYPath& path,
        const TListOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<TNodeId> Copy(
        const TTransactionId& transaction,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<TNodeId> Move(
        const TTransactionId& transaction,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<TNodeId> Link(
        const TTransactionId& transaction,
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<TLockId> Lock(
        const TTransactionId& transaction,
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<void> Unlock(
        const TTransactionId& transaction,
        const TYPath& path,
        const TUnlockOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<TMaybe<TYPath>> GetFileFromCache(
        const TTransactionId& transactionId,
        const TString& md5Signature,
        const TYPath& cachePath,
        const TGetFileFromCacheOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<TYPath> PutFileToCache(
        const TTransactionId& transactionId,
        const TYPath& filePath,
        const TString& md5Signature,
        const TYPath& cachePath,
        const TPutFileToCacheOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<TCheckPermissionResponse> CheckPermission(
        const TString& user,
        EPermission permission,
        const TYPath& path,
        const TCheckPermissionOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<TOperationAttributes> GetOperation(
        const TOperationId& operationId,
        const TGetOperationOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<void> AbortOperation(const TOperationId& operationId) = 0;

    virtual ::NThreading::TFuture<void> CompleteOperation(const TOperationId& operationId) = 0;

    virtual ::NThreading::TFuture<void> SuspendOperation(
        const TOperationId& operationId,
        const TSuspendOperationOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<void> ResumeOperation(
        const TOperationId& operationId,
        const TResumeOperationOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<void> UpdateOperationParameters(
        const TOperationId& operationId,
        const TUpdateOperationParametersOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<TRichYPath> CanonizeYPath(const TRichYPath& path) = 0;

    virtual ::NThreading::TFuture<TVector<TTableColumnarStatistics>> GetTableColumnarStatistics(
        const TTransactionId& transaction,
        const TVector<TRichYPath>& paths,
        const TGetTableColumnarStatisticsOptions& options = {}) = 0;

    virtual ::NThreading::TFuture<TMultiTablePartitions> GetTablePartitions(
        const TTransactionId& transaction,
        const TVector<TRichYPath>& paths,
        const TGetTablePartitionsOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
