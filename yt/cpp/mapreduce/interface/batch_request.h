#pragma once

#include "fwd.h"

#include "client_method_options.h"

#include <library/cpp/threading/future/future.h>
#include <util/generic/ptr.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// Helper base of @ref NYT::IBatchRequest holding most of useful methods.
class IBatchRequestBase
    : public TThrRefBase
{
public:
    virtual ~IBatchRequestBase() = default;

    ///
    /// @brief Create cypress node.
    ///
    /// @see NYT::ICypressClient::Create
    virtual ::NThreading::TFuture<TNodeId> Create(
        const TYPath& path,
        ENodeType type,
        const TCreateOptions& options = TCreateOptions()) = 0;

    ///
    /// @brief Remove cypress node.
    ///
    /// @see NYT::ICypressClient::Remove
    virtual ::NThreading::TFuture<void> Remove(
        const TYPath& path,
        const TRemoveOptions& options = TRemoveOptions()) = 0;

    ///
    /// @brief Check wether cypress node exists.
    ///
    /// @see NYT::ICypressClient::Exists
    virtual ::NThreading::TFuture<bool> Exists(
        const TYPath& path,
        const TExistsOptions& options = TExistsOptions()) = 0;

    ///
    /// @brief Get cypress node.
    ///
    /// @see NYT::ICypressClient::Get
    virtual ::NThreading::TFuture<TNode> Get(
        const TYPath& path,
        const TGetOptions& options = TGetOptions()) = 0;

    ///
    /// @brief Set cypress node.
    ///
    /// @see NYT::ICypressClient::Set
    virtual ::NThreading::TFuture<void> Set(
        const TYPath& path,
        const TNode& node,
        const TSetOptions& options = TSetOptions()) = 0;

    ///
    /// @brief List cypress directory.
    ///
    /// @see NYT::ICypressClient::List
    virtual ::NThreading::TFuture<TNode::TListType> List(
        const TYPath& path,
        const TListOptions& options = TListOptions()) = 0;

    ///
    /// @brief Copy cypress node.
    ///
    /// @see NYT::ICypressClient::Copy
    virtual ::NThreading::TFuture<TNodeId> Copy(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = TCopyOptions()) = 0;

    ///
    /// @brief Move cypress node.
    ///
    /// @see NYT::ICypressClient::Move
    virtual ::NThreading::TFuture<TNodeId> Move(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = TMoveOptions()) = 0;

    ///
    /// @brief Create symbolic link.
    ///
    /// @see NYT::ICypressClient::Link.
    virtual ::NThreading::TFuture<TNodeId> Link(
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options = TLinkOptions()) = 0;

    ///
    /// @brief Lock cypress node.
    ///
    /// @see NYT::ICypressClient::Lock
    virtual ::NThreading::TFuture<ILockPtr> Lock(
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options = TLockOptions()) = 0;

    ///
    /// @brief Unlock cypress node.
    ///
    /// @see NYT::ICypressClient::Unlock
    virtual ::NThreading::TFuture<void> Unlock(
        const TYPath& path,
        const TUnlockOptions& options = TUnlockOptions()) = 0;

    ///
    /// @brief Abort operation.
    ///
    /// @see NYT::IClient::AbortOperation
    virtual ::NThreading::TFuture<void> AbortOperation(const TOperationId& operationId) = 0;

    ///
    /// @brief Force complete operation.
    ///
    /// @see NYT::IClient::CompleteOperation
    virtual ::NThreading::TFuture<void> CompleteOperation(const TOperationId& operationId) = 0;

    ///
    /// @brief Suspend operation.
    ///
    /// @see NYT::IClient::SuspendOperation
    virtual ::NThreading::TFuture<void> SuspendOperation(
        const TOperationId& operationId,
        const TSuspendOperationOptions& options = TSuspendOperationOptions()) = 0;

    ///
    /// @brief Resume operation.
    ///
    /// @see NYT::IClient::ResumeOperation
    virtual ::NThreading::TFuture<void> ResumeOperation(
        const TOperationId& operationId,
        const TResumeOperationOptions& options = TResumeOperationOptions()) = 0;

    ///
    /// @brief Update parameters of running operation.
    ///
    /// @see NYT::IClient::UpdateOperationParameters
    virtual ::NThreading::TFuture<void> UpdateOperationParameters(
        const TOperationId& operationId,
        const TUpdateOperationParametersOptions& options = TUpdateOperationParametersOptions()) = 0;

    ///
    /// @brief Canonize cypress path
    ///
    /// @see NYT::ICypressClient::CanonizeYPath
    virtual ::NThreading::TFuture<TRichYPath> CanonizeYPath(const TRichYPath& path) = 0;

    ///
    /// @brief Get table columnar statistic
    ///
    /// @see NYT::ICypressClient::GetTableColumnarStatistics
    virtual ::NThreading::TFuture<TVector<TTableColumnarStatistics>> GetTableColumnarStatistics(
        const TVector<TRichYPath>& paths,
        const TGetTableColumnarStatisticsOptions& options = {}) = 0;

    ///
    /// @brief Check permission for given path.
    ///
    /// @see NYT::IClient::CheckPermission
    virtual ::NThreading::TFuture<TCheckPermissionResponse> CheckPermission(
        const TString& user,
        EPermission permission,
        const TYPath& path,
        const TCheckPermissionOptions& options = TCheckPermissionOptions()) = 0;
};

///
/// @brief Batch request object.
///
/// Allows to send multiple lightweight requests at once significantly
/// reducing time of their execution.
///
/// Methods of this class accept same arguments as @ref NYT::IClient methods but
/// return TFuture that is set after execution of @ref NYT::IBatchRequest::ExecuteBatch
///
/// @see [Example of usage](https://a.yandex-team.ru/arc/trunk/arcadia/yt/cpp/mapreduce/examples/tutorial/batch_request/main.cpp)
class IBatchRequest
    : public IBatchRequestBase
{
public:
    ///
    /// @brief Temporary override current transaction.
    ///
    /// Using WithTransaction user can temporary override default transaction.
    /// Example of usage:
    ///   TBatchRequest batchRequest;
    ///   auto noTxResult = batchRequest.Get("//some/path");
    ///   auto txResult = batchRequest.WithTransaction(tx).Get("//some/path");
    virtual IBatchRequestBase& WithTransaction(const TTransactionId& transactionId) = 0;
    IBatchRequestBase& WithTransaction(const ITransactionPtr& transaction);

    ///
    /// @brief Executes all subrequests of batch request.
    ///
    /// After execution of this method all TFuture objects returned by subrequests will
    /// be filled with either result or error.
    ///
    /// @note It is undefined in which order these requests are executed.
    ///
    /// @note This method doesn't throw if subrequest emits error.
    /// Instead corresponding future is set with exception.
    /// So it is always important to check TFuture status.
    ///
    /// Single TBatchRequest instance may be executed only once
    /// and cannot be modified (filled with additional requests) after execution.
    /// Exception is thrown on attempt to modify executed batch request
    /// or execute it again.
    virtual void ExecuteBatch(const TExecuteBatchOptions& options = TExecuteBatchOptions()) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
