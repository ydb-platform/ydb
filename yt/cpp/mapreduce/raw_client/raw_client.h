#pragma once

#include <yt/cpp/mapreduce/http/context.h>

#include <yt/cpp/mapreduce/interface/client_method_options.h>
#include <yt/cpp/mapreduce/interface/raw_client.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

class THttpRawClient
    : public IRawClient
{
public:
    THttpRawClient(const TClientContext& context);

    // Cypress

    TNode Get(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TGetOptions& options = {}) override;

    TNode TryGet(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TGetOptions& options) override;

    void Set(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TNode& value,
        const TSetOptions& options = {}) override;

    bool Exists(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TExistsOptions& options = {}) override;

    void MultisetAttributes(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TNode::TMapType& value,
        const TMultisetAttributesOptions& options = {}) override;

    TNodeId Create(
        TMutationId& mutatatonId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const ENodeType& type,
        const TCreateOptions& options = {}) override;

    TNodeId CopyWithoutRetries(
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = {}) override;

    TNodeId CopyInsideMasterCell(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options = {}) override;

    TNodeId MoveWithoutRetries(
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = {}) override;

    TNodeId MoveInsideMasterCell(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options = {}) override;

    void Remove(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TRemoveOptions& options = {}) override;

    TNode::TListType List(
        const TTransactionId& transactionId,
        const TYPath& path,
        const TListOptions& options = {}) override;

    TNodeId Link(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options = {}) override;

    TLockId Lock(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options = {}) override;

    void Unlock(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TUnlockOptions& options = {}) override;

    void Concatenate(
        const TTransactionId& transactionId,
        const TVector<TRichYPath>& sourcePaths,
        const TRichYPath& destinationPath,
        const TConcatenateOptions& options = {}) override;

    // Transactions

    void PingTx(const TTransactionId& transactionId) override;

    // Operations

    TOperationAttributes GetOperation(
        const TOperationId& operationId,
        const TGetOperationOptions& options = {}) override;

    TOperationAttributes GetOperation(
        const TString& operationId,
        const TGetOperationOptions& options = {}) override;

    void AbortOperation(
        TMutationId& mutationId,
        const TOperationId& operationId) override;

    void CompleteOperation(
        TMutationId& mutationId,
        const TOperationId& operationId) override;

    void SuspendOperation(
        TMutationId& mutationId,
        const TOperationId& operationId,
        const TSuspendOperationOptions& options = {}) override;

    void ResumeOperation(
        TMutationId& mutationId,
        const TOperationId& operationId,
        const TResumeOperationOptions& options = {}) override;

    TListOperationsResult ListOperations(const TListOperationsOptions& options = {}) override;

    void UpdateOperationParameters(
        const TOperationId& operationId,
        const TUpdateOperationParametersOptions& options = {}) override;

private:
    const TClientContext Context_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
