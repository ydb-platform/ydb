#include "batch_request_impl.h"

#include "lock.h"

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <yt/cpp/mapreduce/http/retry_request.h>

#include <yt/cpp/mapreduce/interface/config.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/serialize.h>

#include <yt/cpp/mapreduce/raw_client/raw_requests.h>
#include <yt/cpp/mapreduce/raw_client/raw_batch_request.h>
#include <yt/cpp/mapreduce/raw_client/rpc_parameters_serialization.h>

#include <util/generic/guid.h>
#include <util/string/builder.h>

#include <exception>

namespace NYT {
namespace NDetail {

using namespace NRawClient;

using ::NThreading::TFuture;
using ::NThreading::TPromise;
using ::NThreading::NewPromise;

////////////////////////////////////////////////////////////////////////////////

TBatchRequest::TBatchRequest(const TTransactionId& defaultTransaction, ::TIntrusivePtr<TClient> client)
    : DefaultTransaction_(defaultTransaction)
    , Impl_(MakeIntrusive<TRawBatchRequest>(client->GetContext().Config))
    , Client_(client)
{ }

TBatchRequest::TBatchRequest(TRawBatchRequest* impl, ::TIntrusivePtr<TClient> client)
    : Impl_(impl)
    , Client_(std::move(client))
{ }

TBatchRequest::~TBatchRequest() = default;

IBatchRequestBase& TBatchRequest::WithTransaction(const TTransactionId& transactionId)
{
    if (!TmpWithTransaction_) {
        TmpWithTransaction_.Reset(new TBatchRequest(Impl_.Get(), Client_));
    }
    TmpWithTransaction_->DefaultTransaction_ = transactionId;
    return *TmpWithTransaction_;
}

TFuture<TNode> TBatchRequest::Get(
    const TYPath& path,
    const TGetOptions& options)
{
    return Impl_->Get(DefaultTransaction_, path, options);
}

TFuture<void> TBatchRequest::Set(const TYPath& path, const TNode& node, const TSetOptions& options)
{
    return Impl_->Set(DefaultTransaction_, path, node, options);
}

TFuture<TNode::TListType> TBatchRequest::List(const TYPath& path, const TListOptions& options)
{
    return Impl_->List(DefaultTransaction_, path, options);
}

TFuture<bool> TBatchRequest::Exists(const TYPath& path, const TExistsOptions& options)
{
    return Impl_->Exists(DefaultTransaction_, path, options);
}

TFuture<ILockPtr> TBatchRequest::Lock(
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    auto convert = [waitable=options.Waitable_, client=Client_] (TFuture<TNodeId> nodeIdFuture) -> ILockPtr {
        return ::MakeIntrusive<TLock>(nodeIdFuture.GetValue(), client, waitable);
    };
    return Impl_->Lock(DefaultTransaction_, path, mode, options).Apply(convert);
}

::NThreading::TFuture<void> TBatchRequest::Unlock(
    const TYPath& path,
    const TUnlockOptions& options = TUnlockOptions())
{
    return Impl_->Unlock(DefaultTransaction_, path, options);
}

TFuture<TLockId> TBatchRequest::Create(
    const TYPath& path,
    ENodeType type,
    const TCreateOptions& options)
{
    return Impl_->Create(DefaultTransaction_, path, type, options);
}

TFuture<void> TBatchRequest::Remove(
    const TYPath& path,
    const TRemoveOptions& options)
{
    return Impl_->Remove(DefaultTransaction_, path, options);
}

TFuture<TNodeId> TBatchRequest::Move(
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    return Impl_->Move(DefaultTransaction_, sourcePath, destinationPath, options);
}

TFuture<TNodeId> TBatchRequest::Copy(
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    return Impl_->Copy(DefaultTransaction_, sourcePath, destinationPath, options);
}

TFuture<TNodeId> TBatchRequest::Link(
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
{
    return Impl_->Link(DefaultTransaction_, targetPath, linkPath, options);
}

TFuture<void> TBatchRequest::AbortOperation(const NYT::TOperationId& operationId)
{
    return Impl_->AbortOperation(operationId);
}

TFuture<void> TBatchRequest::CompleteOperation(const NYT::TOperationId& operationId)
{
    return Impl_->CompleteOperation(operationId);
}

TFuture<void> TBatchRequest::SuspendOperation(
    const TOperationId& operationId,
    const TSuspendOperationOptions& options)
{
    return Impl_->SuspendOperation(operationId, options);
}

TFuture<void> TBatchRequest::ResumeOperation(
    const TOperationId& operationId,
    const TResumeOperationOptions& options)
{
    return Impl_->ResumeOperation(operationId, options);
}

TFuture<void> TBatchRequest::UpdateOperationParameters(
    const NYT::TOperationId& operationId,
    const NYT::TUpdateOperationParametersOptions& options)
{
    return Impl_->UpdateOperationParameters(operationId, options);
}

TFuture<TRichYPath> TBatchRequest::CanonizeYPath(const TRichYPath& path)
{
    return Impl_->CanonizeYPath(path);
}

TFuture<TVector<TTableColumnarStatistics>> TBatchRequest::GetTableColumnarStatistics(
    const TVector<TRichYPath>& paths,
    const NYT::TGetTableColumnarStatisticsOptions& options)
{
    return Impl_->GetTableColumnarStatistics(DefaultTransaction_, paths, options);
}

TFuture<TCheckPermissionResponse> TBatchRequest::CheckPermission(
    const TString& user,
    EPermission permission,
    const TYPath& path,
    const TCheckPermissionOptions& options)
{
    return Impl_->CheckPermission(user, permission, path, options);
}

void TBatchRequest::ExecuteBatch(const TExecuteBatchOptions& options)
{
    NYT::NDetail::ExecuteBatch(Client_->GetRetryPolicy()->CreatePolicyForGenericRequest(), Client_->GetContext(), *Impl_, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
