#include "client.h"

#include "batch_request_impl.h"
#include "client_reader.h"
#include "client_writer.h"
#include "file_reader.h"
#include "file_writer.h"
#include "file_fragment_writer.h"
#include "format_hints.h"
#include "init.h"
#include "lock.h"
#include "operation.h"
#include "partition_reader.h"
#include "retryful_writer.h"
#include "transaction.h"
#include "transaction_pinger.h"
#include "yt_poller.h"

#include <yt/cpp/mapreduce/common/expected_error_guard.h>
#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <yt/cpp/mapreduce/http/helpers.h>
#include <yt/cpp/mapreduce/http/http.h>
#include <yt/cpp/mapreduce/http/http_client.h>
#include <yt/cpp/mapreduce/http/requests.h>
#include <yt/cpp/mapreduce/http/retry_request.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/error_codes.h>
#include <yt/cpp/mapreduce/interface/fluent.h>
#include <yt/cpp/mapreduce/interface/logging/yt_log.h>
#include <yt/cpp/mapreduce/interface/skiff_row.h>

#include <yt/cpp/mapreduce/io/yamr_table_reader.h>
#include <yt/cpp/mapreduce/io/yamr_table_writer.h>
#include <yt/cpp/mapreduce/io/node_table_reader.h>
#include <yt/cpp/mapreduce/io/node_table_writer.h>
#include <yt/cpp/mapreduce/io/proto_table_reader.h>
#include <yt/cpp/mapreduce/io/proto_table_writer.h>
#include <yt/cpp/mapreduce/io/skiff_row_table_reader.h>
#include <yt/cpp/mapreduce/io/proto_helpers.h>

#include <yt/cpp/mapreduce/library/table_schema/protobuf.h>

#include <yt/cpp/mapreduce/http_client/raw_client.h>
#include <yt/cpp/mapreduce/http_client/raw_requests.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/algorithm.h>
#include <util/string/type.h>
#include <util/system/env.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void ApplyProxyUrlAliasingRules(
    TString& url,
    const THashMap<TString, TString>& proxyUrlAliasingRules)
{
    if (auto ruleIt = proxyUrlAliasingRules.find(url);
        ruleIt != proxyUrlAliasingRules.end()
    ) {
        url = ruleIt->second;
    }
}

bool IsCrossCellDisabledError(const TErrorResponse& e)
{
    return e.GetError().ContainsErrorCode(NClusterErrorCodes::NObjectClient::CrossCellAdditionalPath);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TClientBase::TClientBase(
    IRawClientPtr rawClient,
    const TClientContext& context,
    const TTransactionId& transactionId,
    IClientRetryPolicyPtr retryPolicy)
    : RawClient_(std::move(rawClient))
    , Context_(context)
    , TransactionId_(transactionId)
    , ClientRetryPolicy_(std::move(retryPolicy))
{ }

ITransactionPtr TClientBase::StartTransaction(
    const TStartTransactionOptions& options)
{
    return MakeIntrusive<TTransaction>(RawClient_, GetParentClientImpl(), Context_, TransactionId_, options);
}

TDistributedWriteFileSessionWithCookies TClientBase::StartDistributedWriteFileSession(
    const TRichYPath& richPath,
    i64 cookieCount,
    const TStartDistributedWriteFileOptions& options)
{
    return RequestWithRetry<TDistributedWriteFileSessionWithCookies>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, cookieCount, &richPath, &options] (TMutationId& mutationId) {
            return RawClient_->StartDistributedWriteFileSession(mutationId, TransactionId_, richPath, cookieCount, options);
        });
}

TDistributedWriteTableSessionWithCookies TClientBase::StartDistributedWriteTableSession(
    const TRichYPath& richPath,
    i64 cookieCount,
    const TStartDistributedWriteTableOptions& options)
{
    return RequestWithRetry<TDistributedWriteTableSessionWithCookies>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, cookieCount, &richPath, &options] (TMutationId& mutationId) {
            return RawClient_->StartDistributedWriteTableSession(mutationId, TransactionId_, richPath, cookieCount, options);
        });
}

TNodeId TClientBase::Create(
    const TYPath& path,
    ENodeType type,
    const TCreateOptions& options)
{
    return RequestWithRetry<TNodeId>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &type, &options] (TMutationId& mutationId) {
            return RawClient_->Create(mutationId, TransactionId_, path, type, options);
        });
}

void TClientBase::Remove(
    const TYPath& path,
    const TRemoveOptions& options)
{
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &options] (TMutationId& mutationId) {
            RawClient_->Remove(mutationId, TransactionId_, path, options);
        });
}

bool TClientBase::Exists(
    const TYPath& path,
    const TExistsOptions& options)
{
    return RequestWithRetry<bool>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &options] (TMutationId /*mutationId*/) {
            return RawClient_->Exists(TransactionId_, path, options);
        });
}

TNode TClientBase::Get(
    const TYPath& path,
    const TGetOptions& options)
{
    return RequestWithRetry<TNode>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &options] (TMutationId /*mutationId*/) {
            return RawClient_->Get(TransactionId_, path, options);
        });
}

void TClientBase::Set(
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options)
{
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &value, &options] (TMutationId& mutationId) {
            RawClient_->Set(mutationId, TransactionId_, path, value, options);
        });
}

void TClientBase::MultisetAttributes(
    const TYPath& path,
    const TNode::TMapType& value,
    const TMultisetAttributesOptions& options)
{
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &value, &options] (TMutationId& mutationId) {
            RawClient_->MultisetAttributes(mutationId, TransactionId_, path, value, options);
        });
}

TNode::TListType TClientBase::List(
    const TYPath& path,
    const TListOptions& options)
{
    return RequestWithRetry<TNode::TListType>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &options] (TMutationId /*mutationId*/) {
            return RawClient_->List(TransactionId_, path, options);
        });
}

TNodeId TClientBase::Copy(
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    TExpectedErrorGuard guard(IsCrossCellDisabledError);

    try {
        return RequestWithRetry<TNodeId>(
            ClientRetryPolicy_->CreatePolicyForGenericRequest(),
            [this, &sourcePath, &destinationPath, &options] (TMutationId& mutationId) {
                return RawClient_->CopyInsideMasterCell(mutationId, TransactionId_, sourcePath, destinationPath, options);
            });
    } catch (const TErrorResponse& e) {
        if (IsCrossCellDisabledError(e)) {
            // Do transaction for cross cell copying.
            return RequestWithRetry<TNodeId>(
                ClientRetryPolicy_->CreatePolicyForGenericRequest(),
                [this, &sourcePath, &destinationPath, &options] (TMutationId /*mutationId*/) {
                    auto transaction = StartTransaction(TStartTransactionOptions());
                    auto nodeId = RawClient_->CopyWithoutRetries(transaction->GetId(), sourcePath, destinationPath, options);
                    transaction->Commit();
                    return nodeId;
                });
        } else {
            throw;
        }
    }
}

TNodeId TClientBase::Move(
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    try {
        return RequestWithRetry<TNodeId>(
            ClientRetryPolicy_->CreatePolicyForGenericRequest(),
            [this, &sourcePath, &destinationPath, &options] (TMutationId& mutationId) {
                return RawClient_->MoveInsideMasterCell(mutationId, TransactionId_, sourcePath, destinationPath, options);
            });
    } catch (const TErrorResponse& e) {
        if (e.GetError().ContainsErrorCode(NClusterErrorCodes::NObjectClient::CrossCellAdditionalPath)) {
            // Do transaction for cross cell moving.
            return RequestWithRetry<TNodeId>(
                ClientRetryPolicy_->CreatePolicyForGenericRequest(),
                [this, &sourcePath, &destinationPath, &options] (TMutationId /*mutationId*/) {
                    auto transaction = StartTransaction(TStartTransactionOptions());
                    auto nodeId = RawClient_->MoveWithoutRetries(transaction->GetId(), sourcePath, destinationPath, options);
                    transaction->Commit();
                    return nodeId;
                });
        } else {
            throw;
        }
    }
}

TNodeId TClientBase::Link(
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
{
    return RequestWithRetry<TNodeId>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &targetPath, &linkPath, &options] (TMutationId& mutationId) {
            return RawClient_->Link(mutationId, TransactionId_, targetPath, linkPath, options);
        });
}

void TClientBase::Concatenate(
    const TVector<TRichYPath>& sourcePaths,
    const TRichYPath& destinationPath,
    const TConcatenateOptions& options)
{
    Y_ABORT_IF(options.MaxBatchSize_ <= 0);

    ITransactionPtr outerTransaction;
    IClientBase* outerClient;
    if (std::ssize(sourcePaths) > options.MaxBatchSize_) {
        outerTransaction = StartTransaction(TStartTransactionOptions());
        outerClient = outerTransaction.Get();
    } else {
        outerClient = this;
    }

    TVector<TRichYPath> batch;
    for (ssize_t i = 0; i < std::ssize(sourcePaths); i += options.MaxBatchSize_) {
        auto begin = sourcePaths.begin() + i;
        auto end = sourcePaths.begin() + std::min(i + options.MaxBatchSize_, std::ssize(sourcePaths));
        batch.assign(begin, end);

        bool firstBatch = (i == 0);
        RequestWithRetry<void>(
            ClientRetryPolicy_->CreatePolicyForGenericRequest(),
            [this, &batch, &destinationPath, &options, outerClient, firstBatch] (TMutationId /*mutationId*/) {
                auto transaction = outerClient->StartTransaction(TStartTransactionOptions());

                if (firstBatch && !options.Append_ && !batch.empty() && !transaction->Exists(destinationPath.Path_)) {
                    auto typeNode = transaction->Get(transaction->CanonizeYPath(batch.front()).Path_ + "/@type");
                    auto type = FromString<ENodeType>(typeNode.AsString());
                    transaction->Create(destinationPath.Path_, type, TCreateOptions().IgnoreExisting(true));
                }

                TConcatenateOptions currentOptions = options;
                if (!firstBatch) {
                    currentOptions.Append_ = true;
                }

                RawClient_->Concatenate(transaction->GetId(), batch, destinationPath, currentOptions);

                transaction->Commit();
            });
    }

    if (outerTransaction) {
        outerTransaction->Commit();
    }
}

TRichYPath TClientBase::CanonizeYPath(const TRichYPath& path)
{
    return NRawClient::CanonizeYPath(RawClient_, path);
}

TVector<TTableColumnarStatistics> TClientBase::GetTableColumnarStatistics(
    const TVector<TRichYPath>& paths,
    const TGetTableColumnarStatisticsOptions& options)
{
    return RequestWithRetry<TVector<TTableColumnarStatistics>>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &paths, &options] (TMutationId /*mutationId*/) {
            return RawClient_->GetTableColumnarStatistics(TransactionId_, paths, options);
        });
}

TMultiTablePartitions TClientBase::GetTablePartitions(
    const TVector<TRichYPath>& paths,
    const TGetTablePartitionsOptions& options)
{
    return RequestWithRetry<TMultiTablePartitions>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &paths, &options] (TMutationId /*mutationId*/) {
            return RawClient_->GetTablePartitions(TransactionId_, paths, options);
        });
}

TMaybe<TYPath> TClientBase::GetFileFromCache(
    const TString& md5Signature,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions& options)
{
    return RequestWithRetry<TMaybe<TYPath>>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &md5Signature, &cachePath, &options] (TMutationId /*mutationId*/) {
            return RawClient_->GetFileFromCache(TransactionId_, md5Signature, cachePath, options);
        });
}

TYPath TClientBase::PutFileToCache(
    const TYPath& filePath,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options)
{
    return RequestWithRetry<TYPath>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &filePath, &md5Signature, &cachePath, &options] (TMutationId /*mutationId*/) {
            return RawClient_->PutFileToCache(TransactionId_, filePath, md5Signature, cachePath, options);
        });
}

IFileReaderPtr TClientBase::CreateBlobTableReader(
    const TYPath& path,
    const TKey& key,
    const TBlobTableReaderOptions& options)
{
    return new TBlobTableReader(
        path,
        key,
        RawClient_,
        ClientRetryPolicy_,
        GetTransactionPinger(),
        Context_,
        TransactionId_,
        options);
}

IFileReaderPtr TClientBase::CreateFileReader(
    const TRichYPath& path,
    const TFileReaderOptions& options)
{
    return new TFileReader(
        CanonizeYPath(path),
        RawClient_,
        ClientRetryPolicy_,
        GetTransactionPinger(),
        Context_,
        TransactionId_,
        options);
}

IFileWriterPtr TClientBase::CreateFileWriter(
    const TRichYPath& path,
    const TFileWriterOptions& options)
{
    auto realPath = CanonizeYPath(path);

    auto exists = RequestWithRetry<bool>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &realPath] (TMutationId /*mutationId*/) {
            return RawClient_->Exists(TransactionId_, realPath.Path_);
        });
    if (!exists) {
        RequestWithRetry<void>(
            ClientRetryPolicy_->CreatePolicyForGenericRequest(),
            [this, &realPath] (TMutationId& mutationId) {
                RawClient_->Create(mutationId, TransactionId_, realPath.Path_, NT_FILE, TCreateOptions().IgnoreExisting(true));
            });
    }

    return new TFileWriter(realPath, RawClient_, ClientRetryPolicy_, GetTransactionPinger(), Context_, TransactionId_, options);
}

IFileFragmentWriterPtr TClientBase::CreateFileFragmentWriter(
    const TDistributedWriteFileCookie& cookie,
    const TFileFragmentWriterOptions& options)
{
    return NDetail::CreateFileFragmentWriter(RawClient_, ClientRetryPolicy_->CreatePolicyForGenericRequest(), cookie, options);
}

TTableWriterPtr<::google::protobuf::Message> TClientBase::CreateTableWriter(
    const TRichYPath& path, const ::google::protobuf::Descriptor& descriptor, const TTableWriterOptions& options)
{
    const Message* prototype = google::protobuf::MessageFactory::generated_factory()->GetPrototype(&descriptor);
    return new TTableWriter<::google::protobuf::Message>(CreateProtoWriter(path, options, prototype));
}

TRawTableReaderPtr TClientBase::CreateRawReader(
    const TRichYPath& path,
    const TFormat& format,
    const TTableReaderOptions& options)
{
    return CreateClientReader(path, format, options).Get();
}

TRawTableReaderPtr TClientBase::CreateRawTablePartitionReader(
        const TString& cookie,
        const TFormat& format,
        const TTablePartitionReaderOptions& options)
{
    return NDetail::CreateTablePartitionReader(RawClient_, ClientRetryPolicy_->CreatePolicyForReaderRequest(), cookie, format, options);
}

TRawTableWriterPtr TClientBase::CreateRawWriter(
    const TRichYPath& path,
    const TFormat& format,
    const TTableWriterOptions& options)
{
    return ::MakeIntrusive<TRetryfulWriter>(
        RawClient_,
        ClientRetryPolicy_,
        GetTransactionPinger(),
        Context_,
        TransactionId_,
        format,
        CanonizeYPath(path),
        options).Get();
}

IOperationPtr TClientBase::DoMap(
    const TMapOperationSpec& spec,
    ::TIntrusivePtr<IStructuredJob> mapper,
    const TOperationOptions& options)
{
    auto operation = ::MakeIntrusive<TOperation>(GetParentClientImpl());
    auto prepareOperation = [
        this_ = ::TIntrusivePtr(this),
        operation,
        spec,
        mapper,
        options
    ] () {
        ExecuteMap(
            operation,
            ::MakeIntrusive<TOperationPreparer>(this_->GetParentClientImpl(), this_->TransactionId_),
            spec,
            mapper,
            options);
    };
    return ProcessOperation(GetParentClientImpl(), std::move(prepareOperation), std::move(operation), options);
}

IOperationPtr TClientBase::RawMap(
    const TRawMapOperationSpec& spec,
    ::TIntrusivePtr<IRawJob> mapper,
    const TOperationOptions& options)
{
    auto operation = ::MakeIntrusive<TOperation>(GetParentClientImpl());
    auto prepareOperation = [
        this_=::TIntrusivePtr(this),
        operation,
        spec,
        mapper,
        options
    ] () {
        ExecuteRawMap(
            operation,
            ::MakeIntrusive<TOperationPreparer>(this_->GetParentClientImpl(), this_->TransactionId_),
            spec,
            mapper,
            options);
    };
    return ProcessOperation(GetParentClientImpl(), std::move(prepareOperation), std::move(operation), options);
}

IOperationPtr TClientBase::DoReduce(
    const TReduceOperationSpec& spec,
    ::TIntrusivePtr<IStructuredJob> reducer,
    const TOperationOptions& options)
{
    auto operation = ::MakeIntrusive<TOperation>(GetParentClientImpl());
    auto prepareOperation = [
        this_=::TIntrusivePtr(this),
        operation,
        spec,
        reducer,
        options
    ] () {
        ExecuteReduce(
            operation,
            ::MakeIntrusive<TOperationPreparer>(this_->GetParentClientImpl(), this_->TransactionId_),
            spec,
            reducer,
            options);
    };
    return ProcessOperation(GetParentClientImpl(), std::move(prepareOperation), std::move(operation), options);
}

IOperationPtr TClientBase::RawReduce(
    const TRawReduceOperationSpec& spec,
    ::TIntrusivePtr<IRawJob> reducer,
    const TOperationOptions& options)
{
    auto operation = ::MakeIntrusive<TOperation>(GetParentClientImpl());
    auto prepareOperation = [
        this_=::TIntrusivePtr(this),
        operation,
        spec,
        reducer,
        options
    ] () {
        ExecuteRawReduce(
            operation,
            ::MakeIntrusive<TOperationPreparer>(this_->GetParentClientImpl(), this_->TransactionId_),
            spec,
            reducer,
            options);
    };
    return ProcessOperation(GetParentClientImpl(), std::move(prepareOperation), std::move(operation), options);
}

IOperationPtr TClientBase::DoJoinReduce(
    const TJoinReduceOperationSpec& spec,
    ::TIntrusivePtr<IStructuredJob> reducer,
    const TOperationOptions& options)
{
    auto operation = ::MakeIntrusive<TOperation>(GetParentClientImpl());
    auto prepareOperation = [
        this_=::TIntrusivePtr(this),
        operation,
        spec,
        reducer,
        options
    ] () {
        ExecuteJoinReduce(
            operation,
            ::MakeIntrusive<TOperationPreparer>(this_->GetParentClientImpl(), this_->TransactionId_),
            spec,
            reducer,
            options);
    };
    return ProcessOperation(GetParentClientImpl(), std::move(prepareOperation), std::move(operation), options);
}

IOperationPtr TClientBase::RawJoinReduce(
    const TRawJoinReduceOperationSpec& spec,
    ::TIntrusivePtr<IRawJob> reducer,
    const TOperationOptions& options)
{
    auto operation = ::MakeIntrusive<TOperation>(GetParentClientImpl());
    auto prepareOperation = [
        this_=::TIntrusivePtr(this),
        operation,
        spec,
        reducer,
        options
    ] () {
        ExecuteRawJoinReduce(
            operation,
            ::MakeIntrusive<TOperationPreparer>(this_->GetParentClientImpl(), this_->TransactionId_),
            spec,
            reducer,
            options);
    };
    return ProcessOperation(GetParentClientImpl(), std::move(prepareOperation), std::move(operation), options);
}

IOperationPtr TClientBase::DoMapReduce(
    const TMapReduceOperationSpec& spec,
    ::TIntrusivePtr<IStructuredJob> mapper,
    ::TIntrusivePtr<IStructuredJob> reduceCombiner,
    ::TIntrusivePtr<IStructuredJob> reducer,
    const TOperationOptions& options)
{
    auto operation = ::MakeIntrusive<TOperation>(GetParentClientImpl());
    auto prepareOperation = [
        this_=::TIntrusivePtr(this),
        operation,
        spec,
        mapper,
        reduceCombiner,
        reducer,
        options
    ] () {
        ExecuteMapReduce(
            operation,
            ::MakeIntrusive<TOperationPreparer>(this_->GetParentClientImpl(), this_->TransactionId_),
            spec,
            mapper,
            reduceCombiner,
            reducer,
            options);
    };
    return ProcessOperation(GetParentClientImpl(), std::move(prepareOperation), std::move(operation), options);
}

IOperationPtr TClientBase::RawMapReduce(
    const TRawMapReduceOperationSpec& spec,
    ::TIntrusivePtr<IRawJob> mapper,
    ::TIntrusivePtr<IRawJob> reduceCombiner,
    ::TIntrusivePtr<IRawJob> reducer,
    const TOperationOptions& options)
{
    auto operation = ::MakeIntrusive<TOperation>(GetParentClientImpl());
    auto prepareOperation = [
        this_=::TIntrusivePtr(this),
        operation,
        spec,
        mapper,
        reduceCombiner,
        reducer,
        options
    ] () {
        ExecuteRawMapReduce(
            operation,
            ::MakeIntrusive<TOperationPreparer>(this_->GetParentClientImpl(), this_->TransactionId_),
            spec,
            mapper,
            reduceCombiner,
            reducer,
            options);
    };
    return ProcessOperation(GetParentClientImpl(), std::move(prepareOperation), std::move(operation), options);
}

IOperationPtr TClientBase::Sort(
    const TSortOperationSpec& spec,
    const TOperationOptions& options)
{
    auto operation = ::MakeIntrusive<TOperation>(GetParentClientImpl());
    auto prepareOperation = [
        this_ = ::TIntrusivePtr(this),
        operation,
        spec,
        options
    ] () {
        ExecuteSort(
            operation,
            ::MakeIntrusive<TOperationPreparer>(this_->GetParentClientImpl(), this_->TransactionId_),
            spec,
            options);
    };
    return ProcessOperation(GetParentClientImpl(), std::move(prepareOperation), std::move(operation), options);
}

IOperationPtr TClientBase::Merge(
    const TMergeOperationSpec& spec,
    const TOperationOptions& options)
{
    auto operation = ::MakeIntrusive<TOperation>(GetParentClientImpl());
    auto prepareOperation = [
        this_ = ::TIntrusivePtr(this),
        operation,
        spec,
        options
    ] () {
        ExecuteMerge(
            operation,
            ::MakeIntrusive<TOperationPreparer>(this_->GetParentClientImpl(), this_->TransactionId_),
            spec,
            options);
    };
    return ProcessOperation(GetParentClientImpl(), std::move(prepareOperation), std::move(operation), options);
}

IOperationPtr TClientBase::Erase(
    const TEraseOperationSpec& spec,
    const TOperationOptions& options)
{
    auto operation = ::MakeIntrusive<TOperation>(GetParentClientImpl());
    auto prepareOperation = [
        this_ = ::TIntrusivePtr(this),
        operation,
        spec,
        options
    ] () {
        ExecuteErase(
            operation,
            ::MakeIntrusive<TOperationPreparer>(this_->GetParentClientImpl(), this_->TransactionId_),
            spec,
            options);
    };
    return ProcessOperation(GetParentClientImpl(), std::move(prepareOperation), std::move(operation), options);
}

IOperationPtr TClientBase::RemoteCopy(
    const TRemoteCopyOperationSpec& spec,
    const TOperationOptions& options)
{
    auto operation = ::MakeIntrusive<TOperation>(GetParentClientImpl());
    auto prepareOperation = [
        this_ = ::TIntrusivePtr(this),
        operation,
        spec,
        options
    ] () {
        ExecuteRemoteCopy(
            operation,
            ::MakeIntrusive<TOperationPreparer>(this_->GetParentClientImpl(), this_->TransactionId_),
            spec,
            options);
    };
    return ProcessOperation(GetParentClientImpl(), std::move(prepareOperation), std::move(operation), options);
}

IOperationPtr TClientBase::RunVanilla(
    const TVanillaOperationSpec& spec,
    const TOperationOptions& options)
{
    auto operation = ::MakeIntrusive<TOperation>(GetParentClientImpl());
    auto prepareOperation = [
        this_ = ::TIntrusivePtr(this),
        operation,
        spec,
        options
    ] () {
        ExecuteVanilla(
            operation,
            ::MakeIntrusive<TOperationPreparer>(this_->GetParentClientImpl(), this_->TransactionId_),
            spec,
            options);
    };
    return ProcessOperation(GetParentClientImpl(), std::move(prepareOperation), std::move(operation), options);
}

IOperationPtr TClientBase::AttachOperation(const TOperationId& operationId)
{
    auto operation = ::MakeIntrusive<TOperation>(operationId, GetParentClientImpl());
    operation->GetBriefState(); // check that operation exists
    return operation;
}

EOperationBriefState TClientBase::CheckOperation(const TOperationId& operationId)
{
    return NYT::NDetail::CheckOperation(RawClient_, ClientRetryPolicy_, operationId);
}

void TClientBase::AbortOperation(const TOperationId& operationId)
{
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &operationId] (TMutationId& mutationId) {
            RawClient_->AbortOperation(mutationId, operationId);
        });
}

void TClientBase::CompleteOperation(const TOperationId& operationId)
{
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &operationId] (TMutationId& mutationId) {
            RawClient_->CompleteOperation(mutationId, operationId);
        });
}

void TClientBase::WaitForOperation(const TOperationId& operationId)
{
    NYT::NDetail::WaitForOperation(ClientRetryPolicy_, RawClient_, Context_, operationId);
}

void TClientBase::AlterTable(
    const TYPath& path,
    const TAlterTableOptions& options)
{
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &options] (TMutationId& mutationId) {
            RawClient_->AlterTable(mutationId, TransactionId_, path, options);
        });
}

::TIntrusivePtr<TClientReader> TClientBase::CreateClientReader(
    const TRichYPath& path,
    const TFormat& format,
    const TTableReaderOptions& options,
    bool useFormatFromTableAttributes)
{
    return ::MakeIntrusive<TClientReader>(
        CanonizeYPath(path),
        RawClient_,
        ClientRetryPolicy_,
        GetTransactionPinger(),
        Context_,
        TransactionId_,
        format,
        options,
        useFormatFromTableAttributes);
}

THolder<TClientWriter> TClientBase::CreateClientWriter(
    const TRichYPath& path,
    const TFormat& format,
    const TTableWriterOptions& options)
{
    auto realPath = CanonizeYPath(path);

    auto exists = RequestWithRetry<bool>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &realPath] (TMutationId /*mutationId*/) {
            return RawClient_->Exists(TransactionId_, realPath.Path_);
        });
    if (!exists) {
        RequestWithRetry<void>(
            ClientRetryPolicy_->CreatePolicyForGenericRequest(),
            [this, &realPath] (TMutationId& mutataionId) {
                RawClient_->Create(mutataionId, TransactionId_, realPath.Path_, NT_TABLE, TCreateOptions().IgnoreExisting(true));
            });
    }

    return MakeHolder<TClientWriter>(
        realPath,
        RawClient_,
        ClientRetryPolicy_,
        GetTransactionPinger(),
        Context_,
        TransactionId_,
        format,
        options
    );
}

::TIntrusivePtr<INodeReaderImpl> TClientBase::CreateNodeReader(
    const TRichYPath& path, const TTableReaderOptions& options)
{
    auto format = TFormat::YsonBinary();
    ApplyFormatHints<TNode>(&format, options.FormatHints_);

    // Skiff is disabled here because of large header problem (see https://st.yandex-team.ru/YT-6926).
    // Revert this code to r3614168 when it is fixed.
    return new TNodeTableReader(
        CreateClientReader(path, format, options));
}

::TIntrusivePtr<IYaMRReaderImpl> TClientBase::CreateYaMRReader(
    const TRichYPath& path, const TTableReaderOptions& options)
{
    return new TYaMRTableReader(
        CreateClientReader(path, TFormat::YaMRLenval(), options, /* useFormatFromTableAttributes = */ true));
}

::TIntrusivePtr<IProtoReaderImpl> TClientBase::CreateProtoReader(
    const TRichYPath& path,
    const TTableReaderOptions& options,
    const Message* prototype)
{
    TVector<const ::google::protobuf::Descriptor*> descriptors;
    descriptors.push_back(prototype->GetDescriptor());

    if (Context_.Config->UseClientProtobuf) {
        return new TProtoTableReader(
            CreateClientReader(path, TFormat::YsonBinary(), options),
            std::move(descriptors));
    } else {
        auto format = TFormat::Protobuf({prototype->GetDescriptor()}, Context_.Config->ProtobufFormatWithDescriptors);
        return new TLenvalProtoTableReader(
            CreateClientReader(path, format, options),
            std::move(descriptors));
    }
}

::TIntrusivePtr<ISkiffRowReaderImpl> TClientBase::CreateSkiffRowReader(
    const TRichYPath& path,
    const TTableReaderOptions& options,
    const ISkiffRowSkipperPtr& skipper,
    const NSkiff::TSkiffSchemaPtr& requestedSchema,
    const NSkiff::TSkiffSchemaPtr& parserSchema)
{
    auto skiffOptions = TCreateSkiffSchemaOptions().HasRangeIndex(true);
    auto resultRequestedSchema = NYT::NDetail::CreateSkiffSchema(TVector{requestedSchema}, skiffOptions);
    auto resultParserSchema = NYT::NDetail::CreateSkiffSchema(TVector{parserSchema}, skiffOptions);
    return new TSkiffRowTableReader(
        CreateClientReader(path, NYT::NDetail::CreateSkiffFormat(resultRequestedSchema), options),
        resultParserSchema,
        {skipper},
        std::move(skiffOptions));
}

::TIntrusivePtr<INodeReaderImpl> TClientBase::CreateNodeTablePartitionReader(
    const TString& cookie,
    const TTablePartitionReaderOptions& options)
{
    auto format = TFormat::YsonBinary();
    ApplyFormatHints<TNode>(&format, options.FormatHints_);

    return MakeIntrusive<TNodeTableReader>(CreateRawTablePartitionReader(cookie, format, options));
}

::TIntrusivePtr<IProtoReaderImpl> TClientBase::CreateProtoTablePartitionReader(
    const TString& cookie,
    const TTablePartitionReaderOptions& options,
    const Message* prototype)
{
    auto descriptors = TVector<const ::google::protobuf::Descriptor*>{
        prototype->GetDescriptor(),
    };
    auto format = TFormat::Protobuf(descriptors, Context_.Config->ProtobufFormatWithDescriptors);
    return MakeIntrusive<TLenvalProtoTableReader>(
        CreateRawTablePartitionReader(cookie, format, options),
        std::move(descriptors));
}

::TIntrusivePtr<ISkiffRowReaderImpl> TClientBase::CreateSkiffRowTablePartitionReader(
    const TString& cookie,
    const TTablePartitionReaderOptions& options,
    const ISkiffRowSkipperPtr& skipper,
    const NSkiff::TSkiffSchemaPtr& requestedSchema,
    const NSkiff::TSkiffSchemaPtr& parserSchema)
{
    auto skiffOptions = TCreateSkiffSchemaOptions().HasRangeIndex(true);
    auto resultRequestedSchema = NYT::NDetail::CreateSkiffSchema(TVector{requestedSchema}, skiffOptions);
    auto resultParserSchema = NYT::NDetail::CreateSkiffSchema(TVector{parserSchema}, skiffOptions);

    return new TSkiffRowTableReader(
        CreateRawTablePartitionReader(cookie, NYT::NDetail::CreateSkiffFormat(resultRequestedSchema), options),
        resultParserSchema,
        {skipper},
        std::move(skiffOptions));
}

::TIntrusivePtr<INodeWriterImpl> TClientBase::CreateNodeWriter(
    const TRichYPath& path, const TTableWriterOptions& options)
{
    auto format = TFormat::YsonBinary();
    ApplyFormatHints<TNode>(&format, options.FormatHints_);

    return new TNodeTableWriter(
        CreateClientWriter(path, format, options));
}

::TIntrusivePtr<IYaMRWriterImpl> TClientBase::CreateYaMRWriter(
    const TRichYPath& path, const TTableWriterOptions& options)
{
    auto format = TFormat::YaMRLenval();
    ApplyFormatHints<TYaMRRow>(&format, options.FormatHints_);

    return new TYaMRTableWriter(
        CreateClientWriter(path, format, options));
}

::TIntrusivePtr<IProtoWriterImpl> TClientBase::CreateProtoWriter(
    const TRichYPath& path,
    const TTableWriterOptions& options,
    const Message* prototype)
{
    TVector<const ::google::protobuf::Descriptor*> descriptors;
    descriptors.push_back(prototype->GetDescriptor());

    auto pathWithSchema = path;
    if (options.InferSchema_.GetOrElse(Context_.Config->InferTableSchema) && !path.Schema_) {
        pathWithSchema.Schema(CreateTableSchema(*prototype->GetDescriptor()));
    }

    if (Context_.Config->UseClientProtobuf) {
        auto format = TFormat::YsonBinary();
        ApplyFormatHints<TNode>(&format, options.FormatHints_);
        return new TProtoTableWriter(
            CreateClientWriter(pathWithSchema, format, options),
            std::move(descriptors));
    } else {
        auto format = TFormat::Protobuf({prototype->GetDescriptor()}, Context_.Config->ProtobufFormatWithDescriptors);
        ApplyFormatHints<::google::protobuf::Message>(&format, options.FormatHints_);
        return new TLenvalProtoTableWriter(
            CreateClientWriter(pathWithSchema, format, options),
            std::move(descriptors));
    }
}

::TIntrusivePtr<ITableFragmentWriter<TNode>> TClientBase::CreateNodeFragmentWriter(
    const TDistributedWriteTableCookie& cookie,
    const TTableFragmentWriterOptions& options)
{
    auto format = TFormat::YsonBinary();

    // TODO(achains): Make proper wrapper class with retries and auto ping.
    auto stream = NDetail::RequestWithRetry<std::unique_ptr<IOutputStreamWithResponse>>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [&] (TMutationId /*mutationId*/) {
            return RawClient_->WriteTableFragment(cookie, format, options);
        }
    );

    return ::MakeIntrusive<TNodeTableFragmentWriter>(std::move(stream));
}

::TIntrusivePtr<ITableFragmentWriter<TYaMRRow>> TClientBase::CreateYaMRFragmentWriter(
    const TDistributedWriteTableCookie& cookie,
    const TTableFragmentWriterOptions& options)
{
    auto format = TFormat::YaMRLenval();

    // TODO(achains): Make proper wrapper class with retries and auto ping.
    auto stream = NDetail::RequestWithRetry<std::unique_ptr<IOutputStreamWithResponse>>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [&] (TMutationId /*mutationId*/) {
            return RawClient_->WriteTableFragment(cookie, format, options);
        }
    );

    return ::MakeIntrusive<TYaMRTableFragmentWriter>(std::move(stream));
}

::TIntrusivePtr<ITableFragmentWriter<Message>> TClientBase::CreateProtoFragmentWriter(
    const TDistributedWriteTableCookie& cookie,
    const TTableFragmentWriterOptions& options,
    const Message* prototype)
{
    TVector<const ::google::protobuf::Descriptor*> descriptors;
    descriptors.push_back(prototype->GetDescriptor());

    TFormat format;
    if (Context_.Config->UseClientProtobuf) {
        format = TFormat::YsonBinary();
    } else {
        format = TFormat::Protobuf({prototype->GetDescriptor()}, Context_.Config->ProtobufFormatWithDescriptors);
    }

    // TODO(achains): Make proper wrapper class with retries and auto ping.
    auto stream = NDetail::RequestWithRetry<std::unique_ptr<IOutputStreamWithResponse>>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [&] (TMutationId /*mutationId*/) {
            return RawClient_->WriteTableFragment(cookie, format, options);
        }
    );

    if (Context_.Config->UseClientProtobuf) {
        return ::MakeIntrusive<TProtoTableFragmentWriter>(std::move(stream), std::move(descriptors));
    }

    return ::MakeIntrusive<TLenvalProtoTableFragmentWriter>(std::move(stream), std::move(descriptors));
}

TBatchRequestPtr TClientBase::CreateBatchRequest()
{
    return MakeIntrusive<TBatchRequest>(TransactionId_, GetParentClientImpl());
}

IRawClientPtr TClientBase::GetRawClient() const
{
    return RawClient_;
}

const TClientContext& TClientBase::GetContext() const
{
    return Context_;
}

const IClientRetryPolicyPtr& TClientBase::GetRetryPolicy() const
{
    return ClientRetryPolicy_;
}

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(
    const IRawClientPtr& rawClient,
    TClientPtr parentClient,
    const TClientContext& context,
    const TTransactionId& parentTransactionId,
    const TStartTransactionOptions& options)
    : TClientBase(rawClient, context, parentTransactionId, parentClient->GetRetryPolicy())
    , TransactionPinger_(parentClient->GetTransactionPinger())
    , PingableTx_(
        std::make_unique<TPingableTransaction>(
            rawClient,
            parentClient->GetRetryPolicy(),
            context,
            parentTransactionId,
            TransactionPinger_->GetChildTxPinger(),
            options))
    , ParentClient_(parentClient)
{
    TransactionId_ = PingableTx_->GetId();
}

TTransaction::TTransaction(
    const IRawClientPtr& rawClient,
    TClientPtr parentClient,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TAttachTransactionOptions& options)
    : TClientBase(rawClient, context, transactionId, parentClient->GetRetryPolicy())
    , TransactionPinger_(parentClient->GetTransactionPinger())
    , PingableTx_(
        new TPingableTransaction(
            rawClient,
            parentClient->GetRetryPolicy(),
            context,
            transactionId,
            parentClient->GetTransactionPinger()->GetChildTxPinger(),
            options))
    , ParentClient_(parentClient)
{ }

const TTransactionId& TTransaction::GetId() const
{
    return TransactionId_;
}

ILockPtr TTransaction::Lock(
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    auto lockId = RequestWithRetry<TLockId>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &mode, &options] (TMutationId& mutationId) {
            return RawClient_->Lock(mutationId, TransactionId_, path, mode, options);
        });
    return ::MakeIntrusive<TLock>(lockId, GetParentClientImpl(), options.Waitable_);
}

void TTransaction::Unlock(
    const TYPath& path,
    const TUnlockOptions& options)
{
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &options] (TMutationId& mutationId) {
            RawClient_->Unlock(mutationId, TransactionId_, path, options);
        });
}

void TTransaction::Commit()
{
    PingableTx_->Commit();
}

void TTransaction::Abort()
{
    PingableTx_->Abort();
}

void TTransaction::Ping()
{
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this] (TMutationId /*mutationId*/) {
            RawClient_->PingTransaction(TransactionId_);
        });
}

void TTransaction::Detach()
{
    PingableTx_->Detach();
}

ITransactionPingerPtr TTransaction::GetTransactionPinger()
{
    return TransactionPinger_;
}

IClientPtr TTransaction::GetParentClient(bool ignoreGlobalTx)
{
    return GetParentClientImpl()->GetParentClient(ignoreGlobalTx);
}

TClientPtr TTransaction::GetParentClientImpl()
{
    return ParentClient_;
}

////////////////////////////////////////////////////////////////////////////////

TClient::TClient(
    IRawClientPtr rawClient,
    const TClientContext& context,
    const TTransactionId& globalId,
    IClientRetryPolicyPtr retryPolicy)
    : TClientBase(std::move(rawClient), context, globalId, retryPolicy)
    , TransactionPinger_(nullptr)
{ }

TClient::~TClient() = default;

ITransactionPtr TClient::AttachTransaction(
    const TTransactionId& transactionId,
    const TAttachTransactionOptions& options)
{
    CheckShutdown();

    return MakeIntrusive<TTransaction>(RawClient_, this, Context_, transactionId, options);
}

void TClient::MountTable(
    const TYPath& path,
    const TMountTableOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &options] (TMutationId& mutationId) {
            RawClient_->MountTable(mutationId, path, options);
        });
}

void TClient::UnmountTable(
    const TYPath& path,
    const TUnmountTableOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &options] (TMutationId& mutationId) {
            RawClient_->UnmountTable(mutationId, path, options);
        });
}

void TClient::RemountTable(
    const TYPath& path,
    const TRemountTableOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &options] (TMutationId& mutationId) {
            RawClient_->RemountTable(mutationId, path, options);
        });
}

void TClient::FreezeTable(
    const TYPath& path,
    const TFreezeTableOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &options] (TMutationId /*mutationId*/) {
            RawClient_->FreezeTable(path, options);
        });
}

void TClient::UnfreezeTable(
    const TYPath& path,
    const TUnfreezeTableOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &options] (TMutationId /*mutationId*/) {
            RawClient_->UnfreezeTable(path, options);
        });
}

void TClient::ReshardTable(
    const TYPath& path,
    const TVector<TKey>& keys,
    const TReshardTableOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &keys, &options] (TMutationId& mutationId) {
            RawClient_->ReshardTableByPivotKeys(mutationId, path, keys, options);
        });
}

void TClient::ReshardTable(
    const TYPath& path,
    i64 tabletCount,
    const TReshardTableOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, tabletCount, &options] (TMutationId& mutationId) {
            RawClient_->ReshardTableByTabletCount(mutationId, path, tabletCount, options);
        });
}

void TClient::InsertRows(
    const TYPath& path,
    const TNode::TListType& rows,
    const TInsertRowsOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &rows, &options] (TMutationId /*mutationId*/) {
            RawClient_->InsertRows(path, rows, options);
        });
}

void TClient::DeleteRows(
    const TYPath& path,
    const TNode::TListType& keys,
    const TDeleteRowsOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &keys, &options] (TMutationId /*mutationId*/) {
            RawClient_->DeleteRows(path, keys, options);
        });
}

void TClient::TrimRows(
    const TYPath& path,
    i64 tabletIndex,
    i64 rowCount,
    const TTrimRowsOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, tabletIndex, rowCount, &options] (TMutationId /*mutationId*/) {
            RawClient_->TrimRows(path, tabletIndex, rowCount, options);
        });
}

TNode::TListType TClient::LookupRows(
    const TYPath& path,
    const TNode::TListType& keys,
    const TLookupRowsOptions& options)
{
    CheckShutdown();
    return RequestWithRetry<TNode::TListType>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &keys, &options] (TMutationId /*mutationId*/) {
            return RawClient_->LookupRows(path, keys, options);
        });
}

TNode::TListType TClient::SelectRows(
    const TString& query,
    const TSelectRowsOptions& options)
{
    CheckShutdown();
    return RequestWithRetry<TNode::TListType>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &query, &options] (TMutationId /*mutationId*/) {
            return RawClient_->SelectRows(query, options);
        });
}

void TClient::AlterTableReplica(const TReplicaId& replicaId, const TAlterTableReplicaOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &replicaId, &options] (TMutationId& mutationId) {
            RawClient_->AlterTableReplica(mutationId, replicaId, options);
        });
}

ui64 TClient::GenerateTimestamp()
{
    CheckShutdown();
    return RequestWithRetry<ui64>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this] (TMutationId /*mutationId*/) {
            return RawClient_->GenerateTimestamp();
        });
}

TAuthorizationInfo TClient::WhoAmI()
{
    CheckShutdown();
    return RequestWithRetry<TAuthorizationInfo>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this] (TMutationId /*mutationId*/) {
            return NRawClient::WhoAmI(Context_);
        });
}

TOperationAttributes TClient::GetOperation(
    const TOperationId& operationId,
    const TGetOperationOptions& options)
{
    CheckShutdown();
    return RequestWithRetry<TOperationAttributes>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &operationId, &options] (TMutationId /*mutationId*/) {
            return RawClient_->GetOperation(operationId, options);
        });
}

TOperationAttributes TClient::GetOperation(
    const TString& alias,
    const TGetOperationOptions& options)
{
    CheckShutdown();
    return RequestWithRetry<TOperationAttributes>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &alias, &options] (TMutationId /*mutationId*/) {
            return RawClient_->GetOperation(alias, options);
        });
}

TListOperationsResult TClient::ListOperations(const TListOperationsOptions& options)
{
    CheckShutdown();
    return RequestWithRetry<TListOperationsResult>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &options] (TMutationId /*mutationId*/) {
            return RawClient_->ListOperations(options);
        });
}

void TClient::UpdateOperationParameters(
    const TOperationId& operationId,
    const TUpdateOperationParametersOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &operationId, &options] (TMutationId /*mutationId*/) {
            RawClient_->UpdateOperationParameters(operationId, options);
        });
}

TJobAttributes TClient::GetJob(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobOptions& options)
{
    CheckShutdown();
    auto result = RequestWithRetry<NYson::TYsonString>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &operationId, &jobId, &options] (TMutationId /*mutationId*/) {
            return RawClient_->GetJob(operationId, jobId, options);
        });
    return NRawClient::ParseJobAttributes(NodeFromYsonString(result.AsStringBuf()));
}

TListJobsResult TClient::ListJobs(
    const TOperationId& operationId,
    const TListJobsOptions& options)
{
    CheckShutdown();
    return RequestWithRetry<TListJobsResult>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &operationId, &options] (TMutationId /*mutationId*/) {
            return RawClient_->ListJobs(operationId, options);
        });
}

IFileReaderPtr TClient::GetJobInput(
    const TJobId& jobId,
    const TGetJobInputOptions& options)
{
    CheckShutdown();
    return RawClient_->GetJobInput(jobId, options);
}

IFileReaderPtr TClient::GetJobFailContext(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobFailContextOptions& options)
{
    CheckShutdown();
    return RawClient_->GetJobFailContext(operationId, jobId, options);
}

IFileReaderPtr TClient::GetJobStderr(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobStderrOptions& options)
{
    CheckShutdown();
    return RawClient_->GetJobStderr(operationId, jobId, options);
}

IFileReaderPtr TClient::GetJobTrace(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobTraceOptions& options)
{
    CheckShutdown();
    return RawClient_->GetJobTrace(operationId, jobId, options);
}

TNode::TListType TClient::SkyShareTable(
    const std::vector<TYPath>& tablePaths,
    const TSkyShareTableOptions& options)
{
    CheckShutdown();

    // As documented at https://wiki.yandex-team.ru/yt/userdoc/blob_tables/#shag3.sozdajomrazdachu
    // first request returns HTTP status code 202 (Accepted). And we need retrying until we have 200 (OK).
    NHttpClient::IHttpResponsePtr response;
    do {
        response = RequestWithRetry<NHttpClient::IHttpResponsePtr>(
            ClientRetryPolicy_->CreatePolicyForGenericRequest(),
            [this, &tablePaths, &options] (TMutationId /*mutationId*/) {
                return NRawClient::SkyShareTable(Context_, tablePaths, options);
            });
        TWaitProxy::Get()->Sleep(TDuration::Seconds(5));
    } while (response->GetStatusCode() != 200);

    if (options.KeyColumns_) {
        return NodeFromJsonString(response->GetResponse())["torrents"].AsList();
    } else {
        TNode torrent;
        torrent["key"] = TNode::CreateList();
        torrent["rbtorrent"] = response->GetResponse();
        return TNode::TListType{torrent};
    }
}

TCheckPermissionResponse TClient::CheckPermission(
    const TString& user,
    EPermission permission,
    const TYPath& path,
    const TCheckPermissionOptions& options)
{
    CheckShutdown();
    return RequestWithRetry<TCheckPermissionResponse>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &user, &permission, &path, &options] (TMutationId /*mutationId*/) {
            return RawClient_->CheckPermission(user, permission, path, options);
        });
}

TVector<TTabletInfo> TClient::GetTabletInfos(
    const TYPath& path,
    const TVector<int>& tabletIndexes,
    const TGetTabletInfosOptions& options)
{
    CheckShutdown();
    return RequestWithRetry<TVector<TTabletInfo>>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &path, &tabletIndexes, &options] (TMutationId /*mutationId*/) {
            return RawClient_->GetTabletInfos(path, tabletIndexes, options);
        });
}

void TClient::SuspendOperation(
    const TOperationId& operationId,
    const TSuspendOperationOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &operationId, &options] (TMutationId& mutationId) {
            RawClient_->SuspendOperation(mutationId, operationId, options);
        });
}

void TClient::ResumeOperation(
    const TOperationId& operationId,
    const TResumeOperationOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &operationId, &options] (TMutationId& mutationId) {
            RawClient_->ResumeOperation(mutationId, operationId, options);
        });
}

void TClient::PingDistributedWriteTableSession(
    const TDistributedWriteTableSession& session,
    const TPingDistributedWriteTableOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &session, &options] (TMutationId& /*mutationId*/) {
            RawClient_->PingDistributedWriteTableSession(session, options);
        });
}

void TClient::FinishDistributedWriteTableSession(
    const TDistributedWriteTableSession& session,
    const TVector<TWriteTableFragmentResult>& results,
    const TFinishDistributedWriteTableOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &session, &results, &options] (TMutationId& mutationId) {
            RawClient_->FinishDistributedWriteTableSession(mutationId, session, results, options);
        });
}

void TClient::PingDistributedWriteFileSession(
    const TDistributedWriteFileSession& session,
    const TPingDistributedWriteFileOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &session, &options] (TMutationId& /*mutationId*/) {
            RawClient_->PingDistributedWriteFileSession(session, options);
        });
}

void TClient::FinishDistributedWriteFileSession(
    const TDistributedWriteFileSession& session,
    const TVector<TWriteFileFragmentResult>& results,
    const TFinishDistributedWriteFileOptions& options)
{
    CheckShutdown();
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &session, &results, &options] (TMutationId& mutationId) {
            RawClient_->FinishDistributedWriteFileSession(mutationId, session, results, options);
        });
}

TYtPoller& TClient::GetYtPoller()
{
    auto g = Guard(Lock_);
    if (!YtPoller_) {
        CheckShutdown();
        // We don't use current client and create new client because YtPoller_ might use
        // this client during current client shutdown.
        // That might lead to incrementing of current client refcount and double delete of current client object.
        YtPoller_ = std::make_unique<TYtPoller>(RawClient_->Clone(), Context_.Config, ClientRetryPolicy_);
    }
    return *YtPoller_;
}

void TClient::Shutdown()
{
    std::unique_ptr<TYtPoller> poller;
    with_lock(Lock_) {
        if (!Shutdown_.exchange(true) && YtPoller_) {
            poller = std::move(YtPoller_);
        }
    }
    if (poller) {
        poller->Stop();
    }
}

ITransactionPingerPtr TClient::GetTransactionPinger()
{
    auto g = Guard(Lock_);
    if (!TransactionPinger_) {
        TransactionPinger_ = CreateTransactionPinger(Context_.Config, Context_.UseTLS);
    }
    return TransactionPinger_;
}

TClientPtr TClient::GetParentClientImpl()
{
    return this;
}

IClientPtr TClient::GetParentClient(bool ignoreGlobalTx)
{
    if (!TransactionId_.IsEmpty() && ignoreGlobalTx) {
        return MakeIntrusive<TClient>(
            RawClient_,
            Context_,
            TTransactionId(),
            ClientRetryPolicy_);
    } else {
        return this;
    }
}

void TClient::CheckShutdown() const
{
    if (Shutdown_) {
        ythrow TApiUsageError() << "Call client's methods after shutdown";
    }
}

const TNode::TMapType& TClient::GetDynamicConfiguration(const TString& configProfile)
{
    auto g = Guard(ClusterConfigLock_);

    if (!ClusterConfig_) {
        TNode clusterConfigNode;

        TYPath clusterConfigPath = Context_.Config->ConfigRemotePatchPath + "/" + configProfile;
        YT_LOG_DEBUG(
            "Fetching cluster config (ConfigPath: %v, ConfigProfile: %v)",
            Context_.Config->ConfigRemotePatchPath,
            configProfile);

        try {
            clusterConfigNode = Get(clusterConfigPath, TGetOptions());
        } catch (const TErrorResponse& error) {
            if (!error.IsResolveError()) {
                throw;
            }

            ClusterConfig_.emplace();
            YT_LOG_WARNING(
                "Could not resolve, saved empty cluster config (ConfigPath: %v, ConfigProfile: %v)",
                Context_.Config->ConfigRemotePatchPath,
                configProfile);
        }

        if (clusterConfigNode.IsMap()) {
            ClusterConfig_ = clusterConfigNode.UncheckedAsMap();
            YT_LOG_DEBUG(
                "Saved cluster config (ConfigPath: %v, ConfigProfile: %v)",
                Context_.Config->ConfigRemotePatchPath,
                configProfile);
        } else if (!ClusterConfig_.has_value()) {
            ClusterConfig_.emplace();
            YT_LOG_WARNING(
                "Config node has incorrect type, saved empty cluster config (NodeType: %v, ConfigPath: %v, ConfigProfile: %v)",
                clusterConfigNode.GetType(),
                Context_.Config->ConfigRemotePatchPath,
                configProfile);
        }
    }

    return *ClusterConfig_;
}

void SetupClusterContext(
    TClientContext& context,
    const TString& serverName)
{
    context.ServerName = serverName;
    context.MultiproxyTargetCluster = serverName;
    ApplyProxyUrlAliasingRules(context.ServerName, context.Config->ProxyUrlAliasingRules);

    if (context.ServerName.find('.') == TString::npos &&
        context.ServerName.find(':') == TString::npos &&
        context.ServerName.find("localhost") == TString::npos)
    {
        context.ServerName += ".yt.yandex.net";
    }

    static constexpr char httpUrlSchema[] = "http://";
    static constexpr char httpsUrlSchema[] = "https://";

    if (!context.UseTLS) {
        context.UseTLS = context.ServerName.StartsWith(httpsUrlSchema);
    }

    if (context.ServerName.StartsWith(httpUrlSchema)) {
        if (context.UseTLS) {
            ythrow TApiUsageError() << "URL schema doesn't match UseTLS option";
        }

        context.ServerName.erase(0, sizeof(httpUrlSchema) - 1);
    }
    if (context.ServerName.StartsWith(httpsUrlSchema)) {
        if (!context.UseTLS) {
            ythrow TApiUsageError() << "URL schema doesn't match UseTLS option";
        }

        context.ServerName.erase(0, sizeof(httpsUrlSchema) - 1);
    }

    if (context.ServerName.find(':') == TString::npos) {
        context.ServerName = CreateHostNameWithPort(context.ServerName, context);
    }
    if (context.TvmOnly) {
        context.ServerName = Format("tvm.%v", context.ServerName);
    }
}

TClientContext CreateClientContext(
    const TString& serverName,
    const TCreateClientOptions& options)
{
    TClientContext context;
    context.Config = options.Config_ ? options.Config_ : TConfig::Get();
    context.TvmOnly = options.TvmOnly_;
    context.ProxyAddress = options.ProxyAddress_;
    context.JobProxySocketPath = options.JobProxySocketPath_;

    if (options.UseTLS_) {
        context.UseTLS = *options.UseTLS_;
    }

    SetupClusterContext(context, serverName);

    if (context.Config->HttpProxyRole && context.Config->Hosts == DefaultHosts) {
        context.Config->Hosts = "hosts?role=" + context.Config->HttpProxyRole;
    }
    if (context.Config->RpcProxyRole) {
        context.RpcProxyRole = context.Config->RpcProxyRole;
    }

    if (context.UseTLS || options.UseCoreHttpClient_) {
        context.HttpClient = NHttpClient::CreateCoreHttpClient(context.UseTLS, context.Config);
    } else {
        context.HttpClient = NHttpClient::CreateDefaultHttpClient();
    }

    context.Token = context.Config->Token;
    if (options.Token_) {
        context.Token = options.Token_;
    } else if (options.TokenPath_) {
        context.Token = TConfig::LoadTokenFromFile(options.TokenPath_);
    } else if (options.ServiceTicketAuth_) {
        context.ServiceTicketAuth = options.ServiceTicketAuth_;
    }

    context.ImpersonationUser = options.ImpersonationUser_;

    if (context.Token) {
        TConfig::ValidateToken(context.Token);
    }

    return context;
}

TClientPtr CreateClientImpl(
    const TString& serverName,
    const TCreateClientOptions& options)
{
    auto context = CreateClientContext(serverName, options);

    auto globalTxId = GetGuid(context.Config->GlobalTxId);

    auto retryConfigProvider = options.RetryConfigProvider_;
    if (!retryConfigProvider) {
        retryConfigProvider = CreateDefaultRetryConfigProvider();
    }

    auto rawClient = MakeIntrusive<THttpRawClient>(context);

    EnsureInitialized();

    return new TClient(
        std::move(rawClient),
        context,
        globalTxId,
        CreateDefaultClientRetryPolicy(retryConfigProvider, context.Config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////


IClientPtr CreateClient(
    const TString& serverName,
    const TCreateClientOptions& options)
{
    return NDetail::CreateClientImpl(serverName, options);
}

IClientPtr CreateClientFromEnv(const TCreateClientOptions& options)
{
    auto serverName = GetEnv("YT_PROXY");
    if (!serverName) {
        ythrow yexception() << "YT_PROXY is not set";
    }
    return CreateClient(serverName, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
