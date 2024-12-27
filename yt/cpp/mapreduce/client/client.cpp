#include "client.h"

#include "batch_request_impl.h"
#include "client_reader.h"
#include "client_writer.h"
#include "file_reader.h"
#include "file_writer.h"
#include "format_hints.h"
#include "init.h"
#include "lock.h"
#include "operation.h"
#include "retryful_writer.h"
#include "transaction.h"
#include "transaction_pinger.h"
#include "yt_poller.h"

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

#include <yt/cpp/mapreduce/raw_client/raw_client.h>
#include <yt/cpp/mapreduce/raw_client/raw_requests.h>
#include <yt/cpp/mapreduce/raw_client/rpc_parameters_serialization.h>

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

THashMap<TString, TString> ParseProxyUrlAliasingRules(TString envConfig)
{
    if (envConfig.empty()) {
        return {};
    }
    return NYTree::ConvertTo<THashMap<TString, TString>>(NYson::TYsonString(envConfig));
}

void ApplyProxyUrlAliasingRules(TString& url)
{
    static auto rules = ParseProxyUrlAliasingRules(GetEnv("YT_PROXY_URL_ALIASING_CONFIG"));
    if (auto ruleIt = rules.find(url); ruleIt != rules.end()) {
        url = ruleIt->second;
    }
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
    try {
        return RequestWithRetry<TNodeId>(
            ClientRetryPolicy_->CreatePolicyForGenericRequest(),
            [this, &sourcePath, &destinationPath, &options] (TMutationId& mutationId) {
                return RawClient_->CopyInsideMasterCell(mutationId, TransactionId_, sourcePath, destinationPath, options);
            });
    } catch (const TErrorResponse& e) {
        if (e.GetError().ContainsErrorCode(NClusterErrorCodes::NObjectClient::CrossCellAdditionalPath)) {
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
    RequestWithRetry<void>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &sourcePaths, &destinationPath, &options] (TMutationId /*mutationId*/) {
            auto transaction = StartTransaction(TStartTransactionOptions());

            if (!options.Append_ && !sourcePaths.empty() && !transaction->Exists(destinationPath.Path_)) {
                auto typeNode = transaction->Get(CanonizeYPath(sourcePaths.front()).Path_ + "/@type");
                auto type = FromString<ENodeType>(typeNode.AsString());
                transaction->Create(destinationPath.Path_, type, TCreateOptions().IgnoreExisting(true));
            }

            RawClient_->Concatenate(transaction->GetId(), sourcePaths, destinationPath, options);

            transaction->Commit();
        });
}

TRichYPath TClientBase::CanonizeYPath(const TRichYPath& path)
{
    return NRawClient::CanonizeYPath(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Context_, path);
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
        GetWriteTableCommand(Context_.Config->ApiVersion),
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
    const NSkiff::TSkiffSchemaPtr& schema)
{
    auto skiffOptions = TCreateSkiffSchemaOptions().HasRangeIndex(true);
    auto resultSchema = NYT::NDetail::CreateSkiffSchema(TVector{schema}, skiffOptions);
    return new TSkiffRowTableReader(
        CreateClientReader(path, NYT::NDetail::CreateSkiffFormat(resultSchema), options),
        resultSchema,
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

TBatchRequestPtr TClientBase::CreateBatchRequest()
{
    return MakeIntrusive<TBatchRequest>(TransactionId_, GetParentClientImpl());
}

IClientPtr TClientBase::GetParentClient()
{
    return GetParentClientImpl();
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
            return RawClient_->WhoAmI();
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

std::vector<TJobTraceEvent> TClient::GetJobTrace(
    const TOperationId& operationId,
    const TGetJobTraceOptions& options)
{
    CheckShutdown();
    return RequestWithRetry<std::vector<TJobTraceEvent>>(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        [this, &operationId, &options] (TMutationId /*mutationId*/) {
            return RawClient_->GetJobTrace(operationId, options);
        });
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
                return RawClient_->SkyShareTable(tablePaths, options);
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

TYtPoller& TClient::GetYtPoller()
{
    auto g = Guard(Lock_);
    if (!YtPoller_) {
        CheckShutdown();
        // We don't use current client and create new client because YtPoller_ might use
        // this client during current client shutdown.
        // That might lead to incrementing of current client refcount and double delete of current client object.
        YtPoller_ = std::make_unique<TYtPoller>(Context_, ClientRetryPolicy_);
    }
    return *YtPoller_;
}

void TClient::Shutdown()
{
    auto g = Guard(Lock_);

    if (!Shutdown_.exchange(true) && YtPoller_) {
        YtPoller_->Stop();
    }
}

ITransactionPingerPtr TClient::GetTransactionPinger()
{
    auto g = Guard(Lock_);
    if (!TransactionPinger_) {
        TransactionPinger_ = CreateTransactionPinger(Context_.Config);
    }
    return TransactionPinger_;
}

TClientPtr TClient::GetParentClientImpl()
{
    return this;
}

void TClient::CheckShutdown() const
{
    if (Shutdown_) {
        ythrow TApiUsageError() << "Call client's methods after shutdown";
    }
}

TClientPtr CreateClientImpl(
    const TString& serverName,
    const TCreateClientOptions& options)
{
    TClientContext context;
    context.Config = options.Config_ ? options.Config_ : TConfig::Get();
    context.TvmOnly = options.TvmOnly_;
    context.ProxyAddress = options.ProxyAddress_;

    context.ServerName = serverName;
    ApplyProxyUrlAliasingRules(context.ServerName);

    if (context.ServerName.find('.') == TString::npos &&
        context.ServerName.find(':') == TString::npos &&
        context.ServerName.find("localhost") == TString::npos)
    {
        context.ServerName += ".yt.yandex.net";
    }

    static constexpr char httpUrlSchema[] = "http://";
    static constexpr char httpsUrlSchema[] = "https://";
    if (options.UseTLS_) {
        context.UseTLS = *options.UseTLS_;
    } else {
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
    if (options.TvmOnly_) {
        context.ServerName = Format("tvm.%v", context.ServerName);
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
