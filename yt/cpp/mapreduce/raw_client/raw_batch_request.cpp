#include "raw_batch_request.h"

#include "raw_requests.h"
#include "rpc_parameters_serialization.h"

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

#include <library/cpp/yson/node/node.h>

#include <yt/cpp/mapreduce/http/context.h>
#include <yt/cpp/mapreduce/http/retry_request.h>

#include <util/generic/guid.h>
#include <util/string/builder.h>

#include <exception>

namespace NYT::NDetail::NRawClient {

using NThreading::TFuture;
using NThreading::TPromise;
using NThreading::NewPromise;

////////////////////////////////////////////////////////////////////

static TString RequestInfo(const TNode& request)
{
    return ::TStringBuilder()
        << request["command"].AsString() << ' ' << NodeToYsonString(request["parameters"]);
}

static void EnsureNothing(const TMaybe<TNode>& node)
{
    Y_ENSURE(!node, "Internal error: expected to have no response, but got response of type " << node->GetType());
}

static void EnsureSomething(const TMaybe<TNode>& node)
{
    Y_ENSURE(node, "Internal error: expected to have response of any type, but got no response.");
}

static void EnsureType(const TNode& node, TNode::EType type)
{
    Y_ENSURE(node.GetType() == type, "Internal error: unexpected response type. "
        << "Expected: " << type << ", actual: " << node.GetType());
}

static void EnsureType(const TMaybe<TNode>& node, TNode::EType type)
{
    Y_ENSURE(node, "Internal error: expected to have response of type " << type << ", but got no response.");
    EnsureType(*node, type);
}

////////////////////////////////////////////////////////////////////

template <typename TReturnType>
class TResponseParserBase
    : public TRawBatchRequest::IResponseItemParser
{
public:
    using TFutureResult = TFuture<TReturnType>;

public:
    TResponseParserBase()
        : Result_(NewPromise<TReturnType>())
    { }

    void SetException(std::exception_ptr e) override
    {
        Result_.SetException(std::move(e));
    }

    TFuture<TReturnType> GetFuture()
    {
        return Result_.GetFuture();
    }

protected:
    TPromise<TReturnType> Result_;
};

////////////////////////////////////////////////////////////////////


class TGetResponseParser
    : public TResponseParserBase<TNode>
{
public:
    void SetResponse(TMaybe<TNode> node) override
    {
        EnsureSomething(node);
        Result_.SetValue(std::move(*node));
    }
};

////////////////////////////////////////////////////////////////////

class TVoidResponseParser
    : public TResponseParserBase<void>
{
public:
    void SetResponse(TMaybe<TNode> node) override
    {
        EnsureNothing(node);
        Result_.SetValue();
    }
};

////////////////////////////////////////////////////////////////////

class TListResponseParser
    : public TResponseParserBase<TNode::TListType>
{
public:
    void SetResponse(TMaybe<TNode> node) override
    {
        EnsureType(node, TNode::List);
        Result_.SetValue(std::move(node->AsList()));
    }
};

////////////////////////////////////////////////////////////////////

class TExistsResponseParser
    : public TResponseParserBase<bool>
{
public:
    void SetResponse(TMaybe<TNode> node) override
    {
        EnsureType(node, TNode::Bool);
        Result_.SetValue(std::move(node->AsBool()));
    }
};

////////////////////////////////////////////////////////////////////

class TGuidResponseParser
    : public TResponseParserBase<TGUID>
{
public:
    void SetResponse(TMaybe<TNode> node) override
    {
        EnsureType(node, TNode::String);
        Result_.SetValue(GetGuid(node->AsString()));
    }
};

////////////////////////////////////////////////////////////////////

class TCanonizeYPathResponseParser
    : public TResponseParserBase<TRichYPath>
{
public:
    explicit TCanonizeYPathResponseParser(TString pathPrefix, const TRichYPath& original)
        : OriginalNode_(PathToNode(original))
        , PathPrefix_(std::move(pathPrefix))
    { }

    void SetResponse(TMaybe<TNode> node) override
    {
        EnsureType(node, TNode::String);

        for (const auto& item : OriginalNode_.GetAttributes().AsMap()) {
            node->Attributes()[item.first] = item.second;
        }
        TRichYPath result;
        Deserialize(result, *node);
        result.Path_ = AddPathPrefix(result.Path_, PathPrefix_);
        Result_.SetValue(result);
    }

private:
    TNode OriginalNode_;
    TString PathPrefix_;
};

////////////////////////////////////////////////////////////////////

class TGetOperationResponseParser
    : public TResponseParserBase<TOperationAttributes>
{
public:
    void SetResponse(TMaybe<TNode> node) override
    {
        EnsureType(node, TNode::Map);
        Result_.SetValue(ParseOperationAttributes(*node));
    }
};

////////////////////////////////////////////////////////////////////

class TTableColumnarStatisticsParser
    : public TResponseParserBase<TVector<TTableColumnarStatistics>>
{
public:
    void SetResponse(TMaybe<TNode> node) override
    {
        EnsureType(node, TNode::List);
        TVector<TTableColumnarStatistics> statistics;
        Deserialize(statistics, *node);
        Result_.SetValue(std::move(statistics));
    }
};

////////////////////////////////////////////////////////////////////

class TTablePartitionsParser
    : public TResponseParserBase<TMultiTablePartitions>
{
public:
    void SetResponse(TMaybe<TNode> node) override
    {
        EnsureType(node, TNode::Map);
        TMultiTablePartitions partitions;
        Deserialize(partitions, *node);
        Result_.SetValue(std::move(partitions));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGetFileFromCacheParser
    : public TResponseParserBase<TMaybe<TYPath>>
{
public:
    void SetResponse(TMaybe<TNode> node) override
    {
        EnsureType(node, TNode::String);
        if (node->AsString().empty()) {
            Result_.SetValue(Nothing());
        } else {
            Result_.SetValue(node->AsString());
        }
    }
};

////////////////////////////////////////////////////////////////////

class TYPathParser
    : public TResponseParserBase<TYPath>
{
public:
    void SetResponse(TMaybe<TNode> node) override
    {
        EnsureType(node, TNode::String);
        Result_.SetValue(node->AsString());
    }
};

////////////////////////////////////////////////////////////////////

class TCheckPermissionParser
    : public TResponseParserBase<TCheckPermissionResponse>
{
public:
    void SetResponse(TMaybe<TNode> node) override
    {
        EnsureType(node, TNode::Map);
        Result_.SetValue(ParseCheckPermissionResponse(*node));
    }
};

////////////////////////////////////////////////////////////////////

TRawBatchRequest::TBatchItem::TBatchItem(TNode parameters, ::TIntrusivePtr<IResponseItemParser> responseParser)
    : Parameters(std::move(parameters))
    , ResponseParser(std::move(responseParser))
    , NextTry()
{ }

TRawBatchRequest::TBatchItem::TBatchItem(const TBatchItem& batchItem, TInstant nextTry)
    : Parameters(batchItem.Parameters)
    , ResponseParser(batchItem.ResponseParser)
    , NextTry(nextTry)
{ }

////////////////////////////////////////////////////////////////////

TRawBatchRequest::TRawBatchRequest(const TConfigPtr& config)
    : Config_(config)
{ }

TRawBatchRequest::~TRawBatchRequest() = default;

bool TRawBatchRequest::IsExecuted() const
{
    return Executed_;
}

void TRawBatchRequest::MarkExecuted()
{
    Executed_ = true;
}

template <typename TResponseParser>
typename TResponseParser::TFutureResult TRawBatchRequest::AddRequest(
    const TString& command,
    TNode parameters,
    TMaybe<TNode> input)
{
    return AddRequest(command, parameters, input, MakeIntrusive<TResponseParser>());
}

template <typename TResponseParser>
typename TResponseParser::TFutureResult TRawBatchRequest::AddRequest(
    const TString& command,
    TNode parameters,
    TMaybe<TNode> input,
    ::TIntrusivePtr<TResponseParser> parser)
{
    Y_ENSURE(!Executed_, "Cannot add request: batch request is already executed");
    TNode request;
    request["command"] = command;
    request["parameters"] = std::move(parameters);
    if (input) {
        request["input"] = std::move(*input);
    }
    BatchItemList_.emplace_back(std::move(request), parser);
    return parser->GetFuture();
}

void TRawBatchRequest::AddRequest(TBatchItem batchItem)
{
    Y_ENSURE(!Executed_, "Cannot add request: batch request is already executed");
    BatchItemList_.push_back(batchItem);
}

TFuture<TNodeId> TRawBatchRequest::Create(
    const TTransactionId& transaction,
    const TYPath& path,
    ENodeType type,
    const TCreateOptions& options)
{
    return AddRequest<TGuidResponseParser>(
        "create",
        SerializeParamsForCreate(transaction, Config_->Prefix, path, type, options),
        Nothing());
}

TFuture<void> TRawBatchRequest::Remove(
    const TTransactionId& transaction,
    const TYPath& path,
    const TRemoveOptions& options)
{
    return AddRequest<TVoidResponseParser>(
        "remove",
        SerializeParamsForRemove(transaction, Config_->Prefix, path, options),
        Nothing());
}

TFuture<bool> TRawBatchRequest::Exists(
    const TTransactionId& transaction,
    const TYPath& path,
    const TExistsOptions& options)
{
    return AddRequest<TExistsResponseParser>(
        "exists",
        SerializeParamsForExists(transaction, Config_->Prefix, path, options),
        Nothing());
}

TFuture<TNode> TRawBatchRequest::Get(
    const TTransactionId& transaction,
    const TYPath& path,
    const TGetOptions& options)
{
    return AddRequest<TGetResponseParser>(
        "get",
        SerializeParamsForGet(transaction, Config_->Prefix, path, options),
        Nothing());
}

TFuture<void> TRawBatchRequest::Set(
    const TTransactionId& transaction,
    const TYPath& path,
    const TNode& node,
    const TSetOptions& options)
{
    return AddRequest<TVoidResponseParser>(
        "set",
        SerializeParamsForSet(transaction, Config_->Prefix, path, options),
        node);
}

TFuture<TNode::TListType> TRawBatchRequest::List(
    const TTransactionId& transaction,
    const TYPath& path,
    const TListOptions& options)
{
    return AddRequest<TListResponseParser>(
        "list",
        SerializeParamsForList(transaction, Config_->Prefix, path, options),
        Nothing());
}

TFuture<TNodeId> TRawBatchRequest::Copy(
    const TTransactionId& transaction,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    return AddRequest<TGuidResponseParser>(
        "copy",
        SerializeParamsForCopy(transaction, Config_->Prefix, sourcePath, destinationPath, options),
        Nothing());
}

TFuture<TNodeId> TRawBatchRequest::Move(
    const TTransactionId& transaction,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    return AddRequest<TGuidResponseParser>(
        "move",
        SerializeParamsForMove(transaction, Config_->Prefix, sourcePath, destinationPath, options),
        Nothing());
}

TFuture<TNodeId> TRawBatchRequest::Link(
    const TTransactionId& transaction,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
{
    return AddRequest<TGuidResponseParser>(
        "link",
        SerializeParamsForLink(transaction, Config_->Prefix, targetPath, linkPath, options),
        Nothing());
}

TFuture<TLockId> TRawBatchRequest::Lock(
    const TTransactionId& transaction,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    return AddRequest<TGuidResponseParser>(
        "lock",
        SerializeParamsForLock(transaction, Config_->Prefix, path, mode, options),
        Nothing());
}

TFuture<void> TRawBatchRequest::Unlock(
    const TTransactionId& transaction,
    const TYPath& path,
    const TUnlockOptions& options)
{
    return AddRequest<TVoidResponseParser>(
        "unlock",
        SerializeParamsForUnlock(transaction, Config_->Prefix, path, options),
        Nothing());
}

TFuture<TMaybe<TYPath>> TRawBatchRequest::GetFileFromCache(
    const TTransactionId& transactionId,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions& options)
{
    return AddRequest<TGetFileFromCacheParser>(
        "get_file_from_cache",
        SerializeParamsForGetFileFromCache(transactionId, md5Signature, cachePath, options),
        Nothing());
}

TFuture<TYPath> TRawBatchRequest::PutFileToCache(
    const TTransactionId& transactionId,
    const TYPath& filePath,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options)
{
    return AddRequest<TYPathParser>(
        "put_file_to_cache",
        SerializeParamsForPutFileToCache(transactionId, Config_->Prefix, filePath, md5Signature, cachePath, options),
        Nothing());
}

TFuture<TCheckPermissionResponse> TRawBatchRequest::CheckPermission(
    const TString& user,
    EPermission permission,
    const TYPath& path,
    const TCheckPermissionOptions& options)
{
    return AddRequest<TCheckPermissionParser>(
        "check_permission",
        SerializeParamsForCheckPermission(user, permission, Config_->Prefix, path, options),
        Nothing());
}

TFuture<TOperationAttributes> TRawBatchRequest::GetOperation(
    const TOperationId& operationId,
    const TGetOperationOptions& options)
{
    return AddRequest<TGetOperationResponseParser>(
        "get_operation",
        SerializeParamsForGetOperation(operationId, options),
        Nothing());
}

TFuture<void> TRawBatchRequest::AbortOperation(const TOperationId& operationId)
{
    return AddRequest<TVoidResponseParser>(
        "abort_op",
        SerializeParamsForAbortOperation(operationId),
        Nothing());
}

TFuture<void> TRawBatchRequest::CompleteOperation(const TOperationId& operationId)
{
    return AddRequest<TVoidResponseParser>(
        "complete_op",
        SerializeParamsForCompleteOperation(operationId),
        Nothing());
}
TFuture<void> TRawBatchRequest::SuspendOperation(
    const TOperationId& operationId,
    const TSuspendOperationOptions& options)
{
    return AddRequest<TVoidResponseParser>(
        "suspend_operation",
        SerializeParamsForSuspendOperation(operationId, options),
        Nothing());
}
TFuture<void> TRawBatchRequest::ResumeOperation(
    const TOperationId& operationId,
    const TResumeOperationOptions& options)
{
    return AddRequest<TVoidResponseParser>(
        "resume_operation",
        SerializeParamsForResumeOperation(operationId, options),
        Nothing());
}

TFuture<void> TRawBatchRequest::UpdateOperationParameters(
    const TOperationId& operationId,
    const TUpdateOperationParametersOptions& options)
{
    return AddRequest<TVoidResponseParser>(
        "update_op_parameters",
        SerializeParamsForUpdateOperationParameters(operationId, options),
        Nothing());
}

TFuture<TRichYPath> TRawBatchRequest::CanonizeYPath(const TRichYPath& path)
{
    TRichYPath result = path;
    // Out of the symbols in the canonization branch below, only '<' can appear in the beggining of a valid rich YPath.
    if (!result.Path_.StartsWith("<")) {
        result.Path_ = AddPathPrefix(result.Path_, Config_->Prefix);
    }

    if (result.Path_.find_first_of("<>{}[]") != TString::npos) {
        return AddRequest<TCanonizeYPathResponseParser>(
            "parse_ypath",
            SerializeParamsForParseYPath(result),
            Nothing(),
            MakeIntrusive<TCanonizeYPathResponseParser>(Config_->Prefix, result));
    }
    return NThreading::MakeFuture(result);
}

TFuture<TVector<TTableColumnarStatistics>> TRawBatchRequest::GetTableColumnarStatistics(
    const TTransactionId& transaction,
    const TVector<TRichYPath>& paths,
    const TGetTableColumnarStatisticsOptions& options)
{
    return AddRequest<TTableColumnarStatisticsParser>(
        "get_table_columnar_statistics",
        SerializeParamsForGetTableColumnarStatistics(transaction, paths, options),
        Nothing());
}

TFuture<TMultiTablePartitions> TRawBatchRequest::GetTablePartitions(
    const TTransactionId& transaction,
    const TVector<TRichYPath>& paths,
    const TGetTablePartitionsOptions& options)
{
    return AddRequest<TTablePartitionsParser>(
        "partition_tables",
        SerializeParamsForGetTablePartitions(transaction, paths, options),
        Nothing());
}

void TRawBatchRequest::FillParameterList(size_t maxSize, TNode* result, TInstant* nextTry) const
{
    Y_ABORT_UNLESS(result);
    Y_ABORT_UNLESS(nextTry);

    *nextTry = TInstant();
    maxSize = Min(maxSize, BatchItemList_.size());
    *result = TNode::CreateList();
    for (size_t i = 0; i < maxSize; ++i) {
        YT_LOG_DEBUG("ExecuteBatch preparing: %v",
            RequestInfo(BatchItemList_[i].Parameters));

        result->Add(BatchItemList_[i].Parameters);
        if (BatchItemList_[i].NextTry > *nextTry) {
            *nextTry = BatchItemList_[i].NextTry;
        }
    }
}

void TRawBatchRequest::ParseResponse(
    const TResponseInfo& requestResult,
    const IRequestRetryPolicyPtr& retryPolicy,
    TRawBatchRequest* retryBatch,
    TInstant now)
{
    TNode node = NodeFromYsonString(requestResult.Response);
    return ParseResponse(node, requestResult.RequestId, retryPolicy, retryBatch, now);
}

void TRawBatchRequest::ParseResponse(
    TNode node,
    const TString& requestId,
    const IRequestRetryPolicyPtr& retryPolicy,
    TRawBatchRequest* retryBatch,
    TInstant now)
{
    Y_ABORT_UNLESS(retryBatch);

    EnsureType(node, TNode::List);
    auto& responseList = node.AsList();
    const auto size = responseList.size();
    Y_ENSURE(size <= BatchItemList_.size(),
        "Size of server response exceeds size of batch request;"
        " size of batch: " << BatchItemList_.size() <<
        " size of server response: " << size << '.');

    for (size_t i = 0; i != size; ++i) {
        try {
            EnsureType(responseList[i], TNode::Map);
            auto& responseNode = responseList[i].AsMap();
            const auto outputIt = responseNode.find("output");
            if (outputIt != responseNode.end()) {
                BatchItemList_[i].ResponseParser->SetResponse(std::move(outputIt->second));
            } else {
                const auto errorIt = responseNode.find("error");
                if (errorIt == responseNode.end()) {
                    BatchItemList_[i].ResponseParser->SetResponse(Nothing());
                } else {
                    TErrorResponse error(400, requestId);
                    error.SetError(TYtError(errorIt->second));
                    if (auto curInterval = IsRetriable(error) ? retryPolicy->OnRetriableError(error) : Nothing()) {
                        YT_LOG_INFO(
                            "Batch subrequest (%s) failed, will retry, error: %s",
                            RequestInfo(BatchItemList_[i].Parameters),
                            error.what());
                        retryBatch->AddRequest(TBatchItem(BatchItemList_[i], now + *curInterval));
                    } else {
                        YT_LOG_ERROR(
                            "Batch subrequest (%s) failed, error: %s",
                            RequestInfo(BatchItemList_[i].Parameters),
                            error.what());
                        BatchItemList_[i].ResponseParser->SetException(std::make_exception_ptr(error));
                    }
                }
            }
        } catch (const std::exception& e) {
            // We don't expect other exceptions, so we don't catch (...)
            BatchItemList_[i].ResponseParser->SetException(std::current_exception());
        }
    }
    BatchItemList_.erase(BatchItemList_.begin(), BatchItemList_.begin() + size);
}

void TRawBatchRequest::SetErrorResult(std::exception_ptr e) const
{
    for (const auto& batchItem : BatchItemList_) {
        batchItem.ResponseParser->SetException(e);
    }
}

size_t TRawBatchRequest::BatchSize() const
{
    return BatchItemList_.size();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail::NRawClient
