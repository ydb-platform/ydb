#include "file_reader.h"

#include "transaction.h"
#include "transaction_pinger.h"

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/tvm.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/cpp/mapreduce/io/helpers.h>

#include <yt/cpp/mapreduce/http/helpers.h>
#include <yt/cpp/mapreduce/http/http.h>
#include <yt/cpp/mapreduce/http/http_client.h>
#include <yt/cpp/mapreduce/http/retry_request.h>

#include <yt/cpp/mapreduce/raw_client/raw_requests.h>

namespace NYT {
namespace NDetail {

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

static TMaybe<ui64> GetEndOffset(const TFileReaderOptions& options) {
    if (options.Length_) {
        return options.Offset_.GetOrElse(0) + *options.Length_;
    } else {
        return Nothing();
    }
}

////////////////////////////////////////////////////////////////////////////////

TStreamReaderBase::TStreamReaderBase(
    IClientRetryPolicyPtr clientRetryPolicy,
    ITransactionPingerPtr transactionPinger,
    const TClientContext& context,
    const TTransactionId& transactionId)
    : Context_(context)
    , ClientRetryPolicy_(std::move(clientRetryPolicy))
    , ReadTransaction_(MakeHolder<TPingableTransaction>(
        ClientRetryPolicy_,
        context,
        transactionId,
        transactionPinger->GetChildTxPinger(),
        TStartTransactionOptions()))
{ }

TStreamReaderBase::~TStreamReaderBase() = default;

TYPath TStreamReaderBase::Snapshot(const TYPath& path)
{
    return NYT::Snapshot(ClientRetryPolicy_, Context_, ReadTransaction_->GetId(), path);
}

TString TStreamReaderBase::GetActiveRequestId() const
{
    if (Response_) {
        return Response_->GetRequestId();;
    } else {
        return "<no-active-request>";
    }
}

size_t TStreamReaderBase::DoRead(void* buf, size_t len)
{
    const int retryCount = Context_.Config->ReadRetryCount;
    for (int attempt = 1; attempt <= retryCount; ++attempt) {
        try {
            if (!Input_) {
                Response_ = Request(Context_, ReadTransaction_->GetId(), CurrentOffset_);
                Input_ = Response_->GetResponseStream();
            }
            if (len == 0) {
                return 0;
            }
            const size_t read = Input_->Read(buf, len);
            CurrentOffset_ += read;
            return read;
        } catch (TErrorResponse& e) {
            YT_LOG_ERROR("RSP %v - failed: %v (attempt %v of %v)",
                GetActiveRequestId(),
                e.what(),
                attempt,
                retryCount);

            if (!IsRetriable(e) || attempt == retryCount) {
                throw;
            }
            NDetail::TWaitProxy::Get()->Sleep(GetBackoffDuration(e, Context_.Config));
        } catch (std::exception& e) {
            YT_LOG_ERROR("RSP %v - failed: %v (attempt %v of %v)",
                GetActiveRequestId(),
                e.what(),
                attempt,
                retryCount);

            // Invalidate connection.
            Response_.reset();

            if (attempt == retryCount) {
                throw;
            }
            NDetail::TWaitProxy::Get()->Sleep(GetBackoffDuration(e, Context_.Config));
        }
        Input_ = nullptr;
    }
    Y_UNREACHABLE(); // we should either return or throw from loop above
}

////////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader(
    const TRichYPath& path,
    IClientRetryPolicyPtr clientRetryPolicy,
    ITransactionPingerPtr transactionPinger,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TFileReaderOptions& options)
    : TStreamReaderBase(std::move(clientRetryPolicy), std::move(transactionPinger), context, transactionId)
    , FileReaderOptions_(options)
    , Path_(path)
    , StartOffset_(FileReaderOptions_.Offset_.GetOrElse(0))
    , EndOffset_(GetEndOffset(FileReaderOptions_))
{
    Path_.Path_ = TStreamReaderBase::Snapshot(Path_.Path_);
}

NHttpClient::IHttpResponsePtr TFileReader::Request(const TClientContext& context, const TTransactionId& transactionId, ui64 readBytes)
{
    const ui64 currentOffset = StartOffset_ + readBytes;
    TString hostName = GetProxyForHeavyRequest(context);

    THttpHeader header("GET", GetReadFileCommand(context.Config->ApiVersion));
    if (context.ServiceTicketAuth) {
        header.SetServiceTicket(context.ServiceTicketAuth->Ptr->IssueServiceTicket());
    } else {
        header.SetToken(context.Token);
    }

    if (context.ImpersonationUser) {
        header.SetImpersonationUser(*context.ImpersonationUser);
    }

    UpdateHeaderForProxyIfNeed(hostName, context, header);

    header.AddTransactionId(transactionId);
    header.SetOutputFormat(TMaybe<TFormat>()); // Binary format

    if (EndOffset_) {
        Y_ABORT_UNLESS(*EndOffset_ >= currentOffset);
        FileReaderOptions_.Length(*EndOffset_ - currentOffset);
    }
    FileReaderOptions_.Offset(currentOffset);
    header.MergeParameters(FormIORequestParameters(Path_, FileReaderOptions_));

    header.SetResponseCompression(ToString(context.Config->AcceptEncoding));

    auto requestId = CreateGuidAsString();
    NHttpClient::IHttpResponsePtr response;
    try {
        response = context.HttpClient->Request(GetFullUrl(hostName, context, header), requestId, header);
    } catch (const std::exception& ex) {
        LogRequestError(requestId, header, ex.what(), "");
        throw;
    }

    YT_LOG_DEBUG("RSP %v - file stream",
        requestId);

    return response;
}

////////////////////////////////////////////////////////////////////////////////

TBlobTableReader::TBlobTableReader(
    const TYPath& path,
    const TKey& key,
    IClientRetryPolicyPtr retryPolicy,
    ITransactionPingerPtr transactionPinger,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TBlobTableReaderOptions& options)
    : TStreamReaderBase(std::move(retryPolicy), std::move(transactionPinger), context, transactionId)
    , Key_(key)
    , Options_(options)
{
    Path_ = TStreamReaderBase::Snapshot(path);
}

NHttpClient::IHttpResponsePtr TBlobTableReader::Request(const TClientContext& context, const TTransactionId& transactionId, ui64 readBytes)
{
    TString hostName = GetProxyForHeavyRequest(context);

    THttpHeader header("GET", "read_blob_table");
    if (context.ServiceTicketAuth) {
        header.SetServiceTicket(context.ServiceTicketAuth->Ptr->IssueServiceTicket());
    } else {
        header.SetToken(context.Token);
    }

    if (context.ImpersonationUser) {
        header.SetImpersonationUser(*context.ImpersonationUser);
    }

    UpdateHeaderForProxyIfNeed(hostName, context, header);

    header.AddTransactionId(transactionId);
    header.SetOutputFormat(TMaybe<TFormat>()); // Binary format

    const ui64 currentOffset = Options_.Offset_ + readBytes;
    const i64 startPartIndex = currentOffset / Options_.PartSize_;
    const ui64 skipBytes = currentOffset - Options_.PartSize_ * startPartIndex;
    auto lowerLimitKey = Key_;
    lowerLimitKey.Parts_.push_back(startPartIndex);
    auto upperLimitKey = Key_;
    upperLimitKey.Parts_.push_back(std::numeric_limits<i64>::max());
    TNode params = PathToParamNode(TRichYPath(Path_).AddRange(TReadRange()
        .LowerLimit(TReadLimit().Key(lowerLimitKey))
        .UpperLimit(TReadLimit().Key(upperLimitKey))));
    params["start_part_index"] = TNode(startPartIndex);
    params["offset"] = skipBytes;
    if (Options_.PartIndexColumnName_) {
        params["part_index_column_name"] = *Options_.PartIndexColumnName_;
    }
    if (Options_.DataColumnName_) {
        params["data_column_name"] = *Options_.DataColumnName_;
    }
    params["part_size"] = Options_.PartSize_;
    header.MergeParameters(params);
    header.SetResponseCompression(ToString(context.Config->AcceptEncoding));

    auto requestId = CreateGuidAsString();
    NHttpClient::IHttpResponsePtr response;
    try {
        response = context.HttpClient->Request(GetFullUrl(hostName, context, header), requestId, header);
    } catch (const std::exception& ex) {
        LogRequestError(requestId, header, ex.what(), "");
        throw;
    }

    YT_LOG_DEBUG("RSP %v - blob table stream",
        requestId);
    return response;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
