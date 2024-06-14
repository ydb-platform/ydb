#include "client_reader.h"

#include "structured_table_formats.h"
#include "transaction.h"
#include "transaction_pinger.h"

#include <yt/cpp/mapreduce/common/helpers.h>
#include <yt/cpp/mapreduce/common/retry_lib.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>

#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/interface/tvm.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <yt/cpp/mapreduce/io/helpers.h>
#include <yt/cpp/mapreduce/io/yamr_table_reader.h>

#include <yt/cpp/mapreduce/http/helpers.h>
#include <yt/cpp/mapreduce/http/requests.h>
#include <yt/cpp/mapreduce/http/retry_request.h>

#include <yt/cpp/mapreduce/raw_client/raw_requests.h>

#include <library/cpp/yson/node/serialize.h>

#include <util/random/random.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NYT {

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

TClientReader::TClientReader(
    const TRichYPath& path,
    IClientRetryPolicyPtr clientRetryPolicy,
    ITransactionPingerPtr transactionPinger,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TFormat& format,
    const TTableReaderOptions& options,
    bool useFormatFromTableAttributes)
    : Path_(path)
    , ClientRetryPolicy_(std::move(clientRetryPolicy))
    , Context_(context)
    , ParentTransactionId_(transactionId)
    , Format_(format)
    , Options_(options)
    , ReadTransaction_(nullptr)
{
    if (options.CreateTransaction_) {
        Y_ABORT_UNLESS(transactionPinger, "Internal error: transactionPinger is null");
        ReadTransaction_ = MakeHolder<TPingableTransaction>(
            ClientRetryPolicy_,
            Context_,
            transactionId,
            transactionPinger->GetChildTxPinger(),
            TStartTransactionOptions());
        Path_.Path(Snapshot(
            ClientRetryPolicy_,
            Context_,
            ReadTransaction_->GetId(),
            path.Path_));
    }

    if (useFormatFromTableAttributes) {
        auto transactionId2 = ReadTransaction_ ? ReadTransaction_->GetId() : ParentTransactionId_;
        auto newFormat = GetTableFormat(ClientRetryPolicy_, Context_, transactionId2, Path_);
        if (newFormat) {
            Format_->Config = *newFormat;
        }
    }

    TransformYPath();
    CreateRequest();
}

bool TClientReader::Retry(
    const TMaybe<ui32>& rangeIndex,
    const TMaybe<ui64>& rowIndex,
    const std::exception_ptr& error)
{
    if (CurrentRequestRetryPolicy_) {
        TMaybe<TDuration> backoffDuration;
        try {
            std::rethrow_exception(error);
        } catch (const TErrorResponse& ex) {
            if (!IsRetriable(ex)) {
                throw;
            }
            backoffDuration = CurrentRequestRetryPolicy_->OnRetriableError(ex);
        } catch (const std::exception& ex) {
            if (!IsRetriable(ex)) {
                throw;
            }
            backoffDuration = CurrentRequestRetryPolicy_->OnGenericError(ex);
        } catch (...) {
        }

        if (!backoffDuration) {
            return false;
        }

        NDetail::TWaitProxy::Get()->Sleep(*backoffDuration);
    }

    try {
        CreateRequest(rangeIndex, rowIndex);
        return true;
    } catch (const std::exception& ex) {
        YT_LOG_ERROR("Client reader retry failed: %v",
            ex.what());

        return false;
    }
}

void TClientReader::ResetRetries()
{
    CurrentRequestRetryPolicy_ = nullptr;
}

size_t TClientReader::DoRead(void* buf, size_t len)
{
    return Input_->Read(buf, len);
}

void TClientReader::TransformYPath()
{
    for (auto& range : Path_.MutableRangesView()) {
        auto& exact = range.Exact_;
        if (IsTrivial(exact)) {
            continue;
        }

        if (exact.RowIndex_) {
            range.LowerLimit(TReadLimit().RowIndex(*exact.RowIndex_));
            range.UpperLimit(TReadLimit().RowIndex(*exact.RowIndex_ + 1));
            exact.RowIndex_.Clear();

        } else if (exact.Key_) {
            range.LowerLimit(TReadLimit().Key(*exact.Key_));

            auto lastPart = TNode::CreateEntity();
            lastPart.Attributes() = TNode()("type", "max");
            exact.Key_->Parts_.push_back(lastPart);

            range.UpperLimit(TReadLimit().Key(*exact.Key_));
            exact.Key_.Clear();
        }
    }
}

void TClientReader::CreateRequest(const TMaybe<ui32>& rangeIndex, const TMaybe<ui64>& rowIndex)
{
    if (!CurrentRequestRetryPolicy_) {
        CurrentRequestRetryPolicy_ = ClientRetryPolicy_->CreatePolicyForGenericRequest();
    }

    bool areRangesUpdated = false;

    while (true) {
        CurrentRequestRetryPolicy_->NotifyNewAttempt();

        THttpHeader header("GET", GetReadTableCommand(Context_.Config->ApiVersion));
        if (Context_.ServiceTicketAuth) {
            header.SetServiceTicket(Context_.ServiceTicketAuth->Ptr->IssueServiceTicket());
        } else {
            header.SetToken(Context_.Token);
        }

        if (Context_.ImpersonationUser) {
            header.SetImpersonationUser(*Context_.ImpersonationUser);
        }

        auto transactionId = (ReadTransaction_ ? ReadTransaction_->GetId() : ParentTransactionId_);
        header.AddTransactionId(transactionId);

        const auto& controlAttributes = Options_.ControlAttributes_;
        header.AddParameter("control_attributes", TNode()
            ("enable_row_index", controlAttributes.EnableRowIndex_)
            ("enable_range_index", controlAttributes.EnableRangeIndex_));
        header.SetOutputFormat(Format_);

        header.SetResponseCompression(ToString(Context_.Config->AcceptEncoding));

        if (rowIndex.Defined() && !areRangesUpdated) {
            auto& ranges = Path_.MutableRanges();
            if (ranges.Empty()) {
                ranges.ConstructInPlace(TVector{TReadRange()});
            } else {
                if (rangeIndex.GetOrElse(0) >= ranges->size()) {
                    ythrow yexception()
                        << "range index " << rangeIndex.GetOrElse(0)
                        << " is out of range, input range count is " << ranges->size();
                }
                ranges->erase(ranges->begin(), ranges->begin() + rangeIndex.GetOrElse(0));
            }
            ranges->begin()->LowerLimit(TReadLimit().RowIndex(*rowIndex));
            areRangesUpdated = true;
        }

        header.MergeParameters(FormIORequestParameters(Path_, Options_));

        auto requestId = CreateGuidAsString();

        try {
            const auto proxyName = GetProxyForHeavyRequest(Context_);
            UpdateHeaderForProxyIfNeed(proxyName, Context_, header);
            Response_ = Context_.HttpClient->Request(GetFullUrlForProxy(proxyName, Context_, header), requestId, header);

            Input_ = Response_->GetResponseStream();

            YT_LOG_DEBUG(
                "RSP %v - table stream (RangeIndex: %v, RowIndex: %v)",
                requestId,
                rangeIndex,
                rowIndex);

            return;
        } catch (const TErrorResponse& e) {
            LogRequestError(
                requestId,
                header,
                e.what(),
                CurrentRequestRetryPolicy_->GetAttemptDescription());

            if (!IsRetriable(e)) {
                throw;
            }
            auto backoff = CurrentRequestRetryPolicy_->OnRetriableError(e);
            if (!backoff) {
                throw;
            }
            NDetail::TWaitProxy::Get()->Sleep(*backoff);
        } catch (const std::exception& e) {
            LogRequestError(
                requestId,
                header,
                e.what(),
                CurrentRequestRetryPolicy_->GetAttemptDescription());

            Response_.reset();
            Input_ = nullptr;

            auto backoff = CurrentRequestRetryPolicy_->OnGenericError(e);
            if (!backoff) {
                throw;
            }
            NDetail::TWaitProxy::Get()->Sleep(*backoff);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
