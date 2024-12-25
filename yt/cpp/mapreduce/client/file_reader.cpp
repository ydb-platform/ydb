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

#include <yt/cpp/mapreduce/raw_client/raw_client.h>
#include <yt/cpp/mapreduce/raw_client/raw_requests.h>

namespace NYT {
namespace NDetail {

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

static TMaybe<ui64> GetEndOffset(const TFileReaderOptions& options) {
    if (options.Length_) {
        return options.Offset_ + *options.Length_;
    } else {
        return Nothing();
    }
}

////////////////////////////////////////////////////////////////////////////////

TStreamReaderBase::TStreamReaderBase(
    const IRawClientPtr& rawClient,
    IClientRetryPolicyPtr clientRetryPolicy,
    ITransactionPingerPtr transactionPinger,
    const TClientContext& context,
    const TTransactionId& transactionId)
    : RawClient_(rawClient)
    , ClientRetryPolicy_(std::move(clientRetryPolicy))
    , ReadTransaction_(std::make_unique<TPingableTransaction>(
        RawClient_,
        ClientRetryPolicy_,
        context,
        transactionId,
        transactionPinger->GetChildTxPinger(),
        TStartTransactionOptions()))
{ }

TStreamReaderBase::~TStreamReaderBase() = default;

TYPath TStreamReaderBase::Snapshot(const TYPath& path)
{
    return NYT::Snapshot(RawClient_, ClientRetryPolicy_, ReadTransaction_->GetId(), path);
}

size_t TStreamReaderBase::DoRead(void* buf, size_t len)
{
    if (len == 0) {
        return 0;
    }
    return RequestWithRetry<size_t>(
        ClientRetryPolicy_->CreatePolicyForReaderRequest(),
        [this, &buf, len] (TMutationId /*mutationId*/) {
            try {
                if (!Input_) {
                    Input_ = Request(ReadTransaction_->GetId(), CurrentOffset_);
                }
                const size_t read = Input_->Read(buf, len);
                CurrentOffset_ += read;
                return read;
            } catch (...) {
                Input_ = nullptr;
                throw;
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

TFileReader::TFileReader(
    const TRichYPath& path,
    const IRawClientPtr& rawClient,
    IClientRetryPolicyPtr clientRetryPolicy,
    ITransactionPingerPtr transactionPinger,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TFileReaderOptions& options)
    : TStreamReaderBase(rawClient, std::move(clientRetryPolicy), std::move(transactionPinger), context, transactionId)
    , StartOffset_(options.Offset_)
    , EndOffset_(GetEndOffset(options))
    , Options_(options)
    , Path_(path)
{
    Path_.Path_ = TStreamReaderBase::Snapshot(Path_.Path_);
}

std::unique_ptr<IInputStream> TFileReader::Request(const TTransactionId& transactionId, ui64 readBytes)
{
    const ui64 currentOffset = StartOffset_ + readBytes;

    if (EndOffset_) {
        Y_ABORT_UNLESS(*EndOffset_ >= currentOffset);
        Options_.Length(*EndOffset_ - currentOffset);
    }

    Options_.Offset(currentOffset);
    return RawClient_->ReadFile(transactionId, Path_, Options_);
}

////////////////////////////////////////////////////////////////////////////////

TBlobTableReader::TBlobTableReader(
    const TYPath& path,
    const TKey& key,
    const IRawClientPtr& rawClient,
    IClientRetryPolicyPtr retryPolicy,
    ITransactionPingerPtr transactionPinger,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TBlobTableReaderOptions& options)
    : TStreamReaderBase(rawClient, std::move(retryPolicy), std::move(transactionPinger), context, transactionId)
    , StartOffset_(options.Offset_)
    , Key_(key)
    , Options_(options)
{
    Path_ = TStreamReaderBase::Snapshot(path);
}

std::unique_ptr<IInputStream> TBlobTableReader::Request(const TTransactionId& transactionId, ui64 readBytes)
{
    const i64 currentOffset = StartOffset_ + readBytes;
    const i64 startPartIndex = currentOffset / Options_.PartSize_;
    const i64 skipBytes = currentOffset - Options_.PartSize_ * startPartIndex;

    Options_.Offset(skipBytes);
    Options_.StartPartIndex(startPartIndex);
    return RawClient_->ReadBlobTable(transactionId, Path_, Key_, Options_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
