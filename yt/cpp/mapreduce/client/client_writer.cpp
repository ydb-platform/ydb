#include "client_writer.h"

#include "retryful_writer.h"
#include "retryless_writer.h"

#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/common/fwd.h>
#include <yt/cpp/mapreduce/common/helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TClientWriter::TClientWriter(
    const TRichYPath& path,
    IClientRetryPolicyPtr clientRetryPolicy,
    ITransactionPingerPtr transactionPinger,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TMaybe<TFormat>& format,
    const TTableWriterOptions& options)
    : BufferSize_(options.BufferSize_)
{
    if (options.SingleHttpRequest_) {
        RawWriter_.Reset(new TRetrylessWriter(
            context,
            transactionId,
            GetWriteTableCommand(context.Config->ApiVersion),
            format,
            path,
            BufferSize_,
            options));
    } else {
        RawWriter_.Reset(new TRetryfulWriter(
            std::move(clientRetryPolicy),
            std::move(transactionPinger),
            context,
            transactionId,
            GetWriteTableCommand(context.Config->ApiVersion),
            format,
            path,
            options));
    }
}

size_t TClientWriter::GetStreamCount() const
{
    return 1;
}

IOutputStream* TClientWriter::GetStream(size_t tableIndex) const
{
    Y_UNUSED(tableIndex);
    return RawWriter_.Get();
}

void TClientWriter::OnRowFinished(size_t)
{
    RawWriter_->NotifyRowEnd();
}

void TClientWriter::Abort()
{
    RawWriter_->Abort();
}

size_t TClientWriter::GetBufferMemoryUsage() const
{
    return RawWriter_->GetBufferMemoryUsage();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
