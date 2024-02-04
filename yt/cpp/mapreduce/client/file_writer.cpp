#include "file_writer.h"

#include <yt/cpp/mapreduce/io/helpers.h>
#include <yt/cpp/mapreduce/interface/finish_or_die.h>

#include <yt/cpp/mapreduce/common/helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(
    const TRichYPath& path,
    IClientRetryPolicyPtr clientRetryPolicy,
    ITransactionPingerPtr transactionPinger,
    const TClientContext& context,
    const TTransactionId& transactionId,
    const TFileWriterOptions& options)
    : AutoFinish_(options.AutoFinish_)
    , RetryfulWriter_(
        std::move(clientRetryPolicy),
        std::move(transactionPinger),
        context,
        transactionId,
        GetWriteFileCommand(context.Config->ApiVersion),
        TMaybe<TFormat>(),
        path,
        options)
{ }

TFileWriter::~TFileWriter()
{
    NDetail::FinishOrDie(this, AutoFinish_, "TFileWriter");
}

void TFileWriter::DoWrite(const void* buf, size_t len)
{
    // If user tunes RetryBlockSize / DesiredChunkSize he expects
    // us to send data exactly by RetryBlockSize. So behaviour of the writer is predictable.
    //
    // We want to avoid situation when size of sent data slightly exceeded DesiredChunkSize
    // and server produced one chunk of desired size and one small chunk.
    while (len > 0) {
        const auto retryBlockRemainingSize = RetryfulWriter_.GetRetryBlockRemainingSize();
        Y_ABORT_UNLESS(retryBlockRemainingSize > 0);
        const auto firstWriteLen = Min(len, retryBlockRemainingSize);
        RetryfulWriter_.Write(buf, firstWriteLen);
        RetryfulWriter_.NotifyRowEnd();
        len -= firstWriteLen;
        buf = static_cast<const char*>(buf) + firstWriteLen;
    }
}

void TFileWriter::DoFinish()
{
    RetryfulWriter_.Finish();
}

size_t TFileWriter::GetBufferMemoryUsage() const
{
    return RetryfulWriter_.GetBufferMemoryUsage();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
