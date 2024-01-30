#include "retryful_writer.h"

#include "retry_heavy_write_request.h"

#include <yt/cpp/mapreduce/http/requests.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/finish_or_die.h>

#include <yt/cpp/mapreduce/interface/logging/yt_log.h>

#include <util/generic/size_literals.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TRetryfulWriter::~TRetryfulWriter()
{
    NDetail::FinishOrDie(this, AutoFinish_, "TRetryfulWriter");
}

void TRetryfulWriter::CheckWriterState()
{
    switch (WriterState_) {
        case Ok:
            break;
        case Completed:
            ythrow TApiUsageError() << "Cannot use table writer that is finished";
        case Error:
            ythrow TApiUsageError() << "Cannot use table writer that finished with error";
    }
}

void TRetryfulWriter::NotifyRowEnd()
{
    CheckWriterState();
    if (Buffer_.Size() >= BufferSize_) {
        FlushBuffer(false);
    }
}

void TRetryfulWriter::DoWrite(const void* buf, size_t len)
{
    CheckWriterState();
    while (Buffer_.Size() + len > Buffer_.Capacity()) {
        Buffer_.Reserve(Buffer_.Capacity() * 2);
    }
    Buffer_.Append(static_cast<const char*>(buf), len);
}

void TRetryfulWriter::DoFinish()
{
    if (WriterState_ != Ok) {
        return;
    }
    FlushBuffer(true);
    if (Started_) {
        FilledBuffers_.Stop();
        Thread_.Join();
    }
    if (Exception_) {
        WriterState_ = Error;
        std::rethrow_exception(Exception_);
    }
    if (WriteTransaction_) {
        WriteTransaction_->Commit();
    }
    WriterState_ = Completed;
}

void TRetryfulWriter::FlushBuffer(bool lastBlock)
{
    if (!Started_) {
        if (lastBlock) {
            try {
                Send(Buffer_);
            } catch (...) {
                WriterState_ = Error;
                throw;
            }
            return;
        } else {
            Started_ = true;
            Thread_.Start();
        }
    }

    auto emptyBuffer = EmptyBuffers_.Pop();
    if (!emptyBuffer) {
        WriterState_ = Error;
        std::rethrow_exception(Exception_);
    }
    FilledBuffers_.Push(std::move(Buffer_));
    Buffer_ = std::move(emptyBuffer.GetRef());
}

void TRetryfulWriter::Send(const TBuffer& buffer)
{
    THttpHeader header("PUT", Command_);
    header.SetInputFormat(Format_);
    header.MergeParameters(Parameters_);

    auto streamMaker = [&buffer] () {
        return MakeHolder<TBufferInput>(buffer);
    };

    auto transactionId = (WriteTransaction_ ? WriteTransaction_->GetId() : ParentTransactionId_);
    RetryHeavyWriteRequest(ClientRetryPolicy_, TransactionPinger_, Context_, transactionId, header, streamMaker);

    Parameters_ = SecondaryParameters_; // all blocks except the first one are appended
}

void TRetryfulWriter::SendThread()
{
    while (auto maybeBuffer = FilledBuffers_.Pop()) {
        auto& buffer = maybeBuffer.GetRef();
        try {
            Send(buffer);
        } catch (const std::exception&) {
            Exception_ = std::current_exception();
            EmptyBuffers_.Stop();
            break;
        }
        buffer.Clear();
        EmptyBuffers_.Push(std::move(buffer));
    }
}

void* TRetryfulWriter::SendThread(void* opaque)
{
    static_cast<TRetryfulWriter*>(opaque)->SendThread();
    return nullptr;
}

void TRetryfulWriter::Abort()
{
    if (Started_) {
        FilledBuffers_.Stop();
        Thread_.Join();
    }
    if (WriteTransaction_) {
        WriteTransaction_->Abort();
    }
    WriterState_ = Completed;
}

size_t TRetryfulWriter::GetBufferMemoryUsage() const
{
    return BufferSize_ * 4;
}

size_t TRetryfulWriter::GetBufferSize(const TMaybe<TWriterOptions>& writerOptions)
{
    auto retryBlockSize = TMaybe<size_t>();
    if (writerOptions) {
        if (writerOptions->RetryBlockSize_) {
            retryBlockSize = *writerOptions->RetryBlockSize_;
        } else if (writerOptions->DesiredChunkSize_) {
            retryBlockSize = *writerOptions->DesiredChunkSize_;
        }
    }
    return retryBlockSize.GetOrElse(64_MB);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
