#include "compression.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NLogging {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TAppendableCompressedFile::TAppendableCompressedFile(
    TFile file,
    ILogCompressionCodecPtr codec,
    IInvokerPtr compressInvoker,
    bool writeTruncateMessage)
    : Codec_(std::move(codec))
    , CompressInvoker_(std::move(compressInvoker))
    , SerializedInvoker_(CreateSerializedInvoker(CompressInvoker_, NProfiling::TTagSet({{"invoker", "appendable_compressed_file"}, {"file_name", file.GetName()}})))
    , MaxBlockSize_(static_cast<size_t>(Codec_->GetMaxBlockSize()))
    , File_(std::move(file))
{
    i64 fileSize = File_.GetLength();
    Codec_->Repair(&File_, OutputPosition_);
    if (OutputPosition_ != fileSize && writeTruncateMessage) {
        TStringBuilder message;
        message.AppendFormat("Truncated %v bytes due to zstd repair.\n", fileSize - OutputPosition_);
        TString messageStr = message.Flush();

        Input_.Append(messageStr.Data(), messageStr.Size());
        Flush();
    }
}

void TAppendableCompressedFile::DoWrite(const void* buf, size_t len)
{
    const char* in = reinterpret_cast<const char*>(buf);
    while (len > 0) {
        size_t toWrite = len;
        if (Input_.Size() >= MaxBlockSize_) {
            toWrite = 0;
        } else if (Input_.Size() + len >= MaxBlockSize_) {
            toWrite = MaxBlockSize_ - Input_.Size();
        }

        Input_.Append(in, toWrite);
        in += toWrite;
        len -= toWrite;

        while (Input_.Size() >= MaxBlockSize_) {
            EnqueueOneFrame();
        }
    }
}

void TAppendableCompressedFile::EnqueueOneFrame()
{
    if (Input_.Empty()) {
        return;
    }

    size_t toWrite = std::min(Input_.Size(), MaxBlockSize_);
    EnqueueBuffer(TBuffer(Input_.Data(), toWrite));
    Input_.ChopHead(toWrite);
}

void TAppendableCompressedFile::EnqueueBuffer(TBuffer buffer)
{
    i64 bufferId = EnqueuedBuffersCount_;
    ++EnqueuedBuffersCount_;

    BIND([this_ = MakeStrong(this), this, buffer = std::move(buffer)] {
        TBuffer output;
        Codec_->Compress(buffer, output);
        return output;
    })
        .AsyncVia(CompressInvoker_)
        .Run()
        .Subscribe(BIND([this_ = MakeStrong(this), this, bufferId] (TErrorOr<TBuffer> result) {
            YT_VERIFY(result.IsOK());

            CompressedBuffers_[bufferId] = std::move(result.Value());

            auto guard = Guard(FlushSpinLock_);
            ++CompressedBuffersCount_;
            if (CompressedBuffersCount_ == BuffersToFlushCount_) {
                ReadyToFlushEvent_.Set();
            }
        })
            .Via(SerializedInvoker_));
}

void TAppendableCompressedFile::DoFlush()
{
    while (!Input_.Empty()) {
        EnqueueOneFrame();
    }
    FlushOutput();
}

void TAppendableCompressedFile::FlushOutput()
{
    TFuture<void> readyToFlushFuture;

    {
        auto guard = Guard(FlushSpinLock_);
        BuffersToFlushCount_ = EnqueuedBuffersCount_;
        if (BuffersToFlushCount_ == CompressedBuffersCount_) {
            readyToFlushFuture = VoidFuture;
        } else {
            ReadyToFlushEvent_ = NewPromise<void>();
            readyToFlushFuture = ReadyToFlushEvent_.ToFuture();
        }
    }

    auto asyncOutputBuffer = readyToFlushFuture
        .Apply(BIND([this_ = MakeStrong(this), this, outputPosition = OutputPosition_] {
            TBuffer output;

            while (!CompressedBuffers_.empty()) {
                auto it = CompressedBuffers_.find(WrittenBuffersCount_);
                YT_VERIFY(it != CompressedBuffers_.end());

                output.Append(it->second.Data(), it->second.Size());
                Codec_->AddSyncTag(outputPosition + output.Size(), output);

                CompressedBuffers_.erase(it);
                ++WrittenBuffersCount_;
            }

            return output;
        })
            .AsyncVia(SerializedInvoker_));

    // We use .Get() here instead of WaitFor() here to ensure that this method doesn't do context switches.
    // Otherwise, flush events in TLogManager may intersect, because another event could start while we are
    // waiting in WaitFor().
    auto outputBuffer = asyncOutputBuffer.Get().ValueOrThrow();
    File_.Pwrite(outputBuffer.Data(), outputBuffer.Size(), OutputPosition_);
    OutputPosition_ += outputBuffer.Size();
}

void TAppendableCompressedFile::DoFinish()
{
    Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
