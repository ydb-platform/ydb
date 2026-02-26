#include "appendable_compressed_file.h"

#include "log_codec.h"
#include "stream_output.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NLogging {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TAppendableCompressedFile
    : public IStreamLogOutput
{
public:
    TAppendableCompressedFile(
        TFile file,
        ILogCodecPtr codec,
        IInvokerPtr compressInvoker,
        const TAppendableCompressedFileOptions& options)
        : Codec_(std::move(codec))
        , CompressInvoker_(std::move(compressInvoker))
        , Options_(options)
        , SerializedInvoker_(CreateSerializedInvoker(
            CompressInvoker_,
            NProfiling::TTagSet({
                {"invoker", "appendable_compressed_file"},
                {"file_name", file.GetName()},
            })))
        , MaxBlockSize_(Codec_->GetMaxBlockSize())
        , File_(std::move(file))
    {
        i64 fileSize = File_.GetLength();
        OutputPosition_ = Codec_->Repair(&File_);
        if (OutputPosition_ != fileSize && Options_.WriteTruncateMessage) {
            auto message = Format("*** Truncated %v trailing bytes due to zstd repair\n", fileSize - OutputPosition_);
            Input_.Append(message.data(), message.size());
            Flush();
        }
    }

private:
    const ILogCodecPtr Codec_;
    const IInvokerPtr CompressInvoker_;
    const TAppendableCompressedFileOptions Options_;
    const IInvokerPtr SerializedInvoker_;
    const i64 MaxBlockSize_;

    TFile File_;

    // These fields are read and updated in the thread that owns this TAppendableCompressedFile.
    TBuffer Input_;
    i64 EnqueuedBuffersCount_ = 0;
    i64 OutputPosition_ = 0;

    // These fields are read and updated in SerializedInvoker_.
    THashMap<i64, TBuffer> CompressedBuffers_;
    i64 WrittenBuffersCount_ = 0;

    // These fields are read and updated under FlushSpinLock_.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, FlushSpinLock_);
    i64 BuffersToFlushCount_ = 0;
    i64 CompressedBuffersCount_ = 0;
    TPromise<void> ReadyToFlushEvent_;

    void DoWrite(const void* buf, size_t len) final
    {
        const char* in = reinterpret_cast<const char*>(buf);
        while (len > 0) {
            size_t toWrite = len;
            if (static_cast<i64>(Input_.Size()) >= MaxBlockSize_) {
                toWrite = 0;
            } else if (static_cast<i64>(Input_.Size() + len) >= MaxBlockSize_) {
                toWrite = MaxBlockSize_ - Input_.Size();
            }

            Input_.Append(in, toWrite);
            in += toWrite;
            len -= toWrite;

            while (static_cast<i64>(Input_.Size()) >= MaxBlockSize_) {
                EnqueueOneFrame();
            }
        }
    }

    void DoFlush() final
    {
        while (!Input_.Empty()) {
            EnqueueOneFrame();
        }
        FlushOutput();
    }

    void DoFinish() final
    {
        Flush();
    }

    void EnqueueBuffer(TBuffer buffer)
    {
        i64 bufferId = EnqueuedBuffersCount_;
        ++EnqueuedBuffersCount_;

        BIND([this_ = MakeStrong(this), this, buffer = std::move(buffer)] {
            TBuffer output;
            Codec_->Compress(buffer, &output);
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

    void EnqueueOneFrame()
    {
        if (Input_.Empty()) {
            return;
        }

        size_t toWrite = std::min(Input_.Size(), static_cast<size_t>(MaxBlockSize_));
        if (Options_.TryNotBreakLines) {
            auto newlinePos = std::string_view(Input_.Begin(), Input_.End()).find_last_of('\n');
            if (newlinePos != std::string_view::npos) {
                toWrite = newlinePos + 1;
            }
        }
        EnqueueBuffer(TBuffer(Input_.Data(), toWrite));
        Input_.ChopHead(toWrite);
    }

    void FlushOutput()
    {
        TFuture<void> readyToFlushFuture;

        {
            auto guard = Guard(FlushSpinLock_);
            BuffersToFlushCount_ = EnqueuedBuffersCount_;
            if (BuffersToFlushCount_ == CompressedBuffersCount_) {
                readyToFlushFuture = OKFuture;
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
                    Codec_->AddSyncTag(outputPosition + output.Size(), &output);

                    CompressedBuffers_.erase(it);
                    ++WrittenBuffersCount_;
                }

                return output;
            })
                .AsyncVia(SerializedInvoker_));

        // We use .Get() here instead of WaitFor() here to ensure that this method doesn't do context switches.
        // Otherwise, flush events in TLogManager may intersect, because another event could start while we are
        // waiting in WaitFor().
        auto outputBuffer = asyncOutputBuffer.BlockingGet().ValueOrThrow();
        File_.Pwrite(outputBuffer.Data(), outputBuffer.Size(), OutputPosition_);
        OutputPosition_ += outputBuffer.Size();
    }
};

////////////////////////////////////////////////////////////////////////////////

IStreamLogOutputPtr CreateAppendableCompressedFile(
    TFile file,
    ILogCodecPtr codec,
    IInvokerPtr compressInvoker,
    const TAppendableCompressedFileOptions& options)
{
    return New<TAppendableCompressedFile>(
        std::move(file),
        std::move(codec),
        std::move(compressInvoker),
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
