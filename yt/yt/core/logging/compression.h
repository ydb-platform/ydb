#pragma once

#include "public.h"

#include "stream_output.h"

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <util/generic/buffer.h>

#include <util/stream/file.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

//! Base interface for compression codec in logs. It is different from ICodec in yt/yt/core/compression,
//! as we must have the possibility to repair the corrupted log file if the process died unexpectedly.
struct ILogCompressionCodec
    : public TRefCounted
{
    // NB. All the methods in this interface must be thread-safe.

    virtual i64 GetMaxBlockSize() const = 0;
    virtual void Compress(const TBuffer& input, TBuffer& output) = 0;
    virtual void AddSyncTag(i64 offset, TBuffer& output) = 0;
    virtual void Repair(TFile* file, i64& outputPosition) = 0;
};

DEFINE_REFCOUNTED_TYPE(ILogCompressionCodec)

////////////////////////////////////////////////////////////////////////////////

class TAppendableCompressedFile
    : public IStreamLogOutput
{
public:
    TAppendableCompressedFile(
        TFile file,
        ILogCompressionCodecPtr codec,
        IInvokerPtr compressInvoker,
        bool writeTruncateMessage);

private:
    const ILogCompressionCodecPtr Codec_;
    const IInvokerPtr CompressInvoker_;
    const IInvokerPtr SerializedInvoker_;
    const size_t MaxBlockSize_;

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

    void DoWrite(const void* buf, size_t len) override;
    void DoFlush() override;
    void DoFinish() override;

    void EnqueueBuffer(TBuffer buffer);
    void EnqueueOneFrame();

    void FlushOutput();
};

DECLARE_REFCOUNTED_TYPE(TAppendableCompressedFile)
DEFINE_REFCOUNTED_TYPE(TAppendableCompressedFile)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
