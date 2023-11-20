#pragma once

#include "public.h"

#include <library/cpp/yt/memory/ref.h>

#include <util/stream/input.h>
#include <util/stream/output.h>

#include <yt/yt/core/concurrency/async_stream.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TChecksum GetChecksum(TRef data, TChecksum seed = 0);

TChecksum CombineChecksums(const std::vector<TChecksum>& blockChecksums);

////////////////////////////////////////////////////////////////////////////////

class TChecksumInput
    : public IInputStream
{
public:
    explicit TChecksumInput(IInputStream* input);
    TChecksum GetChecksum() const;

protected:
    size_t DoRead(void* buf, size_t len) override;

private:
    IInputStream* const Input_;
    TChecksum Checksum_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TChecksumOutput
    : public IOutputStream
{
public:
    explicit TChecksumOutput(IOutputStream* output);
    TChecksum GetChecksum() const;

protected:
    void DoWrite(const void* buf, size_t len) override;
    void DoFlush() override;
    void DoFinish() override;

private:
    IOutputStream* const Output_;
    TChecksum Checksum_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TChecksumAsyncOutput
    : public NConcurrency::IAsyncOutputStream
{
public:
    explicit TChecksumAsyncOutput(NConcurrency::IAsyncOutputStreamPtr underlyingStream);

    TFuture<void> Close() override;

    TFuture<void> Write(const TSharedRef& block) override;

    DEFINE_BYVAL_RW_PROPERTY(TChecksum, Checksum);

private:
    const NConcurrency::IAsyncOutputStreamPtr UnderlyingStream_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
