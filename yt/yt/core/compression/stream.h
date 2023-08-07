#pragma once

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/memory/ref.h>

#include <contrib/libs/snappy/snappy-sinksource.h>
#include <contrib/libs/snappy/snappy.h>

#include <array>
#include <vector>

namespace NYT::NCompression::NDetail {

////////////////////////////////////////////////////////////////////////////////

class TSource
    : public snappy::Source
    , public IInputStream
{
public:
    using Source::Skip;

protected:
    size_t DoRead(void* buffer, size_t size) override;
};

////////////////////////////////////////////////////////////////////////////////

class TSink
    : public snappy::Sink
    , public IOutputStream
{
protected:
    void DoWrite(const void *buf, size_t size) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRefSource
    : public TSource
{
public:
    explicit TRefSource(TRef block);

    size_t Available() const override;
    const char* Peek(size_t* len) override;
    void Skip(size_t len) override;

private:
    const TRef Block_;
    size_t Available_;
    size_t Position_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TRefsVectorSource
    : public TSource
{
public:
    explicit TRefsVectorSource(const std::vector<TSharedRef>& blocks);

    size_t Available() const override;
    const char* Peek(size_t* len) override;
    void Skip(size_t len) override;

private:
    void SkipCompletedBlocks();

    const std::vector<TSharedRef>& Blocks_;
    size_t Available_;
    size_t Index_ = 0;
    size_t Position_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TBlobSink
    : public TSink
{
public:
    explicit TBlobSink(TBlob* output);

    void Append(const char* data, size_t size) override;

private:
    TBlob* const Output_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression::NDetail
