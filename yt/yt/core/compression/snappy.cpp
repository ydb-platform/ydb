#include "snappy.h"

#include <contrib/libs/snappy/snappy-stubs-internal.h>
#include <contrib/libs/snappy/snappy.h>

namespace NYT::NCompression::NDetail {

////////////////////////////////////////////////////////////////////////////////

class TPreloadingSource
    : public TSource
{
public:
    explicit TPreloadingSource(TSource* source)
        : Source_(source)
        , Length_(std::min(Source_->Available(), Buffer_.size()))
    {
        ReadRef(*Source_, TMutableRef(Buffer_.data(), Length_));
    }

    size_t Available() const override
    {
        return Source_->Available() + Length_ - Position_;
    }

    const char* Peek(size_t* length) override
    {
        if (Y_UNLIKELY(Position_ < Length_)) {
            *length = Length_ - Position_;
            return Buffer_.begin() + Position_;
        } else {
            return Source_->Peek(length);
        }
    }

    void Skip(size_t length) override
    {
        if (Y_UNLIKELY(Position_ < Length_)) {
            auto delta = std::min(length, Length_ - Position_);
            Position_ += delta;
            length -= delta;
        }
        if (length > 0) {
            Source_->Skip(length);
        }
    }

    const char* begin() const
    {
        return Buffer_.begin();
    }

    const char* end() const
    {
        return Buffer_.begin() + Length_;
    }

private:
    TSource* const Source_;
    std::array<char, snappy::Varint::kMax32> Buffer_;
    size_t Length_ = 0;
    size_t Position_ = 0;
};

void SnappyCompress(TSource* source, TBlob* output)
{
    // Snappy implementation relies on entire input length to fit into an integer.
    if (source->Available() > std::numeric_limits<int>::max()) {
        THROW_ERROR_EXCEPTION("Snappy compression failed: input size is too big")
            << TErrorAttribute("size", source->Available());
    }

    output->Resize(snappy::MaxCompressedLength(source->Available()), /*initializeStorage*/ false);
    snappy::UncheckedByteArraySink writer(output->Begin());
    size_t compressedSize = snappy::Compress(source, &writer);
    output->Resize(compressedSize);
}

void SnappyDecompress(TSource* source, TBlob* output)
{
    // Empty input leads to an empty output in Snappy.
    if (source->Available() == 0) {
        return;
    }

    // We hack into Snappy implementation to preallocate appropriate buffer.
    ui32 uncompressedSize = 0;
    TPreloadingSource preloadingSource(source);
    snappy::Varint::Parse32WithLimit(
        preloadingSource.begin(),
        preloadingSource.end(),
        &uncompressedSize);

    output->Resize(uncompressedSize, /*initializeStorage*/ false);

    if (!snappy::RawUncompress(&preloadingSource, output->Begin())) {
        THROW_ERROR_EXCEPTION("Snappy compression failed: RawUncompress returned an error");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression::NDetail

