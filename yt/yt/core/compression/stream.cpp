#include "stream.h"

namespace NYT::NCompression::NDetail {

////////////////////////////////////////////////////////////////////////////////

size_t TSource::DoRead(void* buffer, size_t size)
{
    size_t peekedLen;
    const char* peekedPtr = Peek(&peekedLen);
    auto toRead = Min(size, peekedLen, Available());
    ::memcpy(buffer, peekedPtr, toRead);
    Skip(toRead);
    return toRead;
}

////////////////////////////////////////////////////////////////////////////////

void TSink::DoWrite(const void* buf, size_t size)
{
    Append(static_cast<const char*>(buf), size);
}

////////////////////////////////////////////////////////////////////////////////

TRefSource::TRefSource(TRef block)
    : Block_(block)
    , Available_(Block_.Size())
{ }

size_t TRefSource::Available() const
{
    return Available_;
}

const char* TRefSource::Peek(size_t* len)
{
    *len = Block_.Size() - Position_;
    return Block_.Begin() + Position_;
}

void TRefSource::Skip(size_t len)
{
    size_t toSkip = std::min(Available_, len);
    Position_ += toSkip;
    Available_ -= toSkip;
}

////////////////////////////////////////////////////////////////////////////////

TRefsVectorSource::TRefsVectorSource(const std::vector<TSharedRef>& blocks)
    : Blocks_(blocks)
    , Available_(GetByteSize(blocks))
{
    SkipCompletedBlocks();
}

size_t TRefsVectorSource::Available() const
{
    return Available_;
}

const char* TRefsVectorSource::Peek(size_t* len)
{
    if (Index_ == Blocks_.size()) {
        *len = 0;
        return nullptr;
    }
    *len = Blocks_[Index_].Size() - Position_;
    return Blocks_[Index_].Begin() + Position_;
}

void TRefsVectorSource::Skip(size_t len)
{
    while (len > 0 && Index_ < Blocks_.size()) {
        size_t toSkip = std::min(Blocks_[Index_].Size() - Position_, len);

        Position_ += toSkip;
        SkipCompletedBlocks();

        len -= toSkip;
        Available_ -= toSkip;
    }
}

void TRefsVectorSource::SkipCompletedBlocks()
{
    while (Index_ < Blocks_.size() && Position_ == Blocks_[Index_].Size()) {
        ++Index_;
        Position_ = 0;
    }
}

////////////////////////////////////////////////////////////////////////////////

TBlobSink::TBlobSink(TBlob* output)
    : Output_(output)
{ }

void TBlobSink::Append(const char* data, size_t size)
{
    Output_->Append(data, size);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression::NDetail
