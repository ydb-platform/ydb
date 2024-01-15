#include "blob_output.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static constexpr size_t InitialBlobOutputCapacity = 16;
static constexpr double BlobOutputCapacityMultiplier = 1.5;

TBlobOutput::TBlobOutput(
    size_t capacity,
    bool pageAligned,
    TRefCountedTypeCookie tagCookie)
    : Blob_(
        tagCookie,
        /*size*/ 0,
        /*initializeStorage*/ true,
        pageAligned)
{
    Reserve(capacity);
}

size_t TBlobOutput::DoNext(void** ptr)
{
    if (Blob_.Size() == Blob_.Capacity()) {
        if (Blob_.Capacity() >= InitialBlobOutputCapacity) {
            Reserve(static_cast<size_t>(Blob_.Capacity() * BlobOutputCapacityMultiplier));
        } else {
            Reserve(InitialBlobOutputCapacity);
        }
    }
    auto previousSize = Blob_.Size();
    Blob_.Resize(Blob_.Capacity(), /*initializeStorage*/ false);
    *ptr = Blob_.Begin() + previousSize;
    return Blob_.Size() - previousSize;
}

TMutableRef TBlobOutput::RequestBuffer(size_t requiredSize)
{
    Reserve(Size() + requiredSize);
    char* data;
    size_t size = Next(&data);
    YT_VERIFY(size >= requiredSize);
    Undo(size - requiredSize);
    return TMutableRef(data, size);
}

void TBlobOutput::DoUndo(size_t len)
{
    YT_VERIFY(len <= Blob_.Size());
    Blob_.Resize(Blob_.Size() - len);
}

void TBlobOutput::DoWrite(const void* buffer, size_t length)
{
    Blob_.Append(buffer, length);
}

void TBlobOutput::Reserve(size_t capacity)
{
    Blob_.Reserve(RoundUpToPage(capacity));
}

void TBlobOutput::Clear()
{
    Blob_.Clear();
}

TSharedRef TBlobOutput::Flush()
{
    auto result = TSharedRef::FromBlob(std::move(Blob_));
    Blob_.Clear();
    return result;
}

void swap(TBlobOutput& left, TBlobOutput& right)
{
    if (&left != &right) {
        swap(left.Blob_, right.Blob_);
    }
}

TBlob& TBlobOutput::Blob()
{
    return Blob_;
}

const TBlob& TBlobOutput::Blob() const
{
    return Blob_;
}

const char* TBlobOutput::Begin() const
{
    return Blob_.Begin();
}

size_t TBlobOutput::Size() const
{
    return Blob_.Size();
}

size_t TBlobOutput::size() const
{
    return Blob_.Size();
}

size_t TBlobOutput::Capacity() const
{
    return Blob_.Capacity();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
