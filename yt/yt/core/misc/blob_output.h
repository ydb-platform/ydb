#pragma once

#include "blob.h"

#include <util/stream/zerocopy_output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TBlobOutputTag
{ };

class TBlobOutput
    : public IZeroCopyOutput
{
public:
    explicit TBlobOutput(
        size_t capacity = 0,
        bool pageAligned = false,
        TRefCountedTypeCookie tagCookie = GetRefCountedTypeCookie<TBlobOutputTag>());

    TBlob& Blob();
    const TBlob& Blob() const;

    const char* Begin() const;
    size_t Size() const;
    size_t size() const;
    size_t Capacity() const;

    void Reserve(size_t capacity);
    void Clear();
    TSharedRef Flush();

    TMutableRef RequestBuffer(size_t requiredSize);

    friend void swap(TBlobOutput& left, TBlobOutput& right);

private:
    size_t DoNext(void** ptr) override;
    void DoUndo(size_t len) override;
    void DoWrite(const void* buf, size_t len) override;

    TBlob Blob_;
};

void swap(TBlobOutput& left, TBlobOutput& right);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
