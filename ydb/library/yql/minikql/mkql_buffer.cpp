#include "mkql_buffer.h"
#include "mkql_alloc.h"

namespace NKikimr {

namespace NMiniKQL {

const size_t TBufferPage::PageCapacity = TBufferPage::PageAllocSize - sizeof(TBufferPage);

TBufferPage* TBufferPage::Allocate() {
    static_assert(PageAllocSize <= std::numeric_limits<ui32>::max());
    static_assert(sizeof(TBufferPage) < PageAllocSize, "Page allocation size is too small");
    TBufferPage* result = ::new (MKQLAllocWithSize(PageAllocSize, EMemorySubPool::Temporary)) TBufferPage();
    return result;
}

void TBufferPage::Free(TBufferPage* page) {
    MKQLFreeWithSize(page, PageAllocSize, EMemorySubPool::Temporary);
}

void TPagedBuffer::AppendPage() {
    TBufferPage* page = nullptr;
    if (Tail_) {
        auto tailPage = TBufferPage::GetPage(Tail_);
        auto next = tailPage->Next();
        if (next) {
            page = next;
            page->Clear();
        } else {
            page = TBufferPage::Allocate();
            tailPage->Next_ = page;
        }
        tailPage->Size_ = TailSize_;
        ClosedPagesSize_ += TailSize_;
    } else {
        Y_VERIFY_DEBUG(Head_ == nullptr);
        page = TBufferPage::Allocate();
        Head_ = page->Data();
    }
    TailSize_ = 0;
    Tail_ = page->Data();
}

namespace {
class TPagedBufferChunk : public IContiguousChunk {
public:
    TPagedBufferChunk(const TPagedBuffer::TConstPtr& buffer, const char* data, size_t size)
        : Buffer_(buffer)
        , Data_(data)
        , Size_(size)
    {
    }

private:
    TContiguousSpan GetData() const override {
        return {Data_, Size_};
    }

    TMutableContiguousSpan GetDataMut() override {
        MKQL_ENSURE(false, "Modification of TPagedBuffer is not supported");
    }

    size_t GetOccupiedMemorySize() const override {
        return TBufferPage::PageAllocSize;
    }

    const TPagedBuffer::TConstPtr Buffer_;
    const char* const Data_;
    const size_t Size_;
};

} // namespace

TRope TPagedBuffer::AsRope(const TConstPtr& buffer) {
    TRope result;
    buffer->ForEachPage([&](const char* data, size_t size) {
        TPagedBufferChunk::TPtr chunk(new TPagedBufferChunk(buffer, data, size));
        result.Insert(result.End(), chunk);
    });

    return result;
}

} // NMiniKQL

} // NKikimr
