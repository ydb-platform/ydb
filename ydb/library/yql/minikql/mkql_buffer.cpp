#include "mkql_buffer.h"

#include <ydb/library/yql/utils/rope_over_buffer.h>

namespace NKikimr {

namespace NMiniKQL {

const size_t TBufferPage::PageCapacity = TBufferPage::PageAllocSize - sizeof(TBufferPage);

TBufferPage* TBufferPage::Allocate() {
    static_assert(PageAllocSize <= std::numeric_limits<ui32>::max());
    static_assert(sizeof(TBufferPage) < PageAllocSize, "Page allocation size is too small");
    void* ptr = malloc(PageAllocSize);
    if (!ptr) {
        throw std::bad_alloc();
    }
    TBufferPage* result = ::new (ptr) TBufferPage();
    return result;
}

void TBufferPage::Free(TBufferPage* page) {
    free(page);
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
        Y_DEBUG_ABORT_UNLESS(Head_ == nullptr);
        page = TBufferPage::Allocate();
        Head_ = page->Data();
    }
    TailSize_ = 0;
    Tail_ = page->Data();
}

TRope TPagedBuffer::AsRope(const TConstPtr& buffer) {
    TRope result;
    buffer->ForEachPage([&](const char* data, size_t size) {
        result.Insert(result.End(), NYql::MakeReadOnlyRope(buffer, data, size));
    });

    return result;
}

} // NMiniKQL

} // NKikimr
