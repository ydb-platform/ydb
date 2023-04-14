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
    } else {
        Y_VERIFY_DEBUG(Head_ == nullptr);
        page = TBufferPage::Allocate();
        Head_ = page->Data();
    }
    TailSize_ = 0;
    Tail_ = page->Data();
}

} // NMiniKQL

} // NKikimr
