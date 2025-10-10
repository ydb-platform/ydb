#include "mkql_buffer.h"

namespace NKikimr {

namespace NMiniKQL {

static_assert(IsValidPageAllocSize(TBufferPage::DefaultPageAllocSize));

const size_t TBufferPage::DefaultPageCapacity = TBufferPage::DefaultPageAllocSize - sizeof(TBufferPage);

TBufferPage* TBufferPage::Allocate(size_t pageAllocSize) {
    void* ptr = malloc(pageAllocSize);
    if (!ptr) {
        throw std::bad_alloc();
    }
    TBufferPage* result = ::new (ptr) TBufferPage();
    return result;
}

void TBufferPage::Free(TBufferPage* page, size_t pageAllocSize) {
    Y_UNUSED(pageAllocSize);
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
            page = TBufferPage::Allocate(PageAllocSize);
            tailPage->Next_ = page;
        }
        tailPage->Size_ = TailSize_;
        ClosedPagesSize_ += TailSize_;
    } else {
        Y_DEBUG_ABORT_UNLESS(Head_ == nullptr);
        page = TBufferPage::Allocate(PageAllocSize);
        Head_ = page->Data();
    }
    TailSize_ = 0;
    Tail_ = page->Data();
}

using NYql::TChunkedBuffer;
TChunkedBuffer TPagedBuffer::AsChunkedBuffer(const TConstPtr& buffer) {
    TChunkedBuffer result;
    buffer->ForEachPage([&](const char* data, size_t size) {
        result.Append(TStringBuf(data, size), buffer);
    });

    return result;
}

} // NMiniKQL

} // NKikimr
