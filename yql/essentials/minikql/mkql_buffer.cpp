#include "mkql_buffer.h"

namespace NKikimr::NMiniKQL {

#if defined(PROFILE_MEMORY_ALLOCATIONS)
NMonitoring::TDynamicCounters::TCounterPtr TotalBytesWastedCounter;

void InitializeGlobalPagedBufferCounters(::NMonitoring::TDynamicCounterPtr root) {
    NMonitoring::TDynamicCounterPtr subGroup = root->GetSubgroup("counters", "utils")->GetSubgroup("subsystem", "mkqlalloc");
    TotalBytesWastedCounter = subGroup->GetCounter("PagedBuffer/TotalBytesWasted");
}
#endif

static_assert(IsValidPageAllocSize(TBufferPage::DefaultPageAllocSize));

const size_t TBufferPage::DefaultPageCapacity = TBufferPage::DefaultPageAllocSize - sizeof(TBufferPage);

TBufferPage* TBufferPage::Allocate(size_t pageAllocSize) {
    void* ptr = malloc(pageAllocSize);
    if (!ptr) {
        throw std::bad_alloc();
    }
    TBufferPage* result = ::new (ptr) TBufferPage();
#if defined(PROFILE_MEMORY_ALLOCATIONS)
    // After allocation the whole page capacity is wasted (PageCapacity - PageSize)
    TotalBytesWastedCounter->Add(pageAllocSize - sizeof(TBufferPage));
#endif
    return result;
}

void TBufferPage::Free(TBufferPage* page, size_t pageAllocSize) {
#if defined(PROFILE_MEMORY_ALLOCATIONS)
    // After free don't account the page's wasted (PageCapacity - PageSize)
    TotalBytesWastedCounter->Sub(pageAllocSize - sizeof(TBufferPage) - page->Size());
#else
    Y_UNUSED(pageAllocSize);
#endif
    free(page);
}

void TPagedBuffer::AppendPage() {
    TBufferPage* page = nullptr;
    if (Tail_) {
        auto tailPage = TBufferPage::GetPage(Tail_);
        if (auto next = tailPage->Next()) {
            // TODO: can we get here?
            page = next;
            page->Clear();
        } else {
            page = TBufferPage::Allocate(PageAllocSize_);
            tailPage->Next_ = page;
        }
        tailPage->Size_ = TailSize_;
        ClosedPagesSize_ += TailSize_;
    } else {
        Y_DEBUG_ABORT_UNLESS(Head_ == nullptr);
        page = TBufferPage::Allocate(PageAllocSize_);
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

} // namespace NKikimr::NMiniKQL
