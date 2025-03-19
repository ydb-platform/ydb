#include "mkql_buffer.h"

namespace NKikimr::NMiniKQL {

    NMonitoring::TDynamicCounters::TCounterPtr PageBufferBytesWastedCounter;

    void InitializeGlobalPagedBufferCounters(::NMonitoring::TDynamicCounterPtr root) {
        NMonitoring::TDynamicCounterPtr subGroup = root->GetSubgroup("counters", "utils")->GetSubgroup("subsystem", "mkqlalloc");
        PageBufferBytesWastedCounter = subGroup->GetCounter("PagedBuffer/TotalBytesWasted");
    }

const size_t TBufferPage::PageCapacity = TBufferPage::PageAllocSize - sizeof(TBufferPage);

TBufferPage* TBufferPage::Allocate() {
    static_assert(PageAllocSize <= std::numeric_limits<ui32>::max());
    static_assert(sizeof(TBufferPage) < PageAllocSize, "Page allocation size is too small");
    void* ptr = malloc(PageAllocSize);
    if (!ptr) {
        throw std::bad_alloc();
    }
    TBufferPage* result = ::new (ptr) TBufferPage();
    (*PageBufferBytesWastedCounter) += result->Wasted();
    return result;
}

void TBufferPage::Free(TBufferPage* page) {
    (*PageBufferBytesWastedCounter) -= page->Wasted();
    Y_ABORT_UNLESS(*PageBufferBytesWastedCounter >= 0, "Total wasted vs page wasted: %ld, %ld", PageBufferBytesWastedCounter->Val(), page->Wasted());
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
            page = TBufferPage::Allocate();
            tailPage->Next_ = page;
        }
        tailPage->Size_ = TailSize_;
        ClosedPagesSize_ += TailSize_;
    } else {
        // TODO: custom first page size
        Y_DEBUG_ABORT_UNLESS(Head_ == nullptr);
        page = TBufferPage::Allocate();
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
