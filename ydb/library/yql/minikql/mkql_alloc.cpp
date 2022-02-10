#include "mkql_alloc.h" 
#include <util/system/align.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <tuple>
 
namespace NKikimr { 
 
namespace NMiniKQL { 
 
Y_POD_THREAD(TAllocState*) TlsAllocState;

TAllocPageHeader TAllocState::EmptyPageHeader = { 0, 0, 0, 0, 0, nullptr };

void TAllocState::TListEntry::Link(TAllocState::TListEntry* root) noexcept {
    Left = root;
    Right = root->Right;
    Right->Left = Left->Right = this;
}

void TAllocState::TListEntry::Unlink() noexcept {
    std::tie(Right->Left, Left->Right) = std::make_pair(Left, Right);
    Left = Right = nullptr;
}

TAllocState::TAllocState(const NKikimr::TAlignedPagePoolCounters &counters, bool supportsSizedAllocators)
    : TAlignedPagePool(counters)
    , SupportsSizedAllocators(supportsSizedAllocators)
{
    GetRoot()->InitLinks();
    OffloadedBlocksRoot.InitLinks();
}

void TAllocState::KillAllBoxed() {
    {
        const auto root = GetRoot();
        for (auto next = root->GetRight(); next != root; next = root->GetLeft()) {
            next->Ref();
            next->~TBoxedValueLink();
        }
    }

    {
        const auto root = &OffloadedBlocksRoot;
        for (auto curr = root->Right; curr != root; ) {
            auto next = curr->Right;
            free(curr);
            curr = next;
        }
    }

#ifndef NDEBUG
    ActiveMemInfo.clear();
#endif
}

void TAllocState::InvalidateMemInfo() {
#ifndef NDEBUG
    for (auto& pair : ActiveMemInfo) {
        pair.first->CheckOnExit(false);
    }
#endif
}

size_t TAllocState::GetDeallocatedInPages() const {
    size_t deallocated = 0;
    for (auto x : AllPages) {
        auto currPage = (TAllocPageHeader*)x;
        if (currPage->UseCount) {
            deallocated += currPage->Deallocated;
        }
    }

    return deallocated;
}

void* MKQLAllocSlow(size_t sz, TAllocState* state) {
    auto roundedSize = AlignUp(sz + sizeof(TAllocPageHeader), MKQL_ALIGNMENT);
    auto capacity = Max(ui64(TAlignedPagePool::POOL_PAGE_SIZE), roundedSize);
    auto currPage = (TAllocPageHeader*)state->GetBlock(capacity);
    currPage->Deallocated = 0;
    currPage->Capacity = capacity;
    currPage->Offset = roundedSize;

    auto newPageAvailable = capacity - roundedSize;
    auto curPageAvailable = state->CurrentPage->Capacity - state->CurrentPage->Offset;

    if (newPageAvailable > curPageAvailable) {
        state->CurrentPage = currPage;
    }

    void* ret = (char*)currPage + sizeof(TAllocPageHeader);
    currPage->UseCount = 1;
    currPage->Link = nullptr;
    return ret;
}

void MKQLFreeSlow(TAllocPageHeader* header) noexcept {
    TAllocState *state = TlsAllocState;
    Y_VERIFY_DEBUG(state);
    state->ReturnBlock(header, header->Capacity);
    if (header == state->CurrentPage) {
        state->CurrentPage = &TAllocState::EmptyPageHeader;
    }
}

void* TPagedArena::AllocSlow(size_t sz) {
    auto prevLink = CurrentPage_;
    auto roundedSize = AlignUp(sz + sizeof(TAllocPageHeader), MKQL_ALIGNMENT);
    auto capacity = Max(ui64(TAlignedPagePool::POOL_PAGE_SIZE), roundedSize);
    CurrentPage_ = (TAllocPageHeader*)PagePool_->GetBlock(capacity);
    CurrentPage_->Capacity = capacity;
    void* ret = (char*)CurrentPage_ + sizeof(TAllocPageHeader);
    CurrentPage_->Offset = roundedSize;
    CurrentPage_->UseCount = 0;
    CurrentPage_->Link = prevLink;
    return ret;
}

void TPagedArena::Clear() noexcept {
    auto current = CurrentPage_;
    while (current != &TAllocState::EmptyPageHeader) {
        auto next = current->Link;
        PagePool_->ReturnBlock(current, current->Capacity);
        current = next;
    }

    CurrentPage_ = &TAllocState::EmptyPageHeader;
}

} // NMiniKQL 
 
} // NKikimr 
