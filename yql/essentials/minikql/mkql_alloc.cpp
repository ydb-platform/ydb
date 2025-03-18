#include "mkql_alloc.h"
#include <util/system/align.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <tuple>

namespace NKikimr {

namespace NMiniKQL {

constexpr ui64 ArrowSizeForArena = (TAllocState::POOL_PAGE_SIZE >> 2);

Y_POD_THREAD(TAllocState*) TlsAllocState;

TAllocPageHeader TAllocState::EmptyPageHeader = { 0, 0, 0, 0, nullptr, nullptr };
TAllocState::TCurrentPages TAllocState::EmptyCurrentPages = { &TAllocState::EmptyPageHeader, &TAllocState::EmptyPageHeader };

void TAllocState::TListEntry::Link(TAllocState::TListEntry* root) noexcept {
    Left = root;
    Right = root->Right;
    Right->Left = Left->Right = this;
}

void TAllocState::TListEntry::Unlink() noexcept {
    std::tie(Right->Left, Left->Right) = std::make_pair(Left, Right);
    Clear();
}

TAllocState::TAllocState(const TSourceLocation& location, const NKikimr::TAlignedPagePoolCounters &counters, bool supportsSizedAllocators)
    : TAlignedPagePool(location, counters)
    , SupportsSizedAllocators(supportsSizedAllocators)
    , CurrentPAllocList(&GlobalPAllocList)
{
    GetRoot()->InitLinks();
    OffloadedBlocksRoot.InitLinks();
    GlobalPAllocList.InitLinks();
    ArrowBlocksRoot.InitLinks();
}

void TAllocState::CleanupPAllocList(TListEntry* root) {
    for (auto curr = root->Right; curr != root; ) {
        auto next = curr->Right;
        auto size = ((TMkqlPAllocHeader*)curr)->Size;
        auto fullSize = size + sizeof(TMkqlPAllocHeader);
        MKQLFreeWithSize(curr, fullSize, EMemorySubPool::Default); // may free items from OffloadedBlocksRoot
        curr = next;
    }

    root->InitLinks();
}

void TAllocState::CleanupArrowList(TListEntry* root) {
    for (auto curr = root->Right; curr != root; ) {
        auto next = curr->Right;
#if defined(ALLOW_DEFAULT_ALLOCATOR)
        if (Y_UNLIKELY(TAllocState::IsDefaultAllocatorUsed())) {
            free(curr);
        } else {
#endif
            auto size = ((TMkqlArrowHeader*)curr)->Size;
            auto fullSize = size + sizeof(TMkqlArrowHeader);
            ReleaseAlignedPage(curr, fullSize);
#if defined(ALLOW_DEFAULT_ALLOCATOR)
        }
#endif

        curr = next;
    }

    root->InitLinks();
}

void TAllocState::KillAllBoxed() {
    {
        const auto root = GetRoot();
        for (auto next = root->GetRight(); next != root; next = root->GetLeft()) {
            next->Ref();
            next->~TBoxedValueLink();
        }

        GetRoot()->InitLinks();
    }

    {
        Y_ABORT_UNLESS(CurrentPAllocList == &GlobalPAllocList);
        CleanupPAllocList(&GlobalPAllocList);
    }

    {
        const auto root = &OffloadedBlocksRoot;
        for (auto curr = root->Right; curr != root; ) {
            auto next = curr->Right;
            free(curr);
            curr = next;
        }

        OffloadedBlocksRoot.InitLinks();
    }

    if (CurrentArrowPages) {
        MKQLArrowFree(CurrentArrowPages, 0);
        CurrentArrowPages = nullptr;
    }
    CleanupArrowList(&ArrowBlocksRoot);

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

void TAllocState::LockObject(::NKikimr::NUdf::TUnboxedValuePod value) {
    if (!UseRefLocking) {
        return;
    }

    void* obj;
    if (value.IsString()) {
       obj = value.AsStringRef().Data();
    } else if (value.IsBoxed()) {
       obj = value.AsBoxed().Get();
    } else {
       return;
    }

    auto [it, isNew] = LockedObjectsRefs.emplace(obj, TLockInfo{ 0, 0 });
    if (isNew) {
        it->second.OriginalRefs = value.LockRef();
    }

    ++it->second.Locks;
}

void TAllocState::UnlockObject(::NKikimr::NUdf::TUnboxedValuePod value) {
    if (!UseRefLocking) {
        return;
    }

    void* obj;
    if (value.IsString()) {
       obj = value.AsStringRef().Data();
    } else if (value.IsBoxed()) {
       obj = value.AsBoxed().Get();
    } else {
       return;
    }

    auto it = LockedObjectsRefs.find(obj);
    Y_ABORT_UNLESS(it != LockedObjectsRefs.end());
    if (--it->second.Locks == 0) {
       value.UnlockRef(it->second.OriginalRefs);
       LockedObjectsRefs.erase(it);
    }
}

void TScopedAlloc::Acquire() {
    if (!AttachedCount_) {
        if (PrevState_) {
            PgReleaseThreadContext(PrevState_->MainContext);
        }
        PrevState_ = TlsAllocState;
        TlsAllocState = &MyState_;
        PgAcquireThreadContext(MyState_.MainContext);
    } else {
        Y_ABORT_UNLESS(TlsAllocState == &MyState_, "Mismatch allocator in thread");

    }
    ++AttachedCount_;
}

void TScopedAlloc::Release() {
    if (AttachedCount_ && --AttachedCount_ == 0) {
        Y_ABORT_UNLESS(TlsAllocState == &MyState_, "Mismatch allocator in thread");
        PgReleaseThreadContext(MyState_.MainContext);
        TlsAllocState = PrevState_;
        if (PrevState_) {
            PgAcquireThreadContext(PrevState_->MainContext);
        }
        PrevState_ = nullptr;
    }
}

void* MKQLAllocSlow(size_t sz, TAllocState* state, const EMemorySubPool mPool) {
    auto roundedSize = AlignUp(sz + sizeof(TAllocPageHeader), MKQL_ALIGNMENT);
    auto capacity = Max(ui64(TAlignedPagePool::POOL_PAGE_SIZE), roundedSize);
    auto currPage = (TAllocPageHeader*)state->GetBlock(capacity);
    currPage->Deallocated = 0;
    currPage->Capacity = capacity;
    currPage->Offset = roundedSize;

    auto& mPage = state->CurrentPages[(TMemorySubPoolIdx)mPool];
    auto newPageAvailable = capacity - roundedSize;
    auto curPageAvailable = mPage->Capacity - mPage->Offset;

    if (newPageAvailable > curPageAvailable) {
        mPage = currPage;
    }

    void* ret = (char*)currPage + sizeof(TAllocPageHeader);
    currPage->UseCount = 1;
    currPage->MyAlloc = state;
    currPage->Link = nullptr;
    return ret;
}

void MKQLFreeSlow(TAllocPageHeader* header, TAllocState *state, const EMemorySubPool mPool) noexcept {
    Y_DEBUG_ABORT_UNLESS(state);
    Y_DEBUG_ABORT_UNLESS(header->MyAlloc == state, "%s", (TStringBuilder() << "wrong allocator was used; "
        "allocated with: " << header->MyAlloc->GetDebugInfo() << " freed with: " << TlsAllocState->GetDebugInfo()).data());
    state->ReturnBlock(header, header->Capacity);
    if (header == state->CurrentPages[(TMemorySubPoolIdx)mPool]) {
        state->CurrentPages[(TMemorySubPoolIdx)mPool] = &TAllocState::EmptyPageHeader;
    }
}

void* TPagedArena::AllocSlow(const size_t sz, const EMemorySubPool mPool) {
    auto& currentPage = CurrentPages_[(TMemorySubPoolIdx)mPool];
    auto prevLink = currentPage;
    auto roundedSize = AlignUp(sz + sizeof(TAllocPageHeader), MKQL_ALIGNMENT);
    auto capacity = Max(ui64(TAlignedPagePool::POOL_PAGE_SIZE), roundedSize);
    currentPage = (TAllocPageHeader*)PagePool_->GetBlock(capacity);
    currentPage->Capacity = capacity;
    void* ret = (char*)currentPage + sizeof(TAllocPageHeader);
    currentPage->Offset = roundedSize;
    currentPage->UseCount = 0;
    currentPage->MyAlloc = PagePool_;
    currentPage->Link = prevLink;
    return ret;
}

void TPagedArena::Clear() noexcept {
    for (auto&& i : CurrentPages_) {
        auto current = i;
        while (current != &TAllocState::EmptyPageHeader) {
            auto next = current->Link;
            PagePool_->ReturnBlock(current, current->Capacity);
            current = next;
        }

        i = &TAllocState::EmptyPageHeader;
    }
}

void* MKQLArrowAllocateOnArena(ui64 size) {
    TAllocState* state = TlsAllocState;
    Y_ENSURE(state);

    auto alignedSize = AlignUp(size, ArrowAlignment);
    auto& page = state->CurrentArrowPages;

    if (Y_UNLIKELY(!page || page->Offset + alignedSize > page->Size)) {
        const auto pageSize = TAllocState::POOL_PAGE_SIZE;

        if (state->EnableArrowTracking) {
            state->OffloadAlloc(pageSize);
        }

        if (page) {
            MKQLArrowFree(page, 0);
        }

        page = (TMkqlArrowHeader*)GetAlignedPage();
        page->Offset = sizeof(TMkqlArrowHeader);
        page->Size = pageSize;
        page->UseCount = 1;

        if (state->EnableArrowTracking) {
            page->Entry.Link(&state->ArrowBlocksRoot);
            Y_ENSURE(state->ArrowBuffers.insert(page).second);
        } else {
            page->Entry.Clear();
        }
    }

    void* ptr = (ui8*)page + page->Offset;
    page->Offset += alignedSize;
    ++page->UseCount;

    return ptr;
}

void* MKQLArrowAllocate(ui64 size) {
#if defined(ALLOW_DEFAULT_ALLOCATOR)
    if (Y_LIKELY(!TAllocState::IsDefaultAllocatorUsed())) {
#endif
        if (size <= ArrowSizeForArena) {
            return MKQLArrowAllocateOnArena(size);
        }
#if defined(ALLOW_DEFAULT_ALLOCATOR)
    }
#endif

    TAllocState* state = TlsAllocState;
    Y_ENSURE(state);
    auto fullSize = size + sizeof(TMkqlArrowHeader);
    if (state->EnableArrowTracking) {
        state->OffloadAlloc(fullSize);
    }

    void* ptr;
#if defined(ALLOW_DEFAULT_ALLOCATOR)
    if (Y_UNLIKELY(TAllocState::IsDefaultAllocatorUsed())) {
        ptr = malloc(fullSize);
        if (!ptr) {
            throw TMemoryLimitExceededException();
        }
    } else {
#endif
        ptr = GetAlignedPage(fullSize);
#if defined(ALLOW_DEFAULT_ALLOCATOR)
    }
#endif

    auto* header = (TMkqlArrowHeader*)ptr;
    header->Offset = 0;
    header->UseCount = 0;

    if (state->EnableArrowTracking) {
        header->Entry.Link(&state->ArrowBlocksRoot);
        Y_ENSURE(state->ArrowBuffers.insert(header + 1).second);
    } else {
        header->Entry.Clear();
    }

    header->Size = size;
    return header + 1;
}

void* MKQLArrowReallocate(const void* mem, ui64 prevSize, ui64 size) {
    auto res = MKQLArrowAllocate(size);
    memcpy(res, mem, Min(prevSize, size));
    MKQLArrowFree(mem, prevSize);
    return res;
}

void MKQLArrowFreeOnArena(const void* ptr) {
    auto* page = (TMkqlArrowHeader*)TAllocState::GetPageStart(ptr);
    if (page->UseCount.fetch_sub(1) == 1) {
        if (!page->Entry.IsUnlinked()) {
            TAllocState* state = TlsAllocState;
            Y_ENSURE(state);
            state->OffloadFree(page->Size);
            page->Entry.Unlink();

            auto it = state->ArrowBuffers.find(page);
            Y_ENSURE(it != state->ArrowBuffers.end());
            state->ArrowBuffers.erase(it);
        }

        ReleaseAlignedPage(page);
    }

    return;
}

void MKQLArrowFree(const void* mem, ui64 size) {
#if defined(ALLOW_DEFAULT_ALLOCATOR)
    if (Y_LIKELY(!TAllocState::IsDefaultAllocatorUsed())) {
#endif
        if (size <= ArrowSizeForArena) {
            return MKQLArrowFreeOnArena(mem);
        }
#if defined(ALLOW_DEFAULT_ALLOCATOR)
    }
#endif

    auto fullSize = size + sizeof(TMkqlArrowHeader);
    auto header = ((TMkqlArrowHeader*)mem) - 1;
    if (!header->Entry.IsUnlinked()) {
        TAllocState* state = TlsAllocState;
        Y_ENSURE(state);
        state->OffloadFree(fullSize);
        header->Entry.Unlink();
        auto it = state->ArrowBuffers.find(mem);
        Y_ENSURE(it != state->ArrowBuffers.end());
        state->ArrowBuffers.erase(it);
    }

    Y_ENSURE(size == header->Size);

#if defined(ALLOW_DEFAULT_ALLOCATOR)
    if (Y_UNLIKELY(TAllocState::IsDefaultAllocatorUsed())) {
        free(header);
        return;
    }
#endif
    ReleaseAlignedPage(header, fullSize);
}

void MKQLArrowUntrack(const void* mem, ui64 size) {
    TAllocState* state = TlsAllocState;
    Y_ENSURE(state);
    if (!state->EnableArrowTracking) {
        return;
    }

#if defined(ALLOW_DEFAULT_ALLOCATOR)
    if (Y_LIKELY(!TAllocState::IsDefaultAllocatorUsed())) {
#endif
        if (size <= ArrowSizeForArena) {
            auto* page = (TMkqlArrowHeader*)TAllocState::GetPageStart(mem);

            auto it = state->ArrowBuffers.find(page);
            if (it == state->ArrowBuffers.end()) {
                return;
            }

            if (!page->Entry.IsUnlinked()) {
                page->Entry.Unlink(); // unlink page immediately so we don't accidentally free untracked memory within `TAllocState`
                state->ArrowBuffers.erase(it);
                state->OffloadFree(page->Size);
            }

            return;
        }
#if defined(ALLOW_DEFAULT_ALLOCATOR)
    }
#endif

    auto it = state->ArrowBuffers.find(mem);
    if (it == state->ArrowBuffers.end()) {
        return;
    }

    auto* header = ((TMkqlArrowHeader*)mem) - 1;
    Y_ENSURE(header->UseCount == 0);
    if (!header->Entry.IsUnlinked()) {
        header->Entry.Unlink();
        auto fullSize = header->Size + sizeof(TMkqlArrowHeader);
        state->OffloadFree(fullSize);
        state->ArrowBuffers.erase(it);
    }
}

} // NMiniKQL

} // NKikimr
