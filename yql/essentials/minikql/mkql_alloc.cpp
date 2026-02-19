#include "mkql_alloc.h"

#include <yql/essentials/public/udf/udf_value.h>

#include <arrow/memory_pool.h>

#include <util/system/align.h>
#include <util/generic/scope.h>

#include <tuple>

namespace NKikimr::NMiniKQL {

namespace {

// NOLINTNEXTLINE(modernize-avoid-c-arrays)
ui8 ZeroSizeObject alignas(ArrowAlignment)[0];

} // namespace

constexpr ui64 ArrowSizeForArena = (TAllocState::POOL_PAGE_SIZE >> 2);

Y_POD_THREAD(TAllocState*) TlsAllocState;

TAllocPageHeader TAllocState::EmptyPageHeader = {0, 0, 0, 0, nullptr, nullptr};
TAllocState::TCurrentPages TAllocState::EmptyCurrentPages = {&TAllocState::EmptyPageHeader, &TAllocState::EmptyPageHeader};

void TAllocState::TListEntry::Link(TAllocState::TListEntry* root) noexcept {
    Left = root;
    Right = root->Right;
    Right->Left = Left->Right = this;
}

void TAllocState::TListEntry::Unlink() noexcept {
    std::tie(Right->Left, Left->Right) = std::make_pair(Left, Right);
    Clear();
}

TAllocState::TAllocState(const TSourceLocation& location, const NKikimr::TAlignedPagePoolCounters& counters, bool supportsSizedAllocators)
    : TAlignedPagePool(location, counters)
#ifndef NDEBUG
    , DefaultMemInfo(MakeIntrusive<TMemoryUsageInfo>("default"))
#endif
    , SupportsSizedAllocators(supportsSizedAllocators)
    , CurrentPAllocList(&GlobalPAllocList)
{
#ifndef NDEBUG
    ActiveMemInfo.emplace(DefaultMemInfo.Get(), DefaultMemInfo);
#endif
    GetRoot()->InitLinks();
    OffloadedBlocksRoot.InitLinks();
    GlobalPAllocList.InitLinks();
    ArrowBlocksRoot.InitLinks();
}

void TAllocState::CleanupPAllocList(TListEntry* root) {
    for (auto curr = root->Right; curr != root;) {
        auto next = curr->Right;
        auto size = ((TMkqlPAllocHeader*)curr)->Size;
        auto fullSize = size + sizeof(TMkqlPAllocHeader);
        MKQLFreeWithSize(curr, fullSize, EMemorySubPool::Default); // may free items from OffloadedBlocksRoot
        curr = next;
    }

    root->InitLinks();
}

void TAllocState::CleanupArrowList(TListEntry* root) {
    for (auto curr = root->Right; curr != root;) {
        auto next = curr->Right;
        if (Y_UNLIKELY(TAllocState::IsDefaultAllocatorUsed())) {
            free(curr);
        } else {
            auto size = ((TMkqlArrowHeader*)curr)->Size;
            auto fullSize = size + sizeof(TMkqlArrowHeader);
            ReleaseAlignedPage(curr, fullSize);
        }

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
        for (auto curr = root->Right; curr != root;) {
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

Y_NO_SANITIZE("address") Y_NO_SANITIZE("memory")
size_t
TAllocState::GetDeallocatedInPages() const {
    size_t deallocated = 0;
    for (auto x : AllPages_) {
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

    auto [it, isNew] = LockedObjectsRefs.emplace(obj, TLockInfo{0, 0});
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
    NYql::NUdf::SanitizerMakeRegionAccessible(currPage, sizeof(TAllocPageHeader));
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

void MKQLFreeSlow(TAllocPageHeader* header, TAllocState* state, const EMemorySubPool mPool) noexcept {
    Y_DEBUG_ABORT_UNLESS(state);
    Y_DEBUG_ABORT_UNLESS(header->MyAlloc == state, "%s", (TStringBuilder() << "wrong allocator was used; "
                                                                              "allocated with: "
                                                                           << header->MyAlloc->GetDebugInfo() << " freed with: " << TlsAllocState->GetDebugInfo())
                                                             .data());
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
    NYql::NUdf::SanitizerMakeRegionAccessible(currentPage, sizeof(TAllocPageHeader));
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

namespace {

void* MKQLArrowAllocateOnArena(ui64 size) {
    Y_ENSURE(size);
    // If size is zero we can get in trouble: when `page->Offset == page->Size`.
    // The zero size leads to return `ptr` just after the current page.
    // Then getting start of page for such pointer returns next page - which may be unmapped or unrelevant to `ptr`.

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
        NYql::NUdf::SanitizerMakeRegionAccessible(page, sizeof(TMkqlArrowHeader));
        page->Offset = 0;
        page->Size = pageSize - sizeof(TMkqlArrowHeader); // for consistency with CleanupArrowList()
        page->UseCount = 1;

        if (state->EnableArrowTracking) {
            page->Entry.Link(&state->ArrowBlocksRoot);
            Y_ENSURE(state->ArrowBuffers.insert(page).second);
        } else {
            page->Entry.Clear();
        }
    }

    void* ptr = (ui8*)page + page->Offset + sizeof(TMkqlArrowHeader);
    page->Offset += alignedSize;
    ++page->UseCount;

    Y_DEBUG_ABORT_UNLESS(TAllocState::GetPageStart(ptr) == page);

    return ptr;
}

void* MKQLArrowAllocateImpl(ui64 size) {
    if (Y_UNLIKELY(size == 0)) {
        return reinterpret_cast<void*>(ZeroSizeObject);
    }

    if (!TAllocState::IsDefaultArrowAllocatorUsed()) {
        if (size <= ArrowSizeForArena) {
            return MKQLArrowAllocateOnArena(size);
        }
    }

    TAllocState* state = TlsAllocState;
    Y_ENSURE(state);
    auto fullSize = size + sizeof(TMkqlArrowHeader);
    if (state->EnableArrowTracking) {
        state->OffloadAlloc(fullSize);
    }

    void* ptr;
    if (TAllocState::IsDefaultArrowAllocatorUsed()) {
        auto pool = arrow::default_memory_pool();
        Y_ENSURE(pool);
        uint8_t* res;
        if (!pool->Allocate(fullSize, &res).ok()) {
            // NOLINTNEXTLINE(hicpp-exception-baseclass)
            throw TMemoryLimitExceededException();
        }
        Y_ENSURE(res);
        ptr = res;
    } else {
        ptr = GetAlignedPage(fullSize);
    }

    auto* header = (TMkqlArrowHeader*)ptr;
    NYql::NUdf::SanitizerMakeRegionAccessible(header, sizeof(TMkqlArrowHeader));
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

void MKQLArrowFreeOnArena(const void* ptr) {
    auto* page = (TMkqlArrowHeader*)TAllocState::GetPageStart(ptr);
    if (page->UseCount.fetch_sub(1) == 1) {
        if (!page->Entry.IsUnlinked()) {
            TAllocState* state = TlsAllocState;
            Y_ENSURE(state);
            state->OffloadFree(page->Size + sizeof(TMkqlArrowHeader));
            page->Entry.Unlink();

            auto it = state->ArrowBuffers.find(page);
            Y_ENSURE(it != state->ArrowBuffers.end());
            state->ArrowBuffers.erase(it);
        }
        NYql::NUdf::SanitizerMakeRegionInaccessible(page, sizeof(TMkqlArrowHeader));
        ReleaseAlignedPage(page);
    }

    return;
}

void MKQLArrowFreeImpl(const void* mem, ui64 size) {
    if (Y_UNLIKELY(mem == reinterpret_cast<const void*>(ZeroSizeObject))) {
        Y_DEBUG_ABORT_UNLESS(size == 0);
        return;
    }

    if (!TAllocState::IsDefaultArrowAllocatorUsed()) {
        if (size <= ArrowSizeForArena) {
            return MKQLArrowFreeOnArena(mem);
        }
    }

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

    if (TAllocState::IsDefaultArrowAllocatorUsed()) {
        auto pool = arrow::default_memory_pool();
        Y_ABORT_UNLESS(pool);
        pool->Free(reinterpret_cast<uint8_t*>(header), static_cast<int64_t>(fullSize));
        return;
    }

    ReleaseAlignedPage(header, fullSize);
}

} // namespace

void* MKQLArrowAllocate(ui64 size) {
    auto sizeWithRedzones = NYql::NUdf::GetSizeToAlloc(size);
    void* mem = MKQLArrowAllocateImpl(sizeWithRedzones);
    return NYql::NUdf::WrapPointerWithRedZones(mem, sizeWithRedzones);
}

void* MKQLArrowReallocate(const void* mem, ui64 prevSize, ui64 size) {
    auto res = MKQLArrowAllocate(size);
    memcpy(res, mem, Min(prevSize, size));
    MKQLArrowFree(mem, prevSize);
    return res;
}

void MKQLArrowFree(const void* mem, ui64 size) {
    mem = NYql::NUdf::UnwrapPointerWithRedZones(mem, size);
    auto sizeWithRedzones = NYql::NUdf::GetSizeToAlloc(size);
    return MKQLArrowFreeImpl(mem, sizeWithRedzones);
}

void MKQLArrowUntrack(const void* mem) {
    // NOTE: we expect the `mem` size to be non-zero, unless it's an explicitly allocated zero size object.
    if (Y_UNLIKELY(mem == reinterpret_cast<const void*>(ZeroSizeObject))) {
        return;
    }

    mem = NYql::NUdf::GetOriginalAllocatedObject(mem);
    TAllocState* state = TlsAllocState;
    Y_ENSURE(state);
    if (!state->EnableArrowTracking) {
        return;
    }

    // NOTE: Check original pointer first and only then check for an arena page.
    // There is a special case of class `arrow::ImportedBuffer` which is used to transfer buffers across .so boundaries (i.e. UDFs),
    // this buffer shrinks original capacity and if it's too small it may wrongly choose the branch for arena page untracking.
    auto it = state->ArrowBuffers.find(mem);
    if (it == state->ArrowBuffers.end()) {
        if (!TAllocState::IsDefaultArrowAllocatorUsed()) {
            auto* page = (TMkqlArrowHeader*)TAllocState::GetPageStart(mem);

            auto it = state->ArrowBuffers.find(page);
            if (it == state->ArrowBuffers.end()) {
                return;
            }

            if (!page->Entry.IsUnlinked()) {
                page->Entry.Unlink(); // unlink page immediately so we don't accidentally free untracked memory within `TAllocState`
                state->ArrowBuffers.erase(it);
                state->OffloadFree(page->Size + sizeof(TMkqlArrowHeader));
            }
        }

        return;
    }

    // If original pointer is found among buffers then it's definitely a non-arena page,
    // because arena pages are stored by the page-start pointer.

    auto* header = ((TMkqlArrowHeader*)mem) - 1;
    Y_ENSURE(header->UseCount == 0);
    if (!header->Entry.IsUnlinked()) {
        header->Entry.Unlink();
        auto fullSize = header->Size + sizeof(TMkqlArrowHeader);
        state->OffloadFree(fullSize);
        state->ArrowBuffers.erase(it);
    }
}

} // namespace NKikimr::NMiniKQL
