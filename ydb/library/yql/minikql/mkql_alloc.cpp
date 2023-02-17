#include "mkql_alloc.h"
#include <util/system/align.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <tuple>

namespace NKikimr {

namespace NMiniKQL {

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
    Left = Right = nullptr;
}

TAllocState::TAllocState(const TSourceLocation& location, const NKikimr::TAlignedPagePoolCounters &counters, bool supportsSizedAllocators)
    : TAlignedPagePool(location, counters)
    , SupportsSizedAllocators(supportsSizedAllocators)
    , CurrentPAllocList(&GlobalPAllocList)
{
    GetRoot()->InitLinks();
    OffloadedBlocksRoot.InitLinks();
    GlobalPAllocList.InitLinks();
}

void TAllocState::CleanupPAllocList(TListEntry* root) {
    for (auto curr = root->Right; curr != root; ) {
        auto next = curr->Right;
        MKQLFreeDeprecated(curr); // may free items from OffloadedBlocksRoot
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
        Y_VERIFY(CurrentPAllocList == &GlobalPAllocList);
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
    Y_VERIFY(it != LockedObjectsRefs.end());
    if (--it->second.Locks == 0) {
       value.UnlockRef(it->second.OriginalRefs);
       LockedObjectsRefs.erase(it);
    }
}

void TScopedAlloc::Acquire() {
    if (!AttachedCount_) {
        PrevState_ = TlsAllocState;
        TlsAllocState = &MyState_;
        PgAcquireThreadContext(MyState_.MainContext);
    } else {
        Y_VERIFY(TlsAllocState == &MyState_, "Mismatch allocator in thread");
    }

    ++AttachedCount_;
}

void TScopedAlloc::Release() {
    if (AttachedCount_ && --AttachedCount_ == 0) {
        Y_VERIFY(TlsAllocState == &MyState_, "Mismatch allocator in thread");
        PgReleaseThreadContext(MyState_.MainContext);
        TlsAllocState = PrevState_;
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
    Y_VERIFY_DEBUG(state);
    Y_VERIFY_DEBUG(header->MyAlloc == state, "%s", (TStringBuilder() << "wrong allocator was used; "
        "allocated with: " << header->MyAlloc->GetInfo() << " freed with: " << TlsAllocState->GetInfo()).data());
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

} // NMiniKQL

} // NKikimr
