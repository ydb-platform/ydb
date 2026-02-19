#pragma once

#include "aligned_page_pool.h"
#include "mkql_mem_info.h"

#include <yql/essentials/core/pg_settings/guc_settings.h>
#include <yql/essentials/public/udf/sanitizer_utils.h>
#include <yql/essentials/parser/pg_wrapper/interface/context.h>
#include <yql/essentials/public/udf/udf_allocator.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/string/builder.h>
#include <util/system/align.h>
#include <util/system/defaults.h>
#include <util/system/tls.h>
#include <util/generic/scope.h>

#include <unordered_map>
#include <atomic>
#include <memory>
#include <source_location>

namespace NKikimr::NMiniKQL {

const ui64 MKQL_ALIGNMENT = 16;

struct TAllocPageHeader {
    ui64 Capacity;
    ui64 Offset;
    ui64 UseCount;
    ui64 Deallocated;
    TAlignedPagePool* MyAlloc;
    TAllocPageHeader* Link;
};

using TMemorySubPoolIdx = ui32;
enum class EMemorySubPool: TMemorySubPoolIdx {
    Default = 0,
    Temporary = 1,

    Count
};

constexpr ui32 MaxPageUserData = TAlignedPagePool::POOL_PAGE_SIZE - sizeof(TAllocPageHeader);

static_assert(sizeof(TAllocPageHeader) % MKQL_ALIGNMENT == 0, "Incorrect size of header");

struct TMkqlArrowHeader;

#ifndef NDEBUG
using TAllocLocation = std::source_location;
#else
struct TAllocLocation {
    const char* file_name() const {
        return "";
    }

    std::uint_least32_t line() const {
        return 0;
    }

    static TAllocLocation current() {
        return {};
    }
};
#endif

struct TAllocState: public TAlignedPagePool {
    struct TListEntry {
        TListEntry* Left = nullptr;
        TListEntry* Right = nullptr;

        void Link(TListEntry* root) noexcept;
        void Unlink() noexcept;
        void InitLinks() noexcept {
            Left = Right = this;
        }
        void Clear() noexcept {
            Left = Right = nullptr;
        }
        bool IsUnlinked() const noexcept {
            return !Left && !Right;
        }
    };

#ifndef NDEBUG
    TIntrusivePtr<TMemoryUsageInfo> DefaultMemInfo;
    std::unordered_map<TMemoryUsageInfo*, TIntrusivePtr<TMemoryUsageInfo>> ActiveMemInfo;
#endif
    bool SupportsSizedAllocators = false;

    using TCurrentPages = std::array<TAllocPageHeader*, (TMemorySubPoolIdx)EMemorySubPool::Count>;

    static TAllocPageHeader EmptyPageHeader;
    static TCurrentPages EmptyCurrentPages;

    std::array<TAllocPageHeader*, (TMemorySubPoolIdx)EMemorySubPool::Count> CurrentPages = EmptyCurrentPages;
    TListEntry OffloadedBlocksRoot;
    TListEntry GlobalPAllocList;
    TListEntry* CurrentPAllocList;
    TListEntry ArrowBlocksRoot;
    TMkqlArrowHeader* CurrentArrowPages = nullptr; // page arena for small arrow allocations
    std::unordered_set<const void*> ArrowBuffers;
    bool EnableArrowTracking = true;

    void* MainContext = nullptr;
    void* CurrentContext = nullptr;

    struct TLockInfo {
        i32 OriginalRefs;
        i32 Locks;
    };

    bool UseRefLocking = false;
    std::unordered_map<void*, TLockInfo> LockedObjectsRefs;

    ::NKikimr::NUdf::TBoxedValueLink Root;

    NKikimr::NUdf::TBoxedValueLink* GetRoot() noexcept {
        return &Root;
    }

    explicit TAllocState(const TSourceLocation& location, const TAlignedPagePoolCounters& counters, bool supportsSizedAllocators);
    void KillAllBoxed();
    void InvalidateMemInfo();
    Y_NO_SANITIZE("address") Y_NO_SANITIZE("memory") size_t GetDeallocatedInPages() const;
    static void CleanupPAllocList(TListEntry* root);
    static void CleanupArrowList(TListEntry* root);

    void LockObject(::NKikimr::NUdf::TUnboxedValuePod value);
    void UnlockObject(::NKikimr::NUdf::TUnboxedValuePod value);
};

extern Y_POD_THREAD(TAllocState*) TlsAllocState;

class TPAllocScope {
public:
    TPAllocScope() {
        PAllocList_.InitLinks();
        Attach();
    }

    ~TPAllocScope() {
        Cleanup();
        Detach();
    }

    void Attach() {
        Y_ABORT_UNLESS(!Prev_);
        Prev_ = TlsAllocState->CurrentPAllocList;
        Y_ABORT_UNLESS(Prev_);
        TlsAllocState->CurrentPAllocList = &PAllocList_;
    }

    void Detach() {
        if (Prev_) {
            Y_ABORT_UNLESS(TlsAllocState->CurrentPAllocList == &PAllocList_);
            TlsAllocState->CurrentPAllocList = Prev_;
            Prev_ = nullptr;
        }
    }

    void Cleanup() {
        TAllocState::CleanupPAllocList(&PAllocList_);
    }

private:
    TAllocState::TListEntry PAllocList_;
    TAllocState::TListEntry* Prev_ = nullptr;
};

// TListEntry and IBoxedValue use the same place
static_assert(sizeof(NUdf::IBoxedValue) == sizeof(TAllocState::TListEntry));

class TBoxedValueWithFree: public NUdf::TBoxedValueBase {
public:
    void operator delete(void* mem) noexcept;
};

struct TMkqlPAllocHeader {
    union {
        TAllocState::TListEntry Entry;
        TBoxedValueWithFree Boxed;
    } U;

    size_t Size;
    ui64 Self; // should be placed right before pointer to allocated area, see GetMemoryChunkContext
};

static_assert(sizeof(TMkqlPAllocHeader) ==
                  sizeof(size_t) +
                      sizeof(TAllocState::TListEntry) +
                      sizeof(void*), "Padding is not allowed");

constexpr size_t ArrowAlignment = 64;
struct TMkqlArrowHeader {
    TAllocState::TListEntry Entry;
    ui64 Size;
    ui64 Offset;
    std::atomic<ui64> UseCount;
    std::array<
        char,
        ArrowAlignment -
            sizeof(TAllocState::TListEntry) -
            sizeof(ui64) -
            sizeof(ui64) -
            sizeof(std::atomic<ui64>)>
        Padding;
};

static_assert(sizeof(TMkqlArrowHeader) == ArrowAlignment);

class TScopedAlloc {
public:
    explicit TScopedAlloc(const TSourceLocation& location,
                          const TAlignedPagePoolCounters& counters = TAlignedPagePoolCounters(), bool supportsSizedAllocators = false, bool initiallyAcquired = true)
        : InitiallyAcquired_(initiallyAcquired)
        , MyState_(location, counters, supportsSizedAllocators)
    {
        MyState_.MainContext = PgInitializeMainContext();
        if (InitiallyAcquired_) {
            Acquire();
        }
    }

    ~TScopedAlloc()
    {
        if (!InitiallyAcquired_) {
            Acquire();
        }
        MyState_.KillAllBoxed();
        Release();
        PgDestroyMainContext(MyState_.MainContext);
    }

    TAllocState& Ref() {
        return MyState_;
    }

    void Acquire();
    void Release();

    size_t GetUsed() const {
        return MyState_.GetUsed();
    }
    size_t GetPeakUsed() const {
        return MyState_.GetPeakUsed();
    }
    size_t GetAllocated() const {
        return MyState_.GetAllocated();
    }
    size_t GetPeakAllocated() const {
        return MyState_.GetPeakAllocated();
    }

    size_t GetLimit() const {
        return MyState_.GetLimit();
    }
    void SetLimit(size_t limit) {
        MyState_.SetLimit(limit);
    }
    void DisableStrictAllocationCheck() {
        MyState_.DisableStrictAllocationCheck();
    }

    void ReleaseFreePages() {
        MyState_.ReleaseFreePages();
    }
    void InvalidateMemInfo() {
        MyState_.InvalidateMemInfo();
    }

    bool IsAttached() const {
        return AttachedCount_ > 0;
    }

    void SetGUCSettings(const TGUCSettings::TPtr& GUCSettings) {
        Acquire();
        PgSetGUCSettings(MyState_.MainContext, GUCSettings);
        Release();
    }

    void SetMaximumLimitValueReached(bool IsReached) {
        MyState_.SetMaximumLimitValueReached(IsReached);
    }

private:
    const bool InitiallyAcquired_;
    TAllocState MyState_;
    size_t AttachedCount_ = 0;
    TAllocState* PrevState_ = nullptr;
};

class TPagedArena {
public:
    explicit TPagedArena(TAlignedPagePool* pagePool) noexcept
        : PagePool_(pagePool)
        , CurrentPages_(TAllocState::EmptyCurrentPages)
    {
    }

    TPagedArena(const TPagedArena&) = delete;
    TPagedArena(TPagedArena&& other) noexcept
        : PagePool_(other.PagePool_)
        , CurrentPages_(other.CurrentPages_)
    {
        other.CurrentPages_ = TAllocState::EmptyCurrentPages;
    }

    TPagedArena& operator=(const TPagedArena&) = delete;
    TPagedArena& operator=(TPagedArena&& other) noexcept {
        Clear();
        PagePool_ = other.PagePool_;
        CurrentPages_ = other.CurrentPages_;
        other.CurrentPages_ = TAllocState::EmptyCurrentPages;
        return *this;
    }

    ~TPagedArena() noexcept {
        Clear();
    }

    void* AllocImpl(size_t sz, const EMemorySubPool pagePool) {
        auto& currentPage = CurrentPages_[(TMemorySubPoolIdx)pagePool];
        if (Y_LIKELY(currentPage->Offset + sz <= currentPage->Capacity)) {
            void* ret = (char*)currentPage + currentPage->Offset;
            currentPage->Offset = AlignUp(currentPage->Offset + sz, MKQL_ALIGNMENT);
            return ret;
        }
        return AllocSlow(sz, pagePool);
    }

    void* Alloc(size_t sz, const EMemorySubPool pagePool = EMemorySubPool::Default) {
        sz = NYql::NUdf::GetSizeToAlloc(sz);
        void* mem = AllocImpl(sz, pagePool);
        return NYql::NUdf::WrapPointerWithRedZones(mem, sz);
    }

    void Clear() noexcept;

private:
    void* AllocSlow(const size_t sz, const EMemorySubPool pagePool);

private:
    TAlignedPagePool* PagePool_;
    TAllocState::TCurrentPages CurrentPages_ = TAllocState::EmptyCurrentPages;
};

void* MKQLAllocSlow(size_t sz, TAllocState* state, const EMemorySubPool mPool);

inline void* MKQLAllocFastDeprecated(size_t sz, TAllocState* state, const EMemorySubPool mPool, const TAllocLocation& location = TAllocLocation::current()) {
#ifdef NDEBUG
    Y_UNUSED(location);
#endif
    Y_DEBUG_ABORT_UNLESS(state);

    if (Y_UNLIKELY(TAllocState::IsDefaultAllocatorUsed())) {
        auto ret = (TAllocState::TListEntry*)malloc(sizeof(TAllocState::TListEntry) + sz);
        if (!ret) {
            // NOLINTNEXTLINE(hicpp-exception-baseclass)
            throw TMemoryLimitExceededException();
        }

        ret->Link(&state->OffloadedBlocksRoot);
#ifndef NDEBUG
        state->DefaultMemInfo->Take(ret + 1, sz, {location.file_name(), (int)location.line()});
#endif
        return ret + 1;
    }

    auto currPage = state->CurrentPages[(TMemorySubPoolIdx)mPool];
    if (Y_LIKELY(currPage->Offset + sz <= currPage->Capacity)) {
        void* ret = (char*)currPage + currPage->Offset;
        currPage->Offset = AlignUp(currPage->Offset + sz, MKQL_ALIGNMENT);
        ++currPage->UseCount;
#ifndef NDEBUG
        state->DefaultMemInfo->Take(ret, sz, {location.file_name(), (int)location.line()});
#endif
        return ret;
    }

    auto ret = MKQLAllocSlow(sz, state, mPool);
#ifndef NDEBUG
    state->DefaultMemInfo->Take(ret, sz, {location.file_name(), (int)location.line()});
#endif
    return ret;
}

inline void* MKQLAllocFastWithSizeImpl(size_t sz, TAllocState* state, const EMemorySubPool mPool, const TAllocLocation& location) {
#ifdef NDEBUG
    Y_UNUSED(location);
#endif
    Y_DEBUG_ABORT_UNLESS(state);

    bool useMalloc = (state->SupportsSizedAllocators && sz > MaxPageUserData) || TAllocState::IsDefaultAllocatorUsed();

    if (Y_UNLIKELY(useMalloc)) {
        state->OffloadAlloc(sizeof(TAllocState::TListEntry) + sz);
        auto ret = (TAllocState::TListEntry*)malloc(sizeof(TAllocState::TListEntry) + sz);
        if (!ret) {
            // NOLINTNEXTLINE(hicpp-exception-baseclass)
            throw TMemoryLimitExceededException();
        }

        ret->Link(&state->OffloadedBlocksRoot);
#ifndef NDEBUG
        state->DefaultMemInfo->Take(ret + 1, sz, {location.file_name(), (int)location.line()});
#endif
        return ret + 1;
    }

    auto currPage = state->CurrentPages[(TMemorySubPoolIdx)mPool];
    if (Y_LIKELY(currPage->Offset + sz <= currPage->Capacity)) {
        void* ret = (char*)currPage + currPage->Offset;
        currPage->Offset = AlignUp(currPage->Offset + sz, MKQL_ALIGNMENT);
        ++currPage->UseCount;
#ifndef NDEBUG
        state->DefaultMemInfo->Take(ret, sz, {location.file_name(), (int)location.line()});
#endif
        return ret;
    }

    auto ret = MKQLAllocSlow(sz, state, mPool);
#ifndef NDEBUG
    state->DefaultMemInfo->Take(ret, sz, {location.file_name(), (int)location.line()});
#endif
    return ret;
}

inline void* MKQLAllocFastWithSize(size_t sz, TAllocState* state, const EMemorySubPool mPool, const TAllocLocation& location = TAllocLocation::current()) {
    sz = NYql::NUdf::GetSizeToAlloc(sz);
    void* mem = MKQLAllocFastWithSizeImpl(sz, state, mPool, location);
    return NYql::NUdf::WrapPointerWithRedZones(mem, sz);
}

void MKQLFreeSlow(TAllocPageHeader* header, TAllocState* state, const EMemorySubPool mPool) noexcept;

inline void MKQLFreeDeprecated(const void* mem, const EMemorySubPool mPool) noexcept {
    if (!mem) {
        return;
    }

#ifndef NDEBUG
    TlsAllocState->DefaultMemInfo->Return(mem);
#endif

    if (Y_UNLIKELY(TAllocState::IsDefaultAllocatorUsed())) {
        TAllocState* state = TlsAllocState;
        Y_DEBUG_ABORT_UNLESS(state);

        auto entry = (TAllocState::TListEntry*)(mem)-1;
        entry->Unlink();
        free(entry);
        return;
    }

    TAllocPageHeader* header = (TAllocPageHeader*)TAllocState::GetPageStart(mem);
    Y_DEBUG_ABORT_UNLESS(header->MyAlloc == TlsAllocState, "%s", (TStringBuilder() << "wrong allocator was used; "
                                                                                      "allocated with: "
                                                                                   << header->MyAlloc->GetDebugInfo() << " freed with: " << TlsAllocState->GetDebugInfo())
                                                                     .data());
    if (Y_LIKELY(--header->UseCount != 0)) {
        return;
    }

    MKQLFreeSlow(header, TlsAllocState, mPool);
}

inline void MKQLFreeFastWithSizeImpl(const void* mem, size_t sz, TAllocState* state, const EMemorySubPool mPool) noexcept {
    if (!mem) {
        return;
    }

    Y_DEBUG_ABORT_UNLESS(state);
#ifndef NDEBUG
    state->DefaultMemInfo->Return(mem, sz);
#endif

    bool useFree = (state->SupportsSizedAllocators && sz > MaxPageUserData) || TAllocState::IsDefaultAllocatorUsed();

    if (Y_UNLIKELY(useFree)) {
        auto entry = (TAllocState::TListEntry*)(mem)-1;
        entry->Unlink();
        free(entry);
        state->OffloadFree(sizeof(TAllocState::TListEntry) + sz);
        return;
    }

    TAllocPageHeader* header = (TAllocPageHeader*)TAllocState::GetPageStart(mem);
    {
        Y_DEBUG_ABORT_UNLESS(header->MyAlloc == state, "Wrong allocator was used. Allocated with: %s, freed with: %s",
                             header->MyAlloc->GetDebugInfo().c_str(), TlsAllocState->GetDebugInfo().c_str());
        if (Y_LIKELY(--header->UseCount != 0)) {
            header->Deallocated += sz;
            return;
        }
    }

    MKQLFreeSlow(header, state, mPool);
}

inline void MKQLFreeFastWithSize(const void* mem, size_t sz, TAllocState* state, const EMemorySubPool mPool) noexcept {
    mem = NYql::NUdf::UnwrapPointerWithRedZones(mem, sz);
    sz = NYql::NUdf::GetSizeToAlloc(sz);
    return MKQLFreeFastWithSizeImpl(mem, sz, state, mPool);
}

inline void* MKQLAllocDeprecated(size_t sz, const EMemorySubPool mPool) {
    return MKQLAllocFastDeprecated(sz, TlsAllocState, mPool);
}

inline void* MKQLAllocWithSize(size_t sz, const EMemorySubPool mPool, const TAllocLocation& location = TAllocLocation::current()) {
    return MKQLAllocFastWithSize(sz, TlsAllocState, mPool, location);
}

inline void MKQLFreeWithSize(const void* mem, size_t sz, const EMemorySubPool mPool) noexcept {
    MKQLFreeFastWithSize(mem, sz, TlsAllocState, mPool);
}

inline void MKQLRegisterObject(NUdf::TBoxedValue* value) noexcept {
    value->Link(TlsAllocState->GetRoot());
}

inline void MKQLUnregisterObject(NUdf::TBoxedValue* value) noexcept {
    value->Unlink();
}

void* MKQLArrowAllocate(ui64 size);
void* MKQLArrowReallocate(const void* mem, ui64 prevSize, ui64 size);
void MKQLArrowFree(const void* mem, ui64 size);
void MKQLArrowUntrack(const void* mem);

template <const EMemorySubPool MemoryPoolExt = EMemorySubPool::Default>
struct TWithMiniKQLAlloc {
    static constexpr EMemorySubPool MemoryPool = MemoryPoolExt;

    static void FreeWithSize(const void* mem, const size_t sz) {
        NMiniKQL::MKQLFreeWithSize(mem, sz, MemoryPool);
    }

    static void* AllocWithSize(const size_t sz) {
        return NMiniKQL::MKQLAllocWithSize(sz, MemoryPool);
    }

    void* operator new(size_t sz) {
        return NMiniKQL::MKQLAllocWithSize(sz, MemoryPool);
    }

    void* operator new[](size_t sz) {
        return NMiniKQL::MKQLAllocWithSize(sz, MemoryPool);
    }

    void operator delete(void* mem, std::size_t sz) noexcept {
        NMiniKQL::MKQLFreeWithSize(mem, sz, MemoryPool);
    }

    void operator delete[](void* mem, std::size_t sz) noexcept {
        NMiniKQL::MKQLFreeWithSize(mem, sz, MemoryPool);
    }
};

template <typename T, typename... Args>
T* AllocateOn(const TAllocLocation& location, TAllocState* state, Args&&... args)
{
    void* addr = MKQLAllocFastWithSize(sizeof(T), state, T::MemoryPool, location);
    return ::new (addr) T(std::forward<Args>(args)...);
    static_assert(std::is_base_of<TWithMiniKQLAlloc<T::MemoryPool>, T>::value, "Class must inherit TWithMiniKQLAlloc.");
}

template <typename T, typename... Args>
T* AllocateOn(TAllocState* state, Args&&... args)
{
    void* addr = MKQLAllocFastWithSize(sizeof(T), state, T::MemoryPool);
    return ::new (addr) T(std::forward<Args>(args)...);
    static_assert(std::is_base_of<TWithMiniKQLAlloc<T::MemoryPool>, T>::value, "Class must inherit TWithMiniKQLAlloc.");
}

template <typename Type, EMemorySubPool MemoryPool = EMemorySubPool::Default>
struct TMKQLAllocator {
    typedef Type value_type;
    typedef Type* pointer;
    typedef const Type* const_pointer;
    typedef Type& reference;
    typedef const Type& const_reference;
    typedef size_t size_type;
    typedef ptrdiff_t difference_type;

    TMKQLAllocator() noexcept = default;
    ~TMKQLAllocator() noexcept = default;

    // Almost a copy costructor.
    template <typename U> // NOLINTNEXTLINE(google-explicit-constructor)
    TMKQLAllocator(const TMKQLAllocator<U, MemoryPool>&) noexcept {
    }

    template <typename U>
    struct rebind { // NOLINT(readability-identifier-naming)
        typedef TMKQLAllocator<U, MemoryPool> other;
    };
    template <typename U>
    bool operator==(const TMKQLAllocator<U, MemoryPool>&) const {
        return true;
    }
    template <typename U>
    bool operator!=(const TMKQLAllocator<U, MemoryPool>&) const {
        return false;
    }

    static pointer allocate(size_type n, const void* = nullptr) // NOLINT(readability-identifier-naming)
    {
        return static_cast<pointer>(MKQLAllocWithSize(n * sizeof(value_type), MemoryPool));
    }

    static void deallocate(const_pointer p, size_type n) noexcept // NOLINT(readability-identifier-naming)
    {
        MKQLFreeWithSize(p, n * sizeof(value_type), MemoryPool);
    }
};

using TWithDefaultMiniKQLAlloc = TWithMiniKQLAlloc<EMemorySubPool::Default>;
using TWithTemporaryMiniKQLAlloc = TWithMiniKQLAlloc<EMemorySubPool::Temporary>;

template <typename Type>
struct TMKQLHugeAllocator {
    typedef Type value_type;
    typedef Type* pointer;
    typedef const Type* const_pointer;
    typedef Type& reference;
    typedef const Type& const_reference;
    typedef size_t size_type;
    typedef ptrdiff_t difference_type;

    TMKQLHugeAllocator() noexcept = default;
    ~TMKQLHugeAllocator() noexcept = default;

    template <typename U>
    explicit TMKQLHugeAllocator(const TMKQLHugeAllocator<U>&) noexcept {
    }

    template <typename U>
    bool operator==(const TMKQLHugeAllocator<U>&) const {
        return true;
    }

    template <typename U>
    bool operator!=(const TMKQLHugeAllocator<U>&) const {
        return false;
    }

    static pointer AllocateImpl(size_type n, const void* = nullptr)
    {
        size_t size = Max(n * sizeof(value_type), TAllocState::POOL_PAGE_SIZE);
        return static_cast<pointer>(TlsAllocState->GetBlock(size));
    }

    static pointer allocate(size_type n, const void* = nullptr) // NOLINT(readability-identifier-naming)
    {
        n = NYql::NUdf::GetSizeToAlloc(n);
        void* mem = AllocateImpl(n);
        return static_cast<pointer>(NYql::NUdf::WrapPointerWithRedZones(mem, n));
    }

    static void DeallocateImpl(const_pointer p, size_type n) noexcept {
        size_t size = Max(n * sizeof(value_type), TAllocState::POOL_PAGE_SIZE);
        TlsAllocState->ReturnBlock(const_cast<pointer>(p), size);
    }

    static void deallocate(const_pointer p, size_type n) noexcept // NOLINT(readability-identifier-naming)
    {
        p = static_cast<const_pointer>(NYql::NUdf::UnwrapPointerWithRedZones(p, n));
        n = NYql::NUdf::GetSizeToAlloc(n);
        return DeallocateImpl(p, n);
    }
};

template <typename T>
class TPagedList {
public:
    static_assert(sizeof(T) <= TAlignedPagePool::POOL_PAGE_SIZE, "Too big object");
    static constexpr size_t OBJECTS_PER_PAGE = TAlignedPagePool::POOL_PAGE_SIZE / sizeof(T);

    class TIterator;
    class TConstIterator;

    explicit TPagedList(TAlignedPagePool& pool)
        : Pool_(pool)
        , IndexInLastPage_(OBJECTS_PER_PAGE)
    {
    }

    TPagedList(const TPagedList&) = delete;
    TPagedList(TPagedList&&) = delete;

    ~TPagedList() {
        Clear();
    }

    void Add(T&& value) {
        if (IndexInLastPage_ < OBJECTS_PER_PAGE) {
            auto ptr = ObjectAt(Pages_.back(), IndexInLastPage_);
            new (ptr) T(std::move(value));
            ++IndexInLastPage_;
            return;
        }

        auto ptr = NYql::NUdf::SanitizerMakeRegionAccessible(Pool_.GetPage(), TAlignedPagePool::POOL_PAGE_SIZE);
        IndexInLastPage_ = 1;
        Pages_.push_back(ptr);
        new (ptr) T(std::move(value));
    }

    void Clear() {
        for (ui32 i = 0; i + 1 < Pages_.size(); ++i) {
            for (ui32 objIndex = 0; objIndex < OBJECTS_PER_PAGE; ++objIndex) {
                ObjectAt(Pages_[i], objIndex)->~T();
            }

            Pool_.ReturnPage(Pages_[i]);
        }

        if (!Pages_.empty()) {
            for (ui32 objIndex = 0; objIndex < IndexInLastPage_; ++objIndex) {
                ObjectAt(Pages_.back(), objIndex)->~T();
            }

            Pool_.ReturnPage(Pages_.back());
        }

        TPages().swap(Pages_);
        IndexInLastPage_ = OBJECTS_PER_PAGE;
    }

    const T& operator[](size_t i) const {
        const auto table = i / OBJECTS_PER_PAGE;
        const auto index = i % OBJECTS_PER_PAGE;
        return *ObjectAt(Pages_[table], index);
    }

    size_t Size() const {
        return Pages_.empty() ? 0 : ((Pages_.size() - 1) * OBJECTS_PER_PAGE + IndexInLastPage_);
    }

    TConstIterator Begin() const {
        return TConstIterator(this, 0, 0);
    }

    TConstIterator begin() const {
        return Begin();
    }

    TConstIterator End() const {
        if (IndexInLastPage_ == OBJECTS_PER_PAGE) {
            return TConstIterator(this, Pages_.size(), 0);
        }

        return TConstIterator(this, Pages_.size() - 1, IndexInLastPage_);
    }

    TConstIterator end() const {
        return End();
    }

    TIterator Begin() {
        return TIterator(this, 0, 0);
    }

    TIterator begin() {
        return Begin();
    }

    TIterator End() {
        if (IndexInLastPage_ == OBJECTS_PER_PAGE) {
            return TIterator(this, Pages_.size(), 0);
        }

        return TIterator(this, Pages_.size() - 1, IndexInLastPage_);
    }

    TIterator end() {
        return End();
    }

    class TIterator {
    public:
        using TOwner = TPagedList<T>;

        TIterator()
            : Owner_(nullptr)
            , PageNo_(0)
            , PageIndex_(0)
        {
        }

        TIterator(const TIterator&) = default;
        TIterator& operator=(const TIterator&) = default;

        TIterator(TOwner* owner, size_t pageNo, size_t pageIndex)
            : Owner_(owner)
            , PageNo_(pageNo)
            , PageIndex_(pageIndex)
        {
        }

        T& operator*() {
            Y_DEBUG_ABORT_UNLESS(PageIndex_ < OBJECTS_PER_PAGE);
            Y_DEBUG_ABORT_UNLESS(PageNo_ < Owner_->Pages_.size());
            Y_DEBUG_ABORT_UNLESS(PageNo_ + 1 < Owner_->Pages_.size() || PageIndex_ < Owner_->IndexInLastPage_);
            return *Owner_->ObjectAt(Owner_->Pages_[PageNo_], PageIndex_);
        }

        TIterator& operator++() {
            if (++PageIndex_ == OBJECTS_PER_PAGE) {
                ++PageNo_;
                PageIndex_ = 0;
            }

            return *this;
        }

        bool operator==(const TIterator& other) const {
            return PageNo_ == other.PageNo_ && PageIndex_ == other.PageIndex_;
        }

        bool operator!=(const TIterator& other) const {
            return !operator==(other);
        }

    private:
        TOwner* Owner_;
        size_t PageNo_;
        size_t PageIndex_;
    };

    class TConstIterator {
    public:
        using TOwner = TPagedList<T>;

        TConstIterator()
            : Owner_(nullptr)
            , PageNo_(0)
            , PageIndex_(0)
        {
        }

        TConstIterator(const TConstIterator&) = default;
        TConstIterator& operator=(const TConstIterator&) = default;

        TConstIterator(const TOwner* owner, size_t pageNo, size_t pageIndex)
            : Owner_(owner)
            , PageNo_(pageNo)
            , PageIndex_(pageIndex)
        {
        }

        const T& operator*() {
            Y_DEBUG_ABORT_UNLESS(PageIndex_ < OBJECTS_PER_PAGE);
            Y_DEBUG_ABORT_UNLESS(PageNo_ < Owner_->Pages_.size());
            Y_DEBUG_ABORT_UNLESS(PageNo_ + 1 < Owner_->Pages_.size() || PageIndex_ < Owner_->IndexInLastPage_);
            return *Owner_->ObjectAt(Owner_->Pages_[PageNo_], PageIndex_);
        }

        TConstIterator& operator++() {
            if (++PageIndex_ == OBJECTS_PER_PAGE) {
                ++PageNo_;
                PageIndex_ = 0;
            }

            return *this;
        }

        bool operator==(const TConstIterator& other) const {
            return PageNo_ == other.PageNo_ && PageIndex_ == other.PageIndex_;
        }

        bool operator!=(const TConstIterator& other) const {
            return !operator==(other);
        }

    private:
        const TOwner* Owner_;
        size_t PageNo_;
        size_t PageIndex_;
    };

private:
    static const T* ObjectAt(const void* page, size_t objectIndex) {
        return reinterpret_cast<const T*>(static_cast<const char*>(page) + objectIndex * sizeof(T));
    }

    static T* ObjectAt(void* page, size_t objectIndex) {
        return reinterpret_cast<T*>(static_cast<char*>(page) + objectIndex * sizeof(T));
    }

    TAlignedPagePool& Pool_;
    using TPages = std::vector<void*, TMKQLAllocator<void*>>;
    TPages Pages_;
    size_t IndexInLastPage_;
};

inline void TBoxedValueWithFree::operator delete(void* mem) noexcept {
    auto size = ((TMkqlPAllocHeader*)mem)->Size + sizeof(TMkqlPAllocHeader);
    MKQLFreeWithSize(mem, size, EMemorySubPool::Default);
}

} // namespace NKikimr::NMiniKQL
