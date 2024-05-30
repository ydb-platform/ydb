#pragma once
#include "aligned_page_pool.h"
#include "mkql_mem_info.h"
#include <ydb/library/yql/core/pg_settings/guc_settings.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/context.h>
#include <ydb/library/yql/public/udf/udf_allocator.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <util/string/builder.h>
#include <util/system/align.h>
#include <util/system/defaults.h>
#include <util/system/tls.h>
#include <new>
#include <unordered_map>
#include <atomic>
#include <memory>

namespace NKikimr {

namespace NMiniKQL {

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

struct TAllocState : public TAlignedPagePool
{
    struct TListEntry {
        TListEntry *Left = nullptr;
        TListEntry *Right = nullptr;

        void Link(TListEntry* root) noexcept;
        void Unlink() noexcept;
        void InitLinks() noexcept { Left = Right = this; }
    };

#ifndef NDEBUG
    std::unordered_map<TMemoryUsageInfo*, TIntrusivePtr<TMemoryUsageInfo>> ActiveMemInfo;
#endif
    bool SupportsSizedAllocators = false;

    void* LargeAlloc(size_t size) {
        return Alloc(size);
    }

    void LargeFree(void* ptr, size_t size) noexcept {
        Free(ptr, size);
    }

    using TCurrentPages = std::array<TAllocPageHeader*, (TMemorySubPoolIdx)EMemorySubPool::Count>;

    static TAllocPageHeader EmptyPageHeader;
    static TCurrentPages EmptyCurrentPages;

    std::array<TAllocPageHeader*, (TMemorySubPoolIdx)EMemorySubPool::Count> CurrentPages = EmptyCurrentPages;
    TListEntry OffloadedBlocksRoot;
    TListEntry GlobalPAllocList;
    TListEntry* CurrentPAllocList;
    std::shared_ptr<std::atomic<size_t>> ArrowMemoryUsage = std::make_shared<std::atomic<size_t>>();
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
    size_t GetDeallocatedInPages() const;
    static void CleanupPAllocList(TListEntry* root);

    void LockObject(::NKikimr::NUdf::TUnboxedValuePod value);
    void UnlockObject(::NKikimr::NUdf::TUnboxedValuePod value);
};

extern Y_POD_THREAD(TAllocState*) TlsAllocState;

class TPAllocScope {
public:
    TPAllocScope() {
        PAllocList.InitLinks();
        Attach();
    }

    ~TPAllocScope() {
        Cleanup();
        Detach();
    }

    void Attach() {
        Y_ABORT_UNLESS(!Prev);
        Prev = TlsAllocState->CurrentPAllocList;
        Y_ABORT_UNLESS(Prev);
        TlsAllocState->CurrentPAllocList = &PAllocList;
    }

    void Detach() {
        if (Prev) {
           Y_ABORT_UNLESS(TlsAllocState->CurrentPAllocList == &PAllocList);
           TlsAllocState->CurrentPAllocList = Prev;
           Prev = nullptr;
        }
    }

    void Cleanup() {
        TAllocState::CleanupPAllocList(&PAllocList);
    }

private:
    TAllocState::TListEntry PAllocList;
    TAllocState::TListEntry* Prev = nullptr;
};

// TListEntry and IBoxedValue use the same place
static_assert(sizeof(NUdf::IBoxedValue) == sizeof(TAllocState::TListEntry));

class TBoxedValueWithFree : public NUdf::TBoxedValueBase {
public:
    void operator delete(void *mem) noexcept;
};

struct TMkqlPAllocHeader {
    union {
        TAllocState::TListEntry Entry;
        TBoxedValueWithFree Boxed;
    } U;

    size_t Size;
    void* Self; // should be placed right before pointer to allocated area, see GetMemoryChunkContext
};

static_assert(sizeof(TMkqlPAllocHeader) == 
    sizeof(size_t) +
    sizeof(TAllocState::TListEntry) +
    sizeof(void*), "Padding is not allowed");

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

    size_t GetUsed() const { return MyState_.GetUsed(); }
    size_t GetPeakUsed() const { return MyState_.GetPeakUsed(); }
    size_t GetAllocated() const { return MyState_.GetAllocated(); }
    size_t GetPeakAllocated() const { return MyState_.GetPeakAllocated(); }

    size_t GetLimit() const { return MyState_.GetLimit(); }
    void SetLimit(size_t limit) { MyState_.SetLimit(limit); }
    void DisableStrictAllocationCheck() { MyState_.DisableStrictAllocationCheck(); }

    void ReleaseFreePages() { MyState_.ReleaseFreePages(); }
    void InvalidateMemInfo() { MyState_.InvalidateMemInfo(); }

    bool IsAttached() const { return AttachedCount_ > 0; }

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
    TPagedArena(TAlignedPagePool* pagePool) noexcept
        : PagePool_(pagePool)
        , CurrentPages_(TAllocState::EmptyCurrentPages)
    {}

    TPagedArena(const TPagedArena&) = delete;
    TPagedArena(TPagedArena&& other) noexcept
        : PagePool_(other.PagePool_)
        , CurrentPages_(other.CurrentPages_)
    {
        other.CurrentPages_ = TAllocState::EmptyCurrentPages;
    }

    void operator=(const TPagedArena&) = delete;
    void operator=(TPagedArena&& other) noexcept {
        Clear();
        PagePool_ = other.PagePool_;
        CurrentPages_ = other.CurrentPages_;
        other.CurrentPages_ = TAllocState::EmptyCurrentPages;
    }

    ~TPagedArena() noexcept {
        Clear();
    }

    void* Alloc(size_t sz, const EMemorySubPool pagePool = EMemorySubPool::Default) {
        auto& currentPage = CurrentPages_[(TMemorySubPoolIdx)pagePool];
        if (Y_LIKELY(currentPage->Offset + sz <= currentPage->Capacity)) {
            void* ret = (char*)currentPage + currentPage->Offset;
            currentPage->Offset = AlignUp(currentPage->Offset + sz, MKQL_ALIGNMENT);
            return ret;
        }

        return AllocSlow(sz, pagePool);
    }

    void Clear() noexcept;

private:
    void* AllocSlow(const size_t sz, const EMemorySubPool pagePool);

private:
    TAlignedPagePool* PagePool_;
    TAllocState::TCurrentPages CurrentPages_ = TAllocState::EmptyCurrentPages;
};

void* MKQLAllocSlow(size_t sz, TAllocState* state, const EMemorySubPool mPool);
inline void* MKQLAllocFastDeprecated(size_t sz, TAllocState* state, const EMemorySubPool mPool) {
    Y_DEBUG_ABORT_UNLESS(state);

#ifdef PROFILE_MEMORY_ALLOCATIONS
    auto ret = (TAllocState::TListEntry*)malloc(sizeof(TAllocState::TListEntry) + sz);
    if (!ret) {
        throw TMemoryLimitExceededException();
    }

    ret->Link(&state->OffloadedBlocksRoot);
    return ret + 1;
#endif

    auto currPage = state->CurrentPages[(TMemorySubPoolIdx)mPool];
    if (Y_LIKELY(currPage->Offset + sz <= currPage->Capacity)) {
        void* ret = (char*)currPage + currPage->Offset;
        currPage->Offset = AlignUp(currPage->Offset + sz, MKQL_ALIGNMENT);
        ++currPage->UseCount;
        return ret;
    }

    return MKQLAllocSlow(sz, state, mPool);
}

inline void* MKQLAllocFastWithSize(size_t sz, TAllocState* state, const EMemorySubPool mPool) {
    Y_DEBUG_ABORT_UNLESS(state);

    bool useMemalloc = state->SupportsSizedAllocators && sz > MaxPageUserData;

#ifdef PROFILE_MEMORY_ALLOCATIONS
    useMemalloc = true;
#endif

    if (useMemalloc) {
        state->OffloadAlloc(sizeof(TAllocState::TListEntry) + sz);
        auto ret = (TAllocState::TListEntry*)malloc(sizeof(TAllocState::TListEntry) + sz);
        if (!ret) {
            throw TMemoryLimitExceededException();
        }

        ret->Link(&state->OffloadedBlocksRoot);
        return ret + 1;
    }

    auto currPage = state->CurrentPages[(TMemorySubPoolIdx)mPool];
    if (Y_LIKELY(currPage->Offset + sz <= currPage->Capacity)) {
        void* ret = (char*)currPage + currPage->Offset;
        currPage->Offset = AlignUp(currPage->Offset + sz, MKQL_ALIGNMENT);
        ++currPage->UseCount;
        return ret;
    }

    return MKQLAllocSlow(sz, state, mPool);
}

void MKQLFreeSlow(TAllocPageHeader* header, TAllocState *state, const EMemorySubPool mPool) noexcept;

inline void MKQLFreeDeprecated(const void* mem, const EMemorySubPool mPool) noexcept {
    if (!mem) {
        return;
    }

#ifdef PROFILE_MEMORY_ALLOCATIONS
    TAllocState *state = TlsAllocState;
    Y_DEBUG_ABORT_UNLESS(state);

    auto entry = (TAllocState::TListEntry*)(mem) - 1;
    entry->Unlink();
    free(entry);
    return;
#endif

    TAllocPageHeader* header = (TAllocPageHeader*)TAllocState::GetPageStart(mem);
    Y_DEBUG_ABORT_UNLESS(header->MyAlloc == TlsAllocState, "%s", (TStringBuilder() << "wrong allocator was used; "
        "allocated with: " << header->MyAlloc->GetDebugInfo() << " freed with: " << TlsAllocState->GetDebugInfo()).data());
    if (Y_LIKELY(--header->UseCount != 0)) {
        return;
    }

    MKQLFreeSlow(header, TlsAllocState, mPool);
}

inline void MKQLFreeFastWithSize(const void* mem, size_t sz, TAllocState* state, const EMemorySubPool mPool) noexcept {
    if (!mem) {
        return;
    }

    Y_DEBUG_ABORT_UNLESS(state);

    bool useFree = state->SupportsSizedAllocators && sz > MaxPageUserData;

#ifdef PROFILE_MEMORY_ALLOCATIONS
    useFree = true;
#endif

    if (useFree) {
        auto entry = (TAllocState::TListEntry*)(mem) - 1;
        entry->Unlink();
        free(entry);
        state->OffloadFree(sizeof(TAllocState::TListEntry) + sz);
        return;
    }

    TAllocPageHeader* header = (TAllocPageHeader*)TAllocState::GetPageStart(mem);
    Y_DEBUG_ABORT_UNLESS(header->MyAlloc == state, "%s", (TStringBuilder() << "wrong allocator was used; "
        "allocated with: " << header->MyAlloc->GetDebugInfo() << " freed with: " << TlsAllocState->GetDebugInfo()).data());
    if (Y_LIKELY(--header->UseCount != 0)) {
        header->Deallocated += sz;
        return;
    }

    MKQLFreeSlow(header, state, mPool);
}

inline void* MKQLAllocDeprecated(size_t sz, const EMemorySubPool mPool) {
    return MKQLAllocFastDeprecated(sz, TlsAllocState, mPool);
}

inline void* MKQLAllocWithSize(size_t sz, const EMemorySubPool mPool) {
    return MKQLAllocFastWithSize(sz, TlsAllocState, mPool);
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

    void operator delete(void *mem, std::size_t sz) noexcept {
        NMiniKQL::MKQLFreeWithSize(mem, sz, MemoryPool);
    }

    void operator delete[](void *mem, std::size_t sz) noexcept {
        NMiniKQL::MKQLFreeWithSize(mem, sz, MemoryPool);
    }
};

template <typename T, typename... Args>
T* AllocateOn(TAllocState* state, Args&&... args)
{
    void* addr = MKQLAllocFastWithSize(sizeof(T), state, T::MemoryPool);
    return ::new(addr) T(std::forward<Args>(args)...);
    static_assert(std::is_base_of<TWithMiniKQLAlloc<T::MemoryPool>, T>::value, "Class must inherit TWithMiniKQLAlloc.");
}

template <typename Type, EMemorySubPool MemoryPool = EMemorySubPool::Default>
struct TMKQLAllocator
{
    typedef Type value_type;
    typedef Type* pointer;
    typedef const Type* const_pointer;
    typedef Type& reference;
    typedef const Type& const_reference;
    typedef size_t size_type;
    typedef ptrdiff_t difference_type;

    TMKQLAllocator() noexcept = default;
    ~TMKQLAllocator() noexcept = default;

    template<typename U> TMKQLAllocator(const TMKQLAllocator<U, MemoryPool>&) noexcept {}
    template<typename U> struct rebind { typedef TMKQLAllocator<U, MemoryPool> other; };
    template<typename U> bool operator==(const TMKQLAllocator<U, MemoryPool>&) const { return true; }
    template<typename U> bool operator!=(const TMKQLAllocator<U, MemoryPool>&) const { return false; }

    static pointer allocate(size_type n, const void* = nullptr)
    {
        return static_cast<pointer>(MKQLAllocWithSize(n * sizeof(value_type), MemoryPool));
    }

    static void deallocate(const_pointer p, size_type n) noexcept
    {
        MKQLFreeWithSize(p, n * sizeof(value_type), MemoryPool);
    }
};

using TWithDefaultMiniKQLAlloc = TWithMiniKQLAlloc<EMemorySubPool::Default>;
using TWithTemporaryMiniKQLAlloc = TWithMiniKQLAlloc<EMemorySubPool::Temporary>;

template <typename T>
class TPagedList
{
public:
    static_assert(sizeof(T) <= TAlignedPagePool::POOL_PAGE_SIZE, "Too big object");
    static constexpr size_t OBJECTS_PER_PAGE = TAlignedPagePool::POOL_PAGE_SIZE / sizeof(T);

    class TIterator;
    class TConstIterator;

    TPagedList(TAlignedPagePool& pool)
        : Pool(pool)
        , IndexInLastPage(OBJECTS_PER_PAGE)
    {}

    TPagedList(const TPagedList&) = delete;
    TPagedList(TPagedList&&) = delete;

    ~TPagedList() {
        Clear();
    }

    void Add(T&& value) {
        if (IndexInLastPage < OBJECTS_PER_PAGE) {
            auto ptr = ObjectAt(Pages.back(), IndexInLastPage);
            new(ptr) T(std::move(value));
            ++IndexInLastPage;
            return;
        }

        auto ptr = Pool.GetPage();
        IndexInLastPage = 1;
        Pages.push_back(ptr);
        new(ptr) T(std::move(value));
    }

    void Clear() {
        for (ui32 i = 0; i + 1 < Pages.size(); ++i) {
            for (ui32 objIndex = 0; objIndex < OBJECTS_PER_PAGE; ++objIndex) {
                ObjectAt(Pages[i], objIndex)->~T();
            }

            Pool.ReturnPage(Pages[i]);
        }

        if (!Pages.empty()) {
            for (ui32 objIndex = 0; objIndex < IndexInLastPage; ++objIndex) {
                ObjectAt(Pages.back(), objIndex)->~T();
            }

            Pool.ReturnPage(Pages.back());
        }

        TPages().swap(Pages);
        IndexInLastPage = OBJECTS_PER_PAGE;
    }

    const T& operator[](size_t i) const {
        const auto table = i / OBJECTS_PER_PAGE;
        const auto index = i % OBJECTS_PER_PAGE;
        return *ObjectAt(Pages[table], index);
    }

    size_t Size() const {
        return Pages.empty() ? 0 : ((Pages.size() - 1) * OBJECTS_PER_PAGE + IndexInLastPage);
    }

    TConstIterator Begin() const {
        return TConstIterator(this, 0, 0);
    }

    TConstIterator begin() const {
        return Begin();
    }

    TConstIterator End() const {
        if (IndexInLastPage == OBJECTS_PER_PAGE) {
            return TConstIterator(this, Pages.size(), 0);
        }

        return TConstIterator(this, Pages.size() - 1, IndexInLastPage);
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
        if (IndexInLastPage == OBJECTS_PER_PAGE) {
            return TIterator(this, Pages.size(), 0);
        }

        return TIterator(this, Pages.size() - 1, IndexInLastPage);
    }

    TIterator end() {
        return End();
    }

    class TIterator
    {
    public:
        using TOwner = TPagedList<T>;

        TIterator()
            : Owner(nullptr)
            , PageNo(0)
            , PageIndex(0)
        {}

        TIterator(const TIterator&) = default;
        TIterator& operator=(const TIterator&) = default;

        TIterator(TOwner* owner, size_t pageNo, size_t pageIndex)
            : Owner(owner)
            , PageNo(pageNo)
            , PageIndex(pageIndex)
        {}

        T& operator*() {
            Y_DEBUG_ABORT_UNLESS(PageIndex < OBJECTS_PER_PAGE);
            Y_DEBUG_ABORT_UNLESS(PageNo < Owner->Pages.size());
            Y_DEBUG_ABORT_UNLESS(PageNo + 1 < Owner->Pages.size() || PageIndex < Owner->IndexInLastPage);
            return *Owner->ObjectAt(Owner->Pages[PageNo], PageIndex);
        }

        TIterator& operator++() {
            if (++PageIndex == OBJECTS_PER_PAGE) {
                ++PageNo;
                PageIndex = 0;
            }

            return *this;
        }

        bool operator==(const TIterator& other) const {
            return PageNo == other.PageNo && PageIndex == other.PageIndex;
        }

        bool operator!=(const TIterator& other) const {
            return !operator==(other);
        }

    private:
        TOwner* Owner;
        size_t PageNo;
        size_t PageIndex;
    };

    class TConstIterator
    {
    public:
        using TOwner = TPagedList<T>;

        TConstIterator()
            : Owner(nullptr)
            , PageNo(0)
            , PageIndex(0)
        {}

        TConstIterator(const TConstIterator&) = default;
        TConstIterator& operator=(const TConstIterator&) = default;

        TConstIterator(const TOwner* owner, size_t pageNo, size_t pageIndex)
            : Owner(owner)
            , PageNo(pageNo)
            , PageIndex(pageIndex)
        {}

        const T& operator*() {
            Y_DEBUG_ABORT_UNLESS(PageIndex < OBJECTS_PER_PAGE);
            Y_DEBUG_ABORT_UNLESS(PageNo < Owner->Pages.size());
            Y_DEBUG_ABORT_UNLESS(PageNo + 1 < Owner->Pages.size() || PageIndex < Owner->IndexInLastPage);
            return *Owner->ObjectAt(Owner->Pages[PageNo], PageIndex);
        }

        TConstIterator& operator++() {
            if (++PageIndex == OBJECTS_PER_PAGE) {
                ++PageNo;
                PageIndex = 0;
            }

            return *this;
        }

        bool operator==(const TConstIterator& other) const {
            return PageNo == other.PageNo && PageIndex == other.PageIndex;
        }

        bool operator!=(const TConstIterator& other) const {
            return !operator==(other);
        }

    private:
        const TOwner* Owner;
        size_t PageNo;
        size_t PageIndex;
    };

private:
    static const T* ObjectAt(const void* page, size_t objectIndex) {
        return reinterpret_cast<const T*>(static_cast<const char*>(page) + objectIndex * sizeof(T));
    }

    static T* ObjectAt(void* page, size_t objectIndex) {
        return reinterpret_cast<T*>(static_cast<char*>(page) + objectIndex * sizeof(T));
    }

    TAlignedPagePool& Pool;
    using TPages = std::vector<void*, TMKQLAllocator<void*>>;
    TPages Pages;
    size_t IndexInLastPage;
};

inline void TBoxedValueWithFree::operator delete(void *mem) noexcept {
    auto size = ((TMkqlPAllocHeader*)mem)->Size + sizeof(TMkqlPAllocHeader);
    MKQLFreeWithSize(mem, size, EMemorySubPool::Default);
}

} // NMiniKQL

} // NKikimr
