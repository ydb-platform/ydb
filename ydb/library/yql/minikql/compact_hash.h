#pragma once

#include <ydb/library/yql/utils/hash.h>

#include "aligned_page_pool.h"
#include "primes.h"

#include <util/generic/vector.h>
#include <util/generic/ptr.h>
#include <util/generic/hash.h>
#include <util/generic/algorithm.h>
#include <util/generic/bitops.h>
#include <util/generic/utility.h>
#include <util/generic/strbuf.h>
#include <util/generic/mem_copy.h>
#include <util/generic/scope.h>
#include <util/system/defaults.h>
#include <util/system/unaligned_mem.h>
#include <util/system/align.h>
#include <util/stream/output.h>

#include <type_traits>
#include <cmath>
#include <array>

// TODO: Allow only PODs in key/value. Use runtime key/value sizes instead of compile time type instantiation. Use Read/WriteUnaligned

namespace NKikimr {

namespace NCHash {

class TListPoolBase {
public:
    enum {
        SMALL_MARK = 1,
        MEDIUM_MARK,
        LARGE_MARK,
    };
    static const size_t MAX_SMALL_LIST_SIZE = 16;
    static const size_t MAX_MEDIUM_LIST_INDEX = 10;

public:
    struct TPageListItem: public TIntrusiveListItem<TPageListItem> {
        template <class T>
        T* As() {
            return static_cast<T*>(TAlignedPagePool::GetPageStart(this));
        }
        template <class T>
        const T* As() const {
            return static_cast<const T*>(TAlignedPagePool::GetPageStart(this));
        }
    };

    struct TListHeader {
        ui16 Mark;
        ui16 ListSize;
        ui16 FreeLists;
        ui16 FreeListOffset;
        TPageListItem ListItem;

        TListHeader(ui16 mark, ui16 listSize, ui16 listCount)
            : Mark(mark)
            , ListSize(listSize)
            , FreeLists(listCount)
            , FreeListOffset(0u)
        {
            Y_ASSERT(ListItem.As<TListHeader>() == this);
            *reinterpret_cast<ui16*>(this + 1) = 0u; // Mark first list for initial usage
        }
        ~TListHeader() {
            Mark = 0u; // Reset mark
        }
    };

    struct TLargeListHeader {
        ui16 Mark;
        ui32 Capacity;
        ui32 Size;
        TPageListItem ListItem;

        TLargeListHeader(ui32 capacity)
            : Mark(LARGE_MARK)
            , Capacity(capacity)
            , Size(0u)
        {
            Y_ASSERT(ListItem.As<TLargeListHeader>() == this);
        }
        ~TLargeListHeader() {
            Mark = 0u; // Reset mark
        }
        template <typename T>
        T* GetList() {
            return reinterpret_cast<T*>(this + 1);
        }

        template <typename T>
        const T* GetList() const {
            return reinterpret_cast<const T*>(this + 1);
        }

        TLargeListHeader* Next() {
            return ListItem.Next()->Node()->As<TLargeListHeader>();
        }

        const TLargeListHeader* Next() const {
            return ListItem.Next()->Node()->As<TLargeListHeader>();
        }

        void Append(TLargeListHeader* header) {
            header->ListItem.LinkAfter(ListItem);
        }
    };

    template <typename T, class THeader>
    class TListIterator {
        using TRawByte = std::conditional_t<std::is_const<T>::value, const ui8*, ui8*>;
    public:
        using TRaw = std::conditional_t<std::is_const<T>::value, const void*, void*>;

        TListIterator() {
        }

        TListIterator(T* list) {
            if (LARGE_MARK == GetMark(list)) {
                CurrentPage = EndPage = GetLargeListHeader(list)->Next();
                Current = CurrentPage->template GetList<T>();
                End = (TRawByte)Current + CurrentPage->Size * sizeof(T);
            } else {
                Current = list;
                End = (TRawByte)Current + GetPartListSize(list) * sizeof(T);
            }
        }

        TListIterator(TRaw start, TRaw end)
            : Current(start)
            , End(end)
        {
        }

        TListIterator& operator++() {
            Y_ASSERT(Ok());
            Current = (TRawByte)Current + sizeof(T);
            if (Current == End) {
                if (CurrentPage) {
                    CurrentPage = CurrentPage->Next();
                    if (CurrentPage != EndPage) {
                        Current = (TRaw)CurrentPage->template GetList<T>();
                        End = (TRawByte)Current + CurrentPage->Size * sizeof(T);
                    } else {
                        CurrentPage = EndPage = nullptr;
                        Current = End = nullptr;
                    }
                } else {
                    Current = End = nullptr;
                }
            }
            return *this;
        }

        bool operator ==(const TListIterator& it) const {
            return Current == it.Current && CurrentPage == it.CurrentPage;
        }

        bool operator !=(const TListIterator& it) const {
            return !operator ==(it);
        }

        T Get() const {
            using TClean = typename std::remove_cv<T>::type;
            return ::ReadUnaligned<TClean>(Current);
        }

        void Set(T val) const {
            ::WriteUnaligned<T>(Current, val);
        }

        TRaw GetRaw() const {
            return Current;
        }

        bool Ok() const {
            return Current != nullptr;
        }

    private:
        TRaw Current = nullptr;
        TRaw End = nullptr;
        THeader* CurrentPage = nullptr;
        THeader* EndPage = nullptr;
    };

public:
    static inline TListHeader* GetListHeader(void* addr) {
        TListHeader* header = reinterpret_cast<TListHeader*>(TAlignedPagePool::GetPageStart(addr));
        Y_ASSERT(SMALL_MARK == header->Mark || MEDIUM_MARK == header->Mark);
        return header;
    }

    static inline const TListHeader* GetListHeader(const void* addr) {
        const TListHeader* header = reinterpret_cast<const TListHeader*>(TAlignedPagePool::GetPageStart(addr));
        Y_ASSERT(SMALL_MARK == header->Mark || MEDIUM_MARK == header->Mark);
        return header;
    }

    static inline TLargeListHeader* GetLargeListHeader(void* addr) {
        TLargeListHeader* header = reinterpret_cast<TLargeListHeader*>(TAlignedPagePool::GetPageStart(addr));
        Y_ASSERT(LARGE_MARK == header->Mark);
        return header;
    }

    static inline const TLargeListHeader* GetLargeListHeader(const void* addr) {
        const TLargeListHeader* header = reinterpret_cast<const TLargeListHeader*>(TAlignedPagePool::GetPageStart(addr));
        Y_ASSERT(LARGE_MARK == header->Mark);
        return header;
    }

    template <typename T>
    static inline constexpr ui16 GetSmallPageCapacity(size_t listSize) {
        // Always keep at least ui16 per list in order to track collection free lists
        return static_cast<ui16>((TAlignedPagePool::POOL_PAGE_SIZE - sizeof(TListHeader)) / (AlignUp<size_t>(listSize * sizeof(T), sizeof(ui16)))) & 0x7FFF;
    }

    template <typename T>
    static inline constexpr ui16 GetMediumPageCapacity(size_t listSize) {
        return static_cast<ui16>((TAlignedPagePool::POOL_PAGE_SIZE - sizeof(TListHeader)) / (FastClp2(listSize) * sizeof(T) + sizeof(ui16))) & 0x7FFF;
    }

    template <typename T>
    static inline constexpr ui32 GetLargePageCapacity() {
        return static_cast<ui32>((TAlignedPagePool::POOL_PAGE_SIZE - sizeof(TLargeListHeader)) / sizeof(T));
    }

    template <typename T>
    static inline constexpr size_t GetMaxListSize() {
#define NCHASH_RAW_SIZE ((((TAlignedPagePool::POOL_PAGE_SIZE - sizeof(TListHeader)) / 2) - sizeof(ui16)) / sizeof(T))
        // For medium lists get downward pow2 num
        return NCHASH_RAW_SIZE > 16 ? 1ULL << MostSignificantBitCT(NCHASH_RAW_SIZE) : NCHASH_RAW_SIZE;
#undef NCHASH_RAW_SIZE
    }

protected:
    using TListType = TIntrusiveList<TPageListItem>;
    struct TUsedPages {
        TUsedPages() = default;
        TUsedPages(const TUsedPages&) = delete;
        TUsedPages(TUsedPages&& other)
            : SmallPages(std::move(other.SmallPages))
            , MediumPages(std::move(other.MediumPages))
            , FullPages(std::move(other.FullPages))
        {
        }

        TUsedPages& operator =(const TUsedPages& other) = delete;
        TUsedPages& operator =(TUsedPages&& other) {
            TUsedPages(std::move(other)).Swap(*this);
            return *this;
        }

        void Swap(TUsedPages& other) {
            DoSwap(SmallPages, other.SmallPages);
            DoSwap(MediumPages, other.MediumPages);
            DoSwap(FullPages, other.FullPages);
        }

        // Returns count of used pages
        size_t PrintStat(const TStringBuf& header, IOutputStream& out) const;
        TString DebugInfo() const;

        std::array<TListType, MAX_SMALL_LIST_SIZE - 1> SmallPages; // 2-16 sizes
        std::array<TListType, MAX_MEDIUM_LIST_INDEX> MediumPages; // 32,64,128,...,16384. Indexed by pow2
        TListType FullPages;
    };

public:
    TListPoolBase(TAlignedPagePool& pagePool)
        : PagePool_(pagePool)
    {}
    TListPoolBase(const TListPoolBase&) = delete;
    TListPoolBase(TListPoolBase&& other)
        : PagePool_(other.PagePool_)
    {
    }

    TListPoolBase& operator =(const TListPoolBase& other) = delete;
    TListPoolBase& operator =(TListPoolBase&& other) {
        PagePool_.Swap(other.PagePool_);
        return *this;
    }

    void Swap(TListPoolBase& other) {
        PagePool_.Swap(other.PagePool_);
    }

    TAlignedPagePool& GetPagePool() const {
        return PagePool_;
    }

    // Returns full list size for short and medium lists. Returns size of last page for a large list
    static size_t GetPartListSize(const void* list) {
        switch (GetMark(list)) {
        case SMALL_MARK:
            return GetListHeader(list)->ListSize;
        case MEDIUM_MARK:
            return *(reinterpret_cast<const ui16*>(list) - 1);
        case LARGE_MARK:
            return GetLargeListHeader(list)->Size;
        default:
            Y_ABORT("Bad list address");
        }
        return 0;
    }

    static size_t GetFullListSize(const void* list) {
        switch (GetMark(list)) {
        case SMALL_MARK:
            return GetListHeader(list)->ListSize;
        case MEDIUM_MARK:
            return *(reinterpret_cast<const ui16*>(list) - 1);
        case LARGE_MARK:
            {
                const TLargeListHeader* start = GetLargeListHeader(list);
                const TLargeListHeader* next = start;
                size_t size = 0;
                do {
                    size += next->Size;
                    next = next->Next();
                } while (next != start);
                return size;
            }
        default:
            Y_ABORT("Bad list address");
        }
        return 0;
    }

    static size_t GetListCapacity(const void* list) {
        switch (GetMark(list)) {
        case SMALL_MARK:
        case MEDIUM_MARK:
            return GetListHeader(list)->ListSize;
        case LARGE_MARK:
            return GetLargeListHeader(list)->Capacity;
        default:
            Y_ABORT("Bad list address");
        }
        return 0;
    }

    static void SetPartListSize(void* list, size_t size) {
        switch (GetMark(list)) {
        case MEDIUM_MARK:
            *(reinterpret_cast<ui16*>(list) - 1) = size;
            break;
        case LARGE_MARK:
            GetLargeListHeader(list)->Size = size;
            break;
        default:
            Y_ABORT("Bad list address");
        }
    }

    static inline ui16 GetMark(const void* addr) {
        return *reinterpret_cast<const ui16*>(TAlignedPagePool::GetPageStart(addr));
    }

protected:
    void FreeListPage(TListHeader* p);

private:
    TAlignedPagePool& PagePool_;
};

template <typename TPrimary, typename TSecondary = TPrimary>
class TListPool: public TListPoolBase {
private:
    static constexpr size_t PoolCount = 1 + !std::is_same<TPrimary, TSecondary>::value;

public:
    TListPool(TAlignedPagePool& pagePool)
        : TListPoolBase(pagePool)
    {}
    TListPool(const TListPool&) = delete;
    TListPool(TListPool&& other)
        : TListPoolBase(std::move(other))
        , Pools(std::move(other.Pools))
    {
    }

    ~TListPool() {
        for (auto& p: Pools) {
            for (auto& list: p.SmallPages) {
                Y_ABORT_UNLESS(list.Empty(), "%s", DebugInfo().data());
            }
            for (auto& list: p.MediumPages) {
                Y_ABORT_UNLESS(list.Empty(), "%s", DebugInfo().data());
            }
            Y_ABORT_UNLESS(p.FullPages.Empty(), "%s", DebugInfo().data());
        }
    }

    TListPool& operator =(const TListPool& other) = delete;
    TListPool& operator =(TListPool&& other) {
        TListPool(std::move(other)).Swap(*this);
        return *this;
    }

    template <typename T>
    T* GetList(size_t size) {
        static_assert(std::is_same<TPrimary, T>::value || std::is_same<TSecondary, T>::value, "Bad requested list type");
        static constexpr size_t PoolNdx = static_cast<size_t>(!std::is_same<TPrimary, T>::value);
        Y_ABORT_UNLESS(size >= 2);
        T* res = nullptr;

        if (Y_LIKELY(size <= TListPoolBase::GetMaxListSize<T>())) {
            if (Y_LIKELY(size <= MAX_SMALL_LIST_SIZE)) {
                res = GetSmallList<PoolNdx, T>(GetSmallListPage<PoolNdx, T>(size));
            } else {
                res = GetMediumList<PoolNdx, T>(GetMediumListPage<PoolNdx, T>(size), size);
            }
        }

        return res;
    }

    // Returns pointer to the new element
    template <typename T>
    T* IncrementList(T*& list) {
        size_t oldSize = TListPoolBase::GetPartListSize(list);
        if (oldSize + 1 <= TListPoolBase::GetListCapacity(list)) {
            TListPoolBase::SetPartListSize(list, oldSize + 1);
            return list + oldSize;
        }

        TLargeListHeader* header = nullptr;
        T* oldList = list;
        if (Y_LIKELY(LARGE_MARK != GetMark(oldList))) {
            list = GetList<T>(oldSize + 1);
            if (nullptr == list) {
                // Convert to large list
                header = GetLargeListPage<T>();
                list = header->GetList<T>();
                Y_ABORT_UNLESS(header->Capacity >= oldSize);
                header->Size = oldSize;
            }

            if (std::is_trivially_copyable<T>::value) {
                memcpy(list, oldList, sizeof(T) * oldSize);
            } else {
                for (size_t i = 0; i < oldSize; ++i) {
                    ::WriteUnaligned<T>(list + i, ::ReadUnaligned<T>(oldList + i));
                }
            }
            ReturnList(oldList);

            if (nullptr == header) {
                return list + oldSize;
            } else if (header->Size < header->Capacity) {
                return header->GetList<T>() + header->Size++;
            }
        } else {
            header = GetLargeListHeader(list);
        }

        // Add new page to large list
        TLargeListHeader* newPage = GetLargeListPage<T>();
        ++newPage->Size;
        list = newPage->GetList<T>();
        header->Append(newPage);
        return list;
    }

    template <typename T>
    T* CloneList(const T* list) {
        if (Y_LIKELY(LARGE_MARK != GetMark(list))) {
            const size_t size = TListPoolBase::GetPartListSize(list);
            T* clonedList = GetList<T>(size);
            for (size_t i = 0; i < size; ++i) {
                new (clonedList + i) T(list[i]);
            }
            return clonedList;
        } else {
            TLargeListHeader* lastListHeader = nullptr;
            const TLargeListHeader* start = GetLargeListHeader(list)->Next();
            const TLargeListHeader* header = start;
            do {
                TLargeListHeader* newHeader = GetLargeListPage<T>();
                newHeader->Size = header->Size;
                T* newList = newHeader->GetList<T>();
                const T* list = header->GetList<T>();
                for (size_t i = 0; i < header->Size; ++i) {
                    new (newList + i) T(list[i]);
                }
                if (lastListHeader) {
                    lastListHeader->Append(newHeader);
                }
                lastListHeader = newHeader;
                header = header->Next();
            } while (header != start);

            return lastListHeader->GetList<T>();
        }
    }

    template <typename T>
    void ReturnList(T* list) {
        static_assert(std::is_same<TPrimary, T>::value || std::is_same<TSecondary, T>::value, "Bad returned list type");
        static constexpr size_t PoolNdx = static_cast<size_t>(!std::is_same<TPrimary, T>::value);
        switch (GetMark(list)) {
        case SMALL_MARK:
            ReturnSmallList<PoolNdx, T>(GetListHeader(list), list);
            break;
        case MEDIUM_MARK:
            ReturnMediumList<PoolNdx, T>(GetListHeader(list), list);
            break;
        case LARGE_MARK:
            ReturnLargeList<T>(list);
            break;
        default:
            Y_ABORT("Bad list address");
        }
    }

    void Swap(TListPool& other) {
        TListPoolBase::Swap(other);
        DoSwap(Pools, other.Pools);
    }

    void PrintStat(IOutputStream& out) const {
        size_t usedPages = 0;
        if (std::is_same<TPrimary, TSecondary>::value) {
            usedPages = Pools[0].PrintStat(TStringBuf(""), out);
        } else {
            usedPages = Pools[0].PrintStat(TStringBuf("Primary: "), out) + Pools[1].PrintStat(TStringBuf("Secondary: "), out);
        }
        GetPagePool().PrintStat(usedPages, out);
    }

    TString DebugInfo() const {
        if (std::is_same<TPrimary, TSecondary>::value) {
            return Pools[0].DebugInfo();
        } else {
            return TString().append("Primary:\n").append(Pools[0].DebugInfo()).append("Secondary:\n").append(Pools[1].DebugInfo());
        }
    }

private:
    template <typename T>
    static void DestroyRange(T* s, T* e) {
        if (!std::is_trivially_destructible<T>::value) {
            while (s != e) {
                --e;
                e->~T();
            }
        }
    }

    template <size_t PoolNdx, typename T>
    TListHeader* GetSmallListPage(size_t size) {
        Y_ASSERT(size > 1 && size <= MAX_SMALL_LIST_SIZE);
        TListType& pages = Pools[PoolNdx].SmallPages[size - 2];
        if (!pages.Empty()) {
            return pages.Front()->As<TListHeader>();
        }
        ui16 listCount = GetSmallPageCapacity<T>(size);
        Y_ASSERT(listCount >= 2);
        TListHeader* header = new (GetPagePool().GetPage()) TListHeader(SMALL_MARK, size, listCount);
        pages.PushFront(&header->ListItem);
        return header;
    }

    template <size_t PoolNdx, typename T>
    TListHeader* GetMediumListPage(size_t size) {
        Y_ASSERT(size > MAX_SMALL_LIST_SIZE && size <= TListPoolBase::GetMaxListSize<T>());
        size_t index = MostSignificantBit((size - 1) >> MostSignificantBitCT(MAX_SMALL_LIST_SIZE));
        Y_ASSERT(index < Pools[PoolNdx].MediumPages.size());
        TListType& pages = Pools[PoolNdx].MediumPages[index];
        if (!pages.Empty()) {
            return pages.Front()->As<TListHeader>();
        }
        ui16 listCapacity = FastClp2(size);
        ui16 listCount = GetMediumPageCapacity<T>(listCapacity);
        Y_ASSERT(listCount >= 2);
        TListHeader* header = new (GetPagePool().GetPage()) TListHeader(MEDIUM_MARK, listCapacity, listCount);
        pages.PushFront(&header->ListItem);
        return header;
    }

    template <typename T>
    TLargeListHeader* GetLargeListPage() {
        TLargeListHeader* const header = new (GetPagePool().GetPage()) TLargeListHeader(GetLargePageCapacity<T>());
        return header;
    }

    template <size_t PoolNdx, typename T>
    T* GetSmallList(TListHeader* listHeader) {
        if (0 == listHeader->FreeLists) {
            return nullptr;
        }
        const bool last = (0 == --listHeader->FreeLists);
        // Always keep at least ui16 per list in order to track collection of free lists
        const size_t byteListSize = AlignUp<size_t>(sizeof(T) * listHeader->ListSize, sizeof(ui16));
        ui16* l = reinterpret_cast<ui16*>(reinterpret_cast<ui8*>(listHeader + 1) + byteListSize * listHeader->FreeListOffset);
        // Distinguish first (0) and repeatedly (0x8000u) used lists
        if ((*l) & 0x8000u) {
            listHeader->FreeListOffset = ((*l) & 0x7FFF);
        } else {
            ++listHeader->FreeListOffset;
            if (!last) {
                // Mark next free list as first used
                *reinterpret_cast<ui16*>(reinterpret_cast<ui8*>(listHeader + 1) + byteListSize * listHeader->FreeListOffset) = 0u;
            }
        }
        if (last) {
            listHeader->ListItem.Unlink();
            Pools[PoolNdx].FullPages.PushBack(&listHeader->ListItem);
        }
        return reinterpret_cast<T*>(l);
    }

    template <size_t PoolNdx, typename T>
    T* GetMediumList(TListHeader* listHeader, size_t size) {
        if (0 == listHeader->FreeLists) {
            return nullptr;
        }
        const bool last = (0 == --listHeader->FreeLists);
        const size_t byteListSize = sizeof(T) * listHeader->ListSize + sizeof(ui16);
        ui16* l = reinterpret_cast<ui16*>(reinterpret_cast<ui8*>(listHeader + 1) + byteListSize * listHeader->FreeListOffset);
        // Distinguish first (0) and repeatedly (0x8000u) used lists
        if ((*l) & 0x8000u) {
            listHeader->FreeListOffset = ((*l) & 0x7FFF);
        } else {
            ++listHeader->FreeListOffset;
            if (!last) {
                // Mark next free list as first used
                *reinterpret_cast<ui16*>(reinterpret_cast<ui8*>(listHeader + 1) + byteListSize * listHeader->FreeListOffset) = 0u;
            }
        }

        if (last) {
            listHeader->ListItem.Unlink();
            Pools[PoolNdx].FullPages.PushBack(&listHeader->ListItem);
        }
        // For medium pages store the list size ahead
        *l = size;
        ++l;
        return reinterpret_cast<T*>(l);
    }

    template <size_t PoolNdx, typename T>
    void ReturnSmallList(TListHeader* listHeader, T* list) {
        DestroyRange(list, list + listHeader->ListSize);
        const size_t byteListSize = AlignUp<size_t>(sizeof(T) * listHeader->ListSize, sizeof(ui16));
        Y_ASSERT((reinterpret_cast<ui8*>(list) - reinterpret_cast<ui8*>(listHeader + 1)) % byteListSize == 0);
        const ui64 offset = (reinterpret_cast<ui8*>(list) - reinterpret_cast<ui8*>(listHeader + 1)) / byteListSize;
        Y_ASSERT(offset < TAlignedPagePool::POOL_PAGE_SIZE);
        *reinterpret_cast<ui16*>(list) = listHeader->FreeListOffset | 0x8000u;
        listHeader->FreeListOffset = offset;
        ++listHeader->FreeLists;
        if (1 == listHeader->FreeLists) {
            listHeader->ListItem.Unlink(); // Remove from full list
            Pools[PoolNdx].SmallPages[listHeader->ListSize - 2].PushFront(&listHeader->ListItem); // Add to partially used
        } else if (GetSmallPageCapacity<T>(listHeader->ListSize) == listHeader->FreeLists) {
            listHeader->ListItem.Unlink(); // Remove from partially used
            FreeListPage(listHeader);
        }
    }

    template <size_t PoolNdx, typename T>
    void ReturnMediumList(TListHeader* listHeader, T* list) {
        ui16* l = reinterpret_cast<ui16*>(list) - 1;
        DestroyRange(list, list + *l);
        Y_ASSERT((reinterpret_cast<ui8*>(l) - reinterpret_cast<ui8*>(listHeader + 1)) % (listHeader->ListSize * sizeof(T) + sizeof(ui16)) == 0);
        ui64 offset = (reinterpret_cast<ui8*>(l) - reinterpret_cast<ui8*>(listHeader + 1)) / (listHeader->ListSize * sizeof(T) + sizeof(ui16));
        Y_ASSERT(offset < TAlignedPagePool::POOL_PAGE_SIZE);
        *l = listHeader->FreeListOffset | 0x8000u;
        listHeader->FreeListOffset = offset;
        ++listHeader->FreeLists;
        if (1 == listHeader->FreeLists) {
            listHeader->ListItem.Unlink(); // Remove from full list
            const size_t index = MostSignificantBit((listHeader->ListSize - 1) >> MostSignificantBitCT(MAX_SMALL_LIST_SIZE));
            Y_ASSERT(index < Pools[PoolNdx].MediumPages.size());
            Pools[PoolNdx].MediumPages[index].PushFront(&listHeader->ListItem); // Add to partially used
        } else if (GetMediumPageCapacity<T>(listHeader->ListSize) == listHeader->FreeLists) {
            listHeader->ListItem.Unlink(); // Remove from partially used
            FreeListPage(listHeader);
        }
    }

    template <typename T>
    void ReturnLargeList(T* list) {
        TLargeListHeader* header = GetLargeListHeader(list);
        while (!header->ListItem.Empty()) {
            TLargeListHeader* next = header->Next();
            DestroyRange(next->GetList<T>(), next->GetList<T>() + next->Size);
            next->ListItem.Unlink();
            next->~TLargeListHeader();
            GetPagePool().ReturnPage(next);
        }
        DestroyRange(header->GetList<T>(), header->GetList<T>() + header->Size);
        header->~TLargeListHeader();
        GetPagePool().ReturnPage(header);
    }
protected:
    std::array<TUsedPages, PoolCount> Pools;
};

#pragma pack(push, 1)
template <typename T>
struct TNode {
    enum {
        FlagEmpty = 0,
        FlagSingle = 1,
        FlagList = 2,
    };

    ui8 Flag;
    union {
        ui8 D1;
        ui8 D2[Max<size_t>(sizeof(T), sizeof(T*))];
    } Storage;

    TNode()
        : Flag(FlagEmpty)
    {
    }
    TNode(const TNode& n)
        : Flag(n.Flag)
    {
        MemCopy(Storage.D2, n.Storage.D2, sizeof(Storage.D2));
    }
    TNode(TNode&& n)
        : Flag(n.Flag)
    {
        MemCopy(Storage.D2, n.Storage.D2, sizeof(Storage.D2));
        n.Flag = FlagEmpty;
    }

    TNode& operator=(const TNode& n) {
        Flag = n.Flag;
        MemCopy(Storage.D2, n.Storage.D2, sizeof(Storage.D2));
        return *this;
    }

    TListPoolBase::TListIterator<T, TListPoolBase::TLargeListHeader> Iter() {
        if (FlagSingle == Flag) {
            return TListPoolBase::TListIterator<T, TListPoolBase::TLargeListHeader>(reinterpret_cast<T*>(&Storage), reinterpret_cast<T*>(&Storage) + 1);
        } else if (FlagList == Flag) {
            return TListPoolBase::TListIterator<T, TListPoolBase::TLargeListHeader>(*reinterpret_cast<T**>(&Storage));
        }
        return TListPoolBase::TListIterator<T, TListPoolBase::TLargeListHeader>();
    }

    TListPoolBase::TListIterator<const T, const TListPoolBase::TLargeListHeader> Iter() const {
        if (FlagSingle == Flag) {
            return TListPoolBase::TListIterator<const T, const TListPoolBase::TLargeListHeader>(reinterpret_cast<const T*>(&Storage), reinterpret_cast<const T*>(&Storage) + 1);
        } else if (FlagList == Flag) {
            return TListPoolBase::TListIterator<const T, const TListPoolBase::TLargeListHeader>(*reinterpret_cast<const T* const*>(&Storage));
        }
        return TListPoolBase::TListIterator<const T, const TListPoolBase::TLargeListHeader>();
    }

    void Set(T val) {
        Flag = FlagSingle;
        ::WriteUnaligned<T>(&Storage, val);
    }

    void SetList(T* list) {
        Flag = FlagList;
        ::WriteUnaligned<T*>(&Storage, list);
    }

    T Get() const {
        Y_ABORT_UNLESS(FlagSingle == Flag);
        return ::ReadUnaligned<T>(&Storage);
    }

    T* GetList() {
        Y_ABORT_UNLESS(FlagList == Flag);
        return *reinterpret_cast<T**>(&Storage);
    }

    const T* GetList() const {
        Y_ABORT_UNLESS(FlagList == Flag);
        return *reinterpret_cast<const T* const*>(&Storage);
    }

    size_t Size() const {
        if (FlagEmpty == Flag) {
            return 0;
        } else if (FlagSingle == Flag) {
            return 1;
        } else {
            return TListPoolBase::GetFullListSize(GetList());
        }
    }

    void Clear() {
        if (FlagSingle == Flag) {
            reinterpret_cast<T*>(&Storage)->~T();
        }
        Flag = FlagEmpty;
    }
};

template <typename TKey, typename TValue>
struct TKeyValuePair {
    using first_type = TKey;
    using second_type = TValue;

    TKeyValuePair() = default;
    TKeyValuePair(const TKeyValuePair&) = default;
    TKeyValuePair(TKeyValuePair&&) = default;

    TKeyValuePair(const std::pair<TKey, TValue>& p)
        : first(p.first)
        , second(p.second)
    {
    }
    TKeyValuePair(const TKey& k, const TValue& v)
        : first(k)
        , second(v)
    {
    }
    TKeyValuePair(TKey&& k, const TValue& v)
        : first(std::move(k))
        , second(v)
    {
    }
    TKeyValuePair(const TKey& k, TValue&& v)
        : first(k)
        , second(std::move(v))
    {
    }
    TKeyValuePair(TKey&& k, TValue&& v)
        : first(std::move(k))
        , second(std::move(v))
    {
    }

    TKeyValuePair& operator =(const TKeyValuePair&) = default;
    TKeyValuePair& operator =(TKeyValuePair&&) = default;

    TKey first;
    TValue second;
};

template <typename TKey, typename TValue>
using TKeyNodePair = TKeyValuePair<TKey, TNode<TValue>>;

#pragma pack(pop)

template <typename TItemType,
          typename TKeyType,
          typename TKeyExtractor,
          typename TKeyHash,
          typename TKeyEqual,
          typename TSubItemType = TItemType
          >
class TCompactHashBase {
protected:
    using TItemNode = TNode<TItemType>;
    static_assert(sizeof(TItemNode) == 1 + Max<size_t>(sizeof(TItemType), sizeof(void*)), "Unexpected size");

public:
    template <typename T>
    class TIteratorImpl {
        friend class TCompactHashBase;
        using TBucketIter = TListPoolBase::TListIterator<const T, const TListPoolBase::TLargeListHeader>;

        // Full scan iterator
        TIteratorImpl(const TCompactHashBase* hash)
            : Hash(hash)
            , Bucket(0)
            , EndBucket(Hash->BucketsCount_)
            , Pos()
        {
            for (; Bucket < EndBucket; ++Bucket) {
                if (!Hash->IsEmptyBucket(Bucket)) {
                    Pos = Hash->GetBucketIter(Bucket);
                    break;
                }
            }
        }

        // Key iterator
        TIteratorImpl(const TCompactHashBase* hash, size_t bucket, const TBucketIter& pos)
            : Hash(hash)
            , Bucket(bucket)
            , EndBucket(bucket + 1)
            , Pos(pos)
        {
        }

        // Empty iterator
        TIteratorImpl() {
        }

    public:
        TIteratorImpl& operator=(const TIteratorImpl& rhs) {
            Hash = rhs.Hash;
            Bucket = rhs.Bucket;
            EndBucket = rhs.EndBucket;
            Pos = rhs.Pos;
            return *this;
        }

        bool Ok() const {
            return Bucket < EndBucket && Pos.Ok();
        }

        TIteratorImpl& operator++() {
            if (Bucket < EndBucket) {
                if ((++Pos).Ok()) {
                    return *this;
                }

                for (++Bucket; Bucket < EndBucket; ++Bucket) {
                    if (!Hash->IsEmptyBucket(Bucket)) {
                        Pos = Hash->GetBucketIter(Bucket);
                        break;
                    }
                }
            }
            return *this;
        }

        T operator*() const {
            Y_ASSERT(Ok());
            return Pos.Get();
        }

        T Get() const {
            Y_ASSERT(Ok());
            return Pos.Get();
        }

        TIteratorImpl MakeCurrentKeyIter() const {
            Y_ASSERT(Ok());
            return TIteratorImpl(Hash, Bucket, TBucketIter(Pos.GetRaw(), Pos.GetRaw() + sizeof(T)));
        }

    private:
        const TCompactHashBase* Hash = nullptr;
        size_t Bucket = 0;
        size_t EndBucket = 0;
        TBucketIter Pos;
    };

    template <typename T>
    class TIteratorImpl<TKeyNodePair<TKeyType, T>> {
        friend class TCompactHashBase;
        using TBucketIter = TListPoolBase::TListIterator<const TKeyNodePair<TKeyType, T>, const TListPoolBase::TLargeListHeader>;
        using TValueIter = TListPoolBase::TListIterator<const T, const TListPoolBase::TLargeListHeader>;

        // Full scan iterator
        TIteratorImpl(const TCompactHashBase* hash)
            : Hash(hash)
            , Bucket(0)
            , EndBucket(Hash->BucketsCount_)
        {
            for (; Bucket < EndBucket; ++Bucket) {
                if (!Hash->IsEmptyBucket(Bucket)) {
                    Pos = Hash->GetBucketIter(Bucket);
                    SubPos = static_cast<const TKeyNodePair<TKeyType, T>*>(Pos.GetRaw())->second.Iter();
                    break;
                }
            }
        }

        // Key iterator
        TIteratorImpl(const TCompactHashBase* hash, size_t bucket, const TBucketIter& pos)
            : Hash(hash)
            , Bucket(bucket)
            , EndBucket(bucket + 1)
            , Pos(pos)
            , SubPos(static_cast<const TKeyNodePair<TKeyType, T>*>(Pos.GetRaw())->second.Iter())
        {
        }

        // Empty iterator
        TIteratorImpl() {
        }

    public:
        bool Ok() const {
            return Bucket < EndBucket;
        }

        TIteratorImpl& operator++() {
            return Shift(false);
        }

        TIteratorImpl& NextKey() {
            return Shift(true);
        }

        TKeyType GetKey() const {
            Y_ASSERT(Ok());
            return Pos.Get().first;
        }

        T GetValue() const {
            Y_ASSERT(Ok());
            return SubPos.Get();
        }

        T operator*() const {
            return GetValue();
        }

        TIteratorImpl MakeCurrentKeyIter() const {
            Y_ASSERT(Ok());
            return TIteratorImpl(Hash, Bucket, TBucketIter(Pos.GetRaw(), static_cast<const ui8*>(Pos.GetRaw()) + sizeof(TKeyNodePair<TKeyType, T>)));
        }

    private:
        TIteratorImpl& Shift(bool nextKey) {
            Y_ASSERT(Bucket < EndBucket);
            Y_ASSERT(SubPos.Ok());
            if (!nextKey && (++SubPos).Ok()) {
                return *this;
            }

            Y_ASSERT(Pos.Ok());
            if ((++Pos).Ok()) {
                SubPos = static_cast<const TKeyNodePair<TKeyType, T>*>(Pos.GetRaw())->second.Iter();
                return *this;
            } else {
                SubPos = TValueIter();
            }

            for (++Bucket; Bucket < EndBucket; ++Bucket) {
                if (!Hash->IsEmptyBucket(Bucket)) {
                    Pos = Hash->GetBucketIter(Bucket);
                    SubPos = static_cast<const TKeyNodePair<TKeyType, T>*>(Pos.GetRaw())->second.Iter();
                    break;
                }
            }

            return *this;
        }

    private:
        const TCompactHashBase* Hash = nullptr;
        size_t Bucket = 0;
        size_t EndBucket = 0;
        TBucketIter Pos;
        TValueIter SubPos;
    };

public:
    using TIterator = TIteratorImpl<TItemType>;
    using TBucketIterator = TListPoolBase::TListIterator<TItemType, TListPoolBase::TLargeListHeader>;
    using TConstBucketIterator = TListPoolBase::TListIterator<const TItemType, const TListPoolBase::TLargeListHeader>;

    TCompactHashBase(TAlignedPagePool& pagePool, size_t size = 0, const TKeyExtractor& keyExtractor = TKeyExtractor(),
        const TKeyHash& keyHash = TKeyHash(), const TKeyEqual& keyEqual = TKeyEqual())
        : ListPool_(pagePool)
        , KeyExtractor_(keyExtractor)
        , KeyHash_(keyHash)
        , KeyEqual_(keyEqual)
    {
        AllocateBuckets(FindNearestPrime(size));
    }

    TCompactHashBase(const TCompactHashBase& other)
        : Size_(other.Size_)
        , UniqSize_(other.UniqSize_)
        , MaxLoadFactor_(other.MaxLoadFactor_)
        , ListPool_(other.ListPool_.GetPagePool())
        , KeyExtractor_(other.KeyExtractor_)
        , KeyHash_(other.KeyHash_)
        , KeyEqual_(other.KeyEqual_)
    {
        AllocateBuckets(other.BucketsCount_);
        for (size_t i = 0; i < other.BucketsCount_; ++i) {
            auto& b = other.Buckets_[i];
            if (TItemNode::FlagSingle == b.Flag) {
                TItemType item = b.Get();
                UnconditionalInsert(KeyExtractor_(item)).Set(CloneItem(item));
            } else if (TItemNode::FlagList == b.Flag) {
                for (auto it = b.Iter(); it.Ok(); ++it) {
                    UnconditionalInsert(KeyExtractor_(it.Get())).Set(CloneItem(it.Get()));
                }
            }
        }
    }

    TCompactHashBase(TCompactHashBase&& other)
        : Size_(std::move(other.Size_))
        , UniqSize_(std::move(other.UniqSize_))
        , MaxLoadFactor_(std::move(other.MaxLoadFactor_))
        , Buckets_(std::move(other.Buckets_))
        , BucketsCount_(std::move(other.BucketsCount_))
        , BucketsMemory_(std::move(other.BucketsMemory_))
        , ListPool_(std::move(other.ListPool_))
        , KeyExtractor_(std::move(other.KeyExtractor_))
        , KeyHash_(std::move(other.KeyHash_))
        , KeyEqual_(std::move(other.KeyEqual_))
    {
        other.Buckets_ = nullptr;
        other.BucketsMemory_ = 0;
        other.BucketsCount_ = 0;
        other.Size_ = 0;
        other.UniqSize_ = 0;
        other.MaxLoadFactor_ = 1.f;
    }

    ~TCompactHashBase() {
        ClearImpl(true);
    }

    TCompactHashBase& operator= (const TCompactHashBase& other) {
        TCompactHashBase(other).Swap(*this);
        return *this;
    }

    TCompactHashBase& operator= (TCompactHashBase&& other) {
        TCompactHashBase(std::move(other)).Swap(*this);
        return *this;
    }

    void Clear() {
        ClearImpl(false);
    }

    bool Has(const TKeyType& key) const {
        auto& b = Buckets_[GetBucket(key)];
        auto res = FindPos(b, key);
        return res.Ok();
    }

    bool Empty() const {
        return Size_ == 0;
    }

    size_t Size() const {
        return Size_;
    }

    size_t UniqSize() const {
        return UniqSize_;
    }

    const TKeyExtractor& GetKeyExtractor() const {
        return KeyExtractor_;
    }

    const TKeyHash& GetKeyHash() const {
        return KeyHash_;
    }

    const TKeyEqual& GetKeyEqual() const {
        return KeyEqual_;
    }

    float GetMaxLoadFactor() const {
        return MaxLoadFactor_;
    }

    void SetMaxLoadFactor(float factor) {
        Y_ABORT_UNLESS(factor > 0);
        MaxLoadFactor_ = factor;
    }

    float GetLoadFactor() const {
        return float(UniqSize_) / float(BucketsCount_);
    }

    TAlignedPagePool& GetPagePool() const {
        return ListPool_.GetPagePool();
    }

    void Swap(TCompactHashBase& other) {
        DoSwap(Size_, other.Size_);
        DoSwap(UniqSize_, other.UniqSize_);
        DoSwap(MaxLoadFactor_, other.MaxLoadFactor_);
        DoSwap(Buckets_, other.Buckets_);
        DoSwap(BucketsCount_, other.BucketsCount_);
        DoSwap(BucketsMemory_, other.BucketsMemory_);
        DoSwap(ListPool_, other.ListPool_);
        DoSwap(KeyExtractor_, other.KeyExtractor_);
        DoSwap(KeyHash_, other.KeyHash_);
        DoSwap(KeyEqual_, other.KeyEqual_);
    }

    // Full scan
    TIterator Iterate() const {
        return TIterator(this);
    }

    // Key scan
    TIterator Find(const TKeyType& key) const {
        size_t bucket = GetBucket(key);
        auto& b = Buckets_[bucket];
        auto pos = FindPos(b, key);
        if (pos.Ok()) {
            return TIterator(this, bucket, pos);
        }
        return TIterator();
    }

    size_t Count(const TKeyType& key) const {
        size_t bucket = GetBucket(key);
        auto& b = Buckets_[bucket];
        auto pos = FindPos(b, key);
        if (pos.Ok()) {
            return ValueCount(pos.Get());
        }
        return 0;
    }

    size_t GetBucketCount() const {
        return BucketsCount_;
    }

    size_t GetBucket(const TKeyType& key) const {
        return NYql::VaryingHash(KeyHash_(key)) % BucketsCount_;
    }

    bool IsEmptyBucket(size_t bucket) const {
        Y_ASSERT(bucket < BucketsCount_);
        return TItemNode::FlagEmpty == Buckets_[bucket].Flag;
    }

    size_t GetBucketSize(size_t bucket) const {
        Y_ASSERT(bucket < BucketsCount_);
        return Buckets_[bucket].Size();
    }

    TConstBucketIterator GetBucketIter(size_t bucket) const {
        Y_ASSERT(bucket < BucketsCount_);
        const TItemNode& b = Buckets_[bucket];
        return b.Iter();
    }

    bool Rehash(size_t size) {
        if (double(size) / BucketsCount_ <= MaxLoadFactor_) {
            return false;
        }

        TItemNode* oldBuckets = Buckets_;
        size_t oldBucketsCount = BucketsCount_;
        size_t oldBucketsMemory = BucketsMemory_;
        AllocateBuckets(FindNearestPrime(ceil(double(size) / MaxLoadFactor_)));

        Y_DEFER {
            for (size_t i = 0; i < oldBucketsCount; ++i) {
                auto& b = oldBuckets[i];
                if (TItemNode::FlagList == b.Flag) {
                    ListPool_.ReturnList(b.GetList());
                }
                b.Clear();
            }
            FreeBuckets(oldBuckets, oldBucketsMemory);
        };
        try {
            for (size_t i = 0; i < oldBucketsCount; ++i) {
                auto& b = oldBuckets[i];
                if (TItemNode::FlagSingle == b.Flag) {
                    TItemType item = b.Get();
                    UnconditionalInsert(KeyExtractor_(item)).Set(item);
                } else if (TItemNode::FlagList == b.Flag) {
                    for (TBucketIterator it = b.Iter(); it.Ok(); ++it) {
                        UnconditionalInsert(KeyExtractor_(it.Get())).Set(it.Get());
                    }
                }
            }
        } catch (...) {
            DoSwap(oldBuckets, Buckets_);
            DoSwap(oldBucketsCount, BucketsCount_);
            DoSwap(oldBucketsMemory, BucketsMemory_);
            throw;
        }

        return true;
    }

    void PrintStat(IOutputStream& out) const {
        size_t empty = 0;
        size_t single = 0;
        size_t list = 0;
        for (size_t i = 0; i < BucketsCount_; ++i) {
            auto& b = Buckets_[i];
            if (TItemNode::FlagEmpty == b.Flag) {
                ++empty;
            } else if (TItemNode::FlagSingle == b.Flag) {
                ++single;
            } else {
                ++list;
            }
        }
        out << "Empty buckets: " << empty << Endl;
        out << "Single buckets: " << single << Endl;
        out << "List buckets: " << list << Endl;
        ListPool_.PrintStat(out);
    }

protected:
    void ClearImpl(bool fromDtor) {
        for (size_t i = 0; i < BucketsCount_; ++i) {
            ClearNode(Buckets_[i]);
        }
        FreeBuckets(Buckets_, BucketsMemory_);
        Buckets_ = nullptr;
        BucketsCount_ = 0;
        BucketsMemory_ = 0;
        if (!fromDtor) {
            AllocateBuckets(FindNearestPrime(0));
        }

        Size_ = 0;
        UniqSize_ = 0;
    }

    void AllocateBuckets(size_t count) {
        auto bucketsMemory = Max(sizeof(TItemNode) * count, (size_t)TAlignedPagePool::POOL_PAGE_SIZE);
        Buckets_ = (TItemNode*)GetPagePool().GetBlock(bucketsMemory);
        BucketsCount_ = count;
        BucketsMemory_ = bucketsMemory;
        for (size_t i = 0; i < count; ++i) {
            new (&Buckets_[i]) TItemNode();
        }
    }

    void FreeBuckets(TItemNode* buckets, size_t memory) {
        if (!buckets) {
            return;
        }

        GetPagePool().ReturnBlock(buckets, memory);
    }

    TConstBucketIterator FindPos(const TNode<TItemType>& b, const TKeyType& key) const {
        if (TItemNode::FlagEmpty != b.Flag) {
            for (TConstBucketIterator it = b.Iter(); it.Ok(); ++it) {
                if (KeyEqual_(key, KeyExtractor_(it.Get()))) {
                    return TConstBucketIterator(it.GetRaw(), static_cast<const TItemType*>(it.GetRaw()) + 1); // Limit iterator by current key only
                }
            }
        }
        return TConstBucketIterator();
    }

    TBucketIterator FindPos(TNode<TItemType>& b, const TKeyType& key) {
        if (TItemNode::FlagEmpty != b.Flag) {
            for (TBucketIterator it = b.Iter(); it.Ok(); ++it) {
                if (KeyEqual_(key, KeyExtractor_(it.Get()))) {
                    return TBucketIterator(it.GetRaw(), static_cast<TItemType*>(it.GetRaw()) + 1); // Limit iterator by current key only
                }
            }
        }
        return TBucketIterator();
    }

    template <typename T>
    static size_t ValueCount(const TKeyNodePair<TKeyType, T>& item) {
        return item.second.Size();
    }

    template <typename T>
    static size_t ValueCount(const T& /*item*/) {
        return 1;
    }

    template <typename T>
    T CloneItem(const T& src) {
        return src;
    }

    template <typename T>
    TKeyNodePair<TKeyType, T> CloneItem(const TKeyNodePair<TKeyType, T>& src) {
        if (TNode<T>::FlagList == src.second.Flag) {
            TKeyNodePair<TKeyType, T> res(src.first, TNode<T>());
            res.second.SetList(ListPool_.template CloneList<T>(src.second.GetList()));
            return res;
        } else {
            return TKeyNodePair<TKeyType, T>(src.first, src.second);
        }
    }

    template <typename T>
    struct THasNodeValue : public std::false_type {};

    template <typename T>
    struct THasNodeValue<TKeyNodePair<TKeyType, T>> : public std::true_type {};

    template <typename T>
    void ClearNode(TNode<T>& b) {
        if (THasNodeValue<T>::value) {
            for (auto it = b.Iter(); it.Ok(); ++it) {
                ClearItem(*reinterpret_cast<T*>(it.GetRaw()));
            }
        }

        if (TNode<T>::FlagList == b.Flag) {
            ListPool_.ReturnList(b.GetList());
        }
        b.Clear();
    }

    template <typename T>
    void ClearItem(T& /*item*/) {
    }

    template <typename T>
    void ClearItem(TKeyNodePair<TKeyType, T>& item) {
        ClearNode(item.second);
    }

    std::pair<TBucketIterator, bool> InsertOrReplace(TKeyType key) {
        auto b = &Buckets_[GetBucket(key)];
        auto res = FindPos(*b, key);
        if (res.Ok()) {
            return {res, false};
        }

        if (Rehash(UniqSize_ + 1)) {
            // Update bucket after rehashing
            b = &Buckets_[GetBucket(key)];
        }
        TBucketIterator iter = ExpandBucket<TItemType>(*b);
        ++UniqSize_;
        ++Size_;
        return {iter, true};
    }

    TBucketIterator UnconditionalInsert(TKeyType key) {
        return ExpandBucket<TItemType>(Buckets_[GetBucket(key)]);
    }

    template <typename T>
    TListPoolBase::TListIterator<T, TListPoolBase::TLargeListHeader> ExpandBucket(TNode<T>& b) {
        if (TNode<T>::FlagEmpty == b.Flag) {
            b.Flag = TNode<T>::FlagSingle;
            return b.Iter();
        } else if (TNode<T>::FlagSingle == b.Flag) {
            T* list = ListPool_.template GetList<T>(2);
            *list = b.Get();
            b.SetList(list);
            return TListPoolBase::TListIterator<T, TListPoolBase::TLargeListHeader>(reinterpret_cast<ui8*>(list + 1), reinterpret_cast<ui8*>(list + 2));
        } else {
            T* list = b.GetList();
            T* pos = ListPool_.template IncrementList<T>(list);
            b.SetList(list);
            return TListPoolBase::TListIterator<T, TListPoolBase::TLargeListHeader>(reinterpret_cast<ui8*>(pos), reinterpret_cast<ui8*>(pos + 1));
        }
    }

protected:
    size_t Size_ = 0;
    size_t UniqSize_ = 0;
    float MaxLoadFactor_ = 1.f;
    TItemNode* Buckets_ = nullptr;
    size_t BucketsCount_ = 0;
    size_t BucketsMemory_ = 0;
    TListPool<TItemType, TSubItemType> ListPool_;

    TKeyExtractor KeyExtractor_;
    TKeyHash KeyHash_;
    TKeyEqual KeyEqual_;
};

template <typename TKey,
          typename TValue,
          typename TKeyHash = THash<TKey>,
          typename TKeyEqual = TEqualTo<TKey>>
class TCompactHash: public TCompactHashBase<TKeyValuePair<TKey, TValue>, TKey, TSelect1st, TKeyHash, TKeyEqual> {
private:
    static_assert(std::is_trivially_destructible<TKey>::value
        && std::is_trivially_copy_assignable<TKey>::value
        && std::is_trivially_move_assignable<TKey>::value
        && std::is_trivially_copy_constructible<TKey>::value
        && std::is_trivially_move_constructible<TKey>::value
        , "Expected POD key type");
    static_assert(std::is_trivially_destructible<TValue>::value
        && std::is_trivially_copy_assignable<TValue>::value
        && std::is_trivially_move_assignable<TValue>::value
        && std::is_trivially_copy_constructible<TValue>::value
        && std::is_trivially_move_constructible<TValue>::value
        , "Expected POD value type");

    using TItem = TKeyValuePair<TKey, TValue>;
    using TBase = TCompactHashBase<TItem, TKey, TSelect1st, TKeyHash, TKeyEqual>;
public:
    TCompactHash(TAlignedPagePool& pagePool, size_t size = 0, const TKeyHash& keyHash = TKeyHash(), const TKeyEqual& keyEqual = TKeyEqual())
        : TBase(pagePool, size, TSelect1st(), keyHash, keyEqual)
    {
    }

    TCompactHash(const TCompactHash& other)
        : TBase(other)
    {
    }
    TCompactHash(TCompactHash&& other)
        : TBase(std::move(other))
    {
    }

    TCompactHash& operator= (const TCompactHash& rhs) {
        TBase::operator =(rhs);
        return *this;
    }

    TCompactHash& operator= (TCompactHash&& rhs) {
        TBase::operator =(std::move(rhs));
        return *this;
    }

    bool Insert(TItem item) {
        auto res = TBase::InsertOrReplace(TBase::KeyExtractor_(item));
        res.first.Set(item);
        return res.second;
    }

    bool Insert(TKey key, TValue value) {
        auto res = TBase::InsertOrReplace(key);
        res.first.Set(TItem(key, value));
        return res.second;
    }

    bool InsertNew(TItem item) {
        auto res = TBase::InsertOrReplace(TBase::KeyExtractor_(item));
        if (res.second) {
            res.first.Set(item);
        }
        return res.second;
    }

    bool InsertNew(TKey key, TValue value) {
        auto res = TBase::InsertOrReplace(key);
        if (res.second) {
            res.first.Set(TItem(key, value));
        }
        return res.second;
    }
};

template <typename TKey,
          typename TValue,
          typename TKeyHash = THash<TKey>,
          typename TKeyEqual = TEqualTo<TKey>>
class TCompactMultiHash: public TCompactHashBase<TKeyNodePair<TKey, TValue>, TKey, TSelect1st, TKeyHash, TKeyEqual, TValue> {
private:
    static_assert(std::is_trivially_destructible<TKey>::value
        && std::is_trivially_copy_assignable<TKey>::value
        && std::is_trivially_move_assignable<TKey>::value
        && std::is_trivially_copy_constructible<TKey>::value
        && std::is_trivially_move_constructible<TKey>::value
        , "Expected POD key type");
    static_assert(std::is_trivially_destructible<TValue>::value
        && std::is_trivially_copy_assignable<TValue>::value
        && std::is_trivially_move_assignable<TValue>::value
        && std::is_trivially_copy_constructible<TValue>::value
        && std::is_trivially_move_constructible<TValue>::value
        , "Expected POD value type");

    using TUserItem = std::pair<TKey, TValue>;
    using TStoreItem = TKeyNodePair<TKey, TValue>;
    using TBase = TCompactHashBase<TStoreItem, TKey, TSelect1st, TKeyHash, TKeyEqual, TValue>;

    static_assert(sizeof(TStoreItem) == sizeof(TKey) + sizeof(TNode<TValue>), "Unexpected size");

public:
    TCompactMultiHash(TAlignedPagePool& pagePool, size_t size = 0, const TKeyHash& keyHash = TKeyHash(), const TKeyEqual& keyEqual = TKeyEqual())
        : TBase(pagePool, size, TSelect1st(), keyHash, keyEqual)
    {
    }
    TCompactMultiHash(const TCompactMultiHash& other)
        : TBase(other)
    {
    }
    TCompactMultiHash(TCompactMultiHash&& other)
        : TBase(std::move(other))
    {
    }

    TCompactMultiHash& operator= (const TCompactMultiHash& rhs) {
        TBase::operator =(rhs);
        return *this;
    }

    TCompactMultiHash& operator= (TCompactMultiHash&& rhs) {
        TBase::operator =(std::move(rhs));
        return *this;
    }

    void Insert(const TUserItem& item) {
        GetOrInsert(item.first).Set(item.second);
    }

    void Insert(TKey key, TValue value) {
        GetOrInsert(key).Set(value);
    }

protected:
    template <typename TKeyType>
    TListPoolBase::TListIterator<TValue, TListPoolBase::TLargeListHeader> GetOrInsert(TKeyType key) {
        auto res = TBase::InsertOrReplace(key);
        if (res.second) {
            res.first.Set(TStoreItem(key, TNode<TValue>()));
        }
        auto valueIter = TBase::template ExpandBucket<TValue>(reinterpret_cast<TStoreItem*>(res.first.GetRaw())->second);
        if (!res.second) {
            ++TBase::Size_;
        }
        return valueIter;
    }
};

template <typename TKey,
          typename TKeyHash = THash<TKey>,
          typename TKeyEqual = TEqualTo<TKey>>
class TCompactHashSet: public TCompactHashBase<TKey, TKey, TIdentity, TKeyHash, TKeyEqual> {
private:
    static_assert(std::is_trivially_destructible<TKey>::value
        && std::is_trivially_copy_assignable<TKey>::value
        && std::is_trivially_move_assignable<TKey>::value
        && std::is_trivially_copy_constructible<TKey>::value
        && std::is_trivially_move_constructible<TKey>::value
        , "Expected POD key type");

    using TBase = TCompactHashBase<TKey, TKey, TIdentity, TKeyHash, TKeyEqual>;

public:
    TCompactHashSet(TAlignedPagePool& pagePool, size_t size = 0, const TKeyHash& keyHash = TKeyHash(), const TKeyEqual& keyEqual = TKeyEqual())
        : TBase(pagePool, size, TIdentity(), keyHash, keyEqual)
    {
    }
    TCompactHashSet(const TCompactHashSet& other)
        : TBase(other)
    {
    }
    TCompactHashSet(TCompactHashSet&& other)
        : TBase(std::move(other))
    {
    }

    TCompactHashSet& operator= (const TCompactHashSet& rhs) {
        TBase::operator =(rhs);
        return *this;
    }

    TCompactHashSet& operator= (TCompactHashSet&& rhs) {
        TBase::operator =(std::move(rhs));
        return *this;
    }

    bool Insert(TKey key) {
        auto res = TBase::InsertOrReplace(key);
        if (res.second) {
            res.first.Set(key);
        }
        return res.second;
    }
};

} // NCHash

} // NKikimr
