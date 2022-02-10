#pragma once 
 
#include <util/generic/intrlist.h> 
 
namespace NRopeDetails { 
 
template<typename TChunk> 
class TChunkList { 
    struct TItem : TIntrusiveListItem<TItem>, TChunk { 
        // delegating constructor 
        template<typename... TArgs> TItem(TArgs&&... args) : TChunk(std::forward<TArgs>(args)...) {} 
    }; 
 
    using TList = TIntrusiveList<TItem>; 
    TList List; 
 
    static constexpr size_t NumInplaceItems = 2; 
    char InplaceItems[sizeof(TItem) * NumInplaceItems]; 
 
    template<typename... TArgs> 
    TItem *AllocateItem(TArgs&&... args) { 
        for (size_t index = 0; index < NumInplaceItems; ++index) { 
            TItem *chunk = GetInplaceItemPtr(index); 
            if (!TItem::IsInUse(*chunk)) { 
                return new(chunk) TItem(std::forward<TArgs>(args)...); 
            } 
        } 
        return new TItem(std::forward<TArgs>(args)...); 
    } 
 
    void ReleaseItem(TItem *chunk) { 
        if (IsInplaceItem(chunk)) { 
            chunk->~TItem(); 
            TItem::Clear(*chunk); 
        } else { 
            delete chunk; 
        } 
    } 
 
    void ReleaseItems(TList& list) { 
        while (list) { 
            ReleaseItem(list.Front()); 
        } 
    } 
 
    void Prepare() { 
        for (size_t index = 0; index < NumInplaceItems; ++index) { 
            TItem::Clear(*GetInplaceItemPtr(index)); 
        } 
    } 
 
    TItem *GetInplaceItemPtr(size_t index) { return reinterpret_cast<TItem*>(InplaceItems + index * sizeof(TItem)); } 
    bool IsInplaceItem(TItem *chunk) { return chunk >= GetInplaceItemPtr(0) && chunk < GetInplaceItemPtr(NumInplaceItems); } 
 
public: 
    using iterator = typename TList::iterator; 
    using const_iterator = typename TList::const_iterator; 
 
public: 
    TChunkList() { 
        Prepare(); 
    } 
 
    ~TChunkList() { 
        ReleaseItems(List); 
#ifndef NDEBUG 
        for (size_t index = 0; index < NumInplaceItems; ++index) { 
            Y_VERIFY(!TItem::IsInUse(*GetInplaceItemPtr(index))); 
        } 
#endif 
    } 
 
    TChunkList(const TChunkList& other) { 
        Prepare(); 
        for (const TItem& chunk : other.List) { 
            PutToEnd(TChunk(chunk)); 
        } 
    } 
 
    TChunkList(TChunkList&& other) { 
        Prepare(); 
        Splice(end(), other, other.begin(), other.end()); 
    } 
 
    TChunkList& operator=(const TChunkList& other) { 
        if (this != &other) { 
            ReleaseItems(List); 
            for (const TItem& chunk : other.List) { 
                PutToEnd(TChunk(chunk)); 
            } 
        } 
        return *this; 
    } 
 
    TChunkList& operator=(TChunkList&& other) { 
        if (this != &other) { 
            ReleaseItems(List); 
            Splice(end(), other, other.begin(), other.end()); 
        } 
        return *this; 
    } 
 
    template<typename... TArgs> 
    void PutToEnd(TArgs&&... args) { 
        InsertBefore(end(), std::forward<TArgs>(args)...); 
    } 
 
    template<typename... TArgs> 
    iterator InsertBefore(iterator pos, TArgs&&... args) { 
        TItem *item = AllocateItem<TArgs...>(std::forward<TArgs>(args)...); 
        item->LinkBefore(pos.Item()); 
        return item; 
    } 
 
    iterator Erase(iterator pos) { 
        ReleaseItem(&*pos++); 
        return pos; 
    } 
 
    iterator Erase(iterator first, iterator last) { 
        TList temp; 
        TList::Cut(first, last, temp.end()); 
        ReleaseItems(temp); 
        return last; 
    } 
 
    void EraseFront() { 
        ReleaseItem(List.PopFront()); 
    } 
 
    void EraseBack() { 
        ReleaseItem(List.PopBack()); 
    } 
 
    iterator Splice(iterator pos, TChunkList& from, iterator first, iterator last) { 
        for (auto it = first; it != last; ) { 
            if (from.IsInplaceItem(&*it)) { 
                TList::Cut(first, it, pos); 
                InsertBefore(pos, std::move(*it)); 
                it = first = from.Erase(it); 
            } else { 
                ++it; 
            } 
        } 
        TList::Cut(first, last, pos); 
        return last; 
    } 
 
    operator bool() const               { return static_cast<bool>(List); } 
    TChunk& GetFirstChunk()             { return *List.Front(); } 
    const TChunk& GetFirstChunk() const { return *List.Front(); } 
    TChunk& GetLastChunk()              { return *List.Back(); } 
    iterator begin()                    { return List.begin(); } 
    const_iterator begin() const        { return List.begin(); } 
    iterator end()                      { return List.end(); } 
    const_iterator end() const          { return List.end(); } 
}; 
 
} // NRopeDetails 
