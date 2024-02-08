#pragma once

#include <util/generic/intrlist.h>

#include <util/random/random.h>

namespace NRopeDetails {

template<typename TChunk>
class TChunkList {
    struct TItem : TChunk {
        TItem *Next = nullptr;
        TItem *Prev = nullptr;
#ifndef NDEBUG
        ui64 ValidityToken = RandomNumber<ui64>();
#endif

        template<typename... TArgs> TItem(TArgs&&... args) : TChunk(std::forward<TArgs>(args)...) {}

        ~TItem() {
            Invalidate();
            if (IsInUse()) {
                Unlink();
            }
        }

        void LinkBefore(TItem *item) {
            Next = item;
            Prev = item->Prev;
            Next->Prev = Prev->Next = this;
        }

        void Unlink() {
            Next->Prev = Prev;
            Prev->Next = Next;
        }

        bool IsInUse() const {
            return Next != nullptr;
        }

        void ClearSingleItem() {
            Y_DEBUG_ABORT_UNLESS(Next == this && Prev == this);
            static_cast<TChunk&>(*this) = {};
            Next = Prev = nullptr;
        }

        template<typename... TArgs>
        TItem *PrepareForUse(TArgs&&... args) {
            Y_DEBUG_ABORT_UNLESS(!IsInUse());
            static_cast<TChunk&>(*this) = TChunk(std::forward<TArgs>(args)...);
            Next = Prev = this;
            Invalidate();
            return this;
        }

        static void TransferRange(TItem *insertBefore, TItem *first, TItem *last) { // [first, last] -> insertBefore
            first->Prev->Next = last->Next;
            last->Next->Prev = first->Prev;
            first->Prev = insertBefore->Prev;
            last->Next = insertBefore;
            first->Prev->Next = first;
            last->Next->Prev = last;
        }

        void Invalidate() {
#ifndef NDEBUG
            ValidityToken = RandomNumber<ui64>();
#endif
        }
    };

    // There are three possible states for the list:
    // 1. It is empty. Next = Prev = nullptr, TChunk is default-constructed.
    // 2. It contains single item. Next = Prev = &Root, TChunk contains data.
    // 3. It has more than one item. Next and Prev make up a double-linked list starting with Root item; TChunk contains
    //    first item.
    // This container scheme leads to the following properties:
    // 1. Deleting first item in the list invalidates iterators to the first two items.
    // 2. Inserting something before the first item also invalidates iterators to the first two items.
    // This happens because Root is always the first element of the list and when inserting before the Root, we have to
    // shift original Root element to the allocated item and replace Root with newly inserted value.
    // This also makes right-to-left traversing more efficient in some cases.
    TItem Root;

    template<typename... TArgs>
    TItem *AllocateItem(TArgs&&... args) {
        return Root.IsInUse()
            ? new TItem{std::forward<TArgs>(args)...}
            : Root.PrepareForUse(std::forward<TArgs>(args)...);
    }

private:
    template<bool IsConst>
    class TIterator {
        friend class TChunkList;

        using TChunkListType = std::conditional_t<IsConst, const TChunkList, TChunkList>;
        using TItemType = std::conditional_t<IsConst, const TItem, TItem>;
        using TChunkType = std::conditional_t<IsConst, const TChunk, TChunk>;

        TChunkListType *Cont = nullptr;
        TItemType *Item = nullptr;
#ifndef NDEBUG
        ui64 ValidityToken = 0;
#endif

    private:
        TIterator(TChunkListType *cont, TItemType *item)
            : Cont(cont)
            , Item(item)
        {
            UpdateValidityToken();
        }

    public:
        TIterator() = default;

        template<bool OtherIsConst, typename = std::enable_if_t<OtherIsConst <= IsConst>>
        TIterator(const TIterator<OtherIsConst>& other)
            : Cont(other.Cont)
            , Item(other.Item)
        {
            UpdateValidityToken();
        }

        TChunkType& operator *() const {
            CheckValid();
            return *Item;
        }

        TChunkType *operator ->() const {
            CheckValid();
            return Item;
        }

        TIterator& operator++() {
            CheckValid();
            Y_DEBUG_ABORT_UNLESS(Item);
            Item = Item->Next;
            if (Item == &Cont->Root) {
                Item = nullptr; // make it end
            }
            UpdateValidityToken();
            return *this;
        }

        TIterator operator ++(int) {
            TIterator res(*this);
            ++*this;
            return res;
        }

        TIterator& operator--() {
            CheckValid();
            if (!Item) {
                Y_DEBUG_ABORT_UNLESS(*Cont);
                Item = Cont->Root.Prev;
            } else {
                Y_DEBUG_ABORT_UNLESS(Item != &Cont->Root);
                Item = Item->Prev;
            }
            UpdateValidityToken();
            return *this;
        }

        TIterator operator --(int) {
            TIterator res(*this);
            --*this;
            return res;
        }

        friend bool operator ==(const TIterator& x, const TIterator& y) {
            Y_DEBUG_ABORT_UNLESS(x.Cont == y.Cont);
            x.CheckValid();
            y.CheckValid();
            return x.Item == y.Item;
        }

        friend bool operator !=(const TIterator& x, const TIterator& y) {
            return !(x == y);
        }

    private:
        void CheckValid() const {
#ifndef NDEBUG
            Y_DEBUG_ABORT_UNLESS(ValidityToken == (Item ? Item->ValidityToken : 0));
            Y_DEBUG_ABORT_UNLESS(Cont && (Item != &Cont->Root || *Cont));
#endif
        }

        void UpdateValidityToken() {
#ifndef NDEBUG
            ValidityToken = Item ? Item->ValidityToken : 0;
#endif
            CheckValid();
        }
    };

public:
    using iterator = TIterator<false>;
    using const_iterator = TIterator<true>;

public:
    TChunkList()
    {}

    ~TChunkList() {
        Erase(begin(), end());
        Y_DEBUG_ABORT_UNLESS(!*this);
    }

    TChunkList(const TChunkList& other) {
        *this = other;
    }

    TChunkList(TChunkList&& other) {
        *this = std::move(other);
    }

    TChunkList& operator=(const TChunkList& other) {
        if (this != &other) {
            Erase(begin(), end());
            for (const TChunk& chunk : other) {
                PutToEnd(TChunk(chunk));
            }
        }
        return *this;
    }

    TChunkList& operator=(TChunkList&& other) {
        if (this != &other) {
            Erase(begin(), end());
            Y_DEBUG_ABORT_UNLESS(!*this);
            if (other.Root.IsInUse()) { // do we have something to move?
                Root.PrepareForUse(std::move(static_cast<TChunk&>(other.Root)));
                if (other.Root.Next != &other.Root) { // does other contain more than one item?
                    TItem::TransferRange(&Root, other.Root.Next, other.Root.Prev);
                }
                other.Root.ClearSingleItem();
            }
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
        if (item == &Root) {
            // this is the first item, we don't do anything about it
        } else if (pos.Item != &Root) {
            item->LinkBefore(pos.Item ? pos.Item : &Root);
        } else {
            item->LinkBefore(Root.Next);
            std::swap(static_cast<TChunk&>(*item), static_cast<TChunk&>(Root));
            item = &Root;
            Root.Invalidate();
        }
        return {this, item};
    }

    iterator Erase(iterator pos) {
        Pop(pos);
        return pos;
    }

    iterator Erase(iterator first, iterator last) {
        if (first == last) {
            return last;
        }
        for (;;) {
            if (last == begin()) {
                EraseFront();
                return begin();
            } else if (--last == first) {
                return Erase(last);
            } else {
                last = Erase(last);
            }
        }
    }

    void EraseFront() {
        PopFront();
    }

    void EraseBack() {
        Y_DEBUG_ABORT_UNLESS(*this);
        if (Root.Prev != &Root) {
            delete Root.Prev;
        } else {
            EraseFront();
        }
    }

    // Splice moves elements from the 'from' list in the range [first, last) to *this, inserting them before 'pos'. It
    // returns iterator of the next remaining item in the 'from' list.
    iterator Splice(iterator pos, TChunkList& from, iterator first, iterator last) {
        if (first == last) { // the source range is empty
            return last;
        }

        const bool fromBegin = first == from.begin();
        if (fromBegin) { // remember we have to transfer the first item before returning
            ++first;
        }

        // 'first' here either equals to 'last' or points to the middle of the 'from' list

        const bool toBegin = pos == begin();
        if (toBegin && first != last) {
            // we are inserting item to the begin of the list, so move the first item of the range; it is important here
            // that 'last' iterator doesn't get invalidated
            pos = InsertBefore(begin(), from.Pop(first));
            ++pos;
        }

        const auto temp = last;
        if (first != last) {
            --last; // set 'last' pointing to the actual last element of the source range

            Y_DEBUG_ABORT_UNLESS(first.Item != &from.Root);
            Y_DEBUG_ABORT_UNLESS(pos.Item != &Root);

            TItem* const firstItem = first.Item;
            TItem* const lastItem = last.Item;
            TItem* const posItem = pos.Item ? pos.Item : &Root;

            TItem::TransferRange(posItem, firstItem, lastItem);

            // adjust 'pos' to point to the first inserted item
            pos = {this, firstItem};
        }

        if (fromBegin) {
            InsertBefore(toBegin ? begin() : pos, from.PopFront());
            return from.begin();
        } else {
            return temp;
        }
    }

    operator bool() const { return Root.IsInUse(); }

    TChunk& GetFirstChunk() { Y_DEBUG_ABORT_UNLESS(*this); return Root; }
    const TChunk& GetFirstChunk() const { Y_DEBUG_ABORT_UNLESS(*this); return Root; }
    TChunk& GetLastChunk() { Y_DEBUG_ABORT_UNLESS(*this); return *Root.Prev; }

    iterator begin() { return *this ? iterator(this, &Root) : end(); }
    const_iterator begin() const { return *this ? const_iterator(this, &Root) : end(); }
    iterator end() { return {this, nullptr}; }
    const_iterator end() const { return {this, nullptr}; }

private:
    TChunk Pop(iterator& pos) {
        pos.CheckValid();
        Y_DEBUG_ABORT_UNLESS(pos.Item);

        if (pos.Item == &Root) {
            TChunk res = PopFront();
            pos = begin();
            return res;
        } else {
            Y_DEBUG_ABORT_UNLESS(pos != end());
            TItem* const item = pos++.Item;
            TChunk res = std::move(static_cast<TChunk&>(*item));
            delete item;
            return res;
        }
    }

    TChunk PopFront() {
        Y_DEBUG_ABORT_UNLESS(*this);
        TChunk res = std::move(static_cast<TChunk&>(Root));
        if (Root.Next != &Root) {
            static_cast<TChunk&>(Root) = std::move(static_cast<TChunk&>(*Root.Next));
            delete Root.Next;
            Root.Invalidate();
        } else {
            Root.ClearSingleItem();
        }
        return res;
    }
};

} // NRopeDetails
