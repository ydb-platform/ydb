#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <util/stream/zerocopy.h>
#include <util/stream/str.h>
#include <util/system/sanitizers.h>
#include <util/system/valgrind.h>

// exactly one of them must be included
#include "rope_cont_embedded_list.h"
//#include "rope_cont_list.h"
//#include "rope_cont_deque.h"

#include "rc_buf.h"

class TRopeAlignedBuffer : public IContiguousChunk {
    static constexpr size_t Alignment = 16;
    static constexpr size_t MallocAlignment = sizeof(size_t);

    ui32 Size;
    const ui32 Capacity;
    const ui32 Offset;
    alignas(Alignment) char Data[];

    TRopeAlignedBuffer(size_t size)
        : Size(size)
        , Capacity(size)
        , Offset((Alignment - reinterpret_cast<uintptr_t>(Data)) & (Alignment - 1))
    {
        Y_ABORT_UNLESS(Offset <= Alignment - MallocAlignment);
    }

public:
    static TIntrusivePtr<TRopeAlignedBuffer> Allocate(size_t size) {
        return new(malloc(sizeof(TRopeAlignedBuffer) + size + Alignment - MallocAlignment)) TRopeAlignedBuffer(size);
    }

    void *operator new(size_t) {
        Y_ABORT();
    }

    void *operator new(size_t, void *ptr) {
        return ptr;
    }

    void operator delete(void *ptr) {
        free(ptr);
    }

    void operator delete(void* p, void* ptr) {
        Y_UNUSED(p);
        Y_UNUSED(ptr);
    }

    TContiguousSpan GetData() const override {
        return {Data + Offset, Size};
    }

    TMutableContiguousSpan GetDataMut() override {
        return {Data + Offset, Size};
    }

    size_t GetOccupiedMemorySize() const override {
        return Capacity;
    }

    size_t GetCapacity() const {
        return Capacity;
    }

    char *GetBuffer() {
        return Data + Offset;
    }
};

namespace NRopeDetails {

    template<bool IsConst, typename TRope, typename TList>
    struct TIteratorTraits;

    template<typename TRope, typename TList>
    struct TIteratorTraits<true, TRope, TList> {
        using TRopePtr = const TRope*;
        using TListIterator = typename TList::const_iterator;
    };

    template<typename TRope, typename TList>
    struct TIteratorTraits<false, TRope, TList> {
        using TRopePtr = TRope*;
        using TListIterator = typename TList::iterator;
    };

} // NRopeDetails

class TRopeArena;

template<typename T>
struct always_false : std::false_type {};

class TRope {
    friend class TRopeArena;

    using TChunkList = NRopeDetails::TChunkList<TRcBuf>;

private:
    // we use list here to store chain items as we have to keep valid iterators when erase/insert operations are invoked;
    // iterator uses underlying container's iterator, so we have to use container that keeps valid iterators on delete,
    // thus, the list
    TChunkList Chain;
    size_t Size = 0;

private:
    template<bool IsConst>
    class TIteratorImpl {
        using TTraits = NRopeDetails::TIteratorTraits<IsConst, TRope, TChunkList>;

        typename TTraits::TRopePtr Rope;
        typename TTraits::TListIterator Iter;
        const char *Ptr; // ptr is always nullptr when iterator is positioned at the rope end

#ifndef NDEBUG
        ui32 ValidityToken;
#endif

    private:
        TIteratorImpl(typename TTraits::TRopePtr rope, typename TTraits::TListIterator iter, const char *ptr = nullptr)
            : Rope(rope)
            , Iter(iter)
            , Ptr(ptr)
#ifndef NDEBUG
            , ValidityToken(Rope->GetValidityToken())
#endif
        {}

    public:
        TIteratorImpl()
            : Rope(nullptr)
            , Ptr(nullptr)
        {}

        template<bool IsOtherConst>
        TIteratorImpl(const TIteratorImpl<IsOtherConst>& other)
            : Rope(other.Rope)
            , Iter(other.Iter)
            , Ptr(other.Ptr)
#ifndef NDEBUG
            , ValidityToken(other.ValidityToken)
#endif
        {}

        void CheckValid() const {
#ifndef NDEBUG
            Y_ABORT_UNLESS(ValidityToken == Rope->GetValidityToken());
            Y_ABORT_UNLESS(Iter == Rope->Chain.end() || Iter->Backend);
#endif
        }

        TIteratorImpl& operator +=(size_t amount) {
            CheckValid();

            while (amount) {
                Y_DEBUG_ABORT_UNLESS(Valid());
                const size_t max = ContiguousSize();
                const size_t num = std::min(amount, max);
                amount -= num;
                Ptr += num;
                if (Ptr == Iter->End) {
                    AdvanceToNextContiguousBlock();
                }
            }

            return *this;
        }

        TIteratorImpl operator +(size_t amount) const {
            CheckValid();

            return TIteratorImpl(*this) += amount;
        }

        TIteratorImpl& operator -=(size_t amount) {
            CheckValid();

            while (amount) {
                const size_t num = Ptr ? std::min<size_t>(amount, Ptr - Iter->Begin) : 0;
                amount -= num;
                Ptr -= num;
                if (amount) {
                    Y_DEBUG_ABORT_UNLESS(Iter != GetChainBegin());
                    --Iter;
                    Ptr = Iter->End;
                }
            }

            return *this;
        }

        TIteratorImpl operator -(size_t amount) const {
            CheckValid();
            return TIteratorImpl(*this) -= amount;
        }

        std::pair<const char*, size_t> operator *() const {
            return {ContiguousData(), ContiguousSize()};
        }

        TIteratorImpl& operator ++() {
            AdvanceToNextContiguousBlock();
            return *this;
        }

        TIteratorImpl operator ++(int) const {
            auto it(*this);
            it.AdvanceToNextContiguousBlock();
            return it;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Operation with contiguous data
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // Get the pointer to the contiguous block of data; valid locations are [Data; Data + Size).
        const char *ContiguousData() const {
            CheckValid();
            return Ptr;
        }

        template<bool Mut = !IsConst, std::enable_if_t<Mut, bool> = true>
        char *ContiguousDataMut() {
            CheckValid();
            return GetChunk().GetDataMut();
        }

        template<bool Mut = !IsConst, std::enable_if_t<Mut, bool> = true>
        char *UnsafeContiguousDataMut() {
            CheckValid();
            return GetChunk().UnsafeGetDataMut();
        }

        // Get the amount of contiguous block.
        size_t ContiguousSize() const {
            CheckValid();
            return Ptr ? Iter->End - Ptr : 0;
        }

        size_t ChunkOffset() const {
            return Ptr ? Ptr - Iter->Begin : 0;
        }

        // Advance to next contiguous block of data.
        void AdvanceToNextContiguousBlock() {
            CheckValid();
            Y_DEBUG_ABORT_UNLESS(Valid());
            ++Iter;
            Ptr = Iter != GetChainEnd() ? Iter->Begin : nullptr;
        }

        // Extract some data and advance. Size is not checked here, to it must be provided valid.
        void ExtractPlainDataAndAdvance(void *buffer, size_t len) {
            CheckValid();

            while (len) {
                Y_DEBUG_ABORT_UNLESS(Ptr);

                // calculate amount of bytes we need to move
                const size_t max = ContiguousSize();
                const size_t num = std::min(len, max);

                // copy data to the buffer and advance buffer pointers
                memcpy(buffer, Ptr, num);
                buffer = static_cast<char*>(buffer) + num;
                len -= num;

                // advance iterator itself
                Ptr += num;
                if (Ptr == Iter->End) {
                    AdvanceToNextContiguousBlock();
                }
            }
        }

        // Checks if the iterator points to the end of the rope or not.
        bool Valid() const {
            CheckValid();
            return Ptr;
        }

        template<bool IsOtherConst>
        bool operator ==(const TIteratorImpl<IsOtherConst>& other) const {
            Y_DEBUG_ABORT_UNLESS(Rope == other.Rope);
            CheckValid();
            other.CheckValid();
            return Iter == other.Iter && Ptr == other.Ptr;
        }

        template<bool IsOtherConst>
        bool operator !=(const TIteratorImpl<IsOtherConst>& other) const {
            CheckValid();
            other.CheckValid();
            return !(*this == other);
        }

    private:
        friend class TRope;

        typename TTraits::TListIterator operator ->() const {
            CheckValid();
            return Iter;
        }

        const TRcBuf& GetChunk() const {
            CheckValid();
            return *Iter;
        }

        template<bool Mut = !IsConst, std::enable_if_t<Mut, bool> = true>
        TRcBuf& GetChunk() {
            CheckValid();
            return *Iter;
        }

        typename TTraits::TListIterator GetChainBegin() const {
            CheckValid();
            return Rope->Chain.begin();
        }

        typename TTraits::TListIterator GetChainEnd() const {
            CheckValid();
            return Rope->Chain.end();
        }

        bool PointsToChunkMiddle() const {
            CheckValid();
            return Ptr && Ptr != Iter->Begin;
        }
    };

public:
#ifndef NDEBUG
    ui32 ValidityToken = 0;
    ui32 GetValidityToken() const { return ValidityToken; }
    void InvalidateIterators() { ++ValidityToken; }
#else
    void InvalidateIterators() {}
#endif

public:
    using TConstIterator = TIteratorImpl<true>;
    using TIterator = TIteratorImpl<false>;

public:
    TRope() = default;
    TRope(const TRope& rope) = default;

    TRope(const TRcBuf& data) {
        if(!data.HasBuffer()) {
            return;
        }
        Size = data.GetSize();
        Chain.PutToEnd(data);
    }

    TRope(TRcBuf&& data) {
        if(!data.HasBuffer()) {
            return;
        }
        Size = data.GetSize();
        Chain.PutToEnd(std::move(data));
    }

    TRope(TRope&& rope)
        : Chain(std::move(rope.Chain))
        , Size(std::exchange(rope.Size, 0))
    {
        rope.InvalidateIterators();
    }

    explicit TRope(TString s) {
        if (s) {
            Size = s.size();
            if (s.capacity() < 32) {
                s.reserve(32);
            }
            Chain.PutToEnd(std::move(s));
        }
    }

    explicit TRope(NActors::TSharedData s) {
        Size = s.size();
        Chain.PutToEnd(std::move(s));
    }

    TRope(IContiguousChunk::TPtr item) {
        Size = item->GetData().size();
        Chain.PutToEnd(std::move(item));
    }

    TRope(TConstIterator begin, TConstIterator end) {
        Y_DEBUG_ABORT_UNLESS(begin.Rope == end.Rope);
        if (begin.Rope == this) {
            TRope temp(begin, end);
            *this = std::move(temp);
            return;
        }

        while (begin.Iter != end.Iter) {
            const size_t size = begin.ContiguousSize();
            Chain.PutToEnd(TRcBuf::Piece, begin.ContiguousData(), size, begin.GetChunk());
            begin.AdvanceToNextContiguousBlock();
            Size += size;
        }

        if (begin != end && end.PointsToChunkMiddle()) {
            Chain.PutToEnd(TRcBuf::Piece, begin.Ptr, end.Ptr, begin.GetChunk());
            Size += end.Ptr - begin.Ptr;
        }
    }

    ~TRope() {
    }

    // creates a copy of rope with chunks with inefficient storage ratio being copied with arena allocator
    static TRope CopySpaceOptimized(TRope&& origin, size_t worstRatioPer1k, TRopeArena& arena);

    TRope& operator=(const TRope& other) {
        Chain = other.Chain;
        Size = other.Size;
        return *this;
    }

    TRope& operator=(TRope&& other) {
        Chain = std::move(other.Chain);
        Size = std::exchange(other.Size, 0);
        InvalidateIterators();
        other.InvalidateIterators();
        return *this;
    }

    size_t GetSize() const {
        return Size;
    }

    size_t size() const {
        return Size;
    }

    size_t capacity() const {
        return Size;
    }

    bool IsEmpty() const {
        return !Size;
    }

    bool empty() const {
        return IsEmpty();
    }

    operator bool() const {
        return Chain;
    }

    TIterator Begin() {
        return *this ? TIterator(this, Chain.begin(), Chain.GetFirstChunk().Begin) : End();
    }

    TIterator End() {
        return TIterator(this, Chain.end());
    }

    TIterator Iterator(TChunkList::iterator it) {
        return TIterator(this, it, it != Chain.end() ? it->Begin : nullptr);
    }

    TIterator Position(size_t index) {
        return Begin() + index;
    }

    TConstIterator Begin() const {
        return *this ? TConstIterator(this, Chain.begin(), Chain.GetFirstChunk().Begin) : End();
    }

    TConstIterator End() const {
        return TConstIterator(this, Chain.end());
    }

    TConstIterator Position(size_t index) const {
        return Begin() + index;
    }

    TConstIterator begin() const { return Begin(); }
    TConstIterator end() const { return End(); }

    TIterator Erase(TIterator begin, TIterator end) {
        return Cut(begin, end, nullptr);
    }

    TRope Extract(TIterator begin, TIterator end) {
        TRope res;
        Cut(begin, end, &res);
        return res;
    }

    void ExtractFront(size_t num, TRope *dest) {
        Y_ABORT_UNLESS(Size >= num);
        if (num == Size && !*dest) {
            *dest = std::move(*this);
            return;
        }
        Size -= num;
        dest->Size += num;

        TChunkList::iterator first = Chain.begin();

        if (num >= first->GetSize() && dest->Chain) { // see if we can glue first chunk to the destination rope
            auto& last = dest->Chain.GetLastChunk();
            if (last.Backend == first->Backend && last.End == first->Begin) {
                last.End = first->End;
                num -= first->GetSize();
                first = Chain.Erase(first);
            }
        }

        TChunkList::iterator it;
        for (it = first; num && num >= it->GetSize(); ++it) {
            num -= it->GetSize();
        }
        first = dest->Chain.Splice(dest->Chain.end(), Chain, first, it);

        if (num) { // still more data to extract
            if (dest->Chain) {
                auto& last = dest->Chain.GetLastChunk();
                if (last.Backend == first->Backend && last.End == first->Begin) {
                    first->Begin += num;
                    last.End = first->Begin;
                    return;
                }
            }
            dest->Chain.PutToEnd(TRcBuf::Piece, first->Begin, first->Begin + num, *first);
            first->Begin += num;
        }
    }

    void Insert(TIterator pos, TRope&& rope) {
        Y_DEBUG_ABORT_UNLESS(this == pos.Rope);
        Y_DEBUG_ABORT_UNLESS(this != &rope);

        if (!rope) {
            return; // do nothing for empty rope
        }

        // adjust size
        Size += std::exchange(rope.Size, 0);

        // check if we have to split the block
        if (pos.PointsToChunkMiddle()) {
            pos.Iter = Chain.InsertBefore(pos.Iter, TRcBuf::Piece, pos->Begin, pos.Ptr, pos.GetChunk());
            ++pos.Iter;
            pos->Begin = pos.Ptr;
        }

        // perform glueing if possible
        TRcBuf *ropeLeft = &rope.Chain.GetFirstChunk();
        TRcBuf *ropeRight = &rope.Chain.GetLastChunk();
        bool gluedLeft = false, gluedRight = false;
        if (pos.Iter != Chain.begin()) { // glue left part whenever possible
            // obtain iterator to previous chunk
            auto prev(pos.Iter);
            --prev;
            if (prev->End == ropeLeft->Begin && prev->Backend == ropeLeft->Backend) { // it is glueable
                prev->End = ropeLeft->End;
                gluedLeft = true;
            }
        }
        if (pos.Iter != Chain.end() && ropeRight->End == pos->Begin && ropeRight->Backend == pos->Backend) {
            pos->Begin = ropeRight->Begin;
            gluedRight = true;
        }
        if (gluedLeft) {
            rope.Chain.EraseFront();
        }
        if (gluedRight) {
            if (rope) {
                rope.Chain.EraseBack();
            } else { // it looks like double-glueing for the same chunk, we have to drop previous one
                auto prev(pos.Iter);
                --prev;
                pos->Begin = prev->Begin;
                pos.Iter = Chain.Erase(prev);
            }
        }
        if (rope) { // insert remains
            Chain.Splice(pos.Iter, rope.Chain, rope.Chain.begin(), rope.Chain.end());
        }
        Y_DEBUG_ABORT_UNLESS(!rope);
        InvalidateIterators();
    }

    void EraseFront(size_t len) {
        Y_DEBUG_ABORT_UNLESS(Size >= len);
        Size -= len;

        while (len) {
            Y_DEBUG_ABORT_UNLESS(Chain);
            TRcBuf& item = Chain.GetFirstChunk();
            const size_t itemSize = item.GetSize();
            if (len >= itemSize) {
                Chain.EraseFront();
                len -= itemSize;
            } else {
                item.Begin += len;
                break;
            }
        }

        InvalidateIterators();
    }

    void EraseBack(size_t len) {
        Y_DEBUG_ABORT_UNLESS(Size >= len);
        Size -= len;

        while (len) {
            Y_DEBUG_ABORT_UNLESS(Chain);
            TRcBuf& item = Chain.GetLastChunk();
            const size_t itemSize = item.GetSize();
            if (len >= itemSize) {
                Chain.EraseBack();
                len -= itemSize;
            } else {
                item.End -= len;
                break;
            }
        }

        InvalidateIterators();
    }

    bool ExtractFrontPlain(void *buffer, size_t len) {
        // check if we have enough data in the rope
        if (Size < len) {
            return false;
        }
        Size -= len;
        while (len) {
            auto& chunk = Chain.GetFirstChunk();
            Y_DEBUG_ABORT_UNLESS(chunk.Backend);
            const size_t num = Min(len, chunk.GetSize());
            memcpy(buffer, chunk.Begin, num);
            buffer = static_cast<char*>(buffer) + num;
            len -= num;
            chunk.Begin += num;
            if (chunk.Begin == chunk.End) {
                Chain.EraseFront();
            }
        }
        InvalidateIterators();
        return true;
    }

    bool FetchFrontPlain(char **ptr, size_t *remain) {
        const size_t num = Min(*remain, Size);
        ExtractFrontPlain(*ptr, num);
        *ptr += num;
        *remain -= num;
        return !*remain;
    }

    static int Compare(const TRope& x, const TRope& y) {
        TConstIterator xIter = x.Begin(), yIter = y.Begin();
        while (xIter.Valid() && yIter.Valid()) {
            const size_t step = std::min(xIter.ContiguousSize(), yIter.ContiguousSize());
            if (int res = memcmp(xIter.ContiguousData(), yIter.ContiguousData(), step)) {
                return res;
            }
            xIter += step;
            yIter += step;
        }
        return xIter.Valid() - yIter.Valid();
    }

    static int Compare(const TRope& x, const TContiguousSpan& y) {
        TConstIterator xIter = x.Begin();
        const char* yData = y.data();
        size_t yOffset = 0;
        while (xIter.Valid() && yOffset != y.size()) {
            const size_t step = std::min(xIter.ContiguousSize(), y.size() - yOffset);
            if (int res = memcmp(xIter.ContiguousData(), yData + yOffset, step)) {
                return res;
            }
            xIter += step;
            yOffset += step;
        }
        return xIter.Valid() - (yOffset != y.size());
    }

    static int Compare(const TContiguousSpan& x, const TRope& y) {
        return -Compare(y, x);
    }

    // Use this method carefully -- it may significantly reduce performance when misused.
    TString ConvertToString() const {
        return ExtractUnderlyingContainerOrCopy<TString>();
    }

    /**
     * WARN: this method supports extracting only for natively supported types for any other type the data *will* be copied
     */
    template <class TResult>
    TResult ExtractUnderlyingContainerOrCopy() const {
        if (Chain.begin() != Chain.end() && ++Chain.begin() == Chain.end()) {
            return Chain.GetFirstChunk().ExtractUnderlyingContainerOrCopy<TResult>();
        }

        const size_t size = GetSize();
        TResult res = TResult::Uninitialized(size);
        char* data = NContiguousDataDetails::TContainerTraits<TResult>::UnsafeGetDataMut(res);
        Begin().ExtractPlainDataAndAdvance(data, size);
        return res;
    }

    void clear() {
        Erase(Begin(), End());
    }

    bool IsContiguous() const {
        if(Begin() == End() || (++Begin() == End())) {
            return true;
        }
        return false;
    }

    void Compact(size_t headroom = 0, size_t tailroom = 0) {
        if(!IsContiguous()) {
            // TODO(innokentii): use better container, when most outer users stop use TString
            TRcBuf res = TRcBuf::Uninitialized(GetSize(), headroom, tailroom);
            Begin().ExtractPlainDataAndAdvance(res.UnsafeGetDataMut(), res.size());
            Erase(Begin(), End());
            Insert(End(), TRope(res));
        }
    }

    static TRope Uninitialized(size_t size)
    {
        TRcBuf res = TRcBuf::Uninitialized(size);
        return TRope(res);
    }

    /**
     * Compacts data and calls GetData() on undelying container
     * WARN: Will copy if data isn't contiguous
     */
    TContiguousSpan GetContiguousSpan() {
        if(Begin() == End()) {
            return {nullptr, 0};
        }
        Compact();
        return Begin()->GetContiguousSpan();
    }

    /**
     * Compacts data and calls GetDataMut() on undelying container
     * WARN: Will copy if data isn't contiguous
     */
    TMutableContiguousSpan GetContiguousSpanMut() {
        if(Begin() == End()) {
            return {nullptr, 0};
        }
        Compact();
        return Begin()->GetContiguousSpanMut();
    }

    /**
     * Compacts data and calls UnsafeGetDataMut() on undelying container
     * WARN: Will copy if data isn't contiguous
     * WARN: Even if underlying container is shared - returns reference to its underlying data
     */
    TMutableContiguousSpan UnsafeGetContiguousSpanMut() {
        if(Begin() == End()) {
            return {nullptr, 0};
        }
        Compact();
        return Begin()->UnsafeGetContiguousSpanMut();
    }

    TString DebugString() const {
        TStringStream s;
        s << "{Size# " << Size;
        for (const auto& chunk  : Chain) {
            const char *data;
            data = chunk.Backend.GetData().data();
            s << " [" << chunk.Begin - data << ", " << chunk.End - data << ")@" << chunk.Backend.UniqueId();
        }
        s << "}";
        return s.Str();
    }

    explicit operator TRcBuf() {
        if(GetSize() == 0) {
            return TRcBuf();
        }
        Compact();
        return TRcBuf(Begin().GetChunk());
    }

    size_t GetOccupiedMemorySize() const;

    friend bool operator==(const TRope& x, const TRope& y) { return Compare(x, y) == 0; }
    friend bool operator!=(const TRope& x, const TRope& y) { return Compare(x, y) != 0; }
    friend bool operator< (const TRope& x, const TRope& y) { return Compare(x, y) <  0; }
    friend bool operator<=(const TRope& x, const TRope& y) { return Compare(x, y) <= 0; }
    friend bool operator> (const TRope& x, const TRope& y) { return Compare(x, y) >  0; }
    friend bool operator>=(const TRope& x, const TRope& y) { return Compare(x, y) >= 0; }

    friend bool operator==(const TRope& x, const TContiguousSpan& y) { return Compare(x, y) == 0; }
    friend bool operator!=(const TRope& x, const TContiguousSpan& y) { return Compare(x, y) != 0; }
    friend bool operator< (const TRope& x, const TContiguousSpan& y) { return Compare(x, y) <  0; }
    friend bool operator<=(const TRope& x, const TContiguousSpan& y) { return Compare(x, y) <= 0; }
    friend bool operator> (const TRope& x, const TContiguousSpan& y) { return Compare(x, y) >  0; }
    friend bool operator>=(const TRope& x, const TContiguousSpan& y) { return Compare(x, y) >= 0; }

    friend bool operator==(const TContiguousSpan& x, const TRope& y) { return Compare(x, y) == 0; }
    friend bool operator!=(const TContiguousSpan& x, const TRope& y) { return Compare(x, y) != 0; }
    friend bool operator< (const TContiguousSpan& x, const TRope& y) { return Compare(x, y) <  0; }
    friend bool operator<=(const TContiguousSpan& x, const TRope& y) { return Compare(x, y) <= 0; }
    friend bool operator> (const TContiguousSpan& x, const TRope& y) { return Compare(x, y) >  0; }
    friend bool operator>=(const TContiguousSpan& x, const TRope& y) { return Compare(x, y) >= 0; }

    // FIXME(innokentii) temporary hack
    friend bool operator==(const TRope& x, const TRcBuf& y) { return Compare(x, y.GetContiguousSpan()) == 0; }
    friend bool operator!=(const TRope& x, const TRcBuf& y) { return Compare(x, y.GetContiguousSpan()) != 0; }
    friend bool operator< (const TRope& x, const TRcBuf& y) { return Compare(x, y.GetContiguousSpan()) <  0; }
    friend bool operator<=(const TRope& x, const TRcBuf& y) { return Compare(x, y.GetContiguousSpan()) <= 0; }
    friend bool operator> (const TRope& x, const TRcBuf& y) { return Compare(x, y.GetContiguousSpan()) >  0; }
    friend bool operator>=(const TRope& x, const TRcBuf& y) { return Compare(x, y.GetContiguousSpan()) >= 0; }

    friend bool operator==(const TRcBuf& x, const TRope& y) { return Compare(x.GetContiguousSpan(), y) == 0; }
    friend bool operator!=(const TRcBuf& x, const TRope& y) { return Compare(x.GetContiguousSpan(), y) != 0; }
    friend bool operator< (const TRcBuf& x, const TRope& y) { return Compare(x.GetContiguousSpan(), y) <  0; }
    friend bool operator<=(const TRcBuf& x, const TRope& y) { return Compare(x.GetContiguousSpan(), y) <= 0; }
    friend bool operator> (const TRcBuf& x, const TRope& y) { return Compare(x.GetContiguousSpan(), y) >  0; }
    friend bool operator>=(const TRcBuf& x, const TRope& y) { return Compare(x.GetContiguousSpan(), y) >= 0; }

private:
    TIterator Cut(TIterator begin, TIterator end, TRope *target) {
        // ensure all iterators are belong to us
        Y_DEBUG_ABORT_UNLESS(this == begin.Rope && this == end.Rope);
        begin.CheckValid();
        end.CheckValid();

        // if begin and end are equal, we do nothing -- checking this case allows us to find out that begin does not
        // point to End(), for example
        if (begin == end) {
            return end;
        }

        auto addBlock = [&](const TRcBuf& from, const char *begin, const char *end) {
            if (target) {
                target->Chain.PutToEnd(TRcBuf::Piece, begin, end, from);
                target->Size += end - begin;
            }
            Size -= end - begin;
        };

        // consider special case -- when begin and end point to the same block; in this case we have to split up this
        // block into two parts
        if (begin.Iter == end.Iter) {
            TRcBuf chunkToSplit = begin.GetChunk();
            addBlock(chunkToSplit, begin.Ptr, end.Ptr);
            const char *firstChunkBegin = begin.PointsToChunkMiddle() ? begin->Begin : nullptr;
            begin->Begin = end.Ptr; // this affects both begin and end iterator pointed values
            if (firstChunkBegin) {
                end.Iter = ++Chain.InsertBefore(begin.Iter, TRcBuf::Piece, firstChunkBegin, begin.Ptr, chunkToSplit);
            }
        } else {
            // check the first iterator -- if it starts not from the begin of the block, we have to adjust end of the
            // first block to match begin iterator and switch to next block
            if (begin.PointsToChunkMiddle()) {
                addBlock(begin.GetChunk(), begin.Ptr, begin->End);
                begin->End = begin.Ptr;
                begin.AdvanceToNextContiguousBlock();
            }

            // now drop full blocks
            size_t rangeSize = 0;
            for (auto it = begin.Iter; it != end.Iter; ++it) {
                Y_DEBUG_ABORT_UNLESS(it->GetSize());
                rangeSize += it->GetSize();
            }
            if (rangeSize) {
                if (target) {
                    end.Iter = target->Chain.Splice(target->Chain.end(), Chain, begin.Iter, end.Iter);
                    target->Size += rangeSize;
                } else {
                    end.Iter = Chain.Erase(begin.Iter, end.Iter);
                }
                Size -= rangeSize;
            }

            // and cut the last block if necessary
            if (end.PointsToChunkMiddle()) {
                addBlock(end.GetChunk(), end->Begin, end.Ptr);
                end->Begin = end.Ptr;
            }
        }

        InvalidateIterators();
        return {this, end.Iter, end.Ptr};
    }
};

class TRopeArena {
    using TAllocateCallback = std::function<TIntrusivePtr<IContiguousChunk>()>;

    TAllocateCallback Allocator;
    TRope Arena;

public:
    TRopeArena(TAllocateCallback&& allocator)
        : Allocator(std::move(allocator))
    {}

    TRope CreateRope(const void *buffer, size_t len) {
        TRope res;

        while (len) {
            if (Arena) {
                auto iter = Arena.Begin();
                Y_DEBUG_ABORT_UNLESS(iter.Valid());
                char *dest = const_cast<char*>(iter.ContiguousData());
                const size_t bytesToCopy = std::min(len, iter.ContiguousSize());
                memcpy(dest, buffer, bytesToCopy);
                buffer = static_cast<const char*>(buffer) + bytesToCopy;
                len -= bytesToCopy;
                res.Insert(res.End(), Arena.Extract(Arena.Begin(), Arena.Position(bytesToCopy)));
            } else {
                Arena.Insert(Arena.End(), TRope(Allocator()));
            }
        }

        // align arena on 8-byte boundary
        const size_t align = 8;
        if (const size_t padding = Arena.GetSize() % align) {
            Arena.EraseFront(padding);
        }

        return res;
    }
};

struct TRopeUtils {
    static void Memset(TRope::TConstIterator dst, char c, size_t size) {
        while (size) {
            Y_DEBUG_ABORT_UNLESS(dst.Valid());
            size_t len = std::min(size, dst.ContiguousSize());
            memset(const_cast<char*>(dst.ContiguousData()), c, len);
            dst += len;
            size -= len;
        }
    }

    static void Memcpy(TRope::TConstIterator dst, TRope::TConstIterator src, size_t size) {
        while (size) {
            Y_DEBUG_ABORT_UNLESS(dst.Valid() && src.Valid(),
                    "Invalid iterator in memcpy: dst.Valid() - %" PRIu32 ", src.Valid() - %" PRIu32,
                      (ui32)dst.Valid(), (ui32)src.Valid());
            size_t len = std::min(size, std::min(dst.ContiguousSize(), src.ContiguousSize()));
            memcpy(const_cast<char*>(dst.ContiguousData()), src.ContiguousData(), len);
            dst += len;
            src += len;
            size -= len;
        }
    }

    static void Memcpy(TRope::TConstIterator dst, const char* src, size_t size) {
        while (size) {
            Y_DEBUG_ABORT_UNLESS(dst.Valid());
            size_t len = std::min(size, dst.ContiguousSize());
            memcpy(const_cast<char*>(dst.ContiguousData()), src, len);
            size -= len;
            dst += len;
            src += len;
        }
    }

    static void Memcpy(char* dst, TRope::TConstIterator src, size_t size) {
        while (size) {
            Y_DEBUG_ABORT_UNLESS(src.Valid());
            size_t len = std::min(size, src.ContiguousSize());
            memcpy(dst, src.ContiguousData(), len);
            size -= len;
            dst += len;
            src += len;
        }
    }

    // copy less or equal to sizeBound bytes, until src is valid
    static size_t SafeMemcpy(char* dst, TRope::TIterator src, size_t sizeBound) {
        size_t origSize = sizeBound;
        while (sizeBound && src.Valid()) {
            size_t len = Min(sizeBound, src.ContiguousSize());
            memcpy(dst, src.ContiguousData(), len);
            sizeBound -= len;
            dst += len;
            src += len;
        }
        return origSize - sizeBound;
    }
};

template<size_t BLOCK, size_t ALIGN = 16>
class TRopeSlideView {
    alignas(ALIGN) char Slide[BLOCK]; // use if distance from current point and next chunk is less than BLOCK
    TRope::TIterator Position; // current position at rope
    size_t Size;
    char* Head; // points to data, it might be current rope chunk or Slide

private:
    void FillBlock() {
        size_t chunkSize = Position.ContiguousSize();
        if (chunkSize >= BLOCK) {
            Size = chunkSize;
            Head = const_cast<char*>(Position.ContiguousData());
        } else {
            Size = TRopeUtils::SafeMemcpy(Slide, Position, BLOCK);
            Head = Slide;
        }
    }

public:
    TRopeSlideView(TRope::TIterator position)
        : Position(position)
    {
        FillBlock();
    }

    TRopeSlideView(TRope &rope)
        : TRopeSlideView(rope.Begin())
    {}

    // if view on slide then copy slide to rope
    void FlushBlock() {
        if (Head == Slide) {
            TRopeUtils::Memcpy(Position, Head, Size);
        }
    }

    TRope::TIterator operator+=(size_t amount) {
        Position += amount;
        FillBlock();
        return Position;
    }

    TRope::TIterator GetPosition() const {
        return Position;
    }

    char* GetHead() const {
        return Head;
    }

    ui8* GetUi8Head() const {
        return reinterpret_cast<ui8*>(Head);
    }

    size_t ContiguousSize() const {
        return Size;
    }

    bool IsOnChunk() const {
        return Head != Slide;
    }
};

class TRopeZeroCopyInput : public IZeroCopyInput {
    TRope::TConstIterator Iter;
    const char* Data = nullptr;
    size_t Len = 0;

private:
    size_t DoNext(const void** ptr, size_t len) override {
        Y_DEBUG_ABORT_UNLESS(ptr);
        if (Len == 0) {
            if (Iter.Valid()) {
                Data = Iter.ContiguousData();
                Len = Iter.ContiguousSize();
                Y_DEBUG_ABORT_UNLESS(Len);
                Y_DEBUG_ABORT_UNLESS(Data);
                ++Iter;
            } else {
                Data = nullptr;
            }
        }

        size_t chunk = std::min(Len, len);
        *ptr = Data;
        Data += chunk;
        Len -= chunk;
        return chunk;
    }

public:
    explicit TRopeZeroCopyInput(TRope::TConstIterator iter)
        : Iter(iter)
    {
    }
};

inline TRope TRope::CopySpaceOptimized(TRope&& origin, size_t worstRatioPer1k, TRopeArena& arena) {
    TRope res;
    for (TRcBuf& chunk : origin.Chain) {
        size_t ratio = chunk.GetSize() * 1024 / chunk.GetOccupiedMemorySize();
        if (ratio < 1024 - worstRatioPer1k) {
            res.Insert(res.End(), arena.CreateRope(chunk.Begin, chunk.GetSize()));
        } else {
            res.Chain.PutToEnd(std::move(chunk));
        }
    }
    res.Size = origin.Size;
    origin = TRope();
    return res;
}


#if defined(WITH_VALGRIND) || defined(_msan_enabled_)

inline void CheckRopeIsDefined(TRope::TConstIterator begin, ui64 size) {
    while (size) {
        ui64 contiguousSize = Min(size, begin.ContiguousSize());
#   if defined(WITH_VALGRIND)
        VALGRIND_CHECK_MEM_IS_DEFINED(begin.ContiguousData(), contiguousSize);
#   endif
#   if defined(_msan_enabled_)
        NSan::CheckMemIsInitialized(begin.ContiguousData(), contiguousSize);
#   endif
        size -= contiguousSize;
        begin += contiguousSize;
    }
}

#   define CHECK_ROPE_IS_DEFINED(begin, size) CheckRopeIsDefined(begin, size)

#else

#   define CHECK_ROPE_IS_DEFINED(begin, size) do {} while (false)

#endif
