#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/hash_set.h>
#include <util/stream/str.h>
#include <util/system/sanitizers.h>
#include <util/system/valgrind.h>

// exactly one of them must be included
#include "rope_cont_list.h"
//#include "rope_cont_deque.h"

struct IRopeChunkBackend : TThrRefBase {
    using TData = std::tuple<const char*, size_t>;
    virtual ~IRopeChunkBackend() = default;
    virtual TData GetData() const = 0;
    virtual size_t GetCapacity() const = 0;
    using TPtr = TIntrusivePtr<IRopeChunkBackend>;
};

class TRopeAlignedBuffer : public IRopeChunkBackend {
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
        Y_VERIFY(Offset <= Alignment - MallocAlignment);
    }

public:
    static TIntrusivePtr<TRopeAlignedBuffer> Allocate(size_t size) {
        return new(malloc(sizeof(TRopeAlignedBuffer) + size + Alignment - MallocAlignment)) TRopeAlignedBuffer(size);
    }

    void *operator new(size_t) {
        Y_FAIL();
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

    TData GetData() const override {
        return {Data + Offset, Size};
    }

    size_t GetCapacity() const override {
        return Capacity;
    }

    char *GetBuffer() {
        return Data + Offset;
    }

    void AdjustSize(size_t size) {
        Y_VERIFY(size <= Capacity);
        Size = size;
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

    struct TChunk
    {
        class TBackend {
            enum class EType : uintptr_t {
                STRING,
                ROPE_CHUNK_BACKEND,
            };

            uintptr_t Owner = 0; // lower bits contain type of the owner

        public:
            TBackend() = delete;

            TBackend(const TBackend& other)
                : Owner(Clone(other.Owner))
            {}

            TBackend(TBackend&& other)
                : Owner(std::exchange(other.Owner, 0))
            {}

            TBackend(TString s)
                : Owner(Construct<TString>(EType::STRING, std::move(s)))
            {}

            TBackend(IRopeChunkBackend::TPtr backend)
                : Owner(Construct<IRopeChunkBackend::TPtr>(EType::ROPE_CHUNK_BACKEND, std::move(backend)))
            {}

            ~TBackend() {
                if (Owner) {
                    Destroy(Owner);
                }
            }

            TBackend& operator =(const TBackend& other) {
                if (Owner) {
                    Destroy(Owner);
                }
                Owner = Clone(other.Owner);
                return *this;
            }

            TBackend& operator =(TBackend&& other) {
                if (Owner) {
                    Destroy(Owner);
                }
                Owner = std::exchange(other.Owner, 0);
                return *this;
            }

            bool operator ==(const TBackend& other) const {
                return Owner == other.Owner;
            }

            const void *UniqueId() const {
                return reinterpret_cast<const void*>(Owner);
            }

            const IRopeChunkBackend::TData GetData() const {
                return Visit(Owner, [](EType, auto& value) -> IRopeChunkBackend::TData {
                    using T = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<T, TString>) {
                        return {value.data(), value.size()};
                    } else if constexpr (std::is_same_v<T, IRopeChunkBackend::TPtr>) {
                        return value->GetData();
                    } else {
                        return {};
                    }
                });
            }

            size_t GetCapacity() const {
                return Visit(Owner, [](EType, auto& value) {
                    using T = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<T, TString>) {
                        return value.capacity();
                    } else if constexpr (std::is_same_v<T, IRopeChunkBackend::TPtr>) {
                        return value->GetCapacity();
                    } else {
                        Y_FAIL();
                    }
                });
            }

        private:
            static constexpr uintptr_t TypeMask = (1 << 3) - 1;
            static constexpr uintptr_t ValueMask = ~TypeMask;

            template<typename T>
            struct TObjectHolder {
                struct TWrappedObject : TThrRefBase {
                    T Value;
                    TWrappedObject(T&& value)
                        : Value(std::move(value))
                    {}
                };
                TIntrusivePtr<TWrappedObject> Object;

                TObjectHolder(T&& object)
                    : Object(MakeIntrusive<TWrappedObject>(std::move(object)))
                {}
            };

            template<typename TObject>
            static uintptr_t Construct(EType type, TObject object) {
                if constexpr (sizeof(TObject) <= sizeof(uintptr_t)) {
                    uintptr_t res = 0;
                    new(&res) TObject(std::move(object));
                    Y_VERIFY_DEBUG((res & ValueMask) == res);
                    return res | static_cast<uintptr_t>(type);
                } else {
                    return Construct<TObjectHolder<TObject>>(type, TObjectHolder<TObject>(std::move(object)));
                }
            }

            template<typename TCallback>
            static std::invoke_result_t<TCallback, EType, TString&> VisitRaw(uintptr_t value, TCallback&& callback) {
                Y_VERIFY_DEBUG(value);
                const EType type = static_cast<EType>(value & TypeMask);
                value &= ValueMask;
                auto caller = [&](auto& value) { return std::invoke(std::forward<TCallback>(callback), type, value); };
                auto wrapper = [&](auto& value) {
                    using T = std::decay_t<decltype(value)>;
                    if constexpr (sizeof(T) <= sizeof(uintptr_t)) {
                        return caller(value);
                    } else {
                        return caller(reinterpret_cast<TObjectHolder<T>&>(value));
                    }
                };
                switch (type) {
                    case EType::STRING:             return wrapper(reinterpret_cast<TString&>(value));
                    case EType::ROPE_CHUNK_BACKEND: return wrapper(reinterpret_cast<IRopeChunkBackend::TPtr&>(value));
                }
                Y_FAIL("Unexpected type# %" PRIu64, static_cast<ui64>(type));
            }

            template<typename TCallback>
            static std::invoke_result_t<TCallback, EType, TString&> Visit(uintptr_t value, TCallback&& callback) {
                return VisitRaw(value, [&](EType type, auto& value) {
                    return std::invoke(std::forward<TCallback>(callback), type, Unwrap(value));
                });
            }

            template<typename T> static T& Unwrap(T& object) { return object; }
            template<typename T> static T& Unwrap(TObjectHolder<T>& holder) { return holder.Object->Value; }

            static uintptr_t Clone(uintptr_t value) {
                return VisitRaw(value, [](EType type, auto& value) { return Construct(type, value); });
            }

            static void Destroy(uintptr_t value) {
                VisitRaw(value, [](EType, auto& value) { CallDtor(value); });
            }

            template<typename T>
            static void CallDtor(T& value) {
                value.~T();
            }
        };

        TBackend Backend; // who actually holds the data
        const char *Begin; // data start
        const char *End; // data end

        static constexpr struct TSlice {} Slice{};

        template<typename T>
        TChunk(T&& backend, const IRopeChunkBackend::TData& data)
            : Backend(std::move(backend))
            , Begin(std::get<0>(data))
            , End(Begin + std::get<1>(data))
        {
            Y_VERIFY_DEBUG(Begin != End);
        }

        TChunk(TString s)
            : Backend(std::move(s))
        {
            size_t size;
            std::tie(Begin, size) = Backend.GetData();
            End = Begin + size;
        }

        TChunk(IRopeChunkBackend::TPtr backend)
            : TChunk(backend, backend->GetData())
        {}

        TChunk(TSlice, const char *data, size_t size, const TChunk& from)
            : TChunk(from.Backend, {data, size})
        {}

        TChunk(TSlice, const char *begin, const char *end, const TChunk& from)
            : TChunk(Slice, begin, end - begin, from)
        {}

        explicit TChunk(const TChunk& other)
            : Backend(other.Backend)
            , Begin(other.Begin)
            , End(other.End)
        {}

        TChunk(TChunk&& other)
            : Backend(std::move(other.Backend))
            , Begin(other.Begin)
            , End(other.End)
        {}

        TChunk& operator =(const TChunk&) = default;
        TChunk& operator =(TChunk&&) = default;

        size_t GetSize() const {
            return End - Begin;
        }

        static void Clear(TChunk& chunk) {
            chunk.Begin = nullptr;
        }

        static bool IsInUse(const TChunk& chunk) {
            return chunk.Begin != nullptr;
        }

        size_t GetCapacity() const {
            return Backend.GetCapacity();
        }
    };

    using TChunkList = NRopeDetails::TChunkList<TChunk>;

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
            Y_VERIFY(ValidityToken == Rope->GetValidityToken());
#endif
        }

        TIteratorImpl& operator +=(size_t amount) {
            CheckValid();

            while (amount) {
                Y_VERIFY_DEBUG(Valid());
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
                    Y_VERIFY_DEBUG(Iter != GetChainBegin());
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
            Y_VERIFY_DEBUG(Valid());
            ++Iter;
            Ptr = Iter != GetChainEnd() ? Iter->Begin : nullptr;
        }

        // Extract some data and advance. Size is not checked here, to it must be provided valid.
        void ExtractPlainDataAndAdvance(void *buffer, size_t len) {
            CheckValid();

            while (len) {
                Y_VERIFY_DEBUG(Ptr);

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
            Y_VERIFY_DEBUG(Rope == other.Rope);
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

        const TChunk& GetChunk() const {
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

    TRope(TRope&& rope)
        : Chain(std::move(rope.Chain))
        , Size(std::exchange(rope.Size, 0))
    {
        rope.InvalidateIterators();
    }

    TRope(TString s) {
        if (s) {
            Size = s.size();
            s.reserve(32);
            Chain.PutToEnd(std::move(s));
        }
    }

    TRope(IRopeChunkBackend::TPtr item) {
        std::tie(std::ignore, Size) = item->GetData();
        Chain.PutToEnd(std::move(item));
    }

    TRope(TConstIterator begin, TConstIterator end) {
        Y_VERIFY_DEBUG(begin.Rope == end.Rope);
        if (begin.Rope == this) {
            TRope temp(begin, end);
            *this = std::move(temp);
            return;
        }

        while (begin.Iter != end.Iter) {
            const size_t size = begin.ContiguousSize();
            Chain.PutToEnd(TChunk::Slice, begin.ContiguousData(), size, begin.GetChunk());
            begin.AdvanceToNextContiguousBlock();
            Size += size;
        }

        if (begin != end && end.PointsToChunkMiddle()) {
            Chain.PutToEnd(TChunk::Slice, begin.Ptr, end.Ptr, begin.GetChunk());
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

    bool IsEmpty() const {
        return !Size;
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

    void Erase(TIterator begin, TIterator end) {
        Cut(begin, end, nullptr);
    }

    TRope Extract(TIterator begin, TIterator end) {
        TRope res;
        Cut(begin, end, &res);
        return res;
    }

    void ExtractFront(size_t num, TRope *dest) {
        Y_VERIFY(Size >= num);
        if (num == Size && !*dest) {
            *dest = std::move(*this);
            return;
        }
        Size -= num;
        dest->Size += num;
        TChunkList::iterator it, first = Chain.begin();
        for (it = first; num && num >= it->GetSize(); ++it) {
            num -= it->GetSize();
        }
        if (it != first) {
            if (dest->Chain) {
                auto& last = dest->Chain.GetLastChunk();
                if (last.Backend == first->Backend && last.End == first->Begin) {
                    last.End = first->End;
                    first = Chain.Erase(first); // TODO(alexvru): "it" gets invalidated here on some containers
                }
            }
            dest->Chain.Splice(dest->Chain.end(), Chain, first, it);
        }
        if (num) {
            auto it = Chain.begin();
            if (dest->Chain) {
                auto& last = dest->Chain.GetLastChunk();
                if (last.Backend == first->Backend && last.End == first->Begin) {
                    first->Begin += num;
                    last.End = first->Begin;
                    return;
                }
            }
            dest->Chain.PutToEnd(TChunk::Slice, it->Begin, it->Begin + num, *it);
            it->Begin += num;
        }
    }

    void Insert(TIterator pos, TRope&& rope) {
        Y_VERIFY_DEBUG(this == pos.Rope);
        Y_VERIFY_DEBUG(this != &rope);

        if (!rope) {
            return; // do nothing for empty rope
        }

        // adjust size
        Size += std::exchange(rope.Size, 0);

        // check if we have to split the block
        if (pos.PointsToChunkMiddle()) {
            pos.Iter = Chain.InsertBefore(pos.Iter, TChunk::Slice, pos->Begin, pos.Ptr, pos.GetChunk());
            ++pos.Iter;
            pos->Begin = pos.Ptr;
        }

        // perform glueing if possible
        TChunk *ropeLeft = &rope.Chain.GetFirstChunk();
        TChunk *ropeRight = &rope.Chain.GetLastChunk();
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
        Y_VERIFY_DEBUG(!rope);
        InvalidateIterators();
    }

    void EraseFront(size_t len) {
        Y_VERIFY_DEBUG(Size >= len);
        Size -= len;

        while (len) {
            Y_VERIFY_DEBUG(Chain);
            TChunk& item = Chain.GetFirstChunk();
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
        Y_VERIFY_DEBUG(Size >= len);
        Size -= len;

        while (len) {
            Y_VERIFY_DEBUG(Chain);
            TChunk& item = Chain.GetLastChunk();
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
            const size_t num = Min(len, chunk.GetSize());
            memcpy(buffer, chunk.Begin, num);
            buffer = static_cast<char*>(buffer) + num;
            len -= num;
            chunk.Begin += num;
            if (chunk.Begin == chunk.End) {
                Chain.Erase(Chain.begin());
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

    // Use this method carefully -- it may significantly reduce performance when misused.
    TString ConvertToString() const {
        TString res = TString::Uninitialized(GetSize());
        Begin().ExtractPlainDataAndAdvance(res.Detach(), res.size());
        return res;
    }

    TString DebugString() const {
        TStringStream s;
        s << "{Size# " << Size;
        for (const auto& chunk : Chain) {
            const char *data;
            std::tie(data, std::ignore) = chunk.Backend.GetData();
            s << " [" << chunk.Begin - data << ", " << chunk.End - data << ")@" << chunk.Backend.UniqueId();
        }
        s << "}";
        return s.Str();
    }

    friend bool operator==(const TRope& x, const TRope& y) { return Compare(x, y) == 0; }
    friend bool operator!=(const TRope& x, const TRope& y) { return Compare(x, y) != 0; }
    friend bool operator< (const TRope& x, const TRope& y) { return Compare(x, y) <  0; }
    friend bool operator<=(const TRope& x, const TRope& y) { return Compare(x, y) <= 0; }
    friend bool operator> (const TRope& x, const TRope& y) { return Compare(x, y) >  0; }
    friend bool operator>=(const TRope& x, const TRope& y) { return Compare(x, y) >= 0; }

private:
    void Cut(TIterator begin, TIterator end, TRope *target) {
        // ensure all iterators are belong to us
        Y_VERIFY_DEBUG(this == begin.Rope && this == end.Rope);

        // if begin and end are equal, we do nothing -- checking this case allows us to find out that begin does not
        // point to End(), for example
        if (begin == end) {
            return;
        }

        auto addBlock = [&](const TChunk& from, const char *begin, const char *end) {
            if (target) {
                target->Chain.PutToEnd(TChunk::Slice, begin, end, from);
                target->Size += end - begin;
            }
            Size -= end - begin;
        };

        // consider special case -- when begin and end point to the same block; in this case we have to split up this
        // block into two parts
        if (begin.Iter == end.Iter) {
            addBlock(begin.GetChunk(), begin.Ptr, end.Ptr);
            const char *firstChunkBegin = begin.PointsToChunkMiddle() ? begin->Begin : nullptr;
            begin->Begin = end.Ptr; // this affects both begin and end iterator pointed values
            if (firstChunkBegin) {
                Chain.InsertBefore(begin.Iter, TChunk::Slice, firstChunkBegin, begin.Ptr, begin.GetChunk());
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
                Y_VERIFY_DEBUG(it->GetSize());
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
    }
};

class TRopeArena {
    using TAllocateCallback = std::function<TIntrusivePtr<IRopeChunkBackend>()>;

    TAllocateCallback Allocator;
    TRope Arena;
    size_t Size = 0;
    THashSet<const void*> AccountedBuffers;

public:
    TRopeArena(TAllocateCallback&& allocator)
        : Allocator(std::move(allocator))
    {}

    TRope CreateRope(const void *buffer, size_t len) {
        TRope res;

        while (len) {
            if (Arena) {
                auto iter = Arena.Begin();
                Y_VERIFY_DEBUG(iter.Valid());
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

    size_t GetSize() const {
        return Size;
    }

    void AccountChunk(const TRope::TChunk& chunk) {
        if (AccountedBuffers.insert(chunk.Backend.UniqueId()).second) {
            Size += chunk.GetCapacity();
        }
    }
};

struct TRopeUtils {
    static void Memset(TRope::TConstIterator dst, char c, size_t size) {
        while (size) {
            Y_VERIFY_DEBUG(dst.Valid());
            size_t len = std::min(size, dst.ContiguousSize());
            memset(const_cast<char*>(dst.ContiguousData()), c, len);
            dst += len;
            size -= len;
        }
    }

    static void Memcpy(TRope::TConstIterator dst, TRope::TConstIterator src, size_t size) {
        while (size) {
            Y_VERIFY_DEBUG(dst.Valid() && src.Valid(),
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
            Y_VERIFY_DEBUG(dst.Valid());
            size_t len = std::min(size, dst.ContiguousSize());
            memcpy(const_cast<char*>(dst.ContiguousData()), src, len);
            size -= len;
            dst += len;
            src += len;
        }
    }

    static void Memcpy(char* dst, TRope::TConstIterator src, size_t size) {
        while (size) {
            Y_VERIFY_DEBUG(src.Valid());
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

inline TRope TRope::CopySpaceOptimized(TRope&& origin, size_t worstRatioPer1k, TRopeArena& arena) {
    TRope res;
    for (TChunk& chunk : origin.Chain) {
        size_t ratio = chunk.GetSize() * 1024 / chunk.GetCapacity();
        if (ratio < 1024 - worstRatioPer1k) {
            res.Insert(res.End(), arena.CreateRope(chunk.Begin, chunk.GetSize()));
        } else {
            res.Chain.PutToEnd(std::move(chunk));
        }
    }
    res.Size = origin.Size;
    origin = TRope();
    for (const TChunk& chunk : res.Chain) {
        arena.AccountChunk(chunk);
    }
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
