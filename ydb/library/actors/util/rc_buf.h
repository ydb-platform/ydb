#pragma once

#include <atomic>
#include <new>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <util/stream/str.h>
#include <util/system/sanitizers.h>
#include <util/system/valgrind.h>
#include <util/generic/array_ref.h>
#include <util/system/sys_alloc.h>
#include <util/system/info.h>

#include "shared_data.h"
#include "rc_buf_backend.h"

#ifdef KIKIMR_TRACE_CONTIGUOUS_DATA_GROW
#include "shared_data_backtracing_owner.h"
#endif

namespace NContiguousDataDetails {
    template<typename TContainer>
    struct TContainerTraits {
        static char* UnsafeGetDataMut(const TContainer& backend) {
            return const_cast<char*>(backend.data());
        }
    };
} // NContiguousDataDetails

class TContiguousSpan
{
private:
    const char *Data_ = nullptr;
    size_t Size_ = 0;

public:
    TContiguousSpan() = default;

    TContiguousSpan(const char *data, size_t size)
        : Data_(data)
        , Size_(size)
    {}

    TContiguousSpan(const TString& str)
        : Data_(str.data())
        , Size_(str.size())
    {}

    TContiguousSpan(const TStringBuf& str)
        : Data_(str.data())
        , Size_(str.size())
    {}

    TContiguousSpan(const TArrayRef<char>& ref)
        : Data_(ref.data())
        , Size_(ref.size())
    {}

    TContiguousSpan(const TArrayRef<const char>& ref)
        : Data_(ref.data())
        , Size_(ref.size())
    {}

    TContiguousSpan(const NActors::TSharedData& data)
        : Data_(data.data())
        , Size_(data.size())
    {}

    const char& operator[](size_t index) const {
        Y_DEBUG_ABORT_UNLESS(index < Size_);
        return Data_[index];
    }

    const char *data() const noexcept {
        return Data_;
    }

    size_t size() const noexcept {
        return Size_;
    }

    const char *GetData() const noexcept {
        return Data_;
    }

    size_t GetSize() const noexcept {
        return Size_;
    }

    const char *Data() const noexcept {
        return Data_;
    }

    size_t Size() const noexcept {
        return Size_;
    }

    TContiguousSpan SubSpan(size_t pos, size_t n) const noexcept {
        pos = Min(pos, size());
        n = Min(n, size() - pos);
        return TContiguousSpan(data() + pos, n);
    }

    template<std::size_t Index>
    auto get() const noexcept
    {
        static_assert(Index < 2,
                      "Index out of bounds for TContiguousSpan");
        if constexpr (Index == 0) return Data_;
        if constexpr (Index == 1) return Size_;
    }

    friend bool operator==(const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) == 0; }
    friend bool operator!=(const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) != 0; }
    friend bool operator< (const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) <  0; }
    friend bool operator<=(const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) <= 0; }
    friend bool operator> (const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) >  0; }
    friend bool operator>=(const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) >= 0; }

private:
    static int Compare(const TContiguousSpan& x, const TContiguousSpan& y) {
        int res = 0;
        if (const size_t common = std::min(x.size(), y.size())) {
            res = std::memcmp(x.data(), y.data(), common);
        }
        return res ? res : x.size() - y.size();
    }
};



namespace std
{
  template<>
  struct tuple_size<::TContiguousSpan>
      : integral_constant<size_t, 2> {};

  template<>
  struct tuple_element<0, ::TContiguousSpan>
  {
    using type = const char *;
  };

  template<>
  struct tuple_element<1, ::TContiguousSpan>
  {
    using type = size_t;
  };
}

template <
    class TLeft,
    class TRight,
    typename std::enable_if<std::is_convertible<TLeft,TContiguousSpan>::value>::type* = nullptr,
    typename std::enable_if<std::is_convertible<TRight,TContiguousSpan>::value>::type* = nullptr
    >
bool operator==(const TLeft& lhs, const TRight& rhs) {
    return TContiguousSpan(lhs) == TContiguousSpan(rhs);
}

template <
    class TLeft,
    class TRight,
    typename std::enable_if<std::is_convertible<TLeft,TContiguousSpan>::value>::type* = nullptr,
    typename std::enable_if<std::is_convertible<TRight,TContiguousSpan>::value>::type* = nullptr
    >
bool operator!=(const TLeft& lhs, const TRight& rhs) {
    return TContiguousSpan(lhs) != TContiguousSpan(rhs);
}

template <
    class TLeft,
    class TRight,
    typename std::enable_if<std::is_convertible<TLeft,TContiguousSpan>::value>::type* = nullptr,
    typename std::enable_if<std::is_convertible<TRight,TContiguousSpan>::value>::type* = nullptr
    >
bool operator<(const TLeft& lhs, const TRight& rhs) {
    return TContiguousSpan(lhs) < TContiguousSpan(rhs);
}

template <
    class TLeft,
    class TRight,
    typename std::enable_if<std::is_convertible<TLeft,TContiguousSpan>::value>::type* = nullptr,
    typename std::enable_if<std::is_convertible<TRight,TContiguousSpan>::value>::type* = nullptr
    >
bool operator<=(const TLeft& lhs, const TRight& rhs) {
    return TContiguousSpan(lhs) <= TContiguousSpan(rhs);
}

template <
    class TLeft,
    class TRight,
    typename std::enable_if<std::is_convertible<TLeft,TContiguousSpan>::value>::type* = nullptr,
    typename std::enable_if<std::is_convertible<TRight,TContiguousSpan>::value>::type* = nullptr
    >
bool operator>(const TLeft& lhs, const TRight& rhs) {
    return TContiguousSpan(lhs) > TContiguousSpan(rhs);
}

template <
    class TLeft,
    class TRight,
    typename std::enable_if<std::is_convertible<TLeft,TContiguousSpan>::value>::type* = nullptr,
    typename std::enable_if<std::is_convertible<TRight,TContiguousSpan>::value>::type* = nullptr
    >
bool operator>=(const TLeft& lhs, const TRight& rhs) {
    return TContiguousSpan(lhs) >= TContiguousSpan(rhs);
}


class TMutableContiguousSpan
{
private:
    char *Data = nullptr;
    size_t Size = 0;

public:
    TMutableContiguousSpan() = default;

    TMutableContiguousSpan(char *data, size_t size)
        : Data(data)
        , Size(size)
    {}

    char *data() noexcept {
        return Data;
    }

    char *GetData() noexcept {
        return Data;
    }

    TMutableContiguousSpan SubSpan(size_t pos, size_t n) noexcept {
        pos = Min(pos, size());
        n = Min(n, size() - pos);
        return TMutableContiguousSpan(data() + pos, n);
    }

    const char *data() const noexcept {
        return Data;
    }

    size_t size() const noexcept {
        return Size;
    }

    const char *GetData() const noexcept {
        return Data;
    }

    size_t GetSize() const noexcept {
        return Size;
    }

    TContiguousSpan SubSpan(size_t pos, size_t n) const noexcept {
        pos = Min(pos, size());
        n = Min(n, size() - pos);
        return TContiguousSpan(data() + pos, n);
    }

    operator TContiguousSpan() const noexcept {
        return TContiguousSpan(Data, Size);
    }
};

struct IContiguousChunk : TThrRefBase {
    using TPtr = TIntrusivePtr<IContiguousChunk>;

    virtual ~IContiguousChunk() = default;

    /**
     * Should give immutable access to data
     */
    virtual TContiguousSpan GetData() const = 0;

    /**
     * Should give mutable access to underlying data
     * If data is shared - data should be copied
     * E.g. for TString str.Detach() should be used
     * Possibly invalidates previous *GetData*() calls
     */
    virtual TMutableContiguousSpan GetDataMut() = 0;

    /**
     * Should give mutable access to undelying data as fast as possible
     * Even if data is shared this property should be ignored
     * E.g. in TString const_cast<char *>(str.data()) should be used
     * Possibly invalidates previous *GetData*() calls
     */
    virtual TMutableContiguousSpan UnsafeGetDataMut() {
        return GetDataMut();
    }

    /**
     * Should return true if GetDataMut() would not copy contents when called.
     */
    virtual bool IsPrivate() const {
        return true;
    }

    virtual size_t GetOccupiedMemorySize() const = 0;
};

class TRope;
class TRopeArena;

class TRcBuf {
    friend class TRope;
    friend class TRopeArena;

    using TInternalBackend = NDetail::TRcBufInternalBackend;

    class TBackend {
        enum class EType : uintptr_t {
            STRING,
            SHARED_DATA,
            INTERNAL_BACKEND,
            EXTERNAL_BACKEND,
        };

        struct TBackendHolder {
            uintptr_t Data[2];
            explicit operator bool() const noexcept {
                return Data[0] || Data[1];
            }
            friend bool operator ==(const TBackendHolder& x, const TBackendHolder& y) {
                return x.Data[0] == y.Data[0] && x.Data[1] == y.Data[1];
            }
        };

        constexpr static TBackendHolder Empty = {0, 0};

#ifndef TSTRING_IS_STD_STRING
        static_assert(sizeof(TBackendHolder) >= sizeof(TString));
#endif
        static_assert(sizeof(TBackendHolder) >= sizeof(NActors::TSharedData));
        static_assert(sizeof(TBackendHolder) >= sizeof(TInternalBackend));

        TBackendHolder Owner = TBackend::Empty; // lower bits contain type of the owner

    public:
        using TCookies = TInternalBackend::TCookies;

        static constexpr struct TControlToken {} ControlToken;
        static constexpr size_t CookiesSize = sizeof(TCookies);

        TBackend() = default;

        TBackend(const TBackend& other)
            : Owner(other.Owner ? Clone(other.Owner) : TBackend::Empty)
        {}

        TBackend(TBackend&& other)
            : Owner(std::exchange(other.Owner, TBackend::Empty))
        {}

        TBackend(TString s)
            : Owner(Construct<TString>(EType::STRING, std::move(s)))
        {}

        TBackend(NActors::TSharedData s)
            : Owner(Construct<NActors::TSharedData>(EType::SHARED_DATA, std::move(s)))
        {}

        TBackend(TInternalBackend backend)
            : Owner(Construct<TInternalBackend>(EType::INTERNAL_BACKEND, std::move(backend)))
        {}

        TBackend(IContiguousChunk::TPtr backend)
            : Owner(Construct<IContiguousChunk::TPtr>(EType::EXTERNAL_BACKEND, std::move(backend)))
        {}

        ~TBackend() {
            if (Owner) {
                Destroy(Owner);
            }
        }

        TBackend& operator =(const TBackend& other) {
            if (Y_UNLIKELY(this == &other)) {
                return *this;
            }

            if (Owner) {
                Destroy(Owner);
            }
            if (other.Owner) {
                Owner = Clone(other.Owner);
            } else {
                Owner = TBackend::Empty;
            }
            return *this;
        }

        TBackend& operator =(TBackend&& other) {
            if (Y_UNLIKELY(this == &other)) {
                return *this;
            }

            if (Owner) {
                Destroy(Owner);
            }
            Owner = std::exchange(other.Owner, TBackend::Empty);
            return *this;
        }

        bool operator ==(const TBackend& other) const {
            return Owner == other.Owner;
        }

        const void *UniqueId() const {
            return reinterpret_cast<const void*>(Owner.Data[0]);
        }

        TCookies* GetCookies() {
            if(!Owner) {
                return nullptr;
            }
            return Visit(Owner, [](EType, auto& value) -> TCookies* {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, TInternalBackend>) {
                    return value.GetCookies();
                } else {
                    return nullptr;
                }
            });
        }

        const TCookies* GetCookies() const {
            return const_cast<TBackend&>(*this).GetCookies();
        }

        bool IsPrivate() const {
            if(!Owner) {
                return true;
            }
            return Visit(Owner, [](EType, auto& value) -> bool {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, NActors::TSharedData> || std::is_same_v<T, TInternalBackend>) {
                    return value.IsPrivate();
                } else if constexpr (std::is_same_v<T, TString>) {
                    return value.IsDetached();
                } else if constexpr (std::is_same_v<T, IContiguousChunk::TPtr>) {
                    return value.RefCount() == 1 && value->IsPrivate();
                } else {
                    static_assert(TDependentFalse<T>);
                }
            });
        }

        TContiguousSpan GetData() const {
            if (!Owner) {
                return TContiguousSpan();
            }
            return Visit(Owner, [](EType, auto& value) -> TContiguousSpan {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, TString> || std::is_same_v<T, NActors::TSharedData> || std::is_same_v<T, TInternalBackend>) {
                    return {value.data(), value.size()};
                } else if constexpr (std::is_same_v<T, IContiguousChunk::TPtr>) {
                    return value->GetData();
                } else {
                    static_assert(TDependentFalse<T>, "unexpected type");
                }
            });
        }

        TMutableContiguousSpan GetDataMut() {
            if (!Owner) {
                return TMutableContiguousSpan();
            }
            return Visit(Owner, [](EType, auto& value) -> TMutableContiguousSpan {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, TString>) {
                    return {value.Detach(), value.size()};
                } else if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                    if (value.IsShared()) {
                        value = NActors::TSharedData::Copy(value.data(), value.size());
                    }
                    return {value.mutable_data(), value.size()};
                } else if constexpr (std::is_same_v<T, TInternalBackend>) {
                    if (value.IsShared()) {
                        value = TInternalBackend::Copy(value.data(), value.size());
                    }
                    return {value.mutable_data(), value.size()};
                } else if constexpr (std::is_same_v<T, IContiguousChunk::TPtr>) {
                    return value->GetDataMut();
                } else {
                    static_assert(TDependentFalse<T>, "unexpected type");
                }
            });
        }

        TMutableContiguousSpan UnsafeGetDataMut() const {
            if (!Owner) {
                return TMutableContiguousSpan();
            }
            return Visit(Owner, [](EType, auto& value) -> TMutableContiguousSpan {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, TString>) {
                    return {const_cast<char*>(value.data()), value.size()};
                } else if constexpr (std::is_same_v<T, NActors::TSharedData> || std::is_same_v<T, TInternalBackend>) {
                    return {const_cast<char*>(value.data()), value.size()};
                } else if constexpr (std::is_same_v<T, IContiguousChunk::TPtr>) {
                    return value->UnsafeGetDataMut();
                } else {
                    static_assert(TDependentFalse<T>, "unexpected type");
                }
            });
        }

        size_t GetOccupiedMemorySize() const {
            if (!Owner) {
                return 0;
            }
            return Visit(Owner, [](EType, auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, TString>) {
                    return value.capacity();
                } else if constexpr (std::is_same_v<T, NActors::TSharedData> || std::is_same_v<T, TInternalBackend>) {
                    return value.size(); // There is no capacity
                } else if constexpr (std::is_same_v<T, IContiguousChunk::TPtr>) {
                    return value->GetOccupiedMemorySize();
                } else {
                    static_assert(TDependentFalse<T>, "unexpected type");
                }
            });
        }

        template <class TType>
        bool ContainsNativeType() const {
            if (!Owner) {
                return false;
            }
            return Visit(Owner, [](EType, auto& value) {
                using T = std::decay_t<decltype(value)>;
                return std::is_same_v<T, TType>;
            });
        }

        bool CanGrowFront(const char* begin) const {
            if (!Owner) {
                return false;
            }
            const TCookies* cookies = GetCookies();
            return cookies && (IsPrivate() || cookies->Begin.load() == begin);
        }

        bool CanGrowBack(const char* end) const {
            if (!Owner) {
                return false;
            }
            const TCookies* cookies = GetCookies();
            return cookies && (IsPrivate() || cookies->End.load() == end);
        }

        void UpdateCookiesUnsafe(const char* contBegin, const char* contEnd) {
            if (!Owner) {
                return;
            }
            TCookies* cookies = GetCookies();
            if (cookies) {
                cookies->Begin.store(contBegin);
                cookies->End.store(contEnd);
            }
        }

        bool UpdateCookiesBegin(const char* curBegin, const char* contBegin) {
            if (!Owner) {
                return false;
            }

            TCookies* cookies = GetCookies();
            if (cookies) {
                return cookies->Begin.compare_exchange_weak(curBegin, contBegin);
            }
            return false;
        }

        bool UpdateCookiesEnd(const char* curEnd, const char* contEnd) {
            if (!Owner) {
                return false;
            }

            TCookies* cookies = GetCookies();
            if (cookies) {
                return cookies->End.compare_exchange_weak(curEnd, contEnd);
            }
            return false;
        }

        template <typename TResult, typename TCallback>
        std::invoke_result_t<TCallback, const TResult*> ApplySpecificValue(TCallback&& callback) const {
            static_assert(std::is_same_v<TResult, TString> ||
                std::is_same_v<TResult, NActors::TSharedData> ||
                std::is_same_v<TResult, TInternalBackend> ||
                std::is_same_v<TResult, IContiguousChunk::TPtr>);

            if (!Owner) {
                return callback(nullptr);
            }
            return Visit(Owner, [&](EType, auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, TResult>) {
                    return callback(&value);
                } else {
                    return callback(nullptr);
                }
            });
        }

        explicit operator bool() const {
            return static_cast<bool>(Owner);
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
        static TBackendHolder Construct(EType type, TObject&& object) {
            if constexpr (sizeof(TObject) <= sizeof(TBackendHolder)) {
                TBackendHolder res = TBackend::Empty;
                new(&res) std::decay_t<TObject>(std::forward<TObject>(object));
                Y_DEBUG_ABORT_UNLESS((res.Data[0] & ValueMask) == res.Data[0]);
                res.Data[0] = res.Data[0] | static_cast<uintptr_t>(type);
                return res;
            } else {
                return Construct<TObjectHolder<TObject>>(type, TObjectHolder<TObject>(std::forward<TObject>(object)));
            }
        }

        template<typename TOwner, typename TCallback, bool IsConst = std::is_const_v<TOwner>>
        static std::invoke_result_t<TCallback, EType, std::conditional_t<IsConst, const TString&, TString&>> VisitRaw(TOwner& origValue, TCallback&& callback) {
            Y_DEBUG_ABORT_UNLESS(origValue);
            const EType type = static_cast<EType>(origValue.Data[0] & TypeMask);
            TBackendHolder value(origValue);
            value.Data[0] = value.Data[0] & ValueMask;
            // bring object type back
            Y_SCOPE_EXIT(&value, &origValue, type){
                if constexpr(!IsConst) {
                    value.Data[0] = value.Data[0] | static_cast<uintptr_t>(type);
                    origValue = value;
                } else {
                    Y_UNUSED(value);
                    Y_UNUSED(origValue);
                    Y_UNUSED(type);
                }
            };
            auto caller = [&](auto& value) { return std::invoke(std::forward<TCallback>(callback), type, value); };
            auto wrapper = [&](auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (sizeof(T) <= sizeof(TBackendHolder)) {
                    return caller(value);
                } else {
                    return caller(reinterpret_cast<std::conditional_t<IsConst, const TObjectHolder<T>&, TObjectHolder<T>&>>(value));
                }
            };
            switch (type) {
                case EType::STRING:                          return wrapper(reinterpret_cast<std::conditional_t<IsConst, const TString&, TString&>>(value));
                case EType::SHARED_DATA:                     return wrapper(reinterpret_cast<std::conditional_t<IsConst, const NActors::TSharedData&, NActors::TSharedData&>>(value));
                case EType::INTERNAL_BACKEND:                return wrapper(reinterpret_cast<std::conditional_t<IsConst, const TInternalBackend&, TInternalBackend&>>(value));
                case EType::EXTERNAL_BACKEND:                return wrapper(reinterpret_cast<std::conditional_t<IsConst, const IContiguousChunk::TPtr&, IContiguousChunk::TPtr&>>(value));
            }
            Y_ABORT("Unexpected type# %" PRIu64, static_cast<ui64>(type));
        }

        template<typename TOwner, typename TCallback, bool IsConst = std::is_const_v<TOwner>>
        static std::invoke_result_t<TCallback, EType, std::conditional_t<IsConst, const TString&, TString&>> Visit(TOwner& value, TCallback&& callback) {
            return VisitRaw(value, [&](EType type, auto& value) {
                return std::invoke(std::forward<TCallback>(callback), type, Unwrap(value));
            });
        }

        template<typename T> static T& Unwrap(T& object) { return object; }
        template<typename T> static T& Unwrap(TObjectHolder<T>& holder) { return holder.Object->Value; }
        template<typename T> static const T& Unwrap(const TObjectHolder<T>& holder) { return holder.Object->Value; }

        template<typename TOwner>
        static TBackendHolder Clone(TOwner& value) {
            return VisitRaw(value, [](EType type, auto& value) { return Construct(type, value); });
        }

        template<typename TOwner>
        static void Destroy(TOwner& value) {
            VisitRaw(value, [](EType, auto& value) { CallDtor(value); });
        }

        template<typename T>
        static void CallDtor(T& value) {
            value.~T();
        }
    };

    static constexpr struct TOwnedPiece {} OwnedPiece{};

    TBackend Backend; // who actually holds the data
    const char *Begin; // data start
    const char *End; // data end

    explicit TRcBuf(TInternalBackend s, const char *data, size_t size)
        : Backend(std::move(s))
    {
        Y_ABORT_UNLESS(Backend.GetData().data() == nullptr ||
                 (Backend.GetCookies() && Backend.GetCookies()->Begin == data && Backend.GetCookies()->End == data + size));
        Begin = data;
        End = data + size;
    }

    explicit TRcBuf(TInternalBackend s)
        : Backend(std::move(s))
    {
        auto span = Backend.GetData();
        Begin = span.data();
        End = Begin + span.size();
    }

    TRcBuf(TOwnedPiece, const char *data, size_t size, const TRcBuf& from)
        : TRcBuf(from.Backend, {data, size})
    {
        Y_ABORT_UNLESS(data >= from.GetData());
        Y_ABORT_UNLESS(data < from.GetData() + from.GetSize());
        Y_ABORT_UNLESS(data + size <= from.GetData() + from.GetSize());
        Backend.UpdateCookiesUnsafe(Begin, End);
    }

    TRcBuf(TOwnedPiece, const char *begin, const char *end, const TRcBuf& from)
        : TRcBuf(OwnedPiece, begin, end - begin, from)
    {}

public:
    static constexpr struct TPiece {} Piece{};

    enum class EResizeResult {
        NoAlloc,
        Alloc,
    };

    enum class EResizeStrategy {
        KeepRooms,
        FailOnCopy,
        // SaveAllocs, // Move data if there is enough space in (headroom + size + tailroom)
    };

    TRcBuf()
        : Begin(nullptr)
        , End(nullptr)
    {}

    template<typename T>
    TRcBuf(T&& backend, const TContiguousSpan& data)
        : Backend(std::forward<T>(backend))
        , Begin(data.data())
        , End(Begin + data.size())
    {}

    explicit TRcBuf(TString s)
        : Backend(std::move(s))
    {
        auto span = Backend.GetData();
        Begin = span.data();
        End = Begin + span.size();
    }

    explicit TRcBuf(NActors::TSharedData s)
        : Backend(std::move(s))
    {
        auto span = Backend.GetData();
        Begin = span.data();
        End = Begin + span.size();
    }

    TRcBuf(IContiguousChunk::TPtr backend)
        : TRcBuf(backend, backend->GetData())
    {}

    TRcBuf(TPiece, const char *data, size_t size, const TRcBuf& from)
        : TRcBuf(from.Backend, {data, size})
    {
        Y_ABORT_UNLESS(data >= from.GetData());
        Y_ABORT_UNLESS(data < from.GetData() + from.GetSize());
        Y_ABORT_UNLESS(data + size <= from.GetData() + from.GetSize());
    }

    TRcBuf(TPiece, const char *begin, const char *end, const TRcBuf& from)
        : TRcBuf(Piece, begin, end - begin, from)
    {}

    TRcBuf(const TRcBuf& other)
        : Backend(other.Backend)
        , Begin(other.Begin)
        , End(other.End)
    {}

    TRcBuf(TRcBuf&& other)
        : Backend(std::move(other.Backend))
        , Begin(other.Begin)
        , End(other.End)
    {}

    TRcBuf& operator =(const TRcBuf&) = default;
    TRcBuf& operator =(TRcBuf&&) = default;

    static TRcBuf Uninitialized(size_t size, size_t headroom = 0, size_t tailroom = 0)
    {
        if (size == 0) {
            return TRcBuf();
        }

        if (headroom == 0 && tailroom == 0) {
            TInternalBackend res = TInternalBackend::Uninitialized(size);
            return TRcBuf(
                OwnedPiece,
                res.data(),
                res.data() + res.size(),
                TRcBuf(res));
        }

        TInternalBackend res = TInternalBackend::Uninitialized(size, headroom, tailroom);
        return TRcBuf(res, res.data() + headroom, size);
    }

    static TRcBuf UninitializedPageAligned(size_t size) {
        const size_t pageSize = NSystemInfo::GetPageSize();
        TRcBuf res = Uninitialized(size + pageSize - 1);
        const size_t misalign = (pageSize - reinterpret_cast<uintptr_t>(res.data())) & (pageSize - 1);
        return TRcBuf(Piece, res.data() + misalign, size, res);
    }

    static TRcBuf Copy(TContiguousSpan data, size_t headroom = 0, size_t tailroom = 0) {
        TRcBuf res = Uninitialized(data.size(), headroom, tailroom);
        std::memcpy(res.UnsafeGetDataMut(), data.GetData(), data.GetSize());
        return res;
    }

    static TRcBuf Copy(const char* data, size_t size, size_t headroom = 0, size_t tailroom = 0) {
        return Copy({data, size}, headroom, tailroom);
    }

    template <class TType>
    bool ContainsNativeType() const {
        return Backend.ContainsNativeType<TType>();
    }

    bool CanGrowFront() const noexcept {
        return Backend.CanGrowFront(Begin);
    }

    bool CanGrowBack() const noexcept {
        return Backend.CanGrowBack(End);
    }

    size_t GetSize() const {
        return End - Begin;
    }

    size_t Size() const {
        return End - Begin;
    }

    size_t GetOccupiedMemorySize() const {
        return Backend.GetOccupiedMemorySize();
    }

    const char* GetData() const {
        return Begin;
    }

    char* GetDataMut(size_t headroom = 0, size_t tailroom = 0) {
        const TContiguousSpan backendData = Backend.GetData();
        if (IsPrivate() || (backendData.data() == GetData() && backendData.size() == GetSize())) { // if we own container or reference it whole
            const char* oldBegin = backendData.data();
            ptrdiff_t offset = Begin - oldBegin;
            size_t size = GetSize();
            char* newBegin = Backend.GetDataMut().data();
            Begin = newBegin + offset;
            End = Begin + size;
            return newBegin + offset;
        } else { // make a copy of referenced data
            *this = Copy(GetContiguousSpan(), headroom, tailroom);
            return Backend.GetDataMut().data();
        }
    }

    char* UnsafeGetDataMut() {
        const char* oldBegin = Backend.GetData().data();
        ptrdiff_t offset = Begin - oldBegin;
        size_t size = GetSize();
        char* newBegin = Backend.UnsafeGetDataMut().data();
        Begin = newBegin + offset;
        End = Begin + size;
        return newBegin + offset;
    }

    template <class TResult>
    TResult ExtractUnderlyingContainerOrCopy() const {
        static_assert(std::is_same_v<TResult, TString> ||
            std::is_same_v<TResult, NActors::TSharedData> ||
            std::is_same_v<TResult, TInternalBackend>);

        constexpr bool isSharedData = std::is_same_v<TResult, NActors::TSharedData>;
        TResult res;

        const bool found = Backend.ApplySpecificValue<TResult>([&](const TResult *raw) {
            if (raw && raw->data() == Begin && (isSharedData ? End <= Begin + raw->size() : End == Begin + raw->size())) {
                if constexpr (isSharedData) {
                    raw->TrimBack(size());
                }
                res = TResult(*raw);
                return true;
            }
            return false;
        });

        if (!found) {
            res = TResult::Uninitialized(GetSize());
            char* data = NContiguousDataDetails::TContainerTraits<TResult>::UnsafeGetDataMut(res);
            std::memcpy(data, GetData(), GetSize());
        }

        return res;
    }

    TContiguousSpan GetContiguousSpan() const {
        return {GetData(), GetSize()};
    }

    TStringBuf Slice(size_t pos = 0, size_t len = Max<size_t>()) const noexcept {
        pos = Min(pos, size());
        len = Min(len, size() - pos);
        return {const_cast<TRcBuf*>(this)->UnsafeGetDataMut() + pos, len};
    }

    explicit operator TStringBuf() const noexcept {
        return Slice();
    }

    TMutableContiguousSpan GetContiguousSpanMut() {
        return {GetDataMut(), GetSize()};
    }

    TMutableContiguousSpan UnsafeGetContiguousSpanMut() {
        return {UnsafeGetDataMut(), GetSize()};
    }

    bool HasBuffer() const {
        return static_cast<bool>(Backend);
    }

    size_t size() const {
        return GetSize();
    }

    bool empty() const {
        return !static_cast<bool>(Backend);
    }

    operator bool() const {
        return !empty();
    }

    const char* data() const {
        return GetData();
    }

    const char* Data() const {
        return GetData();
    }

    const char* begin() const {
        return Begin;
    }

    const char* end() const {
        return End;
    }

    char& operator[](size_t pos) {
        return UnsafeGetDataMut()[pos];
    }

    const char& operator[](size_t pos) const {
        return GetData()[pos];
    }

    void reserve(size_t size) {
        ReserveTailroom(size);
    }

    void ReserveHeadroom(size_t size) {
        if (Headroom() >= size) {
            return;
        }
        auto newData = TRcBuf::Uninitialized(GetSize(), size, UnsafeTailroom());
        if (auto data = GetData(); data) {
            std::memcpy(newData.UnsafeGetDataMut(), GetData(), GetSize());
        }
        *this = std::move(newData);
    }

    void ReserveTailroom(size_t size) {
        if (Tailroom() >= size) {
            return;
        }
        auto newData = TRcBuf::Uninitialized(GetSize(), UnsafeHeadroom(), size);
        if (auto data = GetData(); data) {
            std::memcpy(newData.UnsafeGetDataMut(), GetData(), GetSize());
        }
        *this = std::move(newData);
    }

    void ReserveBidi(size_t headroom, size_t tailroom) {
        if (Headroom() >= headroom && Tailroom() >= tailroom) {
            return;
        }
        auto newData = TRcBuf::Uninitialized(
                GetSize(),
                std::max(UnsafeHeadroom(), headroom),
                std::max(UnsafeTailroom(), tailroom));
        if (auto data = GetData(); data) {
            std::memcpy(newData.UnsafeGetDataMut(), GetData(), GetSize());
        }
        *this = std::move(newData);
    }

    EResizeResult GrowFront(size_t size, EResizeStrategy strategy = EResizeStrategy::KeepRooms) {
        if (Headroom() >= size && Backend.UpdateCookiesBegin(Begin, Begin - size)) {
            Begin -= size;
            return EResizeResult::NoAlloc;
        } else {
            if (strategy == EResizeStrategy::FailOnCopy && static_cast<bool>(Backend)) {
                Y_ABORT("Fail on grow");
            }
            auto newData = TRcBuf::Uninitialized(size + GetSize(), UnsafeHeadroom() > size ? UnsafeHeadroom() - size : 0, UnsafeTailroom());
            if (auto data = GetData(); data) {
                std::memcpy(newData.UnsafeGetDataMut() + size, GetData(), GetSize());
            }
            *this = std::move(newData);
            return EResizeResult::Alloc;
        }
    }

    EResizeResult GrowBack(size_t size, EResizeStrategy strategy = EResizeStrategy::KeepRooms) {
        if (Tailroom() > size && Backend.UpdateCookiesEnd(End, End + size)) {
            End += size;
            return EResizeResult::NoAlloc;
        } else {
            if (strategy == EResizeStrategy::FailOnCopy && static_cast<bool>(Backend)) {
                Y_ABORT("Fail on grow");
            }
            auto newData = TRcBuf::Uninitialized(size + GetSize(), UnsafeHeadroom(), UnsafeTailroom() > size ? UnsafeTailroom() - size : 0);
            if (auto data = GetData(); data) {
                std::memcpy(newData.UnsafeGetDataMut(), GetData(), GetSize());
            }
            *this = std::move(newData);
            return EResizeResult::Alloc;
        }
    }

    void TrimBack(size_t size) {
        Y_ABORT_UNLESS(size <= GetSize());
        End = End - (GetSize() - size);
    }

    void TrimFront(size_t size) {
        Y_ABORT_UNLESS(size <= GetSize());
        Begin = Begin + (GetSize() - size);
    }

    char* Detach() {
        return GetDataMut();
    }

    bool IsPrivate() const {
        return Backend.IsPrivate();
    }

    size_t UnsafeHeadroom() const {
        return Begin - Backend.GetData().data();
    }

    size_t UnsafeTailroom() const {
        auto span = Backend.GetData();
        return (span.GetData() + span.GetSize()) - End;
    }

    size_t Headroom() const {
        if (Backend.CanGrowFront(Begin)) {
            return UnsafeHeadroom();
        }

        return 0;
    }

    size_t Tailroom() const {
        if (Backend.CanGrowBack(End)) {
            return UnsafeTailroom();
        }

        return 0;
    }

    operator TContiguousSpan() const noexcept {
        return TContiguousSpan(GetData(), GetSize());
    }

    explicit operator TMutableContiguousSpan() noexcept {
        return TMutableContiguousSpan(GetDataMut(), GetSize());
    }
};
