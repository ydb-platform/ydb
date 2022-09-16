#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <util/stream/str.h>
#include <util/system/sanitizers.h>
#include <util/system/valgrind.h>
#include <util/generic/array_ref.h>

#include "shared_data.h"

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
    const char *Data = nullptr;
    size_t Size = 0;

public:
    TContiguousSpan() = default;

    TContiguousSpan(const char *data, size_t size)
        : Data(data)
        , Size(size)
    {}

    TContiguousSpan(const TString& str)
        : Data(str.data())
        , Size(str.size())
    {}

    TContiguousSpan(const TStringBuf& str)
        : Data(str.data())
        , Size(str.size())
    {}

    TContiguousSpan(const TArrayRef<char>& ref)
        : Data(ref.data())
        , Size(ref.size())
    {}

    TContiguousSpan(const TArrayRef<const char>& ref)
        : Data(ref.data())
        , Size(ref.size())
    {}

    TContiguousSpan(const NActors::TSharedData& data)
        : Data(data.data())
        , Size(data.size())
    {}

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

    friend bool operator==(const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) == 0; }
    friend bool operator!=(const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) != 0; }
    friend bool operator< (const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) <  0; }
    friend bool operator<=(const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) <= 0; }
    friend bool operator> (const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) >  0; }
    friend bool operator>=(const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) >= 0; }

private:
    static int Compare(const TContiguousSpan& x, const TContiguousSpan& y) {
        if(int res = std::memcmp(x.data(), y.data(), std::min(x.size(), y.size())); res) {
            return res;
        }
        return x.size() - y.size();
    }
};

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

    virtual size_t GetCapacity() const = 0;
};

class TRope;
class TRopeArena;

class TContiguousData {
    friend class TRope;
    friend class TRopeArena;
    class TBackend {
        enum class EType : uintptr_t {
            STRING,
            SHARED_DATA,
            ROPE_CHUNK_BACKEND,
        };

        struct TBackendHolder {
            uintptr_t Data[2];
            operator bool() const noexcept {
                return Data[0] || Data[1];
            }
        };

        constexpr static TBackendHolder Empty = {0, 0};

#ifndef TSTRING_IS_STD_STRING
        static_assert(sizeof(TBackendHolder) >= sizeof(TString));
#endif
        static_assert(sizeof(TBackendHolder) >= sizeof(NActors::TSharedData));

        TBackendHolder Owner = TBackend::Empty; // lower bits contain type of the owner

    public:
        TBackend() = default;

        TBackend(const TBackend& other)
            : Owner(Clone(other.Owner))
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

        TBackend(IContiguousChunk::TPtr backend)
            : Owner(Construct<IContiguousChunk::TPtr>(EType::ROPE_CHUNK_BACKEND, std::move(backend)))
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
            Owner = Clone(other.Owner);
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

        TContiguousSpan GetData() const {
            return Visit(Owner, [](EType, auto& value) -> TContiguousSpan {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, TString>) {
                    return {value.data(), value.size()};
                } else if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                    return {value.data(), value.size()};
                } else if constexpr (std::is_same_v<T, IContiguousChunk::TPtr>) {
                    return value->GetData();
                } else {
                    return {};
                }
            });
        }

        TMutableContiguousSpan GetDataMut() {
            return Visit(Owner, [](EType, auto& value) -> TMutableContiguousSpan {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, TString>) {
                    return {value.Detach(), value.size()};
                } else if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                    if(value.IsShared()) {
                        value = NActors::TSharedData::Copy(value.data(), value.size());
                    }
                    return {value.mutable_data(), value.size()};
                } else if constexpr (std::is_same_v<T, IContiguousChunk::TPtr>) {
                    return value->GetDataMut();
                } else {
                    return {};
                }
            });
        }

        TMutableContiguousSpan UnsafeGetDataMut() const {
            return Visit(Owner, [](EType, auto& value) -> TMutableContiguousSpan {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, TString>) {
                    return {const_cast<char*>(value.data()), value.size()};
                } else if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                    return {const_cast<char*>(value.data()), value.size()};
                } else if constexpr (std::is_same_v<T, IContiguousChunk::TPtr>) {
                    return value->UnsafeGetDataMut();
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
                } else if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                    return value.size(); // There is no capacity
                } else if constexpr (std::is_same_v<T, IContiguousChunk::TPtr>) {
                    return value->GetCapacity();
                } else {
                    Y_FAIL();
                }
            });
        }

        template <class TType>
        bool ContainsNativeType() const {
            return Visit(Owner, [](EType, auto& value) {
                using T = std::decay_t<decltype(value)>;
                return std::is_same_v<T, TType>;
            });
        }

        template <class TResult>
        TResult GetRaw() const {
            return Visit(Owner, [](EType, auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, TResult>) {
                    return value;
                } else {
                    Y_FAIL();
                    return TResult{}; // unreachable
                }
            });
        }

        explicit operator bool() const {
            return Owner;
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
        static TBackendHolder Construct(EType type, TObject object) {
            if constexpr (sizeof(TObject) <= sizeof(TBackendHolder)) {
                TBackendHolder res = TBackend::Empty;
                new(&res) TObject(std::move(object));
                Y_VERIFY_DEBUG((res.Data[0] & ValueMask) == res.Data[0]);
                res.Data[0] = res.Data[0] | static_cast<uintptr_t>(type);
                return res;
            } else {
                return Construct<TObjectHolder<TObject>>(type, TObjectHolder<TObject>(std::move(object)));
            }
        }

        template<typename TOwner, typename TCallback, bool IsConst = std::is_const_v<TOwner>>
        static std::invoke_result_t<TCallback, EType, std::conditional_t<IsConst, const TString&, TString&>> VisitRaw(TOwner& origValue, TCallback&& callback) {
            Y_VERIFY_DEBUG(origValue);
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
                case EType::STRING:             return wrapper(reinterpret_cast<std::conditional_t<IsConst, const TString&, TString&>>(value));
                case EType::SHARED_DATA:        return wrapper(reinterpret_cast<std::conditional_t<IsConst, const NActors::TSharedData&, NActors::TSharedData&>>(value));
                case EType::ROPE_CHUNK_BACKEND: return wrapper(reinterpret_cast<std::conditional_t<IsConst, const IContiguousChunk::TPtr&, IContiguousChunk::TPtr&>>(value));
            }
            Y_FAIL("Unexpected type# %" PRIu64, static_cast<ui64>(type));
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

    TBackend Backend; // who actually holds the data
    const char *Begin; // data start
    const char *End; // data end

public:
    static constexpr struct TSlice {} Slice{};

    TContiguousData()
        : Begin(nullptr)
        , End(nullptr)
    {}

    template<typename T>
    TContiguousData(T&& backend, const TContiguousSpan& data)
        : Backend(std::forward<T>(backend))
        , Begin(data.data())
        , End(Begin + data.size())
    {
        Y_VERIFY_DEBUG(Begin != End);
    }

    explicit TContiguousData(TString s)
        : Backend(std::move(s))
    {
        auto span = Backend.GetData();
        Begin = span.data();
        End = Begin + span.size();
    }

    explicit TContiguousData(NActors::TSharedData s)
        : Backend(std::move(s))
    {
        auto span = Backend.GetData();
        Begin = span.data();
        End = Begin + span.size();
    }

    TContiguousData(IContiguousChunk::TPtr backend)
        : TContiguousData(backend, backend->GetData())
    {}

    TContiguousData(TSlice, const char *data, size_t size, const TContiguousData& from)
        : TContiguousData(from.Backend, {data, size})
    {}

    TContiguousData(TSlice, const char *begin, const char *end, const TContiguousData& from)
        : TContiguousData(Slice, begin, end - begin, from)
    {}

    TContiguousData(const TContiguousData& other)
        : Backend(other.Backend)
        , Begin(other.Begin)
        , End(other.End)
    {}

    TContiguousData(TContiguousData&& other)
        : Backend(std::move(other.Backend))
        , Begin(other.Begin)
        , End(other.End)
    {}

    TContiguousData& operator =(const TContiguousData&) = default;
    TContiguousData& operator =(TContiguousData&&) = default;

    template <class TType>
    bool ContainsNativeType() const {
        return Backend.ContainsNativeType<TType>();
    }

    template <class TResult>
    TResult GetRaw() const {
        return Backend.GetRaw<TResult>();
    }

    bool ReferencesWholeContainer() const {
        return Backend.GetData().size() == GetSize();
    }

    size_t GetSize() const {
        return End - Begin;
    }

    size_t GetCapacity() const {
        return Backend.GetCapacity();
    }

    const char* GetData() const {
        return Begin;
    }

    char* GetDataMut() {
        const char* oldBegin = Backend.GetData().data();
        ptrdiff_t offset = Begin - oldBegin;
        size_t size = GetSize();
        char* newBegin = Backend.GetDataMut().data();
        Begin = newBegin + offset;
        End = Begin + size;
        return newBegin + offset;
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
        if (ContainsNativeType<TResult>() && ReferencesWholeContainer()) {
            return GetRaw<TResult>();
        }

        TResult res = TResult::Uninitialized(GetSize());
        char* data = NContiguousDataDetails::TContainerTraits<TResult>::UnsafeGetDataMut(res);
        std::memcpy(data, Begin, End - Begin);
        return res;
    }

    TContiguousSpan GetContiguousSpan() const {
        return {GetData(), GetSize()};
    }

    TMutableContiguousSpan GetContiguousSpanMut() {
        return {GetDataMut(), GetSize()};
    }

    TMutableContiguousSpan UnsafeGetContiguousSpanMut() {
        return {UnsafeGetDataMut(), GetSize()};
    }

    size_t size() const {
        return GetSize();
    }

    size_t capacity() const {
        return GetCapacity();
    }

    const char* data() const {
        return GetData();
    }

    operator TContiguousSpan() const noexcept {
        return TContiguousSpan(GetData(), GetSize());
    }

    explicit operator TMutableContiguousSpan() noexcept {
        return TMutableContiguousSpan(GetDataMut(), GetSize());
    }
};
