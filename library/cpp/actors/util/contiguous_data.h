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

#include "shared_data.h"

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

    const char& operator[](size_t index) const {
        Y_VERIFY_DEBUG(index < Size);
        return Data[index];
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

    template<std::size_t Index>
    auto get() const noexcept
    {
        static_assert(Index < 2,
                      "Index out of bounds for TContiguousSpan");
        if constexpr (Index == 0) return Data;
        if constexpr (Index == 1) return Size;
    }

    friend bool operator==(const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) == 0; }
    friend bool operator!=(const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) != 0; }
    friend bool operator< (const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) <  0; }
    friend bool operator<=(const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) <= 0; }
    friend bool operator> (const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) >  0; }
    friend bool operator>=(const TContiguousSpan& x, const TContiguousSpan& y) { return Compare(x, y) >= 0; }

private:
    static int Compare(const TContiguousSpan& x, const TContiguousSpan& y) {
        if (int res = std::memcmp(x.data(), y.data(), std::min(x.size(), y.size())); res) {
            return res;
        }
        return x.size() - y.size();
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

    virtual size_t GetOccupiedMemorySize() const = 0;
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
            SHARED_DATA_CONTROLLED,
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
        struct TCookies {
            using TSelf = TCookies;
            std::atomic<const char*> Begin;
            std::atomic<const char*> End;

            static size_t BytesToAligned(size_t size) {
                bool misaligned = size % alignof(TSelf);
                return misaligned ? alignof(TSelf) - size % alignof(TSelf) : 0;
            }

            static size_t BytesToAlloc(size_t size) {
                return size + BytesToAligned(size) + sizeof(TSelf);
            }
        };
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

        TBackend(NActors::TSharedData s, TControlToken)
            : Owner(Construct<NActors::TSharedData>(EType::SHARED_DATA_CONTROLLED, std::move(s)))
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

        TContiguousSpan GetData() const {
            if (!Owner) {
                return TContiguousSpan();
            }
            return Visit(Owner, [](EType type, auto& value) -> TContiguousSpan {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, TString>) {
                    return {&(*value.cbegin()), value.size()};
                } else if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                    if (type == EType::SHARED_DATA_CONTROLLED) {
                        return {value.data(), value.size() - CookiesSize};
                    } else {
                        return {value.data(), value.size()};
                    }
                } else if constexpr (std::is_same_v<T, IContiguousChunk::TPtr>) {
                    return value->GetData();
                } else {
                    return {};
                }
            });
        }

        TMutableContiguousSpan GetDataMut() {
            if (!Owner) {
                return TMutableContiguousSpan();
            }
            return Visit(Owner, [](EType type, auto& value) -> TMutableContiguousSpan {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, TString>) {
                    return {value.Detach(), value.size()};
                } else if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                    if (value.IsShared()) {
                        value = NActors::TSharedData::Copy(value.data(), value.size());
                    }
                    if (type == EType::SHARED_DATA_CONTROLLED) {
                        return {value.mutable_data(), value.size() - CookiesSize};
                    } else {
                        return {value.mutable_data(), value.size()};
                    }
                } else if constexpr (std::is_same_v<T, IContiguousChunk::TPtr>) {
                    return value->GetDataMut();
                } else {
                    return {};
                }
            });
        }

        TMutableContiguousSpan UnsafeGetDataMut() const {
            if (!Owner) {
                return TMutableContiguousSpan();
            }
            return Visit(Owner, [](EType type, auto& value) -> TMutableContiguousSpan {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, TString>) {
                    return {const_cast<char*>(value.data()), value.size()};
                } else if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                    if (type == EType::SHARED_DATA_CONTROLLED) {
                        return {const_cast<char*>(value.data()), value.size() - CookiesSize};
                    } else {
                        return {const_cast<char*>(value.data()), value.size()};
                    }
                } else if constexpr (std::is_same_v<T, IContiguousChunk::TPtr>) {
                    return value->UnsafeGetDataMut();
                } else {
                    return {};
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
                } else if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                    return value.size(); // There is no capacity
                } else if constexpr (std::is_same_v<T, IContiguousChunk::TPtr>) {
                    return value->GetOccupiedMemorySize();
                } else {
                    Y_FAIL();
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
            return Visit(Owner, [contBegin = begin](EType type, auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                    if (type == EType::SHARED_DATA_CONTROLLED) {
                        auto* begin = value.data() + value.size() - CookiesSize;
                        const TCookies* cookies = reinterpret_cast<const TCookies*>(begin);
                        return value.IsPrivate() || cookies->Begin.load() == contBegin;
                    }
                }
                return false;
            });
        }

        bool CanGrowBack(const char* end) const {
            if (!Owner) {
                return false;
            }
            return Visit(Owner, [contEnd = end](EType type, auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                    if (type == EType::SHARED_DATA_CONTROLLED) {
                        auto* begin = value.data() + value.size() - CookiesSize;
                        const TCookies* cookies = reinterpret_cast<const TCookies*>(begin);
                        return value.IsPrivate() || cookies->End.load() == contEnd;
                    }
                }
                return false;
            });
        }

        void UpdateCookiesUnsafe(const char* contBegin, const char* contEnd) {
            if (!Owner) {
                return;
            }
            return Visit(Owner, [contBegin, contEnd](EType type, auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                    if (type == EType::SHARED_DATA_CONTROLLED) {
                        auto* begin = const_cast<char*>(value.data() + value.size() - CookiesSize);
                        TCookies* cookies = reinterpret_cast<TCookies*>(begin);
                        cookies->Begin.store(contBegin);
                        cookies->End.store(contEnd);
                    }
                }
            });
        }

        bool UpdateCookies(const char* curBegin, const char* contBegin, const char* curEnd, const char* contEnd) {
            if (!Owner) {
                return false;
            }
            return Visit(Owner, [curBegin, contBegin, curEnd, contEnd](EType type, auto& value) mutable {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                    if (type == EType::SHARED_DATA_CONTROLLED) {
                        auto* begin = const_cast<char*>(value.data() + value.size() - CookiesSize);
                        TCookies* cookies = reinterpret_cast<TCookies*>(begin);
                        if(cookies->Begin.compare_exchange_weak(curBegin, contBegin)) {
                            if(!cookies->End.compare_exchange_weak(curEnd, contEnd)) {
                                cookies->Begin.store(curBegin); // rollback
                                return false;
                            }
                            return true;
                        }
                    }
                    return false;
                }

                return false;
            });
        }

        bool UpdateCookiesBegin(const char* curBegin, const char* contBegin) {
            if (!Owner) {
                return false;
            }
            return Visit(Owner, [curBegin, contBegin](EType type, auto& value) mutable {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                    if (type == EType::SHARED_DATA_CONTROLLED) {
                        auto* begin = const_cast<char*>(value.data() + value.size() - CookiesSize);
                        TCookies* cookies = reinterpret_cast<TCookies*>(begin);
                        return cookies->Begin.compare_exchange_weak(curBegin, contBegin);
                    }
                }
                return false;
            });
        }

        bool UpdateCookiesEnd(const char* curEnd, const char* contEnd) {
            if (!Owner) {
                return false;
            }
            return Visit(Owner, [curEnd, contEnd](EType type, auto& value) mutable {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                    if (type == EType::SHARED_DATA_CONTROLLED) {
                        auto* begin = const_cast<char*>(value.data() + value.size() - CookiesSize);
                        TCookies* cookies = reinterpret_cast<TCookies*>(begin);
                        return cookies->End.compare_exchange_weak(curEnd, contEnd);
                    }
                }
                return false;
            });
        }

        void Disown() {
            if (Owner) {
                const EType type = static_cast<EType>(Owner.Data[0] & TypeMask);

                if (type == EType::SHARED_DATA_CONTROLLED) {
                    bool isPrivate = Visit(Owner, [](EType type, auto& value) {
                        using T = std::decay_t<decltype(value)>;
                        if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                            if (type == EType::SHARED_DATA_CONTROLLED) {
                                return value.IsPrivate();
                            }
                        }
                        return false;
                    });
                    if (!isPrivate) {
                        Owner.Data[0] = (Owner.Data[0] & ValueMask) | static_cast<uintptr_t>(EType::SHARED_DATA);
                    }
                }
            }
        }

        template <class TResult>
        TResult GetRaw() const {
            if (!Owner) {
                return TResult{};
            }
            return Visit(Owner, [](EType type, auto& value) {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, TResult>) {
                    if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                        if (type == EType::SHARED_DATA_CONTROLLED) {
                            NActors::TSharedData data = value;
                            data.Trim(data.size() - CookiesSize);
                            return data;
                        }
                    }
                    return value;
                } else {
                    Y_FAIL();
                    return TResult{}; // unreachable
                }
            });
        }

        NActors::TSharedData GetRawTrimmed(size_t size) const {
            NActors::TSharedData result = GetRaw<NActors::TSharedData>();
            result.Trim(size);
            return result;
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
                case EType::STRING:                  return wrapper(reinterpret_cast<std::conditional_t<IsConst, const TString&, TString&>>(value));
                case EType::SHARED_DATA_CONTROLLED:  [[fallthrough]];
                case EType::SHARED_DATA:             return wrapper(reinterpret_cast<std::conditional_t<IsConst, const NActors::TSharedData&, NActors::TSharedData&>>(value));
                case EType::ROPE_CHUNK_BACKEND:      return wrapper(reinterpret_cast<std::conditional_t<IsConst, const IContiguousChunk::TPtr&, IContiguousChunk::TPtr&>>(value));
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

    static constexpr struct TOwnedSlice {} OwnedSlice{};

    TBackend Backend; // who actually holds the data
    const char *Begin; // data start
    const char *End; // data end

    explicit TContiguousData(NActors::TSharedData s, TBackend::TCookies* cookies, TBackend::TControlToken)
        : Backend(std::move(s), TBackend::ControlToken)
    {
        auto span = Backend.GetData();
        Begin = span.data();
        End = Begin + span.size();
        cookies->Begin.store(Begin);
        cookies->End.store(End);
    }

    TContiguousData(TOwnedSlice, const char *data, size_t size, const TContiguousData& from)
        : TContiguousData(from.Backend, {data, size})
    {
        Y_VERIFY(data >= from.GetData());
        Y_VERIFY(data < from.GetData() + from.GetSize());
        Y_VERIFY(data + size <= from.GetData() + from.GetSize());
        Backend.UpdateCookiesUnsafe(Begin, End);
    }

    TContiguousData(TOwnedSlice, const char *begin, const char *end, const TContiguousData& from)
        : TContiguousData(OwnedSlice, begin, end - begin, from)
    {}

public:
    static constexpr struct TSlice {} Slice{};

    enum class EResizeResult {
        NoAlloc,
        Alloc,
    };

    enum class EResizeStrategy {
        KeepRooms,
        FailOnCopy,
        // SaveAllocs, // Move data if there is enough space in (headroom + size + tailroom)
    };

    TContiguousData()
        : Begin(nullptr)
        , End(nullptr)
    {}

    template<typename T>
    TContiguousData(T&& backend, const TContiguousSpan& data)
        : Backend(std::forward<T>(backend))
        , Begin(data.data())
        , End(Begin + data.size())
    {}

    explicit TContiguousData(TString s)
#ifndef KIKIMR_TRACE_CONTIGUOUS_DATA_GROW
        : Backend(std::move(s))
#endif
    {
#ifdef KIKIMR_TRACE_CONTIGUOUS_DATA_GROW
        auto ss = NActors::TSharedData::Copy(s.data(), s.size());
        TBackTracingOwner::FakeOwner(ss, TBackTracingOwner::INFO_COPIED_STRING);
        Backend = TBackend(std::move(ss));
#endif
        auto span = Backend.GetData();
        Begin = span.data();
        End = Begin + span.size();
    }

    explicit TContiguousData(NActors::TSharedData s)
#ifndef KIKIMR_TRACE_CONTIGUOUS_DATA_GROW
        : Backend(std::move(s))
#endif
    {
#ifdef KIKIMR_TRACE_CONTIGUOUS_DATA_GROW
        TBackTracingOwner::FakeOwner(s, TBackTracingOwner::INFO_FROM_SHARED_DATA);
        Backend = TBackend(std::move(s));
#endif
        auto span = Backend.GetData();
        Begin = span.data();
        End = Begin + span.size();
    }

    TContiguousData(IContiguousChunk::TPtr backend)
        : TContiguousData(backend, backend->GetData())
    {}

    TContiguousData(TSlice, const char *data, size_t size, const TContiguousData& from)
        : TContiguousData(from.Backend, {data, size})
    {
        Y_VERIFY(data >= from.GetData());
        Y_VERIFY(data < from.GetData() + from.GetSize());
        Y_VERIFY(data + size <= from.GetData() + from.GetSize());
        Backend.Disown();
    }

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

    static TContiguousData Uninitialized(size_t size, size_t headroom = 0, size_t tailroom = 0)
    {
        if (size == 0) {
            return TContiguousData();
        }
        if (headroom == 0 && tailroom == 0) {
#ifdef KIKIMR_TRACE_CONTIGUOUS_DATA_GROW
            NActors::TSharedData res = TBackTracingOwner::Allocate(size, TBackTracingOwner::INFO_ALLOC_UNINITIALIZED);
#else
            NActors::TSharedData res = NActors::TSharedData::Uninitialized(size);
#endif
            return TContiguousData(
                OwnedSlice,
                res.data(),
                res.data() + res.size(),
                TContiguousData(res));
        } else {
#ifdef KIKIMR_TRACE_CONTIGUOUS_DATA_GROW
            NActors::TSharedData res = TBackTracingOwner::Allocate(TBackend::TCookies::BytesToAlloc(size + headroom + tailroom), TBackTracingOwner::INFO_ALLOC_UNINIT_ROOMS);
#else
            NActors::TSharedData res = NActors::TSharedData::Uninitialized(TBackend::TCookies::BytesToAlloc(size + headroom + tailroom));
#endif
            auto isAligned = [](const void * ptr, std::uintptr_t alignment) noexcept {
                auto iptr = reinterpret_cast<std::uintptr_t>(ptr);
                return !(iptr % alignment);
            };
            char* place = res.mutable_data() + res.size() - sizeof(TBackend::TCookies);
            Y_VERIFY(isAligned(place, alignof(TBackend::TCookies)));
            TBackend::TCookies* cookies = new(place) TBackend::TCookies();

            return TContiguousData(
                OwnedSlice,
                res.data() + headroom,
                res.data() + headroom + size,
                TContiguousData(res, cookies, TBackend::ControlToken));
        }
    }

    template <class TType>
    bool ContainsNativeType() const {
        return Backend.ContainsNativeType<TType>();
    }

    template <class TResult>
    TResult GetRaw() const {
        return Backend.GetRaw<TResult>();
    }

    NActors::TSharedData GetRawTrimmed(size_t size) const {
        return Backend.GetRawTrimmed(size);
    }

    bool ReferencesWholeContainer() const {
        return Backend.GetData().size() == GetSize();
    }


    bool ReferencesTrimableToWholeContainer() const {
        if (ContainsNativeType<NActors::TSharedData>()) {
            return Backend.GetData().size() == (GetSize() + UnsafeTailroom());
        } else {
            return ReferencesWholeContainer();
        }
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
        if (ContainsNativeType<TResult>() && (ReferencesWholeContainer() || ReferencesTrimableToWholeContainer())) {
            using T = std::decay_t<TResult>;
            if constexpr (std::is_same_v<T, NActors::TSharedData>) {
                return GetRawTrimmed(GetSize());
            } else {
                return GetRaw<TResult>();
            }
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

    const char* begin() const {
        return Begin;
    }

    const char* end() const {
        return End;
    }

    EResizeResult GrowFront(size_t size, EResizeStrategy strategy = EResizeStrategy::KeepRooms) {
        if (Headroom() >= size && Backend.UpdateCookiesBegin(Begin, Begin - size)) {
            Begin -= size;
            return EResizeResult::NoAlloc;
        } else {
#ifdef KIKIMR_TRACE_CONTIGUOUS_DATA_GROW
            if (Backend.ContainsNativeType<NActors::TSharedData>()) {
                Cerr << "GrowFront# " << size;
                NActors::TSharedData data = GetRaw<NActors::TSharedData>();
                TBackTracingOwner::UnsafePrintBackTrace(data);
            }
#endif
            if (strategy == EResizeStrategy::FailOnCopy && static_cast<bool>(Backend)) {
                Y_FAIL("Fail on grow");
            }
            auto newData = TContiguousData::Uninitialized(size + GetSize(), UnsafeHeadroom() > size ? UnsafeHeadroom() - size : 0, UnsafeTailroom());
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
#ifdef KIKIMR_TRACE_CONTIGUOUS_DATA_GROW
            if (Backend.ContainsNativeType<NActors::TSharedData>()) {
                Cerr << "GrowBack# " << size;
                NActors::TSharedData data = GetRaw<NActors::TSharedData>();
                TBackTracingOwner::UnsafePrintBackTrace(data);
            }
#endif
            if (strategy == EResizeStrategy::FailOnCopy && static_cast<bool>(Backend)) {
                Y_FAIL("Fail on grow");
            }
            auto newData = TContiguousData::Uninitialized(size + GetSize(), UnsafeHeadroom(), UnsafeTailroom() > size ? UnsafeTailroom() - size : 0);
            if (auto data = GetData(); data) {
                std::memcpy(newData.UnsafeGetDataMut(), GetData(), GetSize());
            }
            *this = std::move(newData);
            return EResizeResult::Alloc;
        }
    }

    void Trim(size_t size, size_t frontOffset = 0) {
        Y_VERIFY(size <= End - Begin - frontOffset);
        Backend.Disown();
        Begin = Begin + frontOffset;
        End = Begin + size;
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
