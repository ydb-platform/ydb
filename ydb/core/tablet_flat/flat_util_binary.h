#pragma once

#include "util_deref.h"
#include <util/stream/output.h>
#include <util/generic/array_ref.h>

namespace NKikimr {
namespace NUtil {
namespace NBin {

    template<typename L, typename = decltype(std::declval<L>().begin())>
    static inline size_t SizeOfOne_(const L &list) noexcept
    {
        auto num = std::distance(list.begin(), list.end());

        return sizeof(decltype(*list.begin())) * num;
    }

    static inline size_t SizeOf() noexcept { return 0; }

    template<typename T, typename ... Tail>
    inline size_t SizeOf(const T &left, Tail && ... tail) noexcept
    {
        return SizeOfOne_<T>(left) + SizeOf(std::forward<Tail>(tail)...);
    }

    template<typename T, typename = TStdLayoutOrVoid<T>>
    static inline char* ToByte(T *ptr)
    {
        return static_cast<char*>(static_cast<void*>(ptr));
    }

    template<typename T, typename = TStdLayoutOrVoid<T>>
    static inline const char* ToByte(const T *ptr)
    {
        return static_cast<char*>(static_cast<void*>(ptr));
    }

    template<typename T, typename = TStdLayout<T>>
    static inline TArrayRef<const char> ToRef(T &value)
    {
        return { ToByte(&value), sizeof(value) };
    }

    struct TPut {
        TPut(void *ptr) : Ptr(static_cast<char*>(ptr)) { }

        char* operator*() const noexcept
        {
            return Ptr;
        }

        template<typename L, class V = decltype(&*std::declval<L>().begin())>
        TPut& Put(const L &list) noexcept
        {
            auto *array = reinterpret_cast<V>(Ptr);

            Ptr = ToByte(std::copy(list.begin(), list.end(), array));

            return *this;
        }

        template<typename T, typename = TStdLayout<T>>
        T* Skip() noexcept
        {
            return reinterpret_cast<T*>(std::exchange(Ptr, Ptr + sizeof(T)));
        }

    private:
        char *Ptr = nullptr;
    };

    struct TOut {
        TOut(IOutputStream &out) : Out(out) { }

        template<typename T, typename = TStdLayout<T>>
        inline TOut& Put(const T &val)
        {
            Out.Write(static_cast<const void*>(&val), sizeof(val));

            return *this;
        }

        inline TOut& Array(TStringBuf array)
        {
            return Array<const char>(array);
        }

        template<typename T, typename = TStdLayout<T>>
        inline TOut& Array(const TVector<T> &array)
        {
            return Array(TArrayRef<const T>(array.begin(), array.end()));
        }

        template<typename T, typename = TStdLayout<T>>
        inline TOut& Array(TArrayRef<const T> array)
        {
            auto *ptr = static_cast<const void*>(array.begin());

            return Out.Write(ptr, array.size() * sizeof(T)), *this;
        }

    private:
        IOutputStream &Out;
    };
}
}
}
