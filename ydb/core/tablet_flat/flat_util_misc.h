#pragma once

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <typeinfo>

namespace NKikimr {
namespace NUtil {

    struct TAsIs {
        template<typename TWrap>
        static constexpr auto Do(TWrap &wrap) noexcept -> decltype(*&wrap)
        {
            return wrap;
        }
    };

    struct TSecond {
        template<typename TWrap>
        static constexpr auto Do(TWrap &wrap) noexcept -> decltype(*&wrap.second)
        {
            return wrap.second;
        }
    };

    template<typename T>
    struct TDtorDel {
        static inline void Destroy(T* ptr) noexcept
        {
            ptr->~T();

            ::operator delete(ptr);
        }
    };


    template <class T, class = std::enable_if_t<std::is_integral<T>::value>>
    struct TIncDecOps {
        static inline void Acquire(T* value) noexcept
        {
            ++*value;
        }

        static inline void Release(T* value) noexcept
        {
            --*value;
        }
    };

    template <typename TVal>
    TVal SubSafe(TVal &val, TVal sub) noexcept
    {
        Y_ABORT_UNLESS(val >= sub, "Counter is underflowed");

        return val -= Min(val, sub);
    }

    template<typename TVal, typename TBase>
    TVal* ExactCast(TBase *base) noexcept
    {
        bool same = (typeid(*base) == typeid(TVal));

        return !same ? nullptr: static_cast<TVal*>(base);
    }

}
}
