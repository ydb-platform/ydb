#pragma once

#include <cstddef>

namespace NYql {

template <typename T>
struct TArity;

template <typename Ret, typename... Args>
struct TArity<Ret(Args...)> {
    static constexpr std::size_t value = sizeof...(Args);
};

template <typename Ret, typename... Args>
struct TArity<Ret (*)(Args...)>: TArity<Ret(Args...)> {};

#define METHOD_ARITY(TAIL)                                        \
    template <typename Ret, typename Class, typename... Args>     \
    struct TArity<Ret (Class::*)(Args...) TAIL> {                 \
        static constexpr std::size_t value = 1 + sizeof...(Args); \
    };

METHOD_ARITY()
METHOD_ARITY(const)
METHOD_ARITY(volatile)
METHOD_ARITY(const volatile)
METHOD_ARITY(&)
METHOD_ARITY(const&)
METHOD_ARITY(&&)
METHOD_ARITY(const&&)
METHOD_ARITY(noexcept)
METHOD_ARITY(const noexcept)
METHOD_ARITY(volatile noexcept)
METHOD_ARITY(const volatile noexcept)

#undef METHOD_ARITY

}; // namespace NYql

template <typename T>
inline constexpr std::size_t arity_v = NYql::TArity<T>::value;
