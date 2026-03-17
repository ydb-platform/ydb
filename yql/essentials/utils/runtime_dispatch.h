#pragma once

#include <cstddef>
#include <utility>

namespace NYql::NPrivate::NRuntimeDispatch {

template <typename... T>
struct TTypeList {};

template <bool...>
struct TValueList {};

template <size_t N, class F, bool... Values, typename... TArgs>
decltype(auto) RuntimeDispatchOneElement(F&& func,
                                         TValueList<Values...>,
                                         bool boolArg,
                                         TArgs&&... restArgs) {
    if (boolArg) {
        return RuntimeDispatchImpl<N - 1, F>(
            std::forward<F>(func),
            TValueList<Values..., true>(), std::forward<TArgs>(restArgs)...);
    } else {
        return RuntimeDispatchImpl<N - 1, F>(
            std::forward<F>(func),
            TValueList<Values..., false>(), std::forward<TArgs>(restArgs)...);
    }
}

template <size_t N, class F, bool... Values, typename... TArgs>
decltype(auto) RuntimeDispatchImpl(F&& func,
                                   TValueList<Values...>, TArgs&&... args) {
    static_assert(sizeof...(TArgs) >= N,
                  "RuntimeDispatch requires at least N arguments");
    if constexpr (N == 0) {
        return func.template operator()<Values...>(std::forward<TArgs>(args)...);
    } else {
        return RuntimeDispatchOneElement<N, F>(
            std::forward<F>(func), TValueList<Values...>(),
            std::forward<TArgs>(args)...);
    }
}

template <size_t N, class F, typename... TArgs>
auto RuntimeDispatch(F&& func, TArgs&&... args) {
    return RuntimeDispatchImpl<N, F>(std::forward<F>(func), TValueList<>(), std::forward<TArgs>(args)...);
}

} // namespace NYql::NPrivate::NRuntimeDispatch

#define YQL_PRIVATE_WRAP_LAMBDA_FOR_FUNCTION(call, returnType)     \
    []<bool... Bs>(auto&&... args) -> returnType {                 \
        return call<Bs...>(std::forward<decltype(args)>(args)...); \
    }

#define YQL_PRIVATE_WRAP_LAMBDA_FOR_NEW(constructor, returnType)              \
    []<bool... Bs>(auto&&... args) -> returnType {                            \
        return new constructor<Bs...>(std::forward<decltype(args)>(args)...); \
    }

// Dispatches at runtime to a template function based on N leading arguments.
// Avoids the if/else branch hell. Example:
//
//   Before:
//     if (a && b) {
//         return Foo<true, true>(x);
//     } else if (a) {
//         return Foo<true, false>(x);
//     } else if (b) {
//         return Foo<false, true>(x);
//     } else {
//         return Foo<false, false>(x);
//     }
//
//   After:
//     return YQL_RUNTIME_DISPATCH(Foo, 2, a, b, x);
#define YQL_RUNTIME_DISPATCH(funcName, N, ...) \
    (NYql::NPrivate::NRuntimeDispatch::RuntimeDispatch<(N)>(YQL_PRIVATE_WRAP_LAMBDA_FOR_FUNCTION(funcName, decltype(auto)), __VA_ARGS__))

// Same as YQL_RUNTIME_DISPATCH, but for the new operator.
// Example:
//
//   Before:
//     if (a && b) {
//         return new Foo<true, true>(x);
//     } else if (a) {
//         return new Foo<true, false>(x);
//     } else if (b) {
//         return new Foo<false, true>(x);
//     } else {
//         return new Foo<false, false>(x);
//     }
//
//   After:
//     return YQL_RUNTIME_DISPATCH_NEW(Base, Foo, 2, a, b, x);
#define YQL_RUNTIME_DISPATCH_NEW(returnType, constructor, N, ...) \
    (NYql::NPrivate::NRuntimeDispatch::RuntimeDispatch<(N)>(YQL_PRIVATE_WRAP_LAMBDA_FOR_NEW(constructor, returnType), __VA_ARGS__))
