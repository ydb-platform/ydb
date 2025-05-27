#pragma once

#include <yql/essentials/public/udf/udf_value.h>

#if defined(_msan_enabled_) && defined(__linux__)
    #define SHOULD_WRAP_ALL_UNBOXED_VALUES_FOR_CODEGEN 1
#else
    #define SHOULD_WRAP_ALL_UNBOXED_VALUES_FOR_CODEGEN 0
#endif

namespace NYql {

// Concept that checks if a type is a free function.
template <typename T>
concept FunctionPointer = std::is_pointer_v<T> &&
                          std::is_function_v<std::remove_pointer_t<T>>;

// Concept that checks if a type is a pointer-to-member function.
template <typename T>
concept MethodPointer = std::is_member_function_pointer_v<T>;

// When compling with msan you have to replace all NUdf::TUnboxedValuePod with __int128_t.
// See YQL-19520#67da4c599dd9e93523567aff for details.
// This helpers help to solve the problem by converting each NUdf::TUnboxedValuePod with __int128_t for passed method.
// For example:
// 1. You have a function
// NUdf::TUnboxedValuePod Func(OtherType a, NUdf::TUnboxedValuePod b) {
//   ...
// }
// 2. You call GetMethodPtr<&Func>()
// 3. You recieve pointer to function that do something like this:
// __int128_t FuncWrapper(OtherType a, __int128_t b) {
//    NUdf::TUnboxedValuePod realB;
//    memcpy(&realB, &b, sizeof(b));
//    NUdf::TUnboxedValuePod result = Func(std::move(a), NUdf::TUnboxedValuePod(std::move(b)));
//   __int128_t fakeResult;
//   memcpy(&fakeResult, &result, sizeof(fakeResult));
//   return fakeResult;
// }

#if SHOULD_WRAP_ALL_UNBOXED_VALUES_FOR_CODEGEN
template <FunctionPointer Method>
inline uintptr_t GetMethodPtrNumber(Method method) {
    uintptr_t ptr;
    std::memcpy(&ptr, &method, sizeof(uintptr_t));
    return ptr;
}

template <typename T>
struct TReplaceUnboxedValuePodWithUInt128 {
    using TType = T;
};

template <>
struct TReplaceUnboxedValuePodWithUInt128<NUdf::TUnboxedValuePod> {
    using TType = __int128_t;
};

template <typename T>
using TReplaceUnboxedValuePodWithUInt128_t =
    typename TReplaceUnboxedValuePodWithUInt128<T>::TType;

template <typename TR, typename... TArgs>
struct TFunctionWrapper {
    template <FunctionPointer auto function>
    static TReplaceUnboxedValuePodWithUInt128_t<TR> Wrapper(TReplaceUnboxedValuePodWithUInt128_t<TArgs>... wargs) {
        // Call the original function with converted parameters.
        if constexpr (std::is_same_v<TR, void>) {
            function(ConvertArg<TArgs>(TReplaceUnboxedValuePodWithUInt128_t<TArgs>(std::move(wargs)))...);
            return;
        } else {
            return ConvertReturn<TR>(function(ConvertArg<TArgs>(TReplaceUnboxedValuePodWithUInt128_t<TArgs>(std::move(wargs)))...));
        }
    }

private:
    template <typename T>
    static T ConvertArg(TReplaceUnboxedValuePodWithUInt128_t<T> arg Y_LIFETIME_BOUND) {
        if constexpr (std::is_same_v<std::remove_const_t<T>, NUdf::TUnboxedValuePod>) {
            NUdf::TUnboxedValuePod tmp;
            std::memcpy(&tmp, &arg, sizeof(T));
            return tmp;
        } else {
            return std::forward<TReplaceUnboxedValuePodWithUInt128_t<T>>(arg);
        }
    }

    template <typename T>
    static TReplaceUnboxedValuePodWithUInt128_t<T> ConvertReturn(T arg Y_LIFETIME_BOUND) {
        if constexpr (std::is_same_v<std::remove_const_t<T>, NUdf::TUnboxedValuePod>) {
            __int128_t tmp;
            std::memcpy(&tmp, &arg, sizeof(T));
            return tmp;
        } else {
            return std::forward<T>(arg);
        }
    }
};

template <FunctionPointer auto func, typename TR, typename... TArgs>
inline auto DoGetFreeFunctionPtrInternal() {
    return &(TFunctionWrapper<TR, TArgs...>::template Wrapper<func>);
}

template <FunctionPointer auto func>
inline auto DoGetFreeFunctionPtr() {
    return []<typename TR, typename... TArgs>(TR (*fptr)(TArgs...)) {
        Y_UNUSED(fptr, "For type deducing only.");
        return DoGetFreeFunctionPtrInternal<func, TR, TArgs...>();
    }(func);
}

template <FunctionPointer auto func>
inline auto GetMethodPtr() {
    return GetMethodPtrNumber(DoGetFreeFunctionPtr<func>());
}

template <MethodPointer auto func, typename TR, typename TM, typename... TArgs>
inline TR Adapter(TM obj, TArgs&&... args) {
    return (obj->*func)(std::forward<TArgs>(args)...);
}

template <MethodPointer auto func, typename TR, typename TM, typename... TArgs>
inline auto GetMethodPtrImpl() {
    return DoGetFreeFunctionPtrInternal<&Adapter<func, TR, TM, TArgs...>, TR, TM, TArgs...>();
}

template <typename T>
struct is_const_member_function_pointer: std::false_type {};

template <typename TR, typename TM, typename... TArgs>
struct is_const_member_function_pointer<TR (TM::*)(TArgs...) const>: std::true_type {};

template <MethodPointer auto func>
inline auto DoGetMethodPtr() {
    // Just an template helper to get TArgs..., R, T from func.
    if constexpr (is_const_member_function_pointer<decltype(func)>::value) {
        return []<typename TR, typename TM, typename... TArgs>(TR (TM::*fptr)(TArgs...) const) {
            Y_UNUSED(fptr);
            return GetMethodPtrImpl<func, TR, TM*, TArgs...>();
        }(func);
    } else {
        return []<typename TR, typename TM, typename... TArgs>(TR (TM::*fptr)(TArgs...)) {
            Y_UNUSED(fptr);
            return GetMethodPtrImpl<func, TR, TM*, TArgs...>();
        }(func);
    }
}

template <MethodPointer auto func>
inline uintptr_t GetMethodPtr() {
    return GetMethodPtrNumber(DoGetMethodPtr<func>());
}
#else  // SHOULD_WRAP_ALL_UNBOXED_VALUES_FOR_CODEGEN

namespace NInternal {
template <typename Method>
inline uintptr_t GetMethodPtr(Method method) {
    uintptr_t ptr;
    std::memcpy(&ptr, &method, sizeof(uintptr_t));
    return ptr;
}
} // namespace NInternal

template <MethodPointer auto func>
inline uintptr_t GetMethodPtr() {
    return NInternal::GetMethodPtr(func);
}

template <FunctionPointer auto func>
inline uintptr_t GetMethodPtr() {
    return NInternal::GetMethodPtr(func);
}
#endif // SHOULD_WRAP_ALL_UNBOXED_VALUES_FOR_CODEGEN

} // namespace NYql
