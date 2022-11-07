#pragma once
#include <type_traits>
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NKernels {

constexpr size_t NextSize(size_t size) {
    if (size < 8) {
        return size * 2;
    }
    return size;
}

struct TError {};

template <bool is_signed, bool is_floating, size_t size>
struct TConstruct {
    using Type = TError;
};

template <> struct TConstruct<false, false, 1> { using Type = arrow::UInt8Type; };
template <> struct TConstruct<false, false, 2> { using Type = arrow::UInt16Type; };
template <> struct TConstruct<false, false, 4> { using Type = arrow::UInt32Type; };
template <> struct TConstruct<false, false, 8> { using Type = arrow::UInt64Type; };
template <> struct TConstruct<false, true, 1> { using Type = arrow::FloatType; };
template <> struct TConstruct<false, true, 2> { using Type = arrow::FloatType; };
template <> struct TConstruct<false, true, 4> { using Type = arrow::FloatType; };
template <> struct TConstruct<false, true, 8> { using Type = arrow::DoubleType; };
template <> struct TConstruct<true, false, 1> { using Type = arrow::Int8Type; };
template <> struct TConstruct<true, false, 2> { using Type = arrow::Int16Type; };
template <> struct TConstruct<true, false, 4> { using Type = arrow::Int32Type; };
template <> struct TConstruct<true, false, 8> { using Type = arrow::Int64Type; };
template <> struct TConstruct<true, true, 1> { using Type = arrow::FloatType; };
template <> struct TConstruct<true, true, 2> { using Type = arrow::FloatType; };
template <> struct TConstruct<true, true, 4> { using Type = arrow::FloatType; };
template <> struct TConstruct<true, true, 8> { using Type = arrow::DoubleType; };

template <typename A, typename B>
struct TResultOfAdditionMultiplication {
    using Type = typename TConstruct<
        std::is_signed_v<A> || std::is_signed_v<B>,
        std::is_floating_point_v<A> || std::is_floating_point_v<B>,
        NextSize(std::max(sizeof(A), sizeof(B)))>::Type;
};

template <typename A, typename B>
struct TResultOfSubtraction {
    using Type = typename TConstruct<
        true,
        std::is_floating_point_v<A> || std::is_floating_point_v<B>,
        NextSize(std::max(sizeof(A), sizeof(B)))>::Type;
};

template <typename A, typename B>
struct TResultOfFloatingPointDivision {
    using Type = arrow::DoubleType;
};

template <typename A, typename B>
struct TResultOfIntegerDivision {
    using Type = typename TConstruct<
        std::is_signed_v<A> || std::is_signed_v<B>,
        false,
        sizeof(A)>::Type;
};

template <typename A, typename B>
struct TResultOfModulo {
    static constexpr bool result_is_signed = std::is_signed_v<A>;
    /// If modulo of division can yield negative number, we need larger type to accommodate it.
    /// Example: toInt32(-199) % toUInt8(200) will return -199 that does not fit in Int8, only in Int16.
    static constexpr size_t size_of_result = result_is_signed ? NextSize(sizeof(B)) : sizeof(B);
    using Type0 = typename TConstruct<result_is_signed, false, size_of_result>::Type;
    using Type = std::conditional_t<std::is_floating_point_v<A> || std::is_floating_point_v<B>, arrow::DoubleType, Type0>;
};

template <typename A>
struct TResultOfNegate {
    using Type = typename TConstruct<
        true,
        std::is_floating_point_v<A>,
        std::is_signed_v<A> ? sizeof(A) : NextSize(sizeof(A))>::Type;
};

template <typename A>
struct TResultOfAbs {
    using Type = typename TConstruct<
        false,
        std::is_floating_point_v<A>,
        sizeof(A)>::Type;
};

template <typename A> struct TToInteger {
    using Type = typename TConstruct<
        std::is_signed_v<A>,
        false,
        std::is_floating_point_v<A> ? 8 : sizeof(A)>::Type;
};

}
