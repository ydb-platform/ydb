#include "func_common.h"

namespace cp = arrow::compute;

namespace NKikimr::NKernels {

template <typename T, typename TUnsigned = typename std::make_unsigned<T>::type>
constexpr TUnsigned ToUnsigned(T sgnd) {
  return static_cast<TUnsigned>(sgnd);
}

struct TMultiply {
    static_assert(std::is_same<decltype(int8_t() * int8_t()), int32_t>::value, "");
    static_assert(std::is_same<decltype(uint8_t() * uint8_t()), int32_t>::value, "");
    static_assert(std::is_same<decltype(int16_t() * int16_t()), int32_t>::value, "");
    static_assert(std::is_same<decltype(uint16_t() * uint16_t()), int32_t>::value, "");
    static_assert(std::is_same<decltype(int32_t() * int32_t()), int32_t>::value, "");
    static_assert(std::is_same<decltype(uint32_t() * uint32_t()), uint32_t>::value, "");
    static_assert(std::is_same<decltype(int64_t() * int64_t()), int64_t>::value, "");
    static_assert(std::is_same<decltype(uint64_t() * uint64_t()), uint64_t>::value, "");

    template <typename T, typename TArg0, typename TArg1>
    static constexpr arrow::enable_if_floating_point<T> Call(cp::KernelContext*, T left, T right,
                                                            arrow::Status*) {
        return left * right;
    }

    template <typename T, typename TArg0, typename TArg1>
    static constexpr std::enable_if_t<IsUnsignedInteger<T>::value && !std::is_same<T, uint16_t>::value, T>
    Call(cp::KernelContext*, T left, T right, arrow::Status*) {
        return left * right;
    }

    template <typename T, typename TArg0, typename TArg1>
    static constexpr std::enable_if_t<IsSignedInteger<T>::value && !std::is_same<T, int16_t>::value, T>
    Call(cp::KernelContext*, T left, T right, arrow::Status*) {
        return ToUnsigned(left) * ToUnsigned(right);
    }

    // Multiplication of 16 bit integer types implicitly promotes to signed 32 bit
    // integer. However, some inputs may nevertheless overflow (which triggers undefined
    // behaviour). Therefore we first cast to 32 bit unsigned integers where overflow is
    // well defined.
    template <typename T, typename TArg0, typename TArg1>
    static constexpr arrow::enable_if_same<T, int16_t, T> Call(cp::KernelContext*, int16_t left,
                                                                int16_t right, arrow::Status*) {
        return static_cast<uint32_t>(left) * static_cast<uint32_t>(right);
    }
    template <typename T, typename TArg0, typename TArg1>
    static constexpr arrow::enable_if_same<T, uint16_t, T> Call(cp::KernelContext*, uint16_t left,
                                                                uint16_t right, arrow::Status*) {
        return static_cast<uint32_t>(left) * static_cast<uint32_t>(right);
    }
};

}
