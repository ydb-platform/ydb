#pragma once
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/status.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type_fwd.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type_traits.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/function.h>

#include <util/system/yassert.h>

#include <type_traits>

#include "execs.h"

namespace cp = arrow20::compute;
using cp::internal::applicator::ScalarBinary;
using cp::internal::applicator::ScalarUnary;

namespace NKikimr::NKernels {

template <typename T>
using IsUnsignedInteger =
    std::integral_constant<bool, std::is_integral<T>::value &&
                                     std::is_unsigned<T>::value>;

template <typename T>
using IsSignedInteger =
    std::integral_constant<bool, std::is_integral<T>::value &&
                                     std::is_signed<T>::value>;

template<typename T>
using IsNumeric = std::integral_constant<bool, IsSignedInteger<T>::value ||
                                                IsUnsignedInteger<T>::value ||
                                                std::is_floating_point<T>::value>;

template<typename TArr>
using IsArrayNumeric = std::integral_constant<bool, arrow20::is_number_type<typename TArr::TypeClass>::value>;


template <typename T, typename R = T>
using EnableIfSigned =
    std::enable_if_t<IsSignedInteger<T>::value, R>;

template <typename T, typename R = T>
using EnableIfUnsigned =
    std::enable_if_t<IsUnsignedInteger<T>::value, R>;

template <typename T, typename R = T>
using EnableIfInteger = std::enable_if_t<IsSignedInteger<T>::value ||
                                            IsUnsignedInteger<T>::value, R>;

template <typename T, typename R = T>
using EnableIfFloatingPoint =
    std::enable_if_t<std::is_floating_point<T>::value, R>;

template <typename T, typename R = T>
using EnableIfFloat64 =
    std::enable_if_t<std::is_same<T, arrow20::TypeTraits<arrow20::DoubleType>::CType>::value, R>;

template <typename T, typename R = T>
using EnableIfFloat32 =
    std::enable_if_t<std::is_same<T, arrow20::TypeTraits<arrow20::FloatType>::CType>::value, R>;

template <typename T, typename R = T>
using EnableIfNumeric =
    std::enable_if_t<IsNumeric<T>::value, R>;


template <typename TType>
using TArray = typename arrow20::TypeTraits<TType>::ArrayType;

template <typename TType>
using TBuilder = typename arrow20::TypeTraits<TType>::BuilderType;

template <typename TSignedInt>
TSignedInt SafeSignedNegate(TSignedInt u) {
    using TUnsignedInt = typename std::make_unsigned<TSignedInt>::type;
    return static_cast<TSignedInt>(~static_cast<TUnsignedInt>(u) + 1);
}

struct TArithmeticFunction : cp::ScalarFunction {
    using ScalarFunction::ScalarFunction;

    arrow20::Result<const arrow20::compute::Kernel*> DispatchBest(std::vector<arrow20::ValueDescr>* values) const override {
        RETURN_NOT_OK(CheckArity(*values));

        using arrow20::compute::detail::DispatchExactImpl;
        if (auto* kernel = DispatchExactImpl(this, *values)) {
            return kernel;
        }

        arrow20::compute::internal::EnsureDictionaryDecoded(values);

        // Only promote types for binary functions
        if (values->size() == 2) {
            arrow20::compute::internal::ReplaceNullWithOtherType(values);
            if (auto type = arrow20::compute::internal::CommonNumeric(*values)) {
                arrow20::compute::internal::ReplaceTypes(type, values);
            }
            #if 0 // TODO: dates + ints
            else if (auto type = arrow20::compute::internal::CommonTimestamp(*values)) {
                arrow20::compute::internal::ReplaceTypes(type, values);
            }
            #endif
        }

        if (auto* kernel = DispatchExactImpl(this, *values)) {
            return kernel;
        }
        return arrow20::compute::detail::NoMatchingKernel(this, *values);
  }
};



template <typename Op>
std::shared_ptr<cp::ScalarFunction> MakeConstNullary(const std::string& name) {
    auto func = std::make_shared<arrow20::compute::ScalarFunction>(name, cp::Arity::Nullary(), nullptr);
    cp::ArrayKernelExec exec = SimpleNullaryExec<Op, arrow20::DoubleType>;
    Y_ABORT_UNLESS(func->AddKernel({}, arrow20::float64(), exec).ok());
    return func;
}


template <typename Op>
std::shared_ptr<cp::ScalarFunction> MakeArithmeticBinary(const std::string& name) {
    auto func = std::make_shared<TArithmeticFunction>(name, cp::Arity::Binary(), nullptr);
    for (const auto& ty : cp::internal::NumericTypes()) {
        auto exec = ArithmeticBinaryExec<ScalarBinary, Op>(ty);
        Y_ABORT_UNLESS(func->AddKernel({ty, ty}, ty, exec).ok());
    }
    return func;
}

template <typename Op>
std::shared_ptr<cp::ScalarFunction> MakeArithmeticIntBinary(const std::string& name) {
    auto func = std::make_shared<TArithmeticFunction>(name, cp::Arity::Binary(), nullptr);
    for (const auto& ty : cp::internal::IntTypes()) {
        auto exec = ArithmeticBinaryIntExec<ScalarBinary, Op>(ty);
        Y_ABORT_UNLESS(func->AddKernel({ty, ty}, ty, exec).ok());
    }
    return func;
}


template <typename Op>
std::shared_ptr<cp::ScalarFunction> MakeArithmeticUnary(const std::string& name) {
    auto func = std::make_shared<TArithmeticFunction>(name, cp::Arity::Unary(), nullptr);
    for (const auto& ty : cp::internal::NumericTypes()) {
        auto exec = ArithmeticUnaryExec<ScalarUnary, Op>(ty);
        Y_ABORT_UNLESS(func->AddKernel({ty}, ty, exec).ok());
    }
    return func;
}

template <typename Op>
std::shared_ptr<cp::ScalarFunction> MakeMathUnary(const std::string& name) {
    auto func = std::make_shared<TArithmeticFunction>(name, cp::Arity::Unary(), nullptr);
    for (const auto& ty : cp::internal::NumericTypes()) {
        auto exec = MathUnaryExec<ScalarUnary, Op>(ty);
        Y_ABORT_UNLESS(func->AddKernel({ty}, arrow20::float64(), exec).ok());
    }
    return func;
}

template <typename Op>
std::shared_ptr<cp::ScalarFunction> MakeMathBinary(const std::string& name) {
    auto func = std::make_shared<TArithmeticFunction>(name, cp::Arity::Binary(), nullptr);
    for (const auto& ty : cp::internal::NumericTypes()) {
        auto exec = MathBinaryExec<ScalarBinary, Op>(ty);
        Y_ABORT_UNLESS(func->AddKernel({ty, ty}, arrow20::float64(), exec).ok());
    }
    return func;
}

}
