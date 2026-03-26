#pragma once
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/status.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type_fwd.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type_traits.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/builder.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/kernels/codegen_internal.h>
#pragma clang diagnostic pop

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/function.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/cast.h>

#include <util/datetime/base.h>
#include <util/system/yassert.h>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <vector>

namespace cp = arrow20::compute;

namespace NKikimr::NKernels {

template <template <typename... Args> class KernelGenerator, typename Op>
cp::ArrayKernelExec ArithmeticBinaryExec(cp::internal::detail::GetTypeId getId) {
    switch (getId.id) {
        case arrow20::Type::INT8:
            return KernelGenerator<arrow20::Int8Type, arrow20::Int8Type, arrow20::Int8Type, Op>::Exec;
        case arrow20::Type::UINT8:
            return KernelGenerator<arrow20::UInt8Type, arrow20::UInt8Type, arrow20::UInt8Type, Op>::Exec;
        case arrow20::Type::INT16:
            return KernelGenerator<arrow20::Int16Type, arrow20::Int16Type, arrow20::Int16Type, Op>::Exec;
        case arrow20::Type::UINT16:
            return KernelGenerator<arrow20::UInt16Type, arrow20::UInt16Type, arrow20::UInt16Type, Op>::Exec;
        case arrow20::Type::INT32:
            return KernelGenerator<arrow20::Int32Type, arrow20::Int32Type, arrow20::Int32Type, Op>::Exec;
        case arrow20::Type::UINT32:
            return KernelGenerator<arrow20::UInt32Type, arrow20::UInt32Type, arrow20::UInt32Type, Op>::Exec;
        case arrow20::Type::INT64:
            return KernelGenerator<arrow20::Int64Type, arrow20::Int64Type, arrow20::Int64Type, Op>::Exec;
        case arrow20::Type::TIMESTAMP:
            return KernelGenerator<arrow20::Int64Type, arrow20::Int64Type, arrow20::Int64Type, Op>::Exec;
        case arrow20::Type::UINT64:
            return KernelGenerator<arrow20::UInt64Type, arrow20::UInt64Type, arrow20::UInt64Type, Op>::Exec;
        case arrow20::Type::FLOAT:
            return KernelGenerator<arrow20::FloatType, arrow20::FloatType, arrow20::FloatType, Op>::Exec;
        case arrow20::Type::DOUBLE:
            return KernelGenerator<arrow20::DoubleType, arrow20::DoubleType, arrow20::DoubleType, Op>::Exec;
        default:
            Y_ABORT_UNLESS(false);
            return cp::internal::ExecFail;
    }
}

template <template <typename... Args> class KernelGenerator, typename Op>
cp::ArrayKernelExec ArithmeticBinaryIntExec(cp::internal::detail::GetTypeId getId) {
    switch (getId.id) {
        case arrow20::Type::INT8:
            return KernelGenerator<arrow20::Int8Type, arrow20::Int8Type, arrow20::Int8Type, Op>::Exec;
        case arrow20::Type::UINT8:
            return KernelGenerator<arrow20::UInt8Type, arrow20::UInt8Type, arrow20::UInt8Type, Op>::Exec;
        case arrow20::Type::INT16:
            return KernelGenerator<arrow20::Int16Type, arrow20::Int16Type, arrow20::Int16Type, Op>::Exec;
        case arrow20::Type::UINT16:
            return KernelGenerator<arrow20::UInt16Type, arrow20::UInt16Type, arrow20::UInt16Type, Op>::Exec;
        case arrow20::Type::INT32:
            return KernelGenerator<arrow20::Int32Type, arrow20::Int32Type, arrow20::Int32Type, Op>::Exec;
        case arrow20::Type::UINT32:
            return KernelGenerator<arrow20::UInt32Type, arrow20::UInt32Type, arrow20::UInt32Type, Op>::Exec;
        case arrow20::Type::INT64:
            return KernelGenerator<arrow20::Int64Type, arrow20::Int64Type, arrow20::Int64Type, Op>::Exec;
        case arrow20::Type::TIMESTAMP:
            return KernelGenerator<arrow20::Int64Type, arrow20::Int64Type, arrow20::Int64Type, Op>::Exec;
        case arrow20::Type::UINT64:
            return KernelGenerator<arrow20::UInt64Type, arrow20::UInt64Type, arrow20::UInt64Type, Op>::Exec;
        default:
            Y_ABORT_UNLESS(false);
            return cp::internal::ExecFail;
    }
}

template <template <typename... Args> class KernelGenerator, typename Op>
cp::ArrayKernelExec ArithmeticUnaryExec(cp::internal::detail::GetTypeId getId) {
    switch (getId.id) {
        case arrow20::Type::INT8:
            return KernelGenerator<arrow20::Int8Type, arrow20::Int8Type, Op>::Exec;
        case arrow20::Type::UINT8:
            return KernelGenerator<arrow20::UInt8Type, arrow20::UInt8Type, Op>::Exec;
        case arrow20::Type::INT16:
            return KernelGenerator<arrow20::Int16Type, arrow20::Int16Type, Op>::Exec;
        case arrow20::Type::UINT16:
            return KernelGenerator<arrow20::UInt16Type, arrow20::UInt16Type, Op>::Exec;
        case arrow20::Type::INT32:
            return KernelGenerator<arrow20::Int32Type, arrow20::Int32Type, Op>::Exec;
        case arrow20::Type::UINT32:
            return KernelGenerator<arrow20::UInt32Type, arrow20::UInt32Type, Op>::Exec;
        case arrow20::Type::INT64:
            return KernelGenerator<arrow20::Int64Type, arrow20::Int64Type, Op>::Exec;
        case arrow20::Type::TIMESTAMP:
            return KernelGenerator<arrow20::Int64Type, arrow20::Int64Type, Op>::Exec;
        case arrow20::Type::UINT64:
            return KernelGenerator<arrow20::UInt64Type, arrow20::UInt64Type, Op>::Exec;
        case arrow20::Type::FLOAT:
            return KernelGenerator<arrow20::FloatType, arrow20::FloatType, Op>::Exec;
        case arrow20::Type::DOUBLE:
            return KernelGenerator<arrow20::DoubleType, arrow20::DoubleType, Op>::Exec;
        default:
            Y_ABORT_UNLESS(false);
            return cp::internal::ExecFail;
    }
}

template <template <typename... Args> class KernelGenerator, typename Op>
cp::ArrayKernelExec MathUnaryExec(cp::internal::detail::GetTypeId getId) {
    switch (getId.id) {
        case arrow20::Type::INT8:
            return KernelGenerator<arrow20::DoubleType, arrow20::Int8Type, Op>::Exec;
        case arrow20::Type::UINT8:
            return KernelGenerator<arrow20::DoubleType, arrow20::UInt8Type, Op>::Exec;
        case arrow20::Type::INT16:
            return KernelGenerator<arrow20::DoubleType, arrow20::Int16Type, Op>::Exec;
        case arrow20::Type::UINT16:
            return KernelGenerator<arrow20::DoubleType, arrow20::UInt16Type, Op>::Exec;
        case arrow20::Type::INT32:
            return KernelGenerator<arrow20::DoubleType, arrow20::Int32Type, Op>::Exec;
        case arrow20::Type::UINT32:
            return KernelGenerator<arrow20::DoubleType, arrow20::UInt32Type, Op>::Exec;
        case arrow20::Type::INT64:
            return KernelGenerator<arrow20::DoubleType, arrow20::Int64Type, Op>::Exec;
        case arrow20::Type::TIMESTAMP:
            return KernelGenerator<arrow20::DoubleType, arrow20::Int64Type, Op>::Exec;
        case arrow20::Type::UINT64:
            return KernelGenerator<arrow20::DoubleType, arrow20::UInt64Type, Op>::Exec;
        case arrow20::Type::FLOAT:
            return KernelGenerator<arrow20::DoubleType, arrow20::FloatType, Op>::Exec;
        case arrow20::Type::DOUBLE:
            return KernelGenerator<arrow20::DoubleType, arrow20::DoubleType, Op>::Exec;
        default:
            Y_ABORT_UNLESS(false);
            return cp::internal::ExecFail;
    }
}

template <template <typename... Args> class KernelGenerator, typename Op>
cp::ArrayKernelExec MathBinaryExec(cp::internal::detail::GetTypeId getId) {
    switch (getId.id) {
        case arrow20::Type::INT8:
            return KernelGenerator<arrow20::DoubleType, arrow20::Int8Type, arrow20::Int8Type, Op>::Exec;
        case arrow20::Type::UINT8:
            return KernelGenerator<arrow20::DoubleType, arrow20::UInt8Type, arrow20::UInt8Type, Op>::Exec;
        case arrow20::Type::INT16:
            return KernelGenerator<arrow20::DoubleType, arrow20::Int16Type, arrow20::Int16Type, Op>::Exec;
        case arrow20::Type::UINT16:
            return KernelGenerator<arrow20::DoubleType, arrow20::UInt16Type, arrow20::UInt16Type, Op>::Exec;
        case arrow20::Type::INT32:
            return KernelGenerator<arrow20::DoubleType, arrow20::Int32Type, arrow20::Int32Type, Op>::Exec;
        case arrow20::Type::UINT32:
            return KernelGenerator<arrow20::DoubleType, arrow20::UInt32Type, arrow20::UInt32Type, Op>::Exec;
        case arrow20::Type::INT64:
            return KernelGenerator<arrow20::DoubleType, arrow20::Int64Type, arrow20::Int64Type, Op>::Exec;
        case arrow20::Type::TIMESTAMP:
            return KernelGenerator<arrow20::DoubleType, arrow20::Int64Type, arrow20::Int64Type, Op>::Exec;
        case arrow20::Type::UINT64:
            return KernelGenerator<arrow20::DoubleType, arrow20::UInt64Type, arrow20::UInt64Type, Op>::Exec;
        case arrow20::Type::FLOAT:
            return KernelGenerator<arrow20::DoubleType, arrow20::FloatType, arrow20::FloatType, Op>::Exec;
        case arrow20::Type::DOUBLE:
            return KernelGenerator<arrow20::DoubleType, arrow20::DoubleType, arrow20::DoubleType, Op>::Exec;
        default:
            Y_ABORT_UNLESS(false);
            return cp::internal::ExecFail;
    }
}


template <typename TOperator, typename TRes>
static arrow20::Status SimpleNullaryExec(cp::KernelContext* ctx, const cp::ExecBatch&, arrow20::Datum* out) {
    *out = arrow20::MakeScalar(TOperator:: template Call<typename cp::internal::GetViewType<TRes>::T>(ctx));
    return arrow20::Status::OK();
}

}
