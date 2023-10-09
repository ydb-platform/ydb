#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/status.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_fwd.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type_traits.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/builder.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/kernels/codegen_internal.h>
#pragma clang diagnostic pop

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/function.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.h>

#include <util/datetime/base.h>
#include <util/system/yassert.h>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <vector>

namespace cp = arrow::compute;

namespace NKikimr::NKernels {

template <template <typename... Args> class KernelGenerator, typename Op>
cp::ArrayKernelExec ArithmeticBinaryExec(cp::internal::detail::GetTypeId getId) {
    switch (getId.id) {
        case arrow::Type::INT8:
            return KernelGenerator<arrow::Int8Type, arrow::Int8Type, arrow::Int8Type, Op>::Exec;
        case arrow::Type::UINT8:
            return KernelGenerator<arrow::UInt8Type, arrow::UInt8Type, arrow::UInt8Type, Op>::Exec;
        case arrow::Type::INT16:
            return KernelGenerator<arrow::Int16Type, arrow::Int16Type, arrow::Int16Type, Op>::Exec;
        case arrow::Type::UINT16:
            return KernelGenerator<arrow::UInt16Type, arrow::UInt16Type, arrow::UInt16Type, Op>::Exec;
        case arrow::Type::INT32:
            return KernelGenerator<arrow::Int32Type, arrow::Int32Type, arrow::Int32Type, Op>::Exec;
        case arrow::Type::UINT32:
            return KernelGenerator<arrow::UInt32Type, arrow::UInt32Type, arrow::UInt32Type, Op>::Exec;
        case arrow::Type::INT64:
            return KernelGenerator<arrow::Int64Type, arrow::Int64Type, arrow::Int64Type, Op>::Exec;
        case arrow::Type::TIMESTAMP:
            return KernelGenerator<arrow::Int64Type, arrow::Int64Type, arrow::Int64Type, Op>::Exec;
        case arrow::Type::UINT64:
            return KernelGenerator<arrow::UInt64Type, arrow::UInt64Type, arrow::UInt64Type, Op>::Exec;
        case arrow::Type::FLOAT:
            return KernelGenerator<arrow::FloatType, arrow::FloatType, arrow::FloatType, Op>::Exec;
        case arrow::Type::DOUBLE:
            return KernelGenerator<arrow::DoubleType, arrow::DoubleType, arrow::DoubleType, Op>::Exec;
        default:
            Y_ABORT_UNLESS(false);
            return cp::internal::ExecFail;
    }
}

template <template <typename... Args> class KernelGenerator, typename Op>
cp::ArrayKernelExec ArithmeticBinaryIntExec(cp::internal::detail::GetTypeId getId) {
    switch (getId.id) {
        case arrow::Type::INT8:
            return KernelGenerator<arrow::Int8Type, arrow::Int8Type, arrow::Int8Type, Op>::Exec;
        case arrow::Type::UINT8:
            return KernelGenerator<arrow::UInt8Type, arrow::UInt8Type, arrow::UInt8Type, Op>::Exec;
        case arrow::Type::INT16:
            return KernelGenerator<arrow::Int16Type, arrow::Int16Type, arrow::Int16Type, Op>::Exec;
        case arrow::Type::UINT16:
            return KernelGenerator<arrow::UInt16Type, arrow::UInt16Type, arrow::UInt16Type, Op>::Exec;
        case arrow::Type::INT32:
            return KernelGenerator<arrow::Int32Type, arrow::Int32Type, arrow::Int32Type, Op>::Exec;
        case arrow::Type::UINT32:
            return KernelGenerator<arrow::UInt32Type, arrow::UInt32Type, arrow::UInt32Type, Op>::Exec;
        case arrow::Type::INT64:
            return KernelGenerator<arrow::Int64Type, arrow::Int64Type, arrow::Int64Type, Op>::Exec;
        case arrow::Type::TIMESTAMP:
            return KernelGenerator<arrow::Int64Type, arrow::Int64Type, arrow::Int64Type, Op>::Exec;
        case arrow::Type::UINT64:
            return KernelGenerator<arrow::UInt64Type, arrow::UInt64Type, arrow::UInt64Type, Op>::Exec;
        default:
            Y_ABORT_UNLESS(false);
            return cp::internal::ExecFail;
    }
}

template <template <typename... Args> class KernelGenerator, typename Op>
cp::ArrayKernelExec ArithmeticUnaryExec(cp::internal::detail::GetTypeId getId) {
    switch (getId.id) {
        case arrow::Type::INT8:
            return KernelGenerator<arrow::Int8Type, arrow::Int8Type, Op>::Exec;
        case arrow::Type::UINT8:
            return KernelGenerator<arrow::UInt8Type, arrow::UInt8Type, Op>::Exec;
        case arrow::Type::INT16:
            return KernelGenerator<arrow::Int16Type, arrow::Int16Type, Op>::Exec;
        case arrow::Type::UINT16:
            return KernelGenerator<arrow::UInt16Type, arrow::UInt16Type, Op>::Exec;
        case arrow::Type::INT32:
            return KernelGenerator<arrow::Int32Type, arrow::Int32Type, Op>::Exec;
        case arrow::Type::UINT32:
            return KernelGenerator<arrow::UInt32Type, arrow::UInt32Type, Op>::Exec;
        case arrow::Type::INT64:
            return KernelGenerator<arrow::Int64Type, arrow::Int64Type, Op>::Exec;
        case arrow::Type::TIMESTAMP:
            return KernelGenerator<arrow::Int64Type, arrow::Int64Type, Op>::Exec;
        case arrow::Type::UINT64:
            return KernelGenerator<arrow::UInt64Type, arrow::UInt64Type, Op>::Exec;
        case arrow::Type::FLOAT:
            return KernelGenerator<arrow::FloatType, arrow::FloatType, Op>::Exec;
        case arrow::Type::DOUBLE:
            return KernelGenerator<arrow::DoubleType, arrow::DoubleType, Op>::Exec;
        default:
            Y_ABORT_UNLESS(false);
            return cp::internal::ExecFail;
    }
}

template <template <typename... Args> class KernelGenerator, typename Op>
cp::ArrayKernelExec MathUnaryExec(cp::internal::detail::GetTypeId getId) {
    switch (getId.id) {
        case arrow::Type::INT8:
            return KernelGenerator<arrow::DoubleType, arrow::Int8Type, Op>::Exec;
        case arrow::Type::UINT8:
            return KernelGenerator<arrow::DoubleType, arrow::UInt8Type, Op>::Exec;
        case arrow::Type::INT16:
            return KernelGenerator<arrow::DoubleType, arrow::Int16Type, Op>::Exec;
        case arrow::Type::UINT16:
            return KernelGenerator<arrow::DoubleType, arrow::UInt16Type, Op>::Exec;
        case arrow::Type::INT32:
            return KernelGenerator<arrow::DoubleType, arrow::Int32Type, Op>::Exec;
        case arrow::Type::UINT32:
            return KernelGenerator<arrow::DoubleType, arrow::UInt32Type, Op>::Exec;
        case arrow::Type::INT64:
            return KernelGenerator<arrow::DoubleType, arrow::Int64Type, Op>::Exec;
        case arrow::Type::TIMESTAMP:
            return KernelGenerator<arrow::DoubleType, arrow::Int64Type, Op>::Exec;
        case arrow::Type::UINT64:
            return KernelGenerator<arrow::DoubleType, arrow::UInt64Type, Op>::Exec;
        case arrow::Type::FLOAT:
            return KernelGenerator<arrow::DoubleType, arrow::FloatType, Op>::Exec;
        case arrow::Type::DOUBLE:
            return KernelGenerator<arrow::DoubleType, arrow::DoubleType, Op>::Exec;
        default:
            Y_ABORT_UNLESS(false);
            return cp::internal::ExecFail;
    }
}

template <template <typename... Args> class KernelGenerator, typename Op>
cp::ArrayKernelExec MathBinaryExec(cp::internal::detail::GetTypeId getId) {
    switch (getId.id) {
        case arrow::Type::INT8:
            return KernelGenerator<arrow::DoubleType, arrow::Int8Type, arrow::Int8Type, Op>::Exec;
        case arrow::Type::UINT8:
            return KernelGenerator<arrow::DoubleType, arrow::UInt8Type, arrow::UInt8Type, Op>::Exec;
        case arrow::Type::INT16:
            return KernelGenerator<arrow::DoubleType, arrow::Int16Type, arrow::Int16Type, Op>::Exec;
        case arrow::Type::UINT16:
            return KernelGenerator<arrow::DoubleType, arrow::UInt16Type, arrow::UInt16Type, Op>::Exec;
        case arrow::Type::INT32:
            return KernelGenerator<arrow::DoubleType, arrow::Int32Type, arrow::Int32Type, Op>::Exec;
        case arrow::Type::UINT32:
            return KernelGenerator<arrow::DoubleType, arrow::UInt32Type, arrow::UInt32Type, Op>::Exec;
        case arrow::Type::INT64:
            return KernelGenerator<arrow::DoubleType, arrow::Int64Type, arrow::Int64Type, Op>::Exec;
        case arrow::Type::TIMESTAMP:
            return KernelGenerator<arrow::DoubleType, arrow::Int64Type, arrow::Int64Type, Op>::Exec;
        case arrow::Type::UINT64:
            return KernelGenerator<arrow::DoubleType, arrow::UInt64Type, arrow::UInt64Type, Op>::Exec;
        case arrow::Type::FLOAT:
            return KernelGenerator<arrow::DoubleType, arrow::FloatType, arrow::FloatType, Op>::Exec;
        case arrow::Type::DOUBLE:
            return KernelGenerator<arrow::DoubleType, arrow::DoubleType, arrow::DoubleType, Op>::Exec;
        default:
            Y_ABORT_UNLESS(false);
            return cp::internal::ExecFail;
    }
}


template <typename TOperator, typename TRes>
static arrow::Status SimpleNullaryExec(cp::KernelContext* ctx, const cp::ExecBatch&, arrow::Datum* out) {
    *out = arrow::MakeScalar(TOperator:: template Call<typename cp::internal::GetViewType<TRes>::T>(ctx));
    return arrow::Status::OK();
}

}
