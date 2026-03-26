#pragma once

#include "arrow_defs.h"

#include <arrow/array/data.h>
#include <arrow/buffer_builder.h>
#include <arrow/datum.h>
#include <arrow/scalar.h>
#include <arrow/util/bitmap.h>

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/arrow/mkql_bit_utils.h>
#include <yql/essentials/public/udf/arrow/util.h>

namespace NKikimr::NMiniKQL {

using NYql::NUdf::Chop;
using NYql::NUdf::DeepSlice;

/// \brief Remove optional from `data` as new ArrayData object
std::shared_ptr<arrow20::ArrayData> Unwrap(const arrow20::ArrayData& data, TType* itemType);
std::shared_ptr<arrow20::Scalar> UnwrapScalar(std::shared_ptr<arrow20::Scalar> scalar, TType* itemType);

using NYql::NUdf::AllocateBitmapWithReserve;
using NYql::NUdf::MakeDenseBitmap;
using NYql::NUdf::MakeDenseBitmapCopy;
using NYql::NUdf::MakeDenseBitmapCopyIfOffsetDiffers;
using NYql::NUdf::MakeDenseFalseBitmap;

inline arrow20::internal::Bitmap GetBitmap(const arrow20::ArrayData& arr, int index) {
    return arrow20::internal::Bitmap{arr.buffers[index], arr.offset, arr.length};
}

using NYql::NUdf::ForEachArrayData;
using NYql::NUdf::MakeArray;

template <typename T>
T GetPrimitiveScalarValue(const arrow20::Scalar& scalar) {
    return *static_cast<const T*>(dynamic_cast<const arrow20::internal::PrimitiveScalarBase&>(scalar).data());
}

inline const void* GetPrimitiveScalarValuePtr(const arrow20::Scalar& scalar) {
    return dynamic_cast<const arrow20::internal::PrimitiveScalarBase&>(scalar).data();
}

inline void* GetPrimitiveScalarValueMutablePtr(arrow20::Scalar& scalar) {
    return dynamic_cast<arrow20::internal::PrimitiveScalarBase&>(scalar).mutable_data();
}

inline std::string_view GetStringScalarValue(const arrow20::Scalar& scalar) {
    const auto& base = dynamic_cast<const arrow20::BaseBinaryScalar&>(scalar);
    return std::string_view{reinterpret_cast<const char*>(base.value->data()), static_cast<size_t>(base.value->size())};
}

inline arrow20::Datum MakeUint8Array(arrow20::MemoryPool* pool, ui8 value, int64_t len) {
    std::shared_ptr<arrow20::Buffer> data = ARROW_RESULT(arrow20::AllocateBuffer(len, pool));
    std::memset(data->mutable_data(), value, len);
    return arrow20::ArrayData::Make(arrow20::uint8(), len, {std::shared_ptr<arrow20::Buffer>{}, data});
}

inline arrow20::Datum MakeFalseArray(arrow20::MemoryPool* pool, int64_t len) {
    return MakeUint8Array(pool, 0, len);
}

inline arrow20::Datum MakeTrueArray(arrow20::MemoryPool* pool, int64_t len) {
    return MakeUint8Array(pool, 1, len);
}

inline arrow20::Datum MakeBitmapArray(arrow20::MemoryPool* pool, int64_t len, int64_t offset, const ui8* bitmap) {
    std::shared_ptr<arrow20::Buffer> data = ARROW_RESULT(arrow20::AllocateBuffer(len, pool));
    DecompressToSparseBitmap(data->mutable_data(), bitmap, offset, len);
    return arrow20::ArrayData::Make(arrow20::uint8(), len, {std::shared_ptr<arrow20::Buffer>{}, data});
}

template <typename T>
struct TPrimitiveDataType;

template <>
struct TPrimitiveDataType<bool> {
    using TLayout = ui8;
    using TArithmetic = ui8;
    using TResult = arrow20::UInt8Type;
    using TScalarResult = arrow20::UInt8Scalar;
};

template <>
struct TPrimitiveDataType<i8> {
    using TLayout = i8;
    using TArithmetic = i8;
    using TResult = arrow20::Int8Type;
    using TScalarResult = arrow20::Int8Scalar;
};

template <>
struct TPrimitiveDataType<ui8> {
    using TLayout = ui8;
    using TArithmetic = ui8;
    using TResult = arrow20::UInt8Type;
    using TScalarResult = arrow20::UInt8Scalar;
};

template <>
struct TPrimitiveDataType<i16> {
    using TLayout = i16;
    using TArithmetic = i16;
    using TResult = arrow20::Int16Type;
    using TScalarResult = arrow20::Int16Scalar;
};

template <>
struct TPrimitiveDataType<ui16> {
    using TLayout = ui16;
    using TArithmetic = ui16;
    using TResult = arrow20::UInt16Type;
    using TScalarResult = arrow20::UInt16Scalar;
};

template <>
struct TPrimitiveDataType<i32> {
    using TLayout = i32;
    using TArithmetic = i32;
    using TResult = arrow20::Int32Type;
    using TScalarResult = arrow20::Int32Scalar;
};

template <>
struct TPrimitiveDataType<ui32> {
    using TLayout = ui32;
    using TArithmetic = ui32;
    using TResult = arrow20::UInt32Type;
    using TScalarResult = arrow20::UInt32Scalar;
};

template <>
struct TPrimitiveDataType<i64> {
    using TLayout = i64;
    using TArithmetic = i64;
    using TResult = arrow20::Int64Type;
    using TScalarResult = arrow20::Int64Scalar;
};

template <>
struct TPrimitiveDataType<ui64> {
    using TLayout = ui64;
    using TArithmetic = ui64;
    using TResult = arrow20::UInt64Type;
    using TScalarResult = arrow20::UInt64Scalar;
};

template <>
struct TPrimitiveDataType<float> {
    using TLayout = float;
    using TArithmetic = float;
    using TResult = arrow20::FloatType;
    using TScalarResult = arrow20::FloatScalar;
};

template <>
struct TPrimitiveDataType<double> {
    using TLayout = double;
    using TArithmetic = double;
    using TResult = arrow20::DoubleType;
    using TScalarResult = arrow20::DoubleScalar;
};

template <>
struct TPrimitiveDataType<char*> {
    using TResult = arrow20::BinaryType;
    using TScalarResult = arrow20::BinaryScalar;
};

template <>
struct TPrimitiveDataType<NYql::NUdf::TUtf8> {
    using TResult = arrow20::StringType;
    using TScalarResult = arrow20::StringScalar;
};

template <>
struct TPrimitiveDataType<NYql::NDecimal::TInt128> {
    using TLayout = NYql::NDecimal::TInt128;
    using TArithmetic = NYql::NDecimal::TDecimal;

    class TResult: public arrow20::FixedSizeBinaryType {
    public:
        TResult()
            : arrow20::FixedSizeBinaryType(16)
        {
        }
    };

    class TScalarResult: public arrow20::FixedSizeBinaryScalar {
    public:
        explicit TScalarResult(std::shared_ptr<arrow20::Buffer> value)
            : arrow20::FixedSizeBinaryScalar(std::move(value), arrow20::fixed_size_binary(16))
        {
        }

        TScalarResult()
            : arrow20::FixedSizeBinaryScalar(arrow20::fixed_size_binary(16))
        {
        }
    };
};

template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
inline arrow20::Datum MakeScalarDatum(T value) {
    return arrow20::Datum(std::make_shared<typename TPrimitiveDataType<T>::TScalarResult>(value));
}

template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
inline arrow20::Datum MakeDefaultScalarDatum() {
    return MakeScalarDatum<T>({});
}

template <typename T>
inline std::shared_ptr<arrow20::DataType> GetPrimitiveDataType() {
    static std::shared_ptr<arrow20::DataType> Result = std::make_shared<typename TPrimitiveDataType<T>::TResult>();
    return Result;
}

using NYql::NUdf::TTypedBufferBuilder;

std::shared_ptr<arrow20::Buffer> MakeEmptyBuffer();

void UntrackDatum(const arrow20::Datum& datum);

} // namespace NKikimr::NMiniKQL

namespace arrow20 {

template <>
struct TypeTraits<typename NKikimr::NMiniKQL::TPrimitiveDataType<NYql::NDecimal::TInt128>::TResult> {
    static inline std::shared_ptr<DataType> type_singleton() { // NOLINT(readability-identifier-naming)
        return arrow20::fixed_size_binary(16);
    }
};

} // namespace arrow20
