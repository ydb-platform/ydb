#pragma once

#include "arrow_defs.h"

#include <arrow/array/data.h>
#include <arrow/buffer_builder.h>
#include <arrow/datum.h>
#include <arrow/scalar.h>
#include <arrow/util/bitmap.h>

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/arrow/mkql_bit_utils.h>
#include <ydb/library/yql/public/udf/arrow/util.h>

namespace NKikimr::NMiniKQL {

using NYql::NUdf::DeepSlice;
using NYql::NUdf::Chop;

/// \brief Remove optional from `data` as new ArrayData object
std::shared_ptr<arrow::ArrayData> Unwrap(const arrow::ArrayData& data, TType* itemType);

using NYql::NUdf::AllocateBitmapWithReserve;
using NYql::NUdf::MakeDenseBitmap;

inline arrow::internal::Bitmap GetBitmap(const arrow::ArrayData& arr, int index) {
    return arrow::internal::Bitmap{ arr.buffers[index], arr.offset, arr.length };
}

using NYql::NUdf::ForEachArrayData;
using NYql::NUdf::MakeArray;

template <typename T>
T GetPrimitiveScalarValue(const arrow::Scalar& scalar) {
    return *static_cast<const T*>(dynamic_cast<const arrow::internal::PrimitiveScalarBase&>(scalar).data());
}

inline const void* GetPrimitiveScalarValuePtr(const arrow::Scalar& scalar) {
    return dynamic_cast<const arrow::internal::PrimitiveScalarBase&>(scalar).data();
}

inline void* GetPrimitiveScalarValueMutablePtr(arrow::Scalar& scalar) {
    return dynamic_cast<arrow::internal::PrimitiveScalarBase&>(scalar).mutable_data();
}

inline std::string_view GetStringScalarValue(const arrow::Scalar& scalar) {
    const auto& base = dynamic_cast<const arrow::BaseBinaryScalar&>(scalar);
    return std::string_view{reinterpret_cast<const char*>(base.value->data()), static_cast<size_t>(base.value->size())};
}

inline arrow::Datum MakeUint8Array(arrow::MemoryPool* pool, ui8 value, int64_t len) {
    std::shared_ptr<arrow::Buffer> data = ARROW_RESULT(arrow::AllocateBuffer(len, pool));
    std::memset(data->mutable_data(), value, len);
    return arrow::ArrayData::Make(arrow::uint8(), len, { std::shared_ptr<arrow::Buffer>{}, data });
}

inline arrow::Datum MakeFalseArray(arrow::MemoryPool* pool, int64_t len) {
    return MakeUint8Array(pool, 0, len);
}

inline arrow::Datum MakeTrueArray(arrow::MemoryPool* pool, int64_t len) {
    return MakeUint8Array(pool, 1, len);
}

inline arrow::Datum MakeBitmapArray(arrow::MemoryPool* pool, int64_t len, int64_t offset, const ui8* bitmap) {
    std::shared_ptr<arrow::Buffer> data = ARROW_RESULT(arrow::AllocateBuffer(len, pool));
    DecompressToSparseBitmap(data->mutable_data(), bitmap, offset, len);
    return arrow::ArrayData::Make(arrow::uint8(), len, { std::shared_ptr<arrow::Buffer>{}, data });
}

template<typename T>
struct TPrimitiveDataType;

template<>
struct TPrimitiveDataType<bool> {
    using TLayout = ui8;
    using TResult = arrow::UInt8Type;
    using TScalarResult = arrow::UInt8Scalar;
};

template<>
struct TPrimitiveDataType<i8> {
    using TLayout = i8;
    using TResult = arrow::Int8Type;
    using TScalarResult = arrow::Int8Scalar;
};

template<>
struct TPrimitiveDataType<ui8> {
    using TLayout = ui8;
    using TResult = arrow::UInt8Type;
    using TScalarResult = arrow::UInt8Scalar;
};

template<>
struct TPrimitiveDataType<i16> {
    using TLayout = i16;
    using TResult = arrow::Int16Type;
    using TScalarResult = arrow::Int16Scalar;
};

template<>
struct TPrimitiveDataType<ui16> {
    using TLayout = ui16;
    using TResult = arrow::UInt16Type;
    using TScalarResult = arrow::UInt16Scalar;
};

template<>
struct TPrimitiveDataType<i32> {
    using TLayout = i32;
    using TResult = arrow::Int32Type;
    using TScalarResult = arrow::Int32Scalar;
};

template<>
struct TPrimitiveDataType<ui32> {
    using TLayout = ui32;
    using TResult = arrow::UInt32Type;
    using TScalarResult = arrow::UInt32Scalar;
};

template<>
struct TPrimitiveDataType<i64> {
    using TLayout = i64;
    using TResult = arrow::Int64Type;
    using TScalarResult = arrow::Int64Scalar;
};

template<>
struct TPrimitiveDataType<ui64> {
    using TLayout = ui64;
    using TResult = arrow::UInt64Type;
    using TScalarResult = arrow::UInt64Scalar;
};

template<>
struct TPrimitiveDataType<float> {
    using TLayout = float;
    using TResult = arrow::FloatType;
    using TScalarResult = arrow::FloatScalar;
};

template<>
struct TPrimitiveDataType<double> {
    using TLayout = double;
    using TResult = arrow::DoubleType;
    using TScalarResult = arrow::DoubleScalar;
};

template<>
struct TPrimitiveDataType<char*> {
    using TResult = arrow::BinaryType;
    using TScalarResult = arrow::BinaryScalar;
};

template<>
struct TPrimitiveDataType<NYql::NUdf::TUtf8> {
    using TResult = arrow::StringType;
    using TScalarResult = arrow::StringScalar;
};

template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
inline arrow::Datum MakeScalarDatum(T value) {
    return arrow::Datum(std::make_shared<typename TPrimitiveDataType<T>::TScalarResult>(value));
}

template <typename T, typename = typename std::enable_if<std::is_arithmetic<T>::value>::type>
inline arrow::Datum MakeDefaultScalarDatum() {
    return MakeScalarDatum<T>({});
}

template <typename T>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType() {
    static std::shared_ptr<arrow::DataType> result = std::make_shared<typename TPrimitiveDataType<T>::TResult>();
    return result;
}

using NYql::NUdf::TTypedBufferBuilder;

}
