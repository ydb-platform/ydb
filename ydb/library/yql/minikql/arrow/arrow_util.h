#pragma once

#include "arrow_defs.h"

#include <arrow/array/data.h>
#include <arrow/buffer_builder.h>
#include <arrow/datum.h>
#include <arrow/scalar.h>
#include <arrow/util/bitmap.h>

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/public/udf/arrow/util.h>

namespace NKikimr::NMiniKQL {

using NYql::NUdf::DeepSlice;
using NYql::NUdf::Chop;

/// \brief Remove optional from `data` as new ArrayData object
std::shared_ptr<arrow::ArrayData> Unwrap(const arrow::ArrayData& data, TType* itemType);

std::shared_ptr<arrow::Buffer> AllocateBitmapWithReserve(size_t bitCount, arrow::MemoryPool* pool);
std::shared_ptr<arrow::Buffer> MakeDenseBitmap(const ui8* srcSparse, size_t len, arrow::MemoryPool* pool);

inline arrow::internal::Bitmap GetBitmap(const arrow::ArrayData& arr, int index) {
    return arrow::internal::Bitmap{ arr.buffers[index], arr.offset, arr.length };
}

using NYql::NUdf::ForEachArrayData;
using NYql::NUdf::MakeArray;

template <typename T>
T GetPrimitiveScalarValue(const arrow::Scalar& scalar) {
    return *static_cast<const T*>(dynamic_cast<const arrow::internal::PrimitiveScalarBase&>(scalar).data());
}

inline std::string_view GetStringScalarValue(const arrow::Scalar& scalar) {
    const auto& base = dynamic_cast<const arrow::BaseBinaryScalar&>(scalar);
    return std::string_view{reinterpret_cast<const char*>(base.value->data()), static_cast<size_t>(base.value->size())};
}

template <typename T>
std::shared_ptr<arrow::DataType> GetPrimitiveDataType();

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<bool>() {
    return arrow::uint8();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<i8>() {
    return arrow::int8();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<ui8>() {
    return arrow::uint8();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<i16>() {
    return arrow::int16();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<ui16>() {
    return arrow::uint16();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<i32>() {
    return arrow::int32();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<ui32>() {
    return arrow::uint32();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<i64>() {
    return arrow::int64();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<ui64>() {
    return arrow::uint64();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<float>() {
    return arrow::float32();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<double>() {
    return arrow::float64();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<char*>() {
    return arrow::binary();
}

template <>
inline std::shared_ptr<arrow::DataType> GetPrimitiveDataType<NYql::NUdf::TUtf8>() {
    return arrow::utf8();
}

template<typename T>
struct TPrimitiveDataType;

template<>
struct TPrimitiveDataType<bool> {
    using TResult = arrow::UInt8Type;
};

template<>
struct TPrimitiveDataType<i8> {
    using TResult = arrow::Int8Type;
};

template<>
struct TPrimitiveDataType<ui8> {
    using TResult = arrow::UInt8Type;
};

template<>
struct TPrimitiveDataType<i16> {
    using TResult = arrow::Int16Type;
};

template<>
struct TPrimitiveDataType<ui16> {
    using TResult = arrow::UInt16Type;
};

template<>
struct TPrimitiveDataType<i32> {
    using TResult = arrow::Int32Type;
};

template<>
struct TPrimitiveDataType<ui32> {
    using TResult = arrow::UInt32Type;
};

template<>
struct TPrimitiveDataType<i64> {
    using TResult = arrow::Int64Type;
};

template<>
struct TPrimitiveDataType<ui64> {
    using TResult = arrow::UInt64Type;
};

template<>
struct TPrimitiveDataType<float> {
    using TResult = arrow::FloatType;
};

template<>
struct TPrimitiveDataType<double> {
    using TResult = arrow::DoubleType;
};

template<>
struct TPrimitiveDataType<char*> {
    using TResult = arrow::BinaryType;
};

template<>
struct TPrimitiveDataType<NYql::NUdf::TUtf8> {
    using TResult = arrow::StringType;
};

template <typename T>
arrow::Datum MakeScalarDatum(T value);

template <>
inline arrow::Datum MakeScalarDatum<bool>(bool value) {
    return arrow::Datum(std::make_shared<arrow::UInt8Scalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<i8>(i8 value) {
    return arrow::Datum(std::make_shared<arrow::Int8Scalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<ui8>(ui8 value) {
    return arrow::Datum(std::make_shared<arrow::UInt8Scalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<i16>(i16 value) {
    return arrow::Datum(std::make_shared<arrow::Int16Scalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<ui16>(ui16 value) {
    return arrow::Datum(std::make_shared<arrow::UInt16Scalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<i32>(i32 value) {
    return arrow::Datum(std::make_shared<arrow::Int32Scalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<ui32>(ui32 value) {
    return arrow::Datum(std::make_shared<arrow::UInt32Scalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<i64>(i64 value) {
    return arrow::Datum(std::make_shared<arrow::Int64Scalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<ui64>(ui64 value) {
    return arrow::Datum(std::make_shared<arrow::UInt64Scalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<float>(float value) {
    return arrow::Datum(std::make_shared<arrow::FloatScalar>(value));
}

template <>
inline arrow::Datum MakeScalarDatum<double>(double value) {
    return arrow::Datum(std::make_shared<arrow::DoubleScalar>(value));
}

// similar to arrow::TypedBufferBuilder, but with UnsafeAdvance() method
// and shrinkToFit = false
template<typename T>
class TTypedBufferBuilder {
    static_assert(std::is_pod_v<T>);
    static_assert(!std::is_same_v<T, bool>);
public:
    explicit TTypedBufferBuilder(arrow::MemoryPool* pool)
        : Builder(pool)
    {
    }

    inline void Reserve(size_t size) {
        ARROW_OK(Builder.Reserve(size * sizeof(T)));
    }

    inline size_t Length() const {
        return Builder.length() / sizeof(T);
    }

    inline T* MutableData() {
        return reinterpret_cast<T*>(Builder.mutable_data());
    }

    inline T* End() {
        return MutableData() + Length();
    }

    inline const T* Data() const {
        return reinterpret_cast<const T*>(Builder.data());
    }

    inline void UnsafeAppend(const T* values, size_t count) {
        std::memcpy(End(), values, count * sizeof(T));
        UnsafeAdvance(count);
    }

    inline void UnsafeAppend(size_t count, const T& value) {
        T* target = End();
        std::fill(target, target + count, value);
        UnsafeAdvance(count);
    }

    inline void UnsafeAppend(T&& value) {
        *End() = std::move(value);
        UnsafeAdvance(1);
    }

    inline void UnsafeAdvance(size_t count) {
        Builder.UnsafeAdvance(count * sizeof(T));
    }

    inline std::shared_ptr<arrow::Buffer> Finish() {
        bool shrinkToFit = false;
        return ARROW_RESULT(Builder.Finish(shrinkToFit));
    }
private:
    arrow::BufferBuilder Builder;
};

}
