#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKikimr::NKqp::NLogToDB {

template <typename T> struct TArrowTypeMapper {};

// Support type Bool
template <> struct TArrowTypeMapper<bool> {
    using TArrowBuilderType = arrow::BooleanBuilder;
    using TArrowArrayType = arrow::BooleanArray;
    static constexpr const char* TypeName = "Bool";

    static std::shared_ptr<arrow::DataType> GetArrowDataType() {
        return arrow::boolean();
    }

    static std::shared_ptr<TArrowBuilderType> CreateBuilder() {
        return std::make_shared<TArrowBuilderType>();
    }

    static bool AppendValue(arrow::BooleanBuilder& builder, bool value) {
        return builder.Append(value).ok();
    }

    static bool AppendNull(arrow::BooleanBuilder& builder) {
        return builder.AppendNull().ok();
    }
};

// Support type Int8
template <> struct TArrowTypeMapper<i8> {
    using TArrowBuilderType = arrow::Int8Builder;
    using TArrowArrayType = arrow::Int8Array;
    static constexpr const char* TypeName = "Int8";

    static std::shared_ptr<arrow::DataType> GetArrowDataType() {
        return arrow::int8();
    }

    static std::shared_ptr<TArrowBuilderType> CreateBuilder() {
        return std::make_shared<TArrowBuilderType>();
    }

    static bool AppendValue(arrow::Int8Builder& builder, i8 value) {
        return builder.Append(value).ok();
    }

    static bool AppendNull(arrow::Int8Builder& builder) {
        return builder.AppendNull().ok();
    }
};

// Support type Uint8
template <> struct TArrowTypeMapper<ui8> {
    using TArrowBuilderType = arrow::UInt8Builder;
    using TArrowArrayType = arrow::UInt8Array;
    static constexpr const char* TypeName = "Uint8";

    static std::shared_ptr<arrow::DataType> GetArrowDataType() {
        return arrow::uint8();
    }

    static std::shared_ptr<TArrowBuilderType> CreateBuilder() {
        return std::make_shared<TArrowBuilderType>();
    }

    static bool AppendValue(arrow::UInt8Builder& builder, ui8 value) {
        return builder.Append(value).ok();
    }

    static bool AppendNull(arrow::UInt8Builder& builder) {
        return builder.AppendNull().ok();
    }
};

// Support type Int16
template <> struct TArrowTypeMapper<i16> {
    using TArrowBuilderType = arrow::Int16Builder;
    using TArrowArrayType = arrow::Int16Array;
    static constexpr const char* TypeName = "Int16";

    static std::shared_ptr<arrow::DataType> GetArrowDataType() {
        return arrow::int16();
    }

    static std::shared_ptr<TArrowBuilderType> CreateBuilder() {
        return std::make_shared<TArrowBuilderType>();
    }

    static bool AppendValue(arrow::Int16Builder& builder, i16 value) {
        return builder.Append(value).ok();
    }

    static bool AppendNull(arrow::Int16Builder& builder) {
        return builder.AppendNull().ok();
    }
};

// Support type Uint16
template <> struct TArrowTypeMapper<ui16> {
    using TArrowBuilderType = arrow::UInt16Builder;
    using TArrowArrayType = arrow::UInt16Array;
    static constexpr const char* TypeName = "Uint16";

    static std::shared_ptr<arrow::DataType> GetArrowDataType() {
        return arrow::uint16();
    }

    static std::shared_ptr<TArrowBuilderType> CreateBuilder() {
        return std::make_shared<TArrowBuilderType>();
    }

    static bool AppendValue(arrow::UInt16Builder& builder, ui16 value) {
        return builder.Append(value).ok();
    }

    static bool AppendNull(arrow::UInt16Builder& builder) {
        return builder.AppendNull().ok();
    }
};

// Support type Int32
template <> struct TArrowTypeMapper<i32> {
    using TArrowBuilderType = arrow::Int32Builder;
    using TArrowArrayType = arrow::Int32Array;
    static constexpr const char* TypeName = "Int32";

    static std::shared_ptr<arrow::DataType> GetArrowDataType() {
        return arrow::int32();
    }

    static std::shared_ptr<TArrowBuilderType> CreateBuilder() {
        return std::make_shared<TArrowBuilderType>();
    }

    static bool AppendValue(arrow::Int32Builder& builder, i32 value) {
        return builder.Append(value).ok();
    }

    static bool AppendNull(arrow::Int32Builder& builder) {
        return builder.AppendNull().ok();
    }
};

// Support type Uint32
template <> struct TArrowTypeMapper<ui32> {
    using TArrowBuilderType = arrow::UInt32Builder;
    using TArrowArrayType = arrow::UInt32Array;
    static constexpr const char* TypeName = "Uint32";

    static std::shared_ptr<arrow::DataType> GetArrowDataType() {
        return arrow::uint32();
    }

    static std::shared_ptr<TArrowBuilderType> CreateBuilder() {
        return std::make_shared<TArrowBuilderType>();
    }

    static bool AppendValue(arrow::UInt32Builder& builder, ui32 value) {
        return builder.Append(value).ok();
    }

    static bool AppendNull(arrow::UInt32Builder& builder) {
        return builder.AppendNull().ok();
    }
};

// Support type Int64
template <> struct TArrowTypeMapper<i64> {
    using TArrowBuilderType = arrow::Int64Builder;
    using TArrowArrayType = arrow::Int64Array;
    static constexpr const char* TypeName = "Int64";

    static std::shared_ptr<arrow::DataType> GetArrowDataType() {
        return arrow::int64();
    }

    static std::shared_ptr<TArrowBuilderType> CreateBuilder() {
        return std::make_shared<TArrowBuilderType>();
    }

    static bool AppendValue(arrow::Int64Builder& builder, i64 value) {
        return builder.Append(value).ok();
    }

    static bool AppendNull(arrow::Int64Builder& builder) {
        return builder.AppendNull().ok();
    }
};

// Support type Uint64
template <> struct TArrowTypeMapper<ui64> {
    using TArrowBuilderType = arrow::UInt64Builder;
    using TArrowArrayType = arrow::UInt64Array;
    static constexpr const char* TypeName = "Uint64";

    static std::shared_ptr<arrow::DataType> GetArrowDataType() {
        return arrow::uint64();
    }

    static std::shared_ptr<TArrowBuilderType> CreateBuilder() {
        return std::make_shared<TArrowBuilderType>();
    }

    static bool AppendValue(arrow::UInt64Builder& builder, ui64 value) {
        return builder.Append(value).ok();
    }

    static bool AppendNull(arrow::UInt64Builder& builder) {
        return builder.AppendNull().ok();
    }
};

// Support type Float
template <> struct TArrowTypeMapper<float> {
    using TArrowBuilderType = arrow::FloatBuilder;
    using TArrowArrayType = arrow::FloatArray;
    static constexpr const char* TypeName = "Float";

    static std::shared_ptr<arrow::DataType> GetArrowDataType() {
        return arrow::float32();
    }

    static std::shared_ptr<TArrowBuilderType> CreateBuilder() {
        return std::make_shared<TArrowBuilderType>();
    }

    static bool AppendValue(arrow::FloatBuilder& builder, float value) {
        return builder.Append(value).ok();
    }

    static bool AppendNull(arrow::FloatBuilder& builder) {
        return builder.AppendNull().ok();
    }
};

// Support type Double
template <> struct TArrowTypeMapper<double> {
    using TArrowBuilderType = arrow::DoubleBuilder;
    using TArrowArrayType = arrow::DoubleArray;
    static constexpr const char* TypeName = "Double";

    static std::shared_ptr<arrow::DataType> GetArrowDataType() {
        return arrow::float64();
    }

    static std::shared_ptr<TArrowBuilderType> CreateBuilder() {
        return std::make_shared<TArrowBuilderType>();
    }

    static bool AppendValue(arrow::DoubleBuilder& builder, double value) {
        return builder.Append(value).ok();
    }

    static bool AppendNull(arrow::DoubleBuilder& builder) {
        return builder.AppendNull().ok();
    }
};

// Support type Utf8
template <> struct TArrowTypeMapper<TString> {
    using TArrowBuilderType = arrow::StringBuilder;
    using TArrowArrayType = arrow::StringArray;
    static constexpr const char* TypeName = "Utf8";

    static std::shared_ptr<arrow::DataType> GetArrowDataType() {
        return arrow::utf8();
    }

    static std::shared_ptr<TArrowBuilderType> CreateBuilder() {
        return std::make_shared<TArrowBuilderType>();
    }

    static bool AppendValue(arrow::StringBuilder& builder, const TString& value) {
        return builder.Append(value.data(), static_cast<int32_t>(value.size())).ok();
    }

    static bool AppendNull(arrow::StringBuilder& builder) {
        return builder.AppendNull().ok();
    }
};

// Support type Timestamp
template <> struct TArrowTypeMapper<TInstant> {
    using TArrowBuilderType = arrow::TimestampBuilder;
    using TArrowArrayType = arrow::TimestampArray;
    static constexpr const char* TypeName = "Timestamp";

    static std::shared_ptr<arrow::DataType> GetArrowDataType() {
        return arrow::timestamp(arrow::TimeUnit::MICRO);
    }

    static std::shared_ptr<TArrowBuilderType> CreateBuilder() {
        return std::make_shared<TArrowBuilderType>(GetArrowDataType(), arrow::default_memory_pool());
    }

    static bool AppendValue(arrow::TimestampBuilder& builder, TInstant value) {
        return builder.Append(static_cast<int64_t>(value.MicroSeconds())).ok();
    }

    static bool AppendNull(arrow::TimestampBuilder& builder) {
        return builder.AppendNull().ok();
    }
};

}
