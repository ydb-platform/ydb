#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <util/system/yassert.h>
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/scheme/scheme_type_id.h>

namespace NKikimr::NArrow {

template <typename TType>
struct TTypeWrapper
{
    using T = TType;
};

template <class TResult, TResult defaultValue, typename TFunc, bool EnableNull = false>
TResult SwitchTypeImpl(arrow::Type::type typeId, TFunc&& f) {
    switch (typeId) {
        case arrow::Type::NA: {
            if constexpr (EnableNull) {
                return f(TTypeWrapper<arrow::NullType>());
            }
            break;
        }
        case arrow::Type::BOOL:
            return f(TTypeWrapper<arrow::BooleanType>());
        case arrow::Type::UINT8:
            return f(TTypeWrapper<arrow::UInt8Type>());
        case arrow::Type::INT8:
            return f(TTypeWrapper<arrow::Int8Type>());
        case arrow::Type::UINT16:
            return f(TTypeWrapper<arrow::UInt16Type>());
        case arrow::Type::INT16:
            return f(TTypeWrapper<arrow::Int16Type>());
        case arrow::Type::UINT32:
            return f(TTypeWrapper<arrow::UInt32Type>());
        case arrow::Type::INT32:
            return f(TTypeWrapper<arrow::Int32Type>());
        case arrow::Type::UINT64:
            return f(TTypeWrapper<arrow::UInt64Type>());
        case arrow::Type::INT64:
            return f(TTypeWrapper<arrow::Int64Type>());
        case arrow::Type::HALF_FLOAT:
            return f(TTypeWrapper<arrow::HalfFloatType>());
        case arrow::Type::FLOAT:
            return f(TTypeWrapper<arrow::FloatType>());
        case arrow::Type::DOUBLE:
            return f(TTypeWrapper<arrow::DoubleType>());
        case arrow::Type::STRING:
            return f(TTypeWrapper<arrow::StringType>());
        case arrow::Type::BINARY:
            return f(TTypeWrapper<arrow::BinaryType>());
        case arrow::Type::FIXED_SIZE_BINARY:
            return f(TTypeWrapper<arrow::FixedSizeBinaryType>());
        case arrow::Type::DATE32:
            return f(TTypeWrapper<arrow::Date32Type>());
        case arrow::Type::DATE64:
            return f(TTypeWrapper<arrow::Date64Type>());
        case arrow::Type::TIMESTAMP:
            return f(TTypeWrapper<arrow::TimestampType>());
        case arrow::Type::TIME32:
            return f(TTypeWrapper<arrow::Time32Type>());
        case arrow::Type::TIME64:
            return f(TTypeWrapper<arrow::Time64Type>());
        case arrow::Type::INTERVAL_MONTHS:
            return f(TTypeWrapper<arrow::MonthIntervalType>());
        case arrow::Type::DECIMAL:
            return f(TTypeWrapper<arrow::Decimal128Type>());
        case arrow::Type::DURATION:
            return f(TTypeWrapper<arrow::DurationType>());
        case arrow::Type::LARGE_STRING:
            return f(TTypeWrapper<arrow::LargeStringType>());
        case arrow::Type::LARGE_BINARY:
            return f(TTypeWrapper<arrow::LargeBinaryType>());
        case arrow::Type::DECIMAL256:
        case arrow::Type::DENSE_UNION:
        case arrow::Type::DICTIONARY:
        case arrow::Type::EXTENSION:
        case arrow::Type::FIXED_SIZE_LIST:
        case arrow::Type::INTERVAL_DAY_TIME:
        case arrow::Type::LARGE_LIST:
        case arrow::Type::LIST:
        case arrow::Type::MAP:
        case arrow::Type::MAX_ID:
        case arrow::Type::SPARSE_UNION:
        case arrow::Type::STRUCT:
            break;
    }

    return defaultValue;
}

template <typename TFunc, bool EnableNull = false>
bool SwitchType(arrow::Type::type typeId, TFunc&& f) {
    return SwitchTypeImpl<bool, false, TFunc, EnableNull>(typeId, std::move(f));
}

template <typename TFunc>
bool SwitchTypeWithNull(arrow::Type::type typeId, TFunc&& f) {
    return SwitchType<TFunc, true>(typeId, std::move(f));
}

template <typename TFunc>
bool SwitchArrayType(const arrow::Datum& column, TFunc&& f) {
    auto type = column.type();
    Y_VERIFY(type);
    return SwitchType(type->id(), std::forward<TFunc>(f));
}

/**
 * @brief Function to switch yql type correctly and uniformly converting it to arrow type using callback
 *
 * @tparam TFunc Callback type
 * @param typeId Type of data callback work with.
 * @param callback Template function of signature (TTypeWrapper) -> bool
 * @return Result of execution of callback or false if the type typeId is not supported.
 */
template <typename TFunc>
bool SwitchYqlTypeToArrowType(const NScheme::TTypeInfo& typeInfo, TFunc&& callback) {
    switch (typeInfo.GetTypeId()) {
        case NScheme::NTypeIds::Bool:
            return callback(TTypeWrapper<arrow::BooleanType>());
        case NScheme::NTypeIds::Int8:
            return callback(TTypeWrapper<arrow::Int8Type>());
        case NScheme::NTypeIds::Uint8:
            return callback(TTypeWrapper<arrow::UInt8Type>());
        case NScheme::NTypeIds::Int16:
            return callback(TTypeWrapper<arrow::Int16Type>());
        case NScheme::NTypeIds::Date:
        case NScheme::NTypeIds::Uint16:
            return callback(TTypeWrapper<arrow::UInt16Type>());
        case NScheme::NTypeIds::Int32:
            return callback(TTypeWrapper<arrow::Int32Type>());
        case NScheme::NTypeIds::Datetime:
        case NScheme::NTypeIds::Uint32:
            return callback(TTypeWrapper<arrow::UInt32Type>());
        case NScheme::NTypeIds::Int64:
            return callback(TTypeWrapper<arrow::Int64Type>());
        case NScheme::NTypeIds::Uint64:
            return callback(TTypeWrapper<arrow::UInt64Type>());
        case NScheme::NTypeIds::Float:
            return callback(TTypeWrapper<arrow::FloatType>());
        case NScheme::NTypeIds::Double:
            return callback(TTypeWrapper<arrow::DoubleType>());
        case NScheme::NTypeIds::Utf8:
            return callback(TTypeWrapper<arrow::StringType>());
        case NScheme::NTypeIds::String:
        case NScheme::NTypeIds::String4k:
        case NScheme::NTypeIds::String2m:
        case NScheme::NTypeIds::Yson:
        case NScheme::NTypeIds::Json:
        case NScheme::NTypeIds::DyNumber:
        case NScheme::NTypeIds::JsonDocument:
            return callback(TTypeWrapper<arrow::BinaryType>());
        case NScheme::NTypeIds::Timestamp:
            return callback(TTypeWrapper<arrow::TimestampType>());
        case NScheme::NTypeIds::Interval:
            return callback(TTypeWrapper<arrow::DurationType>());
        case NScheme::NTypeIds::Decimal:
            return callback(TTypeWrapper<arrow::Decimal128Type>());

        case NScheme::NTypeIds::PairUi64Ui64:
        case NScheme::NTypeIds::ActorId:
        case NScheme::NTypeIds::StepOrderId:
            break; // Deprecated types

        case NScheme::NTypeIds::Pg:
            break; // TODO: support pg types
    }
    return false;
}

inline bool IsPrimitiveYqlType(const NScheme::TTypeInfo& typeInfo) {
    switch (typeInfo.GetTypeId()) {
        case NScheme::NTypeIds::Int8:
        case NScheme::NTypeIds::Uint8:
        case NScheme::NTypeIds::Int16:
        case NScheme::NTypeIds::Date:
        case NScheme::NTypeIds::Uint16:
        case NScheme::NTypeIds::Int32:
        case NScheme::NTypeIds::Datetime:
        case NScheme::NTypeIds::Uint32:
        case NScheme::NTypeIds::Int64:
        case NScheme::NTypeIds::Uint64:
        case NScheme::NTypeIds::Float:
        case NScheme::NTypeIds::Double:
        case NScheme::NTypeIds::Timestamp:
        case NScheme::NTypeIds::Interval:
            return true;
        default:
            break;
    }
    return false;
}

template <typename T>
bool Append(arrow::ArrayBuilder& builder, const typename T::c_type& value) {
    using TBuilder = typename arrow::TypeTraits<T>::BuilderType;

    auto status = static_cast<TBuilder&>(builder).Append(value);
    return status.ok();
}

template <typename T>
bool Append(arrow::ArrayBuilder& builder, arrow::util::string_view value) {
    using TBuilder = typename arrow::TypeTraits<T>::BuilderType;

    auto status = static_cast<TBuilder&>(builder).Append(value);
    return status.ok();
}

template <typename T>
bool Append(arrow::ArrayBuilder& builder, const typename T::c_type* values, size_t size) {
    using TBuilder = typename arrow::NumericBuilder<T>;

    auto status = static_cast<TBuilder&>(builder).AppendValues(values, size);
    return status.ok();
}

template <typename T>
bool Append(T& builder, const arrow::Array& array, int position) {
    return SwitchType(array.type_id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using TArray = typename arrow::TypeTraits<typename TWrap::T>::ArrayType;
        using TBuilder = typename arrow::TypeTraits<typename TWrap::T>::BuilderType;

        auto& typedArray = static_cast<const TArray&>(array);
        auto& typedBuilder = static_cast<TBuilder&>(builder);

        if (typedArray.IsNull(position)) {
            auto status = typedBuilder.AppendNull();
            return status.ok();
        } else {
            auto status = typedBuilder.Append(typedArray.GetView(position));
            return status.ok();
        }
    });
}

}
