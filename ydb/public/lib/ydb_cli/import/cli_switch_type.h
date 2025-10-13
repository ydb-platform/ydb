/* 
    This file contains code copied from core/formats/switch_type.h in order to cut client dependecies
*/

#pragma once

namespace NYdb_cli::NArrow {

template <typename TType>
struct TTypeWrapper
{
    using T = TType;
};

template <typename TFunc, bool EnableNull = false>
bool SwitchType(arrow20::Type::type typeId, TFunc&& f) {
    switch (typeId) {
        case arrow20::Type::NA: {
            if constexpr (EnableNull) {
                return f(TTypeWrapper<arrow20::NullType>());
            }
            break;
        }
        case arrow20::Type::BOOL:
            return f(TTypeWrapper<arrow20::BooleanType>());
        case arrow20::Type::UINT8:
            return f(TTypeWrapper<arrow20::UInt8Type>());
        case arrow20::Type::INT8:
            return f(TTypeWrapper<arrow20::Int8Type>());
        case arrow20::Type::UINT16:
            return f(TTypeWrapper<arrow20::UInt16Type>());
        case arrow20::Type::INT16:
            return f(TTypeWrapper<arrow20::Int16Type>());
        case arrow20::Type::UINT32:
            return f(TTypeWrapper<arrow20::UInt32Type>());
        case arrow20::Type::INT32:
            return f(TTypeWrapper<arrow20::Int32Type>());
        case arrow20::Type::UINT64:
            return f(TTypeWrapper<arrow20::UInt64Type>());
        case arrow20::Type::INT64:
            return f(TTypeWrapper<arrow20::Int64Type>());
        case arrow20::Type::HALF_FLOAT:
            return f(TTypeWrapper<arrow20::HalfFloatType>());
        case arrow20::Type::FLOAT:
            return f(TTypeWrapper<arrow20::FloatType>());
        case arrow20::Type::DOUBLE:
            return f(TTypeWrapper<arrow20::DoubleType>());
        case arrow20::Type::STRING:
            return f(TTypeWrapper<arrow20::StringType>());
        case arrow20::Type::BINARY:
            return f(TTypeWrapper<arrow20::BinaryType>());
        case arrow20::Type::FIXED_SIZE_BINARY:
            return f(TTypeWrapper<arrow20::FixedSizeBinaryType>());
        case arrow20::Type::DATE32:
            return f(TTypeWrapper<arrow20::Date32Type>());
        case arrow20::Type::DATE64:
            return f(TTypeWrapper<arrow20::Date64Type>());
        case arrow20::Type::TIMESTAMP:
            return f(TTypeWrapper<arrow20::TimestampType>());
        case arrow20::Type::TIME32:
            return f(TTypeWrapper<arrow20::Time32Type>());
        case arrow20::Type::TIME64:
            return f(TTypeWrapper<arrow20::Time64Type>());
        case arrow20::Type::INTERVAL_MONTHS:
            return f(TTypeWrapper<arrow20::MonthIntervalType>());
        case arrow20::Type::DECIMAL:
            return f(TTypeWrapper<arrow20::Decimal128Type>());
        case arrow20::Type::DURATION:
            return f(TTypeWrapper<arrow20::DurationType>());
        case arrow20::Type::LARGE_STRING:
            return f(TTypeWrapper<arrow20::LargeStringType>());
        case arrow20::Type::LARGE_BINARY:
            return f(TTypeWrapper<arrow20::LargeBinaryType>());
        case arrow20::Type::DECIMAL256:
        case arrow20::Type::DENSE_UNION:
        case arrow20::Type::DICTIONARY:
        case arrow20::Type::EXTENSION:
        case arrow20::Type::FIXED_SIZE_LIST:
        case arrow20::Type::INTERVAL_DAY_TIME:
        case arrow20::Type::LARGE_LIST:
        case arrow20::Type::LIST:
        case arrow20::Type::MAP:
        case arrow20::Type::MAX_ID:
        case arrow20::Type::SPARSE_UNION:
        case arrow20::Type::STRUCT:
        default:
            break;
    }

    return false;
}

template <typename TFunc>
bool SwitchTypeWithNull(arrow20::Type::type typeId, TFunc&& f) {
    return SwitchType<TFunc, true>(typeId, std::move(f));
}
}