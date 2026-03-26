#pragma once
#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/scheme/scheme_type_id.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>
#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/api.h>
#include <util/system/yassert.h>

extern "C" {
#include <yql/essentials/parser/pg_wrapper/postgresql/src/include/catalog/pg_type_d.h>
}

namespace NKikimr::NArrow {

/**
 * @brief Function to switch yql type correctly and uniformly converting it to arrow type using callback
 *
 * @tparam TFunc Callback type
 * @param typeId Type of data callback work with.
 * @param callback Template function of signature (TTypeWrapper) -> bool
 * @return Result of execution of callback or false if the type typeId is not supported.
 */
template <typename TFunc>
[[nodiscard]] bool SwitchYqlTypeToArrowType(const NScheme::TTypeInfo& typeInfo, TFunc&& callback) {
    switch (typeInfo.GetTypeId()) {
        case NScheme::NTypeIds::Bool:
            return callback(TTypeWrapper<arrow20::UInt8Type>());
        case NScheme::NTypeIds::Int8:
            return callback(TTypeWrapper<arrow20::Int8Type>());
        case NScheme::NTypeIds::Uint8:
            return callback(TTypeWrapper<arrow20::UInt8Type>());
        case NScheme::NTypeIds::Int16:
            return callback(TTypeWrapper<arrow20::Int16Type>());
        case NScheme::NTypeIds::Date:
        case NScheme::NTypeIds::Uint16:
            return callback(TTypeWrapper<arrow20::UInt16Type>());
        case NScheme::NTypeIds::Int32:
        case NScheme::NTypeIds::Date32:
            return callback(TTypeWrapper<arrow20::Int32Type>());
        case NScheme::NTypeIds::Datetime:
        case NScheme::NTypeIds::Uint32:
            return callback(TTypeWrapper<arrow20::UInt32Type>());
        case NScheme::NTypeIds::Int64:
            return callback(TTypeWrapper<arrow20::Int64Type>());
        case NScheme::NTypeIds::Uint64:
            return callback(TTypeWrapper<arrow20::UInt64Type>());
        case NScheme::NTypeIds::Float:
            return callback(TTypeWrapper<arrow20::FloatType>());
        case NScheme::NTypeIds::Double:
            return callback(TTypeWrapper<arrow20::DoubleType>());
        case NScheme::NTypeIds::Utf8:
        case NScheme::NTypeIds::Json:
            return callback(TTypeWrapper<arrow20::StringType>());
        case NScheme::NTypeIds::String:
        case NScheme::NTypeIds::String4k:
        case NScheme::NTypeIds::String2m:
        case NScheme::NTypeIds::Yson:
        case NScheme::NTypeIds::DyNumber:
        case NScheme::NTypeIds::JsonDocument:
            return callback(TTypeWrapper<arrow20::BinaryType>());
        case NScheme::NTypeIds::Timestamp:
            return callback(TTypeWrapper<arrow20::TimestampType>());
        case NScheme::NTypeIds::Interval:
            return callback(TTypeWrapper<arrow20::DurationType>());
        case NScheme::NTypeIds::Decimal:
        case NScheme::NTypeIds::Uuid:
            return callback(TTypeWrapper<arrow20::FixedSizeBinaryType>());

        case NScheme::NTypeIds::Datetime64:
        case NScheme::NTypeIds::Timestamp64:
        case NScheme::NTypeIds::Interval64:
            return callback(TTypeWrapper<arrow20::Int64Type>());

        case NScheme::NTypeIds::PairUi64Ui64:
        case NScheme::NTypeIds::ActorId:
        case NScheme::NTypeIds::StepOrderId:
            break; // Deprecated types

        case NScheme::NTypeIds::Pg:
            switch (NPg::PgTypeIdFromTypeDesc(typeInfo.GetPgTypeDesc())) {
                case INT2OID:
                    return callback(TTypeWrapper<arrow20::Int16Type>());
                case INT4OID:
                    return callback(TTypeWrapper<arrow20::Int32Type>());
                case INT8OID:
                    return callback(TTypeWrapper<arrow20::Int64Type>());
                case FLOAT4OID:
                    return callback(TTypeWrapper<arrow20::FloatType>());
                case FLOAT8OID:
                    return callback(TTypeWrapper<arrow20::DoubleType>());
                case BYTEAOID:
                    return callback(TTypeWrapper<arrow20::BinaryType>());
                case TEXTOID:
                    return callback(TTypeWrapper<arrow20::StringType>());
                default:
                    return false;
            }
            break;
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
        case NScheme::NTypeIds::Date32:
        case NScheme::NTypeIds::Datetime64:
        case NScheme::NTypeIds::Timestamp64:
        case NScheme::NTypeIds::Interval64:
            return true;
        default:
            break;
    }
    return false;
}

}
