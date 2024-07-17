#include "scalar.h"
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/library/actors/core/log.h>
#include <util/string/cast.h>
#include <util/string/builder.h>

namespace NKikimr::NOlap {

NKikimrColumnShardColumnDefaults::TColumnDefault TColumnDefaultScalarValue::SerializeToProto() const {
    NKikimrColumnShardColumnDefaults::TColumnDefault result;
    if (!Value) {
        return result;
    }
    auto& resultScalar = *result.MutableScalar();
    const auto& scalar = *Value;
    switch (Value->type->id()) {
        case arrow::Type::BOOL:
            resultScalar.SetBool(static_cast<const arrow::BooleanScalar&>(scalar).value);
            break;
        case arrow::Type::UINT8:
            resultScalar.SetUint8(static_cast<const arrow::UInt8Scalar&>(scalar).value);
            break;
        case arrow::Type::UINT16:
            resultScalar.SetUint16(static_cast<const arrow::UInt16Scalar&>(scalar).value);
            break;
        case arrow::Type::UINT32:
            resultScalar.SetUint32(static_cast<const arrow::UInt32Scalar&>(scalar).value);
            break;
        case arrow::Type::UINT64:
            resultScalar.SetUint64(static_cast<const arrow::UInt64Scalar&>(scalar).value);
            break;
        case arrow::Type::INT8:
            resultScalar.SetInt8(static_cast<const arrow::Int8Scalar&>(scalar).value);
            break;
        case arrow::Type::INT16:
            resultScalar.SetInt16(static_cast<const arrow::Int16Scalar&>(scalar).value);
            break;
        case arrow::Type::INT32:
            resultScalar.SetInt32(static_cast<const arrow::Int32Scalar&>(scalar).value);
            break;
        case arrow::Type::INT64:
            resultScalar.SetInt64(static_cast<const arrow::Int64Scalar&>(scalar).value);
            break;
        case arrow::Type::DOUBLE:
            resultScalar.SetDouble(static_cast<const arrow::DoubleScalar&>(scalar).value);
            break;
        case arrow::Type::FLOAT:
            resultScalar.SetFloat(static_cast<const arrow::FloatScalar&>(scalar).value);
            break;
        case arrow::Type::TIMESTAMP:
        {
            auto* ts = resultScalar.MutableTimestamp();
            ts->SetValue(static_cast<const arrow::TimestampScalar&>(scalar).value);
            ts->SetUnit(static_cast<const arrow::TimestampType&>(*scalar.type).unit());
            break;
        }
        case arrow::Type::STRING: {
            const auto& buffer = static_cast<const arrow::StringScalar&>(scalar).value;
            *resultScalar.MutableString() = buffer->ToString();
            break;
        }
        default:
            AFL_VERIFY(false)("problem", "incorrect type for statistics usage")("type", scalar.type->ToString());
    }
    return result;
}

NKikimr::TConclusionStatus TColumnDefaultScalarValue::DeserializeFromProto(const NKikimrColumnShardColumnDefaults::TColumnDefault& proto) {
    const auto& protoScalar = proto.GetScalar();
    Value = nullptr;
    if (protoScalar.HasBool()) {
        Value = std::make_shared<arrow::BooleanScalar>(protoScalar.GetBool());
    } else if (protoScalar.HasUint8()) {
        Value = std::make_shared<arrow::UInt8Scalar>(protoScalar.GetUint8());
    } else if (protoScalar.HasUint16()) {
        Value = std::make_shared<arrow::UInt16Scalar>(protoScalar.GetUint16());
    } else if (protoScalar.HasUint32()) {
        Value = std::make_shared<arrow::UInt32Scalar>(protoScalar.GetUint32());
    } else if (protoScalar.HasUint64()) {
        Value = std::make_shared<arrow::UInt64Scalar>(protoScalar.GetUint64());
    } else if (protoScalar.HasInt8()) {
        Value = std::make_shared<arrow::Int8Scalar>(protoScalar.GetInt8());
    } else if (protoScalar.HasInt16()) {
        Value = std::make_shared<arrow::Int16Scalar>(protoScalar.GetInt16());
    } else if (protoScalar.HasInt32()) {
        Value = std::make_shared<arrow::Int32Scalar>(protoScalar.GetInt32());
    } else if (protoScalar.HasInt64()) {
        Value = std::make_shared<arrow::Int64Scalar>(protoScalar.GetInt64());
    } else if (protoScalar.HasDouble()) {
        Value = std::make_shared<arrow::DoubleScalar>(protoScalar.GetDouble());
    } else if (protoScalar.HasFloat()) {
        Value = std::make_shared<arrow::FloatScalar>(protoScalar.GetFloat());
    } else if (protoScalar.HasTimestamp()) {
        arrow::TimeUnit::type unit = arrow::TimeUnit::type(protoScalar.GetTimestamp().GetUnit());
        Value = std::make_shared<arrow::TimestampScalar>(protoScalar.GetTimestamp().GetValue(), std::make_shared<arrow::TimestampType>(unit));
    } else if (protoScalar.HasString()) {
        Value = std::make_shared<arrow::StringScalar>(protoScalar.GetString());
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TColumnDefaultScalarValue::ParseFromString(const TString& value, const NScheme::TTypeInfo& dataType) {
    const auto dType = NArrow::GetArrowType(dataType);
    if (!dType.ok()) {
        return NKikimr::TConclusionStatus::Fail(dType.status().ToString());
    }
    NKikimr::TConclusionStatus result = NKikimr::TConclusionStatus::Success();
    std::shared_ptr<arrow::Scalar> val;
    NArrow::SwitchType((*dType)->id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        if constexpr (arrow::is_primitive_ctype<typename TWrap::T>()) {
            using CType = typename TWrap::T::c_type;
            using ScalarType = typename arrow::TypeTraits<typename TWrap::T>::ScalarType;
            const TMaybe<CType> castResult = TryFromString<CType>(value);
            if (!castResult) {
                result = NKikimr::TConclusionStatus::Fail("cannot parse from string: '" + value + "' to " + (*dType)->ToString());
            } else {
                val = std::make_shared<ScalarType>(*castResult);
            }
            return true;
        }
        if constexpr (arrow::has_string_view<typename TWrap::T>()) {
            if constexpr (TWrap::T::is_utf8) {
                using ScalarType = typename arrow::TypeTraits<typename TWrap::T>::ScalarType;
                val = std::make_shared<ScalarType>(std::string(value.data(), value.size()));
                return true;
            }
        }
        result = NKikimr::TConclusionStatus::Fail("cannot made default value from '" + value + "' for " + (*dType)->ToString());
        return true;
    });
    if (result.IsSuccess()) {
        Value = val;
    }
    return result;
}

TString TColumnDefaultScalarValue::DebugString() const {
    if (!Value) {
        return "NO_DATA";
    }
    return TStringBuilder() << Value->ToString();
}

}