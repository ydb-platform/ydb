#include "scalar.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr::NOlap {

NKikimrColumnShardColumnDefaults::TColumnDefault TColumnDefaultScalarValue::SerializeToProto() const {
    NKikimrColumnShardColumnDefaults::TColumnDefault result;
    if (!Value) {
        return result;
    }
    auto& resultScalar = *result.MutableScalar();
    const auto& scalar = *Value;
    switch (Value->type->id()) {
        case arrow20::Type::BOOL:
            resultScalar.SetBool(static_cast<const arrow20::BooleanScalar&>(scalar).value);
            break;
        case arrow20::Type::UINT8:
            resultScalar.SetUint8(static_cast<const arrow20::UInt8Scalar&>(scalar).value);
            break;
        case arrow20::Type::UINT16:
            resultScalar.SetUint16(static_cast<const arrow20::UInt16Scalar&>(scalar).value);
            break;
        case arrow20::Type::UINT32:
            resultScalar.SetUint32(static_cast<const arrow20::UInt32Scalar&>(scalar).value);
            break;
        case arrow20::Type::UINT64:
            resultScalar.SetUint64(static_cast<const arrow20::UInt64Scalar&>(scalar).value);
            break;
        case arrow20::Type::INT8:
            resultScalar.SetInt8(static_cast<const arrow20::Int8Scalar&>(scalar).value);
            break;
        case arrow20::Type::INT16:
            resultScalar.SetInt16(static_cast<const arrow20::Int16Scalar&>(scalar).value);
            break;
        case arrow20::Type::INT32:
            resultScalar.SetInt32(static_cast<const arrow20::Int32Scalar&>(scalar).value);
            break;
        case arrow20::Type::INT64:
            resultScalar.SetInt64(static_cast<const arrow20::Int64Scalar&>(scalar).value);
            break;
        case arrow20::Type::DOUBLE:
            resultScalar.SetDouble(static_cast<const arrow20::DoubleScalar&>(scalar).value);
            break;
        case arrow20::Type::FLOAT:
            resultScalar.SetFloat(static_cast<const arrow20::FloatScalar&>(scalar).value);
            break;
        case arrow20::Type::TIMESTAMP: {
            auto* ts = resultScalar.MutableTimestamp();
            ts->SetValue(static_cast<const arrow20::TimestampScalar&>(scalar).value);
            ts->SetUnit(static_cast<const arrow20::TimestampType&>(*scalar.type).unit());
            break;
        }
        case arrow20::Type::STRING: {
            const auto& buffer = static_cast<const arrow20::StringScalar&>(scalar).value;
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
        Value = std::make_shared<arrow20::BooleanScalar>(protoScalar.GetBool());
    } else if (protoScalar.HasUint8()) {
        Value = std::make_shared<arrow20::UInt8Scalar>(protoScalar.GetUint8());
    } else if (protoScalar.HasUint16()) {
        Value = std::make_shared<arrow20::UInt16Scalar>(protoScalar.GetUint16());
    } else if (protoScalar.HasUint32()) {
        Value = std::make_shared<arrow20::UInt32Scalar>(protoScalar.GetUint32());
    } else if (protoScalar.HasUint64()) {
        Value = std::make_shared<arrow20::UInt64Scalar>(protoScalar.GetUint64());
    } else if (protoScalar.HasInt8()) {
        Value = std::make_shared<arrow20::Int8Scalar>(protoScalar.GetInt8());
    } else if (protoScalar.HasInt16()) {
        Value = std::make_shared<arrow20::Int16Scalar>(protoScalar.GetInt16());
    } else if (protoScalar.HasInt32()) {
        Value = std::make_shared<arrow20::Int32Scalar>(protoScalar.GetInt32());
    } else if (protoScalar.HasInt64()) {
        Value = std::make_shared<arrow20::Int64Scalar>(protoScalar.GetInt64());
    } else if (protoScalar.HasDouble()) {
        Value = std::make_shared<arrow20::DoubleScalar>(protoScalar.GetDouble());
    } else if (protoScalar.HasFloat()) {
        Value = std::make_shared<arrow20::FloatScalar>(protoScalar.GetFloat());
    } else if (protoScalar.HasTimestamp()) {
        arrow20::TimeUnit::type unit = arrow20::TimeUnit::type(protoScalar.GetTimestamp().GetUnit());
        Value = std::make_shared<arrow20::TimestampScalar>(protoScalar.GetTimestamp().GetValue(), std::make_shared<arrow20::TimestampType>(unit));
    } else if (protoScalar.HasString()) {
        Value = std::make_shared<arrow20::StringScalar>(protoScalar.GetString());
    }
    return TConclusionStatus::Success();
}

NKikimr::TConclusionStatus TColumnDefaultScalarValue::ParseFromString(const TString& value, const NScheme::TTypeInfo& dataType) {
    const auto dType = NArrow::GetArrowType(dataType);
    if (!dType.ok()) {
        return NKikimr::TConclusionStatus::Fail(dType.status().ToString());
    }
    NKikimr::TConclusionStatus result = NKikimr::TConclusionStatus::Success();
    std::shared_ptr<arrow20::Scalar> val;
    NArrow::SwitchType((*dType)->id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        if constexpr (arrow20::is_primitive_ctype<typename TWrap::T>()) {
            using CType = typename TWrap::T::c_type;
            using ScalarType = typename arrow20::TypeTraits<typename TWrap::T>::ScalarType;
            const TMaybe<CType> castResult = TryFromString<CType>(value);
            if (!castResult) {
                result = NKikimr::TConclusionStatus::Fail("cannot parse from string: '" + value + "' to " + (*dType)->ToString());
            } else {
                val = std::make_shared<ScalarType>(*castResult);
            }
            return true;
        }
        if constexpr (arrow20::has_string_view<typename TWrap::T>()) {
            if constexpr (TWrap::T::is_utf8) {
                using ScalarType = typename arrow20::TypeTraits<typename TWrap::T>::ScalarType;
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

}   // namespace NKikimr::NOlap
