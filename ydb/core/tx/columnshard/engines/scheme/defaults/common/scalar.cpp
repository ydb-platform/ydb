#include "scalar.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/formats/arrow/switch/switch_type.h>

#include <util/generic/ylimits.h>
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
        case arrow::Type::TIMESTAMP: {
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
        case arrow::Type::BINARY: {
            const auto& buffer = static_cast<const arrow::BinaryScalar&>(scalar).value;
            resultScalar.SetBytes(buffer->ToString());
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
        Value = std::make_shared<arrow::UInt8Scalar>(protoScalar.GetBool() ? static_cast<ui8>(1) : static_cast<ui8>(0));
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
    } else if (protoScalar.HasBytes()) {
        Value = std::make_shared<arrow::BinaryScalar>(arrow::Buffer::FromString(std::string(protoScalar.GetBytes())));
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

NKikimr::TConclusionStatus TColumnDefaultScalarValue::ParseFromTypedValue(const Ydb::TypedValue& typed, const NScheme::TTypeInfo& type) {
    if (!typed.has_type() || !typed.type().has_type_id()) {
        return TConclusionStatus::Fail("Default value is not supported for type " + ::ToString(type.GetTypeId()) + " in column tables");
    }
    const auto& value = typed.value();
    const auto valueCase = value.value_case();
    const auto invalidLiteral =
        TConclusionStatus::Fail("Default value is not supported for type " + ::ToString(type.GetTypeId()) + " in column tables");
    std::shared_ptr<arrow::Scalar> scalar;
    switch (typed.type().type_id()) {
        case Ydb::Type::BOOL:
            if (valueCase != Ydb::Value::kBoolValue) {
                return invalidLiteral;
            }
            scalar = std::make_shared<arrow::UInt8Scalar>(value.bool_value() ? 1 : 0);
            break;
        case Ydb::Type::UINT8:
            if (valueCase != Ydb::Value::kUint32Value || value.uint32_value() > Max<ui8>()) {
                return invalidLiteral;
            }
            scalar = std::make_shared<arrow::UInt8Scalar>(static_cast<ui8>(value.uint32_value()));
            break;
        case Ydb::Type::UINT16:
            if (valueCase != Ydb::Value::kUint32Value || value.uint32_value() > Max<ui16>()) {
                return invalidLiteral;
            }
            scalar = std::make_shared<arrow::UInt16Scalar>(static_cast<ui16>(value.uint32_value()));
            break;
        case Ydb::Type::UINT32:
            if (valueCase != Ydb::Value::kUint32Value) {
                return invalidLiteral;
            }
            scalar = std::make_shared<arrow::UInt32Scalar>(value.uint32_value());
            break;
        case Ydb::Type::UINT64:
            if (valueCase != Ydb::Value::kUint64Value) {
                return invalidLiteral;
            }
            scalar = std::make_shared<arrow::UInt64Scalar>(value.uint64_value());
            break;
        case Ydb::Type::INT8:
            if (valueCase != Ydb::Value::kInt32Value || value.int32_value() < Min<i8>() || value.int32_value() > Max<i8>()) {
                return invalidLiteral;
            }
            scalar = std::make_shared<arrow::Int8Scalar>(static_cast<i8>(value.int32_value()));
            break;
        case Ydb::Type::INT16:
            if (valueCase != Ydb::Value::kInt32Value || value.int32_value() < Min<i16>() || value.int32_value() > Max<i16>()) {
                return invalidLiteral;
            }
            scalar = std::make_shared<arrow::Int16Scalar>(static_cast<i16>(value.int32_value()));
            break;
        case Ydb::Type::INT32:
            if (valueCase != Ydb::Value::kInt32Value) {
                return invalidLiteral;
            }
            scalar = std::make_shared<arrow::Int32Scalar>(value.int32_value());
            break;
        case Ydb::Type::INT64:
            if (valueCase != Ydb::Value::kInt64Value) {
                return invalidLiteral;
            }
            scalar = std::make_shared<arrow::Int64Scalar>(value.int64_value());
            break;
        case Ydb::Type::FLOAT:
            if (valueCase != Ydb::Value::kFloatValue) {
                return invalidLiteral;
            }
            scalar = std::make_shared<arrow::FloatScalar>(value.float_value());
            break;
        case Ydb::Type::DOUBLE:
            if (valueCase != Ydb::Value::kDoubleValue) {
                return invalidLiteral;
            }
            scalar = std::make_shared<arrow::DoubleScalar>(value.double_value());
            break;
        case Ydb::Type::UTF8:
            if (valueCase != Ydb::Value::kTextValue) {
                return invalidLiteral;
            }
            scalar = std::make_shared<arrow::StringScalar>(value.text_value());
            break;
        case Ydb::Type::STRING:
            if (valueCase != Ydb::Value::kBytesValue) {
                return invalidLiteral;
            }
            scalar = std::make_shared<arrow::BinaryScalar>(arrow::Buffer::FromString(std::string(value.bytes_value())));
            break;
        case Ydb::Type::TIMESTAMP:
            if (valueCase != Ydb::Value::kUint64Value) {
                return invalidLiteral;
            }
            scalar = std::make_shared<arrow::TimestampScalar>(value.uint64_value(), arrow::timestamp(arrow::TimeUnit::MICRO));
            break;
        default:
            return TConclusionStatus::Fail("Default value is not supported for type " + ::ToString(type.GetTypeId()) + " in column tables");
    }
    if (!IsCompatibleType(scalar->type) && Value) {
        return TConclusionStatus::Fail("incompatible default literal");
    }
    const auto expected = NArrow::GetArrowType(type);
    if (!expected.ok()) {
        return TConclusionStatus::Fail(expected.status().ToString());
    }
    if (type.GetTypeId() != 0 && !(*expected)->Equals(scalar->type)) {
        return TConclusionStatus::Fail("Default value is not supported for type " + ::ToString(type.GetTypeId()) + " in column tables");
    }
    Value = scalar;
    return TConclusionStatus::Success();
}

TConclusion<Ydb::TypedValue> TColumnDefaultScalarValue::SerializeToTypedValue(const NScheme::TTypeInfo& type) const {
    Ydb::TypedValue result;
    if (!Value) {
        return TConclusionStatus::Fail("empty default");
    }
    auto* outType = result.mutable_type();
    auto* value = result.mutable_value();
    switch (Value->type->id()) {
        case arrow::Type::BOOL:
            outType->set_type_id(Ydb::Type::BOOL);
            value->set_bool_value(static_cast<const arrow::BooleanScalar&>(*Value).value);
            break;
        case arrow::Type::UINT8:
            if (type.GetTypeId() == NScheme::NTypeIds::Bool) {
                outType->set_type_id(Ydb::Type::BOOL);
                value->set_bool_value(static_cast<const arrow::UInt8Scalar&>(*Value).value != 0);
            } else {
                outType->set_type_id(Ydb::Type::UINT8);
                value->set_uint32_value(static_cast<const arrow::UInt8Scalar&>(*Value).value);
            }
            break;
        case arrow::Type::UINT16:
            outType->set_type_id(Ydb::Type::UINT16);
            value->set_uint32_value(static_cast<const arrow::UInt16Scalar&>(*Value).value);
            break;
        case arrow::Type::UINT32:
            outType->set_type_id(Ydb::Type::UINT32);
            value->set_uint32_value(static_cast<const arrow::UInt32Scalar&>(*Value).value);
            break;
        case arrow::Type::UINT64:
            outType->set_type_id(Ydb::Type::UINT64);
            value->set_uint64_value(static_cast<const arrow::UInt64Scalar&>(*Value).value);
            break;
        case arrow::Type::INT8:
            outType->set_type_id(Ydb::Type::INT8);
            value->set_int32_value(static_cast<const arrow::Int8Scalar&>(*Value).value);
            break;
        case arrow::Type::INT16:
            outType->set_type_id(Ydb::Type::INT16);
            value->set_int32_value(static_cast<const arrow::Int16Scalar&>(*Value).value);
            break;
        case arrow::Type::INT32:
            outType->set_type_id(Ydb::Type::INT32);
            value->set_int32_value(static_cast<const arrow::Int32Scalar&>(*Value).value);
            break;
        case arrow::Type::INT64:
            outType->set_type_id(Ydb::Type::INT64);
            value->set_int64_value(static_cast<const arrow::Int64Scalar&>(*Value).value);
            break;
        case arrow::Type::FLOAT:
            outType->set_type_id(Ydb::Type::FLOAT);
            value->set_float_value(static_cast<const arrow::FloatScalar&>(*Value).value);
            break;
        case arrow::Type::DOUBLE:
            outType->set_type_id(Ydb::Type::DOUBLE);
            value->set_double_value(static_cast<const arrow::DoubleScalar&>(*Value).value);
            break;
        case arrow::Type::STRING:
            outType->set_type_id(Ydb::Type::UTF8);
            value->set_text_value(static_cast<const arrow::StringScalar&>(*Value).value->ToString());
            break;
        case arrow::Type::BINARY:
            outType->set_type_id(Ydb::Type::STRING);
            value->set_bytes_value(static_cast<const arrow::BinaryScalar&>(*Value).value->ToString());
            break;
        case arrow::Type::TIMESTAMP:
            outType->set_type_id(Ydb::Type::TIMESTAMP);
            value->set_uint64_value(static_cast<const arrow::TimestampScalar&>(*Value).value);
            break;
        default:
            return TConclusionStatus::Fail("Default value is not supported for type " + Value->type->ToString() + " in column tables");
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
