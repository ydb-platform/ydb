#include "scalars.h"

#include <ydb/library/formats/arrow/switch/switch_type.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/system/unaligned_mem.h>

namespace NKikimr::NOlap {

void ScalarToConstant(const arrow20::Scalar& scalar, NKikimrSSA::TProgram_TConstant& value) {
    switch (scalar.type->id()) {
        case arrow20::Type::BOOL:
            value.SetBool(static_cast<const arrow20::BooleanScalar&>(scalar).value);
            break;
        case arrow20::Type::UINT8:
            value.SetUint32(static_cast<const arrow20::UInt8Scalar&>(scalar).value);
            break;
        case arrow20::Type::UINT16:
            value.SetUint32(static_cast<const arrow20::UInt16Scalar&>(scalar).value);
            break;
        case arrow20::Type::UINT32:
            value.SetUint32(static_cast<const arrow20::UInt32Scalar&>(scalar).value);
            break;
        case arrow20::Type::UINT64:
            value.SetUint64(static_cast<const arrow20::UInt64Scalar&>(scalar).value);
            break;
        case arrow20::Type::INT8:
            value.SetInt32(static_cast<const arrow20::Int8Scalar&>(scalar).value);
            break;
        case arrow20::Type::INT16:
            value.SetInt32(static_cast<const arrow20::Int16Scalar&>(scalar).value);
            break;
        case arrow20::Type::INT32:
            value.SetInt32(static_cast<const arrow20::Int32Scalar&>(scalar).value);
            break;
        case arrow20::Type::INT64:
            value.SetInt64(static_cast<const arrow20::Int64Scalar&>(scalar).value);
            break;
        case arrow20::Type::FLOAT:
            value.SetFloat(static_cast<const arrow20::FloatScalar&>(scalar).value);
            break;
        case arrow20::Type::DOUBLE:
            value.SetDouble(static_cast<const arrow20::DoubleScalar&>(scalar).value);
            break;
        case arrow20::Type::DATE32:
            value.SetInt32(static_cast<const arrow20::Date32Scalar&>(scalar).value);
            break;
        case arrow20::Type::DATE64:
            value.SetInt64(static_cast<const arrow20::Date64Scalar&>(scalar).value);
            break;
        case arrow20::Type::TIMESTAMP:
            value.SetInt64(static_cast<const arrow20::TimestampScalar&>(scalar).value);
            break;
        case arrow20::Type::TIME32:
            value.SetInt32(static_cast<const arrow20::Time32Scalar&>(scalar).value);
            break;
        case arrow20::Type::TIME64:
            value.SetInt64(static_cast<const arrow20::Time64Scalar&>(scalar).value);
            break;
        case arrow20::Type::INTERVAL_MONTHS:
            value.SetInt32(static_cast<const arrow20::MonthIntervalScalar&>(scalar).value);
            break;
        case arrow20::Type::DURATION:
            value.SetInt64(static_cast<const arrow20::DurationScalar&>(scalar).value);
            break;
        case arrow20::Type::STRING: {
            auto& buffer = static_cast<const arrow20::StringScalar&>(scalar).value;
            value.SetText(TString(reinterpret_cast<const char*>(buffer->data()), buffer->size()));
            break;
        }
        case arrow20::Type::LARGE_STRING: {
            auto& buffer = static_cast<const arrow20::LargeStringScalar&>(scalar).value;
            value.SetText(TString(reinterpret_cast<const char*>(buffer->data()), buffer->size()));
            break;
        }
        case arrow20::Type::BINARY: {
            auto& buffer = static_cast<const arrow20::BinaryScalar&>(scalar).value;
            value.SetBytes(TString(reinterpret_cast<const char*>(buffer->data()), buffer->size()));
            break;
        }
        case arrow20::Type::LARGE_BINARY: {
            auto& buffer = static_cast<const arrow20::LargeBinaryScalar&>(scalar).value;
            value.SetBytes(TString(reinterpret_cast<const char*>(buffer->data()), buffer->size()));
            break;
        }
        case arrow20::Type::FIXED_SIZE_BINARY: {
            auto& buffer = static_cast<const arrow20::FixedSizeBinaryScalar&>(scalar).value;
            value.SetBytes(TString(reinterpret_cast<const char*>(buffer->data()), buffer->size()));
            break;
        }
        default:
            Y_VERIFY_S(false, "Some types have no constant conversion yet: " << scalar.type->ToString());
    }
}

std::shared_ptr<arrow20::Scalar> ConstantToScalar(const NKikimrSSA::TProgram_TConstant& value, const std::shared_ptr<arrow20::DataType>& type) {
    switch (type->id()) {
        case arrow20::Type::BOOL:
            return std::make_shared<arrow20::BooleanScalar>(value.GetBool());
        case arrow20::Type::UINT8:
            return std::make_shared<arrow20::UInt8Scalar>(value.GetUint32());
        case arrow20::Type::UINT16:
            return std::make_shared<arrow20::UInt16Scalar>(value.GetUint32());
        case arrow20::Type::UINT32:
            return std::make_shared<arrow20::UInt32Scalar>(value.GetUint32());
        case arrow20::Type::UINT64:
            return std::make_shared<arrow20::UInt64Scalar>(value.GetUint64());
        case arrow20::Type::INT8:
            return std::make_shared<arrow20::Int8Scalar>(value.GetInt32());
        case arrow20::Type::INT16:
            return std::make_shared<arrow20::Int16Scalar>(value.GetInt32());
        case arrow20::Type::INT32:
            return std::make_shared<arrow20::Int32Scalar>(value.GetInt32());
        case arrow20::Type::INT64:
            return std::make_shared<arrow20::Int64Scalar>(value.GetInt64());
        case arrow20::Type::FLOAT:
            return std::make_shared<arrow20::FloatScalar>(value.GetFloat());
        case arrow20::Type::DOUBLE:
            return std::make_shared<arrow20::DoubleScalar>(value.GetDouble());
        case arrow20::Type::DATE32:
            return std::make_shared<arrow20::Date32Scalar>(value.GetInt32());
        case arrow20::Type::DATE64:
            return std::make_shared<arrow20::Date64Scalar>(value.GetInt64());
        case arrow20::Type::TIMESTAMP:
            if (value.HasUint64()) {
                return std::make_shared<arrow20::TimestampScalar>(value.GetUint64(), type);
            } else if (value.HasInt64()) {
                return std::make_shared<arrow20::TimestampScalar>(value.GetInt64(), type);
            } else {
                Y_ABORT_UNLESS(false, "Unexpected timestamp");
            }
        case arrow20::Type::TIME32:
            return std::make_shared<arrow20::Time32Scalar>(value.GetInt32(), type);
        case arrow20::Type::TIME64:
            return std::make_shared<arrow20::Time64Scalar>(value.GetInt64(), type);
        case arrow20::Type::INTERVAL_MONTHS:
            return std::make_shared<arrow20::MonthIntervalScalar>(value.GetInt32());
        case arrow20::Type::DURATION:
            return std::make_shared<arrow20::DurationScalar>(value.GetInt64(), type);
        case arrow20::Type::STRING:
            return std::make_shared<arrow20::StringScalar>(value.GetText());
        case arrow20::Type::LARGE_STRING:
            return std::make_shared<arrow20::LargeStringScalar>(value.GetText());
        case arrow20::Type::BINARY: {
            std::string bytes = value.GetBytes();
            return std::make_shared<arrow20::BinaryScalar>(arrow20::Buffer::FromString(bytes));
        }
        case arrow20::Type::LARGE_BINARY: {
            std::string bytes = value.GetBytes();
            return std::make_shared<arrow20::LargeBinaryScalar>(arrow20::Buffer::FromString(bytes));
        }
        case arrow20::Type::FIXED_SIZE_BINARY: {
            std::string bytes = value.GetBytes();
            return std::make_shared<arrow20::FixedSizeBinaryScalar>(arrow20::Buffer::FromString(bytes), type);
        }
        default:
            Y_VERIFY_S(false, "Some types have no constant conversion yet: " << type->ToString());
    }
    return {};
}

TString SerializeKeyScalar(const std::shared_ptr<arrow20::Scalar>& key) {
    TString out;
    NArrow::SwitchType(key->type->id(), [&](const auto& t) {
        using TWrap = std::decay_t<decltype(t)>;
        using T = typename TWrap::T;
        using TScalar = typename arrow20::TypeTraits<T>::ScalarType;

        if constexpr (std::is_same_v<T, arrow20::StringType> || std::is_same_v<T, arrow20::BinaryType> ||
                      std::is_same_v<T, arrow20::LargeStringType> || std::is_same_v<T, arrow20::LargeBinaryType> ||
                      std::is_same_v<T, arrow20::FixedSizeBinaryType>) {
            auto& buffer = static_cast<const TScalar&>(*key).value;
            out = buffer->ToString();
        } else if constexpr (std::is_same_v<T, arrow20::HalfFloatType>) {
            return false;
        } else if constexpr (arrow20::is_temporal_type<T>::value || arrow20::has_c_type<T>::value) {
            using TCType = typename arrow20::TypeTraits<T>::CType;
            static_assert(std::is_same_v<TCType, typename TScalar::ValueType>);

            const TCType& value = static_cast<const TScalar&>(*key).value;
            out = TString(reinterpret_cast<const char*>(&value), sizeof(value));
            Y_VERIFY_S(!out.empty(), key->type->ToString());
        } else {
            return false;
        }
        return true;
    });
    return out;
}

std::shared_ptr<arrow20::Scalar> DeserializeKeyScalar(const TString& key, const std::shared_ptr<arrow20::DataType>& type) {
    std::shared_ptr<arrow20::Scalar> out;
    NArrow::SwitchType(type->id(), [&](const auto& t) {
        using TWrap = std::decay_t<decltype(t)>;
        using T = typename TWrap::T;
        using TScalar = typename arrow20::TypeTraits<T>::ScalarType;

        if constexpr (std::is_same_v<T, arrow20::StringType> || std::is_same_v<T, arrow20::BinaryType> ||
                      std::is_same_v<T, arrow20::LargeStringType> || std::is_same_v<T, arrow20::LargeBinaryType> ||
                      std::is_same_v<T, arrow20::FixedSizeBinaryType>) {
            out = std::make_shared<TScalar>(arrow20::Buffer::FromString(key), type);
        } else if constexpr (std::is_same_v<T, arrow20::HalfFloatType>) {
            return false;
        } else if constexpr (arrow20::is_temporal_type<T>::value) {
            using TCType = typename arrow20::TypeTraits<T>::CType;
            static_assert(std::is_same_v<TCType, typename TScalar::ValueType>);

            Y_ABORT_UNLESS(key.size() == sizeof(TCType));
            TCType value = ReadUnaligned<TCType>(key.data());
            out = std::make_shared<TScalar>(value, type);
        } else if constexpr (arrow20::has_c_type<T>::value) {
            using TCType = typename arrow20::TypeTraits<T>::CType;
            static_assert(std::is_same_v<TCType, typename TScalar::ValueType>);

            Y_ABORT_UNLESS(key.size() == sizeof(TCType));
            TCType value = ReadUnaligned<TCType>(key.data());
            out = std::make_shared<TScalar>(value);
        } else {
            return false;
        }
        return true;
    });
    Y_VERIFY_S(out, type->ToString());
    return out;
}

}   // namespace NKikimr::NOlap
