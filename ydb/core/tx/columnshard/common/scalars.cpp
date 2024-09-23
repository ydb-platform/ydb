#include "scalars.h"

#include <ydb/core/formats/arrow/switch_type.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr::NOlap {

void ScalarToConstant(const arrow::Scalar& scalar, NKikimrSSA::TProgram_TConstant& value) {
    switch (scalar.type->id()) {
        case arrow::Type::BOOL:
            value.SetBool(static_cast<const arrow::BooleanScalar&>(scalar).value);
            break;
        case arrow::Type::UINT8:
            value.SetUint32(static_cast<const arrow::UInt8Scalar&>(scalar).value);
            break;
        case arrow::Type::UINT16:
            value.SetUint32(static_cast<const arrow::UInt16Scalar&>(scalar).value);
            break;
        case arrow::Type::UINT32:
            value.SetUint32(static_cast<const arrow::UInt32Scalar&>(scalar).value);
            break;
        case arrow::Type::UINT64:
            value.SetUint64(static_cast<const arrow::UInt64Scalar&>(scalar).value);
            break;
        case arrow::Type::INT8:
            value.SetInt32(static_cast<const arrow::Int8Scalar&>(scalar).value);
            break;
        case arrow::Type::INT16:
            value.SetInt32(static_cast<const arrow::Int16Scalar&>(scalar).value);
            break;
        case arrow::Type::INT32:
            value.SetInt32(static_cast<const arrow::Int32Scalar&>(scalar).value);
            break;
        case arrow::Type::INT64:
            value.SetInt64(static_cast<const arrow::Int64Scalar&>(scalar).value);
            break;
        case arrow::Type::FLOAT:
            value.SetFloat(static_cast<const arrow::FloatScalar&>(scalar).value);
            break;
        case arrow::Type::DOUBLE:
            value.SetDouble(static_cast<const arrow::DoubleScalar&>(scalar).value);
            break;
        case arrow::Type::DATE32:
            value.SetInt32(static_cast<const arrow::Date32Scalar&>(scalar).value);
            break;
        case arrow::Type::DATE64:
            value.SetInt64(static_cast<const arrow::Date64Scalar&>(scalar).value);
            break;
        case arrow::Type::TIMESTAMP:
            value.SetInt64(static_cast<const arrow::TimestampScalar&>(scalar).value);
            break;
        case arrow::Type::TIME32:
            value.SetInt32(static_cast<const arrow::Time32Scalar&>(scalar).value);
            break;
        case arrow::Type::TIME64:
            value.SetInt64(static_cast<const arrow::Time64Scalar&>(scalar).value);
            break;
        case arrow::Type::INTERVAL_MONTHS:
            value.SetInt32(static_cast<const arrow::MonthIntervalScalar&>(scalar).value);
            break;
        case arrow::Type::DURATION:
            value.SetInt64(static_cast<const arrow::DurationScalar&>(scalar).value);
            break;
        case arrow::Type::STRING: {
            auto& buffer = static_cast<const arrow::StringScalar&>(scalar).value;
            value.SetText(TString(reinterpret_cast<const char *>(buffer->data()), buffer->size()));
            break;
        }
        case arrow::Type::LARGE_STRING: {
            auto& buffer = static_cast<const arrow::LargeStringScalar&>(scalar).value;
            value.SetText(TString(reinterpret_cast<const char *>(buffer->data()), buffer->size()));
            break;
        }
        case arrow::Type::BINARY: {
            auto& buffer = static_cast<const arrow::BinaryScalar&>(scalar).value;
            value.SetBytes(TString(reinterpret_cast<const char *>(buffer->data()), buffer->size()));
            break;
        }
        case arrow::Type::LARGE_BINARY: {
            auto& buffer = static_cast<const arrow::LargeBinaryScalar&>(scalar).value;
            value.SetBytes(TString(reinterpret_cast<const char *>(buffer->data()), buffer->size()));
            break;
        }
        case arrow::Type::FIXED_SIZE_BINARY: {
            auto& buffer = static_cast<const arrow::FixedSizeBinaryScalar&>(scalar).value;
            value.SetBytes(TString(reinterpret_cast<const char *>(buffer->data()), buffer->size()));
            break;
        }
        default:
            Y_VERIFY_S(false, "Some types have no constant conversion yet: " << scalar.type->ToString());
    }
}

std::shared_ptr<arrow::Scalar> ConstantToScalar(const NKikimrSSA::TProgram_TConstant& value,
                                                const std::shared_ptr<arrow::DataType>& type) {
    switch (type->id()) {
        case arrow::Type::BOOL:
            return std::make_shared<arrow::BooleanScalar>(value.GetBool());
        case arrow::Type::UINT8:
            return std::make_shared<arrow::UInt8Scalar>(value.GetUint32());
        case arrow::Type::UINT16:
            return std::make_shared<arrow::UInt16Scalar>(value.GetUint32());
        case arrow::Type::UINT32:
            return std::make_shared<arrow::UInt32Scalar>(value.GetUint32());
        case arrow::Type::UINT64:
            return std::make_shared<arrow::UInt64Scalar>(value.GetUint64());
        case arrow::Type::INT8:
            return std::make_shared<arrow::Int8Scalar>(value.GetInt32());
        case arrow::Type::INT16:
            return std::make_shared<arrow::Int16Scalar>(value.GetInt32());
        case arrow::Type::INT32:
            return std::make_shared<arrow::Int32Scalar>(value.GetInt32());
        case arrow::Type::INT64:
            return std::make_shared<arrow::Int64Scalar>(value.GetInt64());
        case arrow::Type::FLOAT:
            return std::make_shared<arrow::FloatScalar>(value.GetFloat());
        case arrow::Type::DOUBLE:
            return std::make_shared<arrow::DoubleScalar>(value.GetDouble());
        case arrow::Type::DATE32:
            return std::make_shared<arrow::Date32Scalar>(value.GetInt32());
        case arrow::Type::DATE64:
            return std::make_shared<arrow::Date64Scalar>(value.GetInt64());
        case arrow::Type::TIMESTAMP:
            if (value.HasUint64()) {
                return std::make_shared<arrow::TimestampScalar>(value.GetUint64(), type);
            } else if (value.HasInt64()) {
                return std::make_shared<arrow::TimestampScalar>(value.GetInt64(), type);
            } else {
                Y_ABORT_UNLESS(false, "Unexpected timestamp");
            }
        case arrow::Type::TIME32:
            return std::make_shared<arrow::Time32Scalar>(value.GetInt32(), type);
        case arrow::Type::TIME64:
            return std::make_shared<arrow::Time64Scalar>(value.GetInt64(), type);
        case arrow::Type::INTERVAL_MONTHS:
            return std::make_shared<arrow::MonthIntervalScalar>(value.GetInt32());
        case arrow::Type::DURATION:
            return std::make_shared<arrow::DurationScalar>(value.GetInt64(), type);
        case arrow::Type::STRING:
            return std::make_shared<arrow::StringScalar>(value.GetText());
        case arrow::Type::LARGE_STRING:
            return std::make_shared<arrow::LargeStringScalar>(value.GetText());
        case arrow::Type::BINARY: {
            std::string bytes = value.GetBytes();
            return std::make_shared<arrow::BinaryScalar>(arrow::Buffer::FromString(bytes));
        }
        case arrow::Type::LARGE_BINARY: {
            std::string bytes = value.GetBytes();
            return std::make_shared<arrow::LargeBinaryScalar>(arrow::Buffer::FromString(bytes));
        }
        case arrow::Type::FIXED_SIZE_BINARY: {
            std::string bytes = value.GetBytes();
            return std::make_shared<arrow::FixedSizeBinaryScalar>(arrow::Buffer::FromString(bytes), type);
        }
        default:
            Y_VERIFY_S(false, "Some types have no constant conversion yet: " << type->ToString());
    }
    return {};
}

TString SerializeKeyScalar(const std::shared_ptr<arrow::Scalar>& key) {
    TString out;
    NArrow::SwitchType(key->type->id(), [&](const auto& t) {
        using TWrap = std::decay_t<decltype(t)>;
        using T = typename TWrap::T;
        using TScalar = typename arrow::TypeTraits<T>::ScalarType;

        if constexpr (std::is_same_v<T, arrow::StringType> ||
                      std::is_same_v<T, arrow::BinaryType> ||
                      std::is_same_v<T, arrow::LargeStringType> ||
                      std::is_same_v<T, arrow::LargeBinaryType> ||
                      std::is_same_v<T, arrow::FixedSizeBinaryType>) {
            auto& buffer = static_cast<const TScalar&>(*key).value;
            out = buffer->ToString();
        } else if constexpr (std::is_same_v<T, arrow::HalfFloatType>) {
            return false;
        } else if constexpr (arrow::is_temporal_type<T>::value || arrow::has_c_type<T>::value) {
            using TCType = typename arrow::TypeTraits<T>::CType;
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

std::shared_ptr<arrow::Scalar> DeserializeKeyScalar(const TString& key, const std::shared_ptr<arrow::DataType>& type) {
    std::shared_ptr<arrow::Scalar> out;
    NArrow::SwitchType(type->id(), [&](const auto& t) {
        using TWrap = std::decay_t<decltype(t)>;
        using T = typename TWrap::T;
        using TScalar = typename arrow::TypeTraits<T>::ScalarType;

        if constexpr (std::is_same_v<T, arrow::StringType> ||
                      std::is_same_v<T, arrow::BinaryType> ||
                      std::is_same_v<T, arrow::LargeStringType> ||
                      std::is_same_v<T, arrow::LargeBinaryType> ||
                      std::is_same_v<T, arrow::FixedSizeBinaryType>) {
            out = std::make_shared<TScalar>(arrow::Buffer::FromString(key), type);
        } else if constexpr (std::is_same_v<T, arrow::HalfFloatType>) {
            return false;
        } else if constexpr (arrow::is_temporal_type<T>::value) {
            using TCType = typename arrow::TypeTraits<T>::CType;
            static_assert(std::is_same_v<TCType, typename TScalar::ValueType>);

            Y_ABORT_UNLESS(key.size() == sizeof(TCType));
            TCType value = ReadUnaligned<TCType>(key.data());
            out = std::make_shared<TScalar>(value, type);
        } else if constexpr (arrow::has_c_type<T>::value) {
            using TCType = typename arrow::TypeTraits<T>::CType;
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

}
