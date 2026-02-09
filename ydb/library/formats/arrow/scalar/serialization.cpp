#include "serialization.h"
#include <ydb/library/formats/arrow/switch/switch_type.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NArrow::NScalar {

TConclusion<TString> TSerializer::SerializePayloadToString(const std::shared_ptr<arrow::Scalar>& scalar) {
    TString resultString;
    const bool resultFlag = NArrow::SwitchType(scalar->type->id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using T = typename TWrap::T;
        if constexpr (arrow::has_c_type<T>()) {
            using CType = typename T::c_type;
            using CTypeScalar = typename arrow::TypeTraits<T>::ScalarType;
            const CTypeScalar* scalarTyped = static_cast<const CTypeScalar*>(scalar.get());
            resultString = TString(sizeof(CType), '\0');
            memcpy(&resultString[0], scalarTyped->data(), sizeof(CType));
            return true;
        } else if constexpr (arrow::has_string_view<T>()) {
            const arrow::BaseBinaryScalar* typed = static_cast<const arrow::BaseBinaryScalar*>(scalar.get());
            resultString.append(reinterpret_cast<const char*>(typed->value->data()), typed->value->size());
            return true;
        }
        return false;
    });
    if (!resultFlag) {
        return TConclusionStatus::Fail("incorrect scalar type for payload serialization: " + scalar->type->ToString());
    }
    return resultString;
}

TConclusion<std::shared_ptr<arrow::Scalar>> TSerializer::DeserializeFromStringWithPayload(TStringBuf data, const std::shared_ptr<arrow::DataType>& dataType) {
    AFL_VERIFY(dataType);
    std::shared_ptr<arrow::Scalar> result;
    const bool resultFlag = NArrow::SwitchType(dataType->id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        using T = typename TWrap::T;
        if constexpr (arrow::has_c_type<T>()) {
            using CType = typename T::c_type;
            using ScalarType = typename arrow::TypeTraits<T>::ScalarType;
            AFL_VERIFY(data.size() == sizeof(CType))("mismatch", Sprintf("data.size(): %i vs CType: %s with size %i", data.size(), T::type_name(), sizeof(CType)));
            result = std::make_shared<ScalarType>(*(CType*)&data[0], dataType);
            return true;
        } else if constexpr (arrow::has_string_view<T>()) {
            using ScalarType = typename arrow::TypeTraits<T>::ScalarType;
            result = std::make_shared<ScalarType>(std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(data.data()), data.size()), dataType);
            return true;
        } else return false;
    });
    if (!resultFlag) {
        return TConclusionStatus::Fail("incorrect scalar type for payload deserialization: " + dataType->ToString());
    }
    return result;
}

}