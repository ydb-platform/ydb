#include "serialization.h"
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NArrow::NScalar {

TConclusion<TString> TSerializer::SerializePayloadToString(const std::shared_ptr<arrow::Scalar>& scalar) {
    TString resultString;
    const bool resultFlag = NArrow::SwitchType(scalar->type->id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        if constexpr (arrow::has_c_type<typename TWrap::T>()) {
            using CType = typename TWrap::T::c_type;
            using ScalarType = typename arrow::TypeTraits<typename TWrap::T>::ScalarType;
            const ScalarType* scalarTyped = static_cast<const ScalarType*>(scalar.get());
            resultString = TString(sizeof(CType), '\0');
            memcpy(&resultString[0], scalarTyped->data(), sizeof(CType));
            return true;
        }
        return false;
    });
    if (!resultFlag) {
        return TConclusionStatus::Fail("incorrect scalar type for payload serialization: " + scalar->type->ToString());
    }
    return resultString;
}

TConclusion<std::shared_ptr<arrow::Scalar>> TSerializer::DeserializeFromStringWithPayload(const TString& data, const std::shared_ptr<arrow::DataType>& dataType) {
    AFL_VERIFY(dataType);
    std::shared_ptr<arrow::Scalar> result;
    const bool resultFlag = NArrow::SwitchType(dataType->id(), [&](const auto& type) {
        using TWrap = std::decay_t<decltype(type)>;
        if constexpr (arrow::has_c_type<typename TWrap::T>()) {
            using CType = typename TWrap::T::c_type;
            AFL_VERIFY(data.size() == sizeof(CType));
            using ScalarType = typename arrow::TypeTraits<typename TWrap::T>::ScalarType;
            result = std::make_shared<ScalarType>(*(CType*)&data[0], dataType);
            return true;
        }
        return false;
    });
    if (!resultFlag) {
        return TConclusionStatus::Fail("incorrect scalar type for payload deserialization: " + dataType->ToString());
    }
    return result;
}

}