#pragma once
#include <ydb/library/conclusion/result.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/scalar.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

#include <util/generic/string.h>

namespace NKikimr::NArrow::NScalar {
class TSerializer {
public:
    static TConclusion<TString> SerializePayloadToString(const std::shared_ptr<arrow20::Scalar>& scalar);
    static TConclusion<std::shared_ptr<arrow20::Scalar>> DeserializeFromStringWithPayload(const TString& data, const std::shared_ptr<arrow20::DataType>& dataType);
};
}