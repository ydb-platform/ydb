#pragma once

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

namespace NYql::NDom {

bool IsValidJson(const TStringBuf json);

NUdf::TUnboxedValue TryParseJsonDom(const TStringBuf json, const NUdf::IValueBuilder* valueBuilder, bool decodeUtf8 = false);

TString SerializeJsonDom(const NUdf::TUnboxedValuePod dom, bool skipMapEntity = false, bool encodeUtf8 = false, bool writeNanAsString = false);

}
