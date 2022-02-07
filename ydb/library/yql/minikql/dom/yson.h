#pragma once

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

namespace NYql::NDom {

bool IsValidYson(const TStringBuf yson);

NUdf::TUnboxedValue TryParseYsonDom(const TStringBuf yson, const NUdf::IValueBuilder* valueBuilder);

TString SerializeYsonDomToBinary(const NUdf::TUnboxedValue& dom);

TString SerializeYsonDomToText(const NUdf::TUnboxedValue& dom);

TString SerializeYsonDomToPrettyText(const NUdf::TUnboxedValue& dom);

}
