#pragma once

#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/minikql/jsonpath/jsonpath.h>

namespace NJson2Udf {

using namespace NKikimr;
using namespace NUdf;
using namespace NYql;

inline constexpr char JSONPATH_RESOURCE_NAME[] = "JsonPath";
using TJsonPathResource = TBoxedResource<NJsonPath::TJsonPathPtr, JSONPATH_RESOURCE_NAME>;

inline constexpr char JSON_NODE_RESOURCE_NAME[] = "JsonNode";
using TJsonNodeResource = TResource<JSON_NODE_RESOURCE_NAME>;

} // namespace NJson2Udf
