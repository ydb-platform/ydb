#pragma once

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/minikql/jsonpath/jsonpath.h>

namespace NJson2Udf {
    using namespace NKikimr;
    using namespace NUdf;
    using namespace NYql;

    extern const char JSONPATH_RESOURCE_NAME[] = "JsonPath";
    using TJsonPathResource = TBoxedResource<NJsonPath::TJsonPathPtr, JSONPATH_RESOURCE_NAME>;

    extern const char JSON_NODE_RESOURCE_NAME[] = "JsonNode";
    using TJsonNodeResource = TResource<JSON_NODE_RESOURCE_NAME>;
}

