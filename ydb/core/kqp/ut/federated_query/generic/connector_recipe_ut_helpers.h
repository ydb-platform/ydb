#pragma once

#include <util/string/cast.h>
#include <util/system/env.h>

inline const TString GetConnectorHost() {
    return GetEnv("YQL_RECIPE_CONNECTOR_GRPC_HOST", "localhost");
}

inline ui32 GetConnectorPort() {
    return FromString<ui32>(GetEnv("YQL_RECIPE_CONNECTOR_GRPC_PORT"));
}
