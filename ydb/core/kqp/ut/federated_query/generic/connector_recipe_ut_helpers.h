#pragma once

#include <util/string/cast.h>
#include <util/system/env.h>

inline const TString GetConnectorHost() {
    return GetEnv("YDB_CONNECTOR_RECIPE_GRPC_HOST", "localhost");
}

inline ui32 GetConnectorPort() {
    return FromString<ui32>(GetEnv("YDB_CONNECTOR_RECIPE_GRPC_PORT"));
}
