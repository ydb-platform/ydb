#pragma once

#include <util/string/cast.h>
#include <util/system/env.h>

#include <library/cpp/clickhouse/client/client.h>

inline const TString GetChHost() {
    return GetEnv("RECIPE_CLICKHOUSE_HOST", "localhost");
}

inline ui32 GetChPort() {
    return FromString<ui32>(GetEnv("RECIPE_CLICKHOUSE_NATIVE_PORT", "1234"));
}

inline const TString GetChUser() {
    return GetEnv("RECIPE_CLICKHOUSE_USER");
}

inline const TString GetChPassword() {
    return GetEnv("RECIPE_CLICKHOUSE_PASSWORD");
}

inline const TString GetChDatabase() {
    return "default";
}

inline NClickHouse::TClient CreateClickhouseClient() {
    NClickHouse::TClientOptions opt;
    opt
        .SetHost(GetChHost())
        .SetPort(GetChPort())
        .SetUser(GetChUser())
        .SetPassword(GetChPassword());
    return NClickHouse::TClient(opt);
}
