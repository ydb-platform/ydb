#include "ch_recipe_ut_helpers.h"

#include <util/string/cast.h>
#include <util/system/env.h>

namespace NTestUtils {

    TString GetChHost() {
        return GetEnv("RECIPE_CLICKHOUSE_HOST", "localhost");
    }

    ui32 GetChPort() {
        return FromString<ui32>(GetEnv("RECIPE_CLICKHOUSE_NATIVE_PORT", "1234"));
    }

    TString GetChUser() {
        return GetEnv("RECIPE_CLICKHOUSE_USER");
    }

    TString GetChPassword() {
        return GetEnv("RECIPE_CLICKHOUSE_PASSWORD");
    }

    TString GetChDatabase() {
        return "default";
    }

    NClickHouse::TClient CreateClickhouseClient() {
        NClickHouse::TClientOptions opt;
        opt
            .SetHost(GetChHost())
            .SetPort(GetChPort())
            .SetUser(GetChUser())
            .SetPassword(GetChPassword());
        return NClickHouse::TClient(opt);
    }

} // namespace NTestUtils
