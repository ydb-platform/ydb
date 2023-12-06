#include "pg_recipe_ut_helpers.h"

#include <util/string/cast.h>
#include <util/system/env.h>

#include <fmt/format.h>

using namespace fmt::literals;

namespace NTestUtils {

    TString GetPgHost() {
        return GetEnv("POSTGRES_RECIPE_HOST", "localhost");
    }

    ui32 GetPgPort() {
        const TString port = GetEnv("POSTGRES_RECIPE_PORT");
        UNIT_ASSERT_C(port, "No postgresql port specified");
        return FromString<ui32>(port);
    }

    TString GetPgUser() {
        return GetEnv("POSTGRES_RECIPE_USER");
    }

    TString GetPgDatabase() {
        return GetEnv("POSTGRES_RECIPE_DBNAME");
    }

    pqxx::connection CreatePostgresqlConnection() {
        const TString connectionString = fmt::format(
            "host={host} port={port} dbname={database} user={user}",
            "host"_a = GetPgHost(),
            "port"_a = GetPgPort(),
            "database"_a = GetPgDatabase(),
            "user"_a = GetPgUser());
        return pqxx::connection{connectionString};
    }

} // namespace NTestUtils
