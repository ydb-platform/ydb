#pragma once

#include <util/string/cast.h>
#include <util/system/env.h>

#include <fmt/format.h>

#include <pqxx/pqxx>

using namespace fmt::literals;

inline const TString GetPgHost() {
    return GetEnv("POSTGRES_RECIPE_HOST", "localhost");
}

inline ui32 GetPgPort() {
    return FromString<ui32>(GetEnv("POSTGRES_RECIPE_PORT"));
}

inline const TString GetPgUser() {
    return GetEnv("POSTGRES_RECIPE_USER");
}

inline const TString GetPgDatabase() {
    return GetEnv("POSTGRES_RECIPE_DBNAME");
}

inline pqxx::connection CreatePostgresqlConnection() {
    const TString connectionString = fmt::format(
        "host={host} port={port} dbname={database} user={user}",
        "host"_a = GetPgHost(),
        "port"_a = GetPgPort(),
        "database"_a = GetPgDatabase(),
        "user"_a = GetPgUser());
    return pqxx::connection{connectionString};
}
