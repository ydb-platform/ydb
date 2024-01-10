#include "pg_recipe_ut_helpers.h"

#include <util/string/cast.h>
#include <util/system/env.h>

#include <fmt/format.h>

using namespace fmt::literals;

namespace NTestUtils {

    TString GetPgHost() {
        return "localhost";
    }

    ui32 GetPgPort() {
        return 15432;
    }

    TString GetPgUser() {
        return "user";
    }

    TString GetPgDatabase() {
        return "db";
    }

    TString GetPgPassword() {
        return "password";
    }

    pqxx::connection CreatePostgresqlConnection() {
        const TString connectionString = fmt::format(
            "host={host} port={port} dbname={database} user={user} password={password}",
            "host"_a = GetPgHost(),
            "port"_a = GetPgPort(),
            "database"_a = GetPgDatabase(),
            "user"_a = GetPgUser(),
            "password"_a = GetPgPassword());

        TInstant start = TInstant::Now();
        ui32 attempt = 0;
        while ((TInstant::Now() - start).Seconds() < 60) {
            attempt += 1;
            try {
                return pqxx::connection{connectionString};
            } catch (const pqxx::broken_connection& e) {
                Cerr << "Attempt " << attempt << ": " << e.what() << Endl;
                Sleep(TDuration::MilliSeconds(100));
            }
        }

        throw yexception() << "Failed to connect PostgreSQL in " << attempt << " attempt(s)";
    }

} // namespace NTestUtils
