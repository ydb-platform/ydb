#include "ch_recipe_ut_helpers.h"

#include <util/string/cast.h>
#include <util/system/env.h>

namespace NTestUtils {

    TString GetChHost() {
        return "localhost";
    }

    ui32 GetChPort() {
        return 19000;
    }

    TString GetChUser() {
        return "user";
    }

    TString GetChPassword() {
        return "password";
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

        TInstant start = TInstant::Now();
        ui32 attempt = 0;
        while ((TInstant::Now() - start).Seconds() < 60) {
            attempt += 1;
            try {
                return NClickHouse::TClient(opt);
            } catch (const TSystemError& e) {
                Cerr << "Attempt " << attempt << ": " << e.what() << Endl;
                Sleep(TDuration::MilliSeconds(100));
            }
        }

        throw yexception() << "Failed to connect ClickHouse in " << attempt << " attempt(s)";
    }

} // namespace NTestUtils
