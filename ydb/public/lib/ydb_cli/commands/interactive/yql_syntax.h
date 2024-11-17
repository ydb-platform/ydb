#pragma once

#include <util/generic/fwd.h>

namespace NYdb {
    namespace NConsoleClient {

        // Permits invalid special comments
        bool IsAnsiQuery(const TString& queryUtf8);

    } // namespace NConsoleClient
} // namespace NYdb
