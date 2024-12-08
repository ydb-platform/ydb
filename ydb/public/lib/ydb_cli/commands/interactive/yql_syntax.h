#pragma once

#include <util/generic/fwd.h>

namespace NYdb {
    namespace NConsoleClient {

        enum class EYQLSyntaxMode {
            Default,
            ANSI,
        };

        EYQLSyntaxMode QuerySyntaxMode(const TString& queryUtf8);

        // Permits invalid special comments
        bool IsAnsiQuery(const TString& queryUtf8);

    } // namespace NConsoleClient
} // namespace NYdb
