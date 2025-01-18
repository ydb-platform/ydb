#pragma once

#include <util/generic/fwd.h>

namespace NSQLComplete {

    enum class ESqlSyntaxMode {
        Default,
        ANSI,
    };

    ESqlSyntaxMode QuerySyntaxMode(const TString& queryUtf8);

    // Permits invalid special comments
    bool IsAnsiQuery(const TString& queryUtf8);

} // namespace NSQLComplete
