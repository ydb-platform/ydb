#pragma once

#include <util/generic/fwd.h>

namespace NSQLComplete {

    enum class ESqlSyntaxMode {
        Default,
        ANSI,
    };

    ESqlSyntaxMode QuerySyntaxMode(const TString& query);

    // Permits invalid special comments
    bool IsAnsiQuery(const TString& query);

} // namespace NSQLComplete
