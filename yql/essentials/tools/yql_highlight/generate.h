#pragma once

#include <yql/essentials/sql/v1/highlight/sql_highlight.h>

namespace NSQLHighlight {

    bool IsPlain(EUnitKind kind);

    bool IsIgnored(EUnitKind kind);

} // namespace NSQLHighlight
