#pragma once

#include "node.h"

namespace NSQLTranslationV1 {

TNodePtr BuildChecksumTable(TPosition pos, const TTableRef& table, TScopedStatePtr scoped);

} // namespace NSQLTranslationV1
