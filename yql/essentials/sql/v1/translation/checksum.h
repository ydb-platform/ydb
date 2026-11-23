#pragma once

#include "node.h"

namespace NSQLTranslationV1 {

TNodePtr BuildChecksumTable(TPosition pos, const TTableRef& table, TScopedStatePtr scoped);
TNodePtr BuildCheckIndex(TPosition pos, const TTableRef& table, TScopedStatePtr scoped, const TString& indexName);

} // namespace NSQLTranslationV1
