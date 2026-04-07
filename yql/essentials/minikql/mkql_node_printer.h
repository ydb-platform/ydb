#pragma once
#include "defs.h"
#include "mkql_node.h"

namespace NKikimr::NMiniKQL {

TString PrintNode(const TNode* node, bool singleLine = false);

} // namespace NKikimr::NMiniKQL
