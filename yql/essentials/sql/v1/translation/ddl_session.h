#pragma once

#include "node.h"

namespace NSQLTranslationV1 {

TNodePtr BuildKillSession(TPosition pos, TNodePtr sessionId, TScopedStatePtr scoped);

} // namespace NSQLTranslationV1
