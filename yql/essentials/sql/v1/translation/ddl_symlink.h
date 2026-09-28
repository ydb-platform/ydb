#pragma once

#include "node.h"

namespace NSQLTranslationV1 {

struct TSymlinkRef {
    TString Service;
    TDeferredAtom Cluster;
    TDeferredAtom Path;
};

TNodePtr BuildCreateSymlink(TPosition pos, const TSymlinkRef& link, const TDeferredAtom& target, bool existingOk, TScopedStatePtr scoped);
TNodePtr BuildDropSymlink(TPosition pos, const TSymlinkRef& link, bool missingOk, TScopedStatePtr scoped);

} // namespace NSQLTranslationV1
