#pragma once

#include <library/cpp/yson/node/node.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>

namespace NSQLComplete {

    struct TEnvironment {
        // Given `{ "$x": "{ "Data": "foo" }" }`,
        // it will contain `{ "$x": "foo" }`
        THashMap<TString, NYT::TNode> Parameters;
    };

} // namespace NSQLComplete
