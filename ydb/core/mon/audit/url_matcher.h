#pragma once

#include "url_matcher.h"
#include <library/cpp/cgiparam/cgiparam.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <optional>

namespace NMonitoring::NAudit {

struct TUrlPattern {
    TString Path;
    bool Recursive = false;
};

class TUrlMatcher {
public:
    void AddPattern(const TUrlPattern& pattern);
    bool MatchPath(const TStringBuf path) const;
    bool Match(const TStringBuf url) const;

private:
    struct TNode {
        THashMap<TString, TNode> Children;
        bool PatternEnd = false;
        bool Recursive = false;
    };

    TNode Root;
};

}
