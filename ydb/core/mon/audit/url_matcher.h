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
    TString ParamName;
    TString ParamValue;
};

class TUrlMatcher {
public:
    void AddPattern(const TUrlPattern& rule);
    bool Match(const TStringBuf path, const TCgiParameters& params) const;
    bool Match(const TStringBuf url) const;

private:
    struct TParamCondition {
        TString Name;
        TString ExpectedValue;
    };

    struct TNode {
        THashMap<TString, TNode> Children;
        bool MatchWithoutParams = false;
        TVector<TParamCondition> MatchedParams;
    };

    TNode Root;
};

}
