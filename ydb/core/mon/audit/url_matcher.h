#pragma once

#include "url_matcher.h"
#include <library/cpp/cgiparam/cgiparam.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <optional>

namespace NActors::NAudit {

struct TUrlPattern {
    TString Path;
    TString ParamName;
    TString ParamValue;
};

class TUrlMatcher {
public:
    void AddPattern(const TUrlPattern& rule);
    bool Match(const TString& url, const TCgiParameters& params) const;
    bool Match(const TString& url, const TString& params = "") const;

private:
    struct TParamCondition {
        TString Name;
        TString ExpectedValue;
    };

    struct TNode {
        THashMap<TString, THolder<TNode>> Children;
        bool MatchedWithoutParams = false;
        TVector<TParamCondition> MatchedParams;
    };

    THolder<TNode> Root = MakeHolder<TNode>();
};

}
