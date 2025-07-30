#include "url_matcher.h"
#include <util/stream/str.h>

namespace NActors::NAudit {

const TString ANY_PATH = "*";

void TUrlMatcher::AddPattern(const TUrlPattern& pattern) {
    TStringBuf path = pattern.Path;
    TNode* node = Root.Get();
    if (path.StartsWith('/')) {
        path = path.Skip(1);
    }

    while (path) {
        TStringBuf part;
        path.NextTok('/', part);

        TString key(part);

        auto& child = node->Children[key];
        if (!child) {
            child = MakeHolder<TNode>();
        }
        node = child.Get();
    }

    if (pattern.ParamName.empty()) {
        node->MatchedWithoutParams = true;
    } else {
        TParamCondition param;
        param.Name = pattern.ParamName;
        param.ExpectedValue = pattern.ParamValue;
        node->MatchedParams.push_back(std::move(param));
    }
}

bool TUrlMatcher::Match(const TString& url, const TCgiParameters& params) const {
    TStringBuf path(url);
    if (path.StartsWith('/')) {
        path = path.Skip(1);
    }

    const TNode* node = Root.Get();

    do {
        TStringBuf part;
        if (!path.NextTok('/', part)) {
            part = path;
            path = TStringBuf();
        }

        auto it = node->Children.find(part);
        if (it == node->Children.end()) {
            it = node->Children.find(ANY_PATH);
            if (it == node->Children.end()) {
                return false;
            }
            node = it->second.Get();
        } else {
            node = it->second.Get();
        }

        if (node->MatchedWithoutParams) {
            return true;
        }
        for (const auto& p : node->MatchedParams) {
            if (params.Has(p.Name) && (p.ExpectedValue.empty() || params.Get(p.Name) == p.ExpectedValue)) {
                return true;
            }
        }
    } while (path);

    return false;
}

bool TUrlMatcher::Match(const TString& url, const TString& params) const {
    TCgiParameters cgiParams(params);
    return Match(url, cgiParams);
}

}
