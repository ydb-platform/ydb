#include "url_matcher.h"
#include <util/stream/str.h>

namespace NMonitoring::NAudit {

const TString ANY_PATH = "*";

void TUrlMatcher::AddPattern(const TUrlPattern& pattern) {
    TStringBuf path = pattern.Path;
    TNode* node = &Root;
    if (path.StartsWith('/')) {
        path = path.Skip(1);
    }

    while (path) {
        TStringBuf part;
        path.NextTok('/', part);
        node = &node->Children[part];
    }

    if (pattern.ParamName.empty()) {
        node->MatchWithoutParams = true;
    } else {
        TParamCondition param;
        param.Name = pattern.ParamName;
        param.ExpectedValue = pattern.ParamValue;
        node->MatchedParams.push_back(std::move(param));
    }
}

bool TUrlMatcher::Match(const TStringBuf originalPath, const TCgiParameters& params) const {
    TStringBuf path(originalPath);
    if (path.StartsWith('/')) {
        path = path.Skip(1);
    }

    const TNode* node = &Root;

    while (path) {
        TStringBuf part;
        if (!path.NextTok('/', part)) {
            part = path;
            path = TStringBuf();
        }

        auto it = node->Children.find(part);
        if (it == node->Children.end()) {
            it = node->Children.find(ANY_PATH);
        }
        if (it == node->Children.end()) {
            return false;
        }
        node = &it->second;
    };

    if (node->MatchWithoutParams) {
        return true;
    }
    for (const auto& p : node->MatchedParams) {
        if (params.Has(p.Name) && (p.ExpectedValue.empty() || params.Get(p.Name) == p.ExpectedValue)) {
            return true;
        }
    }

    return false;
}

bool TUrlMatcher::Match(const TStringBuf url) const {
    const auto path = url.Before('?');
    const auto params = url.After('?');
    const auto cgiParams = TCgiParameters(params);

    return Match(path, cgiParams);
}

}
