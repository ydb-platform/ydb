#include "url_tree.h"
#include <util/stream/str.h>

namespace NActors {

void TUrlTree::AddPattern(const TUrlPattern& pattern) {
    TStringBuf path = pattern.Path;
    TUrlTreeNode* node = Root.Get();
    if (path.StartsWith('/')) {
        path = path.Skip(1);
    }

    while (path) {
        TStringBuf part;
        path.NextTok('/', part);
        auto& child = node->Children[part];
        if (!child) {
            child = MakeHolder<TUrlTreeNode>();
        }
        node = child.Get();
    }

    if (!pattern.ParamKey) {
        node->MatchedWithoutParams = true;
    } else {
        TUrlTreeParam param;
        param.Key = *pattern.ParamKey;
        if (pattern.ParamValue.has_value()) {
            param.Value = *pattern.ParamValue;
        }
        node->MatchedParams.push_back(std::move(param));
    }
}

bool TUrlTree::Match(const TString& url, const TCgiParameters& params) const {
    TStringBuf path(url);
    if (path.StartsWith('/')) {
        path = path.Skip(1);
    }

    const TUrlTreeNode* node = Root.Get();

    do {
        TStringBuf part;
        if (!path.NextTok('/', part)) {
            part = path;
            path = TStringBuf();
        }

        auto it = node->Children.find(part);
        if (it == node->Children.end()) {
            return false;
        }
        node = it->second.Get();

        if (node->MatchedWithoutParams) {
            return true;
        }
        for (const auto& p : node->MatchedParams) {
            if (params.Has(p.Key) && (!p.Value || params.Get(p.Key) == *p.Value)) {
                return true;
            }
        }
    } while (path);

    return false;
}

bool TUrlTree::Match(const TString& url, const TString& params) const {
    TCgiParameters cgiParams(params);
    return Match(url, cgiParams);
}

}
