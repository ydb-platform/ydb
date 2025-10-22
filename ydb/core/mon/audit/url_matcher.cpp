#include "url_matcher.h"
#include <util/stream/str.h>

namespace NMonitoring::NAudit {

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

    node->PatternEnd = true;
    node->Recursive = pattern.Recursive;
}

bool TUrlMatcher::MatchPath(const TStringBuf originalPath) const {
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
            return false;
        }
        node = &it->second;
        if (node->Recursive) {
            return true;
        }
    };

    return node->PatternEnd;
}

bool TUrlMatcher::Match(const TStringBuf url) const {
    const auto path = url.Before('?');
    const auto params = url.After('?');
    const auto cgiParams = TCgiParameters(params);

    return MatchPath(path);
}

}
