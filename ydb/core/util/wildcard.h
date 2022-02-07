#pragma once

#include <util/generic/strbuf.h>

namespace NKikimr {

inline bool IsMatchesWildcard(TStringBuf source, TStringBuf wildcard) {
    size_t srcPos = 0;
    size_t wldPos = 0;
    size_t srcSize = source.size();
    size_t wldSize = wildcard.size();
    while(wldPos < wldSize) {
        if (wildcard[wldPos] == '*') {
            ++wldPos;
            size_t partPos = wldPos;
            while (wldPos < wldSize && wildcard[wldPos] != '*' && wildcard[wldPos] != '?') {
                ++wldPos;
            }
            if (wldPos == partPos) {
                return true;
            }
            TStringBuf part(wildcard.substr(partPos, wldPos - partPos));
            size_t partFound = source.find(part, srcPos);
            if (partFound == TStringBuf::npos) {
                return false;
            }
            srcPos = partFound + part.size();
            continue;
        }
        if (srcPos < srcSize && (wildcard[wldPos] == '?' || source[srcPos] == wildcard[wldPos])) {
            ++srcPos;
            ++wldPos;
            continue;
        } else {
            return false;
        }
    }
    return true;
}

inline bool IsMatchesWildcards(TStringBuf source, TStringBuf wildcards) {
    while (!wildcards.empty()) {
        if (IsMatchesWildcard(source, wildcards.NextTok(','))) {
            return true;
        }
    }
    return false;
}

} // NKikimr
