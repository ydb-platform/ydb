#pragma once

#include <util/generic/string.h>
#include <util/string/split.h>

constexpr TStringBuf PRIVATE_REQUEST_PATH_PREFIX = "/private";

namespace NKikimr::NSQS {
    static inline bool IsPrivateRequest(const TStringBuf& path) {
        return path.StartsWith(PRIVATE_REQUEST_PATH_PREFIX);
    }

    static inline TString GetRequestPathPart(TStringBuf path, size_t partIdx, bool isPrivateRequest) {
        if (isPrivateRequest) {
            path.SkipPrefix(PRIVATE_REQUEST_PATH_PREFIX);
        }

        TVector<TStringBuf> items;
        StringSplitter(path).Split('/').AddTo(&items);
        if (items.size() > partIdx) {
            return TString(items[partIdx]);
        }
        return TString();
    }

    static inline TString ExtractQueueNameFromPath(const TStringBuf path, bool isPrivateRequest) {
        return GetRequestPathPart(path, 2, isPrivateRequest);
    }

    static inline TString ExtractAccountNameFromPath(const TStringBuf path, bool isPrivateRequest) {
        return GetRequestPathPart(path, 1, isPrivateRequest);
    }
}
