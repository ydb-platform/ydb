#pragma once

#include <util/generic/strbuf.h>

namespace NKikimr::NTabletMon {

// Tablet mon PathInfo (suffix after /tablets in the node monitoring URL) for handlers
// that require administration_allowed_sids. Matches public path /tablets/app/secure/...
inline constexpr TStringBuf AdminAppPathPrefix = "/app/secure";

inline bool IsAdminAppPathInfo(TStringBuf pathInfo) {
    if (!pathInfo.StartsWith(AdminAppPathPrefix)) {
        return false;
    }
    const size_t prefixLen = AdminAppPathPrefix.size();
    return pathInfo.size() == prefixLen || pathInfo[prefixLen] == '/';
}

} // namespace NKikimr::NTabletMon
