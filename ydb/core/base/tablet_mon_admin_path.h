#pragma once

#include "auth.h"
#include <ydb/core/base/appdata.h>
#include <util/generic/strbuf.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/protos/actors.pb.h>

namespace NKikimr {

// PathInfo on TEvRemoteHttpInfo for tablet mon: strip /tablets (index), remainder starts with this for admin-only app handlers.
// Full browser URL: /tablets/app/secure?TabletID=...
inline constexpr TStringBuf TabletAppSecureMonPathPrefix = "/app/secure";

// Relative segment for links from a page under /tablets/app/...
inline constexpr TStringBuf TabletAppSecureMonUrlSuffix = "app/secure";

inline bool IsTabletAppSecureMonPath(TStringBuf pathInfo) {
    if (!pathInfo.StartsWith(TabletAppSecureMonPathPrefix)) {
        return false;
    }
    if (pathInfo.size() == TabletAppSecureMonPathPrefix.size()) {
        return true;
    }
    return pathInfo[TabletAppSecureMonPathPrefix.size()] == '/';
}

// AdministrationAllowedSIDs check for tablet DevUI requests. Uses user token from ExtendedQuery
// when the node mon layer forwarded it (same path as THttpMonLegacyActorRequest::SendRequest).
inline bool TabletMonRequestIsAdministrator(const NActors::TActorContext& ctx,
        const NActors::NMon::TEvRemoteHttpInfo* msg) {
    if (!msg) {
        return IsAdministrator(AppData(ctx), nullptr);
    }
    if (const auto* ext = msg->ExtendedQuery.get()) {
        if (!ext->GetUserToken().empty()) {
            NACLib::TUserToken token(ext->GetUserToken());
            return IsAdministrator(AppData(ctx), &token);
        }
    }
    return IsAdministrator(AppData(ctx), nullptr);
}

} // namespace NKikimr
