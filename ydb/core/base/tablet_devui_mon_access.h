#pragma once

#include "auth.h"
#include <ydb/core/base/appdata.h>
#include <util/generic/strbuf.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/protos/actors.pb.h>

namespace NKikimr {

inline constexpr TStringBuf TabletDevUiSecurePathInfoPrefix = "/app/secure";

inline constexpr TStringBuf TabletDevUiSecureMonRelativePath = "app/secure";

inline bool IsTabletDevUiSecurePathInfo(TStringBuf pathInfo) {
    if (!pathInfo.StartsWith(TabletDevUiSecurePathInfoPrefix)) {
        return false;
    }
    if (pathInfo.size() == TabletDevUiSecurePathInfoPrefix.size()) {
        return true;
    }
    return pathInfo[TabletDevUiSecurePathInfoPrefix.size()] == '/';
}

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

inline bool TabletMonDevuiReplyForbiddenUnlessSecureAdmin(const NActors::TActorContext& ctx,
        const NActors::TActorId& httpSender,
        const NActors::NMon::TEvRemoteHttpInfo* msg,
        TStringBuf pathInfo,
        bool isRelaxedRoute) {
    if (isRelaxedRoute) {
        return false;
    }
    if (!IsTabletDevUiSecurePathInfo(pathInfo) || !TabletMonRequestIsAdministrator(ctx, msg)) {
        ctx.Send(httpSender, new NActors::NMon::TEvRemoteBinaryInfoRes(NMonitoring::HTTPFORBIDDEN));
        return true;
    }
    return false;
}

} // namespace NKikimr
