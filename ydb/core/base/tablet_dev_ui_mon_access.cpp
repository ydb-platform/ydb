#include "tablet_dev_ui_mon_access.h"
#include "auth.h"
#include "appdata.h"

namespace NKikimr {

namespace {

inline constexpr TStringBuf TABLET_DEV_UI_SECURE_PATH_INFO_PREFIX = "/app/secure";

} // namespace

bool IsTabletDevUiSecurePathInfo(TStringBuf pathInfo) {
    if (!pathInfo.SkipPrefix(TABLET_DEV_UI_SECURE_PATH_INFO_PREFIX)) {
        return false;
    }
    return pathInfo.empty() || pathInfo[0] == '/';
}

bool IsAdministratorForTabletMonHttp(const NActors::TActorContext& ctx, const NActors::NMon::TEvRemoteHttpInfo* msg)
{
    if (!msg) {
        return IsAdministrator(AppData(ctx), nullptr);
    }
    if (const auto* ext = msg->ExtendedQuery.get()) {
        return IsAdministrator(AppData(ctx), ext->GetUserToken());
    }
    return IsAdministrator(AppData(ctx), nullptr);
}

bool TabletMonDevUIReplyForbiddenUnlessSecureAdmin(const NActors::TActorContext& ctx,
    const NActors::TActorId& httpSender,
    const NActors::NMon::TEvRemoteHttpInfo* msg,
    TStringBuf pathInfo,
    bool isRelaxedRoute)
{
    if (isRelaxedRoute) {
        return false;
    }
    if (!IsTabletDevUiSecurePathInfo(pathInfo) || !IsAdministratorForTabletMonHttp(ctx, msg)) {
        ctx.Send(httpSender, new NActors::NMon::TEvRemoteBinaryInfoRes(NMonitoring::HTTPFORBIDDEN));
        return true;
    }
    return false;
}

} // namespace NKikimr
