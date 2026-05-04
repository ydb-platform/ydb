#include "tablet_dev_ui_mon_access.h"
#include "auth.h"
#include "appdata.h"

namespace NKikimr {

namespace {

const TString TABLET_DEV_UI_SECURE_PATH_INFO_PREFIX = TStringBuilder() << "/" << TABLET_DEV_UI_SECURE_MON_RELATIVE_PATH;

} // namespace

bool IsTabletDevUiSecurePath(TStringBuf pathInfo) {
    if (!pathInfo.SkipPrefix(TABLET_DEV_UI_SECURE_PATH_INFO_PREFIX)) {
        return false;
    }
    return pathInfo.empty() || pathInfo[0] == '/';
}

bool HasAdminAccessToTabletMon(const NActors::TActorContext& ctx, const NActors::NMon::TEvRemoteHttpInfo* msg)
{
    const auto* ext = msg ? msg->ExtendedQuery.get() : nullptr;
    if (!ext) {
        return false;
    }
    return IsAdministrator(AppData(ctx), ext->GetUserToken());
}

bool TabletMonDevUIReplyForbiddenUnlessSecureAdmin(const NActors::TActorContext& ctx,
    const NActors::TActorId& httpSender,
    const NActors::NMon::TEvRemoteHttpInfo* msg,
    TStringBuf pathInfo)
{
    if (!IsTabletDevUiSecurePath(pathInfo) || !HasAdminAccessToTabletMon(ctx, msg)) {
        ctx.Send(httpSender, new NActors::NMon::TEvRemoteBinaryInfoRes(NMonitoring::HTTPFORBIDDEN));
        return true;
    }
    return false;
}

} // namespace NKikimr
