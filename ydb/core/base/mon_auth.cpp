#include "appdata.h"
#include "auth.h"
#include "mon_auth.h"

namespace NKikimr {

namespace {

const TString TABLET_DEV_UI_SECURE_PATH_INFO_PREFIX = TStringBuilder() << "/" << TABLET_DEV_UI_SECURE_MON_RELATIVE_PATH;

} // namespace

bool IsTabletDevUiSecurePath(TStringBuf pathInfo) {
    if (pathInfo == TABLET_DEV_UI_SECURE_PATH_INFO_PREFIX) {
        return true;
    }
    return pathInfo.StartsWith(TABLET_DEV_UI_SECURE_PATH_INFO_PREFIX + "/");
}

bool HasAdminAccessToTabletMon(const NActors::TActorContext& ctx, const NActors::NMon::TEvRemoteHttpInfo* msg) {
    const TString userToken = msg->GetUserToken();
    if (userToken.empty()) {
        return false;
    }

    return IsAdministrator(AppData(ctx), userToken);
}

} // namespace NKikimr
