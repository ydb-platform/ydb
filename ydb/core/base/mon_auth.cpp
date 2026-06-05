#include "mon_auth.h"

#include "auth.h"
#include "tablet_types.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/mon.h>

#include <util/generic/hashset.h>
#include <util/string/builder.h>

namespace NKikimr {

namespace {

const TString TABLET_DEV_UI_SECURE_PATH_INFO_PREFIX = TStringBuilder() << "/" << TABLET_DEV_UI_SECURE_MON_RELATIVE_PATH;

// All-or-nothing tablets: /app/secure routing is always enabled
// With the feature flag on the handler requires admin for every request (no public whitelist on /app).
bool IsAllOrNothingTabletDevUiSecurePath(TTabletTypes::EType type) {
    return type == TTabletTypes::DataShard
        || type == TTabletTypes::Hive;
}

// Whitelist tablets: /app/secure routing is enabled only with EnableTabletDevUiSecurePath.
const THashSet<TTabletTypes::EType> TABLET_TYPES_WITH_FLAGGED_SECURE_DEV_UI_PATH = {
    TTabletTypes::PersQueue,
};

} // namespace

bool IsTabletDevUiSecurePath(TStringBuf pathInfo) {
    if (pathInfo == TABLET_DEV_UI_SECURE_PATH_INFO_PREFIX) {
        return true;
    }
    return pathInfo.StartsWith(TABLET_DEV_UI_SECURE_PATH_INFO_PREFIX + "/");
}

bool UsesTabletDevUiSecurePath(TTabletTypes::EType type, bool enableSecurePathFlag) {
    if (IsAllOrNothingTabletDevUiSecurePath(type)) {
        return true;
    }
    return enableSecurePathFlag && TABLET_TYPES_WITH_FLAGGED_SECURE_DEV_UI_PATH.contains(type);
}

bool CheckTabletDevUiAccess(
    const TAppData* appData,
    bool securePathMode,
    TStringBuf pathInfo,
    const TString& userToken,
    bool isPublicRequest,
    const NActors::TActorId& sender)
{
    if (!securePathMode || isPublicRequest) {
        return true;
    }
    if (!IsTabletDevUiSecurePath(pathInfo) || !IsAdministrator(appData, userToken)) {
        TActivationContext::Send(new IEventHandle(sender, NActors::TActorId(), new NMon::TEvRemoteBinaryInfoRes(NMonitoring::HTTPFORBIDDEN)));
        return false;
    }
    return true;
}

} // namespace NKikimr
