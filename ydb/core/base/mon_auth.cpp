#include "mon_auth.h"

#include "appdata.h"
#include "auth.h"
#include "tablet_types.h"

#include <util/string/builder.h>

#include <algorithm>
#include <array>

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

bool UsesTabletDevUiSecurePath(const TAppData* appData, TTabletTypes::EType type) {
    if (!appData) {
        return false;
    }
    // Tablets that use the `/app/secure` DevUI path.
    constexpr std::array tabletTypes = {
        TTabletTypes::DataShard,
        TTabletTypes::Hive,
        TTabletTypes::GraphShard,
    };

    return std::find(tabletTypes.begin(), tabletTypes.end(), type) != tabletTypes.end()
        && appData->FeatureFlags.GetEnableTabletDevUiSecurePath();
}

bool IsTabletDevUiAccessAllowed(
    const TAppData* appData,
    bool securePathMode,
    TStringBuf pathInfo,
    const TString& userToken,
    bool isPublicRequest)
{
    if (!securePathMode || isPublicRequest) {
        return true;
    }
    // Mutating handler requires BOTH `/app/secure` path (CGI dispatch can't bypass via `/app/`)
    // AND administrator token (administration_allowed_sids).
    return IsTabletDevUiSecurePath(pathInfo) && IsAdministrator(appData, userToken);
}

} // namespace NKikimr
