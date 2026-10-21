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

bool IsTabletDevUiSecurePath(const TStringBuf pathInfo) {
    if (pathInfo == TABLET_DEV_UI_SECURE_PATH_INFO_PREFIX) {
        return true;
    }
    return pathInfo.StartsWith(TABLET_DEV_UI_SECURE_PATH_INFO_PREFIX + "/");
}

bool HasTabletDevUiSecureSubtree(const TAppData* appData, TTabletTypes::EType type) {
    if (!appData) {
        return false;
    }
    // Tablets that expose an `/app/secure` DevUI subtree.
    constexpr std::array tabletTypes = {
        TTabletTypes::DataShard,
        TTabletTypes::Hive,
        TTabletTypes::GraphShard,
        TTabletTypes::BSController,
        TTabletTypes::PersQueue,
        TTabletTypes::SchemeShard,
    };

    return std::find(tabletTypes.begin(), tabletTypes.end(), type) != tabletTypes.end()
        && appData->FeatureFlags.GetEnableTabletDevUiSecurePath();
}

bool IsTabletDevUiAppPageAdminOnly(const TAppData* appData, TTabletTypes::EType type) {
    if (!appData) {
        return false;
    }

    constexpr std::array tabletTypes = {
        TTabletTypes::DataShard,
        TTabletTypes::BSController,
        TTabletTypes::Hive,
    };

    return appData->FeatureFlags.GetEnableTabletDevUiSecurePath() &&
           std::find(tabletTypes.begin(), tabletTypes.end(), type) != tabletTypes.end();
}

bool IsTabletDevUiAccessAllowed(
    const TAppData* appData,
    TStringBuf pathInfo,
    const TString& userToken,
    bool isMonitoringDevUiRequest)
{
    if (!appData->FeatureFlags.GetEnableTabletDevUiSecurePath() || isMonitoringDevUiRequest) {
        return true;
    }
    // Mutating handler requires BOTH `/app/secure` path (CGI dispatch can't bypass via `/app/`)
    // AND administrator token (administration_allowed_sids).
    return IsTabletDevUiSecurePath(pathInfo) && IsAdministrator(appData, userToken);
}

} // namespace NKikimr
