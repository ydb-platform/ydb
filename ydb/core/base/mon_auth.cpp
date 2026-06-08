#include "mon_auth.h"

#include "auth.h"
#include "tablet_types.h"

#include <util/string/builder.h>

#include <array>

namespace NKikimr {

namespace {

const TString TABLET_DEV_UI_SECURE_PATH_INFO_PREFIX = TStringBuilder() << "/" << TABLET_DEV_UI_SECURE_MON_RELATIVE_PATH;

enum class ETabletDevUiSecurePathPolicy {
    AlwaysOn,   // Always uses `/app/secure`.
    FlagGated,  // Uses `/app/secure` only when EnableTabletDevUiSecurePath is set.
};

struct TTabletDevUiSecurePathRule {
    TTabletTypes::EType Type;
    ETabletDevUiSecurePathPolicy Policy;
};

// Tablets that use the `/app/secure` DevUI path with their activation policy.
constexpr std::array TABLET_DEV_UI_SECURE_PATH_RULES = {
    TTabletDevUiSecurePathRule{TTabletTypes::DataShard,  ETabletDevUiSecurePathPolicy::AlwaysOn},
    TTabletDevUiSecurePathRule{TTabletTypes::Hive,       ETabletDevUiSecurePathPolicy::AlwaysOn},
    TTabletDevUiSecurePathRule{TTabletTypes::GraphShard, ETabletDevUiSecurePathPolicy::FlagGated},
};

} // namespace

bool IsTabletDevUiSecurePath(TStringBuf pathInfo) {
    if (pathInfo == TABLET_DEV_UI_SECURE_PATH_INFO_PREFIX) {
        return true;
    }
    return pathInfo.StartsWith(TABLET_DEV_UI_SECURE_PATH_INFO_PREFIX + "/");
}

bool UsesTabletDevUiSecurePath(TTabletTypes::EType type, bool enableSecurePathFlag) {
    for (const auto& rule : TABLET_DEV_UI_SECURE_PATH_RULES) {
        if (rule.Type != type) {
            continue;
        }
        switch (rule.Policy) {
            case ETabletDevUiSecurePathPolicy::AlwaysOn:
                return true;
            case ETabletDevUiSecurePathPolicy::FlagGated:
                return enableSecurePathFlag;
        }
    }
    return false;
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
