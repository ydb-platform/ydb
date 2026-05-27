#include "mon_auth.h"

#include <util/string/builder.h>

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

} // namespace NKikimr
