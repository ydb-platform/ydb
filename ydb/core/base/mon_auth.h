#pragma once

#include "tablet_types.h"

#include <util/generic/strbuf.h>

namespace NKikimr {

struct TAppData;

// Access model for mutating tablet DevUI requests under `/tablets/app/`:
//   * operations that require administrator-level access
//     (`administration_allowed_sids`) are exposed under `/tablets/app/secure/`;
//   * because tablet monitoring dispatches these requests by query parameters,
//     protected handlers must also verify that they were invoked via the
//     secure subpath and reject calls coming through plain `/tablets/app/`.

// Relative path under the tablet mon page used in HTML links to the secure DevUI path.
inline constexpr TStringBuf TABLET_DEV_UI_SECURE_MON_RELATIVE_PATH = "app/secure";

// True if `pathInfo` is exactly `/app/secure` or starts with `/app/secure/`.
bool IsTabletDevUiSecurePath(TStringBuf pathInfo);

// True if the tablet type uses `/app/secure` and EnableTabletDevUiSecurePath is set.
bool UsesTabletDevUiSecurePath(const TAppData* appData, TTabletTypes::EType type);

// Check, caller must reply with HTTPFORBIDDEN when false.
bool IsTabletDevUiAccessAllowed(
    const TAppData* appData,
    bool securePathMode,
    TStringBuf pathInfo,
    const TString& userToken,
    bool isPublicRequest);

} // namespace NKikimr
