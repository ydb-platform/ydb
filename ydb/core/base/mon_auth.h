#pragma once

#include "tablet_types.h"

#include <util/generic/strbuf.h>

namespace NActors {
struct TActorId;
} // namespace NActors

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

// True if the tablet type may serve DevUI via `/app/secure` for the current request.
// All-or-nothing tablets (DataShard, Hive) are always enabled
// Whitelist tablets require EnableTabletDevUiSecurePath.
bool UsesTabletDevUiSecurePath(TTabletTypes::EType type, bool enableSecurePathFlag);

// Whitelist model: when secure-path mode is on, requests require `/app/secure` and admin.
bool CheckTabletDevUiAccess(
    const TAppData* appData,
    bool securePathMode,
    TStringBuf pathInfo,
    const TString& userToken,
    bool isPublicRequest,
    const NActors::TActorId& sender);

} // namespace NKikimr
