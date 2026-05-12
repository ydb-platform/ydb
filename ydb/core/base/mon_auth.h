#pragma once

#include <util/generic/strbuf.h>

namespace NActors {
class TActorContext;
class TActorId;

namespace NMon {
struct TEvRemoteHttpInfo;
} // namespace NMon

} // namespace NActors

namespace NKikimr {

// Access model for mutating tablet DevUI requests under `/tablets/app/`:
//   * admin-only operations are exposed under `/tablets/app/secure/`;
//   * that secure subpath requires administrator-level access
//     (`administration_allowed_sids`), not just monitoring operator access
//     (`monitoring_allowed_sids`);
//   * because tablet monitoring dispatches these requests by query parameters,
//     protected handlers must also verify that they were invoked via the
//     secure subpath and reject calls coming through plain `/tablets/app/`.

// Relative path under the tablet mon page used in HTML links to the secure DevUI path.
inline constexpr TStringBuf TABLET_DEV_UI_SECURE_MON_RELATIVE_PATH = "app/secure";

// True if `pathInfo` starts with `/app/secure` or `/app/secure/`.
bool IsTabletDevUiSecurePath(TStringBuf pathInfo);

bool HasAdminAccessToTabletMon(const NActors::TActorContext& ctx, const NActors::NMon::TEvRemoteHttpInfo* msg);

// Sends HTTP 403 and returns true unless the request targets a secure
// tablet DevUI path and the caller has administrator access.
bool TabletMonDevUIReplyForbiddenUnlessSecureAdmin(const NActors::TActorContext& ctx,
    const NActors::TActorId& httpSender,
    const NActors::NMon::TEvRemoteHttpInfo* msg,
    TStringBuf pathInfo);

} // namespace NKikimr
