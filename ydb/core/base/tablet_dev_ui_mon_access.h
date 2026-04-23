#pragma once

#include <util/generic/strbuf.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/protos/actors.pb.h>

namespace NKikimr {

// Relative path under the tablet mon page used in HTML links to the secure DevUI subtree.
inline constexpr TStringBuf TABLET_DEV_UI_SECURE_MON_RELATIVE_PATH = "app/secure";

// True when `pathInfo` is exactly `/app/secure` or a path under it (`/app/secure/...`).
bool IsTabletDevUiSecurePathInfo(TStringBuf pathInfo);

// Cluster administrator check for a tablet monitoring HTTP request
bool IsAdministratorForTabletMonHttp(const NActors::TActorContext& ctx, const NActors::NMon::TEvRemoteHttpInfo* msg);

// Enforce "secure DevUI" access on the tablet mon app handler before running page logic.
//
// When the request targets the secure subtree (`IsTabletDevUiSecurePathInfo(pathInfo)`), requires cluster administrator
// (`IsAdministratorForTabletMonHttp`); otherwise sends HTTP 403 to `httpSender` and returns true.
//
// Return value: true  — forbidden response was sent, caller should stop handling this request;
//               false — no forbidden response (either access OK, or checks were skipped).
//
// `httpSender` — recipient for `TEvRemoteBinaryInfoRes`
// `pathInfo`   — same as `TEvRemoteHttpInfo::PathInfo()` for the tablet app mon route.
// `isRelaxedRoute` — when true, the caller opts out of this gate entirely. Must be used deliberately by tablet code.
bool TabletMonDevUIReplyForbiddenUnlessSecureAdmin(const NActors::TActorContext& ctx,
    const NActors::TActorId& httpSender,
    const NActors::NMon::TEvRemoteHttpInfo* msg,
    TStringBuf pathInfo,
    bool isRelaxedRoute);

} // namespace NKikimr
