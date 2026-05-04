#pragma once

#include <util/generic/strbuf.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/protos/actors.pb.h>

namespace NKikimr {

// Relative path under the tablet mon page used in HTML links to the secure DevUI path.
inline constexpr TStringBuf TABLET_DEV_UI_SECURE_MON_RELATIVE_PATH = "app/secure";

// True if `pathInfo` starts with `/app/secure` or `/app/secure/`.
bool IsTabletDevUiSecurePath(TStringBuf pathInfo);

bool HasAdminAccessToTabletMon(const NActors::TActorContext& ctx, const NActors::NMon::TEvRemoteHttpInfo* msg);

// Sends HTTP 403 and returns true when a secure tablet DevUI path is requested without admin access.
bool TabletMonDevUIReplyForbiddenUnlessSecureAdmin(const NActors::TActorContext& ctx,
    const NActors::TActorId& httpSender,
    const NActors::NMon::TEvRemoteHttpInfo* msg,
    TStringBuf pathInfo);

} // namespace NKikimr
