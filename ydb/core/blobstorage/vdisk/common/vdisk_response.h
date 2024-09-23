#pragma once
#include "defs.h"
#include "vdisk_context.h"
#include "vdisk_mongroups.h"

namespace NKikimr {

void SendVDiskResponse(const TActorContext &ctx, const TActorId &recipient, IEventBase *ev, ui64 cookie, const TIntrusivePtr<TVDiskContext>& vCtx);

void SendVDiskResponse(const TActorContext &ctx, const TActorId &recipient, IEventBase *ev, ui64 cookie, ui32 channel, const TIntrusivePtr<TVDiskContext>& vCtx);

}//NKikimr
