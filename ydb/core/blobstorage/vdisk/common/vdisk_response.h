#pragma once
#include "defs.h"
#include "vdisk_mongroups.h"

namespace NKikimr {

void SendVDiskResponse(const TActorContext &ctx, const TActorId &recipient, IEventBase *ev, ui64 cookie, const TString& vDiskLogPrefix, const std::shared_ptr<NMonGroup::TOutOfSpaceGroup>& monGroup);

void SendVDiskResponse(const TActorContext &ctx, const TActorId &recipient, IEventBase *ev, ui64 cookie, ui32 channel, const TString& vDiskLogPrefix, const std::shared_ptr<NMonGroup::TOutOfSpaceGroup>& monGroup);

}//NKikimr
