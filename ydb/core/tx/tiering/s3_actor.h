#pragma once
#ifndef KIKIMR_DISABLE_S3_OPS
#include <library/cpp/actors/core/actorid.h>
#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NColumnShard {

IActor* CreateS3Actor(ui64 tabletId, const TActorId& parent, const TString& tierName);

}

#endif
