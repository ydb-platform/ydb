#pragma once

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NStat {

NActors::TActorId MakeStatServiceID(ui32 node);

}   // namespace NKikimr::NStat
