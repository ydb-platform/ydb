#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NSchemeShard {

NActors::IActor* CreateSchemeQueryExecutor(NActors::TActorId replyTo, ui64 importId, ui32 itemIdx, const TString& creationQuery, const TString& database);

}
