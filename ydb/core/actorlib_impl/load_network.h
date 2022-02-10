#pragma once

#include <library/cpp/actors/core/actorsystem.h>
#include <ydb/core/base/appdata.h>

namespace IC_Load {
    void InitializeService(NActors::TActorSystemSetup* setup,
                           const NKikimr::TAppData* appData,
                           ui32 totalNodesCount);
}
