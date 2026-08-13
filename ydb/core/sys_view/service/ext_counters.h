#pragma once

#include <ydb/library/actors/core/actor.h>

#include <optional>

namespace NKikimr {
namespace NSysView {

struct TExtCountersConfig {
    struct TPool {
        TString Name;
        ui32 ThreadCount = 0;
        std::optional<ui32> PlacementGroupId;
    };
    TVector<TPool> Pools;
};

NActors::IActor* CreateExtCountersUpdater(TExtCountersConfig&& config);

}
}
