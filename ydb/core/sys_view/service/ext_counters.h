#pragma once

#include <library/cpp/actors/core/actor.h>

namespace NKikimr {
namespace NSysView {

struct TExtCountersConfig {
    struct TPool {
        TString Name;
        ui32 ThreadCount = 0;
    };
    TVector<TPool> Pools;
};

NActors::IActor* CreateExtCountersUpdater(TExtCountersConfig&& config);

}
}
