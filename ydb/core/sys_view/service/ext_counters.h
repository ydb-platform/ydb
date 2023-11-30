#pragma once

#include <ydb/library/actors/core/actor.h>

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
