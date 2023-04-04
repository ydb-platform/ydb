#pragma once
#include "config.h"
#include <library/cpp/actors/core/actorid.h>

namespace NKikimr::NConveyor {

class TServiceOperator {
private:
    std::atomic<bool> IsEnabledFlag = false;
public:
    static bool IsEnabled();
    static void Register(const TConfig& serviceConfig);
};

NActors::TActorId MakeServiceId(const ui32 nodeId);

}
