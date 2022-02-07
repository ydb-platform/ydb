#pragma once
#include "defs.h"

namespace NKikimr {
namespace NSchLab {

enum ECbsState {
    CbsStateIdle = 0,
    CbsStateActive = 1,
    CbsStateRunning = 2,
    CbsStateDepleted = 3,
    CbsStateDepletedGrub = 4
};

const char* CbsStateName(ECbsState state);

} // NSchLab
} // NKikimr
