#pragma once
#include "defs.h"

namespace NKikimr {
namespace NSchLab {

enum EJobState {
    JobStatePending = 0,
    JobStateRunning = 1,
    JobStateDone = 2
};

const char* JobStateName(EJobState state);

} // NSchLab
} // NKikimr
