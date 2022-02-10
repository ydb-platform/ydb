#include "cbs_state.h"

namespace NKikimr {
namespace NSchLab {

const char* CbsStateName(ECbsState state) {
    switch(state) {
        case CbsStateIdle:
            return "IDLE";
        case CbsStateActive:
            return "ACTIVE";
        case CbsStateRunning:
            return "RUNNING";
        case CbsStateDepleted:
            return "DEPLETED";
        case CbsStateDepletedGrub:
            return "ACTIVE";
        default:
            return "UNKNOWN";
    }
}

} // NSchLab
} // NKikimr
