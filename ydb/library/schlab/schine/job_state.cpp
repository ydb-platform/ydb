#include "job_state.h"

namespace NKikimr {
namespace NSchLab {

const char* JobStateName(EJobState state) {
    switch(state) {
        case JobStatePending:
            return "PENDING";
        case JobStateRunning:
            return "RUNNING";
        case JobStateDone:
            return "DONE";
        default:
            return "UNKNOWN";
    }
}

} // NSchLab
} // NKikimr
