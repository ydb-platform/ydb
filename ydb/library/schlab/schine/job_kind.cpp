#include "job_kind.h"

namespace NKikimr {
namespace NSchLab {

const char* JobKindName(EJobKind kind) {
    switch(kind) {
        case JobKindRead:
            return "READ";
        case JobKindWrite:
            return "WRITE";
        default:
            return "REQUEST";
    }
}

}  // NSchLab
}  // NKikimr
