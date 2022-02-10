#pragma once
#include "defs.h"

namespace NKikimr {
namespace NSchLab {

enum EJobKind {
    JobKindRequest = 0,
    JobKindRead = 1,
    JobKindWrite = 2
};

const char* JobKindName(EJobKind kind);

}  // NSchLab
}  // NKikimr
