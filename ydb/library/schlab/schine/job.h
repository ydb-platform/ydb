#pragma once
#include "defs.h"
#include "job_state.h"
#include <util/generic/ptr.h>

namespace NKikimr {
namespace NSchLab {

struct TJob : TSimpleRefCount<TJob> {
    void *Payload = nullptr;
    ui64 CbsIdx = 0;

    ui64 Id = 0;
    ui64 JobKind = 0;

    ui64 AccountCbsIdx = 65535;

    i64 Cost = 0;
    EJobState State = JobStatePending;
    ui64 SeqNo = 0;
    ui64 Deadline = 0;
    double WeightedInverseCost = 0.0;

    bool IsNew = true;
};

} // NSchLab
} // NKikimr
