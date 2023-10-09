#include "cbs.h"

namespace NKikimr {
namespace NSchLab {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TCbs

TCbs::TCbs() {
    Reset(InvalidIdx, 0, 0);
}

TCbs::~TCbs() {
    DeleteJobs();
}

void TCbs::Reset(ui64 cbsIdx, ui8 ownerIdx, ui8 gateIdx) {
    IsNew = true;
    CbsIdx = cbsIdx;
    CbsNameIdx = 0;
    CbsName.clear();
    MaxBudget = 0;
    CbsDeadline = 0;
    State = CbsStateIdle;
    CurBudget = 0;
    Period = 0;
    LastSeqNo = 0;
    Weight = 1;
    OwnerIdx = ownerIdx;
    GateIdx = gateIdx;
    DeleteJobs();
}


void TCbs::PushJob(TIntrusivePtr<TJob> &job) {
    Y_ABORT_UNLESS(job);
    
    LastSeqNo++;
    job->SeqNo = LastSeqNo;
    job->CbsIdx = CbsIdx;
    Jobs.push_back(job);
    JobsSize++;
    JobsCost += job->Cost;
}

TIntrusivePtr<TJob> TCbs::PeekTailJob() {
    if (Jobs.empty()) {
        return nullptr;
    }
    return Jobs.back();
}

TIntrusivePtr<TJob> TCbs::PeekJob() {
    Y_ABORT_UNLESS(!Jobs.empty());
    if (Jobs.empty()) {
        return nullptr;
    }
    return Jobs.front();
}

TIntrusivePtr<TJob> TCbs::PopJob() {
    Y_ABORT_UNLESS(!Jobs.empty());
    TIntrusivePtr<TJob> job = Jobs.front();
    JobsSize--;
    JobsCost -= job->Cost;
    Jobs.pop_front();
    return job;
}

bool TCbs::IsEmpty() {
    return Jobs.empty();
}

bool TCbs::IsPresent() {
    return CbsIdx != InvalidIdx;
}

void TCbs::DeleteJobs() {
    Jobs.clear();
    JobsCost = 0;
    JobsSize = 0;
}

} // NSchLab
} // NKikimr
