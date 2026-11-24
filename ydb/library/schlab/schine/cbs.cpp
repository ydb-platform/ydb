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

TCbs::TCbs(TCbs&& other) noexcept
    : TCbs()
{
    Swap(other);
}

TCbs& TCbs::operator=(TCbs&& other) noexcept {
    if (this != &other) {
        TCbs moved(std::move(other));
        Swap(moved);
    }
    return *this;
}

void TCbs::Swap(TCbs& other) noexcept {
    Jobs.Swap(other.Jobs);
    std::swap(CbsIdx, other.CbsIdx);
    std::swap(CbsNameIdx, other.CbsNameIdx);
    std::swap(CbsName, other.CbsName);
    std::swap(MaxBudget, other.MaxBudget);
    std::swap(CbsDeadline, other.CbsDeadline);
    std::swap(State, other.State);
    std::swap(CurBudget, other.CurBudget);
    std::swap(Period, other.Period);
    std::swap(LastSeqNo, other.LastSeqNo);
    std::swap(Weight, other.Weight);
    std::swap(JobsSize, other.JobsSize);
    std::swap(JobsCost, other.JobsCost);
    std::swap(OwnerIdx, other.OwnerIdx);
    std::swap(GateIdx, other.GateIdx);
    std::swap(IsNew, other.IsNew);
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
    Y_ABORT_UNLESS(job->Empty());
    job->Ref(); // The queue owns one reference until PopJob or DeleteJobs.
    Jobs.PushBack(job.Get());
    JobsSize++;
    JobsCost += job->Cost;
}

TIntrusivePtr<TJob> TCbs::PeekTailJob() {
    if (Jobs.Empty()) {
        return nullptr;
    }
    return Jobs.Back();
}


TIntrusivePtr<TJob> TCbs::PopJob() {
    Y_ABORT_UNLESS(!Jobs.Empty());
    TIntrusivePtr<TJob> job(Jobs.PopFront(), TIntrusivePtr<TJob>::TNoIncrement{});
    JobsSize--;
    JobsCost -= job->Cost;
    return job;
}



void TCbs::DeleteJobs() {
    while (!Jobs.Empty()) {
        Jobs.PopFront()->UnRef();
    }
    JobsCost = 0;
    JobsSize = 0;
}

} // NSchLab
} // NKikimr
