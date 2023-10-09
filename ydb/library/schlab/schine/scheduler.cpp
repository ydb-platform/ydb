#include "scheduler.h"

#include <library/cpp/lwtrace/all.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <ydb/library/schlab/probes/probes.h>

namespace NKikimr {
namespace NSchLab {

LWTRACE_USING(SCHLAB_PROVIDER);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TScheduler

TScheduler::TScheduler()
  : Rng(234567891) {
}

void TScheduler::SetIsBinLogEnabled(bool is_enabled) {
    if (is_enabled != IsBinLogEnabled) {
        IsBinLogEnabled = is_enabled;
        if (!is_enabled) {
            SchLog.Clear();
        }
    }
}

TCbs* TScheduler::GetCbs(ui8 ownerIdx, ui8 gateIdx) {
    if (ownerIdx < Owners.size()) {
        auto &owner = Owners[ownerIdx];
        if (gateIdx < owner.Gates.size()) {
            auto &gate = owner.Gates[gateIdx];
            if (gate.CbsIdx < Cbs.size()) {
                auto *cbs = &Cbs[gate.CbsIdx];
                if (cbs->IsPresent()) {
                    return cbs;
                }
            }
        }
    }
    return nullptr;
}

void TScheduler::AddCbs(ui8 ownerIdx, ui8 gateIdx, TCbs newCbs, ui64 timeNs) {
    if (!IsProbeRegistrationDone) {
        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(SCHLAB_PROVIDER));
        IsProbeRegistrationDone = true;
    }
    if (ownerIdx >= Owners.size()) {
        Owners.resize(ownerIdx + 1);
    }
    auto &owner = Owners[ownerIdx];
    if (gateIdx >= owner.Gates.size()) {
        owner.Gates.resize(gateIdx + 1);
    }
    auto &gate = owner.Gates[gateIdx];
    Y_ABORT_UNLESS(gate.CbsIdx == TCbs::InvalidIdx,
        "AddCbs double call, ownerId# %" PRIu32 " gateIdx# %" PRIu32 " Marker# SCH01",
        (ui32)ownerIdx, (ui32)gateIdx);

    ui32 cbsIdx = NewCbs();
    auto *cbs = &Cbs[cbsIdx];
    Y_ABORT_UNLESS(!cbs->IsPresent(), "CbsIdx# %" PRIu32 " ownerId# %" PRIu32 " gateIdx# %" PRIu32 " Marker# SCH02",
        (ui32)cbsIdx, (ui32)ownerIdx, (ui32)gateIdx);

    *cbs = newCbs;
    cbs->CbsIdx = cbsIdx;
    gate.CbsIdx = cbsIdx;

    cbs->OwnerIdx = ownerIdx;
    cbs->GateIdx = gateIdx;

    cbs->CbsNameIdx = NameTable.AddName(cbs->CbsName);

    double prevWeight = TotalWeight;
    TotalWeight += cbs->Weight;

    double prevUact = Uact;
    Uact = Uact * prevWeight / TotalWeight;

    LWPROBE(AddCbs, (ui32)ownerIdx * 1000 + (ui32)gateIdx, timeNs, prevUact, Uact, cbs->Weight, TotalWeight);

    AddSchLogFrame(timeNs);
    if (IsBinLogEnabled) {
        auto &log = SchLog.Bin;
        if (!log.HasSpace(2)) {
            SchLog.IsFull = true;
        } else {
            log.Write((ui64)FrameTypeAddCbs); // FrameType
            log.Write(timeNs); // TimeStamp
            SchLogCbsKeyframeRecord(*cbs);
        }
    }

    return;
}

ui64 TScheduler::MakeNextJobId() {
    ui64 id = NextJobId;
    NextJobId++;
    return id;
}

ui32 TScheduler::NewCbs() {
    if (FirstEmptyCbs < Cbs.size()) {
        for (ui32 idx = FirstEmptyCbs; idx < Cbs.size(); ++idx) {
            if (!Cbs[idx].IsPresent()) {
                FirstEmptyCbs = idx;
                return idx;
            }
        }
    }
    Cbs.emplace_back();
    FirstEmptyCbs = Max<ui32>();
    return (Cbs.size() - 1);
}

TIntrusivePtr<TJob> TScheduler::CreateJob() {
    TIntrusivePtr<NSchLab::TJob> job = new NSchLab::TJob;
    return job;
}

void TScheduler::AddJob(TCbs *cbs, TIntrusivePtr<TJob> &job, ui8 ownerIdx, ui8 gateIdx, ui64 timeNs) {
    AddSchLogFrame(timeNs);

    bool wasEmpty = cbs->IsEmpty();
    job->Id = MakeNextJobId();
    if (job->Cost) {
        job->WeightedInverseCost = (double)cbs->Weight / (double)job->Cost;
    } else {
        job->WeightedInverseCost = (double)cbs->Weight;
    }

    cbs->PushJob(job);
    HasAtLeastOneJob = true;

    LWPROBE(AddJob, (ui32)ownerIdx * 1000 + (ui32)gateIdx, timeNs, cbs->JobsSize, cbs->JobsCost);
    if (wasEmpty) {
        EvaluateNextJob(job->CbsIdx, timeNs, false);
    }
    if (IsBinLogEnabled) {
        auto &log = SchLog.Bin;
        log.Write((ui64)FrameTypeAddJob); // FrameType
        log.Write(timeNs); // TimeStamp
        log.Write(Uact); // Uact
        log.Write(cbs->CbsIdx); // CbsIdx
        // Record JobKeyframe format (5 items):
        log.Write(job->Id); // Id
        log.Write(job->Cost); // Cost
        log.Write((ui64)job->State); // State
        log.Write(job->JobKind); // Kind
        log.Write(job->SeqNo); // SeqNo
    }
}

bool TScheduler::AddJob(TIntrusivePtr<TJob> &job, ui8 ownerIdx, ui8 gateIdx, ui64 timeNs) {
    TCbs *cbs = GetCbs(ownerIdx, gateIdx);
    if (cbs) {
        AddJob(cbs, job, ownerIdx, gateIdx, timeNs);
        return true;
    } else {
        return false;
    }
}

void TScheduler::OnJobCostChange(TCbs *cbs, TIntrusivePtr<TJob> &job, ui64 timeNs, ui64 prevCost) {
    cbs->JobsCost += job->Cost - prevCost;
    LWPROBE(ChangeJob, (ui32)cbs->OwnerIdx * 1000 + (ui32)cbs->GateIdx, timeNs, cbs->JobsSize, cbs->JobsCost);
    if (IsBinLogEnabled) {
        auto &log = SchLog.Bin;
        log.Write((ui64)FrameTypeAddJob); // FrameType
        log.Write(timeNs); // TimeStamp
        log.Write(Uact); // Uact
        log.Write(cbs->CbsIdx); // CbsIdx
        // Record JobKeyframe format (5 items):
        log.Write(job->Id); // Id
        log.Write(job->Cost); // Cost
        log.Write((ui64)job->State); // State
        log.Write(job->JobKind); // Kind
        log.Write(job->SeqNo); // SeqNo
    }
}

TIntrusivePtr<TJob> TScheduler::SelectJob(ui64 timeNs) {
    AddSchLogFrame(timeNs);
    // EDF
    bool doLog = IsBinLogEnabled;
    auto &log = SchLog.Bin;
    if (doLog) {
        log.Write((ui64)FrameTypeSelectJob); // FrameType
        log.Write(timeNs); // TimeStamp
    }
    TIntrusivePtr<TJob> bestJob = nullptr;
    double totalWeightedCost = 0.0;

    bool foundAtLeastOneJob = false;

    if (HasAtLeastOneJob) {
        const size_t cbsSize = Cbs.size();

        for (ui32 idx = 0; idx < cbsSize; ++idx) {
            TCbs &cbs = Cbs[idx];

            if (cbs.IsPresent() && !cbs.IsEmpty()) {
                foundAtLeastOneJob = true;

                totalWeightedCost += (double)cbs.Weight / (double)cbs.PeekJob()->Cost;
                if (cbs.State == CbsStateDepleted || cbs.State == CbsStateDepletedGrub) {
                    EvaluateNextJob(idx, timeNs, doLog);
                    if (cbs.State == CbsStateDepleted || cbs.State == CbsStateDepletedGrub) {
                        continue;
                    }
                }
                if (doLog) {
                    log.Write(cbs.CbsIdx); // CbsIdx
                    log.Write((ui64)cbs.State); // CbsState
                    log.Write(cbs.CbsDeadline); // CbsDeadline
                    log.Write(cbs.CurBudget); // CbsCurBudget
                }
                if (!bestJob) {
                    bestJob = cbs.PeekJob();
                } else if (cbs.PeekJob()->Deadline < bestJob->Deadline) {
                    bestJob = cbs.PeekJob();
                }
            }
        }
    }

    if (!foundAtLeastOneJob) {
        // This branch will be hit either if no jobs were found or if it was knonwn that there are no jobs.
        HasAtLeastOneJob = false;

        if (doLog) {
            log.Write(Max<ui64>()); // CbsUpdate list terminator
            
            log.Write(Max<ui64>()); // CbsIdx
            log.Write(Max<ui64>()); // CbsState
            log.Write(Max<ui64>()); // JobId
            log.Write(Max<ui64>()); // JobState
            log.Write(Uact); // Uact
        }

        return nullptr;
    }

    if (doLog) {
        log.Write(Max<ui64>()); // CbsUpdate list terminator
    }

    ui64 accountCbsIdx = TCbs::InvalidIdx;

    if (bestJob) {
        bestJob->State = JobStateRunning;
        Cbs[bestJob->CbsIdx].State = CbsStateRunning;
        accountCbsIdx = bestJob->CbsIdx;
    } else {
        double threshold = double(Rng() % (1ull << 23)) / double(1ull << 23) * totalWeightedCost;
        double acc = 0;
        TIntrusivePtr<TJob> job = nullptr;
        
        const size_t cbsSize = Cbs.size();

        for (ui32 idx = 0; idx < cbsSize; ++idx) {
            TCbs &cbs = Cbs[idx];
            if (cbs.IsPresent() && !cbs.IsEmpty()) {
                if (!bestJob) {
                    job = cbs.PeekJob();
                    acc += job->WeightedInverseCost;
                    if (acc > threshold) {
                        bestJob = job;
                    }
                }
                //if (!bestJob) {
                //    bestJob = cbs.PeekJob();
                //} else if (cbs.PeekJob()->Deadline < bestJob->Deadline) {
                //    bestJob = cbs.PeekJob();
                //}
            }
            if (cbs.State == CbsStateDepletedGrub) {
                accountCbsIdx = idx;
            }
        }
        
        if (!bestJob) {
            bestJob = job;
        }

        Y_ABORT_UNLESS(bestJob);

        if (accountCbsIdx != TCbs::InvalidIdx) {
            bestJob->State = JobStateRunning;
            bestJob->AccountCbsIdx = accountCbsIdx;
            // TODO(cthulhu): Shouldn't he AccountCbsIdx be running?
        }
    }

    if (doLog) {
        log.Write(accountCbsIdx); // CbsIdx
        log.Write((ui64)CbsStateRunning);//Cbs[accountCbsIdx].State); // CbsState
        log.Write(bestJob->Id); // JobId
        log.Write((ui64)bestJob->State); // JobState
        log.Write(Uact); // Uact
    }

    return bestJob;
}

void TScheduler::UpdateTotalWeight() {
    ui64 prevTotalWeight = TotalWeight;
    TotalWeight = 0;
    for (ui32 idx = 0; idx < Cbs.size(); ++idx) {
        TCbs &cbs = Cbs[idx];
        if (cbs.IsPresent()) {
            TotalWeight += cbs.Weight;
        }
    }
    double prevUact = Uact;
    LWPROBE(UpdateTotalWeight, prevUact, Uact, prevTotalWeight, TotalWeight);
}

void TScheduler::EvaluateNextJob(ui64 cbsIdx, ui64 timeNs, bool doLogCbsUpdate) {
    auto &log = SchLog.Bin;
    TCbs &cbs = Cbs[cbsIdx];
    TIntrusivePtr<TJob> job = cbs.PeekJob();
    i64 jobCost = job->Cost;

    // XXX - a reference to Resource Reservations for General Purpose Applications article (feb. 2009, IEEE)
    //       pages 15 and 16 (IV. HGRUB Algorithm)

    // (!!) depleted tasks also enter here
    // Depleted tasks should stay that way until time t = di
    // When this time instant arrives, the scheduling deadline is postponed (di = di + Ti)
    // and the budget is recharged to Qi (hard enforcement rule)

    if (cbs.CurBudget >= jobCost
            && cbs.CbsDeadline > timeNs + jobCost
            && cbs.State != CbsStateDepleted
            && cbs.State != CbsStateDepletedGrub) {
        // XXX Ti task is activated (bullet 2)
        // first case, current deadline and budget can be used
        // leave Uact unchanged because it already accounts for the fraction of Disk Time currently used by the task
        job->Deadline = cbs.CbsDeadline;
        if (cbs.State != CbsStateActive) {
            cbs.State = CbsStateActive;
        }
    } else {
        if (timeNs >= cbs.CbsDeadline) {
            // XXX Ti task is activated (bullet 2)
            // second case, a new deadline has to be generated using the rule di=ti+Ti, qi is recharged to Qi
            // Uact has be updated by the rule Uact = Uact + Qi/Ti to take into account the additional fraction
            // of Disk Time used by the task.
            // The condition to check if a new deadilne has to be generated is qi > (di - t)*Ui
            ui64 jobPeriod = jobCost * TotalWeight / cbs.Weight;
            cbs.CbsDeadline += jobPeriod;
            double prevUact = Uact;
            if (timeNs >= cbs.CbsDeadline + MaxDelay) {
                cbs.CbsDeadline = timeNs - MaxDelay;
            }
            if (cbs.State == CbsStateDepleted || cbs.State == CbsStateDepletedGrub || cbs.State == CbsStateIdle) {
                Uact += double(cbs.Weight) / double(TotalWeight);
                if (Uact > 1.0) {
                    Uact = 1.0;
                }
            }
            cbs.CurBudget = cbs.MaxBudget > jobCost ? cbs.MaxBudget : jobCost;
            job->Deadline = cbs.CbsDeadline;
            cbs.State = CbsStateActive;
            LWPROBE(ActivateTask, (ui32)cbs.OwnerIdx * 1000 + cbs.GateIdx,
                    timeNs, prevUact, Uact, jobCost, cbs.CurBudget, cbs.Weight, TotalWeight);
        } else {
            // When budget is exhausted (qi = 0), the server is said to be depleted and the served task is not scheduled
            // until time t = di.
            ui64 jobPeriod = jobCost * TotalWeight / cbs.Weight;
            if (timeNs >= cbs.CbsDeadline) {
                cbs.CbsDeadline = timeNs;
                cbs.CbsDeadline += jobPeriod;
            }
            job->Deadline = cbs.CbsDeadline;
            bool isTheLast = true;
            for (ui32 idx = 0; idx < Cbs.size(); ++idx) {
                if (Cbs[idx].State == CbsStateActive ||
                        Cbs[idx].State == CbsStateRunning) {
                    isTheLast = false;
                    break;
                }
            }
            ECbsState state;
            if (!isTheLast) {
                state = CbsStateDepleted;
            } else {
                state = CbsStateDepletedGrub;
            }
            if (state != cbs.State) {
                cbs.State = state;
            }
        }
    }
    if (doLogCbsUpdate) {
        // Record CbsUpdate format:
        log.Write(cbs.CbsIdx); // CbsIdx
        log.Write((ui64)cbs.State); // CbsState
        log.Write(cbs.CbsDeadline); // CbsDeadline
        log.Write(cbs.CurBudget); // CbsCurBudget
    }
}

void TScheduler::CompleteJob(ui64 timeNs, TIntrusivePtr<TJob> &job) {
    Y_ABORT_UNLESS(job);
    Y_ABORT_UNLESS(job->CbsIdx != TCbs::InvalidIdx);
    Y_ABORT_UNLESS(job->CbsIdx < Cbs.size());
    Y_ABORT_UNLESS(!Cbs[job->CbsIdx].IsEmpty());
    Y_ABORT_UNLESS(Cbs[job->CbsIdx].PeekJob() == job.Get());


    AddSchLogFrame(timeNs);
    TCbs &cbs = Cbs[job->CbsIdx];
    TCbs &account = (job->AccountCbsIdx == TCbs::InvalidIdx ? cbs : Cbs[job->AccountCbsIdx]);
    if (account.CurBudget > job->Cost || (Uact > 0.f && account.CurBudget > job->Cost * Uact)) {
        // XXX Ti is blocked
        // qi < (di - t) * Ui, di and qi can be used by future jobs of Ti
        // Uact is left unchanged, it will not be changed upon the activation (?!)
        // Uact will (?!) be decreased by Ui at time t = di - qi/Ui, indeed, beyond this instant the server will have
        // to generate a new scheduling deadline on the arrival of a new job

        // Accounting rule:
        i64 dqi = -Uact * job->Cost;
        if (dqi == 0) {
            dqi = -1;
        }
        account.CurBudget += dqi;
        if (account.CurBudget < 0) {
            account.CurBudget = 0;
        }
        LWPROBE(CompleteJobInActiveCbs, (ui32)cbs.OwnerIdx * 1000 + cbs.GateIdx,
                timeNs, Uact, Uact, job->Cost, dqi, account.CurBudget, account.Weight, TotalWeight);

        // TODO(cthulhu): Change the accounting rule:
        // while task Ti executes, the server budget is decreased as dqi = -Uact * dt (GRUB accounting rule)
        // In this way, in presence of a low Disk workload, the time accounted to the task will be lower than the one
        // it actually counsumed and the fraction of Disk Time given by Ulub - Uact is redistributed among the executing
        // tasks (this is the reclaiming mechangism).
    } else {
        // XXX Ti is blocked
        // qi >= (di - t) * Ui
        // Uact = Uact - Ui

        i64 dqi = -(i64)account.CurBudget;
        // looks like server should get depleted here (??)
        account.CurBudget = 0;
        double prevUact = Uact;
        if (account.State == CbsStateRunning) {
            Uact -= double(account.Weight) / double(TotalWeight);
            if (Uact < 0.0) {
                Uact = 0.0;
            }
            account.State = CbsStateDepleted;
        }
        LWPROBE(CompleteJobInDepletedCbs, (ui32)cbs.OwnerIdx * 1000 + cbs.GateIdx,
                timeNs, prevUact, Uact, job->Cost, dqi, account.CurBudget, account.Weight, TotalWeight);
    }
    // TODO(cthulhu): Implement the fairness preserving rule needed
    // (!!) fairness preserving rule
    // if task Ti is blocked when all the servers associated to the other tasks are depleted, server Si is not
    // immediately deactivated, but it can be used to schedule the task having the earliest scheduling deadline.
    // The server will become inactive at time t = di - (qi)/(Qi)Ti
    job->State = JobStateDone;

    auto &log = SchLog.Bin;
    if (IsBinLogEnabled) {
        log.Write((ui64)FrameTypeCompleteJob); // FrameType
        log.Write(timeNs); // TimeStamp
        log.Write(account.CbsIdx); // AccountCbsIdx
        log.Write(account.CurBudget); // AccountCurBudget
        log.Write(Uact); // Uact
        log.Write(cbs.CbsIdx); // CbsIdx
        log.Write(job->Id); // JobId
        log.Write((ui64)job->State); // JobState
    }

    cbs.PopJob();
    LWPROBE(PopJob, (ui32)cbs.OwnerIdx * 1000 + cbs.GateIdx, timeNs, cbs.JobsSize, cbs.JobsCost);
    if (cbs.IsEmpty()) {
        cbs.State = CbsStateIdle;
    } else {
        EvaluateNextJob(job->CbsIdx, timeNs, false);
    }

    if (IsBinLogEnabled) {
        auto &log = SchLog.Bin;
        log.Write((ui64)FrameTypeRemoveJob); // FrameType
        log.Write(timeNs); // TimeStamp
        // a single CbsUpdate record
        log.Write(cbs.CbsIdx); // CbsIdx
        log.Write((ui64)cbs.State); // CbsState
        log.Write(cbs.CbsDeadline); // CbsDeadline
        log.Write(cbs.CurBudget); // CbsCurBudget
    }
}

void TScheduler::AddSchLogFrame(ui64 timeNs) {
    if (!IsBinLogEnabled) {
        return;
    }
    if (!SchLog.Bin.HasSpace(65536)) {
        SchLog.IsFull = true;
    }
    if (SchLog.IsFull) {
        SchLog.Clear(); // TODO(cthulhu): Keep enough history
    }
    if (SchLog.KeyframeCount == 0) {
        SchLog.FirstKeyframeTimeStamp = timeNs;
        SchLogKeyframe(timeNs);
    }
}

void TScheduler::SchLogCbsKeyframeRecord(const TCbs &cbs) {
    // Record CbsKeyframe fromat (7+ items):
    // 7 + 5 * cbs.Jobs.size() + 1 + 1
    auto &log = SchLog.Bin;
    if (!log.HasSpace(7 + 5 * cbs.Jobs.size() + 1 + 1)) {
        SchLog.IsFull = true;
        return;
    }
    log.Write(cbs.CbsIdx); // Idx
    log.Write(cbs.CbsNameIdx); // NameIdx
    log.Write(cbs.MaxBudget); // MaxBudget
    log.Write(cbs.Period); // Period
    log.Write(cbs.CurBudget); // CurBudget
    log.Write(cbs.CbsDeadline); // Deadline
    log.Write((ui64)cbs.State); // State
    for (auto jobIt = cbs.Jobs.begin(); jobIt != cbs.Jobs.end(); ++jobIt) {
        TJob &job = **jobIt;
        // Record JobKeyframe format (5 items):
        log.Write(job.Id); // Id
        log.Write(job.Cost); // Cost
        log.Write((ui64)job.State); // State
        log.Write(job.JobKind); // Kind
        log.Write(job.SeqNo); // SeqNo
    }
    // Terminator (1 item)
    log.Write(Max<ui64>()); // Id terminator
}

void TScheduler::SchLogKeyframe(ui64 timeNs) {
    auto &log = SchLog.Bin;
    if (!log.HasSpace(1 + 3 + 1)) {
        SchLog.IsFull = true;
        return;
    }
    log.Write((ui64)FrameTypeKey); // FrameType (1+ items)
    // Frame Keyframe format (3+ items):
    // 3 + ?*(7 + 5 * cbs.Jobs.size() + 1) + 1
    log.Write(timeNs); // TimeStamp
    log.Write((ui64)0); //EpochTime
    log.Write(Uact); // Uact
    for (ui64 idx = 0; idx < Cbs.size(); ++idx) {
        TCbs &cbs = Cbs[idx];
        if (cbs.IsPresent()) {
            SchLogCbsKeyframeRecord(cbs);
        }
    }
    // Terminator
    if (log.HasSpace(1)) {
        log.Write(Max<ui64>()); // Idx terminator
    } else {
        SchLog.IsFull = true;
        return;
    }

    SchLog.KeyframeCount++;
    SchLog.LastKeyframeTimeStamp = timeNs;
    return;
}

void TScheduler::OutputLog(IOutputStream &out) {
    SchLog.Output(out, NameTable);
}

bool TScheduler::IsEmpty() {
    for (ui32 idx = 0; idx < Cbs.size(); ++idx) {
        TCbs &cbs = Cbs[idx];
        if (cbs.IsPresent() && !cbs.IsEmpty()) {
            return false;
        }
    }
    return true;
}

} // NSchLab
} // NKikimr
