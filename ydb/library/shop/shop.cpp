#include "shop.h"
#include "probes.h"

#include <util/generic/utility.h>
#include <util/system/yassert.h>

namespace NShop {

LWTRACE_USING(SHOP_PROVIDER);

TOp::TOp(const TStage* stage)
    : DepsLeft(stage->Depends.size())
{}

const char* TOp::StateName() const
{
    switch (State) {
    case EState::Wait: return "Wait";
    case EState::Proc: return "Proc";
    case EState::Done: return "Done";
    case EState::Fail: return "Fail";
    case EState::Skip: return "Skip";
    }
    Y_ABORT("Unexpected");
}

size_t TFlow::AddStage(std::initializer_list<size_t> depends)
{
    Stage.emplace_back();
    size_t sid = Stage.size() - 1;
    TStage& desc = Stage.back();
    desc.MachineId = sid;
    desc.Depends.reserve(depends.size());
    for (size_t idx : depends) {
        desc.Depends.push_back(idx);
        Y_ABORT_UNLESS(idx < Stage.size());
        Stage[idx].Blocks.push_back(sid);
    }
    return sid;
}

size_t TFlow::AddStage(ui64 machineId, std::initializer_list<size_t> depends)
{
    ui64 sid = AddStage(depends);
    Stage[sid].MachineId = machineId;
    return sid;
}

TString TFlow::DebugDump() const
{
    TStringStream ss;
    ss << "=== TFlow ===" << Endl;
    ss << "Name:" << Name << Endl;
    size_t sid = 0;
    for (const TStage& stage : Stage) {
        ss << sid++ << ") Name:" << stage.Name
           << " MachineId:" << stage.MachineId
           << " Blocks:[";
        for (auto x : stage.Blocks) {
            ss << " " << x;
        }
        ss << " ] Depends:[";
        for (auto x : stage.Depends) {
            ss << " " << x;
        }
        ss << " ]" << Endl;
    }
    return ss.Str();
}

void TJob::AddOp(ui64 estcost)
{
    Y_ABORT_UNLESS(Op.size() < Flow->StageCount());
    Op.emplace_back(Flow->GetStage(Op.size()));
    TOp& op = Op.back();
    op.EstCost = estcost;
}

TString TJob::DebugDump() const
{
    TStringStream ss;
    if (Flow) {
        ss << Flow->DebugDump() << Endl;
    } else {
        ss << "Flow:nullptr" << Endl;
    }
    ss << "=== TJob ===" << Endl;
    ss << "JobId:" << JobId << Endl;
    ss << "OpsLeft:" << OpsLeft << Endl;
    ss << "OpsInFly:" << OpsInFly << Endl;
    ss << "Failed:" << Failed << Endl;
    ss << "StartTs:" << StartTs << Endl;
    size_t sid = 0;
    for (const TOp& op : Op) {
        ss << sid++ << ")"
           << " DepsLeft:" << op.DepsLeft
           << " StartTs:" << op.StartTs
           << " State:" << op.StateName()
           << Endl;
    }
    return ss.Str();
}

void TShop::StartJob(TJob* job, double now)
{
    Y_ABORT_UNLESS(job->Flow);
    Y_ABORT_UNLESS(job->Op.size() == job->Flow->StageCount(), "opSize:%lu != flowSize:%lu",
             job->Op.size(), job->Flow->StageCount());

    job->JobId = ++LastJobId;
    job->OpsLeft = job->Op.size();
    job->StartTs = GetCycleCount();
    LWPROBE(StartJob, Name, job->Flow->Name, job->JobId);

    RunOperation(job, 0, now);
}

void TShop::CancelJob(TJob* job)
{
    job->Failed = true;
    job->Canceled = true;
    LWPROBE(CancelJob, Name, job->Flow->Name, job->JobId);
}

void TShop::JobFinished(TJob* job)
{
    job->FinishTs = GetCycleCount();
    job->Duration = Duration(job->StartTs, job->FinishTs);
    LWPROBE(JobFinished, Name, job->Flow->Name, job->JobId, CyclesToMs(job->Duration));
}

void TShop::SkipOperation(TJob* job, size_t sid)
{
    // Mark operation to be skipped
    job->GetOp(sid)->State = TOp::EState::Skip;
    LWPROBE(SkipOperation, Name, job->Flow->Name, job->JobId, sid,
             job->Flow->GetStage(sid)->MachineId);
    OnOperationAbort(job, sid);
}

// Returns true if job has been finished
bool TShop::OperationFinished(TJob* job, size_t sid, ui64 realcost, bool success, double now)
{
    // Monitoring
    TOp* op = job->GetOp(sid);
    op->FinishTs = GetCycleCount();
    op->Duration = Duration(op->StartTs, op->FinishTs);

    // Change state
    job->OpsInFly--;
    job->OpsLeft--;
    LWPROBE(OperationFinished, Name, job->Flow->Name, job->JobId, sid,
             job->Flow->GetStage(sid)->MachineId,
             realcost, success, CyclesToMs(op->Duration));
    if (op->State != TOp::EState::Skip) {
        Y_ABORT_UNLESS(op->State == TOp::EState::Proc);
        op->State = success? TOp::EState::Done : TOp::EState::Fail;
        op->Machine->Count(op);
        if (!success) {
            job->Failed = true;
        }
        OnOperationFinished(job, sid, realcost, now);
    } else {
        op->Machine->Count(op);
    }

    if (!job->Failed) {
        if (job->OpsLeft == 0) {
            JobFinished(job);
            return true;
        } else {
            // Run dependent operations
            for (size_t i : job->Flow->GetStage(sid)->Blocks) {
                if (--job->GetOp(i)->DepsLeft == 0) {
                    if (RunOperation(job, i, now)) {
                        return true;
                    }
                }
            }
        }
    } else {
        // Finish failed/canceled job immediately if all in fly ops are finished
        // and don't start any other operations for this job
        if (job->OpsInFly == 0) {
            for (size_t op2Idx = 0; op2Idx < job->Op.size(); op2Idx++) {
                if (job->GetOp(op2Idx)->State == TOp::EState::Wait) {
                    OnOperationAbort(job, op2Idx);
                }
            }
            JobFinished(job);
            return true;
        } else {
            return false; // In fly ops remain -- wait for them to finish
        }
    }
    Y_ABORT_UNLESS(job->OpsInFly > 0, "no more ops in flight\n%s", job->DebugDump().data());
    return false;
}

// Returns true if job has been finished (3 reasons: skip/cancel/done)
bool TShop::RunOperation(TJob* job, size_t sid, double now)
{
    job->OpsInFly++;

    Y_ABORT_UNLESS(job->Flow);
    TOp::EState state = job->GetOp(sid)->State;
    if (state != TOp::EState::Skip) {
        Y_ABORT_UNLESS(job->GetOp(sid)->State == TOp::EState::Wait);
        job->GetOp(sid)->State = TOp::EState::Proc;
        if (TMachine* machine = GetMachine(job, sid)) {
            return machine->StartOperation(job, sid);
        } else {
            // Required machine is not available; job failed
            LWPROBE(NoMachine, Name, job->Flow->Name, job->JobId, sid,
                     job->Flow->GetStage(sid)->MachineId);
            return OperationFinished(job, sid, 0, false, now);
        }
    } else {
        return OperationFinished(job, sid, 0, true, now);
    }
}

TMachine::TMachine(TShop* shop)
    : Shop(shop)
{}

bool TMachine::StartOperation(TJob* job, size_t sid)
{
    LWPROBE(StartOperation, Shop->GetName(), job->Flow->GetName(), job->JobId, sid,
             job->Flow->GetStage(sid)->MachineId,
             job->GetOp(sid)->EstCost);
    TOp* op = job->GetOp(sid);
    op->StartTs = GetCycleCount();
    op->Machine = this;
    return false;
}

void TMachine::Count(const TOp* op)
{
    if (Counters) {
        Counters->Count(op);
    }
}

void TProcMonCounters::Count(const TOp* op)
{
#define INC_COUNTER(name) Counters.Set##name(Counters.Get##name() + 1)
    if (op->State == TOp::EState::Done) {
        INC_COUNTER(Done);
    } else if (op->State == TOp::EState::Fail) {
        INC_COUNTER(Failed);
    } else if (op->State == TOp::EState::Skip) {
        INC_COUNTER(Aborted);
    }
    double timeMs = CyclesToMs(op->Duration);
    if (timeMs <= 30.0) {
        if (timeMs <= 3.0) {
            if (timeMs <= 1.0) {
                INC_COUNTER(Time1ms);
            } else {
                INC_COUNTER(Time3ms);
            }
        } else {
            if (timeMs <= 10.0) {
                INC_COUNTER(Time10ms);
            } else {
                INC_COUNTER(Time30ms);
            }
        }
    } else if (timeMs <= 3000.0) {
        if (timeMs <= 300.0) {
            if (timeMs <= 100.0) {
                INC_COUNTER(Time100ms);
            } else {
                INC_COUNTER(Time300ms);
            }
        } else {
            if (timeMs <= 1000.0) {
                INC_COUNTER(Time1000ms);
            } else {
                INC_COUNTER(Time3000ms);
            }
        }
    } else {
        if (timeMs <= 30000.0) {
            if (timeMs <= 10000.0) {
                INC_COUNTER(Time10000ms);
            } else {
                INC_COUNTER(Time30000ms);
            }
        } else {
            if (timeMs <= 100000.0) {
                INC_COUNTER(Time100000ms);
            } else {
                INC_COUNTER(TimeGt100000ms);
            }
        }
    }
#undef INC_COUNTER
}

void TMachineCtl::Transition(TFlowCtl::EStateTransition st)
{
    switch (st) {
    case TFlowCtl::None: break;
    case TFlowCtl::Closed: Freeze(); break;
    case TFlowCtl::Opened: Unfreeze(); break;
    }
}

void TShopCtl::Configure(TMachineCtl* ctl, const TFlowCtlConfig& cfg, double now)
{
    ctl->Transition(ctl->GetFlowCtl()->ConfigureST(cfg, now));
}

void TShopCtl::ArriveJob(TJob* job, double now)
{
    // Flow control arrive for all operations
    for (size_t sid = 0; sid < job->Op.size(); sid++) {
        ArriveJobStage(job, sid, now);
    }
}

void TShopCtl::ArriveJobStage(TJob* job, size_t sid, double now)
{
    TOp* op = job->GetOp(sid);
    if (TMachineCtl* ctl = GetMachineCtl(job, sid)) {
        ctl->Transition(ctl->GetFlowCtl()->ArriveST(*op, op->EstCost, now));
    } else {
        // OpId == 0 is marker for disabled flow control
        Y_ABORT_UNLESS(op->OpId == 0, "operation not marked with OpId=0");
    }
}

void TShopCtl::DepartJobStage(TJob* job, size_t sid, ui64 realcost, double now)
{
    if (TMachineCtl* ctl = GetMachineCtl(job, sid)) {
        Depart(ctl, job->GetOp(sid), realcost, now);
    }
}

void TShopCtl::Depart(TMachineCtl* ctl, TFcOp* op, ui64 realcost, double now)
{
    if (op->HasArrived()) {
        ctl->Transition(ctl->GetFlowCtl()->DepartST(*op, realcost, now));
    }
}

void TShopCtl::AbortJobStage(TJob* job, size_t sid)
{
    if (TMachineCtl* ctl = GetMachineCtl(job, sid)) {
        Abort(ctl, job->GetOp(sid));
    }
}

void TShopCtl::Abort(TMachineCtl* ctl, TFcOp* op)
{
    if (op->HasArrived()) {
        ctl->Transition(ctl->GetFlowCtl()->AbortST(*op));
    }
}

}
