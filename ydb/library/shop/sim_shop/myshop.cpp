#include "myshop.h"

#include <library/cpp/protobuf/util/is_equal.h>

#include <util/random/normal.h>
#include <util/random/random.h>

#include <util/generic/hash_set.h>

LWTRACE_DEFINE_PROVIDER(SIMSHOP_PROVIDER);

namespace NShopSim {

LWTRACE_USING(SIMSHOP_PROVIDER);

// Return random value according to distribution described by protobuf
double Random(const TDistrPb& pb)
{
    double x;
    double mean;
    if (pb.HasConst()) {
        mean = x = pb.GetConst();
    } else if (pb.HasGauss()) {
        mean = pb.GetGauss().GetMean();
        x = NormalRandom(mean, pb.GetGauss().GetDisp());
    } else if (pb.HasExp()) {
        mean = pb.GetExp().HasLambda()?
            pb.GetExp().GetLambda() : 1.0 / pb.GetExp().GetPeriod();
        x = -(1.0 / mean) * log(1 - RandomNumber<double>());
    } else if (pb.HasUniform()) {
        double from = pb.GetUniform().GetFrom();
        double to = pb.GetUniform().GetTo();
        mean = (from + to) / 2.0;
        x = from + (to - from) * RandomNumber<double>();
    }
    if (pb.HasMinAbsValue()) {
        x = Max(x, pb.GetMinAbsValue());
    }
    if (pb.HasMaxAbsValue()) {
        x = Min(x, pb.GetMaxAbsValue());
    }
    if (pb.HasMinRelValue()) {
        x = Max(x, mean * pb.GetMinRelValue());
    }
    if (pb.HasMaxAbsValue()) {
        x = Min(x, mean * pb.GetMaxRelValue());
    }
    return x;
}

// Return random value according to distribution described by protobuf
double Random(const TRandomPb& pb)
{
    double totalWeight = 0;
    for (int i = 0; i < pb.GetDistr().size(); i++) {
        double weight = pb.GetDistr(i).GetWeight();
        Y_ABORT_UNLESS(weight >= 0);
        totalWeight += weight;
    }

    double weightSum = 0;
    double p = RandomNumber<double>();
    for (int i = 0; i < pb.GetDistr().size(); i++) {
        TDistrPb distr = pb.GetDistr(i);
        double weight = distr.GetWeight();
        weightSum += weight;
        if (p < weightSum/totalWeight) {
            return Random(distr);
        }
    }
    Y_ABORT("bad weights configuration");
}

TMyShop::TMyShop()
    : SourceThread(&SourceThreadFunc, this)
    , SchedulerThread(&SchedulerThreadFunc, this)
{
    SourceThread.Start();
    SchedulerThread.Start();
}

TMyShop::~TMyShop()
{
    AtomicSet(Running, 0);
    // Explicit join to avoid troubles
    SourceThread.Join();
    SchedulerThread.Join();
}

void TMyShop::Configure(const TConfigPb& newCfg)
{
    TGuard<TMutex> g(Mutex);
    Cfg = newCfg;

    // Create map of current machines by name
    THashMap<TString, TMyMachine*> machines;
    for (const auto& kv : Machines) {
        TMyMachine* machine = kv.second.Machine.Get();
        machines[machine->GetName()] = machine;
    }

    // Create/update machines
    THashSet<TString> names;
    for (int i = 0; i < Cfg.GetMachine().size(); i++) {
        const TMachinePb& pb = Cfg.GetMachine(i);
        bool inserted = names.insert(pb.GetName()).second;
        Y_ABORT_UNLESS(inserted, "duplicate machine name '%s'", pb.GetName().data());

        TMachineItem* item;
        auto mIter = machines.find(pb.GetName());
        if (mIter != machines.end()) {
            item = &Machines.find(mIter->second->GetId())->second;
        } else {
            // Create new machine
            ui64 id = ++LastMachineId;
            item = &Machines[id];
            item->Machine.Reset(new TMyMachine(this, id));
            machines[pb.GetName()] = item->Machine.Get();
        }

        // Machine's flows must be cleared to destroy old flows
        item->Machine->ClearFlows();

        // Configure/reconfigure machine and it's flow control
        item->Machine->Configure(pb);
        auto st = item->Machine->GetFlowCtl()->ConfigureST(pb.GetFlowCtl(), Now());
        switch (st) {
        case TFlowCtl::None: break;
        case TFlowCtl::Closed: item->Machine->Freeze(); break;
        case TFlowCtl::Opened: item->Machine->Unfreeze(); break;
        }
    }

    // Remove machines that are no longer in config
    TVector<TMyMachine*> toDelete; // to avoid deadlocks
    for (const auto& kv : machines) {
        if (names.find(kv.first) == names.end()) {
            TMyMachine* machine = kv.second;
            auto mIter = Machines.find(machine->GetId());
            Y_ABORT_UNLESS(mIter != Machines.end());
            TMachineItem* item = &mIter->second;
            toDelete.push_back(item->Machine.Release());
            Machines.erase(mIter);
        }
    }

    // Create map of current flows by name
    THashMap<TString, TMyFlowPtr> flows;
    for (const THeapItem& item : SourceHeap) {
        const TMyFlowPtr& flow = item.Flow;
        flows[flow->Source.GetName()] = flow;
    }

    // All flows will be actually deleted when all their jobs are done
    SourceHeap.clear();
    Sched.Clear();

    // Create/update flows
    double now = Now();
    for (int i = 0; i < newCfg.GetSource().size(); i++) {
        TMyFlowPtr flow;
        const TSourcePb& source = newCfg.GetSource(i);
        double ia = Random(source.GetInterArrival());
        auto fIter = flows.find(source.GetName());
        if (fIter != flows.end() && NProtoBuf::IsEqual(source, fIter->second->Source)) {
            // Use already existing flow
            flow = fIter->second;
        } else {
            // Create new flow
            flow.Reset(new TMyFlow(source, &Sched));

            // Create ops with simplest linear dependencies graph
            // (i.e. sequential processing)
            size_t prevsid;
            for (int i = 0; i < source.GetOperation().size(); i++) {
                const TOperationPb& pb = source.GetOperation(i);
                auto mIter = machines.find(pb.GetMachine());
                if (mIter != machines.end()) {
                    TMyMachine* machine = mIter->second;
                    ui64 machineId = machine->GetId();
                    if (i == 0) {
                        prevsid = flow->AddStage(machineId, {});
                    } else {
                        prevsid = flow->AddStage(machineId, {prevsid});
                    }

                    // Freeze control
                    machine->AddFlow(flow);
                    if (machine->IsFrozen()) {
                        flow->IncFrozenMachine();
                    }
                } else {
                    Y_ABORT("unknown machine %s", pb.GetMachine().data());
                }
            }
        }
        PushSource(now + ia, flow);
        if (!flow->Empty()) {
            flow->Activate();
        }
    }

    // Delete machines after lock release to avoid deadlock w/ wait-thread
    g.Release();
    for (TMyMachine* machine : toDelete) {
        delete machine; // this will join thread that can lock `Mutex'
    }
}

void TMyShop::OperationFinished(TMyJob* job, size_t sid, ui64 realcost, bool success)
{
    TGuard<TMutex> g(Mutex);
    TShop::OperationFinished(job, sid, realcost, success, Now());
}

TMyMachine* TMyShop::GetMachine(ui64 machineId)
{
    TGuard<TMutex> g(Mutex);
    auto iter = Machines.find(machineId);
    if (iter != Machines.end()) {
        return iter->second.Machine.Get();
    } else {
        return nullptr;
    }
}

TMachine* TMyShop::GetMachine(const TJob* job, size_t sid)
{
    return GetMachine(job->Flow->GetStage(sid)->MachineId);
}

TMachineCtl* TMyShop::GetMachineCtl(const TJob* job, size_t sid)
{
    return GetMachine(job->Flow->GetStage(sid)->MachineId);
}

void TMyShop::StartJob(TMyJob* job)
{
    TGuard<TMutex> g(Mutex);
    double now = Now();
    TShopCtl::ArriveJob(job, now);
    TShop::StartJob(job, now);
}

void TMyShop::JobFinished(TJob* job)
{
    TGuard<TMutex> g(Mutex); // for TMyFlow dtor to work under lock
    TShop::JobFinished(job);
    delete static_cast<TMyJob*>(job);
}

void TMyShop::OnOperationFinished(TJob* job, size_t sid, ui64 realcost, double now)
{
    DepartJobStage(job, sid, realcost, now);
}

void TMyShop::OnOperationAbort(TJob* job, size_t sid)
{
    AbortJobStage(job, sid);
}

void* TMyShop::SourceThreadFunc(void* this_)
{
    ::NShopSim::SetCurrentThreadName("source");
    reinterpret_cast<TMyShop*>(this_)->Source();
    return nullptr;
}

void* TMyShop::SchedulerThreadFunc(void* this_)
{
    ::NShopSim::SetCurrentThreadName("sched");
    reinterpret_cast<TMyShop*>(this_)->Scheduler();
    return nullptr;
}

inline ui64 IntegerCost(double cost)
{
    return Max<ui64>(1, ceilf(cost * 1e6));
}

void TMyShop::Generate(double time, const TMyFlowPtr& flow)
{
    TMyJob* job = new TMyJob();
    job->MyFlow = flow;
    job->GenerateTime = time;
    job->Flow = flow.Get();
    job->Cost = 0;
    for (int i = 0; i < flow->Source.GetOperation().size(); i++) {
        const TOperationPb& op = flow->Source.GetOperation(i);
        double estcost = NAN;
        double realcost = NAN;
        if (op.HasEstCost()) {
            estcost = Random(op.GetEstCost());
            if (op.HasRealCost()) {
                realcost = Random(op.GetRealCost());
            } else if (op.HasEstCostMinusRealCost()) {
                double diff = Random(op.GetEstCostMinusRealCost());
                realcost = estcost - diff;
            } else if (op.HasEstCostOverRealCost()) {
                double ratio = Random(op.GetEstCostOverRealCost());
                realcost = estcost / ratio;
            } else {
                Y_ABORT("wrong flow config");
            }
        } else if (op.HasRealCost()) {
            realcost = Random(op.GetRealCost());
            if (op.HasEstCostMinusRealCost()) {
                double diff = Random(op.GetEstCostMinusRealCost());
                estcost = realcost + diff;
            } else if (op.HasEstCostOverRealCost()) {
                double ratio = Random(op.GetEstCostOverRealCost());
                estcost = realcost * ratio;
            } else {
                Y_ABORT("wrong flow config");
            }
        } else {
            Y_ABORT("wrong flow config");
        }
        ui64 intrealcost = IntegerCost(realcost);
        job->Cost += intrealcost;
        job->RealCost.push_back(intrealcost);
        job->AddOp(IntegerCost(estcost));
    }
    Enqueue(job);
}

void TMyShop::PushSource(double time, const TMyFlowPtr& flow)
{
    SourceHeap.emplace_back(time, flow);
    PushHeap(SourceHeap.begin(), SourceHeap.end());
}

void TMyShop::PopSource()
{
    PopHeap(SourceHeap.begin(), SourceHeap.end());
    SourceHeap.pop_back();
}

void TMyShop::Source()
{
    double deadline = 0.0;
    while (AtomicGet(Running)) {
        if (deadline != 0.0) {
            double waitTime = Now() - deadline;
            if (waitTime > 0.0) {
                NanoSleep(Min(waitTime, 1.0) * 1e9);
            }
        }

        while (AtomicGet(Running)) {
            TGuard<TMutex> g(Mutex);
            double now = Now();
            if (SourceHeap.empty()) {
                deadline = now + 1.0; // Wait for config with sources
                break;
            }

            THeapItem peek = SourceHeap.front();
            if (peek.Time < now) {
                PopSource();
                Generate(peek.Time, peek.Flow);
                double ia = Random(peek.Flow->Source.GetInterArrival());
                PushSource(peek.Time + ia, peek.Flow);
            } else {
                deadline = peek.Time;
                break;
            }
        }
    }
}

bool TMyShop::IsFlowFrozen(TMyFlow* flow)
{
    for (size_t sid = 0; sid < flow->StageCount(); sid++) {
        if (TMachine* machine = GetMachine(flow->GetStage(sid)->MachineId)) {
            if (static_cast<TMyMachine*>(machine)->IsFrozen()) {
                return true;
            }
        } else {
            // Flow is considered to be frozen if any of machines is unavailable
            return true;
        }
    }
    // Flow is not frozen only if every machine is not frozen
    return false;
}

void TMyShop::Enqueue(TMyJob* job)
{
    TGuard<TMutex> g(Mutex);
    CondVar.Signal();
    job->MyFlow->Enqueue(job);
}

TMyJob* TMyShop::Dequeue(TInstant deadline)
{
    TGuard<TMutex> g(Mutex);
    // Wait for jobs
    while (Sched.Empty()) {
        if (!CondVar.WaitD(Mutex, deadline)) {
            return nullptr;
        }
    }
    return static_cast<TMyJob*>(Sched.PopSchedulable());
}

void TMyShop::Scheduler()
{
    while (AtomicGet(Running)) {
        if (TMyJob* job = Dequeue(TInstant::Now() + TDuration::Seconds(1))) {
            StartJob(job);
        }
    }
}

class TFifo : public IScheduler {
private:
    TString Name;
    TMutex Mutex;
    TCondVar CondVar;
    struct TItem {
        TMyJob* Job;
        size_t Sid;
        ui64 Ts;
        TItem() {}
        TItem(TMyJob* job, size_t sid) : Job(job), Sid(sid), Ts(GetCycleCount())  {}
    };
    TDeque<TItem> Queue;
    ui64 EstCostInQueue = 0;
    TMyShop* Shop;
public:
    explicit TFifo(TMyShop* shop)
        : Shop(shop)
    {}

    void SetName(const TString& name) { Name = name; }

    ~TFifo() {
        Y_ABORT_UNLESS(Queue.empty(), "queue must be empty on destruction");
    }

    void Enqueue(TMyJob* job, size_t sid) override {
        TGuard<TMutex> g(Mutex);
        LWPROBE(FifoEnqueue, Shop->GetName(), job->Flow->GetName(), job->JobId, sid,
                job->Flow->GetStage(sid)->MachineId,
                job->GetOp(sid)->EstCost, Queue.size(), EstCostInQueue);
        CondVar.Signal();
        Queue.emplace_back(job, sid);
        EstCostInQueue += job->GetOp(sid)->EstCost;
    }

    TMyJob* Dequeue(size_t* sid) override {
        TGuard<TMutex> g(Mutex);
        if (!Queue.empty()) {
            TItem item = Queue.front();
            Queue.pop_front();
            EstCostInQueue -= item.Job->GetOp(item.Sid)->EstCost;
            if (sid) {
                *sid = item.Sid;
            }
            LWPROBE(FifoDequeue, Shop->GetName(), item.Job->Flow->GetName(), item.Job->JobId, item.Sid,
                    item.Job->Flow->GetStage(item.Sid)->MachineId,
                    item.Job->GetOp(item.Sid)->EstCost, Queue.size(), EstCostInQueue,
                    CyclesToMs(Duration(item.Ts, GetCycleCount())));
            return item.Job;
        }
        return nullptr;
    }

    TMyJob* DequeueWait(size_t* sid, TInstant deadline) override {
        TGuard<TMutex> g(Mutex);
        while (Queue.empty()) {
            if (!CondVar.WaitD(Mutex, deadline)) {
                return nullptr;
            }
        }
        return TFifo::Dequeue(sid);
    }
};

void* TMyMachine::TWorkerThread::ThreadProc()
{
    ::NShopSim::SetCurrentThreadName(ToString(WorkerIdx) + Machine->GetName());
    Machine->Worker(WorkerIdx);
    return nullptr;
}

TMyMachine::TMyMachine(TMyShop* shop, ui64 machineId)
    : TMachine(shop)
    , TMachineCtl(new TFlowCtl())
    , MyShop(shop)
    , MachineId(machineId)
    , WaitThread(&WaitThreadFunc, this)
{
    WaitThread.Start();
}

void TMyMachine::FailAllJobs()
{
    TReadSpinLockGuard g(ConfigureLock);
    size_t sid;
    while (TMyJob* job = Scheduler->Dequeue(&sid)) {
        MyShop->OperationFinished(job, sid, 0, false);
    }
}

TMyMachine::~TMyMachine()
{
    AtomicSet(Running, 0);

    // Fail all enqueued jobs
    FailAllJobs();

    // Join all threads (complete and workers) explicitly
    // to avoid joining on half-destructed TMachine object
    WorkerThread.clear();
    WaitThread.Join();
}

void TMyMachine::Configure(const TMachinePb& cfg)
{
    TWriteSpinLockGuard g(ConfigureLock);
    TSchedulerPb oldSchedulerCfg = GetSchedulerCfg();
    SetConfig(cfg);
    SetName(cfg.GetName());

    // Update scheduler
    if (!NProtoBuf::IsEqual(oldSchedulerCfg, cfg.GetScheduler())) {
        THolder<IScheduler> oldScheduler(Scheduler.Release());
        if (cfg.GetScheduler().HasFIFO()) {
            TFifo* fifo = new TFifo(MyShop);
            fifo->SetName(cfg.GetScheduler().GetFIFO().GetName());
            Scheduler.Reset(fifo);
        } else {
            Y_ABORT_UNLESS("only FIFO queueing disciplice is supported for now");
        }

        if (oldScheduler) {
            // Requeue jobs into new scheduler
            size_t sid;
            while (TMyJob* job = oldScheduler->Dequeue(&sid)) {
                Scheduler->Enqueue(job, sid);
            }
            oldScheduler.Destroy(); // Just to be explicit
        }
    }

    // Adjust workers count
    while (WorkerThread.size() < cfg.GetWorkerCount()) {
        auto worker = new TWorkerThread(this, WorkerThread.size());
        WorkerThread.emplace_back(worker);
        worker->Start();
    }
    WorkerThread.resize(cfg.GetWorkerCount()); // Shrink only, join threads
}

bool TMyMachine::StartOperation(TJob* job, size_t sid)
{
    TReadSpinLockGuard g(ConfigureLock);
    TMachine::StartOperation(job, sid);
    Scheduler->Enqueue(static_cast<TMyJob*>(job), sid);
    return false;
}

void TMyMachine::Freeze()
{
    Frozen = true;
    for (const TMyFlowPtr& p : Flows) {
        p->IncFrozenMachine();
    }
}

void TMyMachine::Unfreeze()
{
    Frozen = false;
    for (const TMyFlowPtr& p : Flows) {
        p->DecFrozenMachine();
    }
}

void TMyMachine::ClearFlows()
{
    if (Frozen) {
        for (const TMyFlowPtr& flow : Flows) {
            flow->DecFrozenMachine();
        }
    }
    Flows.clear();
}

void TMyMachine::AddFlow(const TMyFlowPtr& flow)
{
    Flows.push_back(flow);
}

void TMyMachine::SetConfig(const TMachinePb& cfg)
{
    TGuard<TSpinLock> g(CfgLock);
    Cfg = cfg;
}

TSchedulerPb TMyMachine::GetSchedulerCfg()
{
    TGuard<TSpinLock> g(CfgLock);
    return Cfg.GetScheduler();
}

double TMyMachine::RandomWaitTime()
{
    TGuard<TSpinLock> g(CfgLock);
    return Random(Cfg.GetWait());
}

ui64 TMyMachine::GetWorkerCount()
{
    TGuard<TSpinLock> g(CfgLock);
    return Cfg.GetWorkerCount();
}

double TMyMachine::GetSpeed()
{
    TGuard<TSpinLock> g(CfgLock);
    return Cfg.GetSpeed();
}

TMyJob* TMyMachine::DequeueJob(size_t* sid)
{
    TReadSpinLockGuard g(ConfigureLock);
    return Scheduler->DequeueWait(sid, TInstant::Now() + TDuration::Seconds(1));
}

void TMyMachine::Worker(size_t workerIdx)
{
    NHPTimer::STime workerTimer;
    NHPTimer::GetTime(&workerTimer);

    while (AtomicGet(Running) && workerIdx < GetWorkerCount()) {
        size_t jobLimitPerCycle = 10;
        size_t sid;
        while (TMyJob* job = DequeueJob(&sid)) {
            double workerIdleTime = NHPTimer::GetTimePassed(&workerTimer) * 1000;
            Execute(job, sid);
            double workerActiveTime = NHPTimer::GetTimePassed(&workerTimer) * 1000;
            LWPROBE(MachineWorkerStats, MyShop->GetName(), MachineId,
                    workerIdleTime, workerActiveTime, workerIdleTime + workerActiveTime);
            AtomicAdd(IdleTime, workerIdleTime * 1000);
            AtomicAdd(ActiveTime, workerActiveTime * 1000);
            if (!--jobLimitPerCycle) {
                break;
            }
        }
    }
}

void TMyMachine::Execute(TMyJob* job, size_t sid)
{
    // Emulate execution for real cost nanosaconds
    ui64 ts = GetCycleCount();
    double speed = GetSpeed();
    ui64 realcost = job->RealCost[sid] / speed;
    if (realcost >= 2) {
        Sleep(TDuration::MicroSeconds((realcost - 1)));
    }
    while (CyclesToMs(Duration(ts, GetCycleCount())) * 1000.0 < realcost) ;

    // Time measurements
    ui64 execTs = GetCycleCount();
    double execTimeMs = CyclesToMs(Duration(ts, execTs));
    LWPROBE(MachineExecute, MyShop->GetName(), job->Flow->GetName(), job->JobId, sid,
            MachineId,
            job->GetOp(sid)->EstCost, job->RealCost[sid], speed, execTimeMs);

    double wait = RandomWaitTime();
    TInstant deadline = TInstant::Now() + TDuration::MicroSeconds(ui64(wait * 1000000.0));
    PushToWaitHeap(deadline, job, sid, execTs);
}

void TMyMachine::PushToWaitHeap(TInstant deadline, TMyJob* job, size_t sid, ui64 execTs)
{
    TGuard<TMutex> g(WaitMutex);
    WaitCondVar.Signal();
    WaitHeap.emplace_back(deadline, job, sid, execTs);
    PushHeap(WaitHeap.begin(), WaitHeap.end(), TWaitCmp());
}

TMyJob* TMyMachine::PopFromWaitHeap(size_t* sid)
{
    while (AtomicGet(Running)) {
        TGuard<TMutex> g(WaitMutex);
        while (WaitHeap.empty()) {
            if (!WaitCondVar.WaitT(WaitMutex, TDuration::Seconds(1))) {
                if (!AtomicGet(Running)) {
                    return nullptr;
                }
            }
        }

        TWaitItem peek = WaitHeap.front();
        TInstant now = TInstant::Now();
        if (peek.Deadline < now) {
            PopHeap(WaitHeap.begin(), WaitHeap.end(), TWaitCmp());
            WaitHeap.pop_back();
            if (sid) {
                *sid = peek.Sid;
            }
            LWPROBE(MachineWait, MyShop->GetName(), peek.Job->Flow->GetName(), peek.Job->JobId, peek.Sid,
                    MachineId,
                    CyclesToMs(Duration(peek.Ts, GetCycleCount())));
            return peek.Job;
        } else {
            TInstant deadline = peek.Deadline;
            WaitCondVar.WaitD(WaitMutex, Min(deadline, now + TDuration::Seconds(1)));
        }
    }
    return nullptr;
}

void* TMyMachine::WaitThreadFunc(void* this_)
{
    reinterpret_cast<TMyMachine*>(this_)->Wait();
    return nullptr;
}

void TMyMachine::Wait()
{
    ::NShopSim::SetCurrentThreadName(GetName());

    double lastMonSec = Now();
    ui64 lastIdleTime = 0;
    ui64 lastActiveTime = 0;
    size_t sid;
    while (TMyJob* job = PopFromWaitHeap(&sid)) {
        ui64 realcost = job->RealCost[sid];
        MyShop->OperationFinished(job, sid, realcost, true);

        // Utilization monitoring
        double now = Now();
        if (lastMonSec + 1.0 < now) {
            ui64 idleTime = AtomicGet(IdleTime);
            ui64 activeTime = AtomicGet(ActiveTime);
            ui64 idleDelta = idleTime - lastIdleTime;
            ui64 activeDelta = activeTime - lastActiveTime;
            ui64 elapsed = idleDelta + activeDelta;
            double utilization = (elapsed == 0? 0: double(activeDelta) / elapsed);
            lastIdleTime = idleTime;
            lastActiveTime = activeTime;
            LWPROBE(MachineStats, MyShop->GetName(), MachineId, utilization);
            lastMonSec = now;
        }
    }
}

TMyFlow::TMyFlow(const TSourcePb& source, TScheduler<TSingleResource>* sched)
    : Source(source)
{
    TFlow::SetName(source.GetName());
    Freezable.SetName(source.GetName());
    Freezable.SetScheduler(sched);
    TConsumer<TSingleResource>::SetName(source.GetName());
    SetScheduler(sched);
    SetFreezable(&Freezable);
}

TMyFlow::~TMyFlow()
{
    Deactivate();
}

void TMyFlow::Enqueue(TMyJob* job)
{
    if (Queue.empty()) {
        Activate();
    }
    Queue.push_back(job);
}

TSchedulable<TSingleResource::TCost>* TMyFlow::PopSchedulable()
{
    Y_ABORT_UNLESS(!Queue.empty());
    TMyJob* job = Queue.front();
    Queue.pop_front();
    return job;
}

bool TMyFlow::Empty() const
{
    return Queue.empty();
}

void TMyFlow::IncFrozenMachine()
{
    if (FrozenMachines == 0) {
        Freezable.Freeze();
    }
    FrozenMachines++;
}

void TMyFlow::DecFrozenMachine()
{
    Y_ASSERT(FrozenMachines > 0);
    FrozenMachines--;
    if (FrozenMachines == 0) {
        Freezable.Unfreeze();
    }
}

}
