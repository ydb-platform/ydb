#pragma once

#include <ydb/library/shop/sim_shop/config.pb.h>

#include <ydb/library/shop/flowctl.h>
#include <ydb/library/shop/shop.h>
#include <ydb/library/shop/scheduler.h>

#include <ydb/library/drr/drr.h>

#include <library/cpp/lwtrace/all.h>

#include <util/system/condvar.h>
#include <util/system/execpath.h>
#include <util/system/hp_timer.h>

#include <util/generic/ptr.h>
#include <util/generic/list.h>

#define SIMSHOP_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(FifoEnqueue, GROUPS("SimShopJob", "SimShopOp"), \
      TYPES(TString, TString, ui64, ui64, ui64, ui64, ui64, ui64), \
      NAMES("shop", "flow", "job", "sid", "machineid", "estcost", "queueLength", "queueCost")) \
    PROBE(FifoDequeue, GROUPS("SimShopJob", "SimShopOp"), \
      TYPES(TString, TString, ui64, ui64, ui64, ui64, ui64, ui64, double), \
      NAMES("shop", "flow", "job", "sid", "machineid", "estcost", "queueLength", "queueCost", "queueTimeMs")) \
    PROBE(MachineExecute, GROUPS("SimShopJob", "SimShopOp"), \
      TYPES(TString, TString, ui64, ui64, ui64, ui64, ui64, double, double), \
      NAMES("shop", "flow", "job", "sid", "machineid", "estcost", "realcost", "speed", "execTimeMs")) \
    PROBE(MachineWait, GROUPS("SimShopJob", "SimShopOp"), \
      TYPES(TString, TString, ui64, ui64, ui64, double), \
      NAMES("shop", "flow", "job", "sid", "machineid", "waitTimeMs")) \
    PROBE(MachineWorkerStats, GROUPS("SimShopMachine"), \
      TYPES(TString, ui64, double, double, double), \
      NAMES("shop", "machineid", "idleTimeMs", "activeTimeMs", "totalTimeMs")) \
    PROBE(MachineStats, GROUPS("SimShopMachine"), \
      TYPES(TString, ui64, double), \
      NAMES("shop", "machineid", "utilization")) \
/**/

LWTRACE_DECLARE_PROVIDER(SIMSHOP_PROVIDER)

namespace NShopSim {

inline void SetCurrentThreadName(const TString& name,
                                 const ui32 maxCharsFromProcessName = 8)
{
#if defined(_linux_)
    // linux limits threadname by 15 + \0

    TStringBuf procName(GetExecPath());
    procName = procName.RNextTok('/');
    procName = procName.SubStr(0, maxCharsFromProcessName);

    TStringStream linuxName;
    linuxName << procName << "." << name;
    TThread::SetCurrentThreadName(linuxName.Str().data());
#else
    Y_UNUSED(maxCharsFromProcessName);
    TThread::SetCurrentThreadName(name.c_str());
#endif
}

inline double Now()
{
    return CyclesToDuration(GetCycleCount()).SecondsFloat();
}

using namespace NShop;

class TMyQueue;
class TMyFlow;
struct TMyJob;
class TMyShop;
class TMyMachine;

class TMyFlow
        : public TFlow
        , public TConsumer<TSingleResource>
        , public TAtomicRefCount<TMyFlow>
{
public:
    const TSourcePb Source;

    // Should be accessed under Shop::Mutex lock
    TDeque<TMyJob*> Queue;
    TFreezable<TSingleResource> Freezable;
    ui64 FrozenMachines = 0;

    TMyFlow(const TSourcePb& source, TScheduler<TSingleResource>* sched);
    ~TMyFlow();
    void Enqueue(TMyJob* job);
    TSchedulable<TSingleResource::TCost>* PopSchedulable() override;
    bool Empty() const override;
    void IncFrozenMachine();
    void DecFrozenMachine();
};

using TMyFlowPtr = TIntrusivePtr<TMyFlow>;

struct TMyJob
        : public TJob
        , public TSchedulable<TSingleResource::TCost>
{
    TVector<ui64> RealCost; // us
    double GenerateTime = 0.0;
    TMyFlowPtr MyFlow;
};

class IScheduler {
public:
    virtual ~IScheduler() {}
    virtual void Enqueue(TMyJob* job, size_t sid) = 0;
    virtual TMyJob* Dequeue(size_t* sid) = 0;
    virtual TMyJob* DequeueWait(size_t* sid, TInstant deadline) = 0;
};

class TMyMachine : public TMachine, public TMachineCtl {
private:
    class TWorkerThread
            : public ISimpleThread
            , public TSimpleRefCount<TWorkerThread>
    {
    private:
        TMyMachine* Machine;
        size_t WorkerIdx;
    public:
        TWorkerThread(TMyMachine* machine, size_t workerIdx)
            : Machine(machine)
            , WorkerIdx(workerIdx)
        {}
        void* ThreadProc() override;
    };
    using TWorkerThreadPtr = TIntrusivePtr<TWorkerThread>;

private:
    TAtomic Running = 1;
    TMyShop* MyShop;
    const ui64 MachineId;

    bool Frozen = false;
    TVector<TMyFlowPtr> Flows;

    TSpinLock CfgLock;
    TMachinePb Cfg;

    TRWSpinLock ConfigureLock;
    TFlowCtl MyFlowCtl;
    THolder<IScheduler> Scheduler;
    TList<TWorkerThreadPtr> WorkerThread;

    TMutex WaitMutex;
    TCondVar WaitCondVar;
    struct TWaitItem {
        TInstant Deadline;
        TMyJob* Job;
        size_t Sid;
        ui64 Ts;
        TWaitItem(TInstant d, TMyJob* j, size_t s, ui64 t)
            : Deadline(d), Job(j), Sid(s), Ts(t)
        {}
    };
    struct TWaitCmp {
        bool operator()(const TWaitItem& lhs, const TWaitItem& rhs) const {
            return lhs.Deadline > rhs.Deadline;
        }
    };
    TVector<TWaitItem> WaitHeap;
    TThread WaitThread;

    // Monitoring
    TAtomic IdleTime;
    TAtomic ActiveTime;

public:
    TMyMachine(TMyShop* shop, ui64 machineId);
    ~TMyMachine();

    ui64 GetId() const { return MachineId; }

    void Configure(const TMachinePb& cfg);
    bool StartOperation(TJob* job, size_t sid) override;
    void Freeze() override;
    void Unfreeze() override;
    bool IsFrozen() { return Frozen; }

    void ClearFlows();
    void AddFlow(const TMyFlowPtr& flow);


private:
    void SetConfig(const TMachinePb& cfg);
    TSchedulerPb GetSchedulerCfg();
    double RandomWaitTime();
    ui64 GetWorkerCount();
    double GetSpeed();
    TMyJob* DequeueJob(size_t* sid);
    void Worker(size_t workerIdx);
    void Execute(TMyJob* job, size_t sid);
    void PushToWaitHeap(TInstant deadline, TMyJob* job, size_t sid, ui64 execTs);
    TMyJob* PopFromWaitHeap(size_t* sid);
    static void* WaitThreadFunc(void* this_);
    void Wait();
    void FailAllJobs();
    friend class TWorkerThread;
};

class TMyShop : public TShop, public TShopCtl {
private:
    TAtomic Running = 1;

    TMutex Mutex;
    TCondVar CondVar;
    TConfigPb Cfg;

    // Flows and sources
    TThread SourceThread;
    struct THeapItem {
        double Time;
        TMyFlowPtr Flow;
        THeapItem(double time, const TMyFlowPtr flow)
            : Time(time)
            , Flow(flow)
        {}
        bool operator<(const THeapItem& rhs) const {
            return Time > rhs.Time;
        }
    };
    TVector<THeapItem> SourceHeap;

    // Scheduling and congestion control
    TThread SchedulerThread;
    TScheduler<TSingleResource> Sched;

    // Machines
    struct TMachineItem {
        THolder<TMyMachine> Machine;
    };
    THashMap<ui64, TMachineItem> Machines; // machineId -> machine/flowctl
    ui64 LastMachineId = 0;
public:
    TMyShop();
    ~TMyShop();

    void Configure(const TConfigPb& newCfg);
    void OperationFinished(TMyJob* job, size_t sid, ui64 realcost, bool success);

    TMyMachine* GetMachine(ui64 machineId);
    TMachine* GetMachine(const TJob* job, size_t sid) override;
    TMachineCtl* GetMachineCtl(const TJob* job, size_t sid) override;

    void StartJob(TMyJob* job);
    void JobFinished(TJob* job) override;

    template <class TFunc>
    void ForEachMachine(TFunc func)
    {
        TGuard<TMutex> g(Mutex);
        for (const auto& kv : Machines) {
            ui64 machineId = kv.first;
            TMyMachine* machine = kv.second.Machine.Get();
            func(machineId, machine, machine->GetFlowCtl());
        }
    }

    TConfigPb GetConfig() const { TGuard<TMutex> g(Mutex); return Cfg; }

protected:
    void OnOperationFinished(TJob* job, size_t sid, ui64 realcost, double now) override;
    void OnOperationAbort(TJob* job, size_t sid) override;

private:
    static void* SourceThreadFunc(void* this_);
    static void* SchedulerThreadFunc(void* this_);
    void Generate(double time, const TMyFlowPtr& flow);
    void PushSource(double time, const TMyFlowPtr& flow);
    void PopSource();
    void Source();

    bool IsFlowFrozen(TMyFlow* flow);
    void Enqueue(TMyJob* job);
    TMyJob* Dequeue(TInstant deadline);
    void Scheduler();
};

}
