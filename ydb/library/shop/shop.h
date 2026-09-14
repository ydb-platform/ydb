#pragma once

#include "flowctl.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>
#include <util/system/types.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NShop {

struct TStage;
class TFlow;
struct TOp;
struct TJob;
class TMachine;
class TShop;

struct TStage {
    ui64 MachineId;
    TString Name;
    TStackVec<size_t, 16> Depends; // Idx of stages it depends on
    TStackVec<size_t, 16> Blocks; // Idx of stages dependent on it
};

class TFlow {
private:
    TStackVec<TStage, 16> Stage; // Topologicaly sorted DAG of stages
    TString Name;
public:
    virtual ~TFlow() {}

    size_t AddStage(std::initializer_list<size_t> depends);
    size_t AddStage(ui64 machineId, std::initializer_list<size_t> depends);

    // Accessors
    void SetName(const TString& name) { Name = name; }
    TString GetName() const { return Name; }
    const TStage* GetStage(size_t idx) const { return &Stage[idx]; }
    size_t StageCount() const { return Stage.size(); }

    TString DebugDump() const;

    friend class TShop;
};

struct TOp: public TFcOp {
    enum class EState : ui8 {
        Wait = 0, // Awaits it's dependencies to be done
        Proc = 1, // Is processing on machine
        Done = 2, // Already processed
        Fail = 3, // Already processed, but unsuccessfully
        Skip = 4, // Not going to be processed
    };

    TMachine* Machine = nullptr; // Machine assigned for processing

    EState State = EState::Wait;
    ui64 DepsLeft = 0; // Count of blocker operation left to be done

    ui64 StartTs = 0; // in cycles
    ui64 FinishTs = 0; // in cycles
    ui64 Duration = 0; // in cycles

    TOp() {}
    explicit TOp(const TStage* stage);

    const char* StateName() const;
};

struct TJob {
    TFlow* Flow = nullptr;
    TStackVec<TOp, 16> Op;
    ui64 JobId = 0;
    ui64 OpsLeft = 0;
    ui64 OpsInFly = 0;
    bool Failed = false;
    bool Canceled = false;

    ui64 StartTs = 0; // in cycles
    ui64 FinishTs = 0; // in cycles
    ui64 Duration = 0; // in cycles

    void AddOp(ui64 estcost);
    TOp* GetOp(size_t idx) { return &Op[idx]; }
    const TOp* GetOp(size_t idx) const { return &Op[idx]; }

    TString DebugDump() const;
};

class IMonCounters {
public:
    virtual ~IMonCounters() {}
    virtual void Count(const TOp* op) = 0;
};

class TProcMonCounters : public IMonCounters {
private:
    TProcCounters Counters;
public:
    void Count(const TOp* op) override;
    const TProcCounters& GetCounters() const { return Counters; }
};

class TMachine {
private:
    TShop* Shop;
    TString Name;
    THolder<IMonCounters> Counters;
public:
    explicit TMachine(TShop* shop);
    virtual ~TMachine() {}

    TShop* GetShop() { return Shop; }
    void SetName(const TString& name) { Name = name; }
    TString GetName() const { return Name; }
    void SetCounters(IMonCounters* counters) { Counters.Reset(counters); }

    // Execute sync or start async operation processing.
    // TShop::OperationFinished() must be called on finish
    // Returns:
    //  - result of sync-called OperationFinished()
    //  - false in case of async operation
    virtual bool StartOperation(TJob* job, size_t sid);

    // Monitoring
    void Count(const TOp* op);
};

class TShop {
private:
    ui64 LastJobId = 0;
    TString Name;
public:
    virtual ~TShop() {}

    void SetName(const TString& name) { Name = name; }
    TString GetName() const { return Name; }

    // Machines
    virtual TMachine* GetMachine(const TJob* job, size_t sid) = 0;

    // Jobs
    void StartJob(TJob* job, double now);
    void CancelJob(TJob* job);
    virtual void JobFinished(TJob* job);

    // Operations
    void SkipOperation(TJob* job, size_t sid);
    bool OperationFinished(TJob* job, size_t sid, ui64 realcost, bool success, double now);

protected:
    virtual void OnOperationFinished(TJob* job, size_t sid, ui64 realcost, double now) = 0;
    virtual void OnOperationAbort(TJob* job, size_t sid) = 0;

private:
    bool RunOperation(TJob* job, size_t sid, double now);
};

class TMachineCtl {
private:
    TFlowCtlPtr FlowCtl;
public:
    explicit TMachineCtl(TFlowCtl* flowCtl)
        : FlowCtl(flowCtl)
    {}

    TFlowCtl* GetFlowCtl() { return FlowCtl.Get(); }

    void Transition(TFlowCtl::EStateTransition st);

    // Flow control callbacks (e.g. stop/start incoming job flows in scheduler)
    virtual void Freeze() = 0;
    virtual void Unfreeze() = 0;
};

class TShopCtl {
public:
    void Configure(TMachineCtl* ctl, const TFlowCtlConfig& cfg, double now);

    virtual TMachineCtl* GetMachineCtl(const TJob* job, size_t sid) = 0;

    void ArriveJob(TJob* job, double now);
    void DepartJobStage(TJob* job, size_t sid, ui64 realcost, double now);
    void Depart(TMachineCtl* ctl, TFcOp* op, ui64 realcost, double now);
    void AbortJobStage(TJob* job, size_t sid);
    void Abort(TMachineCtl* ctl, TFcOp* op);

private:
    void ArriveJobStage(TJob* job, size_t sid, double now);
};

}
