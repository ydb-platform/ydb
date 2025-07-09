#include <ydb/library/shop/flowctl.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/lwtrace/all.h>

#include <util/generic/cast.h>
#include <util/generic/deque.h>
#include <util/generic/vector.h>
#include <util/string/vector.h>
#include <util/system/env.h>

#include <functional>

namespace NFcTest {

////////////////////////////////////////////////////////////////////////////////
///  Event-driven simulator core
///
using namespace NShop;

class INode;
class TSimulator;

struct TMyEvent {
    double Time = 0; // Time at which event should be executed
    bool Quit = false;
    INode* Sender = nullptr;
    INode* Receiver = nullptr;

    virtual ~TMyEvent() {}

    virtual void Print(IOutputStream& os);
};

class INode {
protected:
    TSimulator* Sim;
    TString Name;
public:
    INode(TSimulator* sim)
        : Sim(sim)
    {}
    virtual ~INode() {}
    virtual void Receive(TMyEvent* ev) = 0;
    void SetName(const TString& name) { Name = name; }
    const TString& GetName() const { return Name; }
protected:
    void Send(TMyEvent* ev, INode* receiver);
    void SendAt(TMyEvent* ev, INode* receiver, double time);
    void SendAfter(TMyEvent* ev, INode* receiver, double delay);
};

class TSimulator {
public:
    double CurrentTime = 0;
    struct TEventsCmp {
        bool operator()(const TMyEvent* l, const TMyEvent* r) const {
            return l->Time > r->Time;
        }
    };
    TVector<TMyEvent*> Events;
    bool SimLog;
public:
    TSimulator()
    {
        TString path = GetEnv("SIMLOG");
        SimLog = path && TString(path) == "y";
    }

    ~TSimulator()
    {
        for (TMyEvent* ev : Events) {
            delete ev;
        }
    }

    void Schedule(TMyEvent* ev, INode* sender, INode* receiver, double time)
    {
        if (receiver == nullptr && !ev->Quit) {
            delete ev;
        } else {
            ev->Time = time;
            ev->Sender = sender;
            ev->Receiver = receiver;
            Events.push_back(ev);
            PushHeap(Events.begin(), Events.end(), TEventsCmp());
        }
    }

    void Run()
    {
        while (!Events.empty()) {
            PopHeap(Events.begin(), Events.end(), TEventsCmp());
            TMyEvent* ev = Events.back();
            Events.pop_back();
            CurrentTime = ev->Time;
            if (SimLog) {
                ev->Print(Cerr);
                Cerr << Endl;
            }
            if (ev->Quit) {
                delete ev;
                return;
            } else {
                ev->Receiver->Receive(ev);
            }
        }
    }


    void QuitAt(double time)
    {
        TMyEvent* ev = new TMyEvent();
        ev->Quit = true;
        Schedule(ev, nullptr, nullptr, time);
    }

    double Now() const { return CurrentTime; }
};

void INode::Send(TMyEvent* ev, INode* receiver)
{
    Sim->Schedule(ev, this, receiver, Sim->Now());
}

void INode::SendAt(TMyEvent* ev, INode* receiver, double time)
{
    Sim->Schedule(ev, this, receiver, time);
}

void INode::SendAfter(TMyEvent* ev, INode* receiver, double delay)
{
    Sim->Schedule(ev, this, receiver, Sim->Now() + delay);
}

////////////////////////////////////////////////////////////////////////////////
/// Flow control related stuff
///

struct TMyRequest : public TMyEvent {
    bool Arrived = false;
    double ArriveTime = 0;
    double DepartTime = 0;
    ui64 Cost = 0;
    TFcOp FcOp;

    explicit TMyRequest(ui64 cost = 0)
        : Cost(cost)
    {}

    double ResponseTime() const { return DepartTime - ArriveTime; }

    virtual void Print(IOutputStream& os);
};

struct TMyTransition : public TMyEvent {
    TFlowCtl::EStateTransition Transition;
    bool IsOpen;
    bool Arrive; // true = arrive, false = depart

    TMyTransition(TFlowCtl::EStateTransition transition, bool isOpen, bool arrive)
        : Transition(transition)
        , IsOpen(isOpen)
        , Arrive(arrive)
    {}

    virtual void Print(IOutputStream& os);
};

class TFcSystem : public INode {
protected:
    TFlowCtl Fc;
    INode* Source = nullptr;
public:
    TFcSystem(TSimulator* sim)
        : INode(sim)
    {}

    void Configure(const TFlowCtlConfig& cfg)
    {
        Fc.Configure(cfg, Sim->Now());
    }

    void Receive(TMyEvent* ev) override
    {
        TMyRequest* req = CheckedCast<TMyRequest*>(ev);
        if (!req->Arrived) {
            req->ArriveTime = Sim->Now();
            req->Arrived = true;
            TFlowCtl::EStateTransition st = Fc.ArriveST(req->FcOp, req->Cost, req->ArriveTime);
            if (Source) {
                Send(new TMyTransition(st, Fc.IsOpen(), true), Source);
            }
            Enter(req);
        } else {
            req->DepartTime = Sim->Now();
            TFlowCtl::EStateTransition st = Fc.DepartST(req->FcOp, req->Cost, req->DepartTime);
            if (Source) {
                Send(new TMyTransition(st, Fc.IsOpen(), false), Source);
            }
            Exit(req);
        }
    }

    bool IsOpen() const { return Fc.IsOpen(); }

    void SetSource(INode* source) { Source = source; }
protected:
    virtual void Enter(TMyRequest* req) = 0;
    virtual void Exit(TMyRequest* req) = 0;
};

TFlowCtlConfig DefaultFlowCtlConfig()
{
    TFlowCtlConfig cfg;
    return cfg;
}

class TOneServerWithFifo : public TFcSystem {
public:
    double Throughput; // cost/sec
    TDeque<TMyRequest*> Queue;
    INode* Target = nullptr;
    bool Busy = false;

    TOneServerWithFifo(TSimulator* sim, double throughput)
        : TFcSystem(sim)
        , Throughput(throughput)
    {
        Y_ABORT_UNLESS(Throughput > 0);
    }

    ~TOneServerWithFifo()
    {
        for (TMyRequest* req : Queue) {
            delete req;
        }
    }

    void Enter(TMyRequest* req) override
    {
        if (!Busy) {
            Execute(req);
            Busy = true;
        } else {
            Queue.push_back(req);
        }
    }

    void Exit(TMyRequest* req) override
    {
        Send(req, Target);
        if (Queue.empty()) {
            Busy = false;
        } else {
            TMyRequest* nextReq = Queue.back();
            Queue.pop_back();
            Execute(nextReq);
        }
    }

    void SetTarget(INode* target) { Target = target; }
private:
    void Execute(TMyRequest* req)
    {
        SendAfter(req, this, req->Cost / Throughput);
    }
};

class TParallelServersWithFifo : public TFcSystem {
public:
    double Throughput; // cost/sec per server
    ui64 ServersCount;
    TDeque<TMyRequest*> Queue;
    INode* Target = nullptr;
    ui64 BusyCount = 0;

    TParallelServersWithFifo(TSimulator* sim, double throughput, ui64 serversCount)
        : TFcSystem(sim)
        , Throughput(throughput)
        , ServersCount(serversCount)
    {
        Y_ABORT_UNLESS(Throughput > 0);
    }

    ~TParallelServersWithFifo()
    {
        for (TMyRequest* req : Queue) {
            delete req;
        }
    }

    void Enter(TMyRequest* req) override
    {
        if (BusyCount < ServersCount) {
            Execute(req);
            BusyCount++;
        } else {
            Queue.push_back(req);
        }
    }

    void Exit(TMyRequest* req) override
    {
        Send(req, Target);
        if (Queue.empty()) {
            BusyCount--;
        } else {
            TMyRequest* nextReq = Queue.back();
            Queue.pop_back();
            Execute(nextReq);
        }
    }

    void SetTarget(INode* target) { Target = target; }
private:
    void Execute(TMyRequest* req)
    {
        SendAfter(req, this, req->Cost / Throughput);
    }
};

class TUnlimSource : public INode {
private:
    TFcSystem* Target = nullptr;
    ui64 Cost;
    bool InFly = false;
public:
    TUnlimSource(TSimulator* sim, ui64 cost)
        : INode(sim)
        , Cost(cost)
    {}

    void Receive(TMyEvent* ev) override
    {
        TMyTransition* tr = CheckedCast<TMyTransition*>(ev);
        if (tr->Arrive) {
            InFly = false;
        }
        if (tr->IsOpen && !InFly) {
            Generate();
        }
        delete tr;
    }

    void Generate()
    {
        Y_ABORT_UNLESS(!InFly);
        Send(new TMyRequest(Cost), Target);
        InFly = true;
    }

    void SetTarget(TFcSystem* target)
    {
        Y_ABORT_UNLESS(!Target);
        Target = target;
        Generate();
    }
};

class TChecker : public INode {
private:
    using TCheckFunc = std::function<void(TMyRequest*)>;
    TCheckFunc CheckFunc;
    INode* Target = nullptr;
public:
    TChecker(TSimulator* sim, TCheckFunc f)
        : INode(sim)
        , CheckFunc(f)
    {}

    void Receive(TMyEvent* ev) override
    {
        TMyRequest* req = CheckedCast<TMyRequest*>(ev);
        CheckFunc(req);
        Send(req, Target);
    }

    void SetTarget(INode* target) { Target = target; }
};


////////////////////////////////////////////////////////////////////////////////
/// Auxilary stuff
///

struct TAuxEvent : public TMyEvent {
    std::function<void()> Func;

    explicit TAuxEvent(std::function<void()>&& f)
        : Func(std::move(f))
    {}
};

class TAuxNode : public INode {
public:
    explicit TAuxNode(TSimulator* sim)
        : INode(sim)
    {}

    void ExecuteAt(double time, std::function<void()>&& f)
    {
        SendAt(new TAuxEvent(std::move(f)), this, time);
    }

    void Receive(TMyEvent* ev) override
    {
        TAuxEvent* aux = CheckedCast<TAuxEvent*>(ev);
        aux->Func();
        delete aux;
    }
};

void TMyEvent::Print(IOutputStream& os)
{
    if (Quit) {
        os << Sprintf("[%010.3lf] QUIT", Time);
    } else {
        os << Sprintf("[%010.3lf] %s->%s",
            Time,
            Sender? Sender->GetName().data(): "nullptr",
            Receiver? Receiver->GetName().data(): "nullptr"
        );
    }
}

void TMyRequest::Print(IOutputStream& os)
{
    TMyEvent::Print(os);
    os << " Arrived=" << Arrived
       << " Cost=" << Cost
       << " ArriveTime=" << ArriveTime
       << " DepartTime=" << DepartTime
    ;
}

void TMyTransition::Print(IOutputStream& os)
{
    TMyEvent::Print(os);
    os << " Transition=" << int(Transition)
       << " IsOpen=" << IsOpen
       << " Arrive=" << Arrive
    ;
}

} // namespace NFcTest

Y_UNIT_TEST_SUITE(ShopFlowCtl) {

    using namespace NFcTest;
    using namespace NShop;

    Y_UNIT_TEST(StartLwtrace) {
        NLWTrace::StartLwtraceFromEnv();
    }

    Y_UNIT_TEST(OneOp) {
        {
            TSimulator sim;
            TOneServerWithFifo srv(&sim, 1.0);
            srv.Configure(DefaultFlowCtlConfig());
            sim.Schedule(new TMyRequest(3), nullptr, &srv, 5.0);
            sim.QuitAt(10.0);
            sim.Run();
            UNIT_ASSERT(sim.Now() == 10.0);
        }

        {
            TSimulator sim;
            TOneServerWithFifo srv(&sim, 1.0);
            srv.Configure(DefaultFlowCtlConfig());
            sim.Schedule(new TMyRequest(3), nullptr, &srv, 5.0);
            sim.Run();
            UNIT_ASSERT(sim.Now() == 8.0);
        }
    }

    Y_UNIT_TEST(Unlim_D_1_FIFO) {
        TSimulator sim;
        TUnlimSource src(&sim, 1);
        TOneServerWithFifo srv(&sim, 100.0);
        srv.Configure(DefaultFlowCtlConfig());
        TChecker chk(&sim, [&sim] (TMyRequest* req) {
            if (sim.Now() > 500.0) {
                UNIT_ASSERT(req->ResponseTime() < 5.0);
            }
        });

        src.SetTarget(&srv);
        srv.SetSource(&src);
        srv.SetTarget(&chk);

        src.SetName("src");
        srv.SetName("srv");
        chk.SetName("chk");

        sim.QuitAt(2000.0);
        sim.Run();
    }

    Y_UNIT_TEST(Unlim_D_k_FIFO) {
        TSimulator sim;
        TUnlimSource src(&sim, 1);
        TParallelServersWithFifo srv(&sim, 10.0, 10);
        srv.Configure(DefaultFlowCtlConfig());
        TChecker chk(&sim, [&sim] (TMyRequest* req) {
            if (sim.Now() > 500.0) {
                UNIT_ASSERT(req->ResponseTime() < 5.0);
            }
        });

        src.SetTarget(&srv);
        srv.SetSource(&src);
        srv.SetTarget(&chk);

        src.SetName("src");
        srv.SetName("srv");
        chk.SetName("chk");

        sim.QuitAt(2000.0);
        sim.Run();
    }
}
