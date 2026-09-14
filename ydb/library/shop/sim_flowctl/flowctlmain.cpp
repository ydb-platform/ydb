#include <ydb/library/shop/probes.h>
#include <ydb/library/shop/flowctl.h>

#include <ydb/library/drr/drr.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/lwtrace/all.h>
#include <library/cpp/monlib/service/monservice.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <util/random/normal.h>
#include <util/random/random.h>
#include <util/stream/file.h>
#include <util/system/condvar.h>
#include <util/system/hp_timer.h>

#include <google/protobuf/text_format.h>

#include <cmath>

#define SIMFC_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(IncomingRequest, GROUPS("FcSimEvents"), \
      TYPES(ui64, TString, double, double), \
      NAMES("requestId","type","costSec","startTime")) \
    PROBE(ScheduleRequest, GROUPS("FcSimEvents"), \
      TYPES(ui64, TString, double), \
      NAMES("requestId","type","schedulerTimeSec")) \
    PROBE(DequeueRequest, GROUPS("FcSimEvents"), \
      TYPES(ui64, TString, double), \
      NAMES("requestId","type","queueTimeSec")) \
    PROBE(ExecuteRequest, GROUPS("FcSimEvents"), \
      TYPES(ui64, TString, double), \
      NAMES("requestId","type","execTimeSec")) \
    PROBE(WorkerStats, GROUPS("FcSimWorker"), \
      TYPES(double, double, double), \
      NAMES("idleTimeSec", "activeTimeSec", "totalTimeSec")) \
    PROBE(SystemStats, GROUPS("FcSimSystem"), \
      TYPES(double), \
      NAMES("utilization")) \
    PROBE(CompleteRequest, GROUPS("FcSimEvents"), \
      TYPES(ui64, TString, double, double, double, double), \
      NAMES("requestId", "type", "waitTimeSec", "totalTimeSec", "procTimeSec", "costSec")) \
/**/

LWTRACE_DECLARE_PROVIDER(SIMFC_PROVIDER)
LWTRACE_DEFINE_PROVIDER(SIMFC_PROVIDER);

namespace NFcSim {

inline double Now()
{
    return CyclesToDuration(GetCycleCount()).SecondsFloat();
}

LWTRACE_USING(SIMFC_PROVIDER);

using namespace NScheduling;
class TMyTask;
class TMyQueue;

struct TRMeta {
    double m;
    double d;
    double p;
};

// Config
int g_MonPort              = 8080;
double g_AvgPeriodSec      = 0.002;
double g_MaxPeriodSec      = 0.001;
///TRMeta g_CostSec[]         = {{0.5, 0.05, 0.05}, {0.02, 0.01, 1.0}}; // double peek
TRMeta g_CostSec[]         = {{0.02, 0.01, 1.0}}; // small reqs
///TRMeta g_CostSec[]         = {{0.02, 0.0, 1.0}}; // fixed cost
///TRMeta g_CostSec[]         = {{0.2, 0.1, 1.0}}; // large reqs
TRMeta g_WaitSec[]         = {{0.05, 0.005, 1.0}}; // 50ms wait
///TRMeta g_WaitSec[]         = {{0.05, 0.000, 1.0}}; // fixed 50ms wait
///TRMeta g_WaitSec[]         = {{0.005, 0.0005, 1.0}}; // 5ms wait
TDuration g_CompletePeriod = TDuration::MicroSeconds(100);
ui64 g_QuantumNs           = 100 * 1000ull;
NShop::TFlowCtlConfig g_FcCfg;

// Returns exponentially distributed random number (cuts if upperLimit exceeded)
double ExpRandom(double lambda, double upperLimit)
{
    return Min(upperLimit, -(1.0/lambda) * log(1 - RandomNumber<double>()));
}

// Returns ranged Gaussian distribution
double GaussRandom(double m, double d)
{
    while (true) {
        double x = NormalRandom(m, d);
        if (x >= m/3 && x <= m*3) {
            return x;
        }
    }
}

// Returns random value distributed according to sum of gaussian distributions
double GaussSumRandom(TRMeta* meta, size_t size)
{
    double p = RandomNumber<double>();
    for (size_t i = 0; i < size - 1; i++) {
        if (p < meta[i].p) {
            return GaussRandom(meta[i].m, meta[i].d);
        }
    }
    return GaussRandom(meta[size - 1].m, meta[size - 1].d);
}

class TMyTask {
public:
    TUCost Cost; // in microseconds
    TUCost RealCost; // in microseconds
    size_t Type;
    TMyQueue* Queue = nullptr;
    ui64 Id;
    NHPTimer::STime Timer;
    double SchedTime = 0.0;
    double TotalTime = 0.0;
    NShop::TFcOp FcOp;
    TInstant CompleteDeadline;
public:
    TMyTask(TUCost cost, TUCost realcost, size_t type, ui64 id)
        : Cost(cost)
        , RealCost(realcost)
        , Type(type)
        , Id(id)
    {
        NHPTimer::GetTime(&Timer);
    }

    TUCost GetCost() const
    {
        return Cost;
    }
};

class TMyQueue: public TDRRQueue {
public:
    typedef TMyTask TTask;
public:
    typedef TList<TMyTask*> TTasks;
    TString Name;
    TTasks Tasks;
public: // Interface for clients
    TMyQueue(const TString& name, TWeight w = 1, TUCost maxBurst = 0)
        : TDRRQueue(w, maxBurst)
        , Name(name)
    {}

    ~TMyQueue()
    {
        for (TTasks::iterator i = Tasks.begin(), e = Tasks.end(); i != e; ++i) {
            delete *i;
        }
    }

    void PushTask(TMyTask* task)
    {
        task->Queue = this;
        if (Tasks.empty()) {
            // Scheduler must be notified on first task in queue
            if (GetScheduler()) {
                GetScheduler()->ActivateQueue(this);
            }
        }
        Tasks.push_back(task);
        LWPROBE(IncomingRequest, task->Id, task->Queue->Name,
                double(task->Cost) / 1000000.0,
                NHPTimer::GetSeconds(task->Timer));
    }
public: // Interface for scheduler
    void OnSchedulerAttach()
    {
        Y_ABORT_UNLESS(GetScheduler() != nullptr);
        if (!Tasks.empty()) {
            GetScheduler()->ActivateQueue(this);
        }
    }

    TTask* PeekTask()
    {
        Y_ABORT_UNLESS(!Tasks.empty());
        return Tasks.front();
    }

    void PopTask()
    {
        Y_ABORT_UNLESS(!Tasks.empty());
        Tasks.pop_front();
    }

    bool Empty() const
    {
        return Tasks.empty();
    }
};

typedef std::shared_ptr<TMyQueue> TQueuePtr;

// State
NLWTrace::TProbeRegistry* g_Probes;
NLWTrace::TManager* g_TraceMngr;
volatile bool g_Running = true;
ui64 g_LastRequestId = 0;

TMutex g_CompleteLock;
TCondVar g_CompleteCondVar;
TVector<TMyTask*> g_Complete;

TMutex g_ScheduledLock;
TCondVar g_ScheduledCondVar;
TDeque<TMyTask*> g_Scheduled;

TMutex g_Lock;
TCondVar g_CondVar;
TDeque<TMyTask*> g_Incoming;
TVector<TQueuePtr> g_SchedulerQs;
THolder<NShop::TFlowCtl> g_Fc;
THolder<TDRRScheduler<TMyQueue>> g_Drr;

TAtomic g_IdleTime = 0; // in microsec
TAtomic g_ActiveTime = 0; // in microsec

void Arrive(TMyTask* task)
{
    TGuard<TMutex> g(g_Lock);
    bool wasOpen = g_Fc->IsOpen();
    g_Fc->Arrive(task->FcOp, task->Cost, Now());
    if (!wasOpen && g_Fc->IsOpen()) {
        g_CondVar.BroadCast();
    }
}

void Depart(TMyTask* task)
{
    TGuard<TMutex> g(g_Lock);
    bool wasOpen = g_Fc->IsOpen();
    g_Fc->Depart(task->FcOp, task->RealCost, Now());
    if (!wasOpen && g_Fc->IsOpen()) {
        g_CondVar.BroadCast();
    }
}

void Enqueue(TMyTask* task)
{
    TGuard<TMutex> g(g_ScheduledLock);
    g_ScheduledCondVar.Signal();
    g_Scheduled.push_back(task);
}

TMyTask* Dequeue()
{
    TGuard<TMutex> g(g_ScheduledLock);
    while (g_Scheduled.empty()) {
        if (!g_ScheduledCondVar.WaitT(g_ScheduledLock, TDuration::Seconds(1))) {
            if (!g_Running) {
                return nullptr;
            }
        }
    }
    TMyTask* task = g_Scheduled.front();
    g_Scheduled.pop_front();
    return task;
}

struct TCompleteCmp {
    bool operator()(const TMyTask* lhs, const TMyTask* rhs) {
        return lhs->CompleteDeadline > rhs->CompleteDeadline;
    }
};

void PushToWaitHeap(TMyTask* task)
{
    TGuard<TMutex> g(g_CompleteLock);
    if (g_Complete.empty()) {
        g_CompleteCondVar.BroadCast();
    }
    g_Complete.push_back(task);
    PushHeap(g_Complete.begin(), g_Complete.end(), TCompleteCmp());
}

TMyTask* PopFromWaitHeap()
{
    TInstant deadline = TInstant::Zero();
    while (g_Running) {
        if (deadline != TInstant::Zero()) {
            TDuration waitTime = TInstant::Now() - deadline;
            Sleep(Min(waitTime, g_CompletePeriod));
        }

        TGuard<TMutex> g(g_CompleteLock);
        while (g_Complete.empty()) {
            if (!g_CompleteCondVar.WaitT(g_CompleteLock, TDuration::Seconds(1))) {
                if (!g_Running) {
                    return nullptr;
                }
            }
        }

        TMyTask* peek = g_Complete.front();
        TInstant now = TInstant::Now();
        if (peek->CompleteDeadline < now) {
            PopHeap(g_Complete.begin(), g_Complete.end(), TCompleteCmp());
            TMyTask* task = g_Complete.back();
            g_Complete.pop_back();
            return task;
        } else {
            deadline = peek->CompleteDeadline;
        }
    }
    return nullptr;
}

void Execute(TMyTask* task)
{
    // Emulate execution for real cost nanosaconds
    NHPTimer::STime timer;
    NHPTimer::GetTime(&timer);
    if (task->RealCost >= 2) {
        Sleep(TDuration::MicroSeconds((task->RealCost - 1)));
    }
    double passed = 0.0;
    while (passed * 1000000ull < task->RealCost) {
        passed += NHPTimer::GetTimePassed(&timer);
    }

    // Time measurements
    double execTime = NHPTimer::GetTimePassed(&task->Timer);
    task->TotalTime += execTime;
    LWPROBE(ExecuteRequest, task->Id, task->Queue->Name, execTime);

    // Complete request
    double wait = GaussSumRandom(g_WaitSec, Y_ARRAY_SIZE(g_WaitSec));
    task->CompleteDeadline = TInstant::Now()
        + TDuration::MicroSeconds(ui64(wait * 1000000.0));
    PushToWaitHeap(task);
}

void* CompleteThread(void*)
{
    double lastMonSec = Now();
    ui64 lastIdleTime = 0;
    ui64 lastActiveTime = 0;
    while (TMyTask* task = PopFromWaitHeap()) {
        double waitTime = NHPTimer::GetTimePassed(&task->Timer);
        task->TotalTime += waitTime;
        LWPROBE(CompleteRequest, task->Id, task->Queue->Name, waitTime,
                task->TotalTime, task->TotalTime - task->SchedTime,
                double(task->Cost)/1000000.0);
        Depart(task);
        delete task;

        // Utilization monitoring
        double now = Now();
        if (lastMonSec + 1.0 < now) {
            ui64 idleTime = AtomicGet(g_IdleTime);
            ui64 activeTime = AtomicGet(g_ActiveTime);
            ui64 idleDelta = idleTime - lastIdleTime;
            ui64 activeDelta = activeTime - lastActiveTime;
            ui64 elapsed = idleDelta + activeDelta;
            double utilization = (elapsed == 0? 0: double(activeDelta) / elapsed);
            lastIdleTime = idleTime;
            lastActiveTime = activeTime;
            LWPROBE(SystemStats, utilization);
            lastMonSec = now;
        }
    }
    return nullptr;
}

void* WorkerThread(void*)
{
    NHPTimer::STime workerTimer;
    NHPTimer::GetTime(&workerTimer);
    while (TMyTask* task = Dequeue()) {
        double workerIdleTime = NHPTimer::GetTimePassed(&workerTimer);
        double queueTime = NHPTimer::GetTimePassed(&task->Timer);
        task->TotalTime += queueTime;
        LWPROBE(DequeueRequest, task->Id, task->Queue->Name, queueTime);
        Execute(task);
        double workerActiveTime = NHPTimer::GetTimePassed(&workerTimer);
        LWPROBE(WorkerStats, workerIdleTime, workerActiveTime,
                workerIdleTime + workerActiveTime);
        AtomicAdd(g_IdleTime, workerIdleTime * 1000000);
        AtomicAdd(g_ActiveTime, workerActiveTime * 1000000);
    }
    return nullptr;
}

void* SchedulerThread(void*)
{
    while (g_Running) {
        TMyTask* task = g_Drr->PeekTask();
        if (task) {
            g_Drr->PopTask();
            task->SchedTime = NHPTimer::GetTimePassed(&task->Timer);
            task->TotalTime += task->SchedTime;
            LWPROBE(ScheduleRequest, task->Id, task->Queue->Name,
                    task->SchedTime);
            Arrive(task);
            Enqueue(task);
        }

        TGuard<TMutex> g(g_Lock);
        while (!g_Fc->IsOpen() || (!g_Drr->PeekTask() && g_Incoming.empty())) {
            g_CondVar.WaitI(g_Lock);
        }

        for (TMyTask* task : g_Incoming) {
            TMyQueue* q = g_SchedulerQs[task->Type].get();
            q->PushTask(task);
        }
        g_Incoming.clear();
    }
    return nullptr;
}

void* GenerateThread(void*)
{
    while (g_Running) {
        // Wait some random time
        Sleep(TDuration::Seconds(
            ExpRandom(1.0/g_AvgPeriodSec, g_MaxPeriodSec)));

        // Generate some random task
        size_t type = RandomNumber<size_t>(g_SchedulerQs.size());
        TUCost cost = 1000000ull * GaussSumRandom(g_CostSec, Y_ARRAY_SIZE(g_CostSec));
        TUCost realcost = cost * (0.5 + 1.5 * RandomNumber<double>()); // cost;
        TMyTask* task = new TMyTask(cost, realcost, type, ++g_LastRequestId);

        // Push task into incoming queue
        TGuard<TMutex> g(g_Lock);
        if (g_Incoming.empty()) {
            g_CondVar.BroadCast();
        }
        g_Incoming.push_back(task);
    }
    return nullptr;
}

} // namespace NFcSim

class TMachineMonPage : public NMonitoring::IMonPage {
public:
    TMachineMonPage()
        : IMonPage("dashboard", "Dashboard")
    {}
    virtual void Output(NMonitoring::IMonHttpRequest& request) {
        const char* urls[] = {
            "/trace?fullscreen=y&aggr=hist&autoscale=y&refresh=3000&bn=queueTimeSec&id=.SIMFC_PROVIDER.DequeueRequest.d1s&linesfill=y&mode=analytics&out=flot&pointsshow=n&xn=queueTimeSec&y1=0&x1=0&yns=_count_share",
            "/trace?fullscreen=y&aggr=hist&autoscale=y&refresh=3000&bn=execTimeSec&id=.SIMFC_PROVIDER.ExecuteRequest.d1s&linesfill=y&mode=analytics&out=flot&pointsshow=n&xn=execTimeSec&y1=0&x1=0&yns=_count_share",
            "/trace?fullscreen=y&aggr=hist&autoscale=y&refresh=3000&bn=waitTimeSec&id=.SIMFC_PROVIDER.CompleteRequest.d1s&linesfill=y&mode=analytics&out=flot&pointsshow=n&xn=waitTimeSec&y1=0&x1=0&yns=_count_share",

            "/trace?fullscreen=y&id=.SHOP_PROVIDER.Arrive.d200ms&mode=analytics&out=flot&xn=_thrRTime&y1=0&yns=costInFly",
            "/trace?fullscreen=y&aggr=hist&autoscale=y&refresh=3000&bn=procTimeSec&id=.SIMFC_PROVIDER.CompleteRequest.d1s&linesfill=y&mode=analytics&out=flot&pointsshow=n&xn=procTimeSec&y1=0&x1=0&yns=_count_share",
            "/trace?fullscreen=y&aggr=hist&autoscale=y&refresh=3000&bn=schedulerTimeSec&id=.SIMFC_PROVIDER.ScheduleRequest.d1s&linesfill=y&mode=analytics&out=flot&pointsshow=n&xn=schedulerTimeSec&y1=0&x1=0&yns=_count_share",

            "/trace?fullscreen=y&id=.SHOP_PROVIDER.Arrive.d200ms&mode=analytics&out=flot&xn=_thrRTime&y1=0&yns=countInFly",
            "/trace?fullscreen=y&id=.SIMFC_PROVIDER.DequeueRequest.d200ms&mode=analytics&out=flot&xn=_thrRTime&yns=queueTimeSec&y0=0&pointsshow=n&cutts=y",
            "/trace?fullscreen=y&id=.Group.ShopFlowCtlPeriod.d10m&mode=analytics&out=flot&pointsshow=n&xn=periodId&yns=badPeriods:goodPeriods:zeroPeriods",

            "/trace?fullscreen=y&g=periodId&id=.Group.ShopFlowCtlPeriod.d10m&mode=analytics&out=flot&pointsshow=n&xn=periodId&y1=0&yns=dT_T:dL_L",
            "/trace?fullscreen=y&g=periodId&id=.Group.ShopFlowCtlPeriod.d10m&mode=analytics&out=flot&pointsshow=n&xn=periodId&y1=0&yns=pv",
            "/trace?fullscreen=y&g=periodId&id=.Group.ShopFlowCtlPeriod.d10m&linesfill=y&mode=analytics&out=flot&xn=periodId&y1=0&yns=state:window&pointsshow=n",

            "/trace?fullscreen=y&g=periodId&id=.Group.ShopFlowCtlPeriod.d10m&mode=analytics&out=flot&xn=periodId&y1=-1&y2=1&yns=error",
            "/trace?fullscreen=y&g=periodId&id=.Group.ShopFlowCtlPeriod.d10m&linesshow=n&mode=analytics&out=flot&x1=0&xn=window&y1=0&yns=throughput&cutts=y",
            "/trace?fullscreen=y&g=periodId&id=.Group.ShopFlowCtlPeriod.d10m&mode=analytics&out=flot&xn=periodId&y1=0&yns=throughput:throughputMin:throughputMax:throughputLo:throughputHi&pointsshow=n&legendshow=n",

            "/trace?fullscreen=y&g=periodId&id=.Group.ShopFlowCtlPeriod.d10m&mode=analytics&out=flot&xn=periodId&yns=mode",
            "/trace?fullscreen=y&g=periodId&id=.Group.ShopFlowCtlPeriod.d10m&linesshow=n&mode=analytics&out=flot&x1=0&xn=window&y1=0&yns=latencyAvgMs&cutts=y",
            "/trace?fullscreen=y&g=periodId&id=.Group.ShopFlowCtlPeriod.d10m&mode=analytics&out=flot&pointsshow=n&xn=periodId&y1=0&yns=latencyAvgMs:latencyAvgMinMs:latencyAvgMaxMs:latencyAvgLoMs:latencyAvgHiMs&legendshow=n",

            "/trace?fullscreen=y&id=.SIMFC_PROVIDER.SystemStats.d10m&mode=analytics&out=flot&xn=_thrRTime&yns=utilization&linesfill=y&y1=0&y2=1&pointsshow=n",
            "/trace?fullscreen=y&autoscale=y&id=.SIMFC_PROVIDER.WorkerStats.d1s&mode=analytics&out=flot&xn=_thrRTime&yns=activeTimeSec-stack:totalTimeSec-stack&pointsshow=n&linesfill=y&cutts=y&legendshow=n",
            "/trace?fullscreen=y&id=.SHOP_PROVIDER.Depart.d1s&mode=analytics&out=flot&xn=realCost&yns=estCost&linesshow=n"
        };

        TStringStream out;
        out << NMonitoring::HTTPOKHTML;
        HTML(out) {
            out << "<!DOCTYPE html>" << Endl;
            HTML_TAG() {
                //HEAD()
                BODY() {
                    out << "<table border=\"0\" width=\"100%\"><tr>";
                    for (size_t i = 0; i < Y_ARRAY_SIZE(urls); i++) {
                        if (i > 0 && i % 3 == 0) {
                            out << "</tr><tr>";
                        }
                        out << "<td><iframe style=\"border:none;width:100%;height:100%\" src=\""
                            << urls[i]
                            << "\"></iframe></td>";
                    }
                    out << "</tr></table>";
                }
            }
        }
        request.Output() << out.Str();
    }
};

int main(int argc, char** argv)
{
    using namespace NFcSim;
    try {
        NLWTrace::StartLwtraceFromEnv();
#ifdef _unix_
        signal(SIGPIPE, SIG_IGN);
#endif

#ifdef _win32_
        WSADATA dummy;
        WSAStartup(MAKEWORD(2,2), &dummy);
#endif

        // Configure
        using TMonSrvc = NMonitoring::TMonService2;
        THolder<TMonSrvc> MonSrvc;
        NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();
        opts.AddLongOption(0, "mon-port", "port of monitoring service")
                .RequiredArgument("port")
                .StoreResult(&g_MonPort, g_MonPort);
        NLastGetopt::TOptsParseResult res(&opts, argc, argv);

        // Init monservice
        MonSrvc.Reset(new TMonSrvc(g_MonPort));
        MonSrvc->Register(new TMachineMonPage());
        NLwTraceMonPage::RegisterPages(MonSrvc->GetRoot());
        NLwTraceMonPage::ProbeRegistry().AddProbesList(
            LWTRACE_GET_PROBES(SIMFC_PROVIDER));
        NLwTraceMonPage::ProbeRegistry().AddProbesList(
            LWTRACE_GET_PROBES(SHOP_PROVIDER));
        g_Probes = &NLwTraceMonPage::ProbeRegistry();
        g_TraceMngr = &NLwTraceMonPage::TraceManager();

        // Start monservice
        MonSrvc->Start();

        // Initialization
        g_SchedulerQs.push_back(TQueuePtr(new TMyQueue("A:rdc")));
        g_SchedulerQs.push_back(TQueuePtr(new TMyQueue("B:map")));
        g_Fc.Reset(new NShop::TFlowCtl(g_FcCfg, NFcSim::Now()));
        g_Drr.Reset(new TDRRScheduler<TMyQueue>(g_QuantumNs));
        for (TQueuePtr q : g_SchedulerQs) {
            g_Drr->AddQueue(q->Name, q);
        }


        // Start all threads
        TThread completeThread(&CompleteThread, nullptr);
        TThread generateThread(&GenerateThread, nullptr);
        TThread workerThread1(&WorkerThread, nullptr);
        TThread workerThread2(&WorkerThread, nullptr);
        TThread workerThread3(&WorkerThread, nullptr);
        TThread workerThread4(&WorkerThread, nullptr);
        TThread workerThread5(&WorkerThread, nullptr);
        TThread workerThread6(&WorkerThread, nullptr);
        TThread workerThread7(&WorkerThread, nullptr);
        TThread workerThread8(&WorkerThread, nullptr);
        TThread workerThread9(&WorkerThread, nullptr);
        TThread workerThread0(&WorkerThread, nullptr);
        completeThread.Start();
        generateThread.Start();
        workerThread1.Start();
        workerThread2.Start();
        workerThread3.Start();
        workerThread4.Start();
        workerThread5.Start();
        workerThread6.Start();
        workerThread7.Start();
        workerThread8.Start();
        workerThread9.Start();
        workerThread0.Start();
        SchedulerThread(nullptr);

        // Finish
        g_Running = false;
        Cout << "bye" << Endl;
        return 0;
    } catch (...) {
        Cerr << "failure: " << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
