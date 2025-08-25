#pragma once

#include <ydb/library/shop/protos/shop.pb.h>

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

namespace NShop {

struct TFlowCtlCounters {
    TAtomic CostArrived = 0;         // Total estimated cost arrived
    TAtomic CountArrived = 0;        // Total ops arrived
    TAtomic CostDeparted = 0;        // Total real cost departed
    TAtomic CountDeparted = 0;       // Total ops departed
    TAtomic CostDepartedLate = 0;    // Total real cost departed after history forget it
    TAtomic CountDepartedLate = 0;   // Total ops departed after history forget it
    TAtomic CostAborted = 0;         // Total estimated cost aborted
    TAtomic CountAborted = 0;        // Total ops aborted
    TAtomic CostInFly = 0;           // Currently estimated cost in flight
    TAtomic CountInFly = 0;          // Currently ops in flight

    TAtomic Window = 0;              // Current window size in estimated cost units
    TAtomic IdleCount = 0;           // Total switches to idle state (zero in-flight)
    TAtomic CloseCount = 0;          // Total switches to closed state (ecost in-flight >= 80% Window)
    TAtomic IdleUs = 0;              // Total microseconds in idle state
    TAtomic OpenUs = 0;              // Total microseconds in open state
    TAtomic ClosedUs = 0;            // Total microseconds in closed state

    // On every period boundary we analyze history up to `Cfg.HistorySize' periods into past
    // and based on amount and cost of still-in-fly requests we can estimate error
    // of throughtput and latency measurements, and than control it based on `Cfg.MeasurementError'
    // NOTE: one period is counted multiple times (Cfg.HistorySize) and can be classified differently
    TAtomic BadPeriods = 0;          // Total periods with too high measurement error
    TAtomic GoodPeriods = 0;         // Total periods with acceptable measurment error
    TAtomic ZeroPeriods = 0;         // Total periods w/o requests (not crossed by any request)

    // Performance metrics
    // Measured based on good period (up to `Cfg.HistorySize')
    TAtomic Throughput = 0;          // Measured throughput (micro-realcost per second)
    TAtomic Latency = 0;             // Measured latency (microseconds)
    TAtomic ThroughputDispAbs = 0;   // Measured throughput variability (ppm)
    TAtomic LatencyDispAbs = 0;      // Measured latency variability (ppm)

    // On every period flow control operates in one on the following mode:
    TAtomic ModeZero = 0;            // Undefined
    TAtomic ModeRestart = 0;         // Restart with minimal window due to every period being bad
    TAtomic ModeUnused = 0;          // Only zero and/or bad periods (just wait, probably idle)
    TAtomic ModeSlowStart = 0;       // Window exponential growth after Restart
    TAtomic ModeOptimum = 0;         // Optimal operating point (Kleinrock point)
    TAtomic ModeUnsteady = 0;        // Too high variability of latency and/or throughput
    TAtomic ModeUndispersive = 0;    // Too low variability of latency and throughput
    TAtomic ModeWindowInc = 0;       // Increase window
    TAtomic ModeWindowDec = 0;       // Decrease window
    TAtomic ModeOvershoot = 0;       // Too large window, decrease as fast as allowed
    TAtomic ModeCatchUp = 0;         // Catching up, wait

    // Do not apply smart flowctl, use `Cfg.FixedWindow'
    TAtomic ModeFixedWindow = 0;

    // Use max measured estimated cost throughput ETmax (estcost per second)
    // to compute `Window = ETmax * Cfg.FixedLatencyMs'
    TAtomic ModeFixedLatency = 0;
};


struct TFcOp {
    ui64 EstCost = 0; // Estimation of cost; should be known on arrive
    ui64 UsedCost = 0; // Min(EstCost, Window * Cfg.CostCapToWindow)
    double ArriveTime = 0;
    ui64 PeriodId = 0;
    ui64 OpId = 0;

    bool HasArrived() const
    {
        return OpId != 0;
    }

    void Reset()
    {
        OpId = 0;
    }
};

class TFlowCtl : public TAtomicRefCount<TFlowCtl> {
public:
    enum EStateTransition {
        None = 0,
        Closed = 1,
        Opened = 2
    };
private:
    TFlowCtlConfig Cfg;
    TString Name;

    // Flow state
    ui64 Window;
    ui64 CostCap;
    ui64 CostInFly = 0;
    ui64 CountInFly = 0;
    ui64 CurPeriodId = 0;
    ui64 LastOpId = 0;

    // Performance measurements
    struct TPeriod {
        // Constants during period
        ui64 PeriodId = 0;
        double StartTime = 0;
        double PeriodDuration = 0;
        ui64 Window = 0; // Window used for period

        // Intermediate and accumulated values
        ui64 CountInFly = 0; // Number of in fly ops arrived during period
        ui64 CostInFly = 0; // In-fly-cost of arrived during period operations
        double WeightSum = 0; // Total weight of executed operations
        double RealCostSum = 0; // Weighted sum of executed operations' real cost
        double EstCostSum = 0; // Weighted sum of executed operations' estimated cost
        double LatencySum = 0; // Weighted sum of executed operations' latencies
        double LatencySqSum = 0; // Weighted sum of executed operations' latency squares

        // Window utilization
        double LastEventTime = 0;
        double TotalClosedTime = 0;
        void AccumulateClosedTime(double now, bool wasOpen) {
            if (!LastEventTime) {
                LastEventTime = StartTime;
            }
            double end = StartTime + PeriodDuration;
            if (now > end) {
                now = end;
            }
            if (!wasOpen) {
                TotalClosedTime += now - LastEventTime;
            }
            LastEventTime = now;
        }
    };
    TVector<TPeriod> Period; // Cyclic buffer for last periods
    double Now = 0;
    double ConfigureTime = 0;
    double ConfigurePeriodId = 0;
    double LatencyMA = 0;
    double RealCostMA = 0;

    // Controller
    double State;
    ui64 Slowdown = 0;
    ui64 SlowStart = 0;

    // Monitoring
    TFlowCtlCounters* Counters;
    static TFlowCtlCounters FakeCounters;
    ui64 LastEvent = 0;
    bool LastOpen = true;
    bool LastIdle = true;

    // If `UtilizationThreshold' share of Window is used (in-fly) than
    // for monitoring we count window as closed. This is done to provide meaningful
    // `ClosedUs' counter. Note that if we count really !IsOpen() time
    // we'd get metric that depend on reaction time (just opened -> closed again)
    static const ui64 UtilizationThreshold = 80; // percent
    ui64 WindowCloseThreshold;

public:
    // Configuration
    explicit TFlowCtl(ui64 initialWindow = 1);
    TFlowCtl(const TFlowCtlConfig& cfg, double now, ui64 initialWindow = 1);
    // You'd better validate config before Configure() to avoid troubles
    static void Validate(const TFlowCtlConfig& cfg);
    void Configure(const TFlowCtlConfig& cfg, double now);

    // Processing
    bool IsOpen() const;
    bool IsOpenForMonitoring() const;
    void Arrive(TFcOp& op, ui64 estcost, double now);
    void Depart(TFcOp& op, ui64 realcost, double now);
    void Abort(TFcOp& op);

    // Adapters with state transitions (just sugar)
    EStateTransition ConfigureST(const TFlowCtlConfig& cfg, double now);
    EStateTransition ArriveST(TFcOp& op, ui64 estcost, double now);
    EStateTransition DepartST(TFcOp& op, ui64 realcost, double now);
    EStateTransition AbortST(TFcOp& op);

    // Acessors
    const TFlowCtlConfig& GetConfig() { return Cfg; }
    const TString& GetName() const { return Name; }
    void SetName(const TString& name) { Name = name; }

    // Monitoring
    void UpdateCounters();
    void SetCounters(TFlowCtlCounters* counters) { Counters = counters; }
private:
    void SetWindow(ui64 window);
    EStateTransition CreateST(bool wasOpen, bool isOpen);
    TPeriod* AdvanceTime(double now, bool control);
    TPeriod* GetPeriod(ui64 periodId);
    TPeriod* CurPeriod(); // Returns current period
    TPeriod* LastPeriod(); // Returns last remembered period
    TPeriod* NextPeriod(TPeriod* p); // Iterate periods
    TPeriod* InitPeriod(ui64 periodId, ui64 window);
    TPeriod* InitPeriod();
    bool KleinrockControlEnabled();
    void Control();
    void AdvanceMonitoring();
    void UpdateThreshold();
};

using TFlowCtlPtr = TIntrusivePtr<TFlowCtl>;

}
