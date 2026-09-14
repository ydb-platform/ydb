#include "flowctl.h"
#include "probes.h"

#include <util/generic/utility.h>
#include <util/system/yassert.h>

#include <cmath>

namespace NShop {

LWTRACE_USING(SHOP_PROVIDER);

TFlowCtlCounters TFlowCtl::FakeCounters; // instantiate static object

TFlowCtl::TFlowCtl(ui64 initialWindow)
    : Window(initialWindow)
    , State(Window)
    , Counters(&FakeCounters)
{
    UpdateThreshold();
}

TFlowCtl::TFlowCtl(const TFlowCtlConfig& cfg, double now, ui64 initialWindow)
    : TFlowCtl(initialWindow)
{
    Configure(cfg, now);
}

void TFlowCtl::Validate(const TFlowCtlConfig& cfg)
{
#define TLFC_VERIFY(cond, ...) \
    do { \
        if (!(cond)) ythrow yexception() << Sprintf(__VA_ARGS__); \
    } while (false) \
    /**/

    TLFC_VERIFY(cfg.GetPeriodDuration() > 0,
                "PeriodDuration must be positive, got %lf",
                (double)cfg.GetPeriodDuration());
    TLFC_VERIFY(cfg.GetMeasurementError() > 0 && cfg.GetMeasurementError() < 1,
                "MeasurementError must be in (0, 1) range, got %lf",
                (double)cfg.GetMeasurementError());
    TLFC_VERIFY(cfg.GetSteadyLimit() > 0 && cfg.GetSteadyLimit() < 1,
                "SteadyLimit must be in (0, 1) range, got %lf",
                (double)cfg.GetSteadyLimit());
    TLFC_VERIFY(cfg.GetHistorySize() > 1,
                "HistorySize must be at least 2, got %ld",
                (long)cfg.GetHistorySize());
    TLFC_VERIFY(cfg.GetMinCountInFly() > 0,
                "MinCountInFly must be at least 1, got %ld",
                (long)cfg.GetMinCountInFly());
    TLFC_VERIFY(cfg.GetMinCostInFly() > 0,
                "MinCostInFly must be at least 1, got %ld",
                (long)cfg.GetMinCostInFly());
    TLFC_VERIFY(cfg.GetMultiplierLimit() > 0 && cfg.GetMultiplierLimit() < 1,
                "MultiplierLimit must be in (0, 1) range, got %lf",
                (double)cfg.GetMultiplierLimit());

#undef TLFC_VERIFY
}

void TFlowCtl::Configure(const TFlowCtlConfig& cfg, double now)
{
    if (cfg.GetFixedWindow()) {
        Window = cfg.GetFixedWindow();
    }
    Window = Max(Window, cfg.GetMinCostInFly());
    State = Max(State, (double)cfg.GetMinCostInFly());
    UpdateThreshold();
    if (Period.empty()) { // if brand new TFlowCtl
        Cfg = cfg;
        Period.resize(Cfg.GetHistorySize());
        ConfigureTime = now;
        SlowStart = Cfg.GetSlowStartLimit();
        AdvanceTime(now, false);
    } else { // if reconfiguration
        AdvanceTime(now, false);

        // Pretend that current period was started with new config
        Cfg = cfg;
        TPeriod* cp = CurPeriod();
        cp->PeriodDuration = Cfg.GetPeriodDuration();
        ConfigureTime = cp->StartTime;
        ConfigurePeriodId = CurPeriodId;

        // Create new history
        TVector<TPeriod> oldPeriod;
        oldPeriod.swap(Period);
        Period.resize(Cfg.GetHistorySize());

        // Rearrange old history into new one using new modulo
        // and fill old periods with zeros if history was enlarged
        ui64 id = CurPeriodId;
        for (ui64 j = Min(Period.size(), oldPeriod.size()); j > 0 && id != ui64(-1); j--, id--) {
            *GetPeriod(id) = oldPeriod[id % oldPeriod.size()];
        }
        for (ui64 j = oldPeriod.size(); j < Period.size() && id != ui64(-1); j++, id--) {
            InitPeriod(id, 0);
        }
    }
    SetWindow(Window); // update CostCap
    LWPROBE(Configure, GetName(), Now, cfg.ShortDebugString());
}

bool TFlowCtl::IsOpen() const
{
    return Cfg.GetDisabled() || CountInFly < Cfg.GetMinCountInFly() || CostInFly < Window;
}

bool TFlowCtl::IsOpenForMonitoring() const
{
    return Cfg.GetDisabled() || CostInFly < WindowCloseThreshold;
}

enum class EMode {
    Zero = 0,
    Restart = 1,
    Unused = 2,
    SlowStart = 3,
    Optimum = 4,
    Unsteady = 5,
    Undispersive = 6,
    WindowInc = 7,
    WindowDec = 8,
    Overshoot = 9,
    CatchUp = 10,
    FixedWindow = 11,
    FixedLatency = 12
};

void TFlowCtl::Arrive(TFcOp& op, ui64 estcost, double now)
{
    TPeriod* p = AdvanceTime(now, true);
    p->AccumulateClosedTime(now, IsOpen());

    op.EstCost = estcost;
    op.UsedCost = Min(estcost, CostCap);
    op.ArriveTime = Now;
    op.PeriodId = CurPeriodId;
    op.OpId = ++LastOpId;

    p->CountInFly++;
    p->CostInFly += op.EstCost;

    LWPROBE(Arrive, GetName(), op.OpId, op.PeriodId, Now,
            op.EstCost, op.UsedCost, CountInFly, CostInFly, Window);

    CountInFly++;
    CostInFly += op.UsedCost;
    if (KleinrockControlEnabled() && CountInFly <= Cfg.GetMinCountInFly() && State < CostInFly) {
        ui64 minCost = RealCostMA * Cfg.GetMinCountInFly();
        if (State < minCost) {
            SetWindow(minCost);
            State = Window;
            UpdateThreshold();
        }
    }

    AtomicIncrement(Counters->CountArrived);
    AtomicAdd(Counters->CostArrived, op.EstCost);
}

void TFlowCtl::Depart(TFcOp& op, ui64 realcost, double now)
{
    Y_ABORT_UNLESS(Now >= op.ArriveTime);
    Y_ABORT_UNLESS(CurPeriodId >= op.PeriodId);

    TPeriod* p = AdvanceTime(now, true);
    p->AccumulateClosedTime(now, IsOpen());

    CountInFly--;
    CostInFly -= op.UsedCost;

    // Weight is coef of significance of given op in period performance metrics
    // we use 1, 1/2, 1/3... weight if ops was execute during 1, 2, 3... periods
    double weight = 1.0 / (1 + CurPeriodId - op.PeriodId);

    double latency = Now - op.ArriveTime;
    LatencyMA = latency * Cfg.GetLatencyMACoef()
              + LatencyMA * (1 - Cfg.GetLatencyMACoef());
    RealCostMA = realcost * Cfg.GetRealCostMACoef()
               + RealCostMA * (1 - Cfg.GetRealCostMACoef());

    LWPROBE(Depart, GetName(), op.OpId, op.PeriodId, Now,
            op.EstCost, op.UsedCost, CountInFly, CostInFly, realcost,
            LatencyMA * 1000, weight, Window);

    LWPROBE(DepartLatency, GetName(), op.OpId, op.PeriodId, Now,
            latency * 1000, LatencyMA * 1000, RealCostMA, op.ArriveTime);

    // Iterate over all periods that intersect execution of operation
    if (op.PeriodId + Cfg.GetHistorySize() > CurPeriodId) {
        p = GetPeriod(op.PeriodId);
        p->CountInFly--;
        p->CostInFly -= op.EstCost;
    } else {
        p = LastPeriod();
        AtomicIncrement(Counters->CountDepartedLate);
        AtomicAdd(Counters->CostDepartedLate, realcost);
    }
    TPeriod* curPeriod = CurPeriod();
    while (true) {
        p->WeightSum += weight;
        p->RealCostSum += weight * realcost;
        p->EstCostSum += weight * op.EstCost;
        p->LatencySum += weight * LatencyMA;
        p->LatencySqSum += weight * LatencyMA * LatencyMA;
        if (p == curPeriod) {
            break;
        }
        p = NextPeriod(p);
    }

    AtomicIncrement(Counters->CountDeparted);
    AtomicAdd(Counters->CostDeparted, realcost);
}

void TFlowCtl::Abort(TFcOp& op)
{
    CountInFly--;
    CostInFly -= op.UsedCost;

    // Abort op in period it arrived (if it is not forgotten yet)
    if (op.PeriodId + Cfg.GetHistorySize() > CurPeriodId) {
        TPeriod* p = GetPeriod(op.PeriodId);
        p->CountInFly--;
        p->CostInFly -= op.EstCost;
    }

    AtomicIncrement(Counters->CountAborted);
    AtomicAdd(Counters->CostAborted, op.EstCost);

    LWPROBE(Abort, GetName(), op.OpId, op.PeriodId, Now,
            op.EstCost, op.UsedCost, CountInFly, CostInFly);
}

TFlowCtl::EStateTransition TFlowCtl::ConfigureST(const TFlowCtlConfig& cfg, double now)
{
    bool wasOpen = IsOpen();
    Configure(cfg, now);
    return CreateST(wasOpen, IsOpen());
}

TFlowCtl::EStateTransition TFlowCtl::ArriveST(TFcOp& op, ui64 estcost, double now)
{
    bool wasOpen = IsOpen();
    Arrive(op, estcost, now);
    return CreateST(wasOpen, IsOpen());
}

TFlowCtl::EStateTransition TFlowCtl::DepartST(TFcOp& op, ui64 realcost, double now)
{
    bool wasOpen = IsOpen();
    Depart(op, realcost, now);
    return CreateST(wasOpen, IsOpen());
}

TFlowCtl::EStateTransition TFlowCtl::AbortST(TFcOp& op)
{
    bool wasOpen = IsOpen();
    Abort(op);
    return CreateST(wasOpen, IsOpen());
}

void TFlowCtl::UpdateCounters()
{
    AdvanceMonitoring();
    AtomicSet(Counters->CostInFly, CostInFly);
    AtomicSet(Counters->CountInFly, CountInFly);
    AtomicSet(Counters->Window, Window);
}

void TFlowCtl::SetWindow(ui64 window)
{
    Window = window;
    CostCap = Max<ui64>(Cfg.GetMinCostInFly(), ui64(ceil(double(window) * Cfg.GetCostCapToWindow())));
}

TFlowCtl::EStateTransition TFlowCtl::CreateST(bool wasOpen, bool isOpen)
{
    if (wasOpen) {
        if (!isOpen) {
            LWPROBE(TransitionClosed, GetName(), Now);
            return Closed;
        }
    } else {
        if (isOpen) {
            LWPROBE(TransitionOpened, GetName(), Now);
            return Opened;
        }
    }
    return None;
}

TFlowCtl::TPeriod* TFlowCtl::AdvanceTime(double now, bool control)
{
    AdvanceMonitoring();

    LWPROBE(AdvanceTime, GetName(), Now, now);

    if (Now == 0) { // Initialization on first call
        Now = now;
        InitPeriod();
    }

    if (Now < now) {
        Now = now;
    }

    TPeriod* cp = CurPeriod();
    double end = cp->StartTime + cp->PeriodDuration;
    if (end < Now) {
        // Take into account zero periods (skipped; no arrival/departure)
        // TODO: You should test this!! look in debugger!! better write UT
        size_t skipPeriods = (Now - end) / Cfg.GetPeriodDuration();
        size_t addPeriods = Min(skipPeriods, Period.size());
        CurPeriodId += skipPeriods - addPeriods;
        while (addPeriods-- > 0) {
            CurPeriodId++;
            cp = InitPeriod();
        }

        // Adjust window before starting new period, because we want have as
        // much historic information as possible, including oldest period
        if (control) {
            Control();
        }

        // Start period with new value of Window
        CurPeriodId++;
        cp = InitPeriod();
    }

    return cp;
}

TFlowCtl::TPeriod* TFlowCtl::GetPeriod(ui64 periodId)
{
    Y_ABORT_UNLESS(!Period.empty(), "TFlowCtl must be configured");
    return &Period[periodId % Cfg.GetHistorySize()];
}

TFlowCtl::TPeriod* TFlowCtl::CurPeriod()
{
    return GetPeriod(CurPeriodId);
}

TFlowCtl::TPeriod* TFlowCtl::LastPeriod()
{
    if (CurPeriodId < Cfg.GetHistorySize()) {
        return &Period[0];
    } else {
        return GetPeriod(CurPeriodId + 1);
    }
}

TFlowCtl::TPeriod* TFlowCtl::NextPeriod(TFlowCtl::TPeriod* p)
{
    p++;
    if (p == &Period[0] + Cfg.GetHistorySize()) {
        p = &Period[0];
    }
    return p;
}

TFlowCtl::TPeriod* TFlowCtl::InitPeriod(ui64 periodId, ui64 window)
{
    TPeriod* cp = GetPeriod(periodId);
    *cp = TPeriod();
    cp->PeriodId = periodId;
    cp->StartTime = ConfigureTime +
            (periodId - ConfigurePeriodId) * Cfg.GetPeriodDuration();
    cp->PeriodDuration = Cfg.GetPeriodDuration();
    cp->Window = window;
    return cp;
}

TFlowCtl::TPeriod* TFlowCtl::InitPeriod()
{
    TPeriod* cp = InitPeriod(CurPeriodId, Window);
    LWPROBE(InitPeriod, GetName(), CurPeriodId, Now, Window);
    return cp;
}

bool TFlowCtl::KleinrockControlEnabled()
{
    return !Cfg.GetFixedWindow() && Cfg.GetFixedLatencyMs() <= 0.0;
}

void TFlowCtl::Control()
{
    // Iterate over completed periods
    TPeriod* p = LastPeriod();
    TPeriod* curPeriod = CurPeriod();
    double costSumError = 0;
    double weightSumError = 0;
    double latencySumError = 0;
    double latencySqSumError = 0;
    ui64 goodPeriods = 0;
    ui64 badPeriods = 0;
    ui64 zeroPeriods = 0;
    double rthroughputSum = 0;
    double ethroughputSum = 0;
    double latencyAvgSum = 0;
    double rthroughputSqSum = 0;
    double ethroughputSqSum = 0;
    double latencyAvgSqSum = 0;
    double rthroughputMin = -1.0;
    double rthroughputMax = -1.0;
    double ethroughputMin = -1.0;
    double ethroughputMax = -1.0;
    double latencyAvgMin = -1.0;
    double latencyAvgMax = -1.0;
    // Note that curPeriod is not included, because it can have or not have
    // higher error, and could lead to `goodPeriods' blinking
    for (; p != curPeriod; p = NextPeriod(p)) {
        // It is a good approximation to think that op will continue it's
        // execution for as long as it already executes, so it's weight will be
        // about twice less than if op will complete in current period
        ui64 periodsToComplete = 2*(CurPeriodId - p->PeriodId);
        double weightEst = 1.0 / periodsToComplete;
        double latencyEst = p->PeriodDuration * periodsToComplete;

        // Error accumulates over uncompleted ops of periods before `p'
        costSumError += p->CostInFly * weightEst;
        weightSumError += p->CountInFly * weightEst;
        latencySumError += p->CountInFly * weightEst * latencyEst;
        latencySqSumError += p->CountInFly * weightEst * latencyEst * latencyEst;

        // Take in-fly operations into account to increase accuracy
        double rcostSum = p->RealCostSum + costSumError;
        double ecostSum = p->EstCostSum + costSumError;
        double weightSum = p->WeightSum + weightSumError;
        double latencySum = p->LatencySum + latencySumError;
        double latencySqSum = p->LatencySqSum + latencySqSumError;

        LWPROBE(PeriodStats, GetName(), CurPeriodId, Now, p->PeriodId,
                rcostSum, costSumError,
                weightSum, weightSumError,
                latencySum, latencySumError);
        if (rcostSum == 0 || weightSum == 0) {
            zeroPeriods++;
        } else if (costSumError / rcostSum < Cfg.GetMeasurementError() && // cost
            weightSumError / weightSum < Cfg.GetMeasurementError() // count
        ) {
            // Period performace metrics at best we can estimate for now
            double latencyAvg = latencySum / weightSum;
            double latencyErr = sqrt(latencySqSum / weightSum - latencyAvg * latencyAvg);
            double rthroughput = rcostSum / p->PeriodDuration;
            double ethroughput = ecostSum / p->PeriodDuration;

            LWPROBE(PeriodGood, GetName(), CurPeriodId, Now, p->PeriodId,
                    goodPeriods, rthroughput, ethroughput, latencyAvg, latencyErr);

            // Aggregate performance metrics over all "good" periods
            goodPeriods++;
            latencyAvgSum += latencyAvg;
            rthroughputSum += rthroughput;
            ethroughputSum += ethroughput;
            latencyAvgSqSum += latencyAvg * latencyAvg;
            rthroughputSqSum += rthroughput * rthroughput;
            ethroughputSqSum += ethroughput * ethroughput;
            if (rthroughputMax < 0) { // For first pass
                rthroughputMin = rthroughputMax = rthroughput;
                ethroughputMin = ethroughputMax = ethroughput;
                latencyAvgMin = latencyAvgMax = latencyAvg;
            } else {
                rthroughputMin = Min(rthroughput, rthroughputMin);
                rthroughputMax = Max(rthroughput, rthroughputMax);
                ethroughputMin = Min(ethroughput, ethroughputMin);
                ethroughputMax = Max(ethroughput, ethroughputMax);
                latencyAvgMin = Min(latencyAvg, latencyAvgMin);
                latencyAvgMax = Max(latencyAvg, latencyAvgMax);
            }
        } else { // Not enough measurement accuracy
            badPeriods++;
        }
    }

    AtomicAdd(Counters->BadPeriods, badPeriods);
    AtomicAdd(Counters->GoodPeriods, goodPeriods);
    AtomicAdd(Counters->ZeroPeriods, zeroPeriods);
    LWPROBE(Measurement, GetName(), CurPeriodId, Now,
            badPeriods, goodPeriods, zeroPeriods);

    EMode mode = EMode::Zero; // Just for monitoring
    if (KleinrockControlEnabled() && goodPeriods == 0) {
        if (zeroPeriods == 0) {
            mode = EMode::Restart; // Probably we have huge window, restart
            SetWindow(Cfg.GetMinCostInFly());
            State = Window;
            SlowStart = Cfg.GetSlowStartLimit();
            // NOTE: PeriodDuration maybe lower than latency.
            // NOTE: If this is the case try to manually choose PeriodDuration
            // NOTE: to be much higher (x20) than average latency
        } else {
            mode = EMode::Unused; // Looks like utilization is zero mainly, wait
        }
    } else {
        double L = latencyAvgSum / goodPeriods;
        double T = rthroughputSum / goodPeriods;
        double ET = ethroughputSum / goodPeriods;
        double dT = sqrt(rthroughputSqSum / goodPeriods - T*T);
        double dET = sqrt(ethroughputSqSum / goodPeriods - ET*ET);
        double dL = sqrt(latencyAvgSqSum / goodPeriods - L*L);

        LWPROBE(Throughput, GetName(), CurPeriodId, Now,
                T, rthroughputMin, rthroughputMax, T-dT, T+dT, dT/T);
        LWPROBE(EstThroughput, GetName(), CurPeriodId, Now,
                ET, ethroughputMin, ethroughputMax, ET-dET, ET+dET, dET/ET);
        LWPROBE(Latency, GetName(), CurPeriodId, Now,
                L*1000.0, latencyAvgMin*1000.0, latencyAvgMax*1000.0, (L-dL)*1000.0, (L+dL)*1000.0, dL/L);

        // Process variable = abs latency error over abs throughput error
        double pv;
        if (dT/T < 1e-6) {
            pv = 1e6;
        } else {
            pv = (dL/L) / (dT/T); // We want pv -> 1
        }

        // Error. Maps pv=0 -> e=1, pv=1 -> e=0 and pv=+inf -> e=-1
        // Intention is to make throughput and latency symetric for controller
        double err = (pv < 1.0? 1.0 - pv: 1.0/pv - 1.0); // We want e -> 0

        if (!Cfg.GetFixedWindow() && Cfg.GetFixedLatencyMs() <= 0.0 && SlowStart) {
            mode = EMode::SlowStart; // In SlowStart mode we always multiply window by max allowed coef
            State *= 1.0 + Cfg.GetMultiplierLimit();
            SetWindow(State);
            SlowStart--;
        } else {
            // Exclude NaNs and zeros
            if (dL >= 0 && L > 0 && dT >= 0 && T > 0 && pv >= 0) {
                AtomicSet(Counters->Throughput, T * 1e6);
                AtomicSet(Counters->Latency, L * 1e6);
                AtomicSet(Counters->ThroughputDispAbs, dT/T * 1e6);
                AtomicSet(Counters->LatencyDispAbs, dL/L * 1e6);
                LWPROBE(Slowdown, GetName(), CurPeriodId, Now, Slowdown);
                if (Slowdown) {
                    mode = EMode::Optimum; // Just wait if slowdown enabled
                    Slowdown--;
                } else {
                    if (dL/L > Cfg.GetSteadyLimit() ||
                        dT/T > Cfg.GetSteadyLimit())
                    {
                        mode = EMode::Unsteady; // Too unsteady, wait
                    } else {
                        if (dL/L < Cfg.GetMeasurementError() &&
                            dT/T < Cfg.GetMeasurementError())
                        { // Dispersions are lower than measurement accuracy
                            mode = EMode::Undispersive;
                        } else {
                            // Slowdown if we are close to setpoint (optimum)
                            if (Cfg.GetSlowdownLimit() &&
                                0.5 / Cfg.GetSlowdownLimit() < fabs(err) &&
                                fabs(err) < 0.5)
                            {
                                Slowdown = Min(
                                    Cfg.GetSlowdownLimit(),
                                    ui64(1.0/fabs(err)));
                            }

                            // Make controlling decision
                            // e-values closer to 0 lead to finer control
                            // due to e-square term
                            double ratio = 1.0 + Cfg.GetMultiplierLimit() *
                                    err * err * (pv < 1? 1.0: -1.0);
                            double target = T * L * ratio;

                            // System reaction has lag. Real position is T*L.
                            // In steady state with full window, Little's law:
                            //    T*L = Window
                            // So we have two exceptions:
                            //  - not full window due to underutilization
                            //  - not steady state due to reaction lag
                            if (target > 0.5 * State) {
                                mode = ratio > 1.0? EMode::WindowInc: EMode::WindowDec;
                                State *= ratio;
                            } else if (target > Cfg.GetMinCostInFly()
                                && ratio < 1.0)
                            {
                                mode = EMode::Overshoot; // Too large window
                                State *= 1.0 - Cfg.GetMultiplierLimit();
                            } else {
                                mode = EMode::CatchUp; // Catching up, wait
                            }

                            // Apply boundary conditions
                            if (State < Cfg.GetMinCostInFly()) {
                                State = Cfg.GetMinCostInFly();
                            }

                            LWPROBE(Control, GetName(), CurPeriodId, Now,
                                    pv, err, ratio, target);
                        }
                    }
                }
            }
            // Set window to osscilate near current state
            double disp = Cfg.GetMeasurementError() * 2.0;
            double mult = 1.0 - disp +
                    ((CurPeriodId * 2 / Cfg.GetHistorySize() / 3) % 2) * disp;
            SetWindow(mult * State);
            if (Window < Cfg.GetMinCostInFly()) {
                SetWindow(Cfg.GetMinCostInFly());
            }
            if (Cfg.GetFixedWindow()) {
                mode = EMode::FixedWindow; // Fixed window mode
                SetWindow(Cfg.GetFixedWindow());
                State = Window;
            } else if (Cfg.GetFixedLatencyMs() > 0.0) {
                mode = EMode::FixedLatency; // Fixed latency mode
                SetWindow(Max<ui64>(
                    Cfg.GetMinCostInFly(),
                    Cfg.GetFixedLatencyMs() * ethroughputMax / 1000.0));
                State = Window;
            }
            LWPROBE(Window, GetName(), CurPeriodId, Now, Window);
        }
        UpdateThreshold();
    }

    switch (mode) {
#define TLFC_MODE(name) \
    case EMode::name: AtomicIncrement(Counters->Mode ## name); break
    TLFC_MODE(Zero);
    TLFC_MODE(Restart);
    TLFC_MODE(Unused);
    TLFC_MODE(SlowStart);
    TLFC_MODE(Optimum);
    TLFC_MODE(Unsteady);
    TLFC_MODE(Undispersive);
    TLFC_MODE(WindowInc);
    TLFC_MODE(WindowDec);
    TLFC_MODE(Overshoot);
    TLFC_MODE(CatchUp);
    TLFC_MODE(FixedWindow);
    TLFC_MODE(FixedLatency);
    }
#undef TLFC_MODE
    LWPROBE(State, GetName(), CurPeriodId, Now, (ui64)mode, State);
}

void TFlowCtl::AdvanceMonitoring()
{
    if (LastEvent == 0) {
        LastEvent = GetCycleCount();
    }
    ui64 now = GetCycleCount();
    ui64 elapsed = 0;
    if (LastEvent < now) {
        elapsed = now - LastEvent;
        LastEvent = now;
    }
    ui64 elapsedUs = CyclesToDuration(elapsed).MicroSeconds();
    bool isIdle = CountInFly == 0;
    bool isOpen = IsOpenForMonitoring();
    if (!LastIdle && isIdle) {
        AtomicIncrement(Counters->IdleCount);
    }
    if (LastOpen && !isOpen) {
        AtomicIncrement(Counters->CloseCount);
    }
    if (isIdle) {
        AtomicAdd(Counters->IdleUs, elapsedUs);
    } else if (isOpen) {
        AtomicAdd(Counters->OpenUs, elapsedUs);
    } else {
        AtomicAdd(Counters->ClosedUs, elapsedUs);
    }
    LastIdle = isIdle;
    LastOpen = isOpen;
}

void TFlowCtl::UpdateThreshold()
{
    WindowCloseThreshold = Window * UtilizationThreshold / 100;
}

}
