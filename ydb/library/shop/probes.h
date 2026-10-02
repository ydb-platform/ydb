#pragma once

#include <library/cpp/lwtrace/all.h>
#include <util/system/hp_timer.h>

inline ui64 Duration(ui64 ts1, ui64 ts2)
{
    return ts2 > ts1? ts2 - ts1: 0;
}

inline double CyclesToMs(ui64 cycles)
{
    return double(cycles) * 1e3 / NHPTimer::GetCyclesPerSecond();
}

#define SHOP_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(Arrive, GROUPS("ShopFlowCtlOp"), \
      TYPES(TString, ui64, ui64, double, ui64, ui64, ui64, ui64, ui64), \
      NAMES("flowctl", "opId", "periodId", "now", "estCost", "usedCost", "countInFly", "costInFly", "window")) \
    PROBE(Depart, GROUPS("ShopFlowCtlOp", "ShopFlowCtlOpDepart"), \
      TYPES(TString, ui64, ui64, double, ui64, ui64, ui64, ui64, ui64, double, double, ui64), \
      NAMES("flowctl", "opId", "periodId", "now", "estCost", "usedCost", "countInFly", "costInFly", "realCost", "latencyMs", "weight", "window")) \
    PROBE(DepartLatency, GROUPS("ShopFlowCtlOpDepart"), \
      TYPES(TString, ui64, ui64, double, double, double, ui64, double), \
      NAMES("flowctl", "opId", "periodId", "now", "realLatencyMs", "latencyMAMs", "realCostMA", "arriveTime")) \
    PROBE(AdvanceTime, GROUPS(), \
      TYPES(TString, double, double), \
      NAMES("flowctl", "oldNow", "now")) \
    PROBE(Abort, GROUPS("ShopFlowCtlOp"), \
      TYPES(TString, ui64, ui64, double, ui64, ui64, ui64, ui64), \
      NAMES("flowctl", "opId", "periodId", "now", "estCost", "usedCost", "countInFly", "costInFly")) \
    PROBE(InitPeriod, GROUPS("ShopFlowCtlPeriod"), \
      TYPES(TString, ui64, double, ui64), \
      NAMES("flowctl", "periodId", "now", "window")) \
    PROBE(PeriodStats, GROUPS(), \
      TYPES(TString, ui64, double, ui64, double, double, double, double, double, double), \
      NAMES("flowctl", "periodId", "now", "statsPeriodId", "costSum", "costSumError", "weightSum", "weightSumError", "latencySum", "latencySumError")) \
    PROBE(PeriodGood, GROUPS(), \
      TYPES(TString, ui64, double, ui64, ui64, double, double, double, double), \
      NAMES("flowctl", "periodId", "now", "statsPeriodId", "goodPeriodIdx", "periodThroughput", "periodEThroughput", "periodLatencyAvgMs", "periodLatencyErrMs")) \
    PROBE(Measurement, GROUPS("ShopFlowCtlPeriod"), \
      TYPES(TString, ui64, double, ui64, ui64, ui64), \
      NAMES("flowctl", "periodId", "now", "badPeriods", "goodPeriods", "zeroPeriods")) \
    PROBE(Throughput, GROUPS("ShopFlowCtlPeriod"), \
      TYPES(TString, ui64, double, double, double, double, double, double, double), \
      NAMES("flowctl", "periodId", "now", "throughput", "throughputMin", "throughputMax", "throughputLo", "throughputHi", "dT_T")) \
    PROBE(EstThroughput, GROUPS("ShopFlowCtlPeriod"), \
      TYPES(TString, ui64, double, double, double, double, double, double, double), \
      NAMES("flowctl", "periodId", "now", "ethroughput", "ethroughputMin", "ethroughputMax", "ethroughputLo", "ethroughputHi", "dET_ET")) \
    PROBE(Latency, GROUPS("ShopFlowCtlPeriod"), \
      TYPES(TString, ui64, double, double, double, double, double, double, double), \
      NAMES("flowctl", "periodId", "now", "latencyAvgMs", "latencyAvgMinMs", "latencyAvgMaxMs", "latencyAvgLoMs", "latencyAvgHiMs", "dL_L")) \
    PROBE(Slowdown, GROUPS("ShopFlowCtlPeriod"), \
      TYPES(TString, ui64, double, ui64), \
      NAMES("flowctl", "periodId", "now", "waitSteady")) \
    PROBE(Control, GROUPS("ShopFlowCtlPeriod"), \
      TYPES(TString, ui64, double, double, double, double, double), \
      NAMES("flowctl", "periodId", "now", "pv", "error", "ratio", "target")) \
    PROBE(Window, GROUPS("ShopFlowCtlPeriod"), \
      TYPES(TString, ui64, double, ui64), \
      NAMES("flowctl", "periodId", "now", "window")) \
    PROBE(State, GROUPS("ShopFlowCtlPeriod"), \
      TYPES(TString, ui64, double, ui64, double), \
      NAMES("flowctl", "periodId", "now", "mode", "state")) \
    PROBE(TransitionClosed, GROUPS("FlowCtlTransition"), \
      TYPES(TString, double), \
      NAMES("flowctl", "now")) \
    PROBE(TransitionOpened, GROUPS("FlowCtlTransition"), \
      TYPES(TString, double), \
      NAMES("flowctl", "now")) \
    PROBE(Configure, GROUPS(), \
      TYPES(TString, double, TString), \
      NAMES("flowctl", "now", "config")) \
    \
    \
    \
    PROBE(StartJob, GROUPS("ShopJob"), \
      TYPES(TString, TString, ui64), \
      NAMES("shop", "flow", "job")) \
    PROBE(CancelJob, GROUPS("ShopJob"), \
      TYPES(TString, TString, ui64), \
      NAMES("shop", "flow", "job")) \
    PROBE(JobFinished, GROUPS("ShopJob"), \
      TYPES(TString, TString, ui64, double), \
      NAMES("shop", "flow", "job", "jobTimeMs")) \
    PROBE(StartOperation, GROUPS("ShopJob", "ShopOp"), \
      TYPES(TString, TString, ui64, ui64, ui64, ui64), \
      NAMES("shop", "flow", "job", "sid", "machineid", "estcost")) \
    PROBE(SkipOperation, GROUPS("ShopJob", "ShopOp"), \
      TYPES(TString, TString, ui64, ui64, ui64), \
      NAMES("shop", "flow", "job", "sid", "machineid")) \
    PROBE(OperationFinished, GROUPS("ShopJob", "ShopOp"), \
      TYPES(TString, TString, ui64, ui64, ui64, ui64, bool, double), \
      NAMES("shop", "flow", "job", "sid", "machineid", "realcost", "success", "procTimeMs")) \
    PROBE(NoMachine, GROUPS("ShopJob", "ShopOp"), \
      TYPES(TString, TString, ui64, ui64, ui64), \
      NAMES("shop", "flow", "job", "sid", "machineid")) \
    \
    \
    \
    PROBE(GlobalBusyPeriod, GROUPS("ShopScheduler"), \
      TYPES(TString, double, ui64, ui64, ui64, double, double, double), \
      NAMES("scheduler", "vtime", "busyPeriod", "busyPeriodPops", "busyPeriodCost", "idlePeriodDurationMs", "busyPeriodDurationMs", "utilization")) \
    PROBE(LocalBusyPeriod, GROUPS("ShopScheduler"), \
      TYPES(TString, double, ui64, ui64, ui64, double, double, double), \
      NAMES("scheduler", "vtime", "busyPeriod", "busyPeriodPops", "busyPeriodCost", "idlePeriodDurationMs", "busyPeriodDurationMs", "utilization")) \
    PROBE(PopSchedulable, GROUPS("ShopScheduler", "ShopFreezable", "ShopConsumer"), \
      TYPES(TString, TString, TString, double, ui64, double, ui64, i64, double, double), \
      NAMES("scheduler", "freezable", "consumer", "vtime", "busyPeriod", "finish", "cost", "underestimation", "weight", "vduration")) \
    PROBE(Activate, GROUPS("ShopScheduler", "ShopFreezable", "ShopConsumer"), \
      TYPES(TString, TString, TString, double, ui64, double, bool), \
      NAMES("scheduler", "freezable", "consumer", "vtime", "busyPeriod", "start", "frozen")) \
    PROBE(Deactivate, GROUPS("ShopScheduler", "ShopFreezable", "ShopConsumer"), \
      TYPES(TString, TString, TString, double, ui64, double, bool), \
      NAMES("scheduler", "freezable", "consumer", "vtime", "busyPeriod", "start", "frozen")) \
    PROBE(DeactivateImplicit, GROUPS("ShopScheduler", "ShopFreezable", "ShopConsumer"), \
      TYPES(TString, TString, TString, double, ui64, double, bool), \
      NAMES("scheduler", "freezable", "consumer", "vtime", "busyPeriod", "start", "frozen")) \
    PROBE(Freeze, GROUPS("ShopScheduler", "ShopFreezable"), \
      TYPES(TString, TString, double, ui64, ui64, ui64, double), \
      NAMES("scheduler", "freezable", "vtime", "busyPeriod", "freezableBusyPeriod", "frozenCount", "offset")) \
    PROBE(Unfreeze, GROUPS("ShopScheduler", "ShopFreezable"), \
      TYPES(TString, TString, double, ui64, ui64, ui64, double), \
      NAMES("scheduler", "freezable", "vtime", "busyPeriod", "freezableBusyPeriod", "frozenCount", "offset")) \
    \
    \
    \
    PROBE(LazyIdlePeriod, GROUPS("ShopLazyScheduler"), \
      TYPES(TString, double, ui64), \
      NAMES("scheduler", "vtime", "busyPeriod")) \
    PROBE(LazyWasted, GROUPS("ShopLazyScheduler"), \
      TYPES(TString, double, ui64, TString), \
      NAMES("scheduler", "vtime", "busyPeriod", "debugString")) \
    PROBE(LazyPopSchedulable, GROUPS("ShopLazyScheduler", "ShopLazyConsumer"), \
      TYPES(TString, TString, double, ui64, double, ui64, i64, double, double), \
      NAMES("scheduler", "consumer", "vtime", "busyPeriod", "finish", "cost", "underestimation", "weight", "vduration")) \
    PROBE(LazyActivate, GROUPS("ShopLazyScheduler", "ShopLazyConsumer"), \
      TYPES(TString, TString, double, ui64, double, TString, ui64, ui64), \
      NAMES("scheduler", "consumer", "vtime", "busyPeriod", "start", "mask", "machineIdx", "activePerMachine")) \
    PROBE(LazyDeactivate, GROUPS("ShopLazyScheduler", "ShopLazyConsumer"), \
      TYPES(TString, TString, double, ui64, double, TString, ui64, ui64), \
      NAMES("scheduler", "consumer", "vtime", "busyPeriod", "start", "mask", "machineIdx", "activePerMachine")) \
    PROBE(LazyDeny, GROUPS("ShopLazyScheduler", "ShopLazyConsumer"), \
      TYPES(TString, TString, double, ui64, TString), \
      NAMES("scheduler", "consumer", "vtime", "busyPeriod", "machines")) \
    PROBE(LazyAllow, GROUPS("ShopLazyScheduler", "ShopLazyConsumer"), \
      TYPES(TString, TString, double, ui64, ui64), \
      NAMES("scheduler", "consumer", "vtime", "busyPeriod", "machineIdx")) \
    PROBE(LazyRootDeny, GROUPS("ShopLazyScheduler"), \
      TYPES(TString, TString), \
      NAMES("scheduler", "machines")) \
    PROBE(LazyRootAllow, GROUPS("ShopLazyScheduler"), \
      TYPES(TString, ui64), \
      NAMES("scheduler", "machineIdx")) \
/**/

LWTRACE_DECLARE_PROVIDER(SHOP_PROVIDER)
