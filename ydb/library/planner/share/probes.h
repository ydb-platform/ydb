#pragma once

#include <ydb/library/planner/base/defs.h>
#include <ydb/library/planner/share/protos/shareplanner.pb.h>
#include <library/cpp/lwtrace/all.h>

namespace NScheduling {

// Helper class for printing cost in percents of total planner capacity
struct TModelField {
    typedef int TStoreType;
    static void ToString(int value, TString* out) {
        *out = Sprintf("%d(%s)", value, EPlanningModel_Name((EPlanningModel)value).c_str());
    }
};

}

#define PLANNER_PROBE(name, ...) GLOBAL_LWPROBE(SCHEDULING_SHAREPLANNER_PROVIDER, name, ## __VA_ARGS__)

#define SCHEDULING_SHAREPLANNER_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(Done, GROUPS("Scheduling", "SharePlannerInterface", "SharePlannerAccount"), \
      TYPES(TString,TString,double,double), \
      NAMES("planner","account","cost","time")) \
    PROBE(Waste, GROUPS("Scheduling", "SharePlannerInterface"), \
      TYPES(TString,double), \
      NAMES("planner","cost")) \
    PROBE(Run, GROUPS("Scheduling", "SharePlannerInterface"), \
      TYPES(TString), \
      NAMES("planner")) \
    PROBE(Configure, GROUPS("Scheduling", "SharePlannerInterface"), \
      TYPES(TString, TString), \
      NAMES("planner", "cfg")) \
    PROBE(SerializeHistory, GROUPS("Scheduling", "SharePlannerInterface"), \
      TYPES(TString), \
      NAMES("planner")) \
    PROBE(DeserializeHistoryBegin, GROUPS("Scheduling", "SharePlannerInterface"), \
      TYPES(TString), \
      NAMES("planner")) \
    PROBE(DeserializeHistoryEnd, GROUPS("Scheduling", "SharePlannerInterface"), \
      TYPES(TString, bool), \
    NAMES("planner", "success")) \
    PROBE(Add, GROUPS("Scheduling", "SharePlannerInterface"), \
      TYPES(TString,TString,NScheduling::TWeight,TString), \
      NAMES("planner","parent","weight","node")) \
    PROBE(Delete, GROUPS("Scheduling", "SharePlannerInterface"), \
      TYPES(TString,TString), \
      NAMES("planner","node")) \
    \
    PROBE(FirstActivation, GROUPS("Scheduling", "SharePlannerDetails"), \
      TYPES(TString), \
      NAMES("planner")) \
    PROBE(LastDeactivation, GROUPS("Scheduling", "SharePlannerDetails"), \
      TYPES(TString), \
      NAMES("planner")) \
    PROBE(CommitInfo, GROUPS("Scheduling", "SharePlannerDetails"), \
      TYPES(TString, bool,bool,TString), \
      NAMES("planner","model","run","info")) \
    PROBE(Advance, GROUPS("Scheduling", "SharePlannerDetails"), \
      TYPES(TString,double,double), \
      NAMES("planner","cost","wcost")) \
    PROBE(TryDeactivate, GROUPS("Scheduling", "SharePlannerDetails"), \
      TYPES(TString), \
      NAMES("planner")) \
    PROBE(Spoil, GROUPS("Scheduling", "SharePlannerDetails", "SharePlannerAccount"), \
      TYPES(TString,TString,double), \
      NAMES("planner","account","cost")) \
    PROBE(SetRate, GROUPS("Scheduling", "SharePlannerDetails", "SharePlannerAccount"), \
      TYPES(TString,TString,double,double), \
      NAMES("planner","account","oldrate","newrate")) \
    PROBE(SetAssuredRate, GROUPS("Scheduling", "SharePlannerDetails", "SharePlannerAccount"), \
      TYPES(TString,TString,double,double), \
      NAMES("planner","account","oldrate","newrate")) \
    PROBE(ResetRate, GROUPS("Scheduling", "SharePlannerDetails"), \
      TYPES(TString,TString,double), \
      NAMES("planner","group","rate")) \
    PROBE(ResetAssuredRate, GROUPS("Scheduling", "SharePlannerDetails"), \
      TYPES(TString,TString,double), \
      NAMES("planner","group","rate")) \
    PROBE(SetTL, GROUPS("Scheduling", "SharePlannerDetails"), \
      TYPES(TString,double,double), \
      NAMES("planner","old","new")) \
    PROBE(CalcFixedHyperbolicWeight, GROUPS("Scheduling", "SharePlannerDetails", "SharePlannerAccount", "SharePlannerWeight"), \
      TYPES(TString,TString,double,double,double,double,double), \
      NAMES("planner","account","l","x","X","Xxi","result")) \
    PROBE(CalcFixedHyperbolicWeight_I, GROUPS("Scheduling", "SharePlannerDetails", "SharePlannerAccount", "SharePlannerWeight"), \
      TYPES(TString,TString,double,double,double,double,double), \
      NAMES("planner","account","D_R","W","WMax","xi_min","RepaymentPeriod")) \
    PROBE(FloatingLinearModel, GROUPS("Scheduling", "SharePlannerDetails", "SharePlannerAccount", "SharePlannerWeight"), \
      TYPES(TString,TString,double,double,double,double,double, double, double), \
      NAMES("planner","account","w_prev","rho","h","u","dD","xsum","wsum")) \
    PROBE(FloatingLinearModel_I, GROUPS("Scheduling", "SharePlannerDetails", "SharePlannerAccount", "SharePlannerWeight"), \
      TYPES(TString,TString,double,double,double,double,double,double,double,double), \
      NAMES("planner","account","w0","wm","RepaymentPeriod","a0","amin","a","w","result")) \
    \
    PROBE(ActivePoolPush, GROUPS("Scheduling", "SharePlannerDetails", "SharePlannerHeaps", "SharePlannerAccount"), \
      TYPES(TString,TString,double,size_t), \
      NAMES("planner","account","until","newsize")) \
    PROBE(ActivePoolPop, GROUPS("Scheduling", "SharePlannerDetails", "SharePlannerHeaps", "SharePlannerAccount"), \
      TYPES(TString,TString,size_t), \
      NAMES("planner","account","newsize")) \
    PROBE(ActivePoolPeek, GROUPS("Scheduling", "SharePlannerDetails", "SharePlannerHeaps", "SharePlannerAccount"), \
      TYPES(TString,TString,double,size_t), \
      NAMES("planner","account","until","size")) \
    PROBE(ActivePoolRebuild, GROUPS("Scheduling", "SharePlannerDetails", "SharePlannerHeaps"), \
      TYPES(TString,size_t,size_t), \
      NAMES("planner","badnonidle","size")) \
    PROBE(ActivePoolIncBad, GROUPS("Scheduling", "SharePlannerDetails", "SharePlannerHeaps", "SharePlannerAccount"), \
      TYPES(TString,TString,size_t), \
      NAMES("planner","account","new")) \
    PROBE(ActivePoolDecBad, GROUPS("Scheduling", "SharePlannerDetails", "SharePlannerHeaps", "SharePlannerAccount"), \
      TYPES(TString,TString,size_t), \
      NAMES("planner","account","new")) \
    \
    PROBE(Deactivate, GROUPS("Scheduling", "SharePlannerDetails", "SharePlannerAccountTransitions", "SharePlannerAccount"), \
      TYPES(TString,TString), \
      NAMES("planner","account")) \
    PROBE(Activate, GROUPS("Scheduling", "SharePlannerDetails", "SharePlannerAccountTransitions", "SharePlannerAccount"), \
      TYPES(TString,TString), \
      NAMES("planner","account")) \
    /**/

LWTRACE_DECLARE_PROVIDER(SCHEDULING_SHAREPLANNER_PROVIDER)
