#include "counters.h"

namespace NKikimr::NConveyorComposite {

TWorkersPoolCounters::TWorkersPoolCounters(const TString& poolName, const NColumnShard::TCommonCountersOwner& owner)
    : TBase(owner, "pool_name", poolName)
    , PackSizeHistogram(TBase::GetLazyHistogram("ExecutionPack/Count", NMonitoring::LinearHistogram(25, 1, 1)))
    , PackExecuteHistogram(TBase::GetLazyHistogram("PackExecute/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
    , SendBackHistogram(TBase::GetLazyHistogram("SendBack/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
    , SendFwdHistogram(TBase::GetLazyHistogram("SendForward/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
    , SendBackDuration(TBase::GetDeriviative("SendBack/Duration/Us"))
    , SendFwdDuration(TBase::GetDeriviative("SendForward/Duration/Us"))
    , AvailableWorkersCount(TBase::GetValue("AvailableWorkersCount"))
    , WorkersCountLimit(TBase::GetValue("WorkersCountLimit"))
    , AmountCPULimit(TBase::GetValue("AmountCPULimit"))
    , IncomingRate(TBase::GetDeriviative("Incoming"))
    , SolutionsRate(TBase::GetDeriviative("Solved"))
    , OverlimitRate(TBase::GetDeriviative("Overlimit"))
    , WaitWorkerRate(TBase::GetDeriviative("WaitWorker"))
    , UseWorkerRate(TBase::GetDeriviative("UseWorker"))
    , ChangeCPULimitRate(TBase::GetDeriviative("ChangeCPULimit")) {
}

TWPCategorySignals::TWPCategorySignals(NColumnShard::TCommonCountersOwner& base, const ESpecialTaskCategory cat)
    : TBase(base, "wp_category", ::ToString(cat))
    , Category(cat)
    , WaitingHistogram(TBase::GetLazyHistogram("Waiting/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
    , NoTasks(TBase::GetDeriviative("NoTasks"))
    , TaskExecuteHistogram(TBase::GetLazyHistogram("TaskExecute/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
    , ValueWeight(TBase::GetValue("Weight"))
    , ExecuteDuration(TBase::GetDeriviative("Execute/Duration/Us")) {
}

}   // namespace NKikimr::NConveyorComposite
