#include "counters.h"

namespace NKikimr::NConveyorComposite {

TCounters::TCounters(const TString& conveyorName, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals)
    : TBase("CompositeConveyor/" + conveyorName, baseSignals)
    , ProcessesCount(TBase::GetValue("Processes/Count"))
    , WaitingQueueSize(TBase::GetValue("WaitingQueueSize"))
    , WaitingQueueSizeLimit(TBase::GetValue("WaitingQueueSizeLimit"))
    , AvailableWorkersCount(TBase::GetValue("AvailableWorkersCount"))
    , WorkersCountLimit(TBase::GetValue("WorkersCountLimit"))
    , AmountCPULimit(TBase::GetValue("AmountCPULimit"))
    , IncomingRate(TBase::GetDeriviative("Incoming"))
    , SolutionsRate(TBase::GetDeriviative("Solved"))
    , OverlimitRate(TBase::GetDeriviative("Overlimit"))
    , WaitWorkerRate(TBase::GetDeriviative("WaitWorker"))
    , UseWorkerRate(TBase::GetDeriviative("UseWorker"))
    , ChangeCPULimitRate(TBase::GetDeriviative("ChangeCPULimit"))
    , WaitingHistogram(TBase::GetHistogram("Waiting/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
    , PackHistogram(TBase::GetHistogram("ExecutionPack/Count", NMonitoring::LinearHistogram(25, 1, 1)))
    , PackExecuteHistogram(TBase::GetHistogram("PackExecute/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
    , TaskExecuteHistogram(TBase::GetHistogram("TaskExecute/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
    , SendBackHistogram(TBase::GetHistogram("SendBack/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
    , SendFwdHistogram(TBase::GetHistogram("SendForward/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
    , ReceiveTaskHistogram(TBase::GetHistogram("ReceiveTask/Duration/Us", NMonitoring::ExponentialHistogram(25, 2, 50)))
    , SendBackDuration(TBase::GetDeriviative("SendBack/Duration/Us"))
    , SendFwdDuration(TBase::GetDeriviative("SendForward/Duration/Us"))
    , ExecuteDuration(TBase::GetDeriviative("Execute/Duration/Us")) {
}

}
