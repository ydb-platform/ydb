#include "service.h"
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/kqp/query_data/kqp_predictor.h>

namespace NKikimr::NConveyor {

TDistributor::TDistributor(const TConfig& config, const TString& conveyorName, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals)
    : Config(config)
    , ConveyorName(conveyorName)
    , Counters(ConveyorName, conveyorSignals) {

}

void TDistributor::Bootstrap() {
    const ui32 workersCount = Config.GetWorkersCountForConveyor(NKqp::TStagePredictor::GetUsableThreads());
    AFL_NOTICE(NKikimrServices::TX_CONVEYOR)("name", ConveyorName)("action", "conveyor_registered")("config", Config.DebugString());
    for (ui32 i = 0; i < workersCount; ++i) {
        const double usage = Config.GetWorkerCPUUsage(i);
        Workers.emplace_back(Register(new TWorker(ConveyorName, usage, SelfId())));
        if (usage < 1) {
            AFL_VERIFY(!SlowWorkerId);
            SlowWorkerId = Workers.back();
        }
    }
    Counters.AvailableWorkersCount->Set(Workers.size());
    Counters.WorkersCountLimit->Set(Workers.size());
    Counters.WaitingQueueSizeLimit->Set(Config.GetQueueSizeLimit());
    Become(&TDistributor::StateMain);
}

void TDistributor::HandleMain(TEvInternal::TEvTaskProcessedResult::TPtr& ev) {
    const auto now = TMonotonic::Now();
    const TDuration dExecution = now - ev->Get()->GetStartInstant();
    Counters.SolutionsRate->Inc();
    Counters.ExecuteHistogram->Collect(dExecution.MilliSeconds());
    if (Waiting.size()) {
        auto task = Waiting.pop();
        Counters.WaitingHistogram->Collect((now - task.GetCreateInstant()).MilliSeconds());
        task.OnBeforeStart();
        Send(ev->Sender, new TEvInternal::TEvNewTask(task));
    } else {
        Workers.emplace_back(ev->Sender);
    }
    Counters.WaitingQueueSize->Set(Waiting.size());
    Counters.AvailableWorkersCount->Set(Workers.size());
}

void TDistributor::HandleMain(TEvExecution::TEvNewTask::TPtr& ev) {
    AFL_DEBUG(NKikimrServices::TX_CONVEYOR)("action", "add_task")("sender", ev->Sender);
    Counters.IncomingRate->Inc();

    const TString taskClass = ev->Get()->GetTask()->GetTaskClassIdentifier();
    auto itSignal = Signals.find(taskClass);
    if (itSignal == Signals.end()) {
        itSignal = Signals.emplace(taskClass, std::make_shared<TTaskSignals>("Conveyor/" + ConveyorName, taskClass)).first;
    }

    TWorkerTask wTask(ev->Get()->GetTask(), itSignal->second);

    if (Workers.size()) {
        Counters.WaitingHistogram->Collect(0);

        wTask.OnBeforeStart();
        if (Workers.size() == 1 || !SlowWorkerId || Workers.back() != *SlowWorkerId) {
            Send(Workers.back(), new TEvInternal::TEvNewTask(wTask));
            Workers.pop_back();
        } else {
            Send(Workers.front(), new TEvInternal::TEvNewTask(wTask));
            Workers.pop_front();
        }
        Counters.UseWorkerRate->Inc();
    } else if (Waiting.size() < Config.GetQueueSizeLimit()) {
        Waiting.push(wTask);
        Counters.WaitWorkerRate->Inc();
    } else {
        Counters.OverlimitRate->Inc();
        AFL_ERROR(NKikimrServices::TX_CONVEYOR)("action", "queue_overlimit")("sender", ev->Sender)("limit", Config.GetQueueSizeLimit());
        ev->Get()->GetTask()->OnCannotExecute("scan conveyor overloaded (" + ::ToString(Waiting.size()) + " >= " + ::ToString(Config.GetQueueSizeLimit()) + ")");
    }
    Counters.WaitingQueueSize->Set(Waiting.size());
    Counters.AvailableWorkersCount->Set(Workers.size());
}

}
