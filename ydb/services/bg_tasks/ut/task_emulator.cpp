#include "task_emulator.h"

namespace NKikimr {

TTestInsertTaskActivity::TFactory::TRegistrator<TTestInsertTaskActivity> TTestInsertTaskActivity::Registrator(TTestInsertTaskState::GetClassNameStatic());
TTestInsertTaskScheduler::TFactory::TRegistrator<TTestInsertTaskScheduler> TTestInsertTaskScheduler::Registrator(TTestInsertTaskState::GetClassNameStatic());
TTestInsertTaskState::TFactory::TRegistrator<TTestInsertTaskState> TTestInsertTaskState::Registrator(TTestInsertTaskState::GetClassNameStatic());

namespace {
TMutex Mutex;
std::map<TString, ui32> CounterSumByActivityId;
std::map<TString, bool> FinishedByActivityId;
}

void TTestInsertTaskActivity::DoFinished(const NBackgroundTasks::TTaskStateContainer& /*state*/) {
    TGuard<TMutex> g(Mutex);
    FinishedByActivityId[ActivityTaskId] = true;
}

void TTestInsertTaskActivity::DoExecute(NBackgroundTasks::ITaskExecutorController::TPtr controller, const NBackgroundTasks::TTaskStateContainer& currentState) {
    ui32 c = 0;
    {
        TGuard<TMutex> g(Mutex);
        c = currentState.HasObject() ?
            currentState.GetAsSafe<TTestInsertTaskState>().GetCounter() :
            0;
        Cerr << "TASK EXECUTED: " << c << Endl;
        CounterSumByActivityId[ActivityTaskId] += c;
    }
    controller->TaskInterrupted(std::make_shared<TTestInsertTaskState>(c + 1));
}

bool TTestInsertTaskActivity::IsFinished(const TString& id) {
    TGuard<TMutex> g(Mutex);
    auto it = FinishedByActivityId.find(id);
    if (it == FinishedByActivityId.end()) {
        return false;
    } else {
        return it->second;
    }
}

ui32 TTestInsertTaskActivity::GetCounterSum(const TString& id) {
    TGuard<TMutex> g(Mutex);
    auto it = CounterSumByActivityId.find(id);
    if (it == CounterSumByActivityId.end()) {
        return 0;
    } else {
        return it->second;
    }
}

}
