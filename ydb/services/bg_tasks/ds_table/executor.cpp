#include "add_tasks.h"
#include "assign_tasks.h"
#include "executor.h"
#include "lock_pinger.h"
#include "task_executor.h"
#include "task_enabled.h"
#include "fetch_tasks.h"

namespace NKikimr::NBackgroundTasks {

TVector<NMetadataInitializer::ITableModifier::TPtr> TExecutor::BuildModifiers() const {
    const TString tableName = Config.GetTablePath();
    TVector<NMetadataInitializer::ITableModifier::TPtr> result;
    {
        Ydb::Table::CreateTableRequest request;
        request.set_session_id("");
        request.set_path(tableName);
        request.add_primary_key("id");
        {
            auto& column = *request.add_columns();
            column.set_name("id");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("enabled");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::BOOL);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("class");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("executorId");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("lastPing");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT32);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("startInstant");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT32);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("constructInstant");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT32);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("activity");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("scheduler");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        {
            auto& column = *request.add_columns();
            column.set_name("state");
            column.mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::STRING);
        }
        result.emplace_back(new NMetadataInitializer::TGenericTableModifier<NInternal::NRequest::TDialogCreateTable>(request));
    }
    return result;
}

void TExecutor::Handle(TEvStartAssign::TPtr& /*ev*/) {
    ALS_DEBUG(NKikimrServices::BG_TASKS) << "start assign";
    if (Config.GetMaxInFlight() > CurrentTaskIds.size()) {
        Register(new TAssignTasksActor(Config.GetMaxInFlight() - CurrentTaskIds.size(), Controller, ExecutorId));
    }
}

void TExecutor::Handle(TEvAssignFinished::TPtr& /*ev*/) {
    ALS_DEBUG(NKikimrServices::BG_TASKS) << "assign finished";
    Register(new TFetchTasksActor(CurrentTaskIds, ExecutorId, Controller));
}

void TExecutor::Handle(TEvFetchingFinished::TPtr& /*ev*/) {
    ALS_DEBUG(NKikimrServices::BG_TASKS) << "assign scheduled: " << Config.GetPullPeriod();
    Schedule(Config.GetPullPeriod(), new TEvStartAssign);
}

void TExecutor::Handle(TEvLockPingerFinished::TPtr& /*ev*/) {
    ALS_DEBUG(NKikimrServices::BG_TASKS) << "pinger scheduled: " << Config.GetPingPeriod();
    Schedule(Config.GetPingPeriod(), new TEvLockPingerStart);
}

void TExecutor::Handle(TEvLockPingerStart::TPtr& /*ev*/) {
    ALS_DEBUG(NKikimrServices::BG_TASKS) << "pinger start";
    if (CurrentTaskIds.size()) {
        Register(new TLockPingerActor(Controller, CurrentTaskIds));
    } else {
        Schedule(Config.GetPingPeriod(), new TEvLockPingerStart);
    }
}

void TExecutor::Handle(TEvTaskFetched::TPtr& ev) {
    ALS_DEBUG(NKikimrServices::BG_TASKS) << "task fetched";
    if (CurrentTaskIds.emplace(ev->Get()->GetTask().GetId()).second) {
        Register(new TTaskExecutor(ev->Get()->GetTask(), Controller));
    }
}

void TExecutor::Handle(TEvTaskExecutorFinished::TPtr& ev) {
    ALS_DEBUG(NKikimrServices::BG_TASKS) << "task executor finished";
    Y_VERIFY(CurrentTaskIds.contains(ev->Get()->GetTaskId()));
    CurrentTaskIds.erase(ev->Get()->GetTaskId());
    Sender<TEvStartAssign>().SendTo(SelfId());
}

void TExecutor::Handle(TEvAddTask::TPtr& ev) {
    ALS_DEBUG(NKikimrServices::BG_TASKS) << "add task";
    Register(new TAddTasksActor(Controller, ev->Get()->GetTask(), ev->Sender));
}

void TExecutor::Handle(TEvUpdateTaskEnabled::TPtr& ev) {
    ALS_DEBUG(NKikimrServices::BG_TASKS) << "start task";
    Register(new TUpdateTaskEnabledActor(Controller, ev->Get()->GetTaskId(), ev->Get()->GetEnabled(), ev->Sender));
}

void TExecutor::RegisterState() {
    Controller = std::make_shared<TExecutorController>(SelfId(), Config);
    Become(&TExecutor::StateMain);
}

void TExecutor::OnInitialized() {
    Sender<TEvStartAssign>().SendTo(SelfId());
    Schedule(Config.GetPingPeriod(), new TEvLockPingerStart);
}

NActors::IActor* CreateService(const TConfig& config) {
    return new TExecutor(config);
}

}
