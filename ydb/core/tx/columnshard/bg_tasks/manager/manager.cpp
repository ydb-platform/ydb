#include "manager.h"
#include <ydb/core/tx/columnshard/bg_tasks/events/events.h>
#include <ydb/core/tx/columnshard/bg_tasks/transactions/tx_save_state.h>
#include <ydb/core/tx/columnshard/bg_tasks/transactions/tx_add.h>
#include <ydb/core/tx/columnshard/bg_tasks/transactions/tx_remove.h>
#include <ydb/core/tx/columnshard/bg_tasks/protos/data.pb.h>

namespace NKikimr::NOlap::NBackground {

std::unique_ptr<NTabletFlatExecutor::ITransaction> TSessionsManager::TxApplyControl(const TSessionControlContainer& control) {
    auto session = Storage->GetSession(control.GetLogicControlContainer()->GetSessionClassName(), control.GetLogicControlContainer()->GetSessionIdentifier());
    if (!session) {
        control.GetChannelContainer()->OnFail("session not exists");
        return nullptr;
    }
    if (!session->IsRunning()) {
        auto conclusion = control.GetLogicControlContainer()->Apply(session->GetLogicContainer().GetObjectPtr());
        if (conclusion.IsFail()) {
            control.GetChannelContainer()->OnFail(conclusion.GetErrorMessage());
            return nullptr;
        } else {
            return std::make_unique<TTxSaveSessionState>(session, std::nullopt, Adapter, 0);
        }
    } else {
        NActors::TActivationContext::AsActorContext().Send(session->GetActorIdVerified(), new TEvSessionControl(control));
        return nullptr;
    }
}

std::unique_ptr<NTabletFlatExecutor::ITransaction> TSessionsManager::TxApplyControlFromProto(const NKikimrTxBackgroundProto::TSessionControlContainer& controlProto) {
    TSessionControlContainer control;
    auto conclusion = control.DeserializeFromProto(controlProto);
    if (conclusion.IsFail()) {
        control.GetChannelContainer()->OnFail(conclusion.GetErrorMessage());
        return nullptr;
    }
    return TxApplyControl(control);
}

std::unique_ptr<NKikimr::NTabletFlatExecutor::ITransaction> TSessionsManager::TxAddTaskFromProto(const NKikimrTxBackgroundProto::TTaskContainer& taskProto) {
    TTask task;
    auto conclusion = task.DeserializeFromProto(taskProto);
    if (conclusion.IsFail()) {
        task.GetChannelContainer()->OnFail(conclusion.GetErrorMessage());
        return nullptr;
    }
    return TxAddTask(task);
}

std::unique_ptr<NKikimr::NTabletFlatExecutor::ITransaction> TSessionsManager::TxAddTask(const TTask& task) {
    auto session = Storage->GetSession(task.GetDescriptionContainer().GetClassName(), task.GetIdentifier());
    if (!!session) {
        task.GetChannelContainer()->OnFail("session exists already");
        return nullptr;
    }

    if (!task.GetDescriptionContainer()) {
        task.GetChannelContainer()->OnFail("task description is empty");
        return nullptr;
    }
    TConclusion<std::shared_ptr<ISessionLogic>> sessionLogic = task.GetDescriptionContainer()->BuildSessionLogic();
    if (sessionLogic.IsFail()) {
        task.GetChannelContainer()->OnFail(sessionLogic.GetErrorMessage());
        return nullptr;
    }
    std::shared_ptr<TSession> newSession(new TSession(task.GetIdentifier(), task.GetChannelContainer(), sessionLogic.DetachResult()));
    return std::make_unique<TTxAddSession>(Adapter, Storage, std::move(newSession));
}

bool TSessionsManager::LoadIdempotency(NTabletFlatExecutor::TTransactionContext& txc) {
    std::deque<TSessionRecord> records;
    if (!Adapter->LoadSessionsFromLocalDatabase(txc, records)) {
        return false;
    }
    auto storage = std::make_shared<TSessionsStorage>();
    while (records.size()) {
        std::shared_ptr<TSession> session = std::make_shared<TSession>();
        session->DeserializeFromLocalDatabase(std::move(records.front())).Validate("on load from local database");
        storage->AddSession(session);
        records.pop_front();
    }
    Storage = storage;
    return true;
}

std::unique_ptr<NKikimr::NTabletFlatExecutor::ITransaction> TSessionsManager::TxRemove(const TString& className, const TString& identifier) {
    return std::make_unique<TTxRemoveSession>(className, identifier, Adapter, Storage);
}

bool TSessionsManager::HasTask(const TTask& task) const {
    return !!Storage->GetSession(task.GetDescriptionContainer().GetClassName(), task.GetIdentifier());
}

}