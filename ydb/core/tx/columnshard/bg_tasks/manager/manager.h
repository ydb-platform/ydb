#pragma once
#include <ydb/core/tx/columnshard/bg_tasks/abstract/task.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/control.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/adapter.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/session.h>
#include <ydb/core/tx/columnshard/bg_tasks/session/session.h>
#include <ydb/core/tx/columnshard/bg_tasks/session/storage.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimrTxBackgroundProto {
class TTaskContainer;
class TSessionControlContainer;
}

namespace NKikimr::NOlap::NBackground {

class TSessionsManager {
private:
    bool Started = false;
    std::shared_ptr<TSessionsStorage> Storage;
    std::shared_ptr<ITabletAdapter> Adapter;
public:
    TSessionsManager(const std::shared_ptr<ITabletAdapter>& adapter)
        : Adapter(adapter)
    {
        Storage = std::make_shared<TSessionsStorage>();
    }

    bool LoadIdempotency(NTabletFlatExecutor::TTransactionContext& txc);

    [[nodiscard]] std::unique_ptr<NTabletFlatExecutor::ITransaction> AddTask(const TTask& task);
    [[nodiscard]] std::unique_ptr<NTabletFlatExecutor::ITransaction> AddTaskFromProto(const NKikimrTxBackgroundProto::TTaskContainer& taskProto);
    [[nodiscard]] std::unique_ptr<NTabletFlatExecutor::ITransaction> Remove(const TString& className, const TString& identifier);
    [[nodiscard]] std::unique_ptr<NTabletFlatExecutor::ITransaction> ApplyControlFromProto(const NKikimrTxBackgroundProto::TSessionControlContainer& controlProto);
    [[nodiscard]] std::unique_ptr<NTabletFlatExecutor::ITransaction> ApplyControl(const TSessionControlContainer& control);

    void Start() {
        AFL_VERIFY(!Started);
        Storage->Start(Adapter);
        Started = true;
    }

    void Stop() {
        AFL_VERIFY(Started);
        Storage->Finish();
        Started = false;
    }
};

}