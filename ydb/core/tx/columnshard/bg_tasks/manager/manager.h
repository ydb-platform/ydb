#pragma once
#include <ydb/core/tx/columnshard/bg_tasks/abstract/task.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/control.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/adapter.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/session.h>
#include <ydb/core/tx/columnshard/bg_tasks/session/session.h>
#include <ydb/core/tx/columnshard/bg_tasks/session/storage.h>
#include <ydb/core/tx/columnshard/bg_tasks/protos/data.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimr::NOlap::NBackground {

class TSessionsManager {
private:
    bool Started = false;
    std::shared_ptr<TSessionsStorage> Storage;
    std::shared_ptr<ITabletAdapter> Adapter;
public:
    bool LoadIdempotency(NTabletFlatExecutor::TTransactionContext& txc);

    [[nodiscard]] std::unique_ptr<NTabletFlatExecutor::ITransaction> AddTaskFromProto(const NKikimrTxBackgroundProto::TTaskContainer& taskProto);

    [[nodiscard]] std::unique_ptr<NTabletFlatExecutor::ITransaction> Remove(const TString& className, const TString& identifier);

    [[nodiscard]] std::unique_ptr<NTabletFlatExecutor::ITransaction> ApplyControlFromProto(const NKikimrTxBackgroundProto::TSessionControlContainer& controlProto);

    void Start(const TStartContext& context) {
        AFL_VERIFY(!Started);
        Storage->Start(context);
        Started = true;
    }

    void Finish() {
        AFL_VERIFY(Started);
        Storage->Finish();
        Started = false;
    }
};

}