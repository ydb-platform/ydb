#pragma once
#include <ydb/core/tx/columnshard/bg_tasks/abstract/adapter.h>

namespace NKikimr::NSchemeShard::NBackground {

class TAdapter: public NKikimr::NOlap::NBackground::ITabletAdapter {
protected:
    virtual bool DoLoadSessionsFromLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, std::deque<NKikimr::NOlap::NBackground::TSessionRecord>& records) override;
    virtual void DoSaveProgressToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const NKikimr::NOlap::NBackground::TSessionRecord& container) override;
    virtual void DoSaveStateToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const NKikimr::NOlap::NBackground::TSessionRecord& container) override;
    virtual void DoSaveSessionToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const NKikimr::NOlap::NBackground::TSessionRecord& container) override;
    virtual void DoRemoveSessionFromLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TString& className, const TString& identifier) override;;
};
}