#pragma once
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/adapter.h>
#include <ydb/core/tx/columnshard/bg_tasks/session/storage.h>
#include <ydb/core/protos/counters_columnshard.pb.h>

namespace NKikimr::NOlap::NBackground {
class TSessionsContainer;
class TTxRemoveSession: public NTabletFlatExecutor::ITransaction {
private:
    using TBase = NTabletFlatExecutor::ITransaction;
    const TString ClassName;
    const TString Identifier;
    std::shared_ptr<TSessionsStorage> Sessions;
    const std::shared_ptr<ITabletAdapter> Adapter;
public:
    TTxRemoveSession(const TString& className, const TString& identifier, const std::shared_ptr<ITabletAdapter>& adapter, const std::shared_ptr<TSessionsStorage>& sessions)
        : ClassName(className)
        , Identifier(identifier)
        , Sessions(sessions)
        , Adapter(adapter)
    {
        AFL_VERIFY(!!Adapter);
        AFL_VERIFY(!!Sessions);
        AFL_VERIFY(!!ClassName);
        AFL_VERIFY(!!Identifier);
    }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return NColumnShard::TXTYPE_REMOVE_BACKGROUND_SESSION; }
};

}
