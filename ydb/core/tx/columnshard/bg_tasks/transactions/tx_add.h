#pragma once
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/adapter.h>
#include <ydb/core/tx/columnshard/bg_tasks/session/session.h>
#include <ydb/core/protos/counters_columnshard.pb.h>

namespace NKikimr::NOlap::NBackground {
class TSessionsStorage;
class TTxAddSession: public NTabletFlatExecutor::ITransaction {
private:
    std::shared_ptr<TSessionsStorage> Sessions;
    std::shared_ptr<ITabletAdapter> Adapter;
    std::shared_ptr<TSession> Session;
public:
    TTxAddSession(const std::shared_ptr<ITabletAdapter>& adapter, const std::shared_ptr<TSessionsStorage>& sessions, const std::shared_ptr<TSession>& session)
        : Sessions(sessions)
        , Adapter(adapter)
        , Session(session) {
        AFL_VERIFY(!!Session);
    }

    virtual bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void Complete(const TActorContext& ctx) override;
    virtual TTxType GetTxType() const override {
        return NColumnShard::TXTYPE_ADD_BACKGROUND_SESSION;
    };
};

}
