#pragma once
#include "tx_general.h"
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/adapter.h>
#include <ydb/core/tx/columnshard/bg_tasks/session/session.h>
#include <ydb/core/protos/counters_columnshard.pb.h>

namespace NKikimr::NOlap::NBackground {
class TTxSaveSessionState: public TTxGeneral {
private:
    using TBase = TTxGeneral;
    const std::shared_ptr<TSession> Session;
    const std::shared_ptr<ITabletAdapter> Adapter;
    virtual void DoComplete(const TActorContext& ctx) override;
public:
    TTxSaveSessionState(const std::shared_ptr<TSession>& session, const std::optional<NActors::TActorId> progressActorId, const std::shared_ptr<ITabletAdapter>& adapter, const ui64 txInternalId)
        : TBase(progressActorId, txInternalId)
        , Session(session)
        , Adapter(adapter)
    {
        AFL_VERIFY(!!Session);
        AFL_VERIFY(!!Adapter);
    }

    bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override;
    TTxType GetTxType() const override { return NColumnShard::TXTYPE_SAVE_BACKGROUND_SESSION_STATE; }
};

}
