#pragma once
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/bg_tasks/abstract/adapter.h>
#include <ydb/core/tx/columnshard/bg_tasks/session/session.h>
#include <ydb/core/protos/counters_columnshard.pb.h>

namespace NKikimr::NOlap::NBackground {
class TTxGeneral: public NTabletFlatExecutor::ITransaction {
private:
    using TBase = NTabletFlatExecutor::ITransaction;
    const std::optional<TActorId> ProgressActorId;
    const ui64 TxInternalId;
    virtual void DoComplete(const TActorContext& ctx) = 0;
public:
    TTxGeneral(const std::optional<NActors::TActorId> progressActorId, const ui64 txInternalId)
        : ProgressActorId(progressActorId)
        , TxInternalId(txInternalId)
    {
    }

    void Complete(const TActorContext& ctx) override final;
};

}
