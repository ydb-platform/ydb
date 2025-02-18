#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/tablet/ext_tx_base.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/session/destination.h>

namespace NKikimr::NOlap::NDataSharing {

class TTxFinishFromSource: public NColumnShard::TExtendedTransactionBase {
private:
    using TBase = NColumnShard::TExtendedTransactionBase;
    std::shared_ptr<TDestinationSession> Session;
    const TTabletId SourceTabletId;
    bool Finished = false;
protected:
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void DoComplete(const TActorContext& ctx) override;
public:
    TTxFinishFromSource(NColumnShard::TColumnShard* self, const TTabletId sourceTabletId, const std::shared_ptr<TDestinationSession>& session)
        : TBase(self)
        , Session(session)
        , SourceTabletId(sourceTabletId)
    {
        Session->GetCursorVerified(SourceTabletId).ReceiveFinished().Validate();
    }

    TTxType GetTxType() const override { return NColumnShard::TXTYPE_DATA_SHARING_FINISH_FROM_SOURCE; }
};


}
