#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_sharing/common/transactions/tx_extension.h>
#include <ydb/core/tx/columnshard/data_sharing/source/session/source.h>

namespace NKikimr::NOlap::NDataSharing {

class TTxTransferSchemeHistory: public TExtendedTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = TExtendedTransactionBase<NColumnShard::TColumnShard>;
    TString SessionId;
    TTabletId SourceTabletId;
    TTabletId DestinationTabletId;
    ui64 RuntimeId;

protected:
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void DoComplete(const TActorContext& ctx) override;

public:
    TTxTransferSchemeHistory(NColumnShard::TColumnShard* self, const TString& sessionId, TTabletId sourceTabletId,
        TTabletId destinationTabletId, ui64 runtimeId, const TString& info)
        : TBase(self, info)
        , SessionId(sessionId)
        , SourceTabletId(sourceTabletId)
        , DestinationTabletId(destinationTabletId)
        , RuntimeId(runtimeId) {
    }

    TTxType GetTxType() const override {
        return NColumnShard::TXTYPE_DATA_SHARING_TRANSFER_SCHEME_HISTORY;
    }
};

}   // namespace NKikimr::NOlap::NDataSharing
