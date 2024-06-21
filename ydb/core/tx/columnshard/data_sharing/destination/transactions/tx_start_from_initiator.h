#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_sharing/common/transactions/tx_extension.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/session/destination.h>

namespace NKikimr::NOlap::NDataSharing {

class TTxProposeFromInitiator: public TExtendedTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = TExtendedTransactionBase<NColumnShard::TColumnShard>;
    std::shared_ptr<TDestinationSession> Session;
    THashMap<TString, std::shared_ptr<TDestinationSession>>* Sessions;
protected:
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void DoComplete(const TActorContext& ctx) override;
public:
    TTxProposeFromInitiator(NColumnShard::TColumnShard* self, const std::shared_ptr<TDestinationSession>& session, THashMap<TString, std::shared_ptr<TDestinationSession>>& sessions, const TString& info)
        : TBase(self, info)
        , Session(session)
        , Sessions(&sessions) {
    }

    TTxType GetTxType() const override { return NColumnShard::TXTYPE_DATA_SHARING_PROPOSE_FROM_INITIATOR; }
};

class TTxConfirmFromInitiator: public TExtendedTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = TExtendedTransactionBase<NColumnShard::TColumnShard>;
    std::shared_ptr<TDestinationSession> Session;
protected:
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void DoComplete(const TActorContext& ctx) override;
public:
    TTxConfirmFromInitiator(NColumnShard::TColumnShard* self, const std::shared_ptr<TDestinationSession>& session, const TString& info)
        : TBase(self, info)
        , Session(session)
    {
    }

    TTxType GetTxType() const override { return NColumnShard::TXTYPE_DATA_SHARING_CONFIRM_FROM_INITIATOR; }
};


}
