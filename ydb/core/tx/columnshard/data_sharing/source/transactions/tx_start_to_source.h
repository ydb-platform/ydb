#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_sharing/common/transactions/tx_extension.h>
#include <ydb/core/tx/columnshard/data_sharing/source/session/source.h>

namespace NKikimr::NOlap::NDataSharing {

class TTxStartToSource: public TExtendedTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = TExtendedTransactionBase<NColumnShard::TColumnShard>;
    std::shared_ptr<TSourceSession> Session;
    THashMap<TString, std::shared_ptr<TSourceSession>>* Sessions;
protected:
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void DoComplete(const TActorContext& ctx) override;
public:
    TTxStartToSource(NColumnShard::TColumnShard* self, const std::shared_ptr<TSourceSession>& session, THashMap<TString, std::shared_ptr<TSourceSession>>& sessions, const TString& info)
        : TBase(self, info)
        , Session(session)
        , Sessions(&sessions)
    {
    }

    TTxType GetTxType() const override { return NColumnShard::TXTYPE_DATA_SHARING_START_TO_SOURCE; }
};


}
