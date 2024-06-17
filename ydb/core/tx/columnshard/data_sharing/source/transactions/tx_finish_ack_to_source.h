#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_sharing/common/transactions/tx_extension.h>
#include <ydb/core/tx/columnshard/data_sharing/source/session/source.h>

namespace NKikimr::NOlap::NDataSharing {

class TTxFinishAckToSource: public TExtendedTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = TExtendedTransactionBase<NColumnShard::TColumnShard>;
    std::shared_ptr<TSourceSession> Session;
protected:
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void DoComplete(const TActorContext& /*ctx*/) override;
public:
    TTxFinishAckToSource(NColumnShard::TColumnShard* self, const std::shared_ptr<TSourceSession>& session, const TString& info)
        : TBase(self, info)
        , Session(session)
    {
        AFL_VERIFY(!Session->GetCursorVerified()->IsValid());
    }

    TTxType GetTxType() const override { return NColumnShard::TXTYPE_DATA_SHARING_FINISH_ACK_TO_SOURCE; }
};


}
