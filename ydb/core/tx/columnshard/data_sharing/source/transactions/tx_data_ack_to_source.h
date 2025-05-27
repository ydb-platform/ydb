#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/tablet/ext_tx_base.h>
#include <ydb/core/tx/columnshard/data_sharing/source/session/source.h>
#include <ydb/core/tx/columnshard/blob_cache.h>

namespace NKikimr::NOlap::NDataSharing {

class TTxDataAckToSource: public NColumnShard::TExtendedTransactionBase {
private:
    using TBase = NColumnShard::TExtendedTransactionBase;
    std::shared_ptr<TSourceSession> Session;
    THashMap<TString, TTabletsByBlob> SharedBlobIds;
protected:
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void DoComplete(const TActorContext& ctx) override;
public:
    TTxDataAckToSource(NColumnShard::TColumnShard* self, const std::shared_ptr<TSourceSession>& session, const TString& info)
        : TBase(self, info)
        , Session(session)
    {
    }

    TTxType GetTxType() const override { return NColumnShard::TXTYPE_DATA_SHARING_DATA_ACK_TO_SOURCE; }
};


}
