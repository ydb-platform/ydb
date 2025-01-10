#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/tablet/ext_tx_base.h>
#include <ydb/core/tx/columnshard/data_sharing/source/session/source.h>

namespace NKikimr::NOlap::NDataSharing {

class TTxStartSourceCursor: public NColumnShard::TExtendedTransactionBase {
private:
    using TBase = NColumnShard::TExtendedTransactionBase;

    TSourceSession* Session;
    THashMap<ui64, std::vector<TPortionDataAccessor>> Portions;

protected:
    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override;
    virtual void DoComplete(const TActorContext& ctx) override;

public:
    TTxStartSourceCursor(TSourceSession* session, NColumnShard::TColumnShard* self, THashMap<ui64, std::vector<TPortionDataAccessor>>&& portions, const TString& info)
        : TBase(self, info)
        , Session(session)
        , Portions(std::move(portions)) {
    }

    TTxType GetTxType() const override {
        return NColumnShard::TXTYPE_DATA_SHARING_START_SOURCE_CURSOR;
    }
};

}   // namespace NKikimr::NOlap::NDataSharing
