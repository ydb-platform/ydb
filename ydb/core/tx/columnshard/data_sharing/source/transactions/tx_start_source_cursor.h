#pragma once
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/data_sharing/common/transactions/tx_extension.h>
#include <ydb/core/tx/columnshard/data_sharing/source/session/source.h>

namespace NKikimr::NOlap::NDataSharing {

class TTxStartSourceCursor: public TExtendedTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = TExtendedTransactionBase<NColumnShard::TColumnShard>;

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
