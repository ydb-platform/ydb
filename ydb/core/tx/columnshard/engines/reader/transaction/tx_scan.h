#pragma once
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>

namespace NKikimr::NOlap::NReader {
class TTxScan: public NTabletFlatExecutor::TTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<NColumnShard::TColumnShard>;
public:
    using TReadMetadataPtr = TReadMetadataBase::TConstPtr;

    TTxScan(NColumnShard::TColumnShard* self, TEvColumnShard::TEvScan::TPtr& ev)
        : TBase(self)
        , Ev(ev) {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return NColumnShard::TXTYPE_START_SCAN; }

private:
    TString ErrorDescription;
    TEvColumnShard::TEvScan::TPtr Ev;
    TReadMetadataPtr ReadMetadataRange;
};

}