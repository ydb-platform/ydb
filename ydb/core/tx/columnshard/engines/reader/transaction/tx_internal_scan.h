#pragma once
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>

namespace NKikimr::NOlap::NReader {
class TTxInternalScan: public NTabletFlatExecutor::TTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<NColumnShard::TColumnShard>;
    const ui32 ScanGen = 1;
    const ui32 TxId = 1;
    const ui32 ScanId = 1;
    void SendError(const TString& problem, const TString& details) const;

public:
    using TReadMetadataPtr = TReadMetadataBase::TConstPtr;

    TTxInternalScan(NColumnShard::TColumnShard* self, TEvColumnShard::TEvInternalScan::TPtr& ev)
        : TBase(self)
        , InternalScanEvent(ev) {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;
    TTxType GetTxType() const override { return NColumnShard::TXTYPE_START_INTERNAL_SCAN; }

private:
    TEvColumnShard::TEvInternalScan::TPtr InternalScanEvent;
};

}