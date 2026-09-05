#pragma once
#include "common.h"

#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>

namespace NKikimr::NOlap::NReader {
class TTxInternalScan: public NTabletFlatExecutor::TTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<NColumnShard::TColumnShard>;
    using TDiagnosticsEvent = NColumnShard::TEvPrivate::TEvReportScanDiagnostics;

    const TEvColumnShard::TEvInternalScan::TPtr InternalScanEvent;
    const ui32 ScanGen = 1;
    const ui32 ScanId = 1;

    void SendError(const TString& problem, const TString& details, const TActorContext& ctx) const;

    // Properties of the request. Each reads the request and the tablet, and nothing else.
    TSnapshot GetSnapshot() const;
    TReadMetadataBase::ESorting GetSorting() const;

    // Steps building the read description
    TReadDescription MakeReadDescription(const TSnapshot& snapshot, const TReadMetadataBase::ESorting sorting) const;
    TConclusionStatus InitTableAccessor(TReadDescription& read, const TSnapshot& snapshot) const;

    // Null when diagnostics are disabled.
    std::unique_ptr<TDiagnosticsEvent> MakeDiagnosticsEvent(const TReadDescription& read) const;
    void StartScanActor(
        const TReadMetadataBase::TConstPtr& readMetadataRange, std::unique_ptr<TDiagnosticsEvent>&& diagnostics, const TActorContext& ctx) const;

public:
    using TReadMetadataPtr = TReadMetadataBase::TConstPtr;

    TTxInternalScan(NColumnShard::TColumnShard* self, TEvColumnShard::TEvInternalScan::TPtr& ev)
        : TBase(self)
        , InternalScanEvent(ev)
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;

    TTxType GetTxType() const override {
        return NColumnShard::TXTYPE_START_INTERNAL_SCAN;
    }
};

}   // namespace NKikimr::NOlap::NReader
