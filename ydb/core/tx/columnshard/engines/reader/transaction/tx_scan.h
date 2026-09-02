#pragma once
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/conveyor_composite/usage/config.h>

namespace NKikimr::NOlap::NReader {
class TTxScan: public NTabletFlatExecutor::TTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<NColumnShard::TColumnShard>;
    using TDiagnosticsEvent = NColumnShard::TEvPrivate::TEvReportScanDiagnostics;

    void SendError(const TString& problem, const TString& details, const TActorContext& ctx) const;

    // Properties of the request. Each reads the request and the tablet, and nothing else.
    TSnapshot GetSnapshot(const NColumnShard::TSchemeShardLocalPathId& ssPathId) const;
    TReadMetadataBase::ESorting GetSorting() const;
    bool GetDeduplicationEnabled(const TSnapshot& snapshot) const;
    TString GetReaderName() const;
    NConveyorComposite::TCPULimitsConfig GetCpuLimits() const;
    const TVersionedPresetSchemas& GetPresetSchemas() const;

    // Steps building the read description. A step that can fail returns only the failure details:
    // Complete() names the failed step itself, so the scan error keeps its two-part message.
    TReadDescription MakeReadDescription(const TSnapshot& snapshot, const TReadMetadataBase::ESorting sorting,
        const std::shared_ptr<NLWTrace::TOrbit>& orbit, const TString& readerName) const;
    TConclusionStatus InitTableAccessor(
        TReadDescription& read, const NColumnShard::TSchemeShardLocalPathId& ssPathId, const TSnapshot& snapshot) const;
    ui64 OnScanStartedForPath(const TReadDescription& read, NLWTrace::TOrbit& orbit) const;
    TConclusion<std::unique_ptr<IScannerConstructor>> MakeScannerConstructor(
        const TReadDescription& read, const TScannerConstructorContext& context, const TString& readerName) const;
    TConclusionStatus InitScanCursor(TReadDescription& read, const IScannerConstructor& scannerConstructor) const;
    TConclusionStatus InitProgram(TReadDescription& read, const IScannerConstructor& scannerConstructor) const;
    TConclusionStatus InitPKRangesFilter(TReadDescription& read) const;
    TConclusion<TReadMetadataBase::TConstPtr> BuildReadMetadata(
        const TReadDescription& read, const IScannerConstructor& scannerConstructor) const;

    // Null when diagnostics are disabled.
    std::unique_ptr<TDiagnosticsEvent> MakeDiagnosticsEvent(const TReadDescription& read) const;
    void StartScanActor(const TReadMetadataBase::TConstPtr& readMetadataRange, const ui64 rawPathId, std::shared_ptr<NLWTrace::TOrbit>&& orbit,
        std::unique_ptr<TDiagnosticsEvent>&& diagnostics, const NConveyorComposite::TCPULimitsConfig& cpuLimits, const TActorContext& ctx) const;

public:
    using TReadMetadataPtr = TReadMetadataBase::TConstPtr;

    TTxScan(NColumnShard::TColumnShard* self, TEvDataShard::TEvKqpScan::TPtr& ev)
        : TBase(self)
        , Ev(ev)
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;

    TTxType GetTxType() const override {
        return NColumnShard::TXTYPE_START_SCAN;
    }

private:
    TEvDataShard::TEvKqpScan::TPtr Ev;
};

}   // namespace NKikimr::NOlap::NReader
