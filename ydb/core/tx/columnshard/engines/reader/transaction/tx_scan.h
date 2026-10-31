#pragma once
#include "common.h"

#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/constructor.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/conveyor_composite/common/config/config.h>
#include <ydb/core/tx/conveyor_composite/usage/common.h>

namespace NKikimr::NOlap::NReader {
class TTxScan: public NTabletFlatExecutor::TTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<NColumnShard::TColumnShard>;
    using TDiagnosticsEvent = NColumnShard::TEvPrivate::TEvReportScanDiagnostics;

    void SendError(const TString& problem, const TString& details, const TActorContext& ctx) const;

    static const TVersionedPresetSchemas& GetPresetSchemas(NColumnShard::TColumnShard& self);

    // Properties of the request. Each reads the request and the tablet, and nothing else.
    ERequestSorting GetRequestSorting() const;
    bool GetDeduplicationEnabled(const TSnapshot& snapshot) const;
    TString GetReaderName() const;
    NConveyorComposite::TCPULimitsConfig GetCpuLimits() const;
    std::optional<NKqp::NScheduler::NHdrf::TFullPoolId> GetSchedulerPool() const;
    std::optional<ESourcesSorting> GetCursorSourcesSorting() const;

    // Steps building the read description
    TReadDescription MakeReadDescription(const TSnapshot& snapshot, const ERequestSorting requestSorting,
        const std::shared_ptr<NLWTrace::TOrbit>& orbit, const EReaderClass readerClass,
        const std::shared_ptr<ITableMetadataAccessor>& tableMetadataAccessor) const;
    ui64 OnScanStartedForPath(const TReadDescription& read, NLWTrace::TOrbit& orbit) const;
    TConclusion<std::unique_ptr<IScannerConstructor>> MakeScannerConstructor(
        const TScannerConstructorContext& context, const TString& readerName) const;
    TConclusionStatus InitScanCursor(TReadDescription& read, const IScannerConstructor& scannerConstructor) const;
    TConclusionStatus InitProgram(TReadDescription& read, const IScannerConstructor& scannerConstructor) const;

    // Null when diagnostics are disabled.
    std::unique_ptr<TDiagnosticsEvent> MakeDiagnosticsEvent(const TReadDescription& read) const;
    void StartScanActor(const TReadMetadataBase::TConstPtr& readMetadataRange, const ui64 rawPathId, std::shared_ptr<NLWTrace::TOrbit>&& orbit,
        std::unique_ptr<TDiagnosticsEvent>&& diagnostics, const NConveyorComposite::TCPULimitsConfig& cpuLimits, const TActorContext& ctx) const;

public:
    using TReadMetadataPtr = TReadMetadataBase::TConstPtr;

    static TSnapshot GetSnapshot(const NColumnShard::TColumnShard& self, const NKikimrTxDataShard::TEvKqpScan& request,
        const NColumnShard::TSchemeShardLocalPathId& ssPathId);
    static TConclusion<std::shared_ptr<TPKRangesFilter>> MakeRanges(
        NColumnShard::TColumnShard& self, const NKikimrTxDataShard::TEvKqpScan& request, const ITableMetadataAccessor& tableMetadataAccessor);

    TTxScan(NColumnShard::TColumnShard* self, TEvDataShard::TEvKqpScan::TPtr& ev, TScanTarget&& target)
        : TBase(self)
        , Ev(ev)
        , Target(std::move(target))
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override;
    void Complete(const TActorContext& ctx) override;

    TTxType GetTxType() const override {
        return NColumnShard::TXTYPE_START_SCAN;
    }

private:
    TEvDataShard::TEvKqpScan::TPtr Ev;
    const TScanTarget Target;
};

}   // namespace NKikimr::NOlap::NReader
