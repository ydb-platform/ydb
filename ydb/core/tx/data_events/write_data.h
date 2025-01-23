#pragma once
#include "common/modification_type.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/long_tx_service/public/types.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/monotonic.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/formats/arrow/modifier/subset.h>

#include <util/generic/guid.h>

namespace NKikimr::NOlap {
class IBlobsWritingAction;
}

namespace NKikimr::NEvWrite {

enum class EWriteStage {
    Created = 0,
    Queued,
    Started,
    BuildBatch,
    WaitFlush,
    BuildSlices,
    BuildSlicesPack,
    Finished,
    Aborted
};

class IDataContainer {
private:
    YDB_ACCESSOR_DEF(NArrow::NMerger::TIntervalPositions, SeparationPoints);

public:
    using TPtr = std::shared_ptr<IDataContainer>;
    virtual ~IDataContainer() {
    }
    virtual TConclusion<std::shared_ptr<arrow::RecordBatch>> ExtractBatch() = 0;
    virtual ui64 GetSchemaVersion() const = 0;
    virtual ui64 GetSize() const = 0;
};

class TWriteFlowCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;

    std::vector<NMonitoring::TDynamicCounters::TCounterPtr> CountByWriteStage;
    std::vector<NMonitoring::TDynamicCounters::TCounterPtr> WriteStageAdd;
    std::vector<std::vector<NMonitoring::TDynamicCounters::TCounterPtr>> CountByStageMoving;
    std::vector<std::vector<NMonitoring::TDynamicCounters::TCounterPtr>> CountByStageDuration;
    std::vector<NMonitoring::THistogramPtr> DurationToStage;
    NMonitoring::THistogramPtr DurationToFinish;
    NMonitoring::THistogramPtr DurationToAbort;

public:
    void OnStageMove(const EWriteStage fromStage, const EWriteStage toStage, const TDuration d) const {
        CountByWriteStage[(ui32)fromStage]->Sub(1);
        CountByWriteStage[(ui32)toStage]->Add(1);
        WriteStageAdd[(ui32)toStage]->Add(1);
        DurationToStage[(ui32)toStage]->Collect(d.MilliSeconds());
        CountByStageMoving[(ui32)fromStage][(ui32)toStage]->Add(1);
        CountByStageDuration[(ui32)fromStage][(ui32)toStage]->Add(d.MilliSeconds());
    }

    void OnWritingStart(const EWriteStage stage) const {
        WriteStageAdd[(ui32)stage]->Add(1);
        CountByWriteStage[(ui32)stage]->Add(1);
    }

    void OnWriteFinished(const TDuration d) const {
        DurationToFinish->Collect(d.MicroSeconds());
    }

    void OnWriteAborted(const TDuration d) const {
        DurationToAbort->Collect(d.MicroSeconds());
    }

    TWriteFlowCounters()
        : TBase("CSWriteFlow") {
        for (auto&& i : GetEnumAllValues<EWriteStage>()) {
            auto sub = CreateSubGroup("stage", ::ToString(i));
            CountByWriteStage.emplace_back(sub.GetValue("Count"));
            WriteStageAdd.emplace_back(sub.GetDeriviative("Moving/Count"));
            DurationToStage.emplace(i, sub.GetHistogram("DurationToStageMs", NMonitoring::ExponentialHistogram(18, 2, 1)));
            CountByStageMoving.emplace_back();
            CountByStageDuration.emplace_back();
            for (auto&& to : GetEnumAllValues<EWriteStage>()) {
                auto subTo = sub.CreateSubGroup("stage_to", ::ToString(to));
                CountByStageMoving.back().emplace_back(subTo.GetDeriviative("Transfers/Count"));
                CountByStageDuration.back().emplace_back(subTo.GetDeriviative("Transfers/Count"));
            }
        }
    }
};

class TWriteMeta: public NColumnShard::TMonitoringObjectsCounter<TWriteMeta>, TNonCopyable {
private:
    YDB_ACCESSOR(ui64, WriteId, 0);
    YDB_READONLY(ui64, TableId, 0);
    YDB_ACCESSOR_DEF(NActors::TActorId, Source);
    YDB_ACCESSOR_DEF(std::optional<ui32>, GranuleShardingVersion);
    YDB_READONLY(TString, Id, TGUID::CreateTimebased().AsUuidString());

    // Long Tx logic
    YDB_OPT(NLongTxService::TLongTxId, LongTxId);
    YDB_ACCESSOR(ui64, WritePartId, 0);
    YDB_ACCESSOR_DEF(TString, DedupId);

    YDB_ACCESSOR(EModificationType, ModificationType, EModificationType::Replace);
    YDB_READONLY(TMonotonic, WriteStartInstant, TMonotonic::Now());
    std::optional<ui64> LockId;
    const std::shared_ptr<TWriteFlowCounters> Counters;
    mutable TMonotonic LastStageInstant = TMonotonic::Now();
    mutable EWriteStage CurrentStage = EWriteStage::Created;

public:
    void OnStage(const EWriteStage stage) const;

    ~TWriteMeta() {
        if (CurrentStage != EWriteStage::Finished && CurrentStage != EWriteStage::Aborted) {
            Counters->OnWriteAborted(TMonotonic::Now() - WriteStartInstant);
        }
    }

    void SetLockId(const ui64 lockId) {
        LockId = lockId;
    }

    ui64 GetLockIdVerified() const {
        AFL_VERIFY(LockId);
        return *LockId;
    }

    std::optional<ui64> GetLockIdOptional() const {
        return LockId;
    }

    bool IsGuaranteeWriter() const {
        switch (ModificationType) {
            case EModificationType::Delete:
            case EModificationType::Upsert:
            case EModificationType::Insert:
                return true;
            case EModificationType::Update:
            case EModificationType::Replace:
                return false;
        }
    }

    TWriteMeta(const ui64 writeId, const ui64 tableId, const NActors::TActorId& source, const std::optional<ui32> granuleShardingVersion,
        const TString& writingIdentifier, const std::shared_ptr<TWriteFlowCounters>& counters)
        : WriteId(writeId)
        , TableId(tableId)
        , Source(source)
        , GranuleShardingVersion(granuleShardingVersion)
        , Id(writingIdentifier)
        , Counters(counters)
    {
        Counters->OnWritingStart(CurrentStage);
    }
};

class TWriteData: TNonCopyable {
private:
    TWriteMeta WriteMeta;
    YDB_READONLY_DEF(IDataContainer::TPtr, Data);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, PrimaryKeySchema);
    YDB_READONLY_DEF(std::shared_ptr<NOlap::IBlobsWritingAction>, BlobsAction);
    YDB_ACCESSOR_DEF(std::optional<NArrow::TSchemaSubset>, SchemaSubset);
    YDB_READONLY(bool, WritePortions, false);

public:
    TWriteData(const TWriteMeta& writeMeta, IDataContainer::TPtr data, const std::shared_ptr<arrow::Schema>& primaryKeySchema,
        const std::shared_ptr<NOlap::IBlobsWritingAction>& blobsAction, const bool writePortions);

    const NArrow::TSchemaSubset& GetSchemaSubsetVerified() const {
        AFL_VERIFY(SchemaSubset);
        return *SchemaSubset;
    }

    const TWriteMeta& GetWriteMeta() const {
        return WriteMeta;
    }

    TWriteMeta& MutableWriteMeta() {
        return WriteMeta;
    }

    ui64 GetSize() const {
        return Data->GetSize();
    }
};

}   // namespace NKikimr::NEvWrite
