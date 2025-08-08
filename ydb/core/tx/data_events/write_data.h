#pragma once
#include "common/modification_type.h"
#include "common/signals_flow.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/long_tx_service/public/types.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/monotonic.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/formats/arrow/modifier/subset.h>
#include <ydb/library/signals/object_counter.h>

#include <util/generic/guid.h>

namespace NKikimr::NOlap {
class IBlobsWritingAction;
}

namespace NKikimr::NEvWrite {

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

class TWriteMeta: public NColumnShard::TMonitoringObjectsCounter<TWriteMeta>, TNonCopyable {
private:
    YDB_ACCESSOR(ui64, WriteId, 0);
    YDB_READONLY_DEF(NColumnShard::TUnifiedPathId, PathId);
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
    mutable NOlap::NCounters::TStateSignalsOperator<NEvWrite::EWriteStage>::TGuard StateGuard;

    YDB_FLAG_ACCESSOR(Bulk, false);

public:
    void OnStage(const EWriteStage stage) const;

    ~TWriteMeta() {
        if (StateGuard.GetStage() != EWriteStage::Finished) {
            OnStage(EWriteStage::Aborted);
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
            case EModificationType::Increment:
                return false;
        }
    }

    TWriteMeta(const ui64 writeId, const NColumnShard::TUnifiedPathId& pathId, const NActors::TActorId& source,
        const std::optional<ui32> granuleShardingVersion, const TString& writingIdentifier, const std::shared_ptr<TWriteFlowCounters>& counters)
        : WriteId(writeId)
        , PathId(pathId)
        , Source(source)
        , GranuleShardingVersion(granuleShardingVersion)
        , Id(writingIdentifier)
        , Counters(counters)
        , StateGuard(Counters->MutableTracing().BuildGuard(NEvWrite::EWriteStage::Created)) {
    }
};

class TWriteData {
private:
    std::shared_ptr<TWriteMeta> WriteMeta;
    YDB_READONLY_DEF(IDataContainer::TPtr, Data);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, PrimaryKeySchema);
    YDB_READONLY_DEF(std::shared_ptr<NOlap::IBlobsWritingAction>, BlobsAction);
    YDB_ACCESSOR_DEF(std::optional<NArrow::TSchemaSubset>, SchemaSubset);
    YDB_READONLY(bool, WritePortions, false);

public:
    TWriteData(const std::shared_ptr<TWriteMeta>& writeMeta, IDataContainer::TPtr data, const std::shared_ptr<arrow::Schema>& primaryKeySchema,
        const std::shared_ptr<NOlap::IBlobsWritingAction>& blobsAction, const bool writePortions);

    const NArrow::TSchemaSubset& GetSchemaSubsetVerified() const {
        AFL_VERIFY(SchemaSubset);
        return *SchemaSubset;
    }

    const TWriteMeta& GetWriteMeta() const {
        return *WriteMeta;
    }

    const std::shared_ptr<TWriteMeta>& GetWriteMetaPtr() const {
        return WriteMeta;
    }

    TWriteMeta& MutableWriteMeta() {
        return *WriteMeta;
    }

    ui64 GetSize() const {
        return Data->GetSize();
    }
};

}   // namespace NKikimr::NEvWrite
