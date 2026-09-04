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

#include <library/cpp/lwtrace/all.h>

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
    YDB_READONLY(TMonotonic, OrbitStartInstant, TMonotonic::Now());
    YDB_READONLY(ui64, TabletId, 0);
    YDB_READONLY(ui64, Cookie, 0);
    YDB_READONLY(ui64, TxId, 0);
    const std::shared_ptr<TWriteFlowCounters> Counters;
    mutable NOlap::NCounters::TStateSignalsOperator<NEvWrite::EWriteStage>::TGuard StateGuard;
    std::shared_ptr<NLWTrace::TOrbit> Orbit;

    YDB_FLAG_ACCESSOR(Bulk, false);

public:
    void OnStage(const EWriteStage stage) const;

    const std::shared_ptr<NLWTrace::TOrbit>& GetOrbit() const {
        return Orbit;
    }

    ~TWriteMeta() {
        if (StateGuard.GetStage() != EWriteStage::Finished) {
            OnStage(EWriteStage::Aborted);
        }
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
            case EModificationType::UpsertIncrement:
                return false;
        }
    }

    TWriteMeta(const ui64 writeId, const NColumnShard::TUnifiedPathId& pathId, const NActors::TActorId& source,
        const std::optional<ui32> granuleShardingVersion, const TString& writingIdentifier, const std::shared_ptr<TWriteFlowCounters>& counters,
        std::shared_ptr<NLWTrace::TOrbit> orbit = nullptr, const ui64 tabletId = 0, const ui64 cookie = 0, const ui64 txId = 0,
        const TMonotonic orbitStartInstant = TMonotonic::Now())
        : WriteId(writeId)
        , PathId(pathId)
        , Source(source)
        , GranuleShardingVersion(granuleShardingVersion)
        , Id(writingIdentifier)
        , OrbitStartInstant(orbitStartInstant)
        , TabletId(tabletId)
        , Cookie(cookie)
        , TxId(txId)
        , Counters(counters)
        , StateGuard(Counters->MutableTracing().BuildGuard(NEvWrite::EWriteStage::Created))
        , Orbit(std::move(orbit)) {
    }
};

class TWriteData {
private:
    std::shared_ptr<TWriteMeta> WriteMeta;
    YDB_READONLY_DEF(IDataContainer::TPtr, Data);
    YDB_READONLY_DEF(std::shared_ptr<arrow::Schema>, PrimaryKeySchema);
    YDB_READONLY_DEF(std::shared_ptr<NOlap::IBlobsWritingAction>, BlobsAction);
    YDB_ACCESSOR_DEF(std::optional<NArrow::TSchemaSubset>, SchemaSubset);

public:
    TWriteData(const std::shared_ptr<TWriteMeta>& writeMeta, IDataContainer::TPtr data, const std::shared_ptr<arrow::Schema>& primaryKeySchema,
        const std::shared_ptr<NOlap::IBlobsWritingAction>& blobsAction);

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
