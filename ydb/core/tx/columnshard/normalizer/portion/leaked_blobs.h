#pragma once

#include "batch_cursor.h"
#include "normalizer.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NOlap {

class TLeakedBlobsStats {
    const ui64 TabletId;
    ui64 TablePathId = 0;
    TString TablePath = "<unknown>";
    NActors::NLog::EPriority LogLevel = NActors::NLog::PRI_WARN;
    ui64 StoppedOnPortions = 0;
    ui64 StoppedOnIndices = 0;
    ui64 StoppedOnColumns = 0;
    ui64 StoppedOnBlobsToDelete = 0;
    bool Completed = false;

    ui64 PortionsLoaded = 0;
    ui64 PortionsSkipped = 0;
    ui64 PortionsOnlyIndicesInBs = 0;
    ui64 PortionsInBs = 0;

    ui64 IndicesLoaded = 0;
    ui64 IndicesInplaced = 0;
    ui64 IndicesInForeignStorage = 0;
    ui64 IndicesNeedColumnV2 = 0;
    ui64 IndicesHaveItsOwnBlob = 0;

    ui64 ColumnsLoaded = 0;
    ui64 BlobsToDeleteLoaded = 0;

public:
    explicit TLeakedBlobsStats(const ui64 tabletId)
        : TabletId(tabletId)
    {
    }

    void SetLogLevel(const NActors::NLog::EPriority logLevel) {
        LogLevel = logLevel;
    }

    void SetTableIdentity(const ui64 tablePathId, const TString& tablePath) {
        TablePathId = tablePathId;
        TablePath = tablePath;
    }

    void OnStoppedOnPortions() {
        StoppedOnPortions++;
    }

    void OnStoppedOnIndices() {
        StoppedOnIndices++;
    }

    void OnStoppedOnColumns() {
        StoppedOnColumns++;
    }

    void OnStoppedOnBlobsToDelete() {
        StoppedOnBlobsToDelete++;
    }

    void OnPortionLoaded(THowToProcessPortion howToProcessPortion) {
        PortionsLoaded++;
        if (howToProcessPortion == THowToProcessPortion::Skip) {
            PortionsSkipped++;
        } else if (howToProcessPortion == THowToProcessPortion::OnlyIndices) {
            PortionsOnlyIndicesInBs++;
        } else {
            PortionsInBs++;
        }
    }

    void OnIndexLoaded() {
        IndicesLoaded++;
    }

    void OnIndexInplaced() {
        IndicesInplaced++;
    }

    void OnIndexInForeignStorage() {
        IndicesInForeignStorage++;
    }

    void OnIndexNeedColumnV2() {
        IndicesNeedColumnV2++;
    }

    void OnIndexHasItsOwnBlob() {
        IndicesHaveItsOwnBlob++;
    }

    void OnColumnLoaded() {
        ColumnsLoaded++;
    }

    void OnBlobsToDeleteLoaded() {
        BlobsToDeleteLoaded++;
    }

    void OnCompleted() {
        Completed = true;
    }

    bool WannaPrint() const {
        return (StoppedOnPortions + StoppedOnIndices + StoppedOnColumns + StoppedOnBlobsToDelete) % 10 == 0;
    }

    void PrintToLog() const;
};

class TLeakedBlobsNormalizer: public TNormalizationController::INormalizerComponent {
private:
    using TBase = TNormalizationController::INormalizerComponent;

public:
    class TNormalizerResult;
    class TTask;

    static TString GetClassNameStatic() {
        return "LeakedBlobsNormalizer";
    }

private:
    static inline TFactory::TRegistrator<TLeakedBlobsNormalizer> Registrator =
        TFactory::TRegistrator<TLeakedBlobsNormalizer>(GetClassNameStatic());

public:
    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return std::nullopt;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    TLeakedBlobsNormalizer(const TNormalizationController::TInitContext& info);

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(
        const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;

private:
    TVector<TTabletChannelInfo> Channels;
    TActorId TRemoveLeakedBlobsActorId;
    NColumnShard::TBlobGroupSelector DsGroupSelector;

    bool ParamsInitialized = false;
    size_t BatchSize = 1000000;
    bool PrintLeakedBlobIds = false;
    NActors::NLog::EPriority LogLevel = NActors::NLog::PRI_WARN;

    TBatchCursor BatchCursor{ 1000000 };
    TLeakedBlobsStats Stats;
    THashSet<TLogoBlobID> Result;
    THashSet<TLogoBlobID> ResultToDelete;
    ui64 TablePathId = 0;
    TString TablePath = "<unknown>";

    void ReadParamsFromDescription();
    void LoadTableIdentity(NIceDb::TNiceDb& db);

    TConclusionStatus LoadPortionBlobIds(TDbWrapper& wrapper, const TVersionedIndex& versionedIndex);
    TConclusionStatus LoadPortions(TDbWrapper& wrapper, const TVersionedIndex& versionedIndex);
    TConclusionStatus LoadIndices(TDbWrapper& wrapper);
    TConclusionStatus LoadColumns(TDbWrapper& wrapper);
    TConclusionStatus LoadBlobsToDelete(NIceDb::TNiceDb& db);
};
}   // namespace NKikimr::NOlap
