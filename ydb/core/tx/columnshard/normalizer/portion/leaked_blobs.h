#pragma once

#include "normalizer.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NOlap {

enum class TProcessPortionsStep {
    Portions,
    Columns,
    Indices,
    Finished
};

enum class THowToProcessPortion {
    Skip,
    OnlyIndices,
    All
};


class TPortionToProcess {
    using TSchemaPtr = std::shared_ptr<ISnapshotSchema>;

    YDB_READONLY_DEF(TInternalPathId, PathId);
    YDB_READONLY_DEF(ui64, PortionId);
    YDB_READONLY_DEF(TSchemaPtr, Schema);
    YDB_READONLY_DEF(TString, TierName);
    YDB_READONLY_DEF(size_t, IndexInBatch);
    YDB_READONLY_DEF(bool, ProcessColumns);
    YDB_READONLY_DEF(THashSet<ui16>, DeferredIndexBlobIdxs);

public:
    TPortionToProcess(TInternalPathId pathId, ui64 portionId, TSchemaPtr schema, TString tierName, size_t indexInBatch, bool processColumns)
        : PathId(pathId)
        , PortionId(portionId)
        , Schema(std::move(schema))
        , TierName(std::move(tierName))
        , IndexInBatch(indexInBatch)
        , ProcessColumns(processColumns) {
    }

    void AddDeferredIndexBlobIdx(ui16 blobIdx) {
        DeferredIndexBlobIdxs.emplace(blobIdx);
    }

    bool NeedToProcessColumns() const {
        return ProcessColumns || !DeferredIndexBlobIdxs.empty();
    }

    bool IsInDefaultStorage() const {
        return TierName.empty() || TierName == NBlobOperations::TGlobal::DefaultStorageId;
    }

    bool IsInDefaultStorage(const ui32 entityId) const {
        const TString& effectiveTier = TierName.empty() ? NBlobOperations::TGlobal::DefaultStorageId : TierName;
        return Schema->GetIndexInfo().GetEntityStorageId(entityId, effectiveTier) == NBlobOperations::TGlobal::DefaultStorageId;
    }
};

class TBatchCursor {
    size_t MaxSize;
    TProcessPortionsStep Step = TProcessPortionsStep::Portions;
    std::deque<TPortionToProcess> Portions;
    size_t PortionCountToProcess = 0;
    std::pair<TInternalPathId, ui64> EndPortionKey = {TInternalPathId::FromRawValue(0), 0};
    size_t PortionsLoaded = 0;
    bool AllPortionsLoaded = false;

    std::pair<TInternalPathId, ui64> NextLoadPortionKey = {TInternalPathId::FromRawValue(0), 0};
public:
    TBatchCursor(size_t maxSize) : MaxSize(maxSize) {

    }

    bool IsFinished() const {
        return Step == TProcessPortionsStep::Finished;
    }

    TProcessPortionsStep GetStep() const {
        return Step;
    }

    bool IsFull() const {
        return PortionsLoaded >= MaxSize;
    }

    std::pair<TInternalPathId, ui64> GetNextLoadPortionKey() const {
        return NextLoadPortionKey;
    }

    void AddPortion(TInternalPathId pathId, ui64 portionId, std::shared_ptr<ISnapshotSchema> schema, TString tierName, bool processColumns) {
        Portions.emplace_back(pathId, portionId, schema, tierName, Portions.size(), processColumns);
    }

    void OnPortionLoaded(TInternalPathId pathId, ui64 portionId) {
        NextLoadPortionKey = {pathId, portionId + 1};
        PortionsLoaded++;
    }

    void NoMorePortions() {
        AllPortionsLoaded = true;
    }

    void NextStep() {
        switch (Step) {
            case TProcessPortionsStep::Portions:
                if (Portions.empty()) {
                    if (AllPortionsLoaded) {
                        ToFinishedStep();
                    } else {
                        ToPortionsStep();
                    }
                } else {
                    Step = TProcessPortionsStep::Indices;
                    EndPortionKey = {Portions.back().GetPathId(), Portions.back().GetPortionId()};
                    PortionCountToProcess = Portions.size();
                }
                break;
            case TProcessPortionsStep::Indices:
                while (PortionCountToProcess > 0) {
                    AdvanceCurrentPortion();
                }
                if (Portions.empty()) {
                    if (AllPortionsLoaded) {
                        ToFinishedStep();
                    } else {
                        ToPortionsStep();
                    }
                } else {
                    Step = TProcessPortionsStep::Columns;
                    EndPortionKey = {Portions.back().GetPathId(), Portions.back().GetPortionId()};
                    PortionCountToProcess = Portions.size();
                }
                break;
            case TProcessPortionsStep::Columns:
                if (AllPortionsLoaded) {
                    ToFinishedStep();
                } else {
                    ToPortionsStep();
                }
                break;
            case TProcessPortionsStep::Finished:
                AFL_VERIFY(false)("error", "cannot advance from the finished step");
        }

    }

    TInternalPathId StartPathId() const {
        return Portions.front().GetPathId();
    }

    ui64 StartPortionId() const {
        return Portions.front().GetPortionId();
    }

    TInternalPathId EndPathId() const {
        return EndPortionKey.first;
    }

    ui64 EndPortionId() const {
        return EndPortionKey.second;
    }

    TPortionToProcess& GetCurrentPortion() {
        AFL_VERIFY(!Portions.empty())("error", "no current portion");
        return Portions.front();
    }

    std::pair<TInternalPathId, ui64> GetCurrentPortionKey() const {
        AFL_VERIFY(!Portions.empty())("error", "no current portion");
        return {Portions.front().GetPathId(), Portions.front().GetPortionId()};
    }

    void MoveCurrentPortionTo(TInternalPathId pathId, ui64 portionId) {
        std::pair<TInternalPathId, ui64> cur = GetCurrentPortionKey();
        std::pair<TInternalPathId, ui64> key = {pathId, portionId};
        while (cur < key) {
            AdvanceCurrentPortion();
            cur = GetCurrentPortionKey();
        }
    }

    bool NeedToSkip(TInternalPathId pathId, ui64 portionId) const {
        std::pair<TInternalPathId, ui64> cur = GetCurrentPortionKey();
        std::pair<TInternalPathId, ui64> key = {pathId, portionId};
        AFL_VERIFY(key <= cur)("error", "key is greater than current portion");
        return key < cur;
    }

private:

    void ToPortionsStep() {
        Step = TProcessPortionsStep::Portions;
        PortionsLoaded = 0;
        Portions.clear();
        PortionCountToProcess = 0;
    }

    void ToFinishedStep() {
        // just to clean up
        ToPortionsStep();
        Step = TProcessPortionsStep::Finished;
    }

    void AdvanceCurrentPortion() {
        AFL_VERIFY(PortionCountToProcess > 0)("error", "no portion to advance");
        PortionCountToProcess--;
        auto front = Portions.front();
        Portions.pop_front();
        if (Step == TProcessPortionsStep::Indices && front.NeedToProcessColumns()) {
            Portions.push_back(front);
        }
    }

};

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
        : TabletId(tabletId) {
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

    TBatchCursor BatchCursor{1000000};
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

    THowToProcessPortion DefineHowToProcessPortion(const TString& portionTier, const ISnapshotSchema::TPtr& schema) const;
};
}   // namespace NKikimr::NOlap
