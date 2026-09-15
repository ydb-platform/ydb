#pragma once
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

#include <ydb/library/accessor/accessor.h>

#include <util/generic/hash_set.h>

#include <deque>
#include <memory>

namespace NKikimr::NOlap {

class ISnapshotSchema;

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
    YDB_ACCESSOR(bool, ColumnsSeen, false);

public:
    TPortionToProcess(TInternalPathId pathId, ui64 portionId, TSchemaPtr schema, TString tierName, size_t indexInBatch, bool processColumns)
        : PathId(pathId)
        , PortionId(portionId)
        , Schema(std::move(schema))
        , TierName(std::move(tierName))
        , IndexInBatch(indexInBatch)
        , ProcessColumns(processColumns)
    {
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

    // True if the entity lives in the default (local) storage; defined in batch_cursor.cpp.
    bool IsInDefaultStorage(ui32 entityId) const;
};

class TBatchCursor {
    size_t MaxSize;
    TProcessPortionsStep Step = TProcessPortionsStep::Portions;
    std::deque<TPortionToProcess> Portions;
    size_t PortionCountToProcess = 0;
    std::pair<TInternalPathId, ui64> EndPortionKey = { TInternalPathId::FromRawValue(0), 0 };
    size_t PortionsLoaded = 0;
    bool AllPortionsLoaded = false;

    std::pair<TInternalPathId, ui64> NextLoadPortionKey = { TInternalPathId::FromRawValue(0), 0 };

public:
    TBatchCursor(size_t maxSize)
        : MaxSize(maxSize)
    {
    }

    TBatchCursor(size_t maxSize, std::pair<TInternalPathId, ui64> startKey)
        : MaxSize(maxSize)
        , NextLoadPortionKey(startKey)
    {
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
        NextLoadPortionKey = { pathId, portionId + 1 };
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
                    EndPortionKey = { Portions.back().GetPathId(), Portions.back().GetPortionId() };
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
                    EndPortionKey = { Portions.back().GetPathId(), Portions.back().GetPortionId() };
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
        return { Portions.front().GetPathId(), Portions.front().GetPortionId() };
    }

    void MoveCurrentPortionTo(TInternalPathId pathId, ui64 portionId) {
        std::pair<TInternalPathId, ui64> cur = GetCurrentPortionKey();
        std::pair<TInternalPathId, ui64> key = { pathId, portionId };
        while (cur < key) {
            AdvanceCurrentPortion();
            cur = GetCurrentPortionKey();
        }
    }

    bool NeedToSkip(TInternalPathId pathId, ui64 portionId) const {
        std::pair<TInternalPathId, ui64> cur = GetCurrentPortionKey();
        std::pair<TInternalPathId, ui64> key = { pathId, portionId };
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

// Free function replacing TLeakedBlobsNormalizer::DefineHowToProcessPortion member.
THowToProcessPortion DefineHowToProcessPortion(const TString& portionTier, const std::shared_ptr<ISnapshotSchema>& schema);

}   // namespace NKikimr::NOlap
