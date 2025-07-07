#pragma once
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/counters/columnshard.h>
#include <ydb/core/tx/columnshard/counters/splitter.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>

namespace NKikimr::NOlap {

class TWritingContext {
private:
    YDB_READONLY(ui64, TabletId, 0);
    YDB_READONLY(NActors::TActorId, BufferizationPortionsActorId, NActors::TActorId());
    YDB_READONLY(NActors::TActorId, TabletActorId, NActors::TActorId());
    YDB_READONLY_DEF(std::shared_ptr<ISnapshotSchema>, ActualSchema);
    YDB_READONLY_DEF(std::shared_ptr<IStoragesManager>, StoragesManager);
    YDB_READONLY_DEF(std::shared_ptr<NColumnShard::TSplitterCounters>, SplitterCounters);
    YDB_READONLY_DEF(std::shared_ptr<NColumnShard::TWriteCounters>, WritingCounters);
    YDB_READONLY(TSnapshot, ApplyToSnapshot, TSnapshot::Zero());
    const std::shared_ptr<const TAtomicCounter> ActivityChecker;
    YDB_READONLY(bool, NoTxWrite, false);

public:
    void MergeFrom(const TWritingContext& newContext) {
        AFL_VERIFY(TabletId == newContext.TabletId);
        AFL_VERIFY(TabletActorId == newContext.TabletActorId);
        AFL_VERIFY(ActualSchema->GetVersion() == newContext.ActualSchema->GetVersion());
        if (ApplyToSnapshot < newContext.ApplyToSnapshot) {
            ApplyToSnapshot = newContext.ApplyToSnapshot;
        }
    }

    bool IsActive() const {
        return ActivityChecker->Val();
    }

    TWritingContext(const ui64 tabletId, const NActors::TActorId& tabletActorId, const std::shared_ptr<ISnapshotSchema>& actualSchema,
        const std::shared_ptr<IStoragesManager>& operators, const std::shared_ptr<NColumnShard::TSplitterCounters>& splitterCounters,
        const std::shared_ptr<NColumnShard::TWriteCounters>& writingCounters, const TSnapshot& applyToSnapshot,
        const std::shared_ptr<const TAtomicCounter>& activityChecker, const bool noTxWrite, const NActors::TActorId& bufferizationPortionsActorId)
        : TabletId(tabletId)
        , BufferizationPortionsActorId(bufferizationPortionsActorId)
        , TabletActorId(tabletActorId)
        , ActualSchema(actualSchema)
        , StoragesManager(operators)
        , SplitterCounters(splitterCounters)
        , WritingCounters(writingCounters)
        , ApplyToSnapshot(applyToSnapshot)
        , ActivityChecker(activityChecker)
        , NoTxWrite(noTxWrite)
    {
        AFL_VERIFY(ActivityChecker);
    }
};
}   // namespace NKikimr::NOlap
