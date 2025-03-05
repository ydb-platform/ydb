#include "normalizer.h"
#include "restore_portion_from_chunks.h"

#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap::NRestorePortionsFromChunks {

class TPatchItem {
private:
    YDB_READONLY(ui32, SchemaVersion, 0);
    TColumnChunkLoadContext ChunkInfo;

public:
    const TColumnChunkLoadContext& GetChunkInfo() const {
        return ChunkInfo;
    }

    TPatchItem(const ui32 schemaVersion, TColumnChunkLoadContext&& chunkInfo)
        : SchemaVersion(schemaVersion)
        , ChunkInfo(std::move(chunkInfo)) {
    }
};

class TChanges: public INormalizerChanges {
private:
    std::vector<TPatchItem> Patches;

public:
    TChanges(std::vector<TPatchItem>&& patches)
        : Patches(std::move(patches)) {
    }
    virtual bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        for (auto&& i : Patches) {
            AFL_VERIFY(i.GetChunkInfo().GetMetaProto().HasPortionMeta());
            auto metaProtoString = i.GetChunkInfo().GetMetaProto().GetPortionMeta().SerializeAsString();
            using IndexPortions = NColumnShard::Schema::IndexPortions;
            const auto removeSnapshot = i.GetChunkInfo().GetRemoveSnapshot();
            const auto minSnapshotDeprecated = i.GetChunkInfo().GetMinSnapshotDeprecated();
            db.Table<IndexPortions>()
                .Key(i.GetChunkInfo().GetPathId(), i.GetChunkInfo().GetPortionId())
                .Update(NIceDb::TUpdate<IndexPortions::SchemaVersion>(i.GetSchemaVersion()), NIceDb::TUpdate<IndexPortions::ShardingVersion>(0),
                    NIceDb::TUpdate<IndexPortions::CommitPlanStep>(0), NIceDb::TUpdate<IndexPortions::CommitTxId>(0),
                    NIceDb::TUpdate<IndexPortions::InsertWriteId>(0), NIceDb::TUpdate<IndexPortions::XPlanStep>(removeSnapshot.GetPlanStep()),
                    NIceDb::TUpdate<IndexPortions::XTxId>(removeSnapshot.GetTxId()),
                    NIceDb::TUpdate<IndexPortions::MinSnapshotPlanStep>(minSnapshotDeprecated.GetPlanStep()),
                    NIceDb::TUpdate<IndexPortions::MinSnapshotTxId>(minSnapshotDeprecated.GetTxId()),
                    NIceDb::TUpdate<IndexPortions::Metadata>(metaProtoString));
        }

        return true;
    }

    virtual ui64 GetSize() const override {
        return Patches.size();
    }
};

TConclusion<std::vector<INormalizerTask::TPtr>> TNormalizer::DoInit(
    const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);

    bool ready = true;
    ready = ready & Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::IndexPortions>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
    }

    std::shared_ptr<TVersionCounters> versionCounters = std::make_shared<TVersionCounters>();
    TTablesManager tablesManager(controller.GetStoragesManager(), std::make_shared<NDataAccessorControl::TLocalManager>(nullptr),
        std::make_shared<TSchemaObjectsCache>(), std::make_shared<TPortionIndexStats>(), versionCounters, 0);
    if (!tablesManager.InitFromDB(db)) {
        ACFL_TRACE("normalizer", "TChunksNormalizer")("error", "can't initialize tables manager");
        return TConclusionStatus::Fail("Can't load index");
    }

    THashMap<ui64, TPortionLoadContext> dbPortions;

    {
        auto rowset = db.Table<Schema::IndexPortions>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            TPortionLoadContext portion(rowset);
            AFL_VERIFY(dbPortions.emplace(portion.GetPortionId(), portion).second);

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    THashMap<ui64, TColumnChunkLoadContext> portionsToWrite;
    {
        auto rowset = db.Table<Schema::IndexColumns>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        THashSet<ui64> portionsToRestore;
        while (!rowset.EndOfSet()) {
            TColumnChunkLoadContext chunk(rowset, &DsGroupSelector);
            if (!dbPortions.contains(chunk.GetPortionId())) {
                portionsToRestore.emplace(chunk.GetPortionId());
                if (chunk.GetMetaProto().HasPortionMeta()) {
                    AFL_VERIFY(portionsToWrite.emplace(chunk.GetPortionId(), chunk).second);
                }
            }

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
        AFL_VERIFY(portionsToRestore.size() == portionsToWrite.size());
    }

    std::vector<INormalizerTask::TPtr> tasks;
    if (portionsToWrite.empty()) {
        return tasks;
    }

    std::vector<TPatchItem> package;

    for (auto&& [_, chunkWithPortionData] : portionsToWrite) {
        package.emplace_back(
            tablesManager.GetPrimaryIndexSafe().GetVersionedIndex().GetSchemaVerified(chunkWithPortionData.GetMinSnapshotDeprecated())->GetVersion(),
            std::move(chunkWithPortionData));
        if (package.size() == 100) {
            std::vector<TPatchItem> local;
            local.swap(package);
            tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges>(std::move(local))));
        }
    }

    if (package.size() > 0) {
        tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges>(std::move(package))));
    }
    return tasks;
}

}   // namespace NKikimr::NOlap::NRestorePortionsFromChunks
