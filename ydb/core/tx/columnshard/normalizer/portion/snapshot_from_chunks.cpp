#include <ydb/core/tx/columnshard/common/path_id.h>
#include "snapshot_from_chunks.h"
#include "normalizer.h"

#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap::NSyncMinSnapshotFromChunks {

class TPatchItem {
private:
    TPortionLoadContext PortionInfo;
    YDB_READONLY(NOlap::TSnapshot, Snapshot, NOlap::TSnapshot::Zero());

public:
    const TPortionLoadContext& GetPortionInfo() const {
        return PortionInfo;
    }

    TPatchItem(TPortionLoadContext&& portion, const NOlap::TSnapshot& snapshot)
        : PortionInfo(std::move(portion))
        , Snapshot(snapshot) {
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
            db.Table<Schema::IndexPortions>()
                .Key(i.GetPortionInfo().GetPathId().GetInternalPathIdValue(), i.GetPortionInfo().GetPortionId())
                .Update(NIceDb::TUpdate<Schema::IndexPortions::MinSnapshotPlanStep>(i.GetSnapshot().GetPlanStep()),
                    NIceDb::TUpdate<Schema::IndexPortions::MinSnapshotTxId>(i.GetSnapshot().GetTxId())
                    );
        }

        return true;
    }

    virtual ui64 GetSize() const override {
        return Patches.size();
    }

};

TConclusion<std::vector<INormalizerTask::TPtr>> TNormalizer::DoInit(
    const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);

    bool ready = true;
    ready = ready & Schema::Precharge<Schema::IndexPortions>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
    }

    THashMap<ui64, TPortionLoadContext> dbPortions;
    THashMap<ui64, NOlap::TSnapshot> initSnapshot;
    
    {
        auto rowset = db.Table<Schema::IndexPortions>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            TPortionLoadContext portion(rowset);
            if (!portion.GetDeprecatedMinSnapshot()) {
                AFL_VERIFY(dbPortions.emplace(portion.GetPortionId(), portion).second);
            }

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    {
        auto rowset = db.Table<Schema::IndexColumns>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            TColumnChunkLoadContext chunk(rowset, &DsGroupSelector);
            const ui64 portionId = chunk.GetPortionId();
            if (dbPortions.contains(portionId)) {
                auto it = initSnapshot.find(portionId);
                if (it == initSnapshot.end()) {
                    initSnapshot.emplace(portionId, chunk.GetMinSnapshotDeprecated());
                } else {
                    AFL_VERIFY(it->second == chunk.GetMinSnapshotDeprecated());
                }
            }

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }
    AFL_VERIFY(dbPortions.size() == initSnapshot.size())("portions", dbPortions.size())("records", initSnapshot.size());

    std::vector<INormalizerTask::TPtr> tasks;
    if (dbPortions.empty()) {
        return tasks;
    }

    std::vector<TPatchItem> package;

    for (auto&& [portionId, portion] : dbPortions) {
        auto it = initSnapshot.find(portionId);
        AFL_VERIFY(it != initSnapshot.end());
        package.emplace_back(std::move(portion), it->second);
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

}   // namespace NKikimr::NOlap::NChunksActualization
