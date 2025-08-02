#include "normalizer.h"
#include "restore_appearance_snapshot.h"

#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap::NRestoreAppearanceSnapshot {

class TPortionsWriting: public INormalizerChanges {
private:
    THashMap<TPortionAddress, NKikimrTxColumnShard::TIndexPortionMeta> Portions;

public:
    TPortionsWriting(THashMap<TPortionAddress, NKikimrTxColumnShard::TIndexPortionMeta>&& portions)
        : Portions(std::move(portions)) {
    }

    virtual bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        using IndexPortions = NColumnShard::Schema::IndexPortions;
        const TSnapshot ss = TSnapshot::MaxForPlanInstant(TInstant::Now());
        for (auto&& i : Portions) {
            auto copy = i.second;
            *copy.MutableCompacted()->MutableAppearanceSnapshot() = ss.SerializeToProto();
            db.Table<IndexPortions>()
                .Key(i.first.GetPathId().GetRawValue(), i.first.GetPortionId())
                .Update(NIceDb::TUpdate<IndexPortions::Metadata>(copy.SerializeAsString()));
        }

        return true;
    }

    virtual ui64 GetSize() const override {
        return Portions.size();
    }
};

TConclusion<std::vector<INormalizerTask::TPtr>> TNormalizer::DoInit(
    const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);

    bool ready = true;
    ready = ready & Schema::Precharge<Schema::IndexPortions>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
    }
    THashMap<TPortionAddress, NKikimrTxColumnShard::TIndexPortionMeta> portions0;
    {
        auto rowset = db.Table<Schema::IndexPortions>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            if (rowset.template GetValueOrDefault<IndexPortions::InsertWriteId>(0)) {
                continue;
            }
            TPortionLoadContext portion(rowset);
            if (portion.GetMetaProto().HasCompactedPortion() && portion.GetMetaProto().GetCompactedPortion().HasAppearanceSnapshot()) {
                continue;
            }
            AFL_VERIFY(portions0.emplace(portion.GetAddress(), portion.GetMetaProto()).second);

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    std::vector<INormalizerTask::TPtr> tasks;

    {
        THashMap<TPortionAddress, NKikimrTxColumnShard::TIndexPortionMeta> forTask;
        for (auto&& i : portions0) {
            forTask.emplace(i.first, std::move(i.second));
            if (forTask.size() == 1000) {
                tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TPortionsWriting>(std::move(forTask))));
                forTask.clear();
            }
        }
        if (forTask.size()) {
            tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TPortionsWriting>(std::move(forTask))));
        }
    }

    return tasks;
}

}   // namespace NKikimr::NOlap::NRestoreAppearanceSnapshot
