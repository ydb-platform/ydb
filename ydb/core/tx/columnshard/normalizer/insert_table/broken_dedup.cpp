#include "broken_dedup.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NInsertionDedup {

class TNormalizerRemoveChanges: public INormalizerChanges {
private:
    std::vector<TInsertTableRecordLoadContext> Insertions;
public:
    virtual bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /*normalizationContext*/) const override {
        NIceDb::TNiceDb db(txc.DB);
        for (auto&& i : Insertions) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "remove_aborted_record")("write_id", i.GetInsertWriteId());
            i.Remove(db);
        }
        return true;
    }
    virtual void ApplyOnComplete(const TNormalizationController& /*normalizationContext*/) const override {

    }

    virtual ui64 GetSize() const override {
        return Insertions.size();
    }

    TNormalizerRemoveChanges(const std::vector<TInsertTableRecordLoadContext>& insertions)
        : Insertions(insertions)
    {

    }
};

class TNormalizerCleanDedupChanges: public INormalizerChanges {
private:
    mutable std::vector<TInsertTableRecordLoadContext> Insertions;

public:
    virtual bool ApplyOnExecute(
        NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /*normalizationContext*/) const override {
        NIceDb::TNiceDb db(txc.DB);
        for (auto&& i : Insertions) {
            AFL_VERIFY(i.GetDedupId());
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "correct_record")("dedup", i.GetDedupId());
            i.Remove(db);
            i.SetDedupId("");
            i.Upsert(db);
        }
        return true;
    }
    virtual void ApplyOnComplete(const TNormalizationController& /*normalizationContext*/) const override {
    }

    virtual ui64 GetSize() const override {
        return Insertions.size();
    }

    TNormalizerCleanDedupChanges(const std::vector<TInsertTableRecordLoadContext>& insertions)
        : Insertions(insertions) {
    }
};


class TCollectionStates {
private:
    YDB_READONLY_DEF(std::optional<TInsertTableRecordLoadContext>, Inserted);
    YDB_READONLY_DEF(std::optional<TInsertTableRecordLoadContext>, Aborted);
public:
    void SetInserted(const TInsertTableRecordLoadContext& context) {
        AFL_VERIFY(!Inserted);
        Inserted = context;
    }
    void SetAborted(const TInsertTableRecordLoadContext& context) {
        AFL_VERIFY(!Aborted);
        Aborted = context;
    }
};

TConclusion<std::vector<INormalizerTask::TPtr>> TInsertionsDedupNormalizer::DoInit(
    const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    using namespace NColumnShard;
    auto rowset = db.Table<NColumnShard::Schema::InsertTable>().Select();
    if (!rowset.IsReady()) {
        return TConclusionStatus::Fail("cannot read insertion info");
    }
    THashMap<TInsertWriteId, TCollectionStates> insertions;
    while (!rowset.EndOfSet()) {
        TInsertTableRecordLoadContext constructor;
        constructor.ParseFromDatabase(rowset);
        if (constructor.GetRecType() == NColumnShard::Schema::EInsertTableIds::Committed) {
            AFL_VERIFY(constructor.GetPlanStep());
        } else {
            if (constructor.GetRecType() == NColumnShard::Schema::EInsertTableIds::Aborted) {
                insertions[constructor.GetInsertWriteId()].SetAborted(constructor);
            } else if (constructor.GetRecType() == NColumnShard::Schema::EInsertTableIds::Inserted) {
                insertions[constructor.GetInsertWriteId()].SetInserted(constructor);
            } else {
                AFL_VERIFY(false);
            }
        }
        if (!rowset.Next()) {
            return TConclusionStatus::Fail("cannot read insertion info");
        }
    }

    std::vector<INormalizerTask::TPtr> result;
    std::vector<TInsertTableRecordLoadContext> toRemove;
    std::vector<TInsertTableRecordLoadContext> toCleanDedup;
    for (auto&& [id, i] : insertions) {
        if (i.GetInserted() && i.GetAborted()) {
            toRemove.emplace_back(*i.GetInserted());
            if (i.GetAborted()->GetDedupId()) {
                toCleanDedup.emplace_back(*i.GetAborted());
            }
        } else if (i.GetAborted()) {
            if (i.GetAborted()->GetDedupId()) {
                toCleanDedup.emplace_back(*i.GetAborted());
            }
        } else if (i.GetInserted()) {
            if (i.GetInserted()->GetDedupId()) {
                toCleanDedup.emplace_back(*i.GetInserted());
            }
        } else {
            AFL_VERIFY(false);
        }
        if (toCleanDedup.size() == 1000) {
            result.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TNormalizerCleanDedupChanges>(toCleanDedup)));
            toCleanDedup.clear();
        }
        if (toRemove.size() == 1000) {
            result.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TNormalizerRemoveChanges>(toRemove)));
            toRemove.clear();
        }
    }
    if (toCleanDedup.size()) {
        result.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TNormalizerCleanDedupChanges>(toCleanDedup)));
        toCleanDedup.clear();
    }
    if (toRemove.size()) {
        result.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TNormalizerRemoveChanges>(toRemove)));
        toRemove.clear();
    }

    return result;
}

}   // namespace NKikimr::NOlap
