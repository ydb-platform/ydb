#pragma once

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

#include <algorithm>
#include <memory>
#include <tuple>
#include <vector>

namespace NKikimr::NOlap::NCleanUnusedTables {
using namespace NColumnShard;
using NIceDb::TNiceDb;

template <typename TTable, typename TKey>
inline void Delete(TNiceDb& db, const TKey& key) {
    std::apply(
        [&](auto... parts) {
            db.template Table<TTable>().Key(parts...).Delete();
        },
        key);
}

template <typename TTable>
class TCleanUnusedTablesNormalizerTemplate: public TNormalizationController::INormalizerComponent {
    using TBase = TNormalizationController::INormalizerComponent;
    static TString ClassName() {
        return "CleanUnusedTables";
    }

protected:
    virtual bool ValidateConfig() {
        return true;
    }

public:
    static constexpr size_t BATCH = 1000;

    explicit TCleanUnusedTablesNormalizerTemplate(const TNormalizationController::TInitContext& ctx)
        : TBase(ctx) {
    }

    TString GetClassName() const override {
        return ClassName();
    }
    std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return std::nullopt;
    }

    TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(
        const TNormalizationController&, NTabletFlatExecutor::TTransactionContext& txc) override {
        using TKey = typename TTable::TKey::TupleType;

        AFL_VERIFY(ValidateConfig());

        TNiceDb db(txc.DB);
        std::vector<INormalizerTask::TPtr> tasks;

        if (!db.HaveTable<TTable>()) {
            return tasks;
        }

        std::vector<TKey> batch;
        batch.reserve(BATCH);

        auto rs = db.Table<TTable>().Select();
        if (!rs.IsReady()) {
            return TConclusionStatus::Fail("Table not ready");
        }

        while (!rs.EndOfSet()) {
            batch.emplace_back(rs.GetKey());

            if (batch.size() == BATCH) {
                tasks.emplace_back(MakeTask(std::move(batch)));
                batch.clear();
                batch.reserve(BATCH);
            }

            if (!rs.Next()) {
                return TConclusionStatus::Fail("Iterate error");
            }
        }

        if (!batch.empty()) {
            tasks.emplace_back(MakeTask(std::move(batch)));
        }

        return tasks;
    }

private:
    template <typename TVec>
    static INormalizerTask::TPtr MakeTask(TVec&& v) {
        return std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges>(std::forward<TVec>(v)));
    }

    class TChanges final: public INormalizerChanges {
        using TKey = typename TTable::TKey::TupleType;
        std::vector<TKey> Keys;

    public:
        explicit TChanges(std::vector<TKey>&& v)
            : Keys(std::move(v)) {
        }

        bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
            TNiceDb db(txc.DB);
            for (const auto& k : Keys) {
                Delete<TTable>(db, k);
            }

            return true;
        }

        ui64 GetSize() const override {
            return Keys.size();
        }
    };
};

}   // namespace NKikimr::NOlap::NCleanUnusedTables
