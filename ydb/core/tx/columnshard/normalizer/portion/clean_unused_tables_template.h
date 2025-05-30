#pragma once

#include <tuple>
#include <vector>

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NCleanUnusedTables {
using namespace NColumnShard;
using NIceDb::TNiceDb;

template <typename TTable, typename TKey>
inline void Delete(TNiceDb& db, const TKey& key) {
    std::cout << "la-la-la";
    std::apply([&](auto... parts) { db.template Table<TTable>().Key(parts...).Delete(); }, key);
}

template <typename... TTables>
class TCleanUnusedTablesNormalizerTemplate
    : public TNormalizationController::INormalizerComponent {
   protected:
    using TBase = TNormalizationController::INormalizerComponent;

    static TString ClassName() { return "CleanUnusedTables"; }

public:
    static constexpr size_t BATCH = 1000;

    explicit TCleanUnusedTablesNormalizerTemplate(const TNormalizationController::TInitContext& ctx)
        : TBase(ctx) {}

    TString GetClassName() const override { return ClassName(); }

    std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override { return std::nullopt; }

    TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(const TNormalizationController&,
                                                           NTabletFlatExecutor::TTransactionContext& txc) override {
        TNiceDb db(txc.DB);
        std::vector<INormalizerTask::TPtr> tasks;

        auto append = [&](auto&& res) -> decltype(auto) {
            if (!res) {
                return false;
            }

            const auto& vec = res.GetResult();
            tasks.insert(tasks.end(),
                        std::make_move_iterator(vec.begin()),
                        std::make_move_iterator(vec.end()));
            return true;
        };

        if (!(append(ProcessTable<TTables>(db))&& ...)) {
            return TConclusionStatus::Fail("Table not ready");
        }

        return tasks;
    }

private:
    template <typename TTable>
    static bool TableExists(TNiceDb& db) {
        return db.HaveTable<TTable>();
    }

    template <typename TTable, typename TKeyVec>
    INormalizerTask::TPtr MakeTask(TKeyVec&& vec) {
        return std::make_shared<TTrivialNormalizerTask>(
            std::make_shared<TChanges<TTable>>(std::forward<TKeyVec>(vec)));
    }

    template <typename TTable>
    TConclusion<std::vector<INormalizerTask::TPtr>> ProcessTable(TNiceDb& db) {
        using TKey = typename TTable::TKey::TupleType;

        std::vector<INormalizerTask::TPtr> tasks;

        if (!TableExists<TTable>(db)) {
            return tasks;
        }

        std::vector<TKey> keys;
        keys.reserve(BATCH);
        auto rs = db.Table<TTable>().Select();
        if (!rs.IsReady()) {
            return TConclusionStatus::Fail("Table not ready");
        }

        while (!rs.EndOfSet()) {
            keys.emplace_back(rs.GetKey());

            if (keys.size() == BATCH) {
                tasks.emplace_back(MakeTask<TTable>(std::move(keys)));

                keys.clear();
                keys.reserve(BATCH);
            }

            if (!rs.Next()) {
                return TConclusionStatus::Fail("IndexColumns iterate");
            }
        }

        if (!keys.empty()) {
            tasks.emplace_back(MakeTask<TTable>(std::move(keys)));
        }

        return tasks;
    }

    template <typename TTable>
    class TChanges final : public INormalizerChanges {
        using TKey = typename TTable::TKey::TupleType;
        std::vector<TKey> Keys;

       public:
        explicit TChanges(std::vector<TKey>&& k) : Keys(std::move(k)) {}

        bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc,
                            const TNormalizationController&) const override {
            TNiceDb db(txc.DB);
            for (const auto& k : Keys) {
                Delete<TTable>(db, k);
            }

            return true;
        }

        ui64 GetSize() const override { return Keys.size(); }
    };
};

}  // namespace NKikimr::NOlap::NCleanUnusedTables
