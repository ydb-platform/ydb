#include "clean_unused_tables_template.h"
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NCleanUnusedTables {
using namespace NKikimr::NColumnShard;
using NIceDb::TNiceDb;

template <typename TTable>
TString TCleanUnusedTablesNormalizerTemplate<TTable>::ClassName() {
  return "CleanUnusedTables";
}

template <typename TTable>
TCleanUnusedTablesNormalizerTemplate<TTable>::
    TCleanUnusedTablesNormalizerTemplate(
        const TNormalizationController::TInitContext &ctx)
    : TBase(ctx) {}

template <typename TTable>
TString TCleanUnusedTablesNormalizerTemplate<TTable>::GetClassName() const {
  return ClassName();
}

template <typename TTable>
std::optional<ENormalizerSequentialId>
TCleanUnusedTablesNormalizerTemplate<TTable>::DoGetEnumSequentialId() const {
  return std::nullopt;
}

template <typename TTable>
TConclusion<std::vector<INormalizerTask::TPtr>>
TCleanUnusedTablesNormalizerTemplate<TTable>::DoInit(
    const TNormalizationController & /*ctrl*/,
    NTabletFlatExecutor::TTransactionContext &txc) {
  std::vector<TKey> keys;
  TNiceDb db(txc.DB);

  auto rs = db.Table<TTable>().Select();
  if (!rs.IsReady()) {
    return TConclusionStatus::Fail(TStringBuilder() << " not ready");
  }

  while (!rs.EndOfSet()) {
    keys.emplace_back(rs.template GetValue<typename TTable::Index>(),
                      rs.template GetValue<typename TTable::Granule>(),
                      rs.template GetValue<typename TTable::ColumnIdx>(),
                      rs.template GetValue<typename TTable::PlanStep>(),
                      rs.template GetValue<typename TTable::TxId>(),
                      rs.template GetValue<typename TTable::Portion>(),
                      rs.template GetValue<typename TTable::Chunk>());
    if (!rs.Next()) {
      return TConclusionStatus::Fail(TStringBuilder() << " iteration failed");
    }
  }

  std::vector<INormalizerTask::TPtr> tasks;
  for (size_t i = 0; i < keys.size(); i += BATCH) {
    size_t to = std::min(i + BATCH, keys.size());
    tasks.emplace_back(
        std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges>(
            std::vector<TKey>(keys.begin() + i, keys.begin() + to))));
  }
  return tasks;
}

template <typename TTable>
class TCleanUnusedTablesNormalizerTemplate<TTable>::TChanges final
    : public INormalizerChanges {
  std::vector<TKey> Keys;

public:
  explicit TChanges(std::vector<TKey> &&keys) : Keys(std::move(keys)) {}

  bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext &txc,
                      const TNormalizationController &) const override {
    TNiceDb db(txc.DB);
    for (auto &k : Keys) {
      std::apply(
          [&](auto... parts) {
            db.template Table<TTable>().Key(parts...).Delete();
          },
          k);
    }
    return true;
  }

  ui64 GetSize() const override { return Keys.size(); }
};

template class TCleanUnusedTablesNormalizerTemplate<Schema::IndexColumns>;

} // namespace NKikimr::NOlap::NCleanUnusedTables