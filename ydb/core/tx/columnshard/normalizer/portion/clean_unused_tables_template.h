#pragma once

#include <tuple>
#include <vector>

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NCleanUnusedTables {
using namespace NColumnShard;
using NIceDb::TNiceDb;

template<typename TTable>
struct TKeyTraits;

template<>
struct TKeyTraits<Schema::IndexColumns> {
  using TTable = Schema::IndexColumns;
  using TKey = std::tuple<ui32, ui64, ui32, ui64, ui64, ui64, ui32>;

  template<typename Row>
  static TKey Extract(Row& rs) {
    return {
      rs.template GetValue<typename TTable::Index>(),
      rs.template GetValue<typename TTable::Granule>(),
      rs.template GetValue<typename TTable::ColumnIdx>(),
      rs.template GetValue<typename TTable::PlanStep>(),
      rs.template GetValue<typename TTable::TxId>(),
      rs.template GetValue<typename TTable::Portion>(),
      rs.template GetValue<typename TTable::Chunk>() 
    };
  }
};

template <typename TTable, typename TKey>
inline void Delete(TNiceDb& db, const TKey& key) {
  std::cout << "la-la-la1111\n";
  std::apply([&](auto... parts) {
    db.template Table<TTable>().Key(parts...).Delete();
  }, key);
}

template<typename... TTables>
class TCleanUnusedTablesNormalizerTemplate
    : public TNormalizationController::INormalizerComponent {
protected:
  using TBase = TNormalizationController::INormalizerComponent;

  static TString ClassName() { return "CleanUnusedTables"; }

public:
  static constexpr size_t BATCH = 1000;

  explicit TCleanUnusedTablesNormalizerTemplate(
      const TNormalizationController::TInitContext &ctx)
      : TBase(ctx) {}

  TString GetClassName() const override { return ClassName(); }
  std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
    return std::nullopt;
  }

  TConclusion<std::vector<INormalizerTask::TPtr>>
  DoInit(const TNormalizationController &,
         NTabletFlatExecutor::TTransactionContext &txc) override {
          TNiceDb db(txc.DB);
          std::vector<INormalizerTask::TPtr> tasks;
          (ProcessTable<TTables>(db, tasks), ...);
          return tasks;
         }

private:
  template<typename TTable>
  void ProcessTable(TNiceDb& db, std::vector<INormalizerTask::TPtr>& tasks) {
    using TT = TKeyTraits<TTable>;
    using TKey = typename TT::TKey;

    std::vector<TKey> keys;
    auto rs = db.Table<TTable>().Select();
    if (!rs.IsReady()) {
      return;
    }

    while (!rs.EndOfSet()) {
      keys.emplace_back(TT::Extract(rs));
      if (!rs.Next()) {
        break;
      }
    }

    for (size_t i = 0; i < keys.size(); i += BATCH) {
      auto sub = std::vector<TKey>(keys.begin() + i, keys.begin() + std::min(i + BATCH, keys.size()));
      tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges<TTable>>(std::move(sub))));
    }
  }

  template<typename TTable>
  class TChanges final : public INormalizerChanges {
    using TT = TKeyTraits<TTable>;
    using TKey = typename TT::TKey;
    std::vector<TKey> keys;
  
  public:
    explicit TChanges(std::vector<TKey>&& k) : keys(std::move(k)) {}

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
      TNiceDb db(txc.DB);
      for (const auto& k : keys) {
        std::cout << "la-la-la\n";
        Delete<TTable>(db, k);
      }

      return true;
    }

    ui64 GetSize() const override { return keys.size(); }
  };
};

} // namespace NKikimr::NOlap::NCleanUnusedTables