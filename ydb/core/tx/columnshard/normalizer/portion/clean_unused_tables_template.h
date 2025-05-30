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

template<>
struct TKeyTraits<Schema::TtlSettingsPresetInfo> {
  using TTable = Schema::TtlSettingsPresetInfo;
  using TKey = std::tuple<ui32>;

  template<typename Row>
  static TKey Extract(Row& rs) {
    return {
      rs.template GetValue<typename TTable::Id>()
    };
  }
};

template<>
struct TKeyTraits<Schema::TtlSettingsPresetVersionInfo> {
  using TTable = Schema::TtlSettingsPresetVersionInfo;
  using TKey = std::tuple<ui32, ui64, ui64>;

  template<typename Row>
  static TKey Extract(Row& rs) {
    return {
      rs.template GetValue<typename TTable::Id>(),
      rs.template GetValue<typename TTable::SinceStep>(),
      rs.template GetValue<typename TTable::SinceTxId>()
    };
  }
};

template<>
struct TKeyTraits<Schema::OneToOneEvictedBlobs> {
  using TTable = Schema::OneToOneEvictedBlobs;
  using TKey = std::tuple<TString>;

  template<typename Row>
  static TKey Extract(Row& rs) {
    return {
      rs.template GetValue<typename TTable::BlobId>()
    };
  }
};

template<>
struct TKeyTraits<Schema::TxStates> {
  using TTable = Schema::TxStates;
  using TKey = std::tuple<ui64>;

  template<typename Row>
  static TKey Extract(Row& rs) {
    return {
      rs.template GetValue<typename TTable::TxId>()
    };
  }
};

template<>
struct TKeyTraits<Schema::TxEvents> {
  using TTable = Schema::TxEvents;
  using TKey = std::tuple<ui64, ui64, ui64>;

  template<typename Row>
  static TKey Extract(Row& rs) {
    return {
      rs.template GetValue<typename TTable::TxId>(),
      rs.template GetValue<typename TTable::GenerationId>(),
      rs.template GetValue<typename TTable::GenerationInternalId>()
    };
  }
};

template<>
struct TKeyTraits<Schema::LockRanges> {
  using TTable = Schema::LockRanges;
  using TKey = std::tuple<ui64, ui64>;

  template<typename Row>
  static TKey Extract(Row& rs) {
    return {
      rs.template GetValue<typename TTable::LockId>(),
      rs.template GetValue<typename TTable::RangeId>()
    };
  }
};

template<>
struct TKeyTraits<Schema::LockConflicts> {
  using TTable = Schema::LockConflicts;
  using TKey = std::tuple<ui64, ui64>;

  template<typename Row>
  static TKey Extract(Row& rs) {
    return {
      rs.template GetValue<typename TTable::LockId>(),
      rs.template GetValue<typename TTable::ConflictId>()
    };
  }
};

template<>
struct TKeyTraits<Schema::LockVolatileDependencies> {
  using TTable = Schema::LockVolatileDependencies;
  using TKey = std::tuple<ui64, ui64>;

  template<typename Row>
  static TKey Extract(Row& rs) {
    return {
      rs.template GetValue<typename TTable::LockId>(),
      rs.template GetValue<typename TTable::TxId>()
    };
  }
};

template<>
struct TKeyTraits<Schema::BackgroundSessions> {
  using TTable = Schema::BackgroundSessions;
  using TKey = std::tuple<TString, TString>;

  template<typename Row>
  static TKey Extract(Row& rs) {
    return {
      rs.template GetValue<typename TTable::ClassName>(),
      rs.template GetValue<typename TTable::Identifier>()
    };
  }
};

template <typename TTable, typename TKey>
inline void Delete(TNiceDb& db, const TKey& key) {
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
  static bool TableExists(TNiceDb& db) {
    return db.HaveTable<TTable>();
  }

  template<typename TTable, typename TKeyVec>
  INormalizerTask::TPtr MakeTask(TKeyVec&& vec) {
    return std::make_shared<TTrivialNormalizerTask>(
      std::make_shared<TChanges<TTable>>(std::forward<TKeyVec>(vec)));
  }

  template<typename TTable>
  void ProcessTable(TNiceDb& db, std::vector<INormalizerTask::TPtr>& tasks) {
    using TT = TKeyTraits<TTable>;
    using TKey = typename TT::TKey;

    if (!TableExists<TTable>(db)) {
      return;
    }

    std::vector<TKey> keys;
    keys.reserve(BATCH);
    auto rs = db.Table<TTable>().Select();
    if (!rs.IsReady()) {
      return;
    }

    while (!rs.EndOfSet()) {
      keys.emplace_back(TT::Extract(rs));

      if (keys.size() == BATCH) {
        tasks.emplace_back(MakeTask<TTable>(std::move(keys)));

        keys.clear();
        keys.reserve(BATCH);
      }

      if (!rs.Next()) {
        break;
      }
    }

    if (!keys.empty()) {
      tasks.emplace_back(MakeTask<TTable>(std::move(keys)));
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
        Delete<TTable>(db, k);
      }

      return true;
    }

    ui64 GetSize() const override { return keys.size(); }
  };
};

} // namespace NKikimr::NOlap::NCleanUnusedTables