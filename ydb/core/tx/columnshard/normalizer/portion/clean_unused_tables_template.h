#pragma once

#include <tuple>
#include <vector>

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NCleanUnusedTables {
using namespace NColumnShard;

template <typename TTable>
class TCleanUnusedTablesNormalizerTemplate
    : public TNormalizationController::INormalizerComponent {
protected:
  using TBase = TNormalizationController::INormalizerComponent;
  using TKey = std::tuple<ui32, ui64, ui32, ui64, ui64, ui64, ui32>;

  class TChanges;

  static TString ClassName();

public:
  static constexpr size_t BATCH = 1000;

  explicit TCleanUnusedTablesNormalizerTemplate(
      const TNormalizationController::TInitContext &ctx);

  TString GetClassName() const override;
  std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override;

  TConclusion<std::vector<INormalizerTask::TPtr>>
  DoInit(const TNormalizationController &,
         NTabletFlatExecutor::TTransactionContext &txc) override;
};

} // namespace NKikimr::NOlap::NCleanUnusedTables