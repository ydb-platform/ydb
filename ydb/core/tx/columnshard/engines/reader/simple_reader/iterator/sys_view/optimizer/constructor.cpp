#include "constructor.h"
#include "schema.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer {

TConstructor::TConstructor(const NOlap::IPathIdTranslator& pathIdTranslator, const IColumnEngine& engine, const ui64 tabletId,
    const std::optional<NOlap::TInternalPathId> internalPathId, const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter,
    const ERequestSorting sorting)
    : TBase(sorting, tabletId, TSchemaAdapter::GetPKSchema()) {
    const TColumnEngineForLogs* engineImpl = dynamic_cast<const TColumnEngineForLogs*>(&engine);
    std::deque<TDataSourceConstructor> constructors;
    for (auto&& i : engineImpl->GetTables()) {
        if (internalPathId && *internalPathId != i.first) {
            continue;
        }
        constructors.emplace_back(pathIdTranslator.ResolveSchemeShardLocalPathIdVerified(i.first), TabletId, i.second);
        if (!pkFilter->IsUsed(constructors.back().GetStart().GetView(*TSchemaAdapter::GetPKSchema()),
                constructors.back().GetFinish().GetView(*TSchemaAdapter::GetPKSchema()))) {
            constructors.pop_back();
        }
    }
    Constructors.Initialize(std::move(constructors));
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer
