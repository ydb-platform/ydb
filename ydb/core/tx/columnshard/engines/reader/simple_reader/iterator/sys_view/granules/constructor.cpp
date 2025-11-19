#include "constructor.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NGranules {

TConstructor::TConstructor(const NOlap::IPathIdTranslator& pathIdTranslator, const IColumnEngine& engine, const ui64 tabletId,
    const std::optional<NOlap::TInternalPathId> internalPathId, const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter,
    const ERequestSorting sorting)
    : TBase(sorting, tabletId) {
    const TColumnEngineForLogs* engineImpl = dynamic_cast<const TColumnEngineForLogs*>(&engine);
    std::deque<TDataSourceConstructor> constructors;
    for (auto&& i : engineImpl->GetTables()) {
        if (internalPathId && *internalPathId != i.first) {
            continue;
        }
        constructors.emplace_back(pathIdTranslator, TabletId, i.second);
        if (!pkFilter->IsUsed(constructors.back().GetStart(), constructors.back().GetFinish())) {
            constructors.pop_back();
        }
    }
    Constructors.Initialize(std::move(constructors));
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NGranules
