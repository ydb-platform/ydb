#include "constructor.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer {

TConstructor::TConstructor(const NOlap::IPathIdTranslator& pathIdTranslator, const IColumnEngine& engine, const ui64 tabletId,
    const std::optional<NOlap::TInternalPathId> internalPathId, const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter,
    const bool isReverseSort)
    : TabletId(tabletId) {
    const TColumnEngineForLogs* engineImpl = dynamic_cast<const TColumnEngineForLogs*>(&engine);
    std::vector<std::shared_ptr<TGranuleMeta>> granulesAll;
    for (auto&& i : engineImpl->GetTables()) {
        if (internalPathId && *internalPathId != i.first) {
            continue;
        }
        AddConstructors(pathIdTranslator, i.second, pkFilter);
    }

    std::sort(Constructors.begin(), Constructors.end(), TGranuleDataConstructor::TComparator(isReverseSort));
    for (ui32 idx = 0; idx < Constructors.size(); ++idx) {
        Constructors[idx].SetIndex(idx);
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer
