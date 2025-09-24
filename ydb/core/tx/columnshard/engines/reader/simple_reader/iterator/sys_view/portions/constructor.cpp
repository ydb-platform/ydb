#include "constructor.h"
#include "schema.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions {

TConstructor::TConstructor(const NOlap::IPathIdTranslator& pathIdTranslator, const IColumnEngine& engine, const ui64 tabletId,
    const std::optional<NOlap::TInternalPathId> internalPathId, const TSnapshot reqSnapshot,
    const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter, const ERequestSorting sorting)
    : TBase(sorting, tabletId, TSchemaAdapter::GetPKSchema()) {
    const TColumnEngineForLogs* engineImpl = dynamic_cast<const TColumnEngineForLogs*>(&engine);

    std::deque<TDataSourceConstructor> constructors;
    for (auto&& i : engineImpl->GetTables()) {
        if (internalPathId && *internalPathId != i.first) {
            continue;
        }
        std::vector<TPortionInfo::TConstPtr> portionsAll;
        for (auto&& [_, p] : i.second->GetPortions()) {
            AFL_VERIFY(i.first == p->GetPathId());
            if (reqSnapshot < p->RecordSnapshotMin()) {
                continue;
            }
            if (p->IsRemovedFor(reqSnapshot)) {
                continue;
            }
            portionsAll.emplace_back(p);
        }
        std::sort(portionsAll.begin(), portionsAll.end(), TPortionInfo::TPortionAddressComparator());

        std::vector<TPortionInfo::TConstPtr> portions;
        for (auto&& p : portionsAll) {
            portions.emplace_back(p);
            if (portions.size() == 10) {
                constructors.emplace_back(pathIdTranslator.GetUnifiedByInternalVerified(i.first), TabletId, std::move(portions));
                if (!pkFilter->IsUsed(constructors.back().GetStart().GetView(*TSchemaAdapter::GetPKSchema()),
                        constructors.back().GetFinish().GetView(*TSchemaAdapter::GetPKSchema()))) {
                    constructors.pop_back();
                }
                portions.clear();
            }
        }
        if (portions.size()) {
            constructors.emplace_back(pathIdTranslator.GetUnifiedByInternalVerified(i.first), TabletId, std::move(portions));
            if (!pkFilter->IsUsed(constructors.back().GetStart().GetView(*TSchemaAdapter::GetPKSchema()),
                    constructors.back().GetFinish().GetView(*TSchemaAdapter::GetPKSchema()))) {
                constructors.pop_back();
            }
            portions.clear();
        }
    }
    Constructors.Initialize(std::move(constructors));
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions
