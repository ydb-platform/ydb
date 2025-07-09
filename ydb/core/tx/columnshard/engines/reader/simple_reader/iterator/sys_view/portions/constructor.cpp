#include "constructor.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions {

TConstructor::TConstructor(const NOlap::IPathIdTranslator& pathIdTranslator, const IColumnEngine& engine, const ui64 tabletId,
    const std::optional<NOlap::TInternalPathId> internalPathId, const TSnapshot reqSnapshot,
    const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter, const bool isReverseSort)
    : TabletId(tabletId) {
    const TColumnEngineForLogs* engineImpl = dynamic_cast<const TColumnEngineForLogs*>(&engine);
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
            portionsAll.emplace_back(p);
        }
        std::sort(portionsAll.begin(), portionsAll.end(), TPortionInfo::TPortionAddressComparator());

        std::vector<TPortionInfo::TConstPtr> portions;
        for (auto&& p : portionsAll) {
            portions.emplace_back(p);
            if (portions.size() == 10) {
                AddConstructors(pathIdTranslator, i.first, std::move(portions), pkFilter);
                portions.clear();
            }
        }
        if (portions.size()) {
            AddConstructors(pathIdTranslator, i.first, std::move(portions), pkFilter);
            portions.clear();
        }
    }
    std::sort(Constructors.begin(), Constructors.end(), TPortionDataConstructor::TComparator(isReverseSort));
    for (ui32 idx = 0; idx < Constructors.size(); ++idx) {
        Constructors[idx].SetIndex(idx);
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions
