#include "constructor.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions {

TConstructor::TConstructor(const NOlap::IPathIdTranslator& pathIdTranslator, const IColumnEngine& engine, const ui64 tabletId,
    const std::optional<NOlap::TInternalPathId> internalPathId, const TSnapshot reqSnapshot,
    const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter, const ERequestSorting sorting)
    : TabletId(tabletId)
    , Sorting(sorting) {
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
                AddConstructors(pathIdTranslator, i.first, std::move(portions), pkFilter);
                portions.clear();
            }
        }
        if (portions.size()) {
            AddConstructors(pathIdTranslator, i.first, std::move(portions), pkFilter);
            portions.clear();
        }
    }
    if (Sorting != ERequestSorting::NONE) {
        std::make_heap(Constructors.begin(), Constructors.end(), TPortionDataConstructor::TComparator(Sorting == ERequestSorting::DESC));
    }
}

std::shared_ptr<NCommon::IDataSource> TConstructor::DoTryExtractNext(
    const std::shared_ptr<NCommon::TSpecialReadContext>& context, const ui32 /*inFlightCurrentLimit*/) {
    AFL_VERIFY(Constructors.size());

    Constructors.front().SetIndex(CurrentSourceIdx);
    ++CurrentSourceIdx;
    if (Sorting == ERequestSorting::NONE) {
        std::shared_ptr<NReader::NCommon::IDataSource> result = Constructors.front().Construct(context);
        Constructors.pop_front();
        return result;
    } else {
        std::pop_heap(Constructors.begin(), Constructors.end(), TPortionDataConstructor::TComparator(Sorting == ERequestSorting::DESC));
        std::shared_ptr<NReader::NCommon::IDataSource> result = Constructors.back().Construct(context);
        Constructors.pop_back();
        return result;
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions
