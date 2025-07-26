#include "constructor.h"
#include "source.h"

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks {

std::shared_ptr<IDataSource> TPortionDataConstructor::Construct(const std::shared_ptr<NReader::NSimple::TSpecialReadContext>& context) {
    AFL_VERIFY(SourceId);
    return std::make_shared<TSourceData>(
        SourceId, SourceIdx, PathId, TabletId, Portion, std::move(Start), std::move(Finish), context, std::move(Schema));
}

std::shared_ptr<NKikimr::NOlap::NReader::NCommon::IDataSource> TConstructor::DoExtractNext(
    const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) {
    AFL_VERIFY(Constructors.size());
    Constructors.front().SetIndex(CurrentSourceIdx);
    ++CurrentSourceIdx;
    if (Sorting == ERequestSorting::NONE) {
        std::shared_ptr<NReader::NCommon::IDataSource> result =
            Constructors.front().Construct(std::static_pointer_cast<NReader::NSimple::TSpecialReadContext>(context));
        Constructors.pop_front();
        return result;
    } else {
        std::pop_heap(Constructors.begin(), Constructors.end(), TPortionDataConstructor::TComparator(Sorting == ERequestSorting::DESC));
        std::shared_ptr<NReader::NCommon::IDataSource> result =
            Constructors.back().Construct(std::static_pointer_cast<NReader::NSimple::TSpecialReadContext>(context));
        Constructors.pop_back();
        return result;
    }
}

TConstructor::TConstructor(const NOlap::IPathIdTranslator& pathIdTranslator, const IColumnEngine& engine, const ui64 tabletId,
    const std::optional<NOlap::TInternalPathId> internalPathId, const TSnapshot reqSnapshot,
    const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter, const ERequestSorting sorting)
    : Sorting(sorting) {
    const TColumnEngineForLogs* engineImpl = dynamic_cast<const TColumnEngineForLogs*>(&engine);
    const TVersionedIndex& originalSchemaInfo = engineImpl->GetVersionedIndex();
    for (auto&& i : engineImpl->GetTables()) {
        if (internalPathId && *internalPathId != i.first) {
            continue;
        }
        for (auto&& [_, p] : i.second->GetPortions()) {
            if (reqSnapshot < p->RecordSnapshotMin()) {
                continue;
            }
            if (p->IsRemovedFor(reqSnapshot)) {
                continue;
            }
            Constructors.emplace_back(
                pathIdTranslator.GetUnifiedByInternalVerified(p->GetPathId()), tabletId, p, p->GetSchema(originalSchemaInfo));
            if (!pkFilter->IsUsed(Constructors.back().GetStart(), Constructors.back().GetFinish())) {
                Constructors.pop_back();
            }
        }
    }
    if (Sorting != ERequestSorting::NONE) {
        std::make_heap(Constructors.begin(), Constructors.end(), TPortionDataConstructor::TComparator(Sorting == ERequestSorting::DESC));
    }
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks
