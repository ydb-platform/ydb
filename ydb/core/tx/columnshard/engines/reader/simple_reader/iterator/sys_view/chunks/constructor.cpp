#include "constructor.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/plain_read_data.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks {

std::shared_ptr<IDataSource> TPortionDataConstructor::Construct(const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) {
    AFL_VERIFY(SourceId);
    return std::make_shared<TSourceData>(
        SourceId, SourceIdx, PathId, TabletId, std::move(Portion), std::move(Start), std::move(Finish), context, std::move(Schema));
}

std::shared_ptr<IDataSource> TPortionDataConstructor::Construct(
    const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context, std::shared_ptr<TPortionDataAccessor>&& accessor) {
    auto result = Construct(context);
    result->SetPortionAccessor(std::move(accessor));
    return result;
}

std::shared_ptr<NCommon::IDataSource> TConstructor::DoTryExtractNextImpl(const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) {
    auto constructor = PopObjectWithAccessor();
    constructor.MutableObject().SetIndex(CurrentSourceIdx);
    ++CurrentSourceIdx;
    std::shared_ptr<NReader::NCommon::IDataSource> result = constructor.MutableObject().Construct(context, constructor.DetachAccessor());
    return result;
}

TConstructor::TConstructor(const NOlap::IPathIdTranslator& pathIdTranslator, const IColumnEngine& engine, const ui64 tabletId,
    const std::optional<NOlap::TInternalPathId> internalPathId, const TSnapshot reqSnapshot,
    const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter, const ERequestSorting sorting)
    : TBase(sorting) {
    const TColumnEngineForLogs* engineImpl = dynamic_cast<const TColumnEngineForLogs*>(&engine);
    const TVersionedIndex& originalSchemaInfo = engineImpl->GetVersionedIndex();
    std::deque<TPortionDataConstructor> constructors;
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
            constructors.emplace_back(
                pathIdTranslator.GetUnifiedByInternalVerified(p->GetPathId()), tabletId, p, p->GetSchema(originalSchemaInfo));
            if (!pkFilter->IsUsed(constructors.back().GetStart(), constructors.back().GetFinish())) {
                constructors.pop_back();
            }
        }
    }
    InitializeConstructors(std::move(constructors));
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks
