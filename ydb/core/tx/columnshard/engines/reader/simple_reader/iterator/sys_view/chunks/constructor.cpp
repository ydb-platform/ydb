#include "constructor.h"
#include "source.h"

#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/plain_read_data.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks {

std::shared_ptr<IDataSource> TPortionDataConstructor::Construct(const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) {
    return std::make_shared<TSourceData>(GetSourceIdx(), PathId, GetTabletId(), std::move(Portion), ExtractStart().ExtractValue(),
        ExtractFinish().ExtractValue(), context, std::move(Schema));
}

std::shared_ptr<IDataSource> TPortionDataConstructor::Construct(
    const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context, std::shared_ptr<TPortionDataAccessor>&& accessor) {
    auto result = Construct(context);
    result->SetPortionAccessor(std::move(accessor));
    return result;
}

std::shared_ptr<NCommon::IDataSource> TConstructor::DoExtractNextImpl(const std::shared_ptr<NReader::NCommon::TSpecialReadContext>& context) {
    auto constructor = PopObjectWithAccessor();
    std::shared_ptr<NReader::NCommon::IDataSource> result = constructor.MutableObject().Construct(context, constructor.DetachAccessor());
    return result;
}

TConstructor::TConstructor(const IPathIdTranslator& translator, const NColumnShard::TUnifiedOptionalPathId& unifiedPathId, const IColumnEngine& engine, const ui64 tabletId,
    const TSnapshot reqSnapshot, const std::shared_ptr<NOlap::TPKRangesFilter>& pkFilter,
    const ERequestSorting sorting)
    : TBase(sorting) {
    const TColumnEngineForLogs* engineImpl = dynamic_cast<const TColumnEngineForLogs*>(&engine);
    const TVersionedIndex& originalSchemaInfo = engineImpl->GetVersionedIndex();
    std::deque<TPortionDataConstructor> constructors;
    for (auto&& [internalPathId, granuleMeta] : engineImpl->GetTables()) {
        if (unifiedPathId.HasInternalPathId() && unifiedPathId.GetInternalPathIdVerified() != internalPathId) {
            continue;
        }
        AFL_VERIFY(unifiedPathId.HasSchemeShardLocalPathId());
        for (auto&& [_, portionInfo] : granuleMeta->GetPortions()) {
            if (reqSnapshot < portionInfo->RecordSnapshotMin()) {
                continue;
            }
            if (portionInfo->IsRemovedFor(reqSnapshot)) {
                continue;
            }
            if (unifiedPathId.HasInternalPathId()) {
                constructors.emplace_back(NColumnShard::TUnifiedPathId::BuildValid(unifiedPathId.GetInternalPathIdVerified(), unifiedPathId.GetSchemeShardLocalPathIdVerified()), tabletId, portionInfo, portionInfo->GetSchema(originalSchemaInfo));
                if (!pkFilter->IsUsed(
                        constructors.back().GetStart().GetValue().BuildSortablePosition(), constructors.back().GetFinish().GetValue().BuildSortablePosition())) {
                    constructors.pop_back();
                }
                continue;
            }
            for (const auto& schemeShardLocalPathId: translator.ResolveSchemeShardLocalPathIdsVerified(granuleMeta->GetPathId())) {
                constructors.emplace_back(NColumnShard::TUnifiedPathId::BuildValid(granuleMeta->GetPathId(), schemeShardLocalPathId), tabletId, portionInfo, portionInfo->GetSchema(originalSchemaInfo));
                if (!pkFilter->IsUsed(
                        constructors.back().GetStart().GetValue().BuildSortablePosition(), constructors.back().GetFinish().GetValue().BuildSortablePosition())) {
                    constructors.pop_back();
                }
            }
        }
    }
    InitializeConstructors(std::move(constructors));
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks
