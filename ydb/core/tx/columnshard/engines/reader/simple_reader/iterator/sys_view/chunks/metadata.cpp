#include "constructor.h"
#include "metadata.h"
#include "schema.h"

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks {

TAccessor::TAccessor(const TString& tablePath, const NColumnShard::TUnifiedOptionalPathId pathId)
    : TBase(tablePath, pathId) {
}

std::unique_ptr<NCommon::ISourcesConstructor> TAccessor::SelectMetadata(const TSelectMetadataContext& context,
    const NReader::TReadDescription& readDescription, const NColumnShard::IResolveWriteIdToLockId& /*resolver*/, const bool isPlain) const {
    AFL_VERIFY(!isPlain);
    return std::make_unique<TConstructor>(context.GetPathIdTranslator(), context.GetEngine(), readDescription.GetTabletId(),
        GetTableFilterPathId(), readDescription.GetSnapshot(), readDescription.PKRangesFilter, readDescription.GetSorting());
}

std::shared_ptr<ISnapshotSchema> TAccessor::GetSnapshotSchemaOptional(
    const TVersionedPresetSchemas& vSchemas, const TSnapshot& /*snapshot*/) const {
    return vSchemas.GetVersionedIndex(TSchemaAdapter::GetInstance().GetPresetId()).GetLastSchema();
}

std::shared_ptr<const TVersionedIndex> TAccessor::GetVersionedIndexCopyOptional(TVersionedPresetSchemas& vSchemas) const {
    return vSchemas.GetVersionedIndexCopy(TSchemaAdapter::GetInstance().GetPresetId());
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks
