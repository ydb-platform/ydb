#include "constructor.h"
#include "metadata.h"
#include "schema.h"

#include <ydb/core/tx/columnshard/engines/reader/common/description.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NGranules {

TAccessor::TAccessor(const TString& tablePath, const NColumnShard::TUnifiedOptionalPathId pathId)
    : TBase(tablePath, pathId) {
}

std::unique_ptr<NReader::NCommon::ISourcesConstructor> TAccessor::SelectMetadata(const TSelectMetadataContext& context,
    const NReader::TReadDescription& readDescription, const bool isPlain) const {
    AFL_VERIFY(!isPlain);
    auto pathId = GetPathId();
    AFL_VERIFY(!!pathId);
    return std::make_unique<TConstructor>(context.GetPathIdTranslator(), *pathId, context.GetEngine(), readDescription.GetTabletId(),
        readDescription.PKRangesFilter, readDescription.GetSorting());
}

std::shared_ptr<ISnapshotSchema> TAccessor::GetSnapshotSchemaOptional(
    const TVersionedPresetSchemas& vSchemas, const TSnapshot& /*snapshot*/) const {
    return vSchemas.GetVersionedIndex(TSchemaAdapter::GetInstance().GetPresetId()).GetLastSchema();
}

std::shared_ptr<const TVersionedIndex> TAccessor::GetVersionedIndexCopyOptional(TVersionedPresetSchemas& vSchemas) const {
    return vSchemas.GetVersionedIndexCopy(TSchemaAdapter::GetInstance().GetPresetId());
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NGranules
