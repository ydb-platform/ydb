#include "constructor.h"
#include "metadata.h"
#include "schema.h"

#include <ydb/core/tx/columnshard/engines/reader/common/description.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer {

TAccessor::TAccessor(const TString& tablePath, const NColumnShard::TSchemeShardLocalPathId externalPathId,
    const std::optional<NColumnShard::TInternalPathId> internalPathId)
    : TBase(tablePath)
    , PathId(NColumnShard::TUnifiedPathId::BuildNoCheck(internalPathId, externalPathId)) {
    AFL_VERIFY(CheckTablePath(GetTablePath()));
}

std::unique_ptr<NReader::NCommon::ISourcesConstructor> TAccessor::SelectMetadata(const TSelectMetadataContext& context,
    const NReader::TReadDescription& readDescription, const bool /*withUncommitted*/, const bool isPlain) const {
    AFL_VERIFY(!isPlain);
    return std::make_unique<TConstructor>(context.GetPathIdTranslator(), context.GetEngine(), readDescription.GetTabletId(),
        PathId.GetInternalPathId().GetRawValue() ? PathId.GetInternalPathId() : std::optional<NColumnShard::TInternalPathId>(),
        readDescription.PKRangesFilter, readDescription.IsReverseSort());
}

std::shared_ptr<ISnapshotSchema> TAccessor::GetSnapshotSchemaOptional(
    const TVersionedPresetSchemas& vSchemas, const TSnapshot& /*snapshot*/) const {
    return vSchemas.GetVersionedIndex(TSchemaAdapter::GetInstance().GetPresetId()).GetLastSchema();
}

std::shared_ptr<const TVersionedIndex> TAccessor::GetVersionedIndexCopyOptional(TVersionedPresetSchemas& vSchemas) const {
    return vSchemas.GetVersionedIndexCopy(TSchemaAdapter::GetInstance().GetPresetId());
}

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer
