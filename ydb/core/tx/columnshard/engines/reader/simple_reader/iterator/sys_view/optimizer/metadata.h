#pragma once
#include <ydb/core/tx/columnshard/engines/metadata_accessor.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/metadata.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer {

class TAccessor: public NAbstract::TAccessor {
private:
    using TBase = NAbstract::TAccessor;

    virtual std::shared_ptr<const TVersionedIndex> GetVersionedIndexCopyOptional(TVersionedPresetSchemas& vSchemas) const override;

    virtual std::shared_ptr<ISnapshotSchema> GetSnapshotSchemaOptional(
        const TVersionedPresetSchemas& vSchemas, const TSnapshot& snapshot) const override;

public:
    TAccessor(const TString& tableName, const NColumnShard::TSchemeShardLocalPathId externalPathId,
        const std::optional<NColumnShard::TInternalPathId> internalPathId);
    virtual std::unique_ptr<NReader::NCommon::ISourcesConstructor> SelectMetadata(const TSelectMetadataContext& context,
        const NReader::TReadDescription& readDescription, const bool withUncommitted, const bool isPlain) const override;
    virtual std::optional<TGranuleShardingInfo> GetShardingInfo(
        const std::shared_ptr<const TVersionedIndex>& /*indexVersionsPointer*/, const NOlap::TSnapshot& /*ss*/) const override {
        return std::nullopt;
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NOptimizer
