#pragma once
#include <ydb/core/tx/columnshard/engines/metadata_accessor.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/metadata.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas {

class TAccessor: public NAbstract::TAccessor {
private:
    using TBase = NAbstract::TAccessor;

    virtual std::shared_ptr<const TVersionedIndex> GetVersionedIndexCopyOptional(TVersionedPresetSchemas& vSchemas) const override;

    virtual std::shared_ptr<ISnapshotSchema> GetSnapshotSchemaOptional(
        const TVersionedPresetSchemas& vSchemas, const TSnapshot& snapshot) const override;

public:
    TAccessor(const TString& tableName, const NColumnShard::TUnifiedOptionalPathId pathId);
<<<<<<< HEAD
    virtual std::unique_ptr<NReader::NCommon::ISourcesConstructor> SelectMetadata(
        const TSelectMetadataContext& context, const NReader::TReadDescription& readDescription, const bool isPlain) const override;

=======
    virtual std::unique_ptr<NReader::NCommon::ISourcesConstructor> SelectMetadata(const TSelectMetadataContext& context,
        const NReader::TReadDescription& readDescription, const NReader::EReaderClass readerClass) const override;
>>>>>>> af473aa4b23 (trivial reader has been introduced (#38377))
    virtual std::optional<TGranuleShardingInfo> GetShardingInfo(
        const std::shared_ptr<const TVersionedIndex>& /*indexVersionsPointer*/, const NOlap::TSnapshot& /*ss*/) const override {
        return std::nullopt;
    }
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NSchemas
