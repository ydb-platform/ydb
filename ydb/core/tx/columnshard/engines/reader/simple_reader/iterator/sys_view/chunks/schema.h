#pragma once
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/schema.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks {

class TSchemaAdapter: public NAbstract::ISchemaAdapter {
private:
    using TBase = NAbstract::ISchemaAdapter;
    static const inline auto Registrator1 = TFactory::TRegistrator<TSchemaAdapter>("store_primary_index_stats");
    static const inline auto Registrator2 = TFactory::TRegistrator<TSchemaAdapter>("primary_index_stats");

public:
    static const TSchemaAdapter& GetInstance() {
        return *Singleton<TSchemaAdapter>();
    }

    virtual ui64 GetPresetId() const override {
        static ui64 presetId = NAbstract::ISchemaAdapter::Counter.Inc();
        return Max<ui64>() - presetId;
    }

    static NArrow::TSimpleRow GetPKSimpleRow(
        const NColumnShard::TUnifiedPathId pathId, const ui64 tabletId, const ui64 portionId, const ui32 entityId, const ui64 chunkIdx);

    static std::shared_ptr<arrow::Schema> GetPKSchema();

    virtual TIndexInfo GetIndexInfo(
        const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache) const override;
    virtual std::shared_ptr<ITableMetadataAccessor> BuildMetadataAccessor(const TString& tableName,
        const NColumnShard::TSchemeShardLocalPathId externalPathId,
        const std::optional<NColumnShard::TInternalPathId> internalPathId) const override;
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NChunks
