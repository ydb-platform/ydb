#pragma once
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/storages_manager.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/engines/reader/simple_reader/iterator/sys_view/abstract/schema.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/objects_cache.h>

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions {

class TSchemaAdapter: public NAbstract::ISchemaAdapter {
private:
    using TBase = NAbstract::ISchemaAdapter;
    static const inline auto Registrator1 = TFactory::TRegistrator<TSchemaAdapter>("store_primary_index_portion_stats");
    static const inline auto Registrator2 = TFactory::TRegistrator<TSchemaAdapter>("primary_index_portion_stats");

public:
    static NTable::TScheme::TTableSchema GetStatsSchema();

    static const TSchemaAdapter& GetInstance() {
        return *Singleton<TSchemaAdapter>();
    }

    virtual ui64 GetPresetId() const override {
        static ui64 presetId = NAbstract::ISchemaAdapter::Counter.Inc();
        return Max<ui64>() - presetId;
    }
    static NArrow::TSimpleRowContent GetPKSimpleRow(const NColumnShard::TUnifiedPathId pathId, const ui64 tabletId, const ui64 portionId);
    static const std::shared_ptr<arrow::Schema>& GetPKSchema();
    virtual TIndexInfo GetIndexInfo(
        const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache) const override;
    virtual std::shared_ptr<ITableMetadataAccessor> BuildMetadataAccessor(
        const TString& tableName, const NColumnShard::TUnifiedOptionalPathId pathId) const override;
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NPortions
