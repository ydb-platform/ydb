#pragma once
#include <ydb/core/formats/arrow/rows/view.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

#include <util/generic/refcount.h>
#include <util/generic/ylimits.h>

namespace NKikimr::NOlap {
class ITableMetadataAccessor;
}

namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract {

class ISchemaAdapter {
protected:
    static inline TAtomicCounter Counter;

    template <class TSysViewSchema>
    TIndexInfo GetIndexInfo(
        const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache) const {
        static NKikimrSchemeOp::TColumnTableSchema proto = []() {
            NKikimrSchemeOp::TColumnTableSchema proto;
            ui32 currentId = 0;
            const auto pred = [&](const TString& name, const NScheme::TTypeId typeId, const std::optional<ui32> entityId = std::nullopt) {
                auto* col = proto.AddColumns();
                col->SetId(entityId.value_or(++currentId));
                col->SetName(name);
                col->SetTypeId(typeId);
            };
            NTable::TScheme::TTableSchema schema;
            NIceDb::NHelpers::TStaticSchemaFiller<TSysViewSchema>::Fill(schema);
            for (ui32 i = 1; i <= schema.Columns.size(); ++i) {
                auto it = schema.Columns.find(i);
                AFL_VERIFY(it != schema.Columns.end());
                pred(it->second.Name, it->second.PType.GetTypeId());
            }

            for (auto&& i : schema.KeyColumns) {
                auto it = schema.Columns.find(i);
                AFL_VERIFY(it != schema.Columns.end());
                proto.AddKeyColumnNames(it->second.Name);
            }
            return proto;
        }();

        auto indexInfo = TIndexInfo::BuildFromProto(GetPresetId(), proto, storagesManager, schemaObjectsCache);
        AFL_VERIFY(indexInfo);
        return std::move(*indexInfo);
    }

public:
    using TFactory = NObjectFactory::TObjectFactory<ISchemaAdapter, TString>;
    virtual ~ISchemaAdapter() = default;
    virtual ui64 GetPresetId() const = 0;
    virtual TIndexInfo GetIndexInfo(
        const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache) const = 0;
    virtual std::shared_ptr<ITableMetadataAccessor> BuildMetadataAccessor(
        const TString& tableName, const NColumnShard::TUnifiedOptionalPathId pathId) const = 0;
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract
