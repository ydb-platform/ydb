#pragma once
#include <ydb/core/formats/arrow/rows/view.h>
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

public:
    using TFactory = NObjectFactory::TObjectFactory<ISchemaAdapter, TString>;
    virtual ~ISchemaAdapter() = default;
    virtual ui64 GetPresetId() const = 0;
    virtual TIndexInfo GetIndexInfo(
        const std::shared_ptr<IStoragesManager>& storagesManager, const std::shared_ptr<TSchemaObjectsCache>& schemaObjectsCache) const = 0;
    virtual std::shared_ptr<ITableMetadataAccessor> BuildMetadataAccessor(const TString& tableName,
        const NColumnShard::TSchemeShardLocalPathId externalPathId, const std::optional<NColumnShard::TInternalPathId> internalPathId) const = 0;
};

}   // namespace NKikimr::NOlap::NReader::NSimple::NSysView::NAbstract
