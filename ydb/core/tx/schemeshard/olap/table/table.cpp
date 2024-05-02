#include "table.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/object.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/standalone/object.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/object.h>

namespace NKikimr::NSchemeShard {

TColumnTableInfo::TColumnTableInfo(
    ui64 alterVersion, NKikimrSchemeOp::TColumnTableDescription&& description,
    TMaybe<NKikimrSchemeOp::TColumnStoreSharding>&& standaloneSharding)
    : AlterVersion(alterVersion)
    , Description(std::move(description))
    , StandaloneSharding(std::move(standaloneSharding))
{
}

TConclusion<std::shared_ptr<NOlap::NAlter::ISSEntity>> TColumnTableInfo::BuildEntity(const TPathId& pathId, const NOlap::NAlter::TEntityInitializationContext& iContext) const {
    std::shared_ptr<NOlap::NAlter::ISSEntity> result;
    if (IsStandalone()) {
        result = std::make_shared<NOlap::NAlter::TStandaloneTable>(pathId);
    } else {
        result = std::make_shared<NOlap::NAlter::TInStoreTable>(pathId);
    }
    auto initConclusion = result->Initialize(iContext);
    if (initConclusion.IsFail()) {
        return initConclusion;
    }
    return result;
}

}