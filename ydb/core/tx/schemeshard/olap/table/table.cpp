#include "table.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/object.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/standalone/evolution.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/evolution.h>

namespace NKikimr::NSchemeShard {

TColumnTableInfo::TColumnTableInfo(
    ui64 alterVersion,
    NKikimrSchemeOp::TColumnTableDescription&& description,
    TMaybe<NKikimrSchemeOp::TColumnStoreSharding>&& standaloneSharding,
    TMaybe<NKikimrSchemeOp::TAlterColumnTable>&& alterBody)
    : AlterVersion(alterVersion)
    , Description(std::move(description))
    , StandaloneSharding(std::move(standaloneSharding))
    , AlterBody(std::move(alterBody))
{
}

TColumnTableInfo::TPtr TColumnTableInfo::BuildTableWithAlter(const TColumnTableInfo& initialTable, const NKikimrSchemeOp::TAlterColumnTable& alterBody) {
    TColumnTableInfo::TPtr alterData = std::make_shared<TColumnTableInfo>(initialTable);
    alterData->AlterBody.ConstructInPlace(alterBody);
    ++alterData->AlterVersion;
    return alterData;
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

TConclusion<std::shared_ptr<NOlap::NAlter::ISSEntityEvolution>> TColumnTableInfo::BuildEvolution(const TPathId& pathId, const NOlap::NAlter::TEvolutionInitializationContext& iContext) const {
    std::shared_ptr<NOlap::NAlter::ISSEntityEvolution> result;
    if (IsStandalone()) {
        result = std::make_shared<NOlap::NAlter::TStandaloneSchemaEvolution>(pathId);
    } else {
        result = std::make_shared<NOlap::NAlter::TInStoreSchemaEvolution>(pathId);
    }
    auto initConclusion = result->Initialize(iContext);
    if (initConclusion.IsFail()) {
        return initConclusion;
    }
    return result;
}

}