#include "table.h"
#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/object.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/standalone/object.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/object.h>
#include <ydb/core/tx/columnshard/common/protos/snapshot.pb.h>

namespace NKikimr::NSchemeShard {

TColumnTableInfo::TColumnTableInfo(
    ui64 alterVersion,
    const NKikimrSchemeOp::TColumnTableDescription& description,
    TMaybe<NKikimrSchemeOp::TColumnStoreSharding>&& standaloneSharding,
    TMaybe<NKikimrSchemeOp::TAlterColumnTable>&& alterBody)
    : AlterVersion(alterVersion)
    , Description(description)
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

}