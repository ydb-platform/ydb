#include "object.h"
#include "update.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

NKikimr::TConclusion<std::shared_ptr<NKikimr::NSchemeShard::NOlap::NAlter::ISSEntityUpdate>> TStandaloneTable::DoCreateUpdate(const TUpdateInitializationContext& context, const std::shared_ptr<ISSEntity>& selfPtr) const {
    auto result = std::make_shared<TStandaloneSchemaUpdate>(selfPtr);
    auto conclusion = result->Initialize(context);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    return result;
}

NKikimr::TConclusionStatus TStandaloneTable::DoInitialize(const TEntityInitializationContext& context) {
    return InitializeFromTableInfo(context.GetSSOperationContext()->SS->ColumnTables.GetVerifiedPtr(GetPathId()));
}

NKikimr::TConclusionStatus TStandaloneTable::InitializeFromTableInfo(const TColumnTableInfo::TPtr& tableInfo) {
    AFL_VERIFY(!!tableInfo);
    TableInfo = tableInfo;
    if (!TableInfo->Description.HasSchema()) {
        return TConclusionStatus::Fail("path id object has no schema owned for " + GetPathId().ToString());
    }
    TOlapSchema schema;
    schema.ParseFromLocalDB(TableInfo->Description.GetSchema());
    TableSchema = std::move(schema);

    if (TableInfo->Description.HasTtlSettings()) {
        TOlapTTL ttl;
        ttl.DeserializeFromProto(TableInfo->Description.GetTtlSettings()).Validate();
        TableTTL = std::move(ttl);
    }

    return TConclusionStatus::Success();
}

}