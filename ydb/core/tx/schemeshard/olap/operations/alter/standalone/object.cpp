#include "object.h"
#include "schema/update.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

NKikimr::TConclusion<std::shared_ptr<ISSEntityUpdate>> TStandaloneTable::DoCreateUpdate(const TUpdateInitializationContext& context, const std::shared_ptr<ISSEntity>& selfPtr) const {
    auto result = std::make_shared<TStandaloneSchemaUpdate>(selfPtr, *context.GetModification());
    auto conclusion = result->Initialize(context);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    return result;
}

NKikimr::TConclusionStatus TStandaloneTable::DoInitialize(const TEntityInitializationContext& context) {
    auto resultBase = TBase::DoInitialize(context);
    if (resultBase.IsFail()) {
        return resultBase;
    }
    return InitializeFromTableInfo();
}

NKikimr::TConclusionStatus TStandaloneTable::InitializeFromTableInfo() {
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