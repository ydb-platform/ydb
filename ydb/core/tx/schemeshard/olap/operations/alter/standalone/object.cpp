#include "object.h"
#include "update.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

NKikimr::TConclusion<std::shared_ptr<NOperations::ISSEntityUpdate>> TStandaloneTable::DoCreateUpdateImpl(const NOperations::TUpdateInitializationContext& context) const {
    std::shared_ptr<NOperations::ISSEntityUpdate> result;
    if (context.GetModification()->HasAlterTable() || context.GetModification()->HasAlterColumnTable()) {
        result = std::make_shared<TStandaloneSchemaUpdate>();
    }
    if (!result) {
        return TConclusionStatus::Fail("alter data not found");
    }
    auto conclusion = result->Initialize(context);
    if (conclusion.IsFail()) {
        return conclusion;
    }
    return result;
}

NKikimr::TConclusionStatus TStandaloneTable::DoInitializeImpl(const NOperations::TEntityInitializationContext& /*context*/) {
    return InitializeFromTableInfo();
}

NKikimr::TConclusionStatus TStandaloneTable::InitializeFromTableInfo() {
    if (!GetTableInfoPtrVerified()->Description.HasSchema()) {
        return TConclusionStatus::Fail("path id object has no schema owned for " + GetPathId().ToString());
    }
    TOlapSchema schema;
    schema.ParseFromLocalDB(GetTableInfoPtrVerified()->Description.GetSchema());
    TableSchema = std::move(schema);

    if (GetTableInfoPtrVerified()->Description.HasTtlSettings()) {
        TOlapTTL ttl;
        ttl.DeserializeFromProto(GetTableInfoPtrVerified()->Description.GetTtlSettings()).Validate();
        TableTTL = std::move(ttl);
    }

    return TConclusionStatus::Success();
}

}