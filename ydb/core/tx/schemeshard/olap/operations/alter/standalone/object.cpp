#include "object.h"
#include "update.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

NKikimr::TConclusion<std::shared_ptr<ISSEntityUpdate>> TStandaloneTable::DoBuildUpdate(const NKikimrSchemeOp::TModifyScheme& modification) const {
    std::shared_ptr<ISSEntityUpdate> result;
    if (modification.HasAlterTable() || modification.HasAlterColumnTable()) {
        result = std::make_shared<TStandaloneSchemaUpdate>();
    }

    if (!result) {
        return TConclusionStatus::Fail("alter data not found");
    }

    return result;
}

NKikimr::TConclusionStatus TStandaloneTable::DoInitializeImpl(const TEntityInitializationContext& /*context*/) {
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

    TOlapMultiColumnStatisticsDescription statistics;
    statistics.Parse(GetTableInfoPtrVerified()->Description);
    TableMultiColumnStatistics = std::move(statistics);

    return TConclusionStatus::Success();
}

}
