#include "object.h"
#include "update.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

TConclusionStatus TInStoreTable::InitializeWithTableInfo(const TColumnTableInfo::TPtr& tInfo, const TEntityInitializationContext& context) {
    TableInfo = tInfo;
    AFL_VERIFY(!!TableInfo);
    AFL_VERIFY(!TableInfo->IsStandalone());
    const auto storePathId = TableInfo->GetOlapStorePathIdVerified();
    TPath storePath = TPath::Init(storePathId, context.GetSSOperationContext()->SS);
    {
        TPath::TChecker checks = storePath.Check();
        checks
            .NotEmpty()
            .IsResolved()
            .IsOlapStore();

        if (!checks) {
            return TConclusionStatus::Fail(checks.GetError());
        }
    }

    Y_ABORT_UNLESS(context.GetSSOperationContext()->SS->OlapStores.contains(storePathId));
    StoreInfo = context.GetSSOperationContext()->SS->OlapStores.at(storePathId);
    TOlapSchema schema;
    schema.ParseFromLocalDB(GetTableSchemaProto().DetachResult());
    TableSchema = std::move(schema);

    if (TableInfo->Description.HasTtlSettings()) {
        TOlapTTL ttl;
        ttl.DeserializeFromProto(TableInfo->Description.GetTtlSettings()).Validate();
        TableTTL = std::move(ttl);
    }

    return TConclusionStatus::Success();
}

TConclusionStatus TInStoreTable::DoInitialize(const TEntityInitializationContext& context) {
    TableInfo = context.GetSSOperationContext()->SS->ColumnTables.GetVerifiedPtr(GetPathId());
    return InitializeWithTableInfo(TableInfo, context);
}

NKikimr::TConclusion<std::shared_ptr<NKikimr::NSchemeShard::NOlap::NAlter::ISSEntityUpdate>> TInStoreTable::DoCreateUpdate(const TUpdateInitializationContext& context, const std::shared_ptr<ISSEntity>& selfPtr) const {
    std::shared_ptr<ISSEntityUpdate> result = std::make_shared<TInStoreSchemaUpdate>(selfPtr);
    auto initConclusion = result->Initialize(context);
    if (initConclusion.IsFail()) {
        return initConclusion;
    }
    return result;
}

}