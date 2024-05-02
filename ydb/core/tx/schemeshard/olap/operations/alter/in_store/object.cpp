#include "object.h"
#include "new_shards/update.h"
#include "schema/update.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

TConclusionStatus TInStoreTable::InitializeWithTableInfo(const TEntityInitializationContext& context) {
    AFL_VERIFY(!GetTableInfoVerified().IsStandalone());
    const auto storePathId = GetTableInfoVerified().GetOlapStorePathIdVerified();
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

    if (GetTableInfoVerified().Description.HasTtlSettings()) {
        TOlapTTL ttl;
        ttl.DeserializeFromProto(GetTableInfoVerified().Description.GetTtlSettings()).Validate();
        TableTTL = std::move(ttl);
    }

    return TConclusionStatus::Success();
}

TConclusionStatus TInStoreTable::DoInitialize(const TEntityInitializationContext& context) {
    auto resultBase = TBase::DoInitialize(context);
    if (resultBase.IsFail()) {
        return resultBase;
    }
    return InitializeWithTableInfo(context);
}

NKikimr::TConclusion<std::shared_ptr<ISSEntityUpdate>> TInStoreTable::DoCreateUpdate(const TUpdateInitializationContext& context, const std::shared_ptr<ISSEntity>& selfPtr) const {
    std::shared_ptr<ISSEntityUpdate> result;
    if (context.GetModification()->HasReshardColumnTable()) {
        result = std::make_shared<TInStoreReshardingUpdate>(selfPtr, *context.GetModification());
    } else {
        result = std::make_shared<TInStoreSchemaUpdate>(selfPtr, *context.GetModification());
    }
    auto initConclusion = result->Initialize(context);
    if (initConclusion.IsFail()) {
        return initConclusion;
    }
    return result;
}

NKikimr::TConclusionStatus TInStoreTable::DoStartUpdate(TEvolutions&& evolutions, const TStartUpdateContext& context) {
    auto baseResult = TBase::DoStartUpdate(std::move(evolutions), context);
    if (baseResult.IsFail()) {
        return baseResult;
    }
    const auto storePathId = GetTableInfoVerified().GetOlapStorePathIdVerified();
    TPath storePath = TPath::Init(storePathId, context.GetSSOperationContext()->SS);

    Y_ABORT_UNLESS(GetStoreInfo()->ColumnTables.contains((*context.GetObjectPath())->PathId));
    GetStoreInfo()->ColumnTablesUnderOperation.insert((*context.GetObjectPath())->PathId);

    // Sequentially chain operations in the same olap store
    if (context.GetSSOperationContext()->SS->Operations.contains(storePath.Base()->LastTxId)) {
        context.GetSSOperationContext()->OnComplete.Dependence(storePath.Base()->LastTxId, (*context.GetObjectPath())->LastTxId);
    }
    storePath.Base()->LastTxId = (*context.GetObjectPath())->LastTxId;
    context.GetSSOperationContext()->SS->PersistLastTxId(*context.GetDB(), storePath.Base());
    return TConclusionStatus::Success();
}

}