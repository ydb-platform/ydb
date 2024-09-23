#include "object.h"
#include "config_shards/update.h"
#include "resharding/update.h"
#include "schema/update.h"
#include "transfer/update.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

TConclusionStatus TInStoreTable::InitializeWithTableInfo(const TEntityInitializationContext& context) {
    AFL_VERIFY(!GetTableInfoPtrVerified()->IsStandalone());
    const auto storePathId = GetTableInfoPtrVerified()->GetOlapStorePathIdVerified();
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

    if (GetTableInfoPtrVerified()->Description.HasTtlSettings()) {
        TOlapTTL ttl;
        ttl.DeserializeFromProto(GetTableInfoPtrVerified()->Description.GetTtlSettings()).Validate();
        TableTTL = std::move(ttl);
    }

    return TConclusionStatus::Success();
}

TConclusionStatus TInStoreTable::DoInitializeImpl(const TEntityInitializationContext& context) {
    return InitializeWithTableInfo(context);
}

TConclusion<std::shared_ptr<ISSEntityUpdate>> TInStoreTable::DoCreateUpdateImpl(const TUpdateInitializationContext& context) const {
    std::shared_ptr<ISSEntityUpdate> result;
    if (context.GetModification()->HasAlterTable()) {
        result = std::make_shared<TInStoreSchemaUpdate>();
    } else if (context.GetModification()->HasAlterColumnTable()) {
        auto& alter = context.GetModification()->GetAlterColumnTable();

        if (alter.HasAlterShards()) {
            if (alter.GetAlterShards().HasTransfer()) {
                result = std::make_shared<TInStoreShardsTransfer>();
            } else if (alter.GetAlterShards().HasModification()) {
                result = std::make_shared<TInStoreShardsUpdate>();
            }
        } else if (alter.HasReshardColumnTable()) {
            result = std::make_shared<TInStoreShardingUpdate>();
        } else if (alter.HasAlterSchema() || alter.HasAlterTtlSettings()) {
            result = std::make_shared<TInStoreSchemaUpdate>();
        }
    }
    if (!result) {
        return NKikimr::TConclusionStatus::Fail("Undefined modification type");
    }
    auto initConclusion = result->Initialize(context);
    if (initConclusion.IsFail()) {
        return initConclusion;
    }
    return result;
}

NKikimr::TConclusion<NKikimrSchemeOp::TColumnTableSchema> TInStoreTable::GetTableSchemaProto() const {
    AFL_VERIFY(!!StoreInfo);
    if (!StoreInfo->SchemaPresets.count(GetTableInfoPtrVerified()->Description.GetSchemaPresetId())) {
        return TConclusionStatus::Fail("No preset for in-store column table");
    }

    auto& preset = StoreInfo->SchemaPresets.at(GetTableInfoPtrVerified()->Description.GetSchemaPresetId());
    auto& presetProto = StoreInfo->GetDescription().GetSchemaPresets(preset.GetProtoIndex());
    if (!presetProto.HasSchema()) {
        return TConclusionStatus::Fail("No schema in preset for in-store column table");
    }
    return presetProto.GetSchema();
}

}