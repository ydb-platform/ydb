#include "manager.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>


namespace NKikimr::NKqp {

namespace {

void CheckFeatureFlag(TResourcePoolManager::TInternalModificationContext& context) {
    auto* actorSystem = context.GetExternalData().GetActorSystem();
    if (!actorSystem) {
        ythrow yexception() << "This place needs an actor system. Please contact internal support";
    }

    if (!AppData(actorSystem)->FeatureFlags.GetEnableResourcePools()) {
        throw std::runtime_error("Resource pools are disabled. Please contact your system administrator to enable it");
    }
}

void ValidateObjectId(const TString& objectId) {
    if (objectId.find('/') != TString::npos) {
        throw std::runtime_error("Resource pool id should not contain '/' symbol");
    }
}

TResourcePoolManager::TYqlConclusionStatus StatusFromActivityType(TResourcePoolManager::EActivityType activityType) {
    using TYqlConclusionStatus = TResourcePoolManager::TYqlConclusionStatus;
    using EActivityType = TResourcePoolManager::EActivityType;

    switch (activityType) {
        case EActivityType::Undefined:
            return TYqlConclusionStatus::Fail("Undefined operation for RESOURCE_POOL object");
        case EActivityType::Upsert:
            return TYqlConclusionStatus::Fail("Upsert operation for RESOURCE_POOL objects is not implemented");
        case EActivityType::Create:
            return TYqlConclusionStatus::Fail("Create operation for RESOURCE_POOL objects is not implemented");
        case EActivityType::Alter:
            return TYqlConclusionStatus::Fail("Alter operation for RESOURCE_POOL objects is not implemented");
        case EActivityType::Drop:
            return TYqlConclusionStatus::Fail("Drop operation for RESOURCE_POOL objects is not implemented");
    }
}

}  // anonymous namespace

NThreading::TFuture<TResourcePoolManager::TYqlConclusionStatus> TResourcePoolManager::DoModify(const NYql::TObjectSettingsImpl& settings, ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const {
    Y_UNUSED(nodeId, manager);

    try {
        CheckFeatureFlag(context);
        ValidateObjectId(settings.GetObjectId());

        return NThreading::MakeFuture<TYqlConclusionStatus>(StatusFromActivityType(context.GetActivityType()));
    } catch (...) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(CurrentExceptionMessage()));
    }
}

TResourcePoolManager::TYqlConclusionStatus TResourcePoolManager::DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings, const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const {
    Y_UNUSED(schemeOperation, manager);

    try {
        CheckFeatureFlag(context);
        ValidateObjectId(settings.GetObjectId());

        return StatusFromActivityType(context.GetActivityType());
    } catch (...) {
        return TYqlConclusionStatus::Fail(CurrentExceptionMessage());
    }
}

NThreading::TFuture<TResourcePoolManager::TYqlConclusionStatus> TResourcePoolManager::ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation, ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, const IOperationsManager::TExternalModificationContext& context) const {
    Y_UNUSED(nodeId, manager, context);

    return NThreading::MakeFuture(TYqlConclusionStatus::Fail(TStringBuilder() << "Execution of prepare operation for RESOURCE_POOL object: unsupported operation: " << static_cast<i32>(schemeOperation.GetOperationCase())));
}

}  // namespace NKikimr::NKqp
