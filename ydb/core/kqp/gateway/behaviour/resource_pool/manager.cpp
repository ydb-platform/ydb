#include "manager.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>
#include <ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h>
#include <ydb/core/kqp/gateway/utils/metadata_helpers.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>


namespace NKikimr::NKqp {

namespace {

using namespace NResourcePool;

using TYqlConclusionStatus = TResourcePoolManager::TYqlConclusionStatus;
using TAsyncStatus = TResourcePoolManager::TAsyncStatus;

struct TFeatureFlagExtractor : public IFeatureFlagExtractor {
    bool IsEnabled(const NKikimrConfig::TFeatureFlags& flags) const override {
        return flags.GetEnableResourcePools();
    }

    bool IsEnabled(const TFeatureFlags& flags) const override {
        return flags.GetEnableResourcePools();
    }

    TString GetMessageOnDisabled() const override {
        return "Resource pools are disabled. Please contact your system administrator to enable it";
    }
};

//// Sync actions

[[nodiscard]] TYqlConclusionStatus ValidateObjectId(const TString& objectId) {
    if (objectId.find('/') != TString::npos) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED, "Resource pool id should not contain '/' symbol");
    }
    return TYqlConclusionStatus::Success();
}

[[nodiscard]] TYqlConclusionStatus FillResourcePoolDescription(NKikimrSchemeOp::TResourcePoolDescription& resourcePoolDescription, const NYql::TCreateObjectSettings& settings) {
    resourcePoolDescription.SetName(settings.GetObjectId());

    auto& featuresExtractor = settings.GetFeaturesExtractor();
    if (auto error = featuresExtractor.ValidateResetFeatures()) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder() << "Invalid reset properties: " << *error);
    }

    TPoolSettings resourcePoolSettings;
    auto& properties = *resourcePoolDescription.MutableProperties()->MutableProperties();
    for (const auto& [property, setting] : resourcePoolSettings.GetPropertiesMap(true)) {
        if (std::optional<TString> value = featuresExtractor.Extract(property)) {
            try {
                std::visit(TPoolSettings::TParser{*value}, setting);
            } catch (const yexception& error) {
                return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder() << "Failed to parse property " << property << ": " << error.what());
            }
        } else if (!featuresExtractor.ExtractResetFeature(property)) {
            continue;
        }

        const TString value = std::visit(TPoolSettings::TExtractor(), setting);
        properties.insert({property, value});
    }

    if (!featuresExtractor.IsFinished()) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder() << "Unknown property: " << featuresExtractor.GetRemainedParamsString());
    }

    if (settings.GetObjectId() == NResourcePool::DEFAULT_POOL_ID) {
        std::vector<TString> forbiddenProperties = {
            "concurrent_query_limit",
            "database_load_cpu_threshold"
        };
        for (const TString& property : forbiddenProperties) {
            if (properties.contains(property)) {
                return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder() << "Can not change property " << property << " for default pool");
            }
        }
    }
    return TYqlConclusionStatus::Success();
}

[[nodiscard]] TYqlConclusionStatus ErrorFromActivityType(TResourcePoolManager::EActivityType activityType) {
    using EActivityType = TResourcePoolManager::EActivityType;

    switch (activityType) {
        case EActivityType::Undefined:
            return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, "Internal error. Undefined operation for RESOURCE_POOL object");
        case EActivityType::Upsert:
            return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_UNIMPLEMENTED, "Upsert operation for RESOURCE_POOL objects is not implemented");
        default:
            throw yexception() << "Unexpected status to fail: " << activityType;
    }
}

}  // anonymous namespace

//// Immediate modification

TAsyncStatus TResourcePoolManager::DoModify(const NYql::TObjectSettingsImpl& settings, ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const {
    Y_UNUSED(manager);

    try {
        switch (context.GetActivityType()) {
            case EActivityType::Create:
                return CreateResourcePool(settings, context, nodeId);
            case EActivityType::Alter:
                return AlterResourcePool(settings, context, nodeId);
            case EActivityType::Drop:
                return DropResourcePool(settings, context, nodeId);
            default:
                return NThreading::MakeFuture<TYqlConclusionStatus>(ErrorFromActivityType(context.GetActivityType()));
        }
    } catch (...) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, TStringBuilder() << "Internal error. Got unexpected exception during RESOURCE_POOL modification operation: " << CurrentExceptionMessage()));
    }
}

TAsyncStatus TResourcePoolManager::CreateResourcePool(const NYql::TCreateObjectSettings& settings, TInternalModificationContext& context, ui32 nodeId) const {
    NKqpProto::TKqpSchemeOperation schemeOperation;
    if (auto status = PrepareCreateResourcePool(schemeOperation, settings, context); status.IsFail()) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(status);
    }
    return ExecuteSchemeRequest(schemeOperation.GetCreateResourcePool(), context.GetExternalData(), nodeId, NKqpProto::TKqpSchemeOperation::kCreateResourcePool);
}

TAsyncStatus TResourcePoolManager::AlterResourcePool(const NYql::TCreateObjectSettings& settings, TInternalModificationContext& context, ui32 nodeId) const {
    NKqpProto::TKqpSchemeOperation schemeOperation;
    if (auto status = PrepareAlterResourcePool(schemeOperation, settings, context); status.IsFail()) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(status);
    }
    return ExecuteSchemeRequest(schemeOperation.GetAlterResourcePool(), context.GetExternalData(), nodeId, NKqpProto::TKqpSchemeOperation::kAlterResourcePool);
}

TAsyncStatus TResourcePoolManager::DropResourcePool(const NYql::TCreateObjectSettings& settings, TInternalModificationContext& context, ui32 nodeId) const {
    NKqpProto::TKqpSchemeOperation schemeOperation;
    if (auto status = PrepareDropResourcePool(schemeOperation, settings, context); status.IsFail()) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(status);
    }
    return ExecuteSchemeRequest(schemeOperation.GetDropResourcePool(), context.GetExternalData(), nodeId, NKqpProto::TKqpSchemeOperation::kDropResourcePool);
}

//// Deferred modification

TYqlConclusionStatus TResourcePoolManager::DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings, const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const {
    Y_UNUSED(manager);

    try {
        switch (context.GetActivityType()) {
            case EActivityType::Create:
                return PrepareCreateResourcePool(schemeOperation, settings, context);
            case EActivityType::Alter:
                return PrepareAlterResourcePool(schemeOperation, settings, context);
            case EActivityType::Drop:
                return PrepareDropResourcePool(schemeOperation, settings, context);
            default:
                return ErrorFromActivityType(context.GetActivityType());
        }
    } catch (...) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, TStringBuilder() << "Internal error. Got unexpected exception during preparation of RESOURCE_POOL modification operation: " << CurrentExceptionMessage());
    }
}

TYqlConclusionStatus TResourcePoolManager::PrepareCreateResourcePool(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TCreateObjectSettings& settings, TInternalModificationContext& context) const {
    if (auto status = ValidateObjectId(settings.GetObjectId()); status.IsFail()) {
        return status;
    }
    if (settings.GetObjectId() == NResourcePool::DEFAULT_POOL_ID) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED, "Cannot create default pool manually, pool will be created automatically during first request execution");
    }

    auto& schemeTx = *schemeOperation.MutableCreateResourcePool();
    schemeTx.SetWorkingDir(JoinPath({context.GetExternalData().GetDatabase(), ".metadata/workload_manager/pools/"}));
    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateResourcePool);

    return FillResourcePoolDescription(*schemeTx.MutableCreateResourcePool(), settings);
}

TYqlConclusionStatus TResourcePoolManager::PrepareAlterResourcePool(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TDropObjectSettings& settings, TInternalModificationContext& context) const {
    auto& schemeTx = *schemeOperation.MutableAlterResourcePool();
    schemeTx.SetWorkingDir(JoinPath({context.GetExternalData().GetDatabase(), ".metadata/workload_manager/pools/"}));
    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterResourcePool);

    return FillResourcePoolDescription(*schemeTx.MutableCreateResourcePool(), settings);
}

TYqlConclusionStatus TResourcePoolManager::PrepareDropResourcePool(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TDropObjectSettings& settings, TInternalModificationContext& context) const {
    auto& schemeTx = *schemeOperation.MutableDropResourcePool();
    schemeTx.SetWorkingDir(JoinPath({context.GetExternalData().GetDatabase(), ".metadata/workload_manager/pools/"}));
    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropResourcePool);

    schemeTx.MutableDrop()->SetName(settings.GetObjectId());

    return TYqlConclusionStatus::Success();
}

//// Apply deferred modification

TAsyncStatus TResourcePoolManager::ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation, ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, const TExternalModificationContext& context) const {
    Y_UNUSED(manager);

    try {
        switch (schemeOperation.GetOperationCase()) {
            case NKqpProto::TKqpSchemeOperation::kCreateResourcePool:
                return ExecuteSchemeRequest(schemeOperation.GetCreateResourcePool(), context, nodeId, schemeOperation.GetOperationCase());
            case NKqpProto::TKqpSchemeOperation::kAlterResourcePool:
                return ExecuteSchemeRequest(schemeOperation.GetAlterResourcePool(), context, nodeId, schemeOperation.GetOperationCase());
            case NKqpProto::TKqpSchemeOperation::kDropResourcePool:
                return ExecuteSchemeRequest(schemeOperation.GetDropResourcePool(), context, nodeId, schemeOperation.GetOperationCase());
            default:
                return NThreading::MakeFuture(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, TStringBuilder() << "Internal error. Execution of prepared operation for RESOURCE_POOL object: unsupported operation: " << static_cast<i32>(schemeOperation.GetOperationCase())));
        }
    } catch (...) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, TStringBuilder() << "Internal error. Got unexpected exception during execution of RESOURCE_POOL modification operation: " << CurrentExceptionMessage()));
    }
}

TAsyncStatus TResourcePoolManager::ExecuteSchemeRequest(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalModificationContext& context, ui32 nodeId, NKqpProto::TKqpSchemeOperation::OperationCase operationCase) const {
    TAsyncStatus validationFuture = NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Success());
    if (operationCase != NKqpProto::TKqpSchemeOperation::kDropResourcePool) {
        validationFuture = ChainFeatures(validationFuture, [context, nodeId] {
            return CheckFeatureFlag(nodeId, MakeIntrusive<TFeatureFlagExtractor>(), context);
        });
    }
    return ChainFeatures(validationFuture, [schemeTx, context] {
        return SendSchemeRequest(schemeTx, context);
    });
}

}  // namespace NKikimr::NKqp
