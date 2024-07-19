#include "manager.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h>
#include <ydb/core/kqp/gateway/actors/scheme.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>


namespace NKikimr::NKqp {

namespace {

using namespace NResourcePool;

//// Async actions

struct TFeatureFlagCheckResult {
    NYql::EYqlIssueCode Status = NYql::TIssuesIds::SUCCESS;
    NYql::TIssues Issues;

    void SetStatus(NYql::EYqlIssueCode status) {
        Status = status;
    }

    void AddIssue(NYql::TIssue issue) {
        Issues.AddIssue(std::move(issue));
    }

    void AddIssues(NYql::TIssues issues) {
        Issues.AddIssues(std::move(issues));
    }

    TFeatureFlagCheckResult& FromFeatureFlag(bool enableResourcePools) {
        Issues.Clear();
        if (enableResourcePools) {
            Status = NYql::TIssuesIds::SUCCESS;
        } else {
            Status = NYql::TIssuesIds::KIKIMR_UNSUPPORTED;
            Issues.AddIssue("Resource pools are disabled. Please contact your system administrator to enable it");
        }
        return *this;
    }
};

TResourcePoolManager::TAsyncStatus CheckFeatureFlag(const TResourcePoolManager::TExternalModificationContext& context, ui32 nodeId) {
    auto* actorSystem = context.GetActorSystem();
    if (!actorSystem) {
        ythrow yexception() << "This place needs an actor system. Please contact internal support";
    }

    using TRequest = NConsole::TEvConfigsDispatcher::TEvGetConfigRequest;
    using TResponse = NConsole::TEvConfigsDispatcher::TEvGetConfigResponse;
    auto event = std::make_unique<TRequest>((ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem);

    auto promise = NThreading::NewPromise<TFeatureFlagCheckResult>();
    actorSystem->Register(new TActorRequestHandler<TRequest, TResponse, TFeatureFlagCheckResult>(
        NConsole::MakeConfigsDispatcherID(nodeId), event.release(), promise,
        [](NThreading::TPromise<TFeatureFlagCheckResult> promise, TResponse&& response) {
            promise.SetValue(TFeatureFlagCheckResult()
                .FromFeatureFlag(response.Config->GetFeatureFlags().GetEnableResourcePools())
            );
        }
    ));

    return promise.GetFuture().Apply([actorSystem](const NThreading::TFuture<TFeatureFlagCheckResult>& f) {
        auto result = f.GetValue();
        if (result.Status == NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE) {
            // Fallback if CMS is unavailable
            result.FromFeatureFlag(AppData(actorSystem)->FeatureFlags.GetEnableResourcePools());
        }
        if (result.Status == NYql::TIssuesIds::SUCCESS) {
            return TResourcePoolManager::TYqlConclusionStatus::Success();
        }
        return TResourcePoolManager::TYqlConclusionStatus::Fail(result.Status, result.Issues.ToString());
    });
}

TResourcePoolManager::TAsyncStatus SendSchemeRequest(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TResourcePoolManager::TExternalModificationContext& context) {
    auto request = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
    request->Record.SetDatabaseName(context.GetDatabase());
    if (context.GetUserToken()) {
        request->Record.SetUserToken(context.GetUserToken()->GetSerializedToken());
    }
    *request->Record.MutableTransaction()->MutableModifyScheme() = schemeTx;

    auto promise = NThreading::NewPromise<NKqp::TSchemeOpRequestHandler::TResult>();
    context.GetActorSystem()->Register(new TSchemeOpRequestHandler(request.release(), promise, true));

    return promise.GetFuture().Apply([](const NThreading::TFuture<NKqp::TSchemeOpRequestHandler::TResult>& f) {
        try {
            auto response = f.GetValue();
            if (response.Success()) {
                return TResourcePoolManager::TYqlConclusionStatus::Success();
            }
            return TResourcePoolManager::TYqlConclusionStatus::Fail(response.Status(), response.Issues().ToString());
        } catch (...) {
            return TResourcePoolManager::TYqlConclusionStatus::Fail(TStringBuilder() << "Scheme error: " << CurrentExceptionMessage());
        }
    });
}

//// Sync actions

void ValidateObjectId(const TString& objectId) {
    if (objectId.find('/') != TString::npos) {
        throw std::runtime_error("Resource pool id should not contain '/' symbol");
    }
}

void FillResourcePoolDescription(NKikimrSchemeOp::TResourcePoolDescription& resourcePoolDescription, const NYql::TCreateObjectSettings& settings) {
    resourcePoolDescription.SetName(settings.GetObjectId());

    auto& featuresExtractor = settings.GetFeaturesExtractor();
    featuresExtractor.ValidateResetFeatures();

    TPoolSettings resourcePoolSettings;
    auto& properties = *resourcePoolDescription.MutableProperties()->MutableProperties();
    for (const auto& [property, setting] : GetPropertiesMap(resourcePoolSettings, true)) {
        if (std::optional<TString> value = featuresExtractor.Extract(property)) {
            try {
                std::visit(TSettingsParser{*value}, setting);
            } catch (...) {
                throw yexception() << "Failed to parse property " << property << ": " << CurrentExceptionMessage();
            }
        } else if (!featuresExtractor.ExtractResetFeature(property)) {
            continue;
        }

        TString value = std::visit(TSettingsExtractor(), setting);
        properties.insert({property, value});
    }

    if (!featuresExtractor.IsFinished()) {
        ythrow yexception() << "Unknown property: " << featuresExtractor.GetRemainedParamsString();
    }

    if (settings.GetObjectId() == NResourcePool::DEFAULT_POOL_ID) {
        std::vector<TString> forbiddenProperties = {
            "concurrent_query_limit",
            "database_load_cpu_threshold"
        };
        for (const TString& property : forbiddenProperties) {
            if (properties.contains(property)) {
                ythrow yexception() << "Can not change property " << property << " for default pool";
            }
        }
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
        case EActivityType::Alter:
        case EActivityType::Drop:
            return TYqlConclusionStatus::Success();
    }
}

}  // anonymous namespace

//// Immediate modification

TResourcePoolManager::TAsyncStatus TResourcePoolManager::DoModify(const NYql::TObjectSettingsImpl& settings, ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const {
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
                return NThreading::MakeFuture<TYqlConclusionStatus>(StatusFromActivityType(context.GetActivityType()));
        }
    } catch (...) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(CurrentExceptionMessage()));
    }
}

TResourcePoolManager::TAsyncStatus TResourcePoolManager::CreateResourcePool(const NYql::TCreateObjectSettings& settings, TInternalModificationContext& context, ui32 nodeId) const {
    NKqpProto::TKqpSchemeOperation schemeOperation;
    PrepareCreateResourcePool(schemeOperation, settings, context);
    return ExecuteSchemeRequest(schemeOperation.GetCreateResourcePool(), context.GetExternalData(), nodeId);
}

TResourcePoolManager::TAsyncStatus TResourcePoolManager::AlterResourcePool(const NYql::TCreateObjectSettings& settings, TInternalModificationContext& context, ui32 nodeId) const {
    NKqpProto::TKqpSchemeOperation schemeOperation;
    PrepareAlterResourcePool(schemeOperation, settings, context);
    return ExecuteSchemeRequest(schemeOperation.GetAlterResourcePool(), context.GetExternalData(), nodeId);
}

TResourcePoolManager::TAsyncStatus TResourcePoolManager::DropResourcePool(const NYql::TCreateObjectSettings& settings, TInternalModificationContext& context, ui32 nodeId) const {
    NKqpProto::TKqpSchemeOperation schemeOperation;
    PrepareDropResourcePool(schemeOperation, settings, context);
    return ExecuteSchemeRequest(schemeOperation.GetDropResourcePool(), context.GetExternalData(), nodeId);
}

//// Deferred modification

TResourcePoolManager::TYqlConclusionStatus TResourcePoolManager::DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings, const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const {
    Y_UNUSED(manager);

    try {
        switch (context.GetActivityType()) {
            case EActivityType::Create:
                PrepareCreateResourcePool(schemeOperation, settings, context);
                break;
            case EActivityType::Alter:
                PrepareAlterResourcePool(schemeOperation, settings, context);
                break;
            case EActivityType::Drop:
                PrepareDropResourcePool(schemeOperation, settings, context);
                break;
            default:
                break;
        }

        return StatusFromActivityType(context.GetActivityType());
    } catch (...) {
        return TYqlConclusionStatus::Fail(CurrentExceptionMessage());
    }
}

void TResourcePoolManager::PrepareCreateResourcePool(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TCreateObjectSettings& settings, TInternalModificationContext& context) const {
    ValidateObjectId(settings.GetObjectId());

    if (settings.GetObjectId() == NResourcePool::DEFAULT_POOL_ID) {
        ythrow yexception() << "Cannot create default pool manually, pool will be created automatically during first request execution";
    }

    auto& schemeTx = *schemeOperation.MutableCreateResourcePool();
    schemeTx.SetWorkingDir(JoinPath({context.GetExternalData().GetDatabase(), ".resource_pools/"}));
    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateResourcePool);

    FillResourcePoolDescription(*schemeTx.MutableCreateResourcePool(), settings);
}

void TResourcePoolManager::PrepareAlterResourcePool(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TDropObjectSettings& settings, TInternalModificationContext& context) const {
    auto& schemeTx = *schemeOperation.MutableAlterResourcePool();
    schemeTx.SetWorkingDir(JoinPath({context.GetExternalData().GetDatabase(), ".resource_pools/"}));
    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterResourcePool);

    FillResourcePoolDescription(*schemeTx.MutableCreateResourcePool(), settings);
}

void TResourcePoolManager::PrepareDropResourcePool(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TDropObjectSettings& settings, TInternalModificationContext& context) const {
    auto& schemeTx = *schemeOperation.MutableDropResourcePool();
    schemeTx.SetWorkingDir(JoinPath({context.GetExternalData().GetDatabase(), ".resource_pools/"}));
    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropResourcePool);

    schemeTx.MutableDrop()->SetName(settings.GetObjectId());
}

//// Apply deferred modification

TResourcePoolManager::TAsyncStatus TResourcePoolManager::ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation, ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, const TExternalModificationContext& context) const {
    Y_UNUSED(manager);

    try {
        switch (schemeOperation.GetOperationCase()) {
            case NKqpProto::TKqpSchemeOperation::kCreateResourcePool:
                return ExecuteSchemeRequest(schemeOperation.GetCreateResourcePool(), context, nodeId);
            case NKqpProto::TKqpSchemeOperation::kAlterResourcePool:
                return ExecuteSchemeRequest(schemeOperation.GetAlterResourcePool(), context, nodeId);
            case NKqpProto::TKqpSchemeOperation::kDropResourcePool:
                return ExecuteSchemeRequest(schemeOperation.GetDropResourcePool(), context, nodeId);
            default:
                return NThreading::MakeFuture(TYqlConclusionStatus::Fail(TStringBuilder() << "Execution of prepare operation for RESOURCE_POOL object: unsupported operation: " << static_cast<i32>(schemeOperation.GetOperationCase())));
        }
    } catch (...) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(CurrentExceptionMessage()));
    }
}

TResourcePoolManager::TAsyncStatus TResourcePoolManager::ChainFeatures(TAsyncStatus lastFeature, std::function<TAsyncStatus()> callback) const {
    return lastFeature.Apply([callback](const TAsyncStatus& f) {
        auto status = f.GetValue();
        if (!status.Ok()) {
            return NThreading::MakeFuture(status);
        }
        return callback();
    });
}

TResourcePoolManager::TAsyncStatus TResourcePoolManager::ExecuteSchemeRequest(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalModificationContext& context, ui32 nodeId) const {
    auto validationFuture = CheckFeatureFlag(context, nodeId);
    return ChainFeatures(validationFuture, [schemeTx, context] {
        return SendSchemeRequest(schemeTx, context);
    });
}

}  // namespace NKikimr::NKqp
