#include "manager.h"
#include "iam_delegation.h"
#include "iam_object_lookup.h"

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>
#include <ydb/core/kqp/federated_query/actors/kqp_federated_query_actors.h>
#include <ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h>
#include <ydb/core/kqp/gateway/utils/metadata_helpers.h>
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/conclusion/generic/result.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/core/external_sources/iceberg_fields.h>
#include <ydb/services/scheme_secret/resolver.h>

#include <util/generic/guid.h>

namespace NKikimr::NKqp {

namespace {

using TYqlConclusionStatus = TExternalDataSourceManager::TYqlConclusionStatus;
using TAsyncStatus = TExternalDataSourceManager::TAsyncStatus;

template <typename TValue>
using TYqlConclusion = TConclusionImpl<TYqlConclusionStatus, TValue>;

struct TIamObjectDescription : NYql::IKikimrGateway::TGenericResult {
    TYqlConclusionStatus Status = TYqlConclusionStatus::Success();
    bool NotFound = false;
    NExternalDataSource::TIamDelegation Delegation;
};

struct TCloudIdDescription : NYql::IKikimrGateway::TGenericResult {
    TYqlConclusionStatus Status = TYqlConclusionStatus::Success();
    TString CloudId;
};

NThreading::TFuture<TCloudIdDescription> DescribeDatabaseCloudId(
    const TExternalDataSourceManager::TExternalModificationContext& context)
{
    using TRequest = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TResponse = TEvTxProxySchemeCache::TEvNavigateKeySetResult;

    auto navigate = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    auto& entry = navigate->ResultSet.emplace_back();
    entry.Path = NKikimr::SplitPath(context.GetDatabase());
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    navigate->DatabaseName = context.GetDatabase();
    if (context.GetUserToken()) {
        navigate->UserToken = MakeIntrusive<NACLib::TUserToken>(*context.GetUserToken());
    }

    auto promise = NThreading::NewPromise<TCloudIdDescription>();
    auto future = promise.GetFuture();
    context.GetActorSystem()->Register(new TActorRequestHandler<TRequest, TResponse, TCloudIdDescription>(
        MakeSchemeCacheID(), new TRequest(navigate.Release()), promise,
        [](NThreading::TPromise<TCloudIdDescription> promise, TResponse&& response) {
            TCloudIdDescription result;
            const auto& request = *response.Request;
            if (request.ErrorCount || request.ResultSet.size() != 1) {
                result.Status = TYqlConclusionStatus::Fail(
                    NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                    "Cannot describe database for IAM delegation");
            } else if (const auto it = request.ResultSet.front().Attributes.find("cloud_id");
                       it != request.ResultSet.front().Attributes.end() && !it->second.empty())
            {
                result.CloudId = it->second;
            } else {
                result.Status = TYqlConclusionStatus::Fail(
                    NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                    "Database has no cloud_id attribute required by AUTH_METHOD=IAM");
            }
            promise.SetValue(std::move(result));
        }));
    return future;
}

NThreading::TFuture<TIamObjectDescription> DescribeIamObject(
    const TString& path,
    const TExternalDataSourceManager::TExternalModificationContext& context)
{
    using TRequest = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TResponse = TEvTxProxySchemeCache::TEvNavigateKeySetResult;

    auto navigate = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    auto& entry = navigate->ResultSet.emplace_back();
    entry.Path = NKikimr::SplitPath(path);
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown;
    entry.Kind = NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalDataSource;
    navigate->DatabaseName = context.GetDatabase();
    if (context.GetUserToken()) {
        navigate->UserToken = MakeIntrusive<NACLib::TUserToken>(*context.GetUserToken());
    }

    auto promise = NThreading::NewPromise<TIamObjectDescription>();
    auto future = promise.GetFuture();
    context.GetActorSystem()->Register(new TActorRequestHandler<TRequest, TResponse, TIamObjectDescription>(
        MakeSchemeCacheID(), new TRequest(navigate.Release()), promise,
        [](NThreading::TPromise<TIamObjectDescription> promise, TResponse&& response) {
            TIamObjectDescription result;
            const auto& request = *response.Request;
            if (request.ResultSet.size() == 1) {
                const auto& entry = request.ResultSet.front();
                if (NExternalDataSource::ClassifyIamObjectLookup(
                        entry.Status, static_cast<bool>(entry.ExternalDataSourceInfo)) ==
                    NExternalDataSource::EIamObjectLookupResult::NotFound) {
                    result.NotFound = true;
                    promise.SetValue(std::move(result));
                    return;
                }
            }
            if (request.ErrorCount || request.ResultSet.size() != 1 ||
                request.ResultSet.front().Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok ||
                !request.ResultSet.front().ExternalDataSourceInfo) {
                result.Status = TYqlConclusionStatus::Fail(
                    NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                    "Cannot describe external data source for IAM delegation");
                promise.SetValue(std::move(result));
                return;
            }
            const auto& description = request.ResultSet.front().ExternalDataSourceInfo->Description;
            if (!description.GetAuth().HasIam()) {
                promise.SetValue(std::move(result));
                return;
            }
            const auto& iam = description.GetAuth().GetIam();
            if (!iam.HasDelegationReferrerId()) {
                // Objects created before DDL-managed delegation have nothing to
                // revoke. In particular, do not invent a reference from PathId.
                promise.SetValue(std::move(result));
                return;
            }
            result.Delegation.ResourceId = iam.GetResourceId();
            result.Delegation.ServiceAccountId = iam.GetServiceAccountId();
            result.Delegation.ReferrerId = iam.GetDelegationReferrerId();
            promise.SetValue(std::move(result));
        }));
    return future;
}

//// Async actions

TAsyncStatus ValidateExternalDatasourceSecrets(const NKikimrSchemeOp::TExternalDataSourceDescription& externalDataSourceDesc, const TExternalDataSourceManager::TInternalModificationContext& context, const std::shared_ptr<std::vector<TString>>& secrets) {
    const auto& externalData = context.GetExternalData();
    const std::optional<NACLib::TUserToken>& userToken = externalData.GetUserToken();
    auto describeFuture = DescribeExternalDataSourceSecrets(
        externalDataSourceDesc.GetAuth(),
        userToken ? new NACLib::TUserToken(*userToken) : nullptr,
        externalData.GetDatabase(),
        externalData.GetActorSystem()
    );

    return describeFuture.Apply([secrets](const NThreading::TFuture<TEvDescribeSecretsResponse::TDescription>& f) {
        const auto& value = f.GetValue();
        if (value.Status != Ydb::StatusIds::SUCCESS) {
            return TExternalDataSourceManager::TYqlConclusionStatus::Fail(NYql::YqlStatusFromYdbStatus(value.Status), value.Issues.ToString());   
        }
        if (secrets) {
            *secrets = value.SecretValues;
        }
        return TExternalDataSourceManager::TYqlConclusionStatus::Success();
    });
}

//// Sync actions

TString GetOrEmpty(const NYql::TCreateObjectSettings& container, const TString& key) {
    auto fValue = container.GetFeaturesExtractor().Extract(key);
    return fValue ? *fValue : TString{};
}

TString GetSecretName(const NYql::TCreateObjectSettings& settings, const TString& secretKeyPrefix) {
    if (const auto secret = GetOrEmpty(settings, secretKeyPrefix + "_name"); !secret.empty()) {
        return secret;
    }

    return GetOrEmpty(settings, secretKeyPrefix + "_path");
}

[[nodiscard]] TYqlConclusionStatus CheckOldSecretCreationAllowed(
    bool disableOldSecretCreation,
    const TString& secretName)
{
    if (disableOldSecretCreation && secretName && !NSecret::IsSchemeSecret(secretName)) {
        return TYqlConclusionStatus::Fail(
            NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
            "Old secrets are disabled for creating new objects. Please use new secrets");
    }
    return TYqlConclusionStatus::Success();
}

[[nodiscard]] TYqlConclusionStatus FillCreateExternalDataSourceDesc(
    NKikimrSchemeOp::TExternalDataSourceDescription& externalDataSourceDesc,
    const TString& name,
    const NYql::TCreateObjectSettings& settings,
    NActors::TActorSystem* actorSystem)
{
    externalDataSourceDesc.SetName(name);
    externalDataSourceDesc.SetSourceType(GetOrEmpty(settings, "source_type"));
    externalDataSourceDesc.SetLocation(GetOrEmpty(settings, "location"));
    externalDataSourceDesc.SetInstallation(GetOrEmpty(settings, "installation"));

    const bool disableOldSecretCreation = actorSystem &&
        AppData(actorSystem)->FeatureFlags.GetDisableOldSecretCreation();

    const TString& authMethod = GetOrEmpty(settings, "auth_method");
    if (authMethod == "NONE") {
        externalDataSourceDesc.MutableAuth()->MutableNone();
    } else if (authMethod == "SERVICE_ACCOUNT") {
        auto& sa = *externalDataSourceDesc.MutableAuth()->MutableServiceAccount();
        sa.SetId(GetOrEmpty(settings, "service_account_id"));
        sa.SetSecretName(GetSecretName(settings, "service_account_secret"));
        if (const auto status =
            CheckOldSecretCreationAllowed(
                disableOldSecretCreation, sa.GetSecretName()); status.IsFail())
        {
            return status;
        }
    } else if (authMethod == "BASIC") {
        auto& basic = *externalDataSourceDesc.MutableAuth()->MutableBasic();
        basic.SetLogin(GetOrEmpty(settings, "login"));
        basic.SetPasswordSecretName(GetSecretName(settings, "password_secret"));
        if (const auto status =
            CheckOldSecretCreationAllowed(
                disableOldSecretCreation, basic.GetPasswordSecretName()); status.IsFail())
        {
            return status;
        }
    } else if (authMethod == "MDB_BASIC") {
        auto& mdbBasic = *externalDataSourceDesc.MutableAuth()->MutableMdbBasic();
        mdbBasic.SetServiceAccountId(GetOrEmpty(settings, "service_account_id"));
        mdbBasic.SetServiceAccountSecretName(GetSecretName(settings, "service_account_secret"));
        mdbBasic.SetLogin(GetOrEmpty(settings, "login"));
        mdbBasic.SetPasswordSecretName(GetSecretName(settings, "password_secret"));
        if (const auto status =
            CheckOldSecretCreationAllowed(
                disableOldSecretCreation, mdbBasic.GetServiceAccountSecretName()); status.IsFail())
        {
            return status;
        }
        if (const auto status =
            CheckOldSecretCreationAllowed(
                disableOldSecretCreation, mdbBasic.GetPasswordSecretName()); status.IsFail())
        {
            return status;
        }
    } else if (authMethod == "AWS") {
        auto& aws = *externalDataSourceDesc.MutableAuth()->MutableAws();
        aws.SetAwsAccessKeyIdSecretName(GetSecretName(settings, "aws_access_key_id_secret"));
        aws.SetAwsSecretAccessKeySecretName(GetSecretName(settings, "aws_secret_access_key_secret"));
        aws.SetAwsRegion(GetOrEmpty(settings, "aws_region"));
        if (const auto status =
            CheckOldSecretCreationAllowed(
                disableOldSecretCreation, aws.GetAwsAccessKeyIdSecretName()); status.IsFail())
        {
            return status;
        }
        if (const auto status =
            CheckOldSecretCreationAllowed(
                disableOldSecretCreation, aws.GetAwsSecretAccessKeySecretName()); status.IsFail())
        {
            return status;
        }
    } else if (authMethod == "TOKEN") {
        auto& token = *externalDataSourceDesc.MutableAuth()->MutableToken();
        token.SetTokenSecretName(GetSecretName(settings, "token_secret"));
        if (const auto status =
            CheckOldSecretCreationAllowed(
                disableOldSecretCreation, token.GetTokenSecretName()); status.IsFail())
        {
            return status;
        }
    } else if (authMethod == "IAM") {
        auto& iam = *externalDataSourceDesc.MutableAuth()->MutableIam();
        iam.SetServiceAccountId(GetOrEmpty(settings, "service_account_id"));
        const bool delegationEnabled = actorSystem &&
            AppData(actorSystem)->FeatureFlags.GetEnableExternalDataSourceIamDelegation();
        if (delegationEnabled) {
            iam.SetDelegationReferrerId(NExternalDataSource::MakeIamDelegationReferrerId(
                name, CreateGuidAsString()));
            if (iam.GetServiceAccountId().empty()) {
                return TYqlConclusionStatus::Fail(
                    NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                    "SERVICE_ACCOUNT_ID is required for AUTH_METHOD=IAM");
            }
        } else {
            iam.SetInitialTokenSecretName(GetSecretName(settings, "initial_token_secret"));
            if (const auto status = CheckOldSecretCreationAllowed(
                    disableOldSecretCreation, iam.GetInitialTokenSecretName()); status.IsFail())
            {
                return status;
            }
        }
    } else {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, TStringBuilder() << "Internal error. Unknown auth method: " << authMethod);
    }

    static const TSet<TString> properties {
        "database_name", 
        "protocol", // managed PG, CH
        "mdb_cluster_id", // managed PG, CH, GP, MY
        "database_id", // managed YDB
        "use_tls",
        "schema", // managed PG, GP
        "service_name", // oracle
        "folder_id", // logging
        "reading_mode", // mongodb
        "unexpected_type_display_mode", // mongodb
        "unsupported_type_display_mode", // mongodb
        "grpc_location", // solomon
        "project", // solomon
        "cluster", // solomon
        "shared_reading" // ydb (topics)
    };

    auto& featuresExtractor = settings.GetFeaturesExtractor();

    for (const auto& property : properties) {
        if (const auto value = featuresExtractor.Extract(property)) {
            if (property == "shared_reading") {
                if (!actorSystem || !AppData(actorSystem)->FeatureFlags.GetEnableSharedReadingInStreamingQueries()) {
                    return TYqlConclusionStatus::Fail(
                        NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                        "SHARED_READING in External data source is not supported");
                }
            }
            externalDataSourceDesc.MutableProperties()->MutableProperties()->insert({property, *value});
        }
    }

    // Iceberg properties for connector
    for (const auto& property : NKikimr::NExternalSource::NIceberg::FieldsToConnector) {
        if (const auto value = featuresExtractor.Extract(property)) {
            externalDataSourceDesc.MutableProperties()->MutableProperties()->insert({property, *value});
        }
    }

    if (!featuresExtractor.IsFinished()) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder() << "Unknown property: " << featuresExtractor.GetRemainedParamsString());
    }
    return TYqlConclusionStatus::Success();
}

TYqlConclusion<std::pair<TString, TString>> SplitPath(const TString& tableName, const TString& database, bool createDir) {
    std::pair<TString, TString> pathPair;
    TString error;
    if (!NSchemeHelpers::SplitTablePath(tableName, database, pathPair, error, createDir)) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder() << "Invalid external data source path: " << error);
    }
    return pathPair;
}

[[nodiscard]] TYqlConclusionStatus CheckFeatureFlag(const TExternalDataSourceManager::TInternalModificationContext& context) {
    auto* actorSystem = context.GetExternalData().GetActorSystem();
    if (!actorSystem) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, "Internal error. EXTERNAL_DATA_SOURCE creation and drop operations needs an actor system. Please contact internal support");
    }

    if (!AppData(actorSystem)->FeatureFlags.GetEnableExternalDataSources()) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_UNSUPPORTED, "External data sources are disabled. Please contact your system administrator to enable it");
    }
    return TYqlConclusionStatus::Success();
}

[[nodiscard]] TYqlConclusionStatus ErrorFromActivityType(TExternalDataSourceManager::EActivityType activityType) {
    using EActivityType = TExternalDataSourceManager::EActivityType;

    switch (activityType) {
        case EActivityType::Undefined:
            return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, "Internal error. Undefined operation for EXTERNAL_DATA_SOURCE object");
        case EActivityType::Upsert:
            return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_UNIMPLEMENTED, "Upsert operation for EXTERNAL_DATA_SOURCE objects is not implemented");
        case EActivityType::Alter:
            return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_UNIMPLEMENTED, "Alter operation for EXTERNAL_DATA_SOURCE objects is not implemented");
        default:
            throw yexception() << "Unexpected status to fail: " << activityType;
    }
}

}  // anonymous namespace

//// Immediate modification

TAsyncStatus TExternalDataSourceManager::DoModify(const NYql::TObjectSettingsImpl& settings, const ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const {
    Y_UNUSED(nodeId, manager);

    try {
        switch (context.GetActivityType()) {
            case EActivityType::Create:
                return CreateExternalDataSource(settings, context);
            case EActivityType::Drop:
                return DropExternalDataSource(settings, context);
            default:
                return NThreading::MakeFuture<TYqlConclusionStatus>(ErrorFromActivityType(context.GetActivityType()));
        }
    } catch (...) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, TStringBuilder() << "Internal error. Got unexpected exception during EXTERNAL_DATA_SOURCE modification operation: " << CurrentExceptionMessage()));
    }
}

TAsyncStatus TExternalDataSourceManager::CreateExternalDataSource(const NYql::TCreateObjectSettings& settings, TInternalModificationContext& context) const {
    NKqpProto::TKqpSchemeOperation schemeOperation;
    if (auto status = PrepareCreateExternalDataSource(schemeOperation, settings, context); status.IsFail()) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(status);
    }
    return ExecuteSchemeRequest(schemeOperation.GetCreateExternalDataSource(), context.GetExternalData(), NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource);
}

TAsyncStatus TExternalDataSourceManager::DropExternalDataSource(const NYql::TDropObjectSettings& settings, TInternalModificationContext& context) const {
    NKqpProto::TKqpSchemeOperation schemeOperation;
    if (auto status = PrepareDropExternalDataSource(schemeOperation, settings, context); status.IsFail()) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(status);
    }
    return ExecuteSchemeRequest(schemeOperation.GetDropExternalDataSource(), context.GetExternalData(), NKqpProto::TKqpSchemeOperation::kDropExternalDataSource);
}

//// Deferred modification

TYqlConclusionStatus TExternalDataSourceManager::DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings, const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const {
    Y_UNUSED(manager);

    try {
        switch (context.GetActivityType()) {
            case EActivityType::Create:
                return PrepareCreateExternalDataSource(schemeOperation, settings, context);
            case EActivityType::Drop:
                return PrepareDropExternalDataSource(schemeOperation, settings, context);
            default:
                return ErrorFromActivityType(context.GetActivityType());
        }
    } catch (...) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, TStringBuilder() << "Internal error. Got unexpected exception during preparation of EXTERNAL_DATA_SOURCE modification operation: " << CurrentExceptionMessage());
    }
}

TYqlConclusionStatus TExternalDataSourceManager::PrepareCreateExternalDataSource(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TCreateObjectSettings& settings, TInternalModificationContext& context) const {
    if (auto status = CheckFeatureFlag(context); status.IsFail()) {
        return status;
    }

    auto pathPairStatus = SplitPath(settings.GetObjectId(), context.GetExternalData().GetDatabase(), true);
    if (pathPairStatus.IsFail()) {
        return pathPairStatus;
    }
    const auto& [workingDir, name] = pathPairStatus.DetachResult();

    auto& schemeTx = *schemeOperation.MutableCreateExternalDataSource();
    schemeTx.SetWorkingDir(workingDir);
    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateExternalDataSource);
    schemeTx.SetFailedOnAlreadyExists(!settings.GetExistingOk());
    schemeTx.SetReplaceIfExists(settings.GetReplaceIfExists());

    return FillCreateExternalDataSourceDesc(
        *schemeTx.MutableCreateExternalDataSource(), name, settings, context.GetExternalData().GetActorSystem());
}

TYqlConclusionStatus TExternalDataSourceManager::PrepareDropExternalDataSource(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TDropObjectSettings& settings, TInternalModificationContext& context) const {
    if (auto status = CheckFeatureFlag(context); status.IsFail()) {
        return status;
    }

    auto pathPairStatus = SplitPath(settings.GetObjectId(), context.GetExternalData().GetDatabase(), false);
    if (pathPairStatus.IsFail()) {
        return pathPairStatus;
    }
    const auto& [workingDir, name] = pathPairStatus.DetachResult();

    auto& schemeTx = *schemeOperation.MutableDropExternalDataSource();
    schemeTx.SetWorkingDir(workingDir);
    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropExternalDataSource);
    schemeTx.SetSuccessOnNotExist(settings.GetMissingOk());

    schemeTx.MutableDrop()->SetName(name);

    return TYqlConclusionStatus::Success();
}

//// Apply deferred modification

TAsyncStatus TExternalDataSourceManager::ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation, const ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, const TExternalModificationContext& context) const {
    Y_UNUSED(nodeId, manager);

    try {
        switch (schemeOperation.GetOperationCase()) {
            case NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource:
                return ExecuteSchemeRequest(schemeOperation.GetCreateExternalDataSource(), context, schemeOperation.GetOperationCase());
            case NKqpProto::TKqpSchemeOperation::kDropExternalDataSource:
                return ExecuteSchemeRequest(schemeOperation.GetDropExternalDataSource(), context, schemeOperation.GetOperationCase());
            default:
                return NThreading::MakeFuture(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, TStringBuilder() << "Execution of prepared operation for EXTERNAL_DATA_SOURCE object: unsupported operation: " << static_cast<i32>(schemeOperation.GetOperationCase())));
        }
    } catch (...) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, TStringBuilder() << "Internal error. Got unexpected exception during execution of EXTERNAL_DATA_SOURCE modification operation: " << CurrentExceptionMessage()));
    }
}

namespace {
bool IsResolveResourceIdNeeded(const auto& schemeTx) {
    return schemeTx.GetCreateExternalDataSource().GetAuth().identity_case() == NKikimrSchemeOp::TAuth::kIam
        && !schemeTx.GetCreateExternalDataSource().GetAuth().GetIam().HasResourceId();
}

NExternalDataSource::TIamDelegationSettings GetIamDelegationSettings(NActors::TActorSystem* actorSystem) {
    const auto& config = AppData(actorSystem)->ReplicationConfig.GetIamServiceControl();
    NExternalDataSource::TIamDelegationSettings settings;
    settings.Endpoint = config.GetEndpoint();
    settings.ServiceId = config.GetServiceId();
    settings.MicroserviceId = config.GetMicroserviceId();
    settings.ResourceType = config.GetResourceType();
    settings.EnableSsl = config.GetEnableSsl();
    const auto& authConfig = AppData(actorSystem)->AuthConfig;
    if (authConfig.HasLocalMetadataService()) {
        settings.MetadataServiceHost = authConfig.GetLocalMetadataService().GetHost();
        settings.MetadataServicePort = authConfig.GetLocalMetadataService().GetPort();
    }
    return settings;
}

TAsyncStatus DelegationResultToStatus(NThreading::TFuture<NExternalDataSource::TIamDelegationResult> future) {
    return future.Apply([](const auto& f) {
        const auto& result = f.GetValue();
        return result.Success
            ? TYqlConclusionStatus::Success()
            : TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE, result.Error);
    });
}

TString GetIamSubject(const NACLib::TUserToken& token) {
    return NExternalDataSource::NormalizeIamSubject(token.GetUserSID());
}

TAsyncStatus ResolveResourceIdFromDatabase(
    const TExternalDataSourceManager::TExternalModificationContext& context,
    const std::shared_ptr<NKikimrSchemeOp::TModifyScheme>& schemeTxState)
{
    return DescribeDatabaseCloudId(context).Apply([schemeTxState](const auto& f) {
        auto result = f.GetValue();
        if (result.Status.IsFail()) {
            return result.Status;
        }
        schemeTxState->MutableCreateExternalDataSource()
            ->MutableAuth()->MutableIam()->SetResourceId(result.CloudId);
        return TYqlConclusionStatus::Success();
    });
}

TAsyncStatus ExecuteLegacySchemeRequest(
    const NKikimrSchemeOp::TModifyScheme& schemeTx,
    const TExternalDataSourceManager::TExternalModificationContext& context)
{
    if (!IsResolveResourceIdNeeded(schemeTx)) {
        return ValidateExternalDatasourceSecrets(
            schemeTx.GetCreateExternalDataSource(), context, nullptr).Apply(
                [schemeTx, context](const auto& f) {
                    if (f.GetValue().IsFail()) {
                        return NThreading::MakeFuture(f.GetValue());
                    }
                    return SendSchemeRequest(schemeTx, context);
                });
    }

    auto* actorSystem = context.GetActorSystem();
    if (!AppData(actorSystem)->FeatureFlags.GetEnableExternalDataSourceAuthMethodIam()) {
        return NThreading::MakeFuture(TYqlConclusionStatus::Fail(
            NYql::TIssuesIds::KIKIMR_UNSUPPORTED,
            "AUTH_METHOD=IAM is disabled. Please contact your system administrator to enable it"));
    }

    auto schemeTxState = std::make_shared<NKikimrSchemeOp::TModifyScheme>(schemeTx);
    auto secrets = std::make_shared<std::vector<TString>>();
    return ValidateExternalDatasourceSecrets(
        schemeTxState->GetCreateExternalDataSource(), context, secrets).Apply(
            [schemeTxState, secrets, context, actorSystem](const auto& f) -> TAsyncStatus {
                if (f.GetValue().IsFail()) {
                    return NThreading::MakeFuture(f.GetValue());
                }
                if (secrets->size() != 1) {
                    return NThreading::MakeFuture(TYqlConclusionStatus::Fail(
                        NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                        "AUTH=IAM expected resolved secrets"));
                }
                const auto& desc = schemeTxState->GetCreateExternalDataSource();
                if (desc.GetSourceType() != ToString(NYql::EDatabaseType::Ydb)) {
                    return NThreading::MakeFuture(TYqlConclusionStatus::Fail(
                        NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                        TStringBuilder() << "AUTH=IAM supported only for SOURCE_TYPE="
                            << NYql::EDatabaseType::Ydb));
                }
                const auto& props = desc.GetProperties().GetProperties();
                TString database;
                bool useTls = false;
                if (const auto it = props.find("database_name"); it != props.end()) {
                    database = it->second;
                }
                if (const auto it = props.find("use_tls"); it != props.end()) {
                    const auto parsed = TryFromString<bool>(it->second);
                    if (!parsed) {
                        return NThreading::MakeFuture(TYqlConclusionStatus::Fail(
                            NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                            TStringBuilder() << "use_tls: expected bool, got " << it->second));
                    }
                    useTls = *parsed;
                }
                return DescribeExternalDataSourceResourceId(
                    desc.GetLocation(), database, useTls, {}, (*secrets)[0], actorSystem).Apply(
                        [schemeTxState, context](const auto& described) {
                            const auto& value = described.GetValue();
                            if (value.Status != Ydb::StatusIds::SUCCESS) {
                                return NThreading::MakeFuture(TYqlConclusionStatus::Fail(
                                    NYql::YqlStatusFromYdbStatus(value.Status), value.Issues.ToString()));
                            }
                            schemeTxState->MutableCreateExternalDataSource()
                                ->MutableAuth()->MutableIam()->SetResourceId(value.ResourceId);
                            return SendSchemeRequest(*schemeTxState, context);
                        });
            });
}

} // namespace {

TAsyncStatus TExternalDataSourceManager::ExecuteSchemeRequest(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalModificationContext& context, NKqpProto::TKqpSchemeOperation::OperationCase operationCase) const {
    if (!AppData(context.GetActorSystem())->FeatureFlags.GetEnableExternalDataSourceIamDelegation()) {
        if (operationCase == NKqpProto::TKqpSchemeOperation::kDropExternalDataSource) {
            return SendSchemeRequest(schemeTx, context);
        }
        return ExecuteLegacySchemeRequest(schemeTx, context);
    }
    if (operationCase == NKqpProto::TKqpSchemeOperation::kDropExternalDataSource) {
        const TString path = TStringBuilder() << schemeTx.GetWorkingDir() << '/' << schemeTx.GetDrop().GetName();
        auto schemeTxState = std::make_shared<NKikimrSchemeOp::TModifyScheme>(schemeTx);
        return DescribeIamObject(path, context).Apply([schemeTxState, context](const auto& f) -> TAsyncStatus {
            auto described = f.GetValue();
            if (described.Status.IsFail()) {
                return NThreading::MakeFuture(described.Status);
            }
            if (described.NotFound) {
                return SendSchemeRequest(*schemeTxState, context);
            }
            if (!NExternalDataSource::IsManagedIamDelegation(described.Delegation)) {
                return SendSchemeRequest(*schemeTxState, context);
            }
            auto schemeFuture = SendSchemeRequest(*schemeTxState, context);
            return schemeFuture.Apply([delegation = std::move(described.Delegation), context](const auto& f) -> TAsyncStatus {
                const auto schemeStatus = f.GetValue();
                if (NExternalDataSource::SelectCleanupAfterSchemeRequest(
                        !schemeStatus.IsFail(), delegation, {}) !=
                    NExternalDataSource::EDelegationCleanup::Previous)
                {
                    return NThreading::MakeFuture(schemeStatus);
                }
                // A committed DROP cannot be converted to a client-visible error.
                return NExternalDataSource::RevokeIamDelegation(
                    GetIamDelegationSettings(context.GetActorSystem()), delegation,
                    context.GetActorSystem()).Apply([schemeStatus](const auto&) {
                        return schemeStatus;
                    });
            });
        });
    }

    TAsyncStatus validationFuture = NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Success());
    auto schemeTxState = std::make_shared<NKikimrSchemeOp::TModifyScheme>(schemeTx);
    auto previousIam = std::make_shared<TIamObjectDescription>();
    if (operationCase == NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource) {
        const bool hasIam = schemeTx.GetCreateExternalDataSource().GetAuth().HasIam();
        if (hasIam) {
            auto actorSystem = context.GetActorSystem();
            if (!AppData(actorSystem)->FeatureFlags.GetEnableExternalDataSourceAuthMethodIam()) {
                return NThreading::MakeFuture(TYqlConclusionStatus::Fail(
                    NYql::TIssuesIds::KIKIMR_UNSUPPORTED,
                    "AUTH_METHOD=IAM is disabled. Please contact your system administrator to enable it"));
            }
            const auto& userToken = context.GetUserToken();
            if (!userToken || !userToken->HasAuthType() || userToken->GetAuthType() != "AccessService") {
                return NThreading::MakeFuture(TYqlConclusionStatus::Fail(
                    NYql::TIssuesIds::KIKIMR_ACCESS_DENIED,
                    "AUTH_METHOD=IAM requires a cloud IAM authenticated session"));
            }
            if (schemeTx.GetCreateExternalDataSource().GetSourceType() != ToString(NYql::EDatabaseType::Ydb)) {
                return NThreading::MakeFuture(TYqlConclusionStatus::Fail(
                    NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                    "AUTH_METHOD=IAM is supported only for SOURCE_TYPE=Ydb"));
            }
            if (IsResolveResourceIdNeeded(schemeTx)) {
                validationFuture = ChainFeatures(validationFuture, [schemeTxState, context] {
                    return ResolveResourceIdFromDatabase(context, schemeTxState);
                });
            }
        } else {
            validationFuture = ChainFeatures(validationFuture, [schemeTxState, context] {
                return ValidateExternalDatasourceSecrets(
                    schemeTxState->GetCreateExternalDataSource(), context, nullptr);
            });
        }
        if (schemeTx.GetReplaceIfExists()) {
            const TString path = TStringBuilder() << schemeTx.GetWorkingDir() << '/'
                << schemeTx.GetCreateExternalDataSource().GetName();
            validationFuture = ChainFeatures(validationFuture, [previousIam, path, context] {
                return DescribeIamObject(path, context).Apply([previousIam](const auto& f) {
                    auto described = f.GetValue();
                    if (described.Status.IsFail()) {
                        return described.Status;
                    }
                    if (!described.NotFound) {
                        *previousIam = std::move(described);
                    }
                    return TYqlConclusionStatus::Success();
                });
            });
        }
    }
    return ChainFeatures(validationFuture, [schemeTxState, previousIam, context, operationCase]() -> TAsyncStatus {
        if (operationCase != NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource) {
            return SendSchemeRequest(*schemeTxState, context);
        }
        if (!schemeTxState->GetCreateExternalDataSource().GetAuth().HasIam()) {
            if (!NExternalDataSource::IsManagedIamDelegation(previousIam->Delegation)) {
                return SendSchemeRequest(*schemeTxState, context);
            }
            auto schemeFuture = SendSchemeRequest(*schemeTxState, context);
            return schemeFuture.Apply([previousIam, context](const auto& f) -> TAsyncStatus {
                const auto schemeStatus = f.GetValue();
                if (schemeStatus.IsFail()) {
                    return NThreading::MakeFuture(schemeStatus);
                }
                return NExternalDataSource::RevokeIamDelegation(
                    GetIamDelegationSettings(context.GetActorSystem()),
                    previousIam->Delegation,
                    context.GetActorSystem()).Apply([schemeStatus](const auto&) {
                        return schemeStatus;
                    });
            });
        }

        const auto& iam = schemeTxState->GetCreateExternalDataSource().GetAuth().GetIam();
        NExternalDataSource::TIamDelegation delegation {
            .ResourceId = iam.GetResourceId(),
            .ServiceAccountId = iam.GetServiceAccountId(),
            .ReferrerId = iam.GetDelegationReferrerId(),
        };
        auto setupFuture = DelegationResultToStatus(NExternalDataSource::SetupIamDelegation(
            GetIamDelegationSettings(context.GetActorSystem()),
            delegation,
            GetIamSubject(*context.GetUserToken()),
            context.GetActorSystem()));

        const auto& old = previousIam->Delegation;
        return ChainFeatures(setupFuture, [schemeTxState, old, delegation, context] {
            auto schemeFuture = SendSchemeRequest(*schemeTxState, context);
            return schemeFuture.Apply([old, delegation, context](const auto& f) -> TAsyncStatus {
                const auto schemeStatus = f.GetValue();
                const auto cleanup = NExternalDataSource::SelectCleanupAfterSchemeRequest(
                    !schemeStatus.IsFail(), old, delegation);
                if (cleanup == NExternalDataSource::EDelegationCleanup::Staged) {
                    // The staged delegation must not outlive a rejected ALTER.
                    return NExternalDataSource::RevokeIamDelegation(
                        GetIamDelegationSettings(context.GetActorSystem()), delegation,
                        context.GetActorSystem()).Apply([schemeStatus](const auto&) {
                            return schemeStatus;
                        });
                }
                if (cleanup != NExternalDataSource::EDelegationCleanup::Previous) {
                    return NThreading::MakeFuture(schemeStatus);
                }
                // The old delegation remains usable until SchemeShard commits.
                // Cleanup is best effort and cannot turn committed DDL into an error.
                return NExternalDataSource::RevokeIamDelegation(
                    GetIamDelegationSettings(context.GetActorSystem()), old,
                    context.GetActorSystem()).Apply([schemeStatus](const auto&) {
                        return schemeStatus;
                    });
            });
        });
    });
}

}  // namespace NKikimr::NKqp
