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
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/async/wait_for_event.h>
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

TString GetIamSubject(const NACLib::TUserToken& token) {
    return NExternalDataSource::NormalizeIamSubject(token.GetUserSID());
}

class TIamDelegationDdlActor final
    : public NActors::TActorBootstrapped<TIamDelegationDdlActor>
{
    using TThis = TIamDelegationDdlActor;
    using TContext = TExternalDataSourceManager::TExternalModificationContext;
    using TStatus = TExternalDataSourceManager::TYqlConclusionStatus;

public:
    TIamDelegationDdlActor(
        NKikimrSchemeOp::TModifyScheme schemeTx,
        TContext context,
        NKqpProto::TKqpSchemeOperation::OperationCase operationCase,
        NThreading::TPromise<TStatus> promise)
        : SchemeTx(std::move(schemeTx))
        , Context(std::move(context))
        , OperationCase(operationCase)
        , Promise(std::move(promise))
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        Send(SelfId(), new TEvPrivate::TEvStart());
    }

private:
    struct TEvPrivate {
        enum EEv {
            EvStart = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvStatus,
            EvIamObject,
            EvCloudId,
            EvDelegation,
        };

        struct TEvStart : TEventLocal<TEvStart, EvStart> {};

        struct TEvStatus : TEventLocal<TEvStatus, EvStatus> {
            explicit TEvStatus(TStatus status)
                : Status(std::move(status))
            {}
            TStatus Status;
        };

        struct TEvIamObject : TEventLocal<TEvIamObject, EvIamObject> {
            explicit TEvIamObject(TIamObjectDescription description)
                : Description(std::move(description))
            {}
            TIamObjectDescription Description;
        };

        struct TEvCloudId : TEventLocal<TEvCloudId, EvCloudId> {
            explicit TEvCloudId(TCloudIdDescription description)
                : Description(std::move(description))
            {}
            TCloudIdDescription Description;
        };

        struct TEvDelegation : TEventLocal<TEvDelegation, EvDelegation> {
            explicit TEvDelegation(NExternalDataSource::TIamDelegationResult result)
                : Result(std::move(result))
            {}
            NExternalDataSource::TIamDelegationResult Result;
        };
    };

    static constexpr ui64 StatusCookie = 1;
    static constexpr ui64 IamObjectCookie = 2;
    static constexpr ui64 CloudIdCookie = 3;
    static constexpr ui64 DelegationCookie = 4;

    void HandleStart(TEvPrivate::TEvStart::TPtr) {
        co_await Execute();
    }

    NActors::async<TStatus> AwaitStatus(TAsyncStatus future) {
        future.Subscribe([actorSystem = TActivationContext::ActorSystem(), self = SelfId()](const auto& f) {
            actorSystem->Send(self, new TEvPrivate::TEvStatus(f.GetValue()), 0, StatusCookie);
        });
        const auto event = co_await NActors::ActorWaitForEvent<TEvPrivate::TEvStatus>(StatusCookie);
        co_return std::move(event->Get()->Status);
    }

    NActors::async<TIamObjectDescription> AwaitIamObject(
        NThreading::TFuture<TIamObjectDescription> future)
    {
        future.Subscribe([actorSystem = TActivationContext::ActorSystem(), self = SelfId()](const auto& f) {
            actorSystem->Send(self, new TEvPrivate::TEvIamObject(f.GetValue()), 0, IamObjectCookie);
        });
        const auto event = co_await NActors::ActorWaitForEvent<TEvPrivate::TEvIamObject>(IamObjectCookie);
        co_return std::move(event->Get()->Description);
    }

    NActors::async<TCloudIdDescription> AwaitCloudId(
        NThreading::TFuture<TCloudIdDescription> future)
    {
        future.Subscribe([actorSystem = TActivationContext::ActorSystem(), self = SelfId()](const auto& f) {
            actorSystem->Send(self, new TEvPrivate::TEvCloudId(f.GetValue()), 0, CloudIdCookie);
        });
        const auto event = co_await NActors::ActorWaitForEvent<TEvPrivate::TEvCloudId>(CloudIdCookie);
        co_return std::move(event->Get()->Description);
    }

    NActors::async<NExternalDataSource::TIamDelegationResult> AwaitDelegation(
        NThreading::TFuture<NExternalDataSource::TIamDelegationResult> future)
    {
        future.Subscribe([actorSystem = TActivationContext::ActorSystem(), self = SelfId()](const auto& f) {
            actorSystem->Send(self, new TEvPrivate::TEvDelegation(f.GetValue()), 0, DelegationCookie);
        });
        const auto event = co_await NActors::ActorWaitForEvent<TEvPrivate::TEvDelegation>(DelegationCookie);
        co_return std::move(event->Get()->Result);
    }

    TStatus DelegationStatus(const NExternalDataSource::TIamDelegationResult& result) const {
        return result.Success
            ? TStatus::Success()
            : TStatus::Fail(NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE, result.Error);
    }

    NActors::async<void> ExecuteDrop() {
        const TString path = TStringBuilder() << SchemeTx.GetWorkingDir() << '/' << SchemeTx.GetDrop().GetName();
        auto described = co_await AwaitIamObject(DescribeIamObject(path, Context));
        if (described.Status.IsFail()) {
            Finish(std::move(described.Status));
            co_return;
        }

        const auto schemeStatus = co_await AwaitStatus(SendSchemeRequest(SchemeTx, Context));
        if (!schemeStatus.IsFail() && !described.NotFound &&
            NExternalDataSource::IsManagedIamDelegation(described.Delegation))
        {
            // A committed DROP cannot be converted to a client-visible error.
            co_await AwaitDelegation(NExternalDataSource::RevokeIamDelegation(
                GetIamDelegationSettings(Context.GetActorSystem()), described.Delegation,
                Context.GetActorSystem()));
        }
        Finish(schemeStatus);
    }

    NActors::async<void> ExecuteCreateOrAlter() {
        auto previous = TIamObjectDescription{};
        const bool hasIam = SchemeTx.GetCreateExternalDataSource().GetAuth().HasIam();
        if (hasIam) {
            auto* actorSystem = Context.GetActorSystem();
            if (!AppData(actorSystem)->FeatureFlags.GetEnableExternalDataSourceAuthMethodIam()) {
                Finish(TStatus::Fail(NYql::TIssuesIds::KIKIMR_UNSUPPORTED,
                    "AUTH_METHOD=IAM is disabled. Please contact your system administrator to enable it"));
                co_return;
            }
            const auto& userToken = Context.GetUserToken();
            if (!userToken || !userToken->HasAuthType() || userToken->GetAuthType() != "AccessService") {
                Finish(TStatus::Fail(NYql::TIssuesIds::KIKIMR_ACCESS_DENIED,
                    "AUTH_METHOD=IAM requires a cloud IAM authenticated session"));
                co_return;
            }
            if (SchemeTx.GetCreateExternalDataSource().GetSourceType() != ToString(NYql::EDatabaseType::Ydb)) {
                Finish(TStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                    "AUTH_METHOD=IAM is supported only for SOURCE_TYPE=Ydb"));
                co_return;
            }
            if (IsResolveResourceIdNeeded(SchemeTx)) {
                auto cloud = co_await AwaitCloudId(DescribeDatabaseCloudId(Context));
                if (cloud.Status.IsFail()) {
                    Finish(std::move(cloud.Status));
                    co_return;
                }
                SchemeTx.MutableCreateExternalDataSource()->MutableAuth()->MutableIam()->SetResourceId(cloud.CloudId);
            }
        } else {
            const auto status = co_await AwaitStatus(ValidateExternalDatasourceSecrets(
                SchemeTx.GetCreateExternalDataSource(), Context, nullptr));
            if (status.IsFail()) {
                Finish(status);
                co_return;
            }
        }

        if (SchemeTx.GetReplaceIfExists()) {
            const TString path = TStringBuilder() << SchemeTx.GetWorkingDir() << '/'
                << SchemeTx.GetCreateExternalDataSource().GetName();
            previous = co_await AwaitIamObject(DescribeIamObject(path, Context));
            if (previous.Status.IsFail()) {
                Finish(std::move(previous.Status));
                co_return;
            }
        }

        if (!hasIam) {
            const auto schemeStatus = co_await AwaitStatus(SendSchemeRequest(SchemeTx, Context));
            if (!schemeStatus.IsFail() && NExternalDataSource::IsManagedIamDelegation(previous.Delegation)) {
                co_await AwaitDelegation(NExternalDataSource::RevokeIamDelegation(
                    GetIamDelegationSettings(Context.GetActorSystem()), previous.Delegation,
                    Context.GetActorSystem()));
            }
            Finish(schemeStatus);
            co_return;
        }

        const auto& iam = SchemeTx.GetCreateExternalDataSource().GetAuth().GetIam();
        NExternalDataSource::TIamDelegation staged{
            .ResourceId = iam.GetResourceId(),
            .ServiceAccountId = iam.GetServiceAccountId(),
            .ReferrerId = iam.GetDelegationReferrerId(),
        };
        const auto setup = co_await AwaitDelegation(NExternalDataSource::SetupIamDelegation(
            GetIamDelegationSettings(Context.GetActorSystem()), staged,
            GetIamSubject(*Context.GetUserToken()), Context.GetActorSystem()));
        if (!setup.Success) {
            Finish(DelegationStatus(setup));
            co_return;
        }

        const auto schemeStatus = co_await AwaitStatus(SendSchemeRequest(SchemeTx, Context));
        const auto cleanup = NExternalDataSource::SelectCleanupAfterSchemeRequest(
            !schemeStatus.IsFail(), previous.Delegation, staged);
        if (cleanup == NExternalDataSource::EDelegationCleanup::Staged) {
            co_await AwaitDelegation(NExternalDataSource::RevokeIamDelegation(
                GetIamDelegationSettings(Context.GetActorSystem()), staged, Context.GetActorSystem()));
        } else if (cleanup == NExternalDataSource::EDelegationCleanup::Previous) {
            // Cleanup is best effort and cannot turn committed DDL into an error.
            co_await AwaitDelegation(NExternalDataSource::RevokeIamDelegation(
                GetIamDelegationSettings(Context.GetActorSystem()), previous.Delegation,
                Context.GetActorSystem()));
        }
        Finish(schemeStatus);
    }

    NActors::async<void> Execute() {
        if (OperationCase == NKqpProto::TKqpSchemeOperation::kDropExternalDataSource) {
            co_await ExecuteDrop();
        } else if (OperationCase == NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource) {
            co_await ExecuteCreateOrAlter();
        } else {
            Finish(TStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                "Unsupported EXTERNAL_DATA_SOURCE operation"));
        }
    }

    void Finish(TStatus status) {
        Promise.SetValue(std::move(status));
        PassAway();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvPrivate::TEvStart, HandleStart);
    )

private:
    NKikimrSchemeOp::TModifyScheme SchemeTx;
    const TContext Context;
    const NKqpProto::TKqpSchemeOperation::OperationCase OperationCase;
    NThreading::TPromise<TStatus> Promise;
};

TAsyncStatus ExecuteIamDelegationDdl(
    const NKikimrSchemeOp::TModifyScheme& schemeTx,
    const TExternalDataSourceManager::TExternalModificationContext& context,
    NKqpProto::TKqpSchemeOperation::OperationCase operationCase)
{
    auto promise = NThreading::NewPromise<TYqlConclusionStatus>();
    auto future = promise.GetFuture();
    context.GetActorSystem()->Register(new TIamDelegationDdlActor(
        schemeTx, context, operationCase, std::move(promise)));
    return future;
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
    return ExecuteIamDelegationDdl(schemeTx, context, operationCase);
}

}  // namespace NKikimr::NKqp
