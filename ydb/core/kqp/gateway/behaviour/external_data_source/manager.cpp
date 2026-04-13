#include "manager.h"

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>
#include <ydb/core/kqp/federated_query/actors/kqp_federated_query_actors.h>
#include <ydb/core/kqp/gateway/utils/metadata_helpers.h>
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <ydb/library/conclusion/generic/result.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/core/external_sources/iceberg_fields.h>

namespace NKikimr::NKqp {

namespace {

using TYqlConclusionStatus = TExternalDataSourceManager::TYqlConclusionStatus;
using TAsyncStatus = TExternalDataSourceManager::TAsyncStatus;

template <typename TValue>
using TYqlConclusion = TConclusionImpl<TYqlConclusionStatus, TValue>;

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
    externalDataSourceDesc.SetReplaceIfExists(settings.GetReplaceIfExists());

    const TString& authMethod = GetOrEmpty(settings, "auth_method");
    if (authMethod == "NONE") {
        externalDataSourceDesc.MutableAuth()->MutableNone();
    } else if (authMethod == "SERVICE_ACCOUNT") {
        auto& sa = *externalDataSourceDesc.MutableAuth()->MutableServiceAccount();
        sa.SetId(GetOrEmpty(settings, "service_account_id"));
        sa.SetSecretName(GetSecretName(settings, "service_account_secret"));
    } else if (authMethod == "BASIC") {
        auto& basic = *externalDataSourceDesc.MutableAuth()->MutableBasic();
        basic.SetLogin(GetOrEmpty(settings, "login"));
        basic.SetPasswordSecretName(GetSecretName(settings, "password_secret"));
    } else if (authMethod == "MDB_BASIC") {
        auto& mdbBasic = *externalDataSourceDesc.MutableAuth()->MutableMdbBasic();
        mdbBasic.SetServiceAccountId(GetOrEmpty(settings, "service_account_id"));
        mdbBasic.SetServiceAccountSecretName(GetSecretName(settings, "service_account_secret"));
        mdbBasic.SetLogin(GetOrEmpty(settings, "login"));
        mdbBasic.SetPasswordSecretName(GetSecretName(settings, "password_secret"));
    } else if (authMethod == "AWS") {
        auto& aws = *externalDataSourceDesc.MutableAuth()->MutableAws();
        aws.SetAwsAccessKeyIdSecretName(GetSecretName(settings, "aws_access_key_id_secret"));
        aws.SetAwsSecretAccessKeySecretName(GetSecretName(settings, "aws_secret_access_key_secret"));
        aws.SetAwsRegion(GetOrEmpty(settings, "aws_region"));
    } else if (authMethod == "TOKEN") {
        auto& token = *externalDataSourceDesc.MutableAuth()->MutableToken();
        token.SetTokenSecretName(GetSecretName(settings, "token_secret"));
    } else if (authMethod == "IAM") {
        auto& iam = *externalDataSourceDesc.MutableAuth()->MutableIam();
        iam.SetServiceAccountId(GetOrEmpty(settings, "service_account_id"));
        iam.SetInitialTokenSecretName(GetSecretName(settings, "initial_token_secret"));
        // Note: user must not be allowed to specify resource_id;
        // database authorization relies on resource_id lookup;
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

TAsyncStatus ResolveResourceId(TAsyncStatus validationFuture, const TExternalDataSourceManager::TExternalModificationContext& context, const std::shared_ptr<NKikimrSchemeOp::TModifyScheme>& schemeTxState, const std::shared_ptr<std::vector<TString>>& secrets) {
    auto actorSystem = context.GetActorSystem();
    if (!AppData(actorSystem)->FeatureFlags.GetEnableExternalDataSourceAuthMethodIam()) {
        return NThreading::MakeFuture(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_UNSUPPORTED, "AUTH_METHOD=IAM is disabled. Please contact your system administrator to enable it"));
    }
    return ChainFeatures(validationFuture, [schemeTxState, secrets, actorSystem] () -> TAsyncStatus {
        if (!secrets || secrets->size() != 1) {
            return NThreading::MakeFuture(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, TStringBuilder() << "AUTH=IAM expected resolved secrets"));
        }
        const auto& desc = schemeTxState->GetCreateExternalDataSource();
        if (desc.GetSourceType() != ToString(NYql::EDatabaseType::Ydb)) {
            return NThreading::MakeFuture(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder() << "AUTH=IAM supported only for EXTERNAL DATA SOURCES ... SOURCE_TYPE=" << NYql::EDatabaseType::Ydb << ", requested for: " << desc.GetSourceType()));
        }
        const auto& prop = desc.GetProperties().GetProperties();
        TString endpoint = desc.GetLocation();
        TString database;
        bool useTls = false;
        TString caCert;
        if (auto it = prop.find("database_name"); it != prop.end()) {
            database = it->second;
        }
        if (auto it = prop.find("use_tls"); it != prop.end()) {
            auto maybeUseTls = TryFromString<bool>(it->second);
            if (!maybeUseTls) {
                return NThreading::MakeFuture(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder() << "use_tls: expected bool, got " << it->second));
            }
            useTls = *maybeUseTls;
        }
        // XXX caCert: not available
        return DescribeExternalDataSourceResourceId(endpoint, database, useTls, caCert, (*secrets)[0], actorSystem)
            .Apply([schemeTxState](const auto& future) {
                auto& value = future.GetValue();
                if (value.Status != Ydb::StatusIds::SUCCESS) {
                    return TYqlConclusionStatus::Fail(NYql::YqlStatusFromYdbStatus(value.Status), value.Issues.ToString());
                }
                auto& desc = *schemeTxState->MutableCreateExternalDataSource();
                desc.MutableAuth()->MutableIam()->SetResourceId(value.ResourceId);
                return TYqlConclusionStatus::Success();

            });
    });
}
} // namespace {

TAsyncStatus TExternalDataSourceManager::ExecuteSchemeRequest(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalModificationContext& context, NKqpProto::TKqpSchemeOperation::OperationCase operationCase) const {
    TAsyncStatus validationFuture = NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Success());
    auto schemeTxState = std::make_shared<NKikimrSchemeOp::TModifyScheme>(schemeTx);
    if (operationCase == NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource) {
        bool isResolveResourceIdNeeded = IsResolveResourceIdNeeded(schemeTx);
        auto secrets = isResolveResourceIdNeeded ? std::make_shared<std::vector<TString>>() : nullptr;
        validationFuture = ChainFeatures(validationFuture, [schemeTxState, context, secrets] {
            return ValidateExternalDatasourceSecrets(schemeTxState->GetCreateExternalDataSource(), context, secrets);
        });
        if (isResolveResourceIdNeeded) {
            validationFuture = ResolveResourceId(validationFuture, context, schemeTxState, secrets);
        }
    }
    return ChainFeatures(validationFuture, [schemeTxState, context] {
        return SendSchemeRequest(*schemeTxState, context);
    });
}

}  // namespace NKikimr::NKqp
