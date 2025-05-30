#include "manager.h"

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_actors.h>
#include <ydb/core/kqp/gateway/utils/metadata_helpers.h>
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <ydb/library/conclusion/generic/result.h>

namespace NKikimr::NKqp {

namespace {

using TYqlConclusionStatus = TExternalDataSourceManager::TYqlConclusionStatus;
using TAsyncStatus = TExternalDataSourceManager::TAsyncStatus;

template <typename TValue>
using TYqlConclusion = TConclusionImpl<TYqlConclusionStatus, TValue>;

//// Async actions

TAsyncStatus ValidateExternalDatasourceSecrets(const NKikimrSchemeOp::TExternalDataSourceDescription& externaDataSourceDesc, const TExternalDataSourceManager::TInternalModificationContext& context) {
    const auto& externalData = context.GetExternalData();
    const auto& userToken = externalData.GetUserToken();
    auto describeFuture = DescribeExternalDataSourceSecrets(externaDataSourceDesc.GetAuth(), userToken ? userToken->GetUserSID() : "", externalData.GetActorSystem());

    return describeFuture.Apply([](const NThreading::TFuture<TEvDescribeSecretsResponse::TDescription>& f) {
        if (const auto& value = f.GetValue(); value.Status != Ydb::StatusIds::SUCCESS) {
            return TExternalDataSourceManager::TYqlConclusionStatus::Fail(NYql::YqlStatusFromYdbStatus(value.Status), value.Issues.ToString());   
        }
        return TExternalDataSourceManager::TYqlConclusionStatus::Success();
    });
}

//// Sync actions

TString GetOrEmpty(const NYql::TCreateObjectSettings& container, const TString& key) {
    auto fValue = container.GetFeaturesExtractor().Extract(key);
    return fValue ? *fValue : TString{};
}

[[nodiscard]] TYqlConclusionStatus FillCreateExternalDataSourceDesc(NKikimrSchemeOp::TExternalDataSourceDescription& externaDataSourceDesc, const TString& name, const NYql::TCreateObjectSettings& settings) {
    externaDataSourceDesc.SetName(name);
    externaDataSourceDesc.SetSourceType(GetOrEmpty(settings, "source_type"));
    externaDataSourceDesc.SetLocation(GetOrEmpty(settings, "location"));
    externaDataSourceDesc.SetInstallation(GetOrEmpty(settings, "installation"));
    externaDataSourceDesc.SetReplaceIfExists(settings.GetReplaceIfExists());

    const TString& authMethod = GetOrEmpty(settings, "auth_method");
    if (authMethod == "NONE") {
        externaDataSourceDesc.MutableAuth()->MutableNone();
    } else if (authMethod == "SERVICE_ACCOUNT") {
        auto& sa = *externaDataSourceDesc.MutableAuth()->MutableServiceAccount();
        sa.SetId(GetOrEmpty(settings, "service_account_id"));
        sa.SetSecretName(GetOrEmpty(settings, "service_account_secret_name"));
    } else if (authMethod == "BASIC") {
        auto& basic = *externaDataSourceDesc.MutableAuth()->MutableBasic();
        basic.SetLogin(GetOrEmpty(settings, "login"));
        basic.SetPasswordSecretName(GetOrEmpty(settings, "password_secret_name"));
    } else if (authMethod == "MDB_BASIC") {
        auto& mdbBasic = *externaDataSourceDesc.MutableAuth()->MutableMdbBasic();
        mdbBasic.SetServiceAccountId(GetOrEmpty(settings, "service_account_id"));
        mdbBasic.SetServiceAccountSecretName(GetOrEmpty(settings, "service_account_secret_name"));
        mdbBasic.SetLogin(GetOrEmpty(settings, "login"));
        mdbBasic.SetPasswordSecretName(GetOrEmpty(settings, "password_secret_name"));
    } else if (authMethod == "AWS") {
        auto& aws = *externaDataSourceDesc.MutableAuth()->MutableAws();
        aws.SetAwsAccessKeyIdSecretName(GetOrEmpty(settings, "aws_access_key_id_secret_name"));
        aws.SetAwsSecretAccessKeySecretName(GetOrEmpty(settings, "aws_secret_access_key_secret_name"));
        aws.SetAwsRegion(GetOrEmpty(settings, "aws_region"));
    } else if (authMethod == "TOKEN") {
        auto& token = *externaDataSourceDesc.MutableAuth()->MutableToken();
        token.SetTokenSecretName(GetOrEmpty(settings, "token_secret_name"));
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
        "use_ssl", // solomon
        "grpc_port" // solomon
    };

    auto& featuresExtractor = settings.GetFeaturesExtractor();
    for (const auto& property: properties) {
        if (const auto value = featuresExtractor.Extract(property)) {
            externaDataSourceDesc.MutableProperties()->MutableProperties()->insert({property, *value});
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
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder() << "Invalid extarnal data source path: " << error);
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

    return FillCreateExternalDataSourceDesc(*schemeTx.MutableCreateExternalDataSource(), name, settings);
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

TAsyncStatus TExternalDataSourceManager::ExecuteSchemeRequest(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalModificationContext& context, NKqpProto::TKqpSchemeOperation::OperationCase operationCase) const {
    TAsyncStatus validationFuture = NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Success());
    if (operationCase == NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource) {
        validationFuture = ChainFeatures(validationFuture, [desc = schemeTx.GetCreateExternalDataSource(), context] {
            return ValidateExternalDatasourceSecrets(desc, context);
        });
    }
    return ChainFeatures(validationFuture, [schemeTx, context] {
        return SendSchemeRequest(schemeTx, context);
    });
}

}  // namespace NKikimr::NKqp
