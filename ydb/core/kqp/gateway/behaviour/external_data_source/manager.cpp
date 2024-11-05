
#include "manager.h"

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_actors.h>
#include <ydb/core/kqp/gateway/actors/scheme.h>
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/feature_flags.h>

#include <util/string/type.h>

namespace NKikimr::NKqp {

namespace {

TString GetOrEmpty(const NYql::TCreateObjectSettings& container, const TString& key) {
    auto fValue = container.GetFeaturesExtractor().Extract(key);
    return fValue ? *fValue : TString{};
}

void CheckFeatureFlag(TExternalDataSourceManager::TInternalModificationContext& context) {
    auto* actorSystem = context.GetExternalData().GetActorSystem();
    if (!actorSystem) {
        ythrow yexception() << "This place needs an actor system. Please contact internal support";
    }

    if (!AppData(actorSystem)->FeatureFlags.GetEnableExternalDataSources()) {
        throw std::runtime_error("External data sources are disabled. Please contact your system administrator to enable it");
    }
}

void FillCreateExternalDataSourceDesc(NKikimrSchemeOp::TExternalDataSourceDescription& externaDataSourceDesc,
                                      const TString& name,
                                      const NYql::TCreateObjectSettings& settings) {
    externaDataSourceDesc.SetName(name);
    externaDataSourceDesc.SetSourceType(GetOrEmpty(settings, "source_type"));
    externaDataSourceDesc.SetLocation(GetOrEmpty(settings, "location"));
    externaDataSourceDesc.SetInstallation(GetOrEmpty(settings, "installation"));
    externaDataSourceDesc.SetReplaceIfExists(settings.GetReplaceIfExists());

    TString authMethod = GetOrEmpty(settings, "auth_method");
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
        ythrow yexception() << "Internal error. Unknown auth method: " << authMethod;
    }

    static const TSet<TString> properties {
        "database_name", 
        "protocol", // managed PG, CH
        "mdb_cluster_id", // managed PG, CH
        "database_id", // managed YDB
        "use_tls",
        "schema", // managed PG
        "service_name", // oracle
    };

    for (const auto& property: properties) {
        if (auto value = settings.GetFeaturesExtractor().Extract(property)) {
            externaDataSourceDesc.MutableProperties()->MutableProperties()->insert({property, *value});
        }
    }

    if (!settings.GetFeaturesExtractor().IsFinished()) {
        ythrow yexception() << "Unknown property: " << settings.GetFeaturesExtractor().GetRemainedParamsString();
    }
}

void FillCreateExternalDataSourceCommand(NKikimrSchemeOp::TModifyScheme& modifyScheme, const NYql::TCreateObjectSettings& settings,
                                         TExternalDataSourceManager::TInternalModificationContext& context) {
    CheckFeatureFlag(context);

    std::pair<TString, TString> pathPair;
    {
        TString error;
        if (!TrySplitPathByDb(settings.GetObjectId(), context.GetExternalData().GetDatabase(), pathPair, error)) {
            throw std::runtime_error(error.c_str());
        }
    }

    modifyScheme.SetWorkingDir(pathPair.first);
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateExternalDataSource);
    modifyScheme.SetFailedOnAlreadyExists(!settings.GetExistingOk());

    NKikimrSchemeOp::TExternalDataSourceDescription& dataSourceDesc = *modifyScheme.MutableCreateExternalDataSource();
    FillCreateExternalDataSourceDesc(dataSourceDesc, pathPair.second, settings);
}

void FillDropExternalDataSourceCommand(NKikimrSchemeOp::TModifyScheme& modifyScheme, const NYql::TDropObjectSettings& settings,
                                       TExternalDataSourceManager::TInternalModificationContext& context) {
    CheckFeatureFlag(context);

    std::pair<TString, TString> pathPair;
    {
        TString error;
        if (!NSchemeHelpers::TrySplitTablePath(settings.GetObjectId(), pathPair, error)) {
            throw std::runtime_error(error.c_str());
        }
    }

    modifyScheme.SetWorkingDir(pathPair.first);
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropExternalDataSource);
    modifyScheme.SetSuccessOnNotExist(settings.GetMissingOk());

    NKikimrSchemeOp::TDrop& drop = *modifyScheme.MutableDrop();
    drop.SetName(pathPair.second);
}

NThreading::TFuture<TExternalDataSourceManager::TYqlConclusionStatus> SendSchemeRequest(TEvTxUserProxy::TEvProposeTransaction* request, TActorSystem* actorSystem, bool failedOnAlreadyExists, bool successOnNotExist)
{
    auto promiseScheme = NThreading::NewPromise<NKqp::TSchemeOpRequestHandler::TResult>();
    IActor* requestHandler = new TSchemeOpRequestHandler(request, promiseScheme, failedOnAlreadyExists, successOnNotExist);
    actorSystem->Register(requestHandler);
    return promiseScheme.GetFuture().Apply([](const NThreading::TFuture<NKqp::TSchemeOpRequestHandler::TResult>& f) {
        if (f.HasValue() && !f.HasException() && f.GetValue().Success()) {
            return TExternalDataSourceManager::TYqlConclusionStatus::Success();
        } else if (f.HasValue()) {
            return TExternalDataSourceManager::TYqlConclusionStatus::Fail(f.GetValue().Status(), f.GetValue().Issues().ToString());
        }
        return TExternalDataSourceManager::TYqlConclusionStatus::Fail("no value in result");
    });
}

NThreading::TFuture<TExternalDataSourceManager::TYqlConclusionStatus> ValidateCreateExternalDatasource(const NKikimrSchemeOp::TExternalDataSourceDescription& externaDataSourceDesc, const TExternalDataSourceManager::TInternalModificationContext& context) {
    const auto& authDescription = externaDataSourceDesc.GetAuth();
    const auto& externalData = context.GetExternalData();
    const auto& userToken = externalData.GetUserToken();
    auto describeFuture = DescribeExternalDataSourceSecrets(authDescription, userToken ? userToken->GetUserSID() : "", externalData.GetActorSystem());

    return describeFuture.Apply([](const NThreading::TFuture<TEvDescribeSecretsResponse::TDescription>& f) mutable {
        if (const auto& value = f.GetValue(); value.Status != Ydb::StatusIds::SUCCESS) {
            return TExternalDataSourceManager::TYqlConclusionStatus::Fail(NYql::YqlStatusFromYdbStatus(value.Status), value.Issues.ToString());   
        }
        return TExternalDataSourceManager::TYqlConclusionStatus::Success();
    });
}

}

NThreading::TFuture<TExternalDataSourceManager::TYqlConclusionStatus> TExternalDataSourceManager::DoModify(const NYql::TObjectSettingsImpl& settings,
                                                                                                           const ui32 nodeId,
                                                                                                           const NMetadata::IClassBehaviour::TPtr& manager,
                                                                                                           TInternalModificationContext& context) const {
    Y_UNUSED(nodeId, manager, settings);
    try {
        switch (context.GetActivityType()) {
            case EActivityType::Upsert:
            case EActivityType::Undefined:
            case EActivityType::Alter:
                return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("not implemented"));
            case EActivityType::Create:
                return CreateExternalDataSource(settings, context);
            case EActivityType::Drop:
                return DropExternalDataSource(settings, context);
        }
    } catch (...) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(CurrentExceptionMessage()));
    }
}

NThreading::TFuture<TExternalDataSourceManager::TYqlConclusionStatus> TExternalDataSourceManager::CreateExternalDataSource(const NYql::TCreateObjectSettings& settings,
                                                                                                                           TInternalModificationContext& context) const {
    using TRequest = TEvTxUserProxy::TEvProposeTransaction;

    NKikimrSchemeOp::TModifyScheme schemeTx;
    FillCreateExternalDataSourceCommand(schemeTx, settings, context);

    auto validationFuture = ValidateCreateExternalDatasource(schemeTx.GetCreateExternalDataSource(), context);

    return validationFuture.Apply([context, schemeTx](const NThreading::TFuture<TExternalDataSourceManager::TYqlConclusionStatus>& f) {
        if (const auto& value = f.GetValue(); value.IsFail()) {
            return NThreading::MakeFuture(value);
        }

        auto ev = MakeHolder<TRequest>();
        ev->Record.SetDatabaseName(context.GetExternalData().GetDatabase());
        if (context.GetExternalData().GetUserToken()) {
            ev->Record.SetUserToken(context.GetExternalData().GetUserToken()->GetSerializedToken());
        }

        *ev->Record.MutableTransaction()->MutableModifyScheme() = schemeTx;

        return SendSchemeRequest(ev.Release(), context.GetExternalData().GetActorSystem(), schemeTx.GetFailedOnAlreadyExists(), schemeTx.GetSuccessOnNotExist());
    });
}

NThreading::TFuture<TExternalDataSourceManager::TYqlConclusionStatus> TExternalDataSourceManager::DropExternalDataSource(const NYql::TDropObjectSettings& settings,
                                                                                                                         TInternalModificationContext& context) const {
    using TRequest = TEvTxUserProxy::TEvProposeTransaction;

    auto ev = MakeHolder<TRequest>();
    ev->Record.SetDatabaseName(context.GetExternalData().GetDatabase());
    if (context.GetExternalData().GetUserToken()) {
        ev->Record.SetUserToken(context.GetExternalData().GetUserToken()->GetSerializedToken());
    }

    auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
    FillDropExternalDataSourceCommand(schemeTx, settings, context);

    return SendSchemeRequest(ev.Release(), context.GetExternalData().GetActorSystem(), schemeTx.GetFailedOnAlreadyExists(), schemeTx.GetSuccessOnNotExist());
}

TExternalDataSourceManager::TYqlConclusionStatus TExternalDataSourceManager::DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings,
    const NMetadata::IClassBehaviour::TPtr& manager, NMetadata::NModifications::IOperationsManager::TInternalModificationContext& context) const {
    Y_UNUSED(manager);

    try {
        switch (context.GetActivityType()) {
            case EActivityType::Undefined:
                return TYqlConclusionStatus::Fail("Undefined operation for EXTERNAL_DATA_SOURCE object");
            case EActivityType::Upsert:
                return TYqlConclusionStatus::Fail("Upsert operation for EXTERNAL_DATA_SOURCE objects is not implemented");
            case EActivityType::Alter:
                return TYqlConclusionStatus::Fail("Alter operation for EXTERNAL_DATA_SOURCE objects is not implemented");
            case EActivityType::Create:
                PrepareCreateExternalDataSource(schemeOperation, settings, context);
                break;
            case EActivityType::Drop:
                PrepareDropExternalDataSource(schemeOperation, settings, context);
                break;
        }
        return TYqlConclusionStatus::Success();
    } catch (...) {
        return TYqlConclusionStatus::Fail(CurrentExceptionMessage());
    }
}

void TExternalDataSourceManager::PrepareCreateExternalDataSource(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TCreateObjectSettings& settings,
                                                                                                             TInternalModificationContext& context) const {
    FillCreateExternalDataSourceCommand(*schemeOperation.MutableCreateExternalDataSource(), settings, context);
}

void TExternalDataSourceManager::PrepareDropExternalDataSource(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TDropObjectSettings& settings,
                                                                                                           TInternalModificationContext& context) const {
    FillDropExternalDataSourceCommand(*schemeOperation.MutableDropExternalDataSource(), settings, context);
}

NThreading::TFuture<NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus> TExternalDataSourceManager::ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation,
        const ui32 /*nodeId*/, const NMetadata::IClassBehaviour::TPtr& /*manager*/, const IOperationsManager::TExternalModificationContext& context) const {
    using TRequest = TEvTxUserProxy::TEvProposeTransaction;

    auto validationFuture = NThreading::MakeFuture(TExternalDataSourceManager::TYqlConclusionStatus::Success());
    NKikimrSchemeOp::TModifyScheme schemeTx;
    switch (schemeOperation.GetOperationCase()) {
        case NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource: {
            const auto& createExternalDataSource = schemeOperation.GetCreateExternalDataSource();
            validationFuture = ValidateCreateExternalDatasource(createExternalDataSource.GetCreateExternalDataSource(), context);
            schemeTx.CopyFrom(createExternalDataSource);
            break;
        }
        case NKqpProto::TKqpSchemeOperation::kAlterExternalDataSource:
            schemeTx.CopyFrom(schemeOperation.GetAlterExternalDataSource());
            break;
        case NKqpProto::TKqpSchemeOperation::kDropExternalDataSource:
            schemeTx.CopyFrom(schemeOperation.GetDropExternalDataSource());
            break;
        default:
            return NThreading::MakeFuture(NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus::Fail(
                TStringBuilder() << "Execution of prepare operation for EXTERNAL_DATA_SOURCE object: unsupported operation: " << int(schemeOperation.GetOperationCase())));
    }

    return validationFuture.Apply([context, schemeTx](const NThreading::TFuture<TExternalDataSourceManager::TYqlConclusionStatus>& f) {
        if (const auto& value = f.GetValue(); value.IsFail()) {
            return NThreading::MakeFuture(value);
        }

        auto ev = MakeHolder<TRequest>();
        ev->Record.SetDatabaseName(context.GetDatabase());
        if (context.GetUserToken()) {
            ev->Record.SetUserToken(context.GetUserToken()->GetSerializedToken());
        }

        *ev->Record.MutableTransaction()->MutableModifyScheme() = schemeTx;

        return SendSchemeRequest(ev.Release(), context.GetActorSystem(), schemeTx.GetFailedOnAlreadyExists(), schemeTx.GetSuccessOnNotExist());
    });
}

}
