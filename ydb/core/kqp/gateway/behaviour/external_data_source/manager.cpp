
#include "manager.h"

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/kqp/gateway/actors/scheme.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/base/path.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>

#include <util/string/type.h>

namespace NKikimr::NKqp {

namespace {

TString GetOrDefault(const NYql::TCreateObjectSettings& container, const TString& key, const TString& defaultValue = TString{}) {
    auto fValue = container.GetFeaturesExtractor().Extract(key);
    return fValue ? *fValue : defaultValue;
}

void FillCreateExternalDataSourceDesc(NKikimrSchemeOp::TExternalDataSourceDescription& externaDataSourceDesc,
                                      const TString& name,
                                      const NYql::TCreateObjectSettings& settings) {
    externaDataSourceDesc.SetName(name);
    externaDataSourceDesc.SetSourceType(GetOrDefault(settings, "source_type"));
    externaDataSourceDesc.SetLocation(GetOrDefault(settings, "location"));
    externaDataSourceDesc.SetInstallation(GetOrDefault(settings, "installation"));

    TString authMethod = GetOrDefault(settings, "auth_method");
    if (authMethod == "NONE") {
        externaDataSourceDesc.MutableAuth()->MutableNone();
    } else if (authMethod == "SERVICE_ACCOUNT") {
        auto& sa = *externaDataSourceDesc.MutableAuth()->MutableServiceAccount();
        sa.SetId(GetOrDefault(settings, "service_account_id"));
        sa.SetSecretName(GetOrDefault(settings, "service_account_secret_name"));
    }
}

NThreading::TFuture<TExternalDataSourceManager::TYqlConclusionStatus> SendSchemeRequest(TEvTxUserProxy::TEvProposeTransaction* request, TActorSystem* actorSystem, bool failedOnAlreadyExists = false)
{
    auto promiseScheme = NThreading::NewPromise<NKqp::TSchemeOpRequestHandler::TResult>();
    IActor* requestHandler = new TSchemeOpRequestHandler(request, promiseScheme, failedOnAlreadyExists);
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

}

NThreading::TFuture<TExternalDataSourceManager::TYqlConclusionStatus> TExternalDataSourceManager::DoModify(const NYql::TObjectSettingsImpl& settings,
                                                                                                           const ui32 nodeId,
                                                                                                           NMetadata::IClassBehaviour::TPtr manager,
                                                                                                           TInternalModificationContext& context) const {
        Y_UNUSED(nodeId, manager, settings);
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
}

NThreading::TFuture<TExternalDataSourceManager::TYqlConclusionStatus> TExternalDataSourceManager::CreateExternalDataSource(const NYql::TObjectSettingsImpl& settings,
                                                                                                                           TInternalModificationContext& context) const {
    using TRequest = TEvTxUserProxy::TEvProposeTransaction;

    try {
        auto* actorSystem = context.GetExternalData().GetActorSystem();
        if (!actorSystem) {
            return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("This place needs an actor system. Please contact internal support"));
        }

        if (!AppData(actorSystem)->FeatureFlags.GetEnableExternalDataSources()) {
            return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("External data sources are disabled. Please contact your system administrator to enable it"));
        }

        std::pair<TString, TString> pathPair;
        {
            TString error;
            if (!TrySplitPathByDb(settings.GetObjectId(), context.GetExternalData().GetDatabase(), pathPair, error)) {
                return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(error));
            }
        }

        auto ev = MakeHolder<TRequest>();
        ev->Record.SetDatabaseName(context.GetExternalData().GetDatabase());
        if (context.GetExternalData().GetUserToken()) {
            ev->Record.SetUserToken(context.GetExternalData().GetUserToken()->GetSerializedToken());
        }
        auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
        schemeTx.SetWorkingDir(pathPair.first);
        schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateExternalDataSource);

        NKikimrSchemeOp::TExternalDataSourceDescription& dataSourceDesc = *schemeTx.MutableCreateExternalDataSource();
        FillCreateExternalDataSourceDesc(dataSourceDesc, pathPair.second, settings);
        return SendSchemeRequest(ev.Release(), actorSystem, true);
    } catch (...) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(CurrentExceptionMessage()));
    }
}

NThreading::TFuture<TExternalDataSourceManager::TYqlConclusionStatus> TExternalDataSourceManager::DropExternalDataSource(const NYql::TObjectSettingsImpl& settings,
                                                                                                                         TInternalModificationContext& context) const {
    using TRequest = TEvTxUserProxy::TEvProposeTransaction;

    try {
        auto* actorSystem = context.GetExternalData().GetActorSystem();
        if (!actorSystem) {
            return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("This place needs an actor system. Please contact internal support"));
        }

        if (!AppData(actorSystem)->FeatureFlags.GetEnableExternalDataSources()) {
            return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail("External data sources are disabled. Please contact your system administrator to enable it"));
        }

        std::pair<TString, TString> pathPair;
        {
            TString error;
            if (!NYql::IKikimrGateway::TrySplitTablePath(settings.GetObjectId(), pathPair, error)) {
                return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(error));
            }
        }

        auto ev = MakeHolder<TRequest>();
        ev->Record.SetDatabaseName(context.GetExternalData().GetDatabase());
        if (context.GetExternalData().GetUserToken()) {
            ev->Record.SetUserToken(context.GetExternalData().GetUserToken()->GetSerializedToken());
        }
        auto& schemeTx = *ev->Record.MutableTransaction()->MutableModifyScheme();
        schemeTx.SetWorkingDir(pathPair.first);
        schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropExternalDataSource);

        NKikimrSchemeOp::TDrop& drop = *schemeTx.MutableDrop();
        drop.SetName(pathPair.second);
        return SendSchemeRequest(ev.Release(), actorSystem);
    }
    catch (...) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(CurrentExceptionMessage()));
    }
}

}
