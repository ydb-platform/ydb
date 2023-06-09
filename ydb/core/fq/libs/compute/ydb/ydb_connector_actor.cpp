#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/hfunc.h>
#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/compute/common/run_actor_params.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

namespace NFq {

using namespace NActors;
using namespace NFq;

class TYdbConnectorActor : public NActors::TActorBootstrapped<TYdbConnectorActor> {
public:
    explicit TYdbConnectorActor(const TRunActorParams& params)
        : Params(params)
    {}

    void Bootstrap() {
        auto querySettings = NFq::GetClientSettings<NYdb::NQuery::TClientSettings>(Params.Config.GetCompute().GetYdb().GetConnection(), Params.CredentialsProviderFactory);
        QueryClient = std::make_unique<NYdb::NQuery::TQueryClient>(Params.YqSharedResources->UserSpaceYdbDriver, querySettings);
        auto operationSettings = NFq::GetClientSettings<NYdb::TCommonClientSettings>(Params.Config.GetCompute().GetYdb().GetConnection(), Params.CredentialsProviderFactory);
        OperationClient = std::make_unique<NYdb::NOperation::TOperationClient>(Params.YqSharedResources->UserSpaceYdbDriver, operationSettings);
        Become(&TYdbConnectorActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvExecuteScriptRequest, Handle);
        hFunc(TEvPrivate::TEvGetOperationRequest, Handle);
        hFunc(TEvPrivate::TEvFetchScriptResultRequest, Handle);
        hFunc(TEvPrivate::TEvCancelOperationRequest, Handle);
        hFunc(TEvPrivate::TEvForgetOperationRequest, Handle);
        cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
    )

    void Handle(const TEvPrivate::TEvExecuteScriptRequest::TPtr& ev) {
        QueryClient
            ->ExecuteScript(ev->Get()->Sql)
            .Apply([actorSystem = NActors::TActivationContext::ActorSystem(), recipient = ev->Sender, cookie = ev->Cookie](auto future) {
                try {
                    auto response = future.ExtractValueSync();
                    if (response.Status().IsSuccess()) {
                        actorSystem->Send(recipient, new TEvPrivate::TEvExecuteScriptResponse(response.Id(), response.Metadata().ExecutionId), 0, cookie);
                    } else {
                        actorSystem->Send(recipient, new TEvPrivate::TEvExecuteScriptResponse(response.Status().GetIssues(), response.Status().GetStatus()), 0, cookie);
                    }
                } catch (...) {
                    actorSystem->Send(recipient, new TEvPrivate::TEvExecuteScriptResponse(NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}, NYdb::EStatus::GENERIC_ERROR), 0, cookie);
                }
            });
    }

    void Handle(const TEvPrivate::TEvGetOperationRequest::TPtr& ev) {
        OperationClient
            ->Get<NYdb::NQuery::TScriptExecutionOperation>(ev->Get()->OperationId)
            .Apply([actorSystem = NActors::TActivationContext::ActorSystem(), recipient = ev->Sender, cookie = ev->Cookie](auto future) {
                try {
                    auto response = future.ExtractValueSync();
                    if (response.Status().IsSuccess()) {
                        actorSystem->Send(recipient, new TEvPrivate::TEvGetOperationResponse(response.Metadata().ExecStatus, response.Status().GetIssues()), 0, cookie);
                    } else {
                        actorSystem->Send(recipient, new TEvPrivate::TEvGetOperationResponse(response.Status().GetIssues(), response.Status().GetStatus()), 0, cookie);
                    }
                } catch (...) {
                    actorSystem->Send(recipient, new TEvPrivate::TEvGetOperationResponse(NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}, NYdb::EStatus::GENERIC_ERROR), 0, cookie);
                }
            });
    }

    void Handle(const TEvPrivate::TEvFetchScriptResultRequest::TPtr& ev) {
        NYdb::NQuery::TFetchScriptResultsSettings settings;
        settings.RowsOffset(ev->Get()->RowOffset);
        QueryClient
            ->FetchScriptResults(ev->Get()->ExecutionId, settings)
            .Apply([actorSystem = NActors::TActivationContext::ActorSystem(), recipient = ev->Sender, cookie = ev->Cookie](auto future) {
                try {
                    auto response = future.ExtractValueSync();
                    if (response.IsSuccess()) {
                        actorSystem->Send(recipient, new TEvPrivate::TEvFetchScriptResultResponse(response.ExtractResultSet()), 0, cookie);
                    } else {
                        actorSystem->Send(recipient, new TEvPrivate::TEvFetchScriptResultResponse(response.GetIssues(), response.GetStatus()), 0, cookie);
                    }
                } catch (...) {
                    actorSystem->Send(recipient, new TEvPrivate::TEvFetchScriptResultResponse(NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}, NYdb::EStatus::GENERIC_ERROR), 0, cookie);
                }
            });
    }

    void Handle(const TEvPrivate::TEvCancelOperationRequest::TPtr& ev) {
        OperationClient
            ->Cancel(ev->Get()->OperationId)
            .Apply([actorSystem = NActors::TActivationContext::ActorSystem(), recipient = ev->Sender, cookie = ev->Cookie](auto future) {
                try {
                    auto response = future.ExtractValueSync();
                    actorSystem->Send(recipient, new TEvPrivate::TEvCancelOperationResponse(response.GetIssues(), response.GetStatus()), 0, cookie);
                } catch (...) {
                    actorSystem->Send(recipient, new TEvPrivate::TEvCancelOperationResponse(NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}, NYdb::EStatus::GENERIC_ERROR), 0, cookie);
                }
            });
    }

    void Handle(const TEvPrivate::TEvForgetOperationRequest::TPtr& ev) {
        OperationClient
            ->Forget(ev->Get()->OperationId)
            .Apply([actorSystem = NActors::TActivationContext::ActorSystem(), recipient = ev->Sender, cookie = ev->Cookie](auto future) {
                try {
                    auto response = future.ExtractValueSync();
                    actorSystem->Send(recipient, new TEvPrivate::TEvForgetOperationResponse(response.GetIssues(), response.GetStatus()), 0, cookie);
                } catch (...) {
                    actorSystem->Send(recipient, new TEvPrivate::TEvForgetOperationResponse(NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}, NYdb::EStatus::GENERIC_ERROR), 0, cookie);
                }
            });
    }

private:
    TRunActorParams Params;
    std::unique_ptr<NYdb::NQuery::TQueryClient> QueryClient;
    std::unique_ptr<NYdb::NOperation::TOperationClient> OperationClient;
};

std::unique_ptr<NActors::IActor> CreateConnectorActor(const TRunActorParams& params) {
    return std::make_unique<TYdbConnectorActor>(params);
}

}
