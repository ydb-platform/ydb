#include <ydb/core/fq/libs/compute/common/run_actor_params.h>
#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NFq {

using namespace NActors;
using namespace NFq;

class TYdbConnectorActor : public NActors::TActorBootstrapped<TYdbConnectorActor> {
public:
    explicit TYdbConnectorActor(const TRunActorParams& params)
        : YqSharedResources(params.YqSharedResources)
        , CredentialsProviderFactory(params.CredentialsProviderFactory)
        , ComputeConnection(params.ComputeConnection)
    {}

    void Bootstrap() {
        auto querySettings = NFq::GetClientSettings<NYdb::NQuery::TClientSettings>(ComputeConnection, CredentialsProviderFactory);
        QueryClient = std::make_unique<NYdb::NQuery::TQueryClient>(YqSharedResources->UserSpaceYdbDriver, querySettings);
        auto operationSettings = NFq::GetClientSettings<NYdb::TCommonClientSettings>(ComputeConnection, CredentialsProviderFactory);
        OperationClient = std::make_unique<NYdb::NOperation::TOperationClient>(YqSharedResources->UserSpaceYdbDriver, operationSettings);
        Become(&TYdbConnectorActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvExecuteScriptRequest, Handle);
        hFunc(TEvYdbCompute::TEvGetOperationRequest, Handle);
        hFunc(TEvYdbCompute::TEvFetchScriptResultRequest, Handle);
        hFunc(TEvYdbCompute::TEvCancelOperationRequest, Handle);
        hFunc(TEvYdbCompute::TEvForgetOperationRequest, Handle);
        cFunc(NActors::TEvents::TEvPoisonPill::EventType, PassAway);
    )

    void Handle(const TEvYdbCompute::TEvExecuteScriptRequest::TPtr& ev) {
        const auto& event = *ev->Get();
        NYdb::NQuery::TExecuteScriptSettings settings;
        settings.ResultsTtl(event.ResultTtl);
        settings.OperationTimeout(event.OperationTimeout);
        settings.Syntax(event.Syntax);
        settings.ExecMode(event.ExecMode);
        QueryClient
            ->ExecuteScript(event.Sql, settings)
            .Apply([actorSystem = NActors::TActivationContext::ActorSystem(), recipient = ev->Sender, cookie = ev->Cookie](auto future) {
                try {
                    auto response = future.ExtractValueSync();
                    if (response.Status().IsSuccess()) {
                        actorSystem->Send(recipient, new TEvYdbCompute::TEvExecuteScriptResponse(response.Id(), response.Metadata().ExecutionId), 0, cookie);
                    } else {
                        actorSystem->Send(recipient, new TEvYdbCompute::TEvExecuteScriptResponse(response.Status().GetIssues(), response.Status().GetStatus()), 0, cookie);
                    }
                } catch (...) {
                    actorSystem->Send(recipient, new TEvYdbCompute::TEvExecuteScriptResponse(NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}, NYdb::EStatus::GENERIC_ERROR), 0, cookie);
                }
            });
    }

    void Handle(const TEvYdbCompute::TEvGetOperationRequest::TPtr& ev) {
        OperationClient
            ->Get<NYdb::NQuery::TScriptExecutionOperation>(ev->Get()->OperationId)
            .Apply([actorSystem = NActors::TActivationContext::ActorSystem(), recipient = ev->Sender, cookie = ev->Cookie](auto future) {
                try {
                    auto response = future.ExtractValueSync();
                    if (response.Status().IsSuccess()) {
                        actorSystem->Send(recipient, new TEvYdbCompute::TEvGetOperationResponse(response.Metadata().ExecStatus, response.Metadata().ResultSetsMeta, response.Status().GetIssues()), 0, cookie);
                    } else {
                        actorSystem->Send(recipient, new TEvYdbCompute::TEvGetOperationResponse(response.Status().GetIssues(), response.Status().GetStatus()), 0, cookie);
                    }
                } catch (...) {
                    actorSystem->Send(recipient, new TEvYdbCompute::TEvGetOperationResponse(NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}, NYdb::EStatus::GENERIC_ERROR), 0, cookie);
                }
            });
    }

    void Handle(const TEvYdbCompute::TEvFetchScriptResultRequest::TPtr& ev) {
        NYdb::NQuery::TFetchScriptResultsSettings settings;
        settings.FetchToken(ev->Get()->FetchToken);
        QueryClient
            ->FetchScriptResults(ev->Get()->OperationId, ev->Get()->ResultSetId, settings)
            .Apply([actorSystem = NActors::TActivationContext::ActorSystem(), recipient = ev->Sender, cookie = ev->Cookie](auto future) {
                try {
                    auto response = future.ExtractValueSync();
                    if (response.IsSuccess()) {
                        actorSystem->Send(recipient, new TEvYdbCompute::TEvFetchScriptResultResponse(response.ExtractResultSet(), response.GetNextFetchToken()), 0, cookie);
                    } else {
                        actorSystem->Send(recipient, new TEvYdbCompute::TEvFetchScriptResultResponse(response.GetIssues(), response.GetStatus()), 0, cookie);
                    }
                } catch (...) {
                    actorSystem->Send(recipient, new TEvYdbCompute::TEvFetchScriptResultResponse(NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}, NYdb::EStatus::GENERIC_ERROR), 0, cookie);
                }
            });
    }

    void Handle(const TEvYdbCompute::TEvCancelOperationRequest::TPtr& ev) {
        OperationClient
            ->Cancel(ev->Get()->OperationId)
            .Apply([actorSystem = NActors::TActivationContext::ActorSystem(), recipient = ev->Sender, cookie = ev->Cookie](auto future) {
                try {
                    auto response = future.ExtractValueSync();
                    actorSystem->Send(recipient, new TEvYdbCompute::TEvCancelOperationResponse(response.GetIssues(), response.GetStatus()), 0, cookie);
                } catch (...) {
                    actorSystem->Send(recipient, new TEvYdbCompute::TEvCancelOperationResponse(NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}, NYdb::EStatus::GENERIC_ERROR), 0, cookie);
                }
            });
    }

    void Handle(const TEvYdbCompute::TEvForgetOperationRequest::TPtr& ev) {
        OperationClient
            ->Forget(ev->Get()->OperationId)
            .Apply([actorSystem = NActors::TActivationContext::ActorSystem(), recipient = ev->Sender, cookie = ev->Cookie](auto future) {
                try {
                    auto response = future.ExtractValueSync();
                    actorSystem->Send(recipient, new TEvYdbCompute::TEvForgetOperationResponse(response.GetIssues(), response.GetStatus()), 0, cookie);
                } catch (...) {
                    actorSystem->Send(recipient, new TEvYdbCompute::TEvForgetOperationResponse(NYql::TIssues{NYql::TIssue{CurrentExceptionMessage()}}, NYdb::EStatus::GENERIC_ERROR), 0, cookie);
                }
            });
    }

private:
    TYqSharedResources::TPtr YqSharedResources;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory;
    NConfig::TYdbStorageConfig ComputeConnection;
    std::unique_ptr<NYdb::NQuery::TQueryClient> QueryClient;
    std::unique_ptr<NYdb::NOperation::TOperationClient> OperationClient;
};

std::unique_ptr<NActors::IActor> CreateConnectorActor(const TRunActorParams& params) {
    return std::make_unique<TYdbConnectorActor>(params);
}

}
