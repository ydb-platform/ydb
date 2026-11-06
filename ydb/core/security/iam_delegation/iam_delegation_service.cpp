#include "iam_delegation_service.h"
#include "iam_actor_base.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/ycloud/api/operation_service.h>
#include <ydb/library/ycloud/api/service_control_service.h>
#include <ydb/library/ycloud/impl/operation_service.h>
#include <ydb/library/ycloud/impl/service_control_service.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::IAM_DELEGATION

namespace NKikimr::NIamDelegation {

using namespace NActors;
using TOperation = ydb::yc::priv::operation::Operation;

class TIamDelegationService : public TIamActorBase<TIamDelegationService> {
private:
    using TBase = TIamActorBase<TIamDelegationService>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::IAM_DELEGATION_SERVICE_ACTOR;
    }

    explicit TIamDelegationService(const TIamDelegationSettings& settings)
        : TBase(settings)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        // Token manager has no unsubscribe API: this node service is the sole subscriber, regardless
        // of the number of concurrent requests or retries. The initializer selects its factory.
        Send(MakeTokenManagerID(), new TEvTokenManager::TEvSubscribeUpdateToken(Settings.Config.GetSystemTokenName()),
            IEventHandle::FlagTrackDelivery);

        {
            NCloud::TServiceControlServiceSettings clientSettings(Settings.Config.GetServiceControlEndpoint(), "ydb-iam-delegation");
            clientSettings.EnableSsl = Settings.Config.GetEnableSsl();
            clientSettings.RequestTimeoutMs = Settings.RequestTimeout.MilliSeconds();
            ServiceControlClient = Register(NCloud::CreateServiceControlService(clientSettings));
        }
        {
            NCloud::TOperationServiceSettings clientSettings(Settings.Config.GetServiceControlEndpoint(), "ydb-iam-delegation");
            clientSettings.EnableSsl = Settings.Config.GetEnableSsl();
            clientSettings.RequestTimeoutMs = Settings.RequestTimeout.MilliSeconds();
            OperationClient = Register(NCloud::CreateOperationService(clientSettings));
        }

        YDB_LOG_INFO("Delegation service started",
            {"serviceId", Settings.Config.GetServiceId()},
            {"microserviceId", Settings.Config.GetMicroserviceId()},
            {"serviceControlEndpoint", Settings.Config.GetServiceControlEndpoint()}
        );
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvIamDelegation::TEvSetupDelegation, HandleSetup);
        hFunc(TEvIamDelegation::TEvRevokeDelegation, HandleRevoke);
        // late replies after a timeout
        IgnoreFunc(NCloud::TEvServiceControlService::TEvSetupDelegationResponse);
        IgnoreFunc(NCloud::TEvServiceControlService::TEvRevokeDelegationResponse);
        IgnoreFunc(NCloud::TEvOperationService::TEvGetOperationResponse);
        hFunc(TEvTokenManager::TEvUpdateToken, HandleToken);
        hFunc(TEvents::TEvUndelivered, HandleUndelivered);
        cFunc(TEvents::TEvPoison::EventType, BeginShutdown);
    )

    STFUNC(StateDying) {
        Y_UNUSED(ev); // PassAway may be waiting for coroutine tasks
    }

private:
    void HandleToken(TEvTokenManager::TEvUpdateToken::TPtr& ev) {
        const auto& update = *ev->Get();
        if (update.Id != Settings.Config.GetSystemTokenName()) {
            return;
        }
        Credentials.Status = update.Status;
        Credentials.Token = update.Status.Code == TEvTokenManager::TStatus::ECode::SUCCESS ? update.Token : TString();
        Credentials.Updated.NotifyAll();
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->SourceType == TEvTokenManager::TEvSubscribeUpdateToken::EventType) {
            Credentials.Token.clear();
            Credentials.Status = {TEvTokenManager::TStatus::ECode::ERROR, "token manager is not available"};
            Credentials.Updated.NotifyAll();
        }
        // Timed-out client waits no longer intercept their undelivered replies.
    }

    void BeginShutdown() {
        Become(&TThis::StateDying);
        Send(ServiceControlClient, new TEvents::TEvPoison());
        Send(OperationClient, new TEvents::TEvPoison());
        PassAway();
    }

    // top-level coroutine: one task per request
    void HandleSetup(TEvIamDelegation::TEvSetupDelegation::TPtr ev) {
        const TActorId replyTo = ev->Sender;
        const ui64 cookie = ev->Cookie;
        const TDelegationSpec spec = ev->Get()->Spec;
        const TString subjectId = ev->Get()->SubjectId;
        ev.Reset();

        YDB_LOG_INFO("SetupDelegation", {"spec", spec.ToString()}, {"subjectId", subjectId});
        TDelegationResult result = co_await Run(spec, subjectId, ECall::Setup);
        if (result.IsSuccess()) {
            YDB_LOG_INFO("SetupDelegation done", {"spec", spec.ToString()});
        } else {
            YDB_LOG_WARN("SetupDelegation failed", {"spec", spec.ToString()}, {"status", result.Status}, {"issues", result.Issues.ToOneLineString()});
        }
        Send(replyTo, new TEvIamDelegation::TEvSetupDelegationResult(std::move(result)), 0, cookie);
    }

    void HandleRevoke(TEvIamDelegation::TEvRevokeDelegation::TPtr ev) {
        const TActorId replyTo = ev->Sender;
        const ui64 cookie = ev->Cookie;
        const TDelegationSpec spec = ev->Get()->Spec;
        ev.Reset();

        YDB_LOG_INFO("RevokeDelegation", {"spec", spec.ToString()});
        TDelegationResult result = co_await Run(spec, TString(), ECall::Revoke);
        if (result.IsSuccess()) {
            YDB_LOG_INFO("RevokeDelegation done", {"spec", spec.ToString()});
        } else {
            YDB_LOG_WARN("RevokeDelegation failed", {"spec", spec.ToString()}, {"status", result.Status}, {"issues", result.Issues.ToOneLineString()});
        }
        Send(replyTo, new TEvIamDelegation::TEvRevokeDelegationResult(std::move(result)), 0, cookie);
    }

    enum class ECall {
        Setup,
        Revoke,
    };

    // Shared scheduling never replays a mutation. A request ID is correlation
    // metadata; IAM does not guarantee idempotency for it.
    template<class TRequest, class TResponse>
    async<std::optional<TOperation>> CallOnce(TActorId client, THolder<TRequest> request,
            TStringBuf method, ECall call, TString operationId = {}, TMonotonic deadline = TMonotonic::Max()) {
        deadline = Min(deadline, TActivationContext::Monotonic() + Settings.RequestRetryTimeout);
        co_await GetIamCallToken(Credentials, deadline);
        const auto version = co_await WithDeadline(deadline, [this]() -> async<ui64> {
            co_return co_await RetryBackoff.Wait(Settings.RequestTimeout * 2 + TDuration::Seconds(1));
        });
        if (!version) {
            throw TIamCallError(Ydb::StatusIds::TIMEOUT) << "IAM service retry deadline exceeded";
        }
        // Re-read after cooldown to observe token rotation. Local credential
        // failures do not assert an IAM service outage.
        const auto dispatchDeadline = Min(deadline,
            TActivationContext::Monotonic() + Settings.RequestTimeout * 2 + TDuration::Seconds(1));
        request->Token = co_await GetIamCallToken(Credentials, dispatchDeadline);
        request->RequestId = CreateGuidAsString();
        CheckIamCallDeadline(dispatchDeadline);
        auto response = co_await WithDeadline(Min(dispatchDeadline,
                TActivationContext::Monotonic() + Settings.RequestTimeout + TDuration::Seconds(1)),
            &IamClientRequest, client, request.Release());
        CheckIamCallDeadline(deadline);
        if (!response) {
            RetryBackoff.OnFailure(*version);
            throw TIamCallError(Ydb::StatusIds::TIMEOUT) << "IAM response was not received; outcome is unknown";
        }
        if ((*response)->GetTypeRewrite() == TEvents::TEvUndelivered::EventType) {
            RetryBackoff.OnFailure(*version);
            throw TIamCallError(Ydb::StatusIds::UNAVAILABLE) << "IAM client is unavailable; outcome is unknown";
        }
        if ((*response)->GetTypeRewrite() != TResponse::EventType) {
            throw TIamCallError(Ydb::StatusIds::INTERNAL_ERROR) << "Unexpected IAM response; outcome is unknown";
        }
        typename TResponse::TPtr typed(reinterpret_cast<TEventHandle<TResponse>*>(response->Release()));
        const auto& status = typed->Get()->Status;
        if (IamServiceHealth(status) == EIamServiceHealth::Healthy) {
            RetryBackoff.OnSuccess(*version);
        } else {
            RetryBackoff.OnFailure(*version);
        }
        if (!status.Ok()) {
            if (status.InternalError) {
                throw TIamCallError(Ydb::StatusIds::UNAVAILABLE) << "IAM client transport failed; outcome is unknown";
            }
            if (operationId.empty() && call == ECall::Revoke && status.GRpcStatusCode == grpc::StatusCode::NOT_FOUND) {
                co_return std::nullopt;
            }
            throw TIamCallError(MapGrpcStatus(status.GRpcStatusCode), static_cast<grpc::StatusCode>(status.GRpcStatusCode))
                << "IAM request failed with gRPC status " << status.GRpcStatusCode << ExplainIamFailure(status, method);
        }
        const auto& operation = typed->Get()->Response;
        if (operation.id().empty() || (!operationId.empty() && operation.id() != operationId)) {
            throw TIamCallError(Ydb::StatusIds::INTERNAL_ERROR) << "IAM returned a missing or mismatched operation ID; outcome is unknown";
        }
        co_return operation;
    }

    template<class TRequest>
    void FillBinding(TRequest& request, const TDelegationSpec& spec) const {
        request.set_service_id(Settings.Config.GetServiceId());
        request.set_microservice_id(Settings.Config.GetMicroserviceId());
        request.mutable_resource()->set_id(spec.CloudId);
        request.mutable_resource()->set_type(Settings.Config.GetResourceType());
        request.set_target_service_account_id(spec.ServiceAccountId);
        request.mutable_referrer()->set_id(spec.ReferrerId);
        request.mutable_referrer()->set_type(Settings.ReferrerType);
        request.set_with_references(false);
    }

    async<TDelegationResult> Run(TDelegationSpec spec, TString subjectId, ECall call) {
        try {
            if (spec.ServiceAccountId.empty() || spec.CloudId.empty() || spec.ReferrerId.empty()
                    || (call == ECall::Setup && subjectId.empty())) {
                co_return TDelegationResult::Error(Ydb::StatusIds::BAD_REQUEST, "Complete IAM binding and setup subject are required");
            }
            std::optional<TOperation> operation;
            const TStringBuf mutationMethod = call == ECall::Setup ? "ServiceControl.SetupDelegation" : "ServiceControl.RevokeDelegation";
            if (call == ECall::Setup) {
                auto request = MakeHolder<NCloud::TEvServiceControlService::TEvSetupDelegationRequest>();
                FillBinding(request->Request, spec);
                request->Request.set_on_behalf_of_subject_id(subjectId);
                operation = co_await CallOnce<NCloud::TEvServiceControlService::TEvSetupDelegationRequest,
                    NCloud::TEvServiceControlService::TEvSetupDelegationResponse>(ServiceControlClient, std::move(request), mutationMethod, call);
            } else {
                auto request = MakeHolder<NCloud::TEvServiceControlService::TEvRevokeDelegationRequest>();
                FillBinding(request->Request, spec);
                operation = co_await CallOnce<NCloud::TEvServiceControlService::TEvRevokeDelegationRequest,
                    NCloud::TEvServiceControlService::TEvRevokeDelegationResponse>(ServiceControlClient, std::move(request), mutationMethod, call);
            }
            if (!operation) {
                co_return TDelegationResult::Success();
            }
            const auto pollDeadline = TActivationContext::Monotonic() + Settings.OperationPollTimeout;
            while (!operation->done()) {
                CheckIamCallDeadline(pollDeadline);
                co_await AsyncSleepUntil(Min(pollDeadline, TActivationContext::Monotonic() + Settings.OperationPollInterval));
                CheckIamCallDeadline(pollDeadline);
                auto request = MakeHolder<NCloud::TEvOperationService::TEvGetOperationRequest>();
                request->Request.set_operation_id(operation->id());
                try {
                    operation = co_await CallOnce<NCloud::TEvOperationService::TEvGetOperationRequest,
                        NCloud::TEvOperationService::TEvGetOperationResponse>(OperationClient, std::move(request),
                            "OperationService.Get", call, operation->id(), pollDeadline);
                } catch (const TIamCallError& error) {
                    // Repeating a read of a known operation is safe. Transient
                    // failures share the same gate; no per-operation counter.
                    if (error.Status != Ydb::StatusIds::UNAVAILABLE && error.Status != Ydb::StatusIds::TIMEOUT
                        && error.Status != Ydb::StatusIds::OVERLOADED && error.Status != Ydb::StatusIds::INTERNAL_ERROR) {
                        throw;
                    }
                    CheckIamCallDeadline(pollDeadline);
                }
            }
            if (operation->has_error()) {
                if (operation->error().code() == grpc::StatusCode::OK) {
                    throw TIamCallError(Ydb::StatusIds::INTERNAL_ERROR) << "IAM returned a success-coded operation error; outcome is unknown";
                }
                throw TIamCallError(MapGrpcStatus(operation->error().code()))
                    << "IAM operation failed with gRPC status " << operation->error().code()
                    << ExplainIamFailure(operation->error(), mutationMethod);
            }
            co_return TDelegationResult::Success();
        } catch (const TIamCallError& error) {
            co_return TDelegationResult::Error(error.Status, error.what());
        } catch (const std::exception&) {
            co_return TDelegationResult::Error(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected IAM operation failure");
        }
    }

private:
    TActorId ServiceControlClient;
    TActorId OperationClient;
};

IActor* CreateIamDelegationService(const TIamDelegationSettings& settings) {
    return new TIamDelegationService(settings);
}

} // namespace NKikimr::NIamDelegation
