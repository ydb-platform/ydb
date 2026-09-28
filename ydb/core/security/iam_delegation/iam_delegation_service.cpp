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

    TIamDelegationService(const TIamDelegationSettings& settings, const TActorId& systemTokenService)
        : TBase(settings, systemTokenService)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);

        {
            NCloud::TServiceControlServiceSettings clientSettings(Settings.ServiceControlEndpoint, "ydb-iam-delegation");
            clientSettings.EnableSsl = Settings.EnableSsl;
            clientSettings.RequestTimeoutMs = Settings.RequestTimeout.MilliSeconds();
            ServiceControlClient = Register(NCloud::CreateServiceControlService(clientSettings));
        }
        {
            NCloud::TOperationServiceSettings clientSettings(Settings.ServiceControlEndpoint, "ydb-iam-delegation");
            clientSettings.EnableSsl = Settings.EnableSsl;
            clientSettings.RequestTimeoutMs = Settings.RequestTimeout.MilliSeconds();
            OperationClient = Register(NCloud::CreateOperationService(clientSettings));
        }

        YDB_LOG_INFO("Delegation service started",
            {"serviceId", Settings.ServiceId},
            {"microserviceId", Settings.MicroserviceId},
            {"serviceControlEndpoint", Settings.ServiceControlEndpoint}
        );
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvIamDelegation::TEvSetupDelegation, HandleSetup);
        hFunc(TEvIamDelegation::TEvRevokeDelegation, HandleRevoke);
        // late replies after a timeout
        IgnoreFunc(NCloud::TEvServiceControlService::TEvSetupDelegationResponse);
        IgnoreFunc(NCloud::TEvServiceControlService::TEvRevokeDelegationResponse);
        IgnoreFunc(NCloud::TEvOperationService::TEvGetOperationResponse);
        IgnoreFunc(TEvIamDelegation::TEvSystemTokenReady);
        IgnoreFunc(TEvents::TEvUndelivered);
        cFunc(TEvents::TEvPoison::EventType, BeginShutdown);
    )

    STFUNC(StateDying) {
        Y_UNUSED(ev); // PassAway may be waiting for coroutine tasks
    }

private:
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

    async<TDelegationResult> Run(TDelegationSpec spec, TString subjectId, ECall call) {
        try {
            std::optional<TOperation> operation;
            switch (call) {
                case ECall::Setup:
                    operation = co_await CallSetup(spec, subjectId);
                    break;
                case ECall::Revoke:
                    operation = co_await CallRevoke(spec);
                    break;
            }
            if (operation) {
                co_await WaitOperation(*operation, call == ECall::Revoke ? "RevokeDelegation" : "SetupDelegation");
            }
            co_return TDelegationResult::Success();
        } catch (const TIamCallError& e) {
            co_return TDelegationResult::Error(e.Status, e.what());
        } catch (const std::exception& e) {
            co_return TDelegationResult::Error(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "unexpected exception: " << e.what());
        }
    }

    async<TOperation> CallSetup(TDelegationSpec spec, TString subjectId) {
        auto response = co_await CallWithRetry<NCloud::TEvServiceControlService::TEvSetupDelegationRequest, NCloud::TEvServiceControlService::TEvSetupDelegationResponse>(
            ServiceControlClient, "ServiceControl.SetupDelegation",
            [&](yandex::cloud::priv::iam::v1::SetupDelegationRequest& request) {
                request.set_service_id(Settings.ServiceId);
                request.set_microservice_id(Settings.MicroserviceId);
                request.mutable_resource()->set_id(spec.CloudId);
                request.mutable_resource()->set_type(Settings.ResourceType);
                request.set_target_service_account_id(spec.ServiceAccountId);
                request.mutable_referrer()->set_id(spec.ReferrerId);
                request.mutable_referrer()->set_type(Settings.ReferrerType);
                request.set_on_behalf_of_subject_id(subjectId);
                request.set_with_references(false);
            });
        co_return response->Get()->Response;
    }

    // Returns nothing when the delegation does not exist any more (NOT_FOUND).
    async<std::optional<TOperation>> CallRevoke(TDelegationSpec spec) {
        auto response = co_await CallWithRetry<NCloud::TEvServiceControlService::TEvRevokeDelegationRequest, NCloud::TEvServiceControlService::TEvRevokeDelegationResponse>(
            ServiceControlClient, "ServiceControl.RevokeDelegation",
            [&](yandex::cloud::priv::iam::v1::RevokeDelegationRequest& request) {
                request.set_service_id(Settings.ServiceId);
                request.set_microservice_id(Settings.MicroserviceId);
                request.mutable_resource()->set_id(spec.CloudId);
                request.mutable_resource()->set_type(Settings.ResourceType);
                request.set_target_service_account_id(spec.ServiceAccountId);
                request.mutable_referrer()->set_id(spec.ReferrerId);
                request.mutable_referrer()->set_type(Settings.ReferrerType);
                request.set_with_references(false);
            },
            ENotFound::IsAbsent);
        if (!response) {
            YDB_LOG_WARN("RevokeDelegation: delegation not found, treating as revoked", {"spec", spec.ToString()});
            co_return std::nullopt;
        }
        co_return response->Get()->Response;
    }

    async<void> WaitOperation(TOperation operation, TStringBuf method) {
        const TMonotonic deadline = TActivationContext::Monotonic() + Settings.OperationPollTimeout;
        while (!operation.done()) {
            if (TActivationContext::Monotonic() >= deadline) {
                throw TIamCallError(Ydb::StatusIds::TIMEOUT) << method << ": operation " << operation.id() << " is not done after " << Settings.OperationPollTimeout;
            }
            YDB_LOG_DEBUG("Waiting for operation", {"method", method}, {"operationId", operation.id()});
            co_await AsyncSleepFor(Settings.OperationPollInterval);
            const TString operationId = operation.id();
            auto response = co_await CallWithRetry<NCloud::TEvOperationService::TEvGetOperationRequest, NCloud::TEvOperationService::TEvGetOperationResponse>(
                OperationClient, "OperationService.Get",
                [&](yandex::cloud::priv::iam::v1::GetOperationRequest& request) {
                    request.set_operation_id(operationId);
                });
            operation = response->Get()->Response;
        }
        if (operation.has_error()) {
            throw TIamCallError(MapGrpcStatus(operation.error().code())) << method << ": operation " << operation.id() << " failed: " << operation.error().message() << ExplainIamFailure(operation.error());
        }
    }

private:
    TActorId ServiceControlClient;
    TActorId OperationClient;
};

IActor* CreateIamDelegationService(const TIamDelegationSettings& settings, const TActorId& systemTokenService) {
    return new TIamDelegationService(settings, systemTokenService);
}

} // namespace NKikimr::NIamDelegation
