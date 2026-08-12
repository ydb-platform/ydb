#include "iam_delegation.h"

#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/ycloud/api/service_control.h>
#include <ydb/library/ycloud/impl/service_control.h>
#include <util/string/ascii.h>

namespace NKikimr::NKqp::NExternalDataSource {
namespace {

using namespace NActors;

enum class EAction {
    Setup,
    Revoke,
};

class TIamDelegationActor final : public TActorBootstrapped<TIamDelegationActor> {
    using TThis = TIamDelegationActor;

public:
    TIamDelegationActor(
        TIamDelegationSettings settings,
        TIamDelegation delegation,
        TString subjectId,
        TString operationToken,
        EAction action,
        NThreading::TPromise<TIamDelegationResult> promise)
        : Settings(std::move(settings))
        , Delegation(std::move(delegation))
        , SubjectId(std::move(subjectId))
        , OperationToken(std::move(operationToken))
        , Action(action)
        , Promise(std::move(promise))
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);

        NCloud::TServiceControlSettings settings;
        settings.Endpoint = Settings.Endpoint;
        settings.EnableSsl = Settings.EnableSsl;
        settings.RequestTimeoutMs = Settings.Timeout.MilliSeconds();
        ServiceControl = Register(NCloud::CreateServiceControl(settings));

        if (OperationToken.empty()) {
            Finish(false, "user IAM token is empty");
            return;
        }
        Send(SelfId(), new TEvPrivate::TEvToken(std::move(OperationToken)));
    }

private:
    struct TEvPrivate {
        enum EEv {
            EvToken = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        struct TEvToken : TEventLocal<TEvToken, EvToken> {
            explicit TEvToken(TString token)
                : Token(std::move(token))
            {}
            TString Token;
        };

    };

    void HandleToken(TEvPrivate::TEvToken::TPtr ev) {
        if (Action == EAction::Setup) {
            co_await Setup(std::move(ev->Get()->Token));
        } else {
            co_await Revoke(std::move(ev->Get()->Token));
        }
    }

    async<void> Setup(TString token) {
        auto ensure = MakeHolder<NCloud::TEvServiceControl::TEvEnsureEnabledRequest>();
        ensure->Token = token;
        ensure->Request = MakeEnsureEnabledRequest(Settings, Delegation);
        Send(ServiceControl, ensure.Release(), 0, EnsureCookie);

        const auto ensureResponse = co_await ActorWaitForEvent<NCloud::TEvServiceControl::TEvEnsureEnabledResponse>(EnsureCookie);
        if (!CheckResponse(*ensureResponse->Get(), "EnsureEnabled")) {
            co_return;
        }

        auto setup = MakeHolder<NCloud::TEvServiceControl::TEvSetupDelegationRequest>();
        setup->Token = token;
        setup->Request = MakeSetupDelegationRequest(Settings, Delegation, SubjectId);
        Send(ServiceControl, setup.Release(), 0, DelegationCookie);

        const auto setupResponse = co_await ActorWaitForEvent<NCloud::TEvServiceControl::TEvSetupDelegationResponse>(DelegationCookie);
        if (CheckResponse(*setupResponse->Get(), "SetupDelegation")) {
            Finish(true, {});
        }
    }

    async<void> Revoke(TString token) {
        auto revoke = MakeHolder<NCloud::TEvServiceControl::TEvRevokeDelegationRequest>();
        revoke->Token = token;
        revoke->Request = MakeRevokeDelegationRequest(Settings, Delegation);
        Send(ServiceControl, revoke.Release(), 0, DelegationCookie);

        const auto response = co_await ActorWaitForEvent<NCloud::TEvServiceControl::TEvRevokeDelegationResponse>(DelegationCookie);
        if (CheckResponse(*response->Get(), "RevokeDelegation")) {
            Finish(true, {});
        }
    }

    template <typename TResponse>
    bool CheckResponse(const TResponse& response, TStringBuf method) {
        if (!response.Status.Ok()) {
            Finish(false, TStringBuilder() << method << " failed: " << response.Status.Msg);
            return false;
        }
        if (!response.Response.done()) {
            Finish(false, TStringBuilder() << method << " returned an unfinished operation; operation polling is unavailable");
            return false;
        }
        if (response.Response.has_error()) {
            Finish(false, TStringBuilder() << method << " failed: " << response.Response.error().message());
            return false;
        }
        return true;
    }

    void Finish(bool success, TString error) {
        Promise.SetValue({success, std::move(error)});
        Send(ServiceControl, new TEvents::TEvPoisonPill());
        PassAway();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvPrivate::TEvToken, HandleToken);
    )

private:
    static constexpr ui64 EnsureCookie = 1;
    static constexpr ui64 DelegationCookie = 2;

    const TIamDelegationSettings Settings;
    const TIamDelegation Delegation;
    const TString SubjectId;
    TString OperationToken;
    const EAction Action;
    NThreading::TPromise<TIamDelegationResult> Promise;
    NActors::TActorId ServiceControl;
};

NThreading::TFuture<TIamDelegationResult> Run(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation,
    const TString& subjectId,
    const TString& operationToken,
    EAction action,
    TActorSystem* actorSystem)
{
    auto promise = NThreading::NewPromise<TIamDelegationResult>();
    auto future = promise.GetFuture();
    actorSystem->Register(new TIamDelegationActor(
        settings, delegation, subjectId, operationToken, action, std::move(promise)));
    return future;
}

} // anonymous namespace

yandex::cloud::priv::servicecontrol::v1::EnsureEnabledRequest MakeEnsureEnabledRequest(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation)
{
    yandex::cloud::priv::servicecontrol::v1::EnsureEnabledRequest request;
    request.add_service_ids(settings.ServiceId);
    request.mutable_resource()->set_id(delegation.ResourceId);
    request.mutable_resource()->set_type(settings.ResourceType);
    return request;
}

yandex::cloud::priv::servicecontrol::v1::SetupDelegationRequest MakeSetupDelegationRequest(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation,
    const TString& subjectId)
{
    yandex::cloud::priv::servicecontrol::v1::SetupDelegationRequest request;
    request.set_service_id(settings.ServiceId);
    request.set_microservice_id(settings.MicroserviceId);
    request.mutable_resource()->set_id(delegation.ResourceId);
    request.mutable_resource()->set_type(settings.ResourceType);
    request.set_target_service_account_id(delegation.ServiceAccountId);
    request.mutable_referrer()->set_id(delegation.ReferrerId);
    request.mutable_referrer()->set_type(delegation.ReferrerType);
    request.set_on_behalf_of_subject_id(subjectId);
    request.set_with_references(true);
    return request;
}

yandex::cloud::priv::servicecontrol::v1::RevokeDelegationRequest MakeRevokeDelegationRequest(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation)
{
    yandex::cloud::priv::servicecontrol::v1::RevokeDelegationRequest request;
    request.set_service_id(settings.ServiceId);
    request.set_microservice_id(settings.MicroserviceId);
    request.mutable_resource()->set_id(delegation.ResourceId);
    request.mutable_resource()->set_type(settings.ResourceType);
    request.set_target_service_account_id(delegation.ServiceAccountId);
    request.mutable_referrer()->set_id(delegation.ReferrerId);
    request.mutable_referrer()->set_type(delegation.ReferrerType);
    request.set_with_references(true);
    return request;
}

TString NormalizeIamSubject(TString subjectId) {
    constexpr TStringBuf suffix = "@as";
    if (subjectId.EndsWith(suffix)) {
        subjectId.resize(subjectId.size() - suffix.size());
    }
    return subjectId;
}

TString MakeIamDelegationReferrerId(TStringBuf externalDataSourceName, TStringBuf uniqueId) {
    TString readable;
    readable.reserve(8);
    for (const char ch : externalDataSourceName) {
        if (readable.size() == 8) {
            break;
        }
        if (IsAsciiAlnum(ch) || ch == '-' || ch == '_') {
            readable += AsciiToLower(ch);
        } else if (readable.empty() || readable.back() != '-') {
            readable += '-';
        }
    }
    if (readable.empty()) {
        readable = "source";
    }
    TString result = TStringBuilder() << "eds:" << readable << ':';
    result.append(uniqueId.data(), Min(uniqueId.size(), size_t(50 - result.size())));
    return result;
}

bool IsManagedIamDelegation(const TIamDelegation& delegation) {
    return !delegation.ServiceAccountId.empty() && !delegation.ResourceId.empty() &&
        !delegation.ReferrerId.empty();
}

bool IsSameIamDelegation(const TIamDelegation& lhs, const TIamDelegation& rhs) {
    return lhs.ServiceAccountId == rhs.ServiceAccountId &&
        lhs.ResourceId == rhs.ResourceId && lhs.ReferrerId == rhs.ReferrerId;
}

EDelegationCleanup SelectCleanupAfterSchemeRequest(
    bool schemeSuccess,
    const TIamDelegation& previous,
    const TIamDelegation& staged)
{
    if (!schemeSuccess) {
        return IsManagedIamDelegation(staged)
            ? EDelegationCleanup::Staged
            : EDelegationCleanup::None;
    }
    if (IsManagedIamDelegation(previous) &&
        (!IsManagedIamDelegation(staged) || !IsSameIamDelegation(previous, staged)))
    {
        return EDelegationCleanup::Previous;
    }
    return EDelegationCleanup::None;
}

NThreading::TFuture<TIamDelegationResult> SetupIamDelegation(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation,
    const TString& subjectId,
    const TString& operationToken,
    TActorSystem* actorSystem)
{
    return Run(settings, delegation, subjectId, operationToken, EAction::Setup, actorSystem);
}

NThreading::TFuture<TIamDelegationResult> RevokeIamDelegation(
    const TIamDelegationSettings& settings,
    const TIamDelegation& delegation,
    const TString& operationToken,
    TActorSystem* actorSystem)
{
    return Run(settings, delegation, {}, operationToken, EAction::Revoke, actorSystem);
}

} // namespace NKikimr::NKqp::NExternalDataSource
