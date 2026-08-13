#include "iam_delegation_ddl.h"

#include "iam_delegation.h"
#include "iam_delegation_ddl_actor.h"
#include "iam_delegation_ddl_bridge.h"

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/protos/replication.pb.h>

#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/database_type.h>
#include <ydb/library/ycloud/api/service_control.h>
#include <ydb/library/ycloud/impl/service_control.h>

#include <util/generic/guid.h>

namespace NKikimr::NKqp::NExternalDataSource {
namespace {

using TContext = TExternalDataSourceManager::TExternalModificationContext;
using TStatus = TExternalDataSourceManager::TYqlConclusionStatus;

bool IsResolveResourceIdNeeded(const NKikimrSchemeOp::TModifyScheme& schemeTx) {
    return schemeTx.GetCreateExternalDataSource().GetAuth().identity_case() ==
            NKikimrSchemeOp::TAuth::kIam &&
        !schemeTx.GetCreateExternalDataSource().GetAuth().GetIam().HasResourceId();
}

TIamDelegationSettings GetIamDelegationSettings(NActors::TActorSystem* actorSystem) {
    const auto& config = AppData(actorSystem)->ReplicationConfig.GetIamServiceControl();
    TIamDelegationSettings settings;
    settings.Endpoint = config.GetEndpoint();
    settings.ServiceId = config.GetServiceId();
    settings.MicroserviceId = config.GetMicroserviceId();
    settings.ResourceType = config.GetResourceType();
    settings.EnableSsl = config.GetEnableSsl();
    return settings;
}

TString GetIamSubject(const NACLib::TUserToken& token) {
    return NormalizeIamSubject(token.GetUserSID());
}

TString GetIamOperationToken(const TContext& context) {
    const auto& userToken = context.GetUserToken();
    return userToken ? userToken->GetOriginalUserToken() : TString{};
}

TStatus ValidateIamOperationUser(const TContext& context) {
    const auto& userToken = context.GetUserToken();
    if (!userToken || !userToken->HasAuthType() ||
        userToken->GetAuthType() != "AccessService" ||
        userToken->GetOriginalUserToken().empty() ||
        userToken->GetSerializedToken().empty())
    {
        return TStatus::Fail(
            NYql::TIssuesIds::KIKIMR_ACCESS_DENIED,
            "IAM delegation requires a cloud IAM authenticated session");
    }
    return TStatus::Success();
}

struct TEvIamDelegationDdl {
    enum EEv {
        EvStart = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
    };

    struct TEvStart : NActors::TEventLocal<TEvStart, EvStart> {};
};

template <typename TDerived>
class TIamDelegationDdlActorBase : public NActors::TActorBootstrapped<TDerived> {
protected:
    TIamDelegationDdlActorBase(
        NKikimrSchemeOp::TModifyScheme schemeTx,
        TContext context,
        NThreading::TPromise<TStatus> promise)
        : SchemeTx(std::move(schemeTx))
        , Context(std::move(context))
        , Promise(std::move(promise))
    {}

    NActors::async<TIamDelegationResult> SetupDelegation(
        const TIamDelegation& delegation)
    {
        const TString token = GetIamOperationToken(Context);
        if (token.empty()) {
            co_return TIamDelegationResult{false, "user IAM token is empty"};
        }
        EnsureServiceControl();

        auto ensure = MakeHolder<NCloud::TEvServiceControl::TEvEnsureEnabledRequest>();
        ensure->Token = token;
        ensure->Request = MakeEnsureEnabledRequest(DelegationSettings, delegation);
        this->Send(ServiceControl, ensure.Release(), 0, EnsureCookie);

        const auto ensureResponse = co_await NActors::ActorWaitForEvent<
            NCloud::TEvServiceControl::TEvEnsureEnabledResponse>(EnsureCookie);
        if (auto result = CheckResponse(*ensureResponse->Get(), "EnsureEnabled");
            !result.Success)
        {
            co_return result;
        }

        auto setup = MakeHolder<NCloud::TEvServiceControl::TEvSetupDelegationRequest>();
        setup->Token = token;
        setup->Request = MakeSetupDelegationRequest(
            DelegationSettings, delegation, GetIamSubject(*Context.GetUserToken()));
        this->Send(ServiceControl, setup.Release(), 0, DelegationCookie);

        const auto setupResponse = co_await NActors::ActorWaitForEvent<
            NCloud::TEvServiceControl::TEvSetupDelegationResponse>(DelegationCookie);
        co_return CheckResponse(*setupResponse->Get(), "SetupDelegation");
    }

    NActors::async<TIamDelegationResult> RevokeDelegation(
        const TIamDelegation& delegation)
    {
        const TString token = GetIamOperationToken(Context);
        if (token.empty()) {
            co_return TIamDelegationResult{false, "user IAM token is empty"};
        }
        EnsureServiceControl();

        auto revoke = MakeHolder<NCloud::TEvServiceControl::TEvRevokeDelegationRequest>();
        revoke->Token = token;
        revoke->Request = MakeRevokeDelegationRequest(DelegationSettings, delegation);
        this->Send(ServiceControl, revoke.Release(), 0, DelegationCookie);

        const auto response = co_await NActors::ActorWaitForEvent<
            NCloud::TEvServiceControl::TEvRevokeDelegationResponse>(DelegationCookie);
        co_return CheckResponse(*response->Get(), "RevokeDelegation");
    }

    TStatus DelegationStatus(const TIamDelegationResult& result) const {
        return result.Success
            ? TStatus::Success()
            : TStatus::Fail(
                NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE, result.Error);
    }

    void Finish(TStatus status) {
        Promise.SetValue(std::move(status));
        if (ServiceControl) {
            this->Send(ServiceControl, new NActors::TEvents::TEvPoisonPill());
        }
        this->PassAway();
    }

    NKikimrSchemeOp::TModifyScheme SchemeTx;
    const TContext Context;

private:
    void EnsureServiceControl() {
        if (ServiceControl) {
            return;
        }
        NCloud::TServiceControlSettings settings;
        settings.Endpoint = DelegationSettings.Endpoint;
        settings.EnableSsl = DelegationSettings.EnableSsl;
        settings.RequestTimeoutMs = DelegationSettings.Timeout.MilliSeconds();
        ServiceControl = this->Register(NCloud::CreateServiceControl(settings));
    }

    template <typename TResponse>
    TIamDelegationResult CheckResponse(const TResponse& response, TStringBuf method) const {
        if (!response.Status.Ok()) {
            return {
                false,
                TStringBuilder() << method << " failed: " << response.Status.Msg,
            };
        }
        if (!response.Response.done()) {
            return {
                false,
                TStringBuilder() << method
                    << " returned an unfinished operation; operation polling is unavailable",
            };
        }
        if (response.Response.has_error()) {
            return {
                false,
                TStringBuilder() << method << " failed: "
                    << response.Response.error().message(),
            };
        }
        return {true, {}};
    }

    static constexpr ui64 EnsureCookie = 1;
    static constexpr ui64 DelegationCookie = 2;

    NThreading::TPromise<TStatus> Promise;
    const TIamDelegationSettings DelegationSettings =
        GetIamDelegationSettings(Context.GetActorSystem());
    NActors::TActorId ServiceControl;
};

class TDropIamDelegationDdlActor final
    : public TIamDelegationDdlActorBase<TDropIamDelegationDdlActor>
{
    using TBase = TIamDelegationDdlActorBase<TDropIamDelegationDdlActor>;

public:
    TDropIamDelegationDdlActor(
        NKikimrSchemeOp::TModifyScheme schemeTx,
        TContext context,
        NThreading::TPromise<TStatus> promise)
        : TBase(std::move(schemeTx), std::move(context), std::move(promise))
    {}

    void Bootstrap() {
        Become(&TDropIamDelegationDdlActor::StateWork);
        Send(SelfId(), new TEvIamDelegationDdl::TEvStart());
    }

private:
    void HandleStart(TEvIamDelegationDdl::TEvStart::TPtr) {
        co_await Execute();
    }

    NActors::async<void> Execute() {
        const TString path = TStringBuilder()
            << SchemeTx.GetWorkingDir() << '/' << SchemeTx.GetDrop().GetName();
        auto described = co_await DescribeIamObject(path, Context, SelfId());
        if (described.Status.IsFail()) {
            Finish(std::move(described.Status));
            co_return;
        }

        if (IsManagedIamDelegation(described.Delegation)) {
            auto status = ValidateIamOperationUser(Context);
            if (status.IsFail()) {
                Finish(std::move(status));
                co_return;
            }
        }

        const auto schemeStatus = co_await ExecuteIamSchemeRequest(
            SchemeTx, Context, SelfId());
        if (!schemeStatus.IsFail() && !described.NotFound &&
            IsManagedIamDelegation(described.Delegation))
        {
            co_await RevokeDelegation(described.Delegation);
        }
        Finish(schemeStatus);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvIamDelegationDdl::TEvStart, HandleStart);
    )
};

class TCreateOrAlterIamDelegationDdlActor final
    : public TIamDelegationDdlActorBase<TCreateOrAlterIamDelegationDdlActor>
{
    using TBase = TIamDelegationDdlActorBase<TCreateOrAlterIamDelegationDdlActor>;

public:
    TCreateOrAlterIamDelegationDdlActor(
        NKikimrSchemeOp::TModifyScheme schemeTx,
        TContext context,
        NThreading::TPromise<TStatus> promise)
        : TBase(std::move(schemeTx), std::move(context), std::move(promise))
    {}

    void Bootstrap() {
        Become(&TCreateOrAlterIamDelegationDdlActor::StateWork);
        Send(SelfId(), new TEvIamDelegationDdl::TEvStart());
    }

private:
    void HandleStart(TEvIamDelegationDdl::TEvStart::TPtr) {
        co_await Execute();
    }

    NActors::async<void> Execute() {
        TIamObjectDescription previous;
        const bool hasIam = SchemeTx.GetCreateExternalDataSource().GetAuth().HasIam();
        if (hasIam) {
            if (!AppData(Context.GetActorSystem())
                    ->FeatureFlags.GetEnableExternalDataSourceAuthMethodIam())
            {
                Finish(TStatus::Fail(
                    NYql::TIssuesIds::KIKIMR_UNSUPPORTED,
                    "AUTH_METHOD=IAM is disabled. Please contact your system administrator to enable it"));
                co_return;
            }
            auto status = ValidateIamOperationUser(Context);
            if (status.IsFail()) {
                Finish(std::move(status));
                co_return;
            }
            if (SchemeTx.GetCreateExternalDataSource().GetSourceType() !=
                ToString(NYql::EDatabaseType::Ydb))
            {
                Finish(TStatus::Fail(
                    NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                    "AUTH_METHOD=IAM is supported only for SOURCE_TYPE=Ydb"));
                co_return;
            }
            if (IsResolveResourceIdNeeded(SchemeTx)) {
                auto cloud = co_await DescribeDatabaseCloudId(Context, SelfId());
                if (cloud.Status.IsFail()) {
                    Finish(std::move(cloud.Status));
                    co_return;
                }
                SchemeTx.MutableCreateExternalDataSource()
                    ->MutableAuth()->MutableIam()->SetResourceId(cloud.CloudId);
            }
        } else {
            const auto status = co_await ValidateExternalDatasourceSecrets(
                SchemeTx.GetCreateExternalDataSource(), Context, SelfId());
            if (status.IsFail()) {
                Finish(status);
                co_return;
            }
        }

        if (SchemeTx.GetReplaceIfExists()) {
            const TString path = TStringBuilder()
                << SchemeTx.GetWorkingDir() << '/'
                << SchemeTx.GetCreateExternalDataSource().GetName();
            previous = co_await DescribeIamObject(path, Context, SelfId());
            if (previous.Status.IsFail()) {
                Finish(std::move(previous.Status));
                co_return;
            }
        }

        if (!hasIam) {
            if (IsManagedIamDelegation(previous.Delegation)) {
                auto status = ValidateIamOperationUser(Context);
                if (status.IsFail()) {
                    Finish(std::move(status));
                    co_return;
                }
            }
            const auto schemeStatus = co_await ExecuteIamSchemeRequest(
                SchemeTx, Context, SelfId());
            if (!schemeStatus.IsFail() && IsManagedIamDelegation(previous.Delegation)) {
                co_await RevokeDelegation(previous.Delegation);
            }
            Finish(schemeStatus);
            co_return;
        }

        const auto& iam = SchemeTx.GetCreateExternalDataSource().GetAuth().GetIam();
        TIamDelegation staged{
            .ResourceId = iam.GetResourceId(),
            .ServiceAccountId = iam.GetServiceAccountId(),
            .ReferrerId = iam.GetDelegationReferrerId(),
        };
        const auto setup = co_await SetupDelegation(staged);
        if (!setup.Success) {
            Finish(DelegationStatus(setup));
            co_return;
        }

        const auto schemeStatus = co_await ExecuteIamSchemeRequest(
            SchemeTx, Context, SelfId());
        const auto cleanup = SelectCleanupAfterSchemeRequest(
            !schemeStatus.IsFail(), previous.Delegation, staged);
        if (cleanup == EDelegationCleanup::Staged) {
            co_await RevokeDelegation(staged);
        } else if (cleanup == EDelegationCleanup::Previous) {
            co_await RevokeDelegation(previous.Delegation);
        }
        Finish(schemeStatus);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvIamDelegationDdl::TEvStart, HandleStart);
    )
};

} // anonymous namespace

bool IsIamDelegationEnabled(NActors::TActorSystem* actorSystem) {
    return actorSystem &&
        AppData(actorSystem)->FeatureFlags.GetEnableExternalDataSourceIamDelegation();
}

TStatus PrepareIamDelegation(
    NKikimrSchemeOp::TExternalDataSourceDescription& description,
    TStringBuf name)
{
    auto& iam = *description.MutableAuth()->MutableIam();
    if (iam.GetServiceAccountId().empty()) {
        return TStatus::Fail(
            NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
            "SERVICE_ACCOUNT_ID is required for AUTH_METHOD=IAM");
    }
    iam.SetDelegationReferrerId(MakeIamDelegationReferrerId(name, CreateGuidAsString()));
    return TStatus::Success();
}

NActors::IActor* CreateIamDelegationDdlActor(
    NKikimrSchemeOp::TModifyScheme schemeTx,
    TContext context,
    NKqpProto::TKqpSchemeOperation::OperationCase operationCase,
    NThreading::TPromise<TStatus> promise)
{
    if (operationCase == NKqpProto::TKqpSchemeOperation::kDropExternalDataSource) {
        return new TDropIamDelegationDdlActor(
            std::move(schemeTx), std::move(context), std::move(promise));
    }
    if (operationCase == NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource) {
        return new TCreateOrAlterIamDelegationDdlActor(
            std::move(schemeTx), std::move(context), std::move(promise));
    }
    return nullptr;
}

} // namespace NKikimr::NKqp::NExternalDataSource
