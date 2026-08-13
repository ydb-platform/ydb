#include "iam_delegation_ddl.h"

#include "iam_delegation.h"
#include "iam_delegation_ddl_actor.h"
#include "iam_delegation_ddl_bridge.h"

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/protos/replication.pb.h>

#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/async/sleep.h>
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

std::optional<TIamCallerIdentity> GetIamCallerIdentity(const TContext& context) {
    const auto& userToken = context.GetUserToken();
    return userToken
        ? ParseIamCallerIdentity(*userToken)
        : std::nullopt;
}

TStatus InvalidIamCallerIdentityStatus() {
    return TStatus::Fail(
        NYql::TIssuesIds::KIKIMR_ACCESS_DENIED,
        "IAM delegation requires a serializable cloud IAM user token");
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
        const TIamDelegation& delegation,
        const TIamCallerIdentity& caller)
    {
        EnsureServiceControl();

        auto ensure = MakeHolder<NCloud::TEvServiceControl::TEvEnsureEnabledRequest>();
        ensure->Token = caller.BearerToken;
        ensure->Request = MakeEnsureEnabledRequest(DelegationSettings, delegation);
        this->Send(ServiceControl, ensure.Release(), 0, EnsureCookie);

        const auto ensureResponse = co_await NActors::ActorWaitForEvent<
            NCloud::TEvServiceControl::TEvEnsureEnabledResponse>(EnsureCookie);
        if (auto result = co_await WaitForOperation(
                *ensureResponse->Get(), "EnsureEnabled", caller);
            !result.Success)
        {
            co_return result;
        }

        auto setup = MakeHolder<NCloud::TEvServiceControl::TEvSetupDelegationRequest>();
        setup->Token = caller.BearerToken;
        setup->Request = MakeSetupDelegationRequest(
            DelegationSettings, delegation, caller.SubjectId);
        this->Send(ServiceControl, setup.Release(), 0, DelegationCookie);

        const auto setupResponse = co_await NActors::ActorWaitForEvent<
            NCloud::TEvServiceControl::TEvSetupDelegationResponse>(DelegationCookie);
        co_return co_await WaitForOperation(
            *setupResponse->Get(), "SetupDelegation", caller);
    }

    NActors::async<TIamDelegationResult> RevokeDelegation(
        const TIamDelegation& delegation,
        const TIamCallerIdentity& caller)
    {
        EnsureServiceControl();

        auto revoke = MakeHolder<NCloud::TEvServiceControl::TEvRevokeDelegationRequest>();
        revoke->Token = caller.BearerToken;
        revoke->Request = MakeRevokeDelegationRequest(DelegationSettings, delegation);
        this->Send(ServiceControl, revoke.Release(), 0, DelegationCookie);

        const auto response = co_await NActors::ActorWaitForEvent<
            NCloud::TEvServiceControl::TEvRevokeDelegationResponse>(DelegationCookie);
        co_return co_await WaitForOperation(
            *response->Get(), "RevokeDelegation", caller);
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
        if (OperationService) {
            this->Send(OperationService, new NActors::TEvents::TEvPoisonPill());
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

    void EnsureOperationService() {
        if (OperationService) {
            return;
        }
        NCloud::TServiceControlSettings settings;
        settings.Endpoint = DelegationSettings.Endpoint;
        settings.EnableSsl = DelegationSettings.EnableSsl;
        settings.RequestTimeoutMs = DelegationSettings.Timeout.MilliSeconds();
        OperationService = this->Register(NCloud::CreateIamOperationService(settings));
    }

    template <typename TResponse>
    NActors::async<TIamDelegationResult> WaitForOperation(
        const TResponse& response,
        TStringBuf method,
        const TIamCallerIdentity& caller)
    {
        if (!response.Status.Ok()) {
            co_return TIamDelegationResult{
                false,
                TStringBuilder() << method << " failed: " << response.Status.Msg,
            };
        }

        auto operation = response.Response;
        const TString operationId = operation.id();
        while (ClassifyIamOperation(operation) == EIamOperationState::InProgress) {
            if (operationId.empty()) {
                co_return TIamDelegationResult{
                    false,
                    TStringBuilder() << method
                        << " returned an unfinished operation without an id",
                };
            }

            co_await NActors::AsyncSleepFor(OperationPollDelay);
            EnsureOperationService();

            auto get = MakeHolder<NCloud::TEvServiceControl::TEvGetOperationRequest>();
            get->Token = caller.BearerToken;
            // Keep polling the operation id returned by the mutating RPC. A
            // sparse Get response does not change the identity of the
            // accepted operation and must not make us abandon it.
            get->Request.set_operation_id(operationId);
            this->Send(OperationService, get.Release(), 0, OperationCookie);

            const auto polled = co_await NActors::ActorWaitForEvent<
                NCloud::TEvServiceControl::TEvGetOperationResponse>(OperationCookie);
            if (!polled->Get()->Status.Ok()) {
                // Once ServiceControl accepted an operation, abandoning it on
                // a transient polling failure can leak a delegation. Keep the
                // DDL pending until IAM exposes a terminal state.
                continue;
            }
            operation = polled->Get()->Response;
        }

        if (ClassifyIamOperation(operation) == EIamOperationState::Failed) {
            co_return TIamDelegationResult{
                false,
                TStringBuilder() << method << " failed: "
                    << operation.error().message(),
            };
        }
        co_return TIamDelegationResult{true, {}};
    }

    static constexpr ui64 EnsureCookie = 1;
    static constexpr ui64 DelegationCookie = 2;
    static constexpr ui64 OperationCookie = 3;
    static constexpr TDuration OperationPollDelay = TDuration::MilliSeconds(100);

    NThreading::TPromise<TStatus> Promise;
    const TIamDelegationSettings DelegationSettings =
        GetIamDelegationSettings(Context.GetActorSystem());
    NActors::TActorId ServiceControl;
    NActors::TActorId OperationService;
};

class TDropIamDelegationDdlActor final
    : public TIamDelegationDdlActorBase<TDropIamDelegationDdlActor>
{
    using TBase = TIamDelegationDdlActorBase<TDropIamDelegationDdlActor>;

public:
    TDropIamDelegationDdlActor(
        NKikimrSchemeOp::TModifyScheme schemeTx,
        TContext context,
        TLegacyDdlExecutor executeLegacyDdl,
        NThreading::TPromise<TStatus> promise)
        : TBase(std::move(schemeTx), std::move(context), std::move(promise))
        , ExecuteLegacyDdl(std::move(executeLegacyDdl))
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

        if (described.NotFound && SchemeTx.GetSuccessOnNotExist()) {
            Finish(TStatus::Success());
            co_return;
        }

        std::optional<TIamCallerIdentity> caller;
        if (IsManagedIamDelegation(described.Delegation)) {
            AddIamPathVersionPrecondition(
                SchemeTx, described.SnapshotPathId, described.SnapshotPathVersion);
            caller = GetIamCallerIdentity(Context);
            if (!caller) {
                Finish(InvalidIamCallerIdentityStatus());
                co_return;
            }
        }

        const auto schemeStatus = co_await AwaitLegacyDdl(
            ExecuteLegacyDdl(SchemeTx, Context), SelfId());
        if (!schemeStatus.IsFail() && !described.NotFound && caller) {
            co_await RevokeDelegation(described.Delegation, *caller);
        }
        Finish(schemeStatus);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvIamDelegationDdl::TEvStart, HandleStart);
    )

    TLegacyDdlExecutor ExecuteLegacyDdl;
};

class TReplaceIamWithNonIamDdlActor final
    : public TIamDelegationDdlActorBase<TReplaceIamWithNonIamDdlActor>
{
    using TBase = TIamDelegationDdlActorBase<TReplaceIamWithNonIamDdlActor>;

public:
    TReplaceIamWithNonIamDdlActor(
        NKikimrSchemeOp::TModifyScheme schemeTx,
        TContext context,
        TLegacyDdlExecutor executeLegacyDdl,
        NThreading::TPromise<TStatus> promise)
        : TBase(std::move(schemeTx), std::move(context), std::move(promise))
        , ExecuteLegacyDdl(std::move(executeLegacyDdl))
    {}

    void Bootstrap() {
        Become(&TReplaceIamWithNonIamDdlActor::StateWork);
        Send(SelfId(), new TEvIamDelegationDdl::TEvStart());
    }

private:
    void HandleStart(TEvIamDelegationDdl::TEvStart::TPtr) {
        co_await Execute();
    }

    NActors::async<void> Execute() {
        const TString path = TStringBuilder()
            << SchemeTx.GetWorkingDir() << '/'
            << SchemeTx.GetCreateExternalDataSource().GetName();
        auto previous = co_await DescribeIamObject(path, Context, SelfId());
        if (previous.Status.IsFail()) {
            Finish(std::move(previous.Status));
            co_return;
        }

        std::optional<TIamCallerIdentity> caller;
        if (IsManagedIamDelegation(previous.Delegation)) {
            AddIamPathVersionPrecondition(
                SchemeTx, previous.SnapshotPathId, previous.SnapshotPathVersion);
            caller = GetIamCallerIdentity(Context);
            if (!caller) {
                Finish(InvalidIamCallerIdentityStatus());
                co_return;
            }
        }

        const auto schemeStatus = co_await AwaitLegacyDdl(
            ExecuteLegacyDdl(SchemeTx, Context), SelfId());
        if (!schemeStatus.IsFail() && caller) {
            co_await RevokeDelegation(previous.Delegation, *caller);
        }
        Finish(schemeStatus);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvIamDelegationDdl::TEvStart, HandleStart);
    )

    TLegacyDdlExecutor ExecuteLegacyDdl;
};

class TCreateOrReplaceIamDelegationDdlActor final
    : public TIamDelegationDdlActorBase<TCreateOrReplaceIamDelegationDdlActor>
{
    using TBase = TIamDelegationDdlActorBase<TCreateOrReplaceIamDelegationDdlActor>;

public:
    TCreateOrReplaceIamDelegationDdlActor(
        NKikimrSchemeOp::TModifyScheme schemeTx,
        TContext context,
        NThreading::TPromise<TStatus> promise)
        : TBase(std::move(schemeTx), std::move(context), std::move(promise))
    {}

    void Bootstrap() {
        Become(&TCreateOrReplaceIamDelegationDdlActor::StateWork);
        Send(SelfId(), new TEvIamDelegationDdl::TEvStart());
    }

private:
    void HandleStart(TEvIamDelegationDdl::TEvStart::TPtr) {
        co_await Execute();
    }

    NActors::async<void> Execute() {
        if (!SchemeTx.GetCreateExternalDataSource().GetAuth().HasIam()) {
            Finish(TStatus::Fail(
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                "IAM delegation actor received a non-IAM external data source"));
            co_return;
        }
        if (!AppData(Context.GetActorSystem())
                ->FeatureFlags.GetEnableExternalDataSourceAuthMethodIam())
        {
            Finish(TStatus::Fail(
                NYql::TIssuesIds::KIKIMR_UNSUPPORTED,
                "AUTH_METHOD=IAM is disabled. Please contact your system administrator to enable it"));
            co_return;
        }
        auto caller = GetIamCallerIdentity(Context);
        if (!caller) {
            Finish(InvalidIamCallerIdentityStatus());
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

        TIamObjectDescription previous;
        const bool createIfNotExists =
            !SchemeTx.GetReplaceIfExists() && !SchemeTx.GetFailedOnAlreadyExists();
        if (SchemeTx.GetReplaceIfExists() || createIfNotExists) {
            const TString path = TStringBuilder()
                << SchemeTx.GetWorkingDir() << '/'
                << SchemeTx.GetCreateExternalDataSource().GetName();
            previous = co_await DescribeIamObject(path, Context, SelfId());
            if (previous.Status.IsFail()) {
                Finish(std::move(previous.Status));
                co_return;
            }
            if (ShouldSkipIamDelegationSetup(SchemeTx, previous.NotFound)) {
                Finish(TStatus::Success());
                co_return;
            }
            if (SchemeTx.GetReplaceIfExists()) {
                AddIamPathVersionPrecondition(
                    SchemeTx,
                    previous.SnapshotPathId,
                    previous.SnapshotPathVersion);
            }
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

        const auto& iam = SchemeTx.GetCreateExternalDataSource().GetAuth().GetIam();
        TIamDelegation staged{
            .ResourceId = iam.GetResourceId(),
            .ServiceAccountId = iam.GetServiceAccountId(),
            .ReferrerId = iam.GetDelegationReferrerId(),
        };
        const auto setup = co_await SetupDelegation(staged, *caller);
        if (!setup.Success) {
            Finish(DelegationStatus(setup));
            co_return;
        }

        auto schemeStatus = co_await ExecuteIamSchemeRequest(
            SchemeTx, Context, SelfId());
        const auto cleanup = SelectCleanupAfterSchemeRequest(
            !schemeStatus.IsFail(),
            previous.Delegation,
            staged);
        if (cleanup == EDelegationCleanup::Staged) {
            co_await RevokeDelegation(staged, *caller);
        } else if (cleanup == EDelegationCleanup::Previous) {
            co_await RevokeDelegation(previous.Delegation, *caller);
        }
        Finish(std::move(schemeStatus));
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

EIamDelegationDdlRoute SelectIamDelegationDdlRoute(
    bool delegationEnabled,
    const NKikimrSchemeOp::TModifyScheme& schemeTx,
    NKqpProto::TKqpSchemeOperation::OperationCase operationCase)
{
    if (!delegationEnabled) {
        return EIamDelegationDdlRoute::Legacy;
    }
    if (operationCase == NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource) {
        if (schemeTx.GetCreateExternalDataSource().GetAuth().HasIam()) {
            return EIamDelegationDdlRoute::IamOperation;
        }
        return schemeTx.GetReplaceIfExists()
            ? EIamDelegationDdlRoute::LegacyWithIamCleanup
            : EIamDelegationDdlRoute::Legacy;
    }
    if (operationCase == NKqpProto::TKqpSchemeOperation::kDropExternalDataSource) {
        return EIamDelegationDdlRoute::LegacyWithIamCleanup;
    }
    return EIamDelegationDdlRoute::Legacy;
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
    if (operationCase == NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource) {
        return new TCreateOrReplaceIamDelegationDdlActor(
            std::move(schemeTx), std::move(context), std::move(promise));
    }
    return nullptr;
}

NActors::IActor* CreateLegacyDdlWithIamCleanupActor(
    NKikimrSchemeOp::TModifyScheme schemeTx,
    TContext context,
    NKqpProto::TKqpSchemeOperation::OperationCase operationCase,
    TLegacyDdlExecutor executeLegacyDdl,
    NThreading::TPromise<TStatus> promise)
{
    if (operationCase == NKqpProto::TKqpSchemeOperation::kDropExternalDataSource) {
        return new TDropIamDelegationDdlActor(
            std::move(schemeTx),
            std::move(context),
            std::move(executeLegacyDdl),
            std::move(promise));
    }
    if (operationCase == NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource &&
        schemeTx.GetReplaceIfExists() &&
        !schemeTx.GetCreateExternalDataSource().GetAuth().HasIam())
    {
        return new TReplaceIamWithNonIamDdlActor(
            std::move(schemeTx),
            std::move(context),
            std::move(executeLegacyDdl),
            std::move(promise));
    }
    return nullptr;
}

} // namespace NKikimr::NKqp::NExternalDataSource
