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

// Delegation lifecycle for AUTH_METHOD=IAM external data sources.
//
// The single most important property of this file: every IAM call it makes acts
// as the user who issued the DDL, never as the YDB system service account. That
// identity is materialized once per operation as a TIamCallerIdentity holding
//   * BearerToken - the user's own IAM token, which authenticates EnsureEnabled,
//     SetupDelegation, RevokeDelegation and every operation poll, and
//   * SubjectId   - the same token's verified subject, sent as
//     SetupDelegation.on_behalf_of_subject_id so IAM checks the user's rights on
//     the target service account.
// Both come from the AccessService-verified TUserToken that travels with the
// DDL, so the bearer and the subject can never belong to different identities.
// An operation that cannot produce that identity is rejected before it reaches
// SchemeShard.

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

    // Stage the delegation this DDL proposes. Both calls run as `caller`, and
    // SetupDelegation additionally names `caller` as the subject IAM must
    // authorize against the target service account.
    NActors::async<TIamDelegationResult> SetupDelegation(
        const TIamDelegation& delegation,
        const TIamCallerIdentity& caller)
    {
        if (auto enabled = co_await CallServiceControlAsCaller<
                NCloud::TEvServiceControl::TEvEnsureEnabledRequest,
                NCloud::TEvServiceControl::TEvEnsureEnabledResponse>(
                    caller,
                    MakeEnsureEnabledRequest(DelegationSettings, delegation),
                    "EnsureEnabled",
                    EnsureCookie);
            !enabled.Success)
        {
            co_return enabled;
        }

        co_return co_await CallServiceControlAsCaller<
            NCloud::TEvServiceControl::TEvSetupDelegationRequest,
            NCloud::TEvServiceControl::TEvSetupDelegationResponse>(
                caller,
                MakeSetupDelegationRequest(
                    DelegationSettings, delegation, caller.SubjectId),
                "SetupDelegation",
                DelegationCookie);
    }

    // Release a delegation this DDL no longer owns: either the one it just
    // staged, after SchemeShard refused the schema change, or the one the
    // replaced/dropped object used to own.
    NActors::async<TIamDelegationResult> RevokeDelegation(
        const TIamDelegation& delegation,
        const TIamCallerIdentity& caller)
    {
        co_return co_await CallServiceControlAsCaller<
            NCloud::TEvServiceControl::TEvRevokeDelegationRequest,
            NCloud::TEvServiceControl::TEvRevokeDelegationResponse>(
                caller,
                MakeRevokeDelegationRequest(DelegationSettings, delegation),
                "RevokeDelegation",
                DelegationCookie);
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
    NCloud::TServiceControlSettings GrpcSettings() const {
        NCloud::TServiceControlSettings settings;
        settings.Endpoint = DelegationSettings.Endpoint;
        settings.EnableSsl = DelegationSettings.EnableSsl;
        settings.RequestTimeoutMs = DelegationSettings.Timeout.MilliSeconds();
        return settings;
    }

    NActors::TActorId ServiceControlClient() {
        if (!ServiceControl) {
            ServiceControl = this->Register(NCloud::CreateServiceControl(GrpcSettings()));
        }
        return ServiceControl;
    }

    NActors::TActorId OperationServiceClient() {
        if (!OperationService) {
            OperationService =
                this->Register(NCloud::CreateIamOperationService(GrpcSettings()));
        }
        return OperationService;
    }

    // The one place a ServiceControl lifecycle RPC is issued, so the rule that
    // it is authenticated as the initiating user - and never as the YDB system
    // service account - holds for all of them by construction.
    template <typename TRequestEvent, typename TResponseEvent, typename TRequest>
    NActors::async<TIamDelegationResult> CallServiceControlAsCaller(
        const TIamCallerIdentity& caller,
        TRequest request,
        TStringBuf method,
        ui64 cookie)
    {
        auto event = MakeHolder<TRequestEvent>();
        event->Token = caller.BearerToken;
        event->Request = std::move(request);
        this->Send(ServiceControlClient(), event.Release(), 0, cookie);

        const auto response = co_await NActors::ActorWaitForEvent<TResponseEvent>(cookie);
        co_return co_await AwaitIamOperation(*response->Get(), method, caller);
    }

    // ServiceControl may answer with an accepted but unfinished operation. Poll
    // it - again as the initiating user - until IAM reports a terminal state or
    // the polling budget runs out. The budget is what keeps a DDL that IAM never
    // resolves from hanging forever and leaking this actor.
    template <typename TResponse>
    NActors::async<TIamDelegationResult> AwaitIamOperation(
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

        // Both live in the coroutine frame for the whole poll loop, so no actor
        // member state is needed. The wall-clock deadline, not a retry count, is
        // what bounds the wait: each poll can also spend up to the gRPC request
        // timeout.
        const TMonotonic deadline =
            NActors::TActivationContext::Monotonic() + IamOperationPollBudget;
        TBackoff backoff(IamOperationMinPollDelay, IamOperationMaxPollDelay);

        while (ClassifyIamOperation(operation) == EIamOperationState::InProgress) {
            if (operationId.empty()) {
                co_return TIamDelegationResult{
                    false,
                    TStringBuilder() << method
                        << " returned an unfinished operation without an id",
                };
            }
            if (NActors::TActivationContext::Monotonic() >= deadline) {
                // Unknown outcome, not a refusal: IAM accepted the operation and
                // may still apply it. Setup callers fail the DDL and leave a
                // delegation to reconcile - the same leak as losing the process
                // in this window - while post-commit cleanup ignores the result.
                co_return TIamDelegationResult{
                    false,
                    TStringBuilder() << method << " operation " << operationId
                        << " did not reach a terminal state within "
                        << IamOperationPollBudget
                        << " and may still be applied by IAM",
                };
            }

            // Jittered backoff, so a slow operation does not become a tight loop
            // against IAM and concurrent DDLs do not poll in lockstep.
            co_await NActors::AsyncSleepFor(backoff.Next());

            auto get = MakeHolder<NCloud::TEvServiceControl::TEvGetOperationRequest>();
            get->Token = caller.BearerToken;
            // Keep polling the operation id returned by the mutating RPC. A
            // sparse Get response does not change the identity of the
            // accepted operation and must not make us abandon it.
            get->Request.set_operation_id(operationId);
            this->Send(OperationServiceClient(), get.Release(), 0, OperationCookie);

            const auto polled = co_await NActors::ActorWaitForEvent<
                NCloud::TEvServiceControl::TEvGetOperationResponse>(OperationCookie);
            if (!polled->Get()->Status.Ok()) {
                // Once ServiceControl accepted an operation, abandoning it on a
                // transient polling failure can leak a delegation. Keep retrying
                // until the budget expires rather than giving up on this reply.
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

    NThreading::TPromise<TStatus> Promise;
    const TIamDelegationSettings DelegationSettings =
        GetIamDelegationSettings(Context.GetActorSystem());
    NActors::TActorId ServiceControl;
    NActors::TActorId OperationService;
};

// DROP and IAM-to-non-IAM replacement do not create a delegation, so they stay
// on the pre-existing (legacy) executor. They are wrapped only because the
// request does not say how the committed object authenticates, and a committed
// object may own a managed delegation that this operation must release.
//
// The two differ solely in which name they look up and whether a missing object
// is already the requested outcome, so one actor serves both.
struct TIamCleanupTarget {
    TString Name;
    bool SucceedIfAbsent = false;
};

class TLegacyDdlWithIamCleanupActor final
    : public TIamDelegationDdlActorBase<TLegacyDdlWithIamCleanupActor>
{
    using TBase = TIamDelegationDdlActorBase<TLegacyDdlWithIamCleanupActor>;

public:
    TLegacyDdlWithIamCleanupActor(
        NKikimrSchemeOp::TModifyScheme schemeTx,
        TContext context,
        TIamCleanupTarget target,
        TLegacyDdlExecutor executeLegacyDdl,
        NThreading::TPromise<TStatus> promise)
        : TBase(std::move(schemeTx), std::move(context), std::move(promise))
        , Target(std::move(target))
        , ExecuteLegacyDdl(std::move(executeLegacyDdl))
    {}

    void Bootstrap() {
        Become(&TLegacyDdlWithIamCleanupActor::StateWork);
        Send(SelfId(), new TEvIamDelegationDdl::TEvStart());
    }

private:
    void HandleStart(TEvIamDelegationDdl::TEvStart::TPtr) {
        co_await Execute();
    }

    NActors::async<void> Execute() {
        const TString path = TStringBuilder()
            << SchemeTx.GetWorkingDir() << '/' << Target.Name;
        auto committed = co_await DescribeIamObject(path, Context, SelfId());
        if (committed.Status.IsFail()) {
            Finish(std::move(committed.Status));
            co_return;
        }
        if (committed.NotFound && Target.SucceedIfAbsent) {
            Finish(TStatus::Success());
            co_return;
        }

        // An object without a complete persisted delegation tuple is not
        // DDL-managed: it needs no IAM call, and therefore no user identity.
        // A managed one needs both, plus a precondition that pins the snapshot
        // this decision was made on.
        std::optional<TIamCallerIdentity> caller;
        if (IsManagedIamDelegation(committed.Delegation)) {
            AddIamPathVersionPrecondition(
                SchemeTx, committed.SnapshotPathId, committed.SnapshotPathVersion);
            caller = GetIamCallerIdentity(Context);
            if (!caller) {
                Finish(InvalidIamCallerIdentityStatus());
                co_return;
            }
        }

        const auto schemeStatus = co_await AwaitLegacyDdl(
            ExecuteLegacyDdl(SchemeTx, Context), SelfId());
        if (!schemeStatus.IsFail() && caller) {
            // Best effort by design: SchemeShard has already committed, so a
            // revoke failure must not turn its success into a client error.
            co_await RevokeDelegation(committed.Delegation, *caller);
        }
        Finish(schemeStatus);
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvIamDelegationDdl::TEvStart, HandleStart);
    )

    const TIamCleanupTarget Target;
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

    // Nothing here may touch IAM, so it stays outside the coroutine.
    TStatus Validate() const {
        if (!SchemeTx.GetCreateExternalDataSource().GetAuth().HasIam()) {
            return TStatus::Fail(
                NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                "IAM delegation actor received a non-IAM external data source");
        }
        if (!AppData(Context.GetActorSystem())
                ->FeatureFlags.GetEnableExternalDataSourceAuthMethodIam())
        {
            return TStatus::Fail(
                NYql::TIssuesIds::KIKIMR_UNSUPPORTED,
                "AUTH_METHOD=IAM is disabled. Please contact your system administrator to enable it");
        }
        if (SchemeTx.GetCreateExternalDataSource().GetSourceType() !=
            ToString(NYql::EDatabaseType::Ydb))
        {
            return TStatus::Fail(
                NYql::TIssuesIds::KIKIMR_BAD_REQUEST,
                "AUTH_METHOD=IAM is supported only for SOURCE_TYPE=Ydb");
        }
        return TStatus::Success();
    }

    NActors::async<void> Execute() {
        if (auto status = Validate(); status.IsFail()) {
            Finish(std::move(status));
            co_return;
        }

        // Resolve the initiating user once; setup, compensation and cleanup all
        // reuse this one identity rather than reinterpreting the token.
        const auto caller = GetIamCallerIdentity(Context);
        if (!caller) {
            Finish(InvalidIamCallerIdentityStatus());
            co_return;
        }

        // Read the committed object, if this statement can hit one. A
        // replacement must remember the delegation it is about to supersede,
        // and CREATE IF NOT EXISTS must not call IAM for an object that is
        // already there.
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
                // Losing a race with a concurrent replacement must fail the
                // compare-and-swap instead of superseding a newer delegation.
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

        // Stage the new delegation before SchemeShard, so a setup failure fails
        // the DDL while the committed object keeps working.
        const auto& iam = SchemeTx.GetCreateExternalDataSource().GetAuth().GetIam();
        const TIamDelegation staged{
            .ResourceId = iam.GetResourceId(),
            .ServiceAccountId = iam.GetServiceAccountId(),
            .ReferrerId = iam.GetDelegationReferrerId(),
        };
        if (const auto setup = co_await SetupDelegation(staged, *caller);
            !setup.Success)
        {
            Finish(DelegationStatus(setup));
            co_return;
        }

        auto schemeStatus = co_await ExecuteIamSchemeRequest(
            SchemeTx, Context, SelfId());

        // Exactly one delegation is now redundant: the staged one if SchemeShard
        // refused, the superseded one if it committed. Either way the schema
        // result stands - cleanup can never change it.
        switch (SelectCleanupAfterSchemeRequest(
            !schemeStatus.IsFail(), previous.Delegation, staged))
        {
            case EDelegationCleanup::Staged:
                co_await RevokeDelegation(staged, *caller);
                break;
            case EDelegationCleanup::Previous:
                co_await RevokeDelegation(previous.Delegation, *caller);
                break;
            case EDelegationCleanup::None:
                break;
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
    std::optional<TIamCleanupTarget> target;
    if (operationCase == NKqpProto::TKqpSchemeOperation::kDropExternalDataSource) {
        // DROP ... IF EXISTS has nothing left to do once the object is absent.
        target = TIamCleanupTarget{
            .Name = schemeTx.GetDrop().GetName(),
            .SucceedIfAbsent = schemeTx.GetSuccessOnNotExist(),
        };
    } else if (operationCase == NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource &&
        schemeTx.GetReplaceIfExists() &&
        !schemeTx.GetCreateExternalDataSource().GetAuth().HasIam())
    {
        // A replacement still has to create the new object when none exists.
        target = TIamCleanupTarget{
            .Name = schemeTx.GetCreateExternalDataSource().GetName(),
            .SucceedIfAbsent = false,
        };
    }
    if (!target) {
        return nullptr;
    }
    return new TLegacyDdlWithIamCleanupActor(
        std::move(schemeTx),
        std::move(context),
        std::move(*target),
        std::move(executeLegacyDdl),
        std::move(promise));
}

} // namespace NKikimr::NKqp::NExternalDataSource
