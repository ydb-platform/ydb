#include "iam_delegation_ddl.h"

#include "iam_delegation.h"
#include "iam_object_lookup.h"

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>
#include <ydb/core/kqp/federated_query/actors/kqp_federated_query_actors.h>
#include <ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h>
#include <ydb/core/kqp/gateway/utils/metadata_helpers.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/async/async.h>
#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/database_type.h>

#include <util/generic/guid.h>

namespace NKikimr::NKqp::NExternalDataSource {
namespace {

using TContext = TExternalDataSourceManager::TExternalModificationContext;
using TStatus = TExternalDataSourceManager::TYqlConclusionStatus;
using TAsyncStatus = TExternalDataSourceManager::TAsyncStatus;

struct TIamObjectDescription : NYql::IKikimrGateway::TGenericResult {
    TStatus Status = TStatus::Success();
    bool NotFound = false;
    TIamDelegation Delegation;
};

struct TCloudIdDescription : NYql::IKikimrGateway::TGenericResult {
    TStatus Status = TStatus::Success();
    TString CloudId;
};

NThreading::TFuture<TCloudIdDescription> DescribeDatabaseCloudId(const TContext& context) {
    using TRequest = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TResponse = TEvTxProxySchemeCache::TEvNavigateKeySetResult;

    auto navigate = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    auto& entry = navigate->ResultSet.emplace_back();
    entry.Path = NKikimr::SplitPath(context.GetDatabase());
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    navigate->DatabaseName = context.GetDatabase();
    if (context.GetUserToken()) {
        navigate->UserToken = MakeIntrusive<NACLib::TUserToken>(*context.GetUserToken());
    }

    auto promise = NThreading::NewPromise<TCloudIdDescription>();
    auto future = promise.GetFuture();
    context.GetActorSystem()->Register(new TActorRequestHandler<TRequest, TResponse, TCloudIdDescription>(
        MakeSchemeCacheID(), new TRequest(navigate.Release()), promise,
        [](NThreading::TPromise<TCloudIdDescription> promise, TResponse&& response) {
            TCloudIdDescription result;
            const auto& request = *response.Request;
            if (request.ErrorCount || request.ResultSet.size() != 1) {
                result.Status = TStatus::Fail(
                    NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                    "Cannot describe database for IAM delegation");
            } else if (const auto it = request.ResultSet.front().Attributes.find("cloud_id");
                       it != request.ResultSet.front().Attributes.end() && !it->second.empty())
            {
                result.CloudId = it->second;
            } else {
                result.Status = TStatus::Fail(
                    NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                    "Database has no cloud_id attribute required by AUTH_METHOD=IAM");
            }
            promise.SetValue(std::move(result));
        }));
    return future;
}

NThreading::TFuture<TIamObjectDescription> DescribeIamObject(
    const TString& path,
    const TContext& context)
{
    using TRequest = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TResponse = TEvTxProxySchemeCache::TEvNavigateKeySetResult;

    auto navigate = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    auto& entry = navigate->ResultSet.emplace_back();
    entry.Path = NKikimr::SplitPath(path);
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown;
    entry.Kind = NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalDataSource;
    navigate->DatabaseName = context.GetDatabase();
    if (context.GetUserToken()) {
        navigate->UserToken = MakeIntrusive<NACLib::TUserToken>(*context.GetUserToken());
    }

    auto promise = NThreading::NewPromise<TIamObjectDescription>();
    auto future = promise.GetFuture();
    context.GetActorSystem()->Register(new TActorRequestHandler<TRequest, TResponse, TIamObjectDescription>(
        MakeSchemeCacheID(), new TRequest(navigate.Release()), promise,
        [](NThreading::TPromise<TIamObjectDescription> promise, TResponse&& response) {
            TIamObjectDescription result;
            const auto& request = *response.Request;
            if (request.ResultSet.size() == 1) {
                const auto& entry = request.ResultSet.front();
                if (ClassifyIamObjectLookup(
                        entry.Status, static_cast<bool>(entry.ExternalDataSourceInfo)) ==
                    EIamObjectLookupResult::NotFound)
                {
                    result.NotFound = true;
                    promise.SetValue(std::move(result));
                    return;
                }
            }
            if (request.ErrorCount || request.ResultSet.size() != 1 ||
                request.ResultSet.front().Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok ||
                !request.ResultSet.front().ExternalDataSourceInfo)
            {
                result.Status = TStatus::Fail(
                    NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                    "Cannot describe external data source for IAM delegation");
                promise.SetValue(std::move(result));
                return;
            }
            const auto& description = request.ResultSet.front().ExternalDataSourceInfo->Description;
            if (!description.GetAuth().HasIam()) {
                promise.SetValue(std::move(result));
                return;
            }
            const auto& iam = description.GetAuth().GetIam();
            if (!iam.HasDelegationReferrerId()) {
                promise.SetValue(std::move(result));
                return;
            }
            result.Delegation.ResourceId = iam.GetResourceId();
            result.Delegation.ServiceAccountId = iam.GetServiceAccountId();
            result.Delegation.ReferrerId = iam.GetDelegationReferrerId();
            promise.SetValue(std::move(result));
        }));
    return future;
}

TAsyncStatus ValidateExternalDatasourceSecrets(
    const NKikimrSchemeOp::TExternalDataSourceDescription& description,
    const TContext& context)
{
    const auto& userToken = context.GetUserToken();
    return DescribeExternalDataSourceSecrets(
        description.GetAuth(),
        userToken ? new NACLib::TUserToken(*userToken) : nullptr,
        context.GetDatabase(),
        context.GetActorSystem()).Apply([](const auto& future) {
            const auto& value = future.GetValue();
            if (value.Status != Ydb::StatusIds::SUCCESS) {
                return TStatus::Fail(
                    NYql::YqlStatusFromYdbStatus(value.Status), value.Issues.ToString());
            }
            return TStatus::Success();
        });
}

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
    return userToken ? userToken->GetSerializedToken() : TString{};
}

TStatus ValidateIamOperationUser(const TContext& context) {
    const auto& userToken = context.GetUserToken();
    if (!userToken || !userToken->HasAuthType() ||
        userToken->GetAuthType() != "AccessService" ||
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
        EvStatus,
        EvIamObject,
        EvCloudId,
        EvDelegation,
    };

    struct TEvStart : NActors::TEventLocal<TEvStart, EvStart> {};

    struct TEvStatus : NActors::TEventLocal<TEvStatus, EvStatus> {
        explicit TEvStatus(TStatus status)
            : Status(std::move(status))
        {}
        TStatus Status;
    };

    struct TEvIamObject : NActors::TEventLocal<TEvIamObject, EvIamObject> {
        explicit TEvIamObject(TIamObjectDescription description)
            : Description(std::move(description))
        {}
        TIamObjectDescription Description;
    };

    struct TEvCloudId : NActors::TEventLocal<TEvCloudId, EvCloudId> {
        explicit TEvCloudId(TCloudIdDescription description)
            : Description(std::move(description))
        {}
        TCloudIdDescription Description;
    };

    struct TEvDelegation : NActors::TEventLocal<TEvDelegation, EvDelegation> {
        explicit TEvDelegation(TIamDelegationResult result)
            : Result(std::move(result))
        {}
        TIamDelegationResult Result;
    };
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

    NActors::async<TStatus> AwaitStatus(TAsyncStatus future) {
        future.Subscribe([
            actorSystem = TActivationContext::ActorSystem(),
            self = this->SelfId()](const auto& result) {
            actorSystem->Send(
                self, new TEvIamDelegationDdl::TEvStatus(result.GetValue()), 0, StatusCookie);
        });
        const auto event = co_await NActors::ActorWaitForEvent<
            TEvIamDelegationDdl::TEvStatus>(StatusCookie);
        co_return std::move(event->Get()->Status);
    }

    NActors::async<TIamObjectDescription> AwaitIamObject(
        NThreading::TFuture<TIamObjectDescription> future)
    {
        future.Subscribe([
            actorSystem = TActivationContext::ActorSystem(),
            self = this->SelfId()](const auto& result) {
            actorSystem->Send(
                self, new TEvIamDelegationDdl::TEvIamObject(result.GetValue()), 0,
                IamObjectCookie);
        });
        const auto event = co_await NActors::ActorWaitForEvent<
            TEvIamDelegationDdl::TEvIamObject>(IamObjectCookie);
        co_return std::move(event->Get()->Description);
    }

    NActors::async<TCloudIdDescription> AwaitCloudId(
        NThreading::TFuture<TCloudIdDescription> future)
    {
        future.Subscribe([
            actorSystem = TActivationContext::ActorSystem(),
            self = this->SelfId()](const auto& result) {
            actorSystem->Send(
                self, new TEvIamDelegationDdl::TEvCloudId(result.GetValue()), 0,
                CloudIdCookie);
        });
        const auto event = co_await NActors::ActorWaitForEvent<
            TEvIamDelegationDdl::TEvCloudId>(CloudIdCookie);
        co_return std::move(event->Get()->Description);
    }

    NActors::async<TIamDelegationResult> AwaitDelegation(
        NThreading::TFuture<TIamDelegationResult> future)
    {
        future.Subscribe([
            actorSystem = TActivationContext::ActorSystem(),
            self = this->SelfId()](const auto& result) {
            actorSystem->Send(
                self, new TEvIamDelegationDdl::TEvDelegation(result.GetValue()), 0,
                DelegationCookie);
        });
        const auto event = co_await NActors::ActorWaitForEvent<
            TEvIamDelegationDdl::TEvDelegation>(DelegationCookie);
        co_return std::move(event->Get()->Result);
    }

    TStatus DelegationStatus(const TIamDelegationResult& result) const {
        return result.Success
            ? TStatus::Success()
            : TStatus::Fail(
                NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE, result.Error);
    }

    void Finish(TStatus status) {
        Promise.SetValue(std::move(status));
        this->PassAway();
    }

    NKikimrSchemeOp::TModifyScheme SchemeTx;
    const TContext Context;

private:
    static constexpr ui64 StatusCookie = 1;
    static constexpr ui64 IamObjectCookie = 2;
    static constexpr ui64 CloudIdCookie = 3;
    static constexpr ui64 DelegationCookie = 4;

    NThreading::TPromise<TStatus> Promise;
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
    {
    }

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
        auto described = co_await AwaitIamObject(DescribeIamObject(path, Context));
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

        const auto schemeStatus = co_await AwaitStatus(SendSchemeRequest(SchemeTx, Context));
        if (!schemeStatus.IsFail() && !described.NotFound &&
            IsManagedIamDelegation(described.Delegation))
        {
            co_await AwaitDelegation(RevokeIamDelegation(
                GetIamDelegationSettings(Context.GetActorSystem()),
                described.Delegation,
                GetIamOperationToken(Context),
                Context.GetActorSystem()));
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
    {
    }

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
                auto cloud = co_await AwaitCloudId(DescribeDatabaseCloudId(Context));
                if (cloud.Status.IsFail()) {
                    Finish(std::move(cloud.Status));
                    co_return;
                }
                SchemeTx.MutableCreateExternalDataSource()
                    ->MutableAuth()->MutableIam()->SetResourceId(cloud.CloudId);
            }
        } else {
            const auto status = co_await AwaitStatus(ValidateExternalDatasourceSecrets(
                SchemeTx.GetCreateExternalDataSource(), Context));
            if (status.IsFail()) {
                Finish(status);
                co_return;
            }
        }

        if (SchemeTx.GetReplaceIfExists()) {
            const TString path = TStringBuilder()
                << SchemeTx.GetWorkingDir() << '/'
                << SchemeTx.GetCreateExternalDataSource().GetName();
            previous = co_await AwaitIamObject(DescribeIamObject(path, Context));
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
            const auto schemeStatus = co_await AwaitStatus(SendSchemeRequest(SchemeTx, Context));
            if (!schemeStatus.IsFail() && IsManagedIamDelegation(previous.Delegation)) {
                co_await AwaitDelegation(RevokeIamDelegation(
                    GetIamDelegationSettings(Context.GetActorSystem()),
                    previous.Delegation,
                    GetIamOperationToken(Context),
                    Context.GetActorSystem()));
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
        const auto setup = co_await AwaitDelegation(SetupIamDelegation(
            GetIamDelegationSettings(Context.GetActorSystem()),
            staged,
            GetIamSubject(*Context.GetUserToken()),
            GetIamOperationToken(Context),
            Context.GetActorSystem()));
        if (!setup.Success) {
            Finish(DelegationStatus(setup));
            co_return;
        }

        const auto schemeStatus = co_await AwaitStatus(SendSchemeRequest(SchemeTx, Context));
        const auto cleanup = SelectCleanupAfterSchemeRequest(
            !schemeStatus.IsFail(), previous.Delegation, staged);
        if (cleanup == EDelegationCleanup::Staged) {
            co_await AwaitDelegation(RevokeIamDelegation(
                GetIamDelegationSettings(Context.GetActorSystem()),
                staged,
                GetIamOperationToken(Context),
                Context.GetActorSystem()));
        } else if (cleanup == EDelegationCleanup::Previous) {
            co_await AwaitDelegation(RevokeIamDelegation(
                GetIamDelegationSettings(Context.GetActorSystem()),
                previous.Delegation,
                GetIamOperationToken(Context),
                Context.GetActorSystem()));
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

TAsyncStatus ExecuteIamDelegationDdl(
    const NKikimrSchemeOp::TModifyScheme& schemeTx,
    const TContext& context,
    NKqpProto::TKqpSchemeOperation::OperationCase operationCase)
{
    auto promise = NThreading::NewPromise<TStatus>();
    auto future = promise.GetFuture();
    if (operationCase == NKqpProto::TKqpSchemeOperation::kDropExternalDataSource) {
        context.GetActorSystem()->Register(new TDropIamDelegationDdlActor(
            schemeTx, context, std::move(promise)));
    } else if (operationCase == NKqpProto::TKqpSchemeOperation::kCreateExternalDataSource) {
        context.GetActorSystem()->Register(new TCreateOrAlterIamDelegationDdlActor(
            schemeTx, context, std::move(promise)));
    } else {
        promise.SetValue(TStatus::Fail(
            NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
            "Unsupported EXTERNAL_DATA_SOURCE operation"));
    }
    return future;
}

} // namespace NKikimr::NKqp::NExternalDataSource
