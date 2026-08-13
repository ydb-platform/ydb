#include "iam_delegation_ddl_bridge.h"

#include "iam_object_lookup.h"

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h>
#include <ydb/core/kqp/gateway/utils/metadata_helpers.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/async/wait_for_event.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>

namespace NKikimr::NKqp::NExternalDataSource {
namespace {

using TContext = TExternalDataSourceManager::TExternalModificationContext;
using TStatus = TExternalDataSourceManager::TYqlConclusionStatus;

struct TBridgeIamObjectDescription : NYql::IKikimrGateway::TGenericResult {
    TIamObjectDescription Description;
};

struct TBridgeCloudIdDescription : NYql::IKikimrGateway::TGenericResult {
    TCloudIdDescription Description;
};

struct TEvIamDelegationDdlBridge {
    enum EEv {
        EvIamObject = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvCloudId,
        EvSchemeRequest,
        EvIamSchemeRequest,
        EvSystemIamToken,
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

    struct TEvSchemeRequest : NActors::TEventLocal<TEvSchemeRequest, EvSchemeRequest> {
        explicit TEvSchemeRequest(TStatus status)
            : Status(std::move(status))
        {}

        TStatus Status;
    };

    struct TEvIamSchemeRequest
        : NActors::TEventLocal<TEvIamSchemeRequest, EvIamSchemeRequest>
    {
        explicit TEvIamSchemeRequest(TStatus status)
            : Status(std::move(status))
        {}

        TStatus Status;
    };

    struct TEvSystemIamToken
        : NActors::TEventLocal<TEvSystemIamToken, EvSystemIamToken>
    {
        explicit TEvSystemIamToken(TIamTokenResult result)
            : Result(std::move(result))
        {}

        TIamTokenResult Result;
    };
};

constexpr ui64 IamObjectCookie = 101;
constexpr ui64 CloudIdCookie = 102;
constexpr ui64 SchemeRequestCookie = 103;
constexpr ui64 IamSchemeRequestCookie = 104;
constexpr ui64 SystemIamTokenCookie = 105;

NThreading::TFuture<TCloudIdDescription> StartDatabaseCloudIdLookup(const TContext& context) {
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

    auto promise = NThreading::NewPromise<TBridgeCloudIdDescription>();
    auto future = promise.GetFuture().Apply([](const auto& result) {
        auto bridge = result.GetValue();
        if (!bridge.Success()) {
            bridge.Description.Status = TStatus::Fail(
                bridge.Status(), bridge.Issues().ToString());
        }
        return std::move(bridge.Description);
    });
    context.GetActorSystem()->Register(
        new TActorRequestHandler<TRequest, TResponse, TBridgeCloudIdDescription>(
            MakeSchemeCacheID(), new TRequest(navigate.Release()), promise,
            [](NThreading::TPromise<TBridgeCloudIdDescription> promise, TResponse&& response) {
                TBridgeCloudIdDescription result;
                result.SetSuccess();
                const auto& request = *response.Request;
                if (request.ErrorCount || request.ResultSet.size() != 1) {
                    result.Description.Status = TStatus::Fail(
                        NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                        "Cannot describe database for IAM delegation");
                } else if (const auto it = request.ResultSet.front().Attributes.find("cloud_id");
                           it != request.ResultSet.front().Attributes.end() && !it->second.empty())
                {
                    result.Description.CloudId = it->second;
                } else {
                    result.Description.Status = TStatus::Fail(
                        NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED,
                        "Database has no cloud_id attribute required by AUTH_METHOD=IAM");
                }
                promise.SetValue(std::move(result));
            }));
    return future;
}

NThreading::TFuture<TIamObjectDescription> StartIamObjectLookup(
    const TString& path,
    const TContext& context)
{
    using TRequest = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TResponse = TEvTxProxySchemeCache::TEvNavigateKeySetResult;

    auto navigate = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    auto& target = navigate->ResultSet.emplace_back();
    target.Path = NKikimr::SplitPath(path);
    target.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown;
    target.Kind = NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalDataSource;
    auto& parent = navigate->ResultSet.emplace_back();
    parent.Path = target.Path;
    parent.Path.pop_back();
    parent.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    navigate->DatabaseName = context.GetDatabase();
    if (context.GetUserToken()) {
        navigate->UserToken = MakeIntrusive<NACLib::TUserToken>(*context.GetUserToken());
    }

    auto promise = NThreading::NewPromise<TBridgeIamObjectDescription>();
    auto future = promise.GetFuture().Apply([](const auto& result) {
        auto bridge = result.GetValue();
        if (!bridge.Success()) {
            bridge.Description.Status = TStatus::Fail(
                bridge.Status(), bridge.Issues().ToString());
        }
        return std::move(bridge.Description);
    });
    context.GetActorSystem()->Register(
        new TActorRequestHandler<TRequest, TResponse, TBridgeIamObjectDescription>(
            MakeSchemeCacheID(), new TRequest(navigate.Release()), promise,
            [](NThreading::TPromise<TBridgeIamObjectDescription> promise, TResponse&& response) {
                TBridgeIamObjectDescription result;
                result.SetSuccess();
                const auto& request = *response.Request;
                if (request.ResultSet.size() != 2)
                {
                    result.Description.Status = TStatus::Fail(
                        NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                        "Cannot describe external data source for IAM delegation");
                    promise.SetValue(std::move(result));
                    return;
                }

                const auto& target = request.ResultSet[0];
                const auto lookup = ClassifyIamObjectLookup(
                    target.Status, static_cast<bool>(target.ExternalDataSourceInfo));
                const auto& snapshot = lookup == EIamObjectLookupResult::NotFound
                    ? request.ResultSet[1]
                    : target;
                if (lookup == EIamObjectLookupResult::Error ||
                    snapshot.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok ||
                    !snapshot.Self)
                {
                    result.Description.Status = TStatus::Fail(
                        NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                        "Cannot describe external data source for IAM delegation");
                    promise.SetValue(std::move(result));
                    return;
                }

                result.Description.SnapshotPathId = snapshot.Self->Info.GetPathId();
                result.Description.SnapshotPathVersion = snapshot.Self->Info.GetPathVersion();
                if (lookup == EIamObjectLookupResult::NotFound) {
                    result.Description.NotFound = true;
                    promise.SetValue(std::move(result));
                    return;
                }

                const auto& description =
                    target.ExternalDataSourceInfo->Description;
                if (!description.GetAuth().HasIam()) {
                    promise.SetValue(std::move(result));
                    return;
                }
                const auto& iam = description.GetAuth().GetIam();
                if (!iam.HasDelegationReferrerId()) {
                    promise.SetValue(std::move(result));
                    return;
                }
                result.Description.Delegation.ResourceId = iam.GetResourceId();
                result.Description.Delegation.ServiceAccountId = iam.GetServiceAccountId();
                result.Description.Delegation.ReferrerId = iam.GetDelegationReferrerId();
                promise.SetValue(std::move(result));
            }));
    return future;
}

} // anonymous namespace

NActors::async<TIamTokenResult> AcquireSystemIamToken(
    const TIamDelegationSettings& settings,
    const NActors::TActorId& replyTo)
{
    try {
        auto facility = NYdb::CreateSimpleCoreFacility();
        auto credentials = NYdb::CreateIamCredentialsProviderFactory(
            MakeMetadataServiceHost(settings))->CreateProvider(facility);
        credentials->GetAuthInfoAsync().Subscribe(
            [credentials = std::move(credentials),
             facility = std::move(facility),
             actorSystem = NActors::TActivationContext::ActorSystem(),
             replyTo](const auto& future) {
                // Keep the asynchronous provider and its callback facility
                // alive until token acquisition completes.
                Y_UNUSED(credentials);
                Y_UNUSED(facility);
                TIamTokenResult result;
                try {
                    result.Token = future.GetValue();
                    if (result.Token.empty()) {
                        result.Error = "metadata service returned an empty IAM token";
                    } else {
                        result.Success = true;
                    }
                } catch (...) {
                    result.Error = CurrentExceptionMessage();
                }
                actorSystem->Send(
                    replyTo,
                    new TEvIamDelegationDdlBridge::TEvSystemIamToken(
                        std::move(result)),
                    0,
                    SystemIamTokenCookie);
            });
    } catch (...) {
        co_return TIamTokenResult{
            .Error = CurrentExceptionMessage(),
        };
    }

    const auto event = co_await NActors::ActorWaitForEvent<
        TEvIamDelegationDdlBridge::TEvSystemIamToken>(SystemIamTokenCookie);
    co_return std::move(event->Get()->Result);
}

NActors::async<TCloudIdDescription> DescribeDatabaseCloudId(
    const TContext& context,
    const NActors::TActorId& replyTo)
{
    StartDatabaseCloudIdLookup(context).Subscribe(
        [actorSystem = TActivationContext::ActorSystem(), replyTo](const auto& result) {
            actorSystem->Send(
                replyTo,
                new TEvIamDelegationDdlBridge::TEvCloudId(result.GetValue()),
                0,
                CloudIdCookie);
        });
    const auto event = co_await NActors::ActorWaitForEvent<
        TEvIamDelegationDdlBridge::TEvCloudId>(CloudIdCookie);
    co_return std::move(event->Get()->Description);
}

NActors::async<TIamObjectDescription> DescribeIamObject(
    const TString& path,
    const TContext& context,
    const NActors::TActorId& replyTo)
{
    StartIamObjectLookup(path, context).Subscribe(
        [actorSystem = TActivationContext::ActorSystem(), replyTo](const auto& result) {
            actorSystem->Send(
                replyTo,
                new TEvIamDelegationDdlBridge::TEvIamObject(result.GetValue()),
                0,
                IamObjectCookie);
        });
    const auto event = co_await NActors::ActorWaitForEvent<
        TEvIamDelegationDdlBridge::TEvIamObject>(IamObjectCookie);
    co_return std::move(event->Get()->Description);
}

NActors::async<TStatus> AwaitLegacyDdl(
    TExternalDataSourceManager::TAsyncStatus legacyDdl,
    const NActors::TActorId& replyTo)
{
    legacyDdl.Subscribe(
        [actorSystem = TActivationContext::ActorSystem(), replyTo](const auto& result) {
            actorSystem->Send(
                replyTo,
                new TEvIamDelegationDdlBridge::TEvSchemeRequest(result.GetValue()),
                0,
                SchemeRequestCookie);
        });
    const auto event = co_await NActors::ActorWaitForEvent<
        TEvIamDelegationDdlBridge::TEvSchemeRequest>(SchemeRequestCookie);
    co_return std::move(event->Get()->Status);
}

NActors::async<TStatus> ExecuteIamSchemeRequest(
    const NKikimrSchemeOp::TModifyScheme& schemeTx,
    const TContext& context,
    const NActors::TActorId& replyTo)
{
    SendSchemeRequest(schemeTx, context).Subscribe(
        [actorSystem = TActivationContext::ActorSystem(), replyTo](const auto& result) {
            actorSystem->Send(
                replyTo,
                new TEvIamDelegationDdlBridge::TEvIamSchemeRequest(result.GetValue()),
                0,
                IamSchemeRequestCookie);
        });
    const auto event = co_await NActors::ActorWaitForEvent<
        TEvIamDelegationDdlBridge::TEvIamSchemeRequest>(IamSchemeRequestCookie);
    co_return std::move(event->Get()->Status);
}

} // namespace NKikimr::NKqp::NExternalDataSource
