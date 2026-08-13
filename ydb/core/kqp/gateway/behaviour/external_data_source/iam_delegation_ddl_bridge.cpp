#include "iam_delegation_ddl_bridge.h"

#include "iam_object_lookup.h"

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/federated_query/actors/kqp_federated_query_actors.h>
#include <ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h>
#include <ydb/core/kqp/gateway/utils/metadata_helpers.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/async/wait_for_event.h>

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
};

constexpr ui64 IamObjectCookie = 101;
constexpr ui64 CloudIdCookie = 102;
constexpr ui64 SchemeRequestCookie = 103;

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
    auto& entry = navigate->ResultSet.emplace_back();
    entry.Path = NKikimr::SplitPath(path);
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown;
    entry.Kind = NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalDataSource;
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
                if (request.ResultSet.size() == 1) {
                    const auto& entry = request.ResultSet.front();
                    if (ClassifyIamObjectLookup(
                            entry.Status, static_cast<bool>(entry.ExternalDataSourceInfo)) ==
                        EIamObjectLookupResult::NotFound)
                    {
                        result.Description.NotFound = true;
                        promise.SetValue(std::move(result));
                        return;
                    }
                }
                if (request.ErrorCount || request.ResultSet.size() != 1 ||
                    request.ResultSet.front().Status !=
                        NSchemeCache::TSchemeCacheNavigate::EStatus::Ok ||
                    !request.ResultSet.front().ExternalDataSourceInfo)
                {
                    result.Description.Status = TStatus::Fail(
                        NYql::TIssuesIds::KIKIMR_SCHEME_ERROR,
                        "Cannot describe external data source for IAM delegation");
                    promise.SetValue(std::move(result));
                    return;
                }
                const auto& description =
                    request.ResultSet.front().ExternalDataSourceInfo->Description;
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
    co_return co_await AwaitLegacyDdl(SendSchemeRequest(schemeTx, context), replyTo);
}

} // namespace NKikimr::NKqp::NExternalDataSource
