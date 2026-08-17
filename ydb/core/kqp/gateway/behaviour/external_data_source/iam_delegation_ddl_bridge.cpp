#include "iam_delegation_ddl_bridge.h"

#include "iam_object_lookup.h"

#include <ydb/core/base/path.h>
#include <ydb/core/kqp/gateway/actors/kqp_ic_gateway_actors.h>
#include <ydb/core/kqp/gateway/utils/metadata_helpers.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/async/wait_for_event.h>

namespace NKikimr::NKqp::NExternalDataSource {
namespace {

using TContext = TExternalDataSourceManager::TExternalModificationContext;
using TStatus = TExternalDataSourceManager::TYqlConclusionStatus;

enum EBridgeEvent {
    EvIamObject = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
    EvCloudId,
    EvSchemeRequest,
    EvIamSchemeRequest,
};

// One event carries any bridged result back to the waiting actor. The event id
// keeps the four adapters distinguishable even where they carry the same type.
template <typename TPayload, ui32 EventId>
struct TEvBridgeResult : NActors::TEventLocal<TEvBridgeResult<TPayload, EventId>, EventId> {
    explicit TEvBridgeResult(TPayload payload)
        : Payload(std::move(payload))
    {}

    TPayload Payload;
};

// Deliver a future's value to `replyTo` as an event, then suspend the calling
// actor coroutine until it arrives. This is the whole of the bridge: no
// lifecycle decision is made in a future callback.
template <ui32 EventId, ui64 Cookie, typename TPayload>
NActors::async<TPayload> AwaitFuture(
    NThreading::TFuture<TPayload> future,
    const NActors::TActorId& replyTo)
{
    using TEvent = TEvBridgeResult<TPayload, EventId>;
    future.Subscribe(
        [actorSystem = TActivationContext::ActorSystem(), replyTo](const auto& ready) {
            actorSystem->Send(replyTo, new TEvent(ready.GetValue()), 0, Cookie);
        });
    const auto event = co_await NActors::ActorWaitForEvent<TEvent>(Cookie);
    co_return std::move(event->Get()->Payload);
}

// SchemeCache lookups answer through TActorRequestHandler, which reports
// transport failure on the gateway result rather than on our description.
template <typename TDescription>
struct TBridgeDescription : NYql::IKikimrGateway::TGenericResult {
    TDescription Description;
};

template <typename TDescription>
NThreading::TFuture<TDescription> UnwrapDescription(
    NThreading::TFuture<TBridgeDescription<TDescription>> future)
{
    return future.Apply([](const auto& result) {
        auto bridge = result.GetValue();
        if (!bridge.Success()) {
            bridge.Description.Status = TStatus::Fail(
                bridge.Status(), bridge.Issues().ToString());
        }
        return std::move(bridge.Description);
    });
}

using TBridgeIamObjectDescription = TBridgeDescription<TIamObjectDescription>;
using TBridgeCloudIdDescription = TBridgeDescription<TCloudIdDescription>;

constexpr ui64 IamObjectCookie = 101;
constexpr ui64 CloudIdCookie = 102;
constexpr ui64 SchemeRequestCookie = 103;
constexpr ui64 IamSchemeRequestCookie = 104;

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
    auto future = UnwrapDescription<TCloudIdDescription>(promise.GetFuture());
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

    // Ask for the object and its parent in one navigate: when the object is
    // absent, the parent still supplies the snapshot version the caller needs.
    // Keep both paths in locals - emplace_back reallocates ResultSet, so a
    // reference into it must not be read after the next entry is added.
    const TVector<TString> targetPath = NKikimr::SplitPath(path);
    TVector<TString> parentPath = targetPath;
    parentPath.pop_back();

    auto navigate = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    navigate->ResultSet.reserve(2);
    {
        auto& target = navigate->ResultSet.emplace_back();
        target.Path = targetPath;
        target.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown;
        target.Kind = NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalDataSource;
    }
    {
        auto& parent = navigate->ResultSet.emplace_back();
        parent.Path = std::move(parentPath);
        parent.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    }
    navigate->DatabaseName = context.GetDatabase();
    if (context.GetUserToken()) {
        navigate->UserToken = MakeIntrusive<NACLib::TUserToken>(*context.GetUserToken());
    }

    auto promise = NThreading::NewPromise<TBridgeIamObjectDescription>();
    auto future = UnwrapDescription<TIamObjectDescription>(promise.GetFuture());
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

NActors::async<TCloudIdDescription> DescribeDatabaseCloudId(
    const TContext& context,
    const NActors::TActorId& replyTo)
{
    co_return co_await AwaitFuture<EvCloudId, CloudIdCookie>(
        StartDatabaseCloudIdLookup(context), replyTo);
}

NActors::async<TIamObjectDescription> DescribeIamObject(
    const TString& path,
    const TContext& context,
    const NActors::TActorId& replyTo)
{
    co_return co_await AwaitFuture<EvIamObject, IamObjectCookie>(
        StartIamObjectLookup(path, context), replyTo);
}

NActors::async<TStatus> AwaitLegacyDdl(
    TExternalDataSourceManager::TAsyncStatus legacyDdl,
    const NActors::TActorId& replyTo)
{
    co_return co_await AwaitFuture<EvSchemeRequest, SchemeRequestCookie>(
        std::move(legacyDdl), replyTo);
}

NActors::async<TStatus> ExecuteIamSchemeRequest(
    const NKikimrSchemeOp::TModifyScheme& schemeTx,
    const TContext& context,
    const NActors::TActorId& replyTo)
{
    co_return co_await AwaitFuture<EvIamSchemeRequest, IamSchemeRequestCookie>(
        SendSchemeRequest(schemeTx, context), replyTo);
}

} // namespace NKikimr::NKqp::NExternalDataSource
