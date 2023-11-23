#include "grpc_request_proxy.h"
#include "grpc_request_check_actor.h"
#include "local_rate_limiter.h"
#include "operation_helpers.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/nameservice.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/grpc_services/counters/proxy_counters.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/scheme_board/scheme_board.h>

#include <shared_mutex>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;

static const ui32 MAX_DEFERRED_EVENTS_PER_DATABASE = 100;

TString DatabaseFromDomain(const TAppData* appdata = AppData()) {
    auto dinfo = appdata->DomainsInfo;
    if (!dinfo)
        ythrow yexception() << "Invalid DomainsInfo ptr";
    if (dinfo->Domains.empty() || dinfo->Domains.size() != 1)
        ythrow yexception() << "Unexpected domains container size: "
                            << dinfo->Domains.size();
    if (!dinfo->Domains.begin()->second)
        ythrow yexception() << "Empty domain params";

    return TString("/") + dinfo->Domains.begin()->second->Name;
}

struct TDatabaseInfo {
    THolder<TSchemeBoardEvents::TEvNotifyUpdate> SchemeBoardResult;
    TIntrusivePtr<TSecurityObject> SecurityObject;

    bool IsDatabaseReady() const {
        return SchemeBoardResult != nullptr;
    }
};


struct TEventReqHolder {
    TEventReqHolder(TAutoPtr<IEventHandle> ev, IRequestProxyCtx* ctx)
        : Ev(std::move(ev))
        , Ctx(ctx)
    {}
    TAutoPtr<IEventHandle> Ev;
    IRequestProxyCtx* Ctx;
};

class TGRpcRequestProxyImpl
    : public TActorBootstrapped<TGRpcRequestProxyImpl>
    , public TGRpcRequestProxy
{
    using TBase = TActorBootstrapped<TGRpcRequestProxyImpl>;
public:
    explicit TGRpcRequestProxyImpl(const NKikimrConfig::TAppConfig& appConfig)
        : ChannelBufferSize(appConfig.GetTableServiceConfig().GetResourceManager().GetChannelBufferSize())
    { }

    void Bootstrap(const TActorContext& ctx);
    void StateFunc(TAutoPtr<IEventHandle>& ev);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_PROXY;
    }

private:
    void HandleRefreshToken(TRefreshTokenImpl::TPtr& ev, const TActorContext& ctx);
    void HandleConfig(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr& ev);
    void HandleConfig(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev);
    void HandleProxyService(TEvTxUserProxy::TEvGetProxyServicesResponse::TPtr& ev);
    void HandleUndelivery(TEvents::TEvUndelivered::TPtr& ev);
    void HandleSchemeBoard(TSchemeBoardEvents::TEvNotifyUpdate::TPtr& ev, const TActorContext& ctx);
    void HandleSchemeBoard(TSchemeBoardEvents::TEvNotifyDelete::TPtr& ev);
    void ReplayEvents(const TString& databaseName, const TActorContext& ctx);

    static bool IsAuthStateOK(const IRequestProxyCtx& ctx);

    template <typename TEvent>
    void Handle(TAutoPtr<TEventHandle<TEvent>>& event, const TActorContext& ctx) {
        IRequestProxyCtx* requestBaseCtx = event->Get();
        if (ValidateAndReplyOnError(requestBaseCtx)) {
            TGRpcRequestProxyHandleMethods::Handle(event, ctx);
        }
    }

    void Handle(TEvListEndpointsRequest::TPtr& event, const TActorContext& ctx) {
        IRequestProxyCtx* requestBaseCtx = event->Get();
        if (ValidateAndReplyOnError(requestBaseCtx)) {
            TGRpcRequestProxy::Handle(event, ctx);
        }
    }

    void Handle(TEvProxyRuntimeEvent::TPtr& event, const TActorContext&) {
        IRequestProxyCtx* requestBaseCtx = event->Get();
        if (ValidateAndReplyOnError(requestBaseCtx)) {
            event->Release().Release()->Pass(*this);
        }
    }

    void Handle(TRefreshTokenImpl::TPtr& event, const TActorContext& ctx) {
        const auto record = event->Get();
        ctx.Send(record->GetFromId(), new TGRpcRequestProxy::TEvRefreshTokenResponse {
            record->GetAuthState().State == NGrpc::TAuthState::EAuthState::AS_OK,
            record->GetInternalToken(),
            record->GetAuthState().State == NGrpc::TAuthState::EAuthState::AS_UNAVAILABLE,
            NYql::TIssues()});
    }

    // returns true and defer event if no updates for given database
    // otherwice returns false and leave event untouched
    template <typename TEvent>
    bool DeferAndStartUpdate(const TString& database, TAutoPtr<TEventHandle<TEvent>>& ev, IRequestProxyCtx* reqCtx) {
        std::deque<TEventReqHolder>& queue = DeferredEvents[database];
        if (queue.size() >= MAX_DEFERRED_EVENTS_PER_DATABASE) {
            return false;
        }

        if (queue.empty()) {
            DoStartUpdate(database);
        }

        queue.push_back(TEventReqHolder(ev.Release(), reqCtx));
        return true;
    }

    template <typename TEvent>
    void PreHandle(TAutoPtr<TEventHandle<TEvent>>& event, const TActorContext& ctx) {
        IRequestProxyCtx* requestBaseCtx = event->Get();

        LogRequest(event);

        if (!SchemeCache) {
            const TString error = "Grpc proxy is not ready to accept request, no proxy service";
            LOG_ERROR_S(ctx, NKikimrServices::GRPC_SERVER, error);
            const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR, error);
            requestBaseCtx->RaiseIssue(issue);
            requestBaseCtx->ReplyWithYdbStatus(Ydb::StatusIds::UNAVAILABLE);
            return;
        }

        if (IsAuthStateOK(*requestBaseCtx)) {
            Handle(event, ctx);
            return;
        }

        auto state = requestBaseCtx->GetAuthState();

        if (state.State == NGrpc::TAuthState::AS_FAIL) {
            requestBaseCtx->ReplyUnauthenticated();
            return;
        }

        if (state.State == NGrpc::TAuthState::AS_UNAVAILABLE) {
            Counters->IncDatabaseUnavailableCounter();
            const TString error = "Unable to resolve token";
            const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::YDB_AUTH_UNAVAILABLE, error);
            requestBaseCtx->RaiseIssue(issue);
            requestBaseCtx->ReplyUnavaliable();
            return;
        }

        TString databaseName;
        const TDatabaseInfo* database = nullptr;
        bool skipResourceCheck = false;
        // do not check connect rights for the deprecated requests without database
        // remove this along with AllowYdbRequestsWithoutDatabase flag
        bool skipCheckConnectRigths = false;

        if (state.State == NGrpc::TAuthState::AS_NOT_PERFORMED) {
            const auto& maybeDatabaseName = requestBaseCtx->GetDatabaseName();
            if (maybeDatabaseName && !maybeDatabaseName.GetRef().empty()) {
                databaseName = CanonizePath(maybeDatabaseName.GetRef());
            } else {
                if (!AllowYdbRequestsWithoutDatabase && DynamicNode) {
                    requestBaseCtx->ReplyUnauthenticated("Requests without specified database is not allowed");
                    return;
                } else {
                    databaseName = RootDatabase;
                    skipResourceCheck = true;
                    skipCheckConnectRigths = true;
                }
            }
            if (databaseName.empty()) {
                Counters->IncDatabaseUnavailableCounter();
                requestBaseCtx->ReplyUnauthenticated("Empty database name");
                return;
            }
            auto it = Databases.find(databaseName);
            if (it != Databases.end() && it->second.IsDatabaseReady()) {
                database = &it->second;
            } else {
                // No given database found, start update if possible
                if (!DeferAndStartUpdate(databaseName, event, requestBaseCtx)) {
                    Counters->IncDatabaseUnavailableCounter();
                    const TString error = "Grpc proxy is not ready to accept request, database unknown";
                    LOG_ERROR(ctx, NKikimrServices::GRPC_SERVER, "Limit for deferred events per database %s reached", databaseName.c_str());
                    const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::YDB_DB_NOT_READY, error);
                    requestBaseCtx->RaiseIssue(issue);
                    requestBaseCtx->ReplyUnavaliable();
                }
                return;
            }
        }

        if (database) {
            if (database->SchemeBoardResult) {
                const auto& domain = database->SchemeBoardResult->DescribeSchemeResult.GetPathDescription().GetDomainDescription();
                if (domain.HasResourcesDomainKey() && !skipResourceCheck && DynamicNode) {
                    TSubDomainKey subdomainKey(domain.GetResourcesDomainKey());
                    if (!SubDomainKeys.contains(subdomainKey)) {
                        TStringBuilder error;
                        error << "Unexpected node to perform query on database: " << databaseName
                              << ", resource domain: " << domain.GetResourcesDomainKey().ShortDebugString();
                        LOG_ERROR(ctx, NKikimrServices::GRPC_SERVER, error);
                        auto issue = MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
                        requestBaseCtx->RaiseIssue(issue);
                        requestBaseCtx->ReplyWithYdbStatus(Ydb::StatusIds::UNAUTHORIZED);
                        return;
                    }
                }
                if (domain.GetDomainState().GetDiskQuotaExceeded()) {
                    requestBaseCtx->SetDiskQuotaExceeded(true);
                }
            } else {
                Counters->IncDatabaseUnavailableCounter();
                auto issue = MakeIssue(NKikimrIssues::TIssuesIds::YDB_DB_NOT_READY, "database unavailable");
                requestBaseCtx->RaiseIssue(issue);
                requestBaseCtx->ReplyWithYdbStatus(Ydb::StatusIds::UNAVAILABLE);
                return;
            }

            if (requestBaseCtx->IsClientLost()) {
                // Any status here
                LOG_DEBUG(*TlsActivationContext, NKikimrServices::GRPC_SERVER,
                    "Client was disconnected before processing request (grpc request proxy)");
                requestBaseCtx->ReplyWithYdbStatus(Ydb::StatusIds::UNAVAILABLE);
                return;
            }

            Register(CreateGrpcRequestCheckActor<TEvent>(SelfId(),
                database->SchemeBoardResult->DescribeSchemeResult,
                database->SecurityObject,
                event.Release(),
                Counters,
                skipCheckConnectRigths,
                this));
            return;
        }

        // in case we somehow skipped all auth checks
        const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR, "Can't authenticate request");
        requestBaseCtx->RaiseIssue(issue);
        requestBaseCtx->ReplyWithYdbStatus(Ydb::StatusIds::BAD_REQUEST);
    }

    void ForgetDatabase(const TString& database);
    void SubscribeToDatabase(const TString& database);
    void DoStartUpdate(const TString& database);
    bool DeferAndStartUpdate(const TString& database, TAutoPtr<IEventHandle>& ev, IRequestProxyCtx*);

    ui64 GetChannelBufferSize() const override {
        return ChannelBufferSize.load();
    }

    TActorId RegisterActor(IActor* actor) const override {
        return TActivationContext::AsActorContext().Register(actor);
    }

    virtual void PassAway() override {
        for (auto& [_, queue] : DeferredEvents) {
            for (TEventReqHolder& req : queue) {
                req.Ctx->ReplyUnavaliable();
            }
        }

        for (const auto& [_, actor] : Subscribers) {
            Send(actor, new TEvents::TEvPoisonPill());
        }

        if (DiscoveryCacheActorID) {
            Send(DiscoveryCacheActorID, new TEvents::TEvPoisonPill());
        }

        TBase::PassAway();
    }

    std::unordered_map<TString, TDatabaseInfo> Databases;
    std::unordered_map<TString, std::deque<TEventReqHolder>> DeferredEvents; // Events deferred to handle after getting database info
    std::unordered_map<TString, TActorId> Subscribers;
    THashSet<TSubDomainKey> SubDomainKeys;
    bool AllowYdbRequestsWithoutDatabase = true;
    std::atomic<ui64> ChannelBufferSize;
    TActorId SchemeCache;
    bool DynamicNode = false;
    TString RootDatabase;
    IGRpcProxyCounters::TPtr Counters;
};

void TGRpcRequestProxyImpl::Bootstrap(const TActorContext& ctx) {
    AllowYdbRequestsWithoutDatabase = AppData(ctx)->FeatureFlags.GetAllowYdbRequestsWithoutDatabase();

    // Subscribe for TableService config changes
    ui32 tableServiceConfigKind = (ui32) NKikimrConsole::TConfigItem::TableServiceConfigItem;
    Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
         new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({tableServiceConfigKind}),
         IEventHandle::FlagTrackDelivery);

    Send(MakeTxProxyID(), new TEvTxUserProxy::TEvGetProxyServicesRequest);

    auto nodeID = SelfId().NodeId();

    if (AppData()->DynamicNameserviceConfig) {
        DynamicNode = nodeID > AppData()->DynamicNameserviceConfig->MaxStaticNodeId;
        LOG_NOTICE(ctx, NKikimrServices::GRPC_SERVER, "Grpc request proxy started, nodeid# %d, serve as %s node",
            nodeID, (DynamicNode ? "dynamic" : "static"));
    }

    Counters = CreateGRpcProxyCounters(AppData()->Counters);
    InitializeGRpcProxyDbCountersRegistry(ctx.ActorSystem());

    RootDatabase = DatabaseFromDomain();
    Y_ABORT_UNLESS(!RootDatabase.empty());
    Databases.try_emplace(RootDatabase);
    DoStartUpdate(RootDatabase);

    if (RootDatabase != AppData()->TenantName && !AppData()->TenantName.empty()) {
        Databases.try_emplace(AppData()->TenantName);
        DoStartUpdate(AppData()->TenantName);
    }

    Become(&TThis::StateFunc);
}

void TGRpcRequestProxyImpl::ReplayEvents(const TString& databaseName, const TActorContext&) {
    auto itDeferredEvents = DeferredEvents.find(databaseName);
    if (itDeferredEvents != DeferredEvents.end()) {
        std::deque<TEventReqHolder>& queue = itDeferredEvents->second;
        std::deque<TEventReqHolder> deferredEvents;
        std::swap(deferredEvents, queue); // we can put back event to DeferredEvents queue in StateFunc KIKIMR-12851
        while (!deferredEvents.empty()) {
            StateFunc(deferredEvents.front().Ev);
            deferredEvents.pop_front();
        }
        if (queue.empty()) {
            DeferredEvents.erase(itDeferredEvents);
        }
    }
}

void TGRpcRequestProxyImpl::HandleRefreshToken(TRefreshTokenImpl::TPtr& ev, const TActorContext& ctx) {
    const auto record = ev->Get();
    ctx.Send(record->GetFromId(), new TGRpcRequestProxy::TEvRefreshTokenResponse {
        record->GetAuthState().State == NGrpc::TAuthState::EAuthState::AS_OK,
        record->GetInternalToken(),
        record->GetAuthState().State == NGrpc::TAuthState::EAuthState::AS_UNAVAILABLE,
        NYql::TIssues()});
}

void TGRpcRequestProxyImpl::HandleConfig(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
    LOG_INFO(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "Subscribed for config changes");
}

void TGRpcRequestProxyImpl::HandleConfig(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
    auto &event = ev->Get()->Record;

    ChannelBufferSize.store(
        event.GetConfig().GetTableServiceConfig().GetResourceManager().GetChannelBufferSize());
    LOG_INFO(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "Updated app config");

    auto responseEv = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
    Send(ev->Sender, responseEv.Release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
}

void TGRpcRequestProxyImpl::HandleProxyService(TEvTxUserProxy::TEvGetProxyServicesResponse::TPtr& ev) {
    SchemeCache = ev->Get()->Services.SchemeCache;
    LOG_DEBUG(*TlsActivationContext, NKikimrServices::GRPC_SERVER,
        "Got proxy service configuration");
}

void TGRpcRequestProxyImpl::HandleUndelivery(TEvents::TEvUndelivered::TPtr& ev) {
    switch (ev->Get()->SourceType) {
        case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
            LOG_CRIT(*TlsActivationContext, NKikimrServices::GRPC_SERVER,
                "Failed to deliver subscription request to config dispatcher");
            break;
        case NConsole::TEvConsole::EvConfigNotificationResponse:
            LOG_ERROR(*TlsActivationContext, NKikimrServices::GRPC_SERVER,
                "Failed to deliver config notification response");
            break;
        default:
            LOG_ERROR(*TlsActivationContext, NKikimrServices::GRPC_SERVER,
                "Undelivered event with unexpected source type: %d", ev->Get()->SourceType);
            break;
    }
}

bool TGRpcRequestProxyImpl::IsAuthStateOK(const IRequestProxyCtx& ctx) {
    const auto& state = ctx.GetAuthState();
    return state.State == NGrpc::TAuthState::AS_OK ||
           state.State == NGrpc::TAuthState::AS_FAIL && state.NeedAuth == false ||
           state.NeedAuth == false && !ctx.GetYdbToken();
}

void TGRpcRequestProxyImpl::HandleSchemeBoard(TSchemeBoardEvents::TEvNotifyUpdate::TPtr& ev, const TActorContext& ctx) {
    TString databaseName = ev->Get()->Path;
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "SchemeBoardUpdate " << databaseName);
    auto itDatabase = Databases.try_emplace(CanonizePath(databaseName));
    TDatabaseInfo& database = itDatabase.first->second;
    database.SchemeBoardResult = ev->Release();
    const NKikimrScheme::TEvDescribeSchemeResult& describeScheme(database.SchemeBoardResult->DescribeSchemeResult);
    database.SecurityObject = new TSecurityObject(describeScheme.GetPathDescription().GetSelf().GetOwner(),
        describeScheme.GetPathDescription().GetSelf().GetEffectiveACL(), false);

    if (databaseName == AppData()->TenantName
        && describeScheme.GetPathDescription().HasDomainDescription()
        && describeScheme.GetPathDescription().GetDomainDescription().HasDomainKey())
    {
        auto& domainKey = describeScheme.GetPathDescription().GetDomainDescription().GetDomainKey();
        SubDomainKeys.insert(TSubDomainKey(domainKey));
    }

    if (describeScheme.GetPathDescription().HasDomainDescription()
        && describeScheme.GetPathDescription().GetDomainDescription().HasSecurityState()) {
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "Updating SecurityState for " << databaseName);
        Send(MakeTicketParserID(), new TEvTicketParser::TEvUpdateLoginSecurityState(
            describeScheme.GetPathDescription().GetDomainDescription().GetSecurityState()
            ));
    } else {
        if (!describeScheme.GetPathDescription().HasDomainDescription()) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "Can't update SecurityState for " << databaseName << " - no DomainDescription");
        } else if (!describeScheme.GetPathDescription().GetDomainDescription().HasSecurityState()) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "Can't update SecurityState for " << databaseName << " - no SecurityState");
        }
    }

    if (database.IsDatabaseReady()) {
        ReplayEvents(databaseName, ctx);
    }
}

void TGRpcRequestProxyImpl::HandleSchemeBoard(TSchemeBoardEvents::TEvNotifyDelete::TPtr& ev) {
    LOG_WARN_S(*TlsActivationContext, NKikimrServices::GRPC_SERVER,
        "SchemeBoardDelete " << ev->Get()->Path << " Strong=" << ev->Get()->Strong);

    if (ev->Get()->Strong) {
        ForgetDatabase(ev->Get()->Path);
    }
}

void TGRpcRequestProxyImpl::ForgetDatabase(const TString& database) {
    auto itSubscriber = Subscribers.find(database);
    if (itSubscriber != Subscribers.end()) {
        Send(itSubscriber->second, new TEvents::TEvPoisonPill());
        Subscribers.erase(itSubscriber);
    }
    auto itDeferredEvents = DeferredEvents.find(database);
    if (itDeferredEvents != DeferredEvents.end()) {
        auto& queue(itDeferredEvents->second);
        while (!queue.empty()) {
            Counters->IncDatabaseUnavailableCounter();
            queue.front().Ctx->ReplyUnauthenticated("Unknown database");
            queue.pop_front();
        }
        DeferredEvents.erase(itDeferredEvents);
    }
    Databases.erase(database);
}

void TGRpcRequestProxyImpl::SubscribeToDatabase(const TString& database) {
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "Subscribe to " << database);

    TActorId subscriberId = Register(CreateSchemeBoardSubscriber(SelfId(), database));
    auto itSubscriber = Subscribers.emplace(database, subscriberId);
    if (!itSubscriber.second) {
        Send(itSubscriber.first->second, new TEvents::TEvPoisonPill());
        itSubscriber.first->second = subscriberId;
    }
}

void TGRpcRequestProxyImpl::DoStartUpdate(const TString& database) {
    // we will receive update (or delete) upon sucessfull subscription
    SubscribeToDatabase(database);
}

template<typename TEvent>
void LogRequest(const TEvent& event) {
    auto getDebugString = [&event]()->TString {
        TStringStream ss;
        ss << "Got grpc request# " << event->Get()->GetRequestName();
        ss << ", traceId# " << event->Get()->GetTraceId().GetOrElse("undef");
        ss << ", sdkBuildInfo# " << event->Get()->GetSdkBuildInfo().GetOrElse("undef");
        ss << ", state# " << event->Get()->GetAuthState().State;
        ss << ", database# " << event->Get()->GetDatabaseName().GetOrElse("undef");
        ss << ", peer# " << event->Get()->GetPeerName();
        ss << ", grpcInfo# " << event->Get()->GetGrpcUserAgent().GetOrElse("undef");
        if (event->Get()->GetDeadline() == TInstant::Max()) {
            ss << ", timeout# undef";
        } else {
            ss << ", timeout# " << event->Get()->GetDeadline() - TInstant::Now();
        }
        return ss.Str();
    };

    if constexpr (std::is_same_v<TEvListEndpointsRequest::TPtr, TEvent>) {
        LOG_INFO(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "%s", getDebugString().c_str());
    }
    else {
        LOG_DEBUG(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "%s", getDebugString().c_str());
    }
}

void TGRpcRequestProxyImpl::StateFunc(TAutoPtr<IEventHandle>& ev) {
    bool handled = true;
    // handle internal events
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvTxUserProxy::TEvGetProxyServicesResponse, HandleProxyService);
        hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleConfig);
        hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleConfig);
        hFunc(TEvents::TEvUndelivered, HandleUndelivery);
        HFunc(TSchemeBoardEvents::TEvNotifyUpdate, HandleSchemeBoard);
        hFunc(TSchemeBoardEvents::TEvNotifyDelete, HandleSchemeBoard);

        default:
            handled = false;
            break;
    }

    if (handled) {
        return;
    }

    // handle external events
    switch (ev->GetTypeRewrite()) {
        HFunc(TRefreshTokenImpl, PreHandle);
        HFunc(TEvLoginRequest, PreHandle);
        HFunc(TEvListEndpointsRequest, PreHandle);
        HFunc(TEvBiStreamPingRequest, PreHandle);
        HFunc(TEvStreamPQWriteRequest, PreHandle);
        HFunc(TEvStreamPQMigrationReadRequest, PreHandle);
        HFunc(TEvStreamTopicWriteRequest, PreHandle);
        HFunc(TEvStreamTopicReadRequest, PreHandle);
        HFunc(TEvCommitOffsetRequest, PreHandle);
        HFunc(TEvPQReadInfoRequest, PreHandle);
        HFunc(TEvPQDropTopicRequest, PreHandle);
        HFunc(TEvPQCreateTopicRequest, PreHandle);
        HFunc(TEvPQAlterTopicRequest, PreHandle);
        HFunc(TEvPQAddReadRuleRequest, PreHandle);
        HFunc(TEvPQRemoveReadRuleRequest, PreHandle);
        HFunc(TEvPQDescribeTopicRequest, PreHandle);
        HFunc(TEvDiscoverPQClustersRequest, PreHandle);
        HFunc(TEvCoordinationSessionRequest, PreHandle);
        HFunc(TEvDropTopicRequest, PreHandle);
        HFunc(TEvCreateTopicRequest, PreHandle);
        HFunc(TEvAlterTopicRequest, PreHandle);
        HFunc(TEvDescribeTopicRequest, PreHandle);
        HFunc(TEvDescribeConsumerRequest, PreHandle);
        HFunc(TEvDescribePartitionRequest, PreHandle);
        HFunc(TEvNodeCheckRequest, PreHandle);
        HFunc(TEvProxyRuntimeEvent, PreHandle);

        default:
            Y_ABORT("Unknown request: %u\n", ev->GetTypeRewrite());
        break;
    }
}

IActor* CreateGRpcRequestProxy(const NKikimrConfig::TAppConfig& appConfig) {
    return new TGRpcRequestProxyImpl(appConfig);
}

} // namespace NGRpcService
} // namespace NKikimr
