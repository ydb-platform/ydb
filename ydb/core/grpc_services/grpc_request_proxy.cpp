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
#include <ydb/core/jaeger_tracing/sampling_throttling_control.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/scheme_board/scheme_board.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/core/protos/table_service_config.pb.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;

static const ui32 MAX_DEFERRED_EVENTS_PER_DATABASE = 100;

TString DatabaseFromDomain(const TAppData* appdata = AppData()) {
    auto dinfo = appdata->DomainsInfo;
    if (!dinfo)
        ythrow yexception() << "Invalid DomainsInfo ptr";
    if (!dinfo->Domain)
        ythrow yexception() << "No Domain defined";

    return TString("/") + dinfo->GetDomain()->Name;
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
    explicit TGRpcRequestProxyImpl(const NKikimrConfig::TAppConfig& appConfig, TIntrusivePtr<NJaegerTracing::TSamplingThrottlingControl> tracingControl)
        : ChannelBufferSize(appConfig.GetTableServiceConfig().GetResourceManager().GetChannelBufferSize())
        , TracingControl(std::move(tracingControl))
    { }

    void Bootstrap(const TActorContext& ctx);
    void StateFunc(TAutoPtr<IEventHandle>& ev);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_PROXY;
    }

private:
    void HandleConfig(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr& ev);
    void HandleConfig(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev);
    void HandleProxyService(TEvTxUserProxy::TEvGetProxyServicesResponse::TPtr& ev);
    void HandleUndelivery(TEvents::TEvUndelivered::TPtr& ev);
    void HandleSchemeBoard(TSchemeBoardEvents::TEvNotifyUpdate::TPtr& ev, const TActorContext& ctx);
    void HandleSchemeBoard(TSchemeBoardEvents::TEvNotifyDelete::TPtr& ev);
    void ReplayEvents(const TString& databaseName, const TActorContext& ctx);

    void MaybeStartTracing(IRequestProxyCtx& ctx);

    static bool IsAuthStateOK(const IRequestProxyCtx& ctx);

    template <typename TEvent>
    void Handle(TAutoPtr<TEventHandle<TEvent>>& event, const TActorContext& ctx) {
        IRequestProxyCtx* requestBaseCtx = event->Get();
        if (ValidateAndReplyOnError(requestBaseCtx)) {
            requestBaseCtx->FinishSpan();
            TGRpcRequestProxyHandleMethods::Handle(event, ctx);
        }
    }

    void Handle(TEvListEndpointsRequest::TPtr& event, const TActorContext& ctx) {
        IRequestProxyCtx* requestBaseCtx = event->Get();
        if (ValidateAndReplyOnError(requestBaseCtx)) {
            requestBaseCtx->FinishSpan();
            TGRpcRequestProxy::Handle(event, ctx);
        }
    }

    void Handle(TEvProxyRuntimeEvent::TPtr& event, const TActorContext&) {
        IRequestProxyCtx* requestBaseCtx = event->Get();
        if (ValidateAndReplyOnError(requestBaseCtx)) {
            requestBaseCtx->FinishSpan();
            event->Release().Release()->Pass(*this);
        }
    }

    template <ui32 TRpcId>
    void Handle(TAutoPtr<TEventHandle<TRefreshTokenImpl<TRpcId>>>& event, const TActorContext& ctx) {
        const auto record = event->Get();
        ctx.Send(record->GetFromId(), new TGRpcRequestProxy::TEvRefreshTokenResponse {
            record->GetAuthState().State == NYdbGrpc::TAuthState::EAuthState::AS_OK,
            record->GetInternalToken(),
            record->GetAuthState().State == NYdbGrpc::TAuthState::EAuthState::AS_UNAVAILABLE,
            NYql::TIssues()});
    }

    void Handle(TEvRequestAuthAndCheck::TPtr& ev, const TActorContext&) {
        ev->Get()->FinishSpan();
        ev->Get()->ReplyWithYdbStatus(Ydb::StatusIds::SUCCESS);
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

    template<class TEvent>
    void PreHandle(TAutoPtr<TEventHandle<TEvent>>& event, const TActorContext& ctx) {
        LogRequest(event);

        IRequestProxyCtx* requestBaseCtx = event->Get();
        if (!SchemeCache) {
            const TString error = "Grpc proxy is not ready to accept request, no proxy service";
            LOG_ERROR_S(ctx, NKikimrServices::GRPC_SERVER, error);
            const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR, error);
            requestBaseCtx->RaiseIssue(issue);
            requestBaseCtx->ReplyWithYdbStatus(Ydb::StatusIds::UNAVAILABLE);
            requestBaseCtx->FinishSpan();
            return;
        }

        MaybeStartTracing(*requestBaseCtx);

        if (IsAuthStateOK(*requestBaseCtx)) {
            Handle(event, ctx);
            return;
        }

        auto state = requestBaseCtx->GetAuthState();

        if (state.State == NYdbGrpc::TAuthState::AS_FAIL) {
            requestBaseCtx->ReplyUnauthenticated();
            requestBaseCtx->FinishSpan();
            return;
        }

        if (state.State == NYdbGrpc::TAuthState::AS_UNAVAILABLE) {
            Counters->IncDatabaseUnavailableCounter();
            const TString error = "Unable to resolve token";
            const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::YDB_AUTH_UNAVAILABLE, error);
            requestBaseCtx->RaiseIssue(issue);
            requestBaseCtx->ReplyWithYdbStatus(Ydb::StatusIds::UNAVAILABLE);
            requestBaseCtx->FinishSpan();
            return;
        }

        TString databaseName;
        const TDatabaseInfo* database = nullptr;
        bool skipResourceCheck = false;
        // do not check connect rights for the deprecated requests without database
        // remove this along with AllowYdbRequestsWithoutDatabase flag
        bool skipCheckConnectRigths = false;

        if (state.State == NYdbGrpc::TAuthState::AS_NOT_PERFORMED) {
            const auto& maybeDatabaseName = requestBaseCtx->GetDatabaseName();
            if (maybeDatabaseName && !maybeDatabaseName.GetRef().empty()) {
                databaseName = CanonizePath(maybeDatabaseName.GetRef());
            } else {
                if (!AllowYdbRequestsWithoutDatabase && DynamicNode && !std::is_same_v<TEvent, TEvRequestAuthAndCheck>) { // TEvRequestAuthAndCheck is allowed to be processed without database
                    requestBaseCtx->ReplyUnauthenticated("Requests without specified database are not allowed");
                    requestBaseCtx->FinishSpan();
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
                requestBaseCtx->FinishSpan();
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
                    requestBaseCtx->ReplyWithYdbStatus(Ydb::StatusIds::UNAVAILABLE);
                    requestBaseCtx->FinishSpan();
                    return;
                }
                return;
            }
        }

        if (database) {
            if (database->SchemeBoardResult) {
                const auto& domain = database->SchemeBoardResult->DescribeSchemeResult.GetPathDescription().GetDomainDescription();
                if (domain.HasResourcesDomainKey() && !skipResourceCheck && DynamicNode) {
                    const TSubDomainKey resourceDomainKey(domain.GetResourcesDomainKey());
                    const TSubDomainKey domainKey(domain.GetDomainKey());
                    if (!SubDomainKeys.contains(resourceDomainKey) && !SubDomainKeys.contains(domainKey)) {
                        TStringBuilder error;
                        error << "Unexpected node to perform query on database: " << databaseName
                              << ", domain: " << domain.GetDomainKey().ShortDebugString()
                              << ", resource domain: " << domain.GetResourcesDomainKey().ShortDebugString();
                        LOG_ERROR(ctx, NKikimrServices::GRPC_SERVER, error);
                        auto issue = MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, error);
                        requestBaseCtx->RaiseIssue(issue);
                        requestBaseCtx->ReplyWithYdbStatus(Ydb::StatusIds::UNAUTHORIZED);
                        requestBaseCtx->FinishSpan();
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
                requestBaseCtx->FinishSpan();
                return;
            }

            if (requestBaseCtx->IsClientLost()) {
                // Any status here
                LOG_DEBUG(*TlsActivationContext, NKikimrServices::GRPC_SERVER,
                    "Client was disconnected before processing request (grpc request proxy)");
                requestBaseCtx->ReplyWithYdbStatus(Ydb::StatusIds::UNAVAILABLE);
                requestBaseCtx->FinishSpan();
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
        requestBaseCtx->FinishSpan();
        return;
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
                req.Ctx->ReplyWithYdbStatus(Ydb::StatusIds::UNAVAILABLE);
                req.Ctx->FinishSpan();
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
    TIntrusivePtr<NJaegerTracing::TSamplingThrottlingControl> TracingControl;
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
    return state.State == NYdbGrpc::TAuthState::AS_OK ||
           state.State == NYdbGrpc::TAuthState::AS_FAIL && state.NeedAuth == false ||
           state.NeedAuth == false && !ctx.GetYdbToken();
}

void TGRpcRequestProxyImpl::MaybeStartTracing(IRequestProxyCtx& ctx) {
    auto isTracingDecided = ctx.IsTracingDecided();
    if (!isTracingDecided) {
        return;
    }
    if (std::exchange(*isTracingDecided, true)) {
        return;
    }

    NWilson::TTraceId traceId;
    if (const auto otelHeader = ctx.GetPeerMetaValues(NYdb::OTEL_TRACE_HEADER)) {
        traceId = NWilson::TTraceId::FromTraceparentHeader(otelHeader.GetRef(), TComponentTracingLevels::ProductionVerbose);
    }
    TracingControl->HandleTracing(traceId, ctx.GetRequestDiscriminator());
    if (traceId) {
        NWilson::TSpan grpcRequestProxySpan(TWilsonGrpc::RequestProxy, std::move(traceId), "GrpcRequestProxy");
        if (auto database = ctx.GetDatabaseName()) {
            grpcRequestProxySpan.Attribute("database", std::move(*database));
        }
        grpcRequestProxySpan.Attribute("request_type", ctx.GetRequestName());
        ctx.StartTracing(std::move(grpcRequestProxySpan));
    }
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
            queue.front().Ctx->FinishSpan();
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
        HFunc(TRefreshTokenGenericRequest, PreHandle);
        HFunc(TRefreshTokenStreamWriteSpecificRequest, PreHandle);
        HFunc(TEvListEndpointsRequest, PreHandle);
        HFunc(TEvBiStreamPingRequest, PreHandle);
        HFunc(TEvStreamPQWriteRequest, PreHandle);
        HFunc(TEvStreamPQMigrationReadRequest, PreHandle);
        HFunc(TEvStreamTopicWriteRequest, PreHandle);
        HFunc(TEvStreamTopicReadRequest, PreHandle);
        HFunc(TEvStreamTopicDirectReadRequest, PreHandle);
        HFunc(TEvCommitOffsetRequest, PreHandle);
        HFunc(TEvPQReadInfoRequest, PreHandle);
        HFunc(TEvDiscoverPQClustersRequest, PreHandle);
        HFunc(TEvCoordinationSessionRequest, PreHandle);
        HFunc(TEvNodeCheckRequest, PreHandle);
        HFunc(TEvProxyRuntimeEvent, PreHandle);
        HFunc(TEvRequestAuthAndCheck, PreHandle);

        default:
            Y_ABORT("Unknown request: %u\n", ev->GetTypeRewrite());
        break;
    }
}

IActor* CreateGRpcRequestProxy(const NKikimrConfig::TAppConfig& appConfig, TIntrusivePtr<NJaegerTracing::TSamplingThrottlingControl> tracingControl) {
    return new TGRpcRequestProxyImpl(appConfig, std::move(tracingControl));
}

} // namespace NGRpcService
} // namespace NKikimr
