#include "proxy.h"

#include "events.h"
#include "proxy_actor.h"

#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KESUS_PROXY

namespace NKikimr {
namespace NKesus {

using NSchemeCache::TSchemeCacheNavigate;

class TKesusProxyService : public TActor<TKesusProxyService> {
    enum ECacheState {
        CACHE_STATE_NEW,
        CACHE_STATE_RESOLVING,
        CACHE_STATE_ACTIVE,
    };

    struct TResolveReplyInfo {
        const TActorId Sender;
        const ui64 Cookie;

        TResolveReplyInfo(const TActorId& sender, ui64 cookie)
            : Sender(sender)
            , Cookie(cookie)
        {}
    };

    struct TCacheEntry {
        ECacheState State = CACHE_STATE_NEW;
        TVector<TString> KesusPath;
        ui64 TabletId = -1;
        TIntrusivePtr<TSecurityObject> SecurityObject;
        TActorId ProxyActor;
        NKikimrKesus::TKesusError LastError;
        TVector<TResolveReplyInfo> ResolveSubscribers;
    };

    struct TEvPrivate {
        enum EEv {
            EvResolveResult = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
        };

        struct TEvResolveResult : TEventLocal<TEvResolveResult, EvResolveResult> {
            TString KesusPath;
            THolder<TEvTxProxySchemeCache::TEvNavigateKeySetResult> Event;

            TEvResolveResult(const TString& kesusPath, THolder<TEvTxProxySchemeCache::TEvNavigateKeySetResult> event)
                : KesusPath(kesusPath)
                , Event(std::move(event))
            {}
        };
    };

    class TResolveActor;

private:
    THashMap<TString, TCacheEntry> Cache;

public:
    TKesusProxyService()
        : TActor(&TThis::StateWork)
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KESUS_PROXY_ACTOR;
    }

private:
    IActor* CreateResolveActor(const TString& database, const TString& kesusPath);

    void Handle(TEvKesusProxy::TEvResolveKesusProxy::TPtr& ev) {
        const auto* msg = ev->Get();
        const auto& ctx = TActivationContext::AsActorContext();
        YDB_LOG_CTX_TRACE(ctx, "Got TEvResolveKesusProxy for path",
            {"Quote", msg->KesusPath.Quote()});
        auto& entry = Cache[msg->KesusPath];
        switch (entry.State) {
            case CACHE_STATE_NEW:
                entry.KesusPath = SplitPath(msg->KesusPath);
                if (entry.KesusPath.empty()) {
                    YDB_LOG_CTX_DEBUG(ctx, "Not allowing requests with an empty KesusPath");
                    Send(ev->Sender,
                        new TEvKesusProxy::TEvProxyError(
                            Ydb::StatusIds::BAD_REQUEST,
                            "KesusPath cannot be empty"),
                        0, ev->Cookie);
                    Cache.erase(msg->KesusPath);
                    return;
                }
                YDB_LOG_CTX_DEBUG(ctx, "Created new entry for kesus",
                    {"Quote", msg->KesusPath.Quote()});
                [[fallthrough]];

            case CACHE_STATE_ACTIVE:
                // Recheck schemecache for changes
                YDB_LOG_CTX_TRACE(ctx, "Starting resolve for kesus",
                    {"Quote", msg->KesusPath.Quote()});
                RegisterWithSameMailbox(CreateResolveActor(msg->Database, msg->KesusPath));
                entry.State = CACHE_STATE_RESOLVING;
                [[fallthrough]];

            case CACHE_STATE_RESOLVING:
                // Wait for result from schemecache
                entry.ResolveSubscribers.emplace_back(ev->Sender, ev->Cookie);
                break;

            default:
                Y_UNREACHABLE();
        }
    }

    void Handle(TEvPrivate::TEvResolveResult::TPtr& ev) {
        const auto* msg = ev->Get();
        const auto& ctx = TActivationContext::AsActorContext();
        YDB_LOG_CTX_TRACE(ctx, "Got TEvResolveResult for path",
            {"Quote", msg->KesusPath.Quote()});
        auto& entry = Cache[msg->KesusPath];
        Y_ABORT_UNLESS(entry.State == CACHE_STATE_RESOLVING);
        Y_ABORT_UNLESS(msg->Event->Request->ResultSet.size() == 1);
        entry.State = CACHE_STATE_ACTIVE;
        const auto& result = msg->Event->Request->ResultSet.front();
        entry.LastError.Clear();
        switch (result.Status) {
            case TSchemeCacheNavigate::EStatus::Ok: {
                if (!result.KesusInfo) {
                    YDB_LOG_CTX_DEBUG(ctx, "Received an OK result for without KesusInfo: not found",
                        {"Quote", msg->KesusPath.Quote()});
                    entry.LastError.SetStatus(Ydb::StatusIds::NOT_FOUND);
                    entry.LastError.AddIssues()->set_message("Kesus not found");
                    break;
                }
                const auto& desc = result.KesusInfo->Description;
                const ui64 tabletId = desc.GetKesusTabletId();
                if (!tabletId) {
                    YDB_LOG_CTX_DEBUG(ctx, "Received an OK result for without tablet id: not found",
                        {"Quote", msg->KesusPath.Quote()});
                    entry.LastError.SetStatus(Ydb::StatusIds::NOT_FOUND);
                    entry.LastError.AddIssues()->set_message("Kesus not found");
                    break;
                }
                entry.LastError.SetStatus(Ydb::StatusIds::SUCCESS);
                entry.SecurityObject = result.SecurityObject;
                if (entry.ProxyActor && entry.TabletId != tabletId) {
                    // Kill the old proxy
                    YDB_LOG_CTX_DEBUG(ctx, "Tablet for changed -> : destroying the old proxy",
                        {"Quote", msg->KesusPath.Quote()},
                        {"TabletId", entry.TabletId},
                        {"tabletId", tabletId});
                    Send(entry.ProxyActor, new TEvents::TEvPoisonPill());
                    entry.ProxyActor = {};
                }
                if (!entry.ProxyActor) {
                    // Create a new proxy
                    YDB_LOG_CTX_INFO(ctx, "Creating kesus proxy for tablet and path",
                        {"tabletId", tabletId},
                        {"Quote", msg->KesusPath.Quote()});
                    entry.ProxyActor = Register(CreateKesusProxyActor(SelfId(), tabletId, msg->KesusPath));
                    entry.TabletId = tabletId;
                }
                break;
            }
            case TSchemeCacheNavigate::EStatus::RootUnknown:
            case TSchemeCacheNavigate::EStatus::PathErrorUnknown:
            case TSchemeCacheNavigate::EStatus::PathNotPath:
                YDB_LOG_CTX_TRACE(ctx, "Resolve did not find path",
                    {"Quote", msg->KesusPath.Quote()},
                    {"Status", result.Status});
                entry.LastError.SetStatus(Ydb::StatusIds::NOT_FOUND);
                entry.LastError.AddIssues()->set_message("Kesus not found");
                break;
            default:
                YDB_LOG_CTX_ERROR(ctx, "Kesus resolve",
                    {"failed", result.Status});
                entry.LastError.SetStatus(Ydb::StatusIds::INTERNAL_ERROR);
                entry.LastError.AddIssues()->set_message(ToString(result.Status));
                break;
        }
        if (entry.ProxyActor && entry.LastError.GetStatus() != Ydb::StatusIds::SUCCESS) {
            // Entry expired, kill the proxy
            YDB_LOG_CTX_INFO(ctx, "Destroying kesus proxy for path",
                {"Quote", msg->KesusPath.Quote()});
            Send(entry.ProxyActor, new TEvents::TEvPoisonPill());
            entry.ProxyActor = {};
        }
        for (const auto& subscriber : entry.ResolveSubscribers) {
            if (entry.LastError.GetStatus() != Ydb::StatusIds::SUCCESS) {
                Send(subscriber.Sender,
                    new TEvKesusProxy::TEvProxyError(entry.LastError),
                    0, subscriber.Cookie);
            } else {
                Send(subscriber.Sender,
                    new TEvKesusProxy::TEvAttachProxyActor(entry.ProxyActor, entry.SecurityObject),
                    0, subscriber.Cookie);
            }
        }
        entry.ResolveSubscribers.clear();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKesusProxy::TEvResolveKesusProxy, Handle);
            hFunc(TEvPrivate::TEvResolveResult, Handle);

            default:
                Y_ABORT("Unexpected event 0x%x for TKesusProxyService", ev->GetTypeRewrite());
        }
    }
};

class TKesusProxyService::TResolveActor : public TActorBootstrapped<TResolveActor> {
private:
    const TActorId Owner;
    const TString Database;
    const TString KesusPath;

public:
    TResolveActor(const TActorId& owner, const TString& database, const TString& kesusPath)
        : Owner(owner)
        , Database(database)
        , KesusPath(kesusPath)
    {}

    void Bootstrap(const TActorContext& ctx) {
        YDB_LOG_CTX_TRACE(ctx, "Sending resolve request",
            {"to_SchemeCache", KesusPath.Quote()});
        auto request = MakeHolder<TSchemeCacheNavigate>();
        request->DatabaseName = Database;

        auto& entry = request->ResultSet.emplace_back();
        entry.Path = SplitPath(KesusPath);
        entry.Operation = TSchemeCacheNavigate::OpPath;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        Become(&TThis::StateWork);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KESUS_RESOLVE_ACTOR;
    }

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& ctx = TActivationContext::AsActorContext();
        YDB_LOG_CTX_TRACE(ctx, "Forwarding resolve result",
            {"from_SchemeCache", KesusPath.Quote()});
        Send(Owner, new TEvPrivate::TEvResolveResult(KesusPath, THolder<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(ev->Release().Release())));
        PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);

            default:
                Y_ABORT("Unexpected event 0x%x for TKesusProxyService::TResolveActor", ev->GetTypeRewrite());
        }
    }
};

IActor* TKesusProxyService::CreateResolveActor(const TString& database, const TString& kesusPath) {
    return new TResolveActor(SelfId(), database, kesusPath);
}

TActorId MakeKesusProxyServiceId() {
    return TActorId(0, "kesus-proxy");
}

IActor* CreateKesusProxyService() {
    return new TKesusProxyService();
}

}
}
