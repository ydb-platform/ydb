#include "etcd_watch.h"
#include "etcd_shared.h"
#include "etcd_events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NEtcd {

using namespace NActors;

namespace {

class TWatchman : public TActorBootstrapped<TWatchman> {
public:
    using IStreamCtx = NKikimr::NGRpcServer::IGRpcStreamingContext<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>;

    TWatchman(TIntrusivePtr<IStreamCtx> ctx, TActorId watchtower)
        : Ctx(std::move(ctx)), Watchtower(watchtower)
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateFunc);
        Ctx->Attach(ctx.SelfID);
        if (!Ctx->Read())
            return Die(ctx);
    }
private:
    struct TSubscription {
        using TPtr = std::shared_ptr<TSubscription>;
        using TWeakPtr = std::weak_ptr<TSubscription>;

        i64 FromRevision = 0LL;
        i64 WatchId = 0LL;
        bool SendProgress = false;
        bool WithPrevious = false;
        bool MakeFragmets = false;
        EWatchKind Kind = EWatchKind::Unsubscribe;
    };

    using TSubscriptionsMap = std::multimap<std::pair<std::string, std::string>, TSubscription::TWeakPtr>;
    using TUserSubscriptionsMap = std::unordered_multimap<i64, TSubscription::TPtr>;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TEvWakeup::EventType, Wakeup);

            HFunc(IStreamCtx::TEvReadFinished, Handle);
            HFunc(IStreamCtx::TEvWriteFinished, Handle);
            HFunc(IStreamCtx::TEvNotifiedWhenDone, Handle);

            HFunc(TEvChange, Handle);
        }
    }

    void Create(const etcdserverpb::WatchCreateRequest& req, etcdserverpb::WatchResponse& res, const TActorContext& ctx)  {
        const auto& key = req.key();
        const auto& rangeEnd = NEtcd::DecrementKey(req.range_end());
        const auto watchId = req.watch_id();

        const auto& sub = UserSubscriptionsMap.emplace(watchId, std::make_shared<TSubscription>())->second;
        SubscriptionsMap.emplace(std::make_pair(key, rangeEnd), sub);

        sub->FromRevision = req.start_revision();
        sub->WithPrevious = req.prev_kv();
        sub->WatchId = req.watch_id();
        sub->MakeFragmets = req.fragment();
        sub->SendProgress = req.progress_notify();
        bool ignoreUpdate = false, ignoreDelete = false;
        for (const auto f : req.filters()) {
            switch (f) {
                case etcdserverpb::WatchCreateRequest_FilterType_NOPUT: ignoreUpdate = true; break;
                case etcdserverpb::WatchCreateRequest_FilterType_NODELETE: ignoreDelete = false; break;
                default: break;
            }
        }

        if (!(ignoreUpdate && ignoreDelete)) {
            if (ignoreDelete && !ignoreUpdate)
                sub->Kind = EWatchKind::OnUpdates;
            else if (ignoreUpdate && !ignoreDelete)
                sub->Kind = EWatchKind::OnDeletions;
            else
                sub->Kind = EWatchKind::OnChanges;

            ctx.Send(Watchtower, new TEvSubscribe(key, rangeEnd, sub->Kind, sub->WithPrevious));
        }

        res.set_created(true);
        if (sub->WatchId) {
            res.set_watch_id(sub->WatchId);
        }

        if (!Ctx->Write(std::move(res)))
            return UnsubscribeAndDie(ctx);
    }

    void Cancel(const etcdserverpb::WatchCancelRequest& req, etcdserverpb::WatchResponse& res, const TActorContext& ctx)  {
        const auto watchId = req.watch_id();
        const auto range = UserSubscriptionsMap.equal_range(watchId);

        for (auto it = range.first; range.second != it; ++it) {
            Y_UNUSED(it);
        }

        UserSubscriptionsMap.erase(range.first, range.second);

        res.set_canceled(true);
        if (watchId)
            res.set_watch_id(watchId);

        if (!Ctx->Write(std::move(res)))
            return UnsubscribeAndDie(ctx);
    }

    void Progress(const etcdserverpb::WatchProgressRequest&, etcdserverpb::WatchResponse& res, const TActorContext& ctx)  {
        if (!Ctx->Write(std::move(res)))
            return UnsubscribeAndDie(ctx);
    }

    void Wakeup(const TActorContext&) {
    }

    void Handle(TEvChange::TPtr& ev, const TActorContext& ctx) {
        const auto range = SubscriptionsMap.equal_range(std::make_pair(ev->Get()->Key, std::string()));
        for (auto it = range.first; range.second != it; ++it) {
            if (const auto sub = it->second.lock()) {
                if (EWatchKind::OnChanges == sub->Kind ||
                    (ev->Get()->NewData.Version ? EWatchKind::OnUpdates : EWatchKind::OnDeletions) == sub->Kind) {
                    etcdserverpb::WatchResponse res;
                    const auto header = res.mutable_header();
                    header->set_revision(TSharedStuff::Get()->Revision.load());
                    header->set_cluster_id(0ULL);
                    header->set_member_id(0ULL);
                    header->set_raft_term(0ULL);

                    const auto event = res.add_events();
                    event->set_type(ev->Get()->NewData.Version ? mvccpb::Event_EventType_PUT : mvccpb::Event_EventType_DELETE);

                    if (sub->WithPrevious) {
                        const auto kv = event->mutable_prev_kv();
                        kv->set_key(ev->Get()->Key);
                        kv->set_value(ev->Get()->OldData.Value);
                        kv->set_version(ev->Get()->OldData.Version);
                        kv->set_lease(ev->Get()->OldData.Lease);
                        kv->set_mod_revision(ev->Get()->OldData.Modified);
                        kv->set_create_revision(ev->Get()->OldData.Created);
                    }

                    const auto kv = event->mutable_kv();
                    kv->set_key(ev->Get()->Key);
                    if (ev->Get()->NewData.Version) {
                        kv->set_value(ev->Get()->NewData.Value);
                        kv->set_version(ev->Get()->NewData.Version);
                        kv->set_lease(ev->Get()->NewData.Lease);
                        kv->set_mod_revision(ev->Get()->NewData.Modified);
                        kv->set_create_revision(ev->Get()->NewData.Created);
                    }

                    if (sub->WatchId)
                        res.set_watch_id(sub->WatchId);

                    if (!Ctx->Write(std::move(res)))
                        return UnsubscribeAndDie(ctx);
                }
            }
        }
    }

    void Handle(IStreamCtx::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
        if (!ev->Get()->Success)
            return Die(ctx);

        etcdserverpb::WatchResponse response;
        const auto header = response.mutable_header();
        header->set_revision(TSharedStuff::Get()->Revision.load());
        header->set_cluster_id(0ULL);
        header->set_member_id(0ULL);
        header->set_raft_term(0ULL);

        switch (const auto& req = ev->Get()->Record; req.request_union_case()) {
            case etcdserverpb::WatchRequest::RequestUnionCase::kCreateRequest:
                return Create(req.create_request(), response, ctx);
            case etcdserverpb::WatchRequest::RequestUnionCase::kCancelRequest:
                return Cancel(req.cancel_request(), response, ctx);
            case etcdserverpb::WatchRequest::RequestUnionCase::kProgressRequest:
                return Progress(req.progress_request(), response, ctx);
            default:
                break;
        }

        if (!Ctx->Read())
            return UnsubscribeAndDie(ctx);
    }

    void Handle(IStreamCtx::TEvWriteFinished::TPtr& ev, const TActorContext& ctx) {
        if (!ev->Get()->Success)
            return UnsubscribeAndDie(ctx);
    }

    void Handle(IStreamCtx::TEvNotifiedWhenDone::TPtr& ev, const TActorContext& ctx) {
        Cerr << (ev->Get()->Success ? "Finished." : "Failed!") << Endl;
        return UnsubscribeAndDie(ctx);
    }

    void UnsubscribeAndDie(const TActorContext& ctx) {
        ctx.Send(Watchtower, new TEvSubscribe);
        return Die(ctx);
    }

    const TIntrusivePtr<IStreamCtx> Ctx;
    const TActorId Watchtower;
    TSubscriptionsMap SubscriptionsMap;
    TUserSubscriptionsMap UserSubscriptionsMap;
};

class TWatchtower : public TActorBootstrapped<TWatchtower> {
public:
    TWatchtower(TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
        : Counters(std::move(counters))
    {}

    void Bootstrap(const TActorContext&) {
        Become(&TThis::StateFunc);
        TSharedStuff::Get()->Watchtower = SelfId();
    }

private:
    struct TSubscriptions {
        using TPtr = std::shared_ptr<TSubscriptions>;
        using TWeakPtr = std::weak_ptr<TSubscriptions>;

        TSubscriptions(const TActorId& watchman) : Watchman(watchman) {}
        const TActorId Watchman;
        std::set<std::pair<std::string, std::string>> Subscriptions;
    };

    using TSubscriptionsMap = std::multimap<std::pair<std::string, std::string>, TSubscriptions::TWeakPtr>;
    using TWatchmanSubscriptionsMap = std::unordered_map<TActorId, TSubscriptions::TPtr>;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvWatchRequest, Handle);

            HFunc(TEvSubscribe, Handle);

            HFunc(TEvChange, Handle);
        }
    }

    void Handle(TEvWatchRequest::TPtr& ev, const TActorContext& ctx) {
        ctx.RegisterWithSameMailbox(new TWatchman(ev->Get()->ReleaseStreamCtx(), ctx.SelfID));
    }

    void Handle(TEvSubscribe::TPtr& ev, const TActorContext&) {
        if (EWatchKind::Unsubscribe == ev->Get()->Kind && ev->Get()->Key.empty() && ev->Get()->RangeEnd.empty()) {
            WatchmanSubscriptionsMap.erase(ev->Sender);
            return;
        }

        const auto ins = WatchmanSubscriptionsMap.emplace(ev->Sender, nullptr);
        if (ins.second)
            ins.first->second = std::make_shared<TSubscriptions>(ev->Sender);

        if (EWatchKind::Unsubscribe != ev->Get()->Kind) {
            const auto& key = std::make_pair(ev->Get()->Key, ev->Get()->RangeEnd);
            if (ins.first->second->Subscriptions.emplace(key).second)
                SubscriptionsMap.emplace(key, ins.first->second);
        }
    }

    void Handle(TEvChange::TPtr& ev, const TActorContext& ctx) {
        const auto range = SubscriptionsMap.equal_range(std::make_pair(ev->Get()->Key, std::string()));
        for (auto it = range.first; range.second != it; ++it) {
            if (const auto sub = it->second.lock()) {
                ctx.Send(sub->Watchman, new TEvChange(*ev->Get()));
            }
        }
    }

    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

    TWatchmanSubscriptionsMap WatchmanSubscriptionsMap;
    TSubscriptionsMap SubscriptionsMap;
};

}

NActors::IActor* CreateEtcdWatchtower(TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
    return new TWatchtower(std::move(counters));

}

}

