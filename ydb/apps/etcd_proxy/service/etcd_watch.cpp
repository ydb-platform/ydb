#include "etcd_watch.h"
#include "etcd_shared.h"
#include "etcd_events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/executor_thread.h>

namespace NEtcd {

using namespace NActors;

namespace {

class TKeysKeeper : public TActorBootstrapped<TKeysKeeper> {
public:
    using IStreamCtx = NKikimr::NGRpcServer::IGRpcStreamingContext<etcdserverpb::LeaseKeepAliveRequest, etcdserverpb::LeaseKeepAliveResponse>;

    TKeysKeeper(TIntrusivePtr<IStreamCtx> ctx, TSharedStuff::TPtr stuff)
        : Ctx(std::move(ctx)), Stuff(std::move(stuff))
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateFunc);
        Ctx->Attach(ctx.SelfID);
        if (!Ctx->Read())
            return Die(ctx);
    }
private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(IStreamCtx::TEvReadFinished, Handle);
            HFunc(IStreamCtx::TEvWriteFinished, Handle);
            HFunc(IStreamCtx::TEvNotifiedWhenDone, Handle);

            HFunc(NEtcd::TEvQueryResult, Handle);
            hFunc(NEtcd::TEvQueryError, Handle);
        }
    }

    void Handle(NEtcd::TEvQueryResult::TPtr &ev, const TActorContext& ctx) {
        if (auto parser = NYdb::TResultSetParser(ev->Get()->Results.front()); parser.TryNextRow() && 2ULL == parser.ColumnsCount()) {
            etcdserverpb::LeaseKeepAliveResponse response;
            response.set_id(NYdb::TValueParser(parser.GetValue(0)).GetInt64());
            response.set_ttl(NYdb::TValueParser(parser.GetValue(1)).GetInt64());

            const auto header = response.mutable_header();
            header->set_revision(Stuff->Revision.load());
            header->set_cluster_id(0ULL);
            header->set_member_id(0ULL);
            header->set_raft_term(0ULL);

            if (!Ctx->Write(std::move(response)))
                return Die(ctx);
        }
    }

    void Handle(NEtcd::TEvQueryError::TPtr &ev) {
        Cerr << __func__ << ' ' << ev->Get()->Issues.ToString() << Endl;
    }

    void Handle(IStreamCtx::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
        if (!ev->Get()->Success)
            return Die(ctx);

        TStringBuilder sql;
        sql << "update `leases` set `updated` = CurrentUtcDatetime() where $Lease = `id`;" << Endl;
        sql << "select `id`, `ttl` - unwrap(cast(CurrentUtcDatetime() - `updated` as Int64) / 1000000L) as `granted` from `leases` where $Lease = `id`;" << Endl;

        NYdb::TParamsBuilder params;
        params.AddParam("$Lease").Int64(ev->Get()->Record.id()).Build();
        const auto my = this->SelfId();
        const auto ass = NActors::TlsActivationContext->ExecutorThread.ActorSystem;
        Stuff->Client->ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), params.Build()).Subscribe([my, ass](const auto& future) {
            if (const auto res = future.GetValueSync(); res.IsSuccess())
                ass->Send(my, new NEtcd::TEvQueryResult(res.GetResultSets()));
            else
                ass->Send(my, new NEtcd::TEvQueryError(res.GetIssues()));
        });

        if (!Ctx->Read())
            return Die(ctx);
    }

    void Handle(IStreamCtx::TEvWriteFinished::TPtr& ev, const TActorContext& ctx) {
        if (!ev->Get()->Success)
            return Die(ctx);
    }

    void Handle(IStreamCtx::TEvNotifiedWhenDone::TPtr& ev, const TActorContext& ctx) {
        Cerr << (ev->Get()->Success ? "Finished." : "Failed!") << Endl;
        return Die(ctx);
    }

    const TIntrusivePtr<IStreamCtx> Ctx;
    const TSharedStuff::TPtr Stuff;
};

class TWatchman : public TActorBootstrapped<TWatchman> {
public:
    using IStreamCtx = NKikimr::NGRpcServer::IGRpcStreamingContext<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>;

    TWatchman(TIntrusivePtr<IStreamCtx> ctx, TActorId watchtower, TSharedStuff::TPtr stuff)
        : Ctx(std::move(ctx)), Watchtower(std::move(watchtower)), Stuff(std::move(stuff))
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
        for (auto it = range.first; range.second != it;) {
            if (const auto sub = it->second.lock()) {
                ++it;
                if (EWatchKind::OnChanges == sub->Kind ||
                    (ev->Get()->NewData.Version ? EWatchKind::OnUpdates : EWatchKind::OnDeletions) == sub->Kind) {
                    etcdserverpb::WatchResponse res;
                    const auto header = res.mutable_header();
                    header->set_revision(Stuff->Revision.load());
                    header->set_cluster_id(0ULL);
                    header->set_member_id(0ULL);
                    header->set_raft_term(0ULL);

                    const auto event = res.add_events();
                    event->set_type(ev->Get()->NewData.Version ? mvccpb::Event_EventType_PUT : mvccpb::Event_EventType_DELETE);

                    if (sub->WithPrevious && ev->Get()->OldData.Version) {
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
            } else {
                it = SubscriptionsMap.erase(it);
            }
        }
    }

    void Handle(IStreamCtx::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
        if (!ev->Get()->Success)
            return Die(ctx);

        etcdserverpb::WatchResponse response;
        const auto header = response.mutable_header();
        header->set_revision(Stuff->Revision.load());
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
    const TSharedStuff::TPtr Stuff;

    TSubscriptionsMap SubscriptionsMap;
    TUserSubscriptionsMap UserSubscriptionsMap;
};

class TWatchtower : public TActorBootstrapped<TWatchtower> {
public:
    TWatchtower(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TSharedStuff::TPtr stuff)
        : Counters(std::move(counters)), Stuff(std::move(stuff))
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateFunc);
        Stuff->Watchtower = SelfId();
        ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
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
            HFunc(TEvLeaseKeepAliveRequest, Handle);

            HFunc(TEvSubscribe, Handle);
            HFunc(TEvChange, Handle);

            CFunc(TEvents::TEvWakeup::EventType, Wakeup);

            HFunc(NEtcd::TEvQueryResult, Handle);
            HFunc(NEtcd::TEvQueryError, Handle);
        }
    }

    void Handle(TEvWatchRequest::TPtr& ev, const TActorContext& ctx) {
        ctx.RegisterWithSameMailbox(new TWatchman(ev->Get()->GetStreamCtx(), ctx.SelfID, Stuff));
    }

    void Handle(TEvLeaseKeepAliveRequest::TPtr& ev, const TActorContext& ctx) {
        ctx.RegisterWithSameMailbox(new TKeysKeeper(ev->Get()->GetStreamCtx(), Stuff));
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
        for (auto it = range.first; range.second != it;) {
            if (const auto sub = it->second.lock()) {
                ++it;
                ctx.Send(sub->Watchman, new TEvChange(*ev->Get()));
            } else {
                it = SubscriptionsMap.erase(it);
            }
        }
    }

    void Wakeup(const TActorContext&) {
        TStringBuilder sql;
        NYdb::TParamsBuilder params;
        Revision = Stuff->Revision.fetch_add(1LL);
        params.AddParam("$Revision").Int64(Revision).Build();

        sql << "$Expired = select `id` from `leases` where unwrap(interval('PT1S') * `ttl` + `updated`) < CurrentUtcDatetime();" << Endl;
        sql << "$Victims = select `key`, `value`, `created`, `modified`, `version`, `lease` from `huidig` as h" << Endl;
        sql << '\t' << "left semi join $Expired as l on h.`lease` = l.`id`;" << Endl;
        sql << "insert into `verhaal`" << Endl;
        sql << "select `key`, `created`, $Revision as `modified`, 0L as `version`, `value`, `lease` from $Victims;" << Endl;

        if (NotifyWatchtower) {
            sql << "select `key`, `value`, `created`, `modified`, `version`, `lease` from $Victims;" << Endl;
        } else {
            sql << "select count(*) from $Victims;" << Endl;
        }

        sql << "delete from `huidig` on select `key` from $Victims;" << Endl;
        sql << "delete from `leases` on select `id` from $Expired;" << Endl;

        const auto my = this->SelfId();
        const auto ass = NActors::TlsActivationContext->ExecutorThread.ActorSystem;
        Stuff->Client->ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), params.Build()).Subscribe([my, ass](const auto& future) {
            if (const auto res = future.GetValueSync(); res.IsSuccess())
                ass->Send(my, new NEtcd::TEvQueryResult(res.GetResultSets()));
            else
                ass->Send(my, new NEtcd::TEvQueryError(res.GetIssues()));
        });
    }

    void Handle(NEtcd::TEvQueryResult::TPtr &ev, const TActorContext& ctx) {
        i64 deleted = 0ULL;
        if (NotifyWatchtower) {
            for (auto parser = NYdb::TResultSetParser(ev->Get()->Results.front()); parser.TryNextRow(); ++deleted) {
                NEtcd::TData oldData;
                oldData.Value = NYdb::TValueParser(parser.GetValue("value")).GetString();
                oldData.Created = NYdb::TValueParser(parser.GetValue("created")).GetInt64();
                oldData.Modified = NYdb::TValueParser(parser.GetValue("modified")).GetInt64();
                oldData.Version = NYdb::TValueParser(parser.GetValue("version")).GetInt64();
                oldData.Lease = NYdb::TValueParser(parser.GetValue("lease")).GetInt64();
                auto key = NYdb::TValueParser(parser.GetValue("key")).GetString();

                ctx.Send(ctx.SelfID, std::make_unique<NEtcd::TEvChange>(std::move(key), std::move(oldData)));
            }
        } else {
            if (auto parser = NYdb::TResultSetParser(ev->Get()->Results.front()); parser.TryNextRow()) {
                deleted = NYdb::TValueParser(parser.GetValue(0)).GetUint64();
            }
        }

        if (!deleted) {
            auto expected = Revision + 1U;
            Stuff->Revision.compare_exchange_strong(expected, Revision);
        }

        ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
    }

    void Handle(NEtcd::TEvQueryError::TPtr &ev, const TActorContext& ctx) {
        Cerr << __func__ << ' ' << ev->Get()->Issues.ToString() << Endl;
        ctx.Schedule(TDuration::Seconds(7), new TEvents::TEvWakeup);
    }

    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    const TSharedStuff::TPtr Stuff;

    TWatchmanSubscriptionsMap WatchmanSubscriptionsMap;
    TSubscriptionsMap SubscriptionsMap;

    i64 Revision = 0LL;
};

}

NActors::IActor* BuildWatchtower(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TSharedStuff::TPtr stuff) {
    return new TWatchtower(std::move(counters), std::move(stuff));

}

}

