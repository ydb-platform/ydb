#include "etcd_watch.h"
#include "etcd_shared.h"
#include "etcd_events.h"
#include "etcd_impl.h"

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

            HFunc(TEvQueryResult, Handle);
            hFunc(TEvQueryError, Handle);
        }
    }

    void Handle(TEvQueryResult::TPtr &ev, const TActorContext& ctx) {
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

    void Handle(TEvQueryError::TPtr &ev) {
        std::cerr << "Keep error received: " << ev->Get()->Issues.ToString() << std::endl;
    }

    void Handle(IStreamCtx::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
        if (!ev->Get()->Success)
            return Die(ctx);

        std::ostringstream sql;
        sql << "update `leases` set `updated` = CurrentUtcDatetime() where $Lease = `id`;" << std::endl;
        sql << "select `id`, `ttl` - unwrap(cast(CurrentUtcDatetime() - `updated` as Int64) / 1000000L) as `granted` from `leases` where $Lease = `id`;" << std::endl;

        NYdb::TParamsBuilder params;
        params.AddParam("$Lease").Int64(ev->Get()->Record.id()).Build();
        const auto my = this->SelfId();
        const auto ass = NActors::TlsActivationContext->ExecutorThread.ActorSystem;
        Stuff->Client->ExecuteQuery(sql.str(), NYdb::NQuery::TTxControl::BeginTx().CommitTx(), params.Build()).Subscribe([my, ass](const auto& future) {
            if (const auto res = future.GetValueSync(); res.IsSuccess())
                ass->Send(my, new TEvQueryResult(res.GetResultSets()));
            else
                ass->Send(my, new TEvQueryError(res.GetIssues()));
        });

        if (!Ctx->Read())
            return Die(ctx);
    }

    void Handle(IStreamCtx::TEvWriteFinished::TPtr& ev, const TActorContext& ctx) {
        if (!ev->Get()->Success)
            return Die(ctx);
    }

    void Handle(IStreamCtx::TEvNotifiedWhenDone::TPtr& ev, const TActorContext& ctx) {
        std::cerr << "Keep " << (ev->Get()->Success ? "finished." : "failed!") << std::endl;
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
        TimeOfLastWrite = TMonotonic::Now();
        ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
    }
private:
    struct TSubscription {
        using TPtr = std::shared_ptr<TSubscription>;
        using TWeakPtr = std::weak_ptr<TSubscription>;

        i64 WatchId = 0LL;
        i64 FromRevision = 0LL;
        bool SendProgress = false;
        bool WithPrevious = false;
        bool MakeFragmets = false;
        EWatchKind Kind = EWatchKind::Unsubscribe;
        bool AwaitHistory = false;

        std::unordered_map<std::string, std::pair<std::optional<TData>, std::queue<TEvChange::TPtr>>> Buffer;
    };

    using TByExactKeyMap = std::unordered_multimap<std::string, TSubscription::TWeakPtr>;
    using TByKeyPrefixMap = std::multimap<std::string, TSubscription::TWeakPtr>;
    // TODO: Add by range.

    using TUserSubscriptionsMap = std::unordered_multimap<i64, TSubscription::TPtr>;

    template<class TResultEvent>
    struct TMyEventResult : public TResultEvent {
        template<class TResult>
        TMyEventResult(TSubscription::TWeakPtr subscription, const TResult& result)
            : TResultEvent(result), Subscription(std::move(subscription))
        {}

        const TSubscription::TWeakPtr Subscription;
    };

    using TMyQueryResult = TMyEventResult<TEvQueryResult>;
    using TMyQueryError = TMyEventResult<TEvQueryError>;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TEvWakeup::EventType, Wakeup);

            HFunc(IStreamCtx::TEvReadFinished, Handle);
            HFunc(IStreamCtx::TEvWriteFinished, Handle);
            HFunc(IStreamCtx::TEvNotifiedWhenDone, Handle);

            HFunc(TEvChange, Handle);

            HFunc(TMyQueryResult, Handle);
            hFunc(TMyQueryError, Handle);
        }
    }

    void Create(const etcdserverpb::WatchCreateRequest& req, etcdserverpb::WatchResponse& res, const TActorContext& ctx)  {
        const auto& key = req.key();
        const auto& rangeEnd = DecrementKey(req.range_end());
        const auto watchId = req.watch_id();

        const auto& sub = UserSubscriptionsMap.emplace(watchId, std::make_shared<TSubscription>())->second;

        if (rangeEnd.empty())
            ByExactKeyMap.emplace(key, sub);
        else if (rangeEnd == key) {
            ByKeyPrefixMap.emplace(key, sub);
            if (!MinSizeOfPrefix || MinSizeOfPrefix > key.size())
                MinSizeOfPrefix = key.size();
        }

        sub->FromRevision = req.start_revision();
        sub->WithPrevious = req.prev_kv();
        sub->WatchId = req.watch_id();
        sub->MakeFragmets = req.fragment();
        sub->SendProgress = req.progress_notify();
        sub->AwaitHistory = sub->FromRevision && sub->FromRevision <  res.header().revision();

        bool ignoreUpdate = false, ignoreDelete = false;
        for (const auto f : req.filters()) {
            switch (f) {
                case etcdserverpb::WatchCreateRequest_FilterType_NOPUT: ignoreUpdate = true; break;
                case etcdserverpb::WatchCreateRequest_FilterType_NODELETE: ignoreDelete = false; break;
                default: break;
            }
        }

        std::cout << "Watch(";
        DumpKeyRange(std::cout, key, rangeEnd);
        if (sub->FromRevision)
            std::cout << ",rev=" << sub->FromRevision;
        if (sub->WithPrevious)
            std::cout << ",previous";
        if (sub->WatchId)
            std::cout << ",id=" << sub->WatchId;
        if (ignoreUpdate)
            std::cout << ",w/o updates";
        if (ignoreDelete)
            std::cout << ",w/o deletes";
        if (sub->MakeFragmets)
            std::cout << ",fragment";
        if (sub->SendProgress)
            std::cout << ",progress";
        std::cout << ')' << std::endl;

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
        TimeOfLastWrite = TMonotonic::Now();

        if (sub->AwaitHistory)
            RequestHistory(key, rangeEnd, sub);
    }

    void Cancel(const etcdserverpb::WatchCancelRequest& req, etcdserverpb::WatchResponse& res, const TActorContext& ctx)  {
        const auto watchId = req.watch_id();
        const auto erased = UserSubscriptionsMap.erase(watchId);
        std::cout << __func__ << '(' << watchId << ')' << '/' << erased << std::endl;

        res.set_canceled(true);
        if (watchId)
            res.set_watch_id(watchId);

        if (!Ctx->Write(std::move(res)))
            return UnsubscribeAndDie(ctx);
        TimeOfLastWrite = TMonotonic::Now();
    }

    void Progress(const etcdserverpb::WatchProgressRequest&, etcdserverpb::WatchResponse& res, const TActorContext& ctx)  {
        std::cout << __func__ << std::endl;
        if (!Ctx->Write(std::move(res)))
            return UnsubscribeAndDie(ctx);
        TimeOfLastWrite = TMonotonic::Now();
    }

    void Wakeup(const TActorContext& ctx) {
        if (std::any_of(UserSubscriptionsMap.cbegin(), UserSubscriptionsMap.cend(), [](const auto& sub) { return sub.second->SendProgress; } )) {
            if (TMonotonic::Now() - TimeOfLastWrite > TDuration::Seconds(13)) {
                etcdserverpb::WatchResponse response;
                const auto header = response.mutable_header();
                header->set_revision(Stuff->Revision.load());
                header->set_cluster_id(0ULL);
                header->set_member_id(0ULL);
                header->set_raft_term(0ULL);

                if (!Ctx->Write(std::move(response)))
                    return UnsubscribeAndDie(ctx);
                TimeOfLastWrite = TMonotonic::Now();
            }
        }
        ctx.Schedule(TDuration::Seconds(11), new TEvents::TEvWakeup);
    }

    void RequestHistory(const std::string_view& key, const std::string_view& rangeEnd, const TSubscription::TPtr& sub) {
        NYdb::TParamsBuilder params;
        const auto& revName = AddParam("Revision", params, sub->FromRevision);

        std::ostringstream where;
        MakeSimplePredicate(key, rangeEnd, where, params);

        std::ostringstream sql;
        sql << "select `key`, `value`, `created`, `modified`, `version`, `lease` from `verhaal` where " << revName << " <= `modified` and " << where.str() << " order by `modified` asc;" << std::endl;

        const auto my = this->SelfId();
        const auto ass = NActors::TlsActivationContext->ExecutorThread.ActorSystem;
        const TSubscription::TWeakPtr weak(sub);
        Stuff->Client->ExecuteQuery(sql.str(), NYdb::NQuery::TTxControl::BeginTx().CommitTx(), params.Build()).Subscribe([my, weak, ass](const auto& future) {
            if (const auto res = future.GetValueSync(); res.IsSuccess())
                ass->Send(my, new TMyQueryResult(weak, res.GetResultSets()));
            else
                ass->Send(my, new TMyQueryError(weak, res.GetIssues()));
        });
    }

    static void FillEvent(const TSubscription& sub, const std::string& key, const TData& oldData, const TData& newData, mvccpb::Event& event) {
        event.set_type(newData.Version ? mvccpb::Event_EventType_PUT : mvccpb::Event_EventType_DELETE);

        if (sub.WithPrevious && oldData.Version) {
            const auto kv = event.mutable_prev_kv();
            kv->set_key(key);
            kv->set_value(oldData.Value);
            kv->set_version(oldData.Version);
            kv->set_lease(oldData.Lease);
            kv->set_mod_revision(oldData.Modified);
            kv->set_create_revision(oldData.Created);
        }

        const auto kv = event.mutable_kv();
        kv->set_key(key);
        if (newData.Version) {
            kv->set_value(newData.Value);
            kv->set_version(newData.Version);
            kv->set_lease(newData.Lease);
            kv->set_mod_revision(newData.Modified);
            kv->set_create_revision(newData.Created);
        }

        std::cout << (newData.Version ? "Put" : "Drop") << '(' << key << ',';
        if (sub.WithPrevious && oldData.Version) {
            std::cout << "old " << oldData.Version << ',' << oldData.Created << ',' << oldData.Modified << ',' << oldData.Value.size() << ',' << oldData.Lease  << ',';
        }
        if (newData.Version) {
            std::cout << "new " << newData.Version << ',' << newData.Created << ',' << newData.Modified << ',' << newData.Value.size() << ',' << newData.Lease;
        }
        std::cout << ')' << std::endl;
    }

    void Handle(TMyQueryResult::TPtr &ev, const TActorContext& ctx) {
        if (const auto& sub = static_cast<TMyQueryResult*>(ev->Get())->Subscription.lock()) {
            etcdserverpb::WatchResponse res;
            const auto header = res.mutable_header();
            header->set_revision(Stuff->Revision.load());
            header->set_cluster_id(0ULL);
            header->set_member_id(0ULL);
            header->set_raft_term(0ULL);

            for (auto parser = NYdb::TResultSetParser(ev->Get()->Results.front()); parser.TryNextRow();) {
                const auto& key = NYdb::TValueParser(parser.GetValue("key")).GetString();
                TData data {
                    .Value = NYdb::TValueParser(parser.GetValue("value")).GetString(),
                    .Created = NYdb::TValueParser(parser.GetValue("created")).GetInt64(),
                    .Modified = NYdb::TValueParser(parser.GetValue("modified")).GetInt64(),
                    .Version = NYdb::TValueParser(parser.GetValue("version")).GetInt64(),
                    .Lease = NYdb::TValueParser(parser.GetValue("lease")).GetInt64()
                };

                auto& buff = sub->Buffer[key];

                if ((EWatchKind::OnChanges == sub->Kind || (data.Version ? EWatchKind::OnUpdates : EWatchKind::OnDeletions) == sub->Kind) &&
                    (!sub->WithPrevious || buff.first))
                    FillEvent(*sub, key, buff.first ? *buff.first : TData{}, data, *res.add_events());

                while (!buff.second.empty() && buff.second.front()->Get()->NewData.Modified <= data.Modified)
                    buff.second.pop();

                buff.first = std::move(data);
            }

            std::multimap<i64, TEvChange::TPtr> events;
            for (auto& [_, buff] : sub->Buffer)
                for (; !buff.second.empty(); buff.second.pop())
                    events.emplace(buff.second.front()->Get()->NewData.Modified, std::move(buff.second.front()));

            for (const auto& [_, ev] : events)
                FillEvent(*sub, ev->Get()->Key, ev->Get()->OldData, ev->Get()->NewData, *res.add_events());

            sub->AwaitHistory = false;

            if (!res.events().empty() || sub->SendProgress) {
                if (!Ctx->Write(std::move(res)))
                    return UnsubscribeAndDie(ctx);
                TimeOfLastWrite = TMonotonic::Now();
            }
        }
    }

    void Handle(TMyQueryError::TPtr &ev) {
        std::cerr << "Watch error received: " << ev->Get()->Issues.ToString() << std::endl;
    }

    void Handle(TEvChange::TPtr& ev, const TActorContext& ctx) {
        const auto revision = Stuff->Revision.load();
        const auto process = [revision](const TEvChange::TPtr& ev, TSubscription& sub) -> std::optional<etcdserverpb::WatchResponse> {
             if (EWatchKind::OnChanges == sub.Kind ||
                (ev->Get()->NewData.Version ? EWatchKind::OnUpdates : EWatchKind::OnDeletions) == sub.Kind) {
                if (sub.AwaitHistory) {
                    sub.Buffer[ev->Get()->Key].second.push(ev);
                    return std::nullopt;
                }

                etcdserverpb::WatchResponse res;
                const auto header = res.mutable_header();
                header->set_revision(revision);
                header->set_cluster_id(0ULL);
                header->set_member_id(0ULL);
                header->set_raft_term(0ULL);

                FillEvent(sub, ev->Get()->Key, ev->Get()->OldData, ev->Get()->NewData, *res.add_events());

                if (sub.WatchId)
                    res.set_watch_id(sub.WatchId);
                 return std::move(res);
             } else
                 return std::nullopt;
        };

        if (!ByExactKeyMap.empty()) {
            const auto range = ByExactKeyMap.equal_range(ev->Get()->Key);
            for (auto it = range.first; range.second != it;) {
                if (ev->Get()->Key == it->first) {
                    if (const auto sub = it->second.lock()) {
                        if (auto res = process(ev, *sub)) {
                            if (!Ctx->Write(std::move(*res)))
                                return UnsubscribeAndDie(ctx);
                            TimeOfLastWrite = TMonotonic::Now();
                        }
                    } else {
                        it = ByExactKeyMap.erase(it);
                        continue;
                    }
                }
                ++it;
            }
        }

        if (!ByKeyPrefixMap.empty()) {
            const auto& prefix = ev->Get()->Key.substr(0, MinSizeOfPrefix);
            const auto end = ByKeyPrefixMap.lower_bound(IncrementKey(prefix));
            for (auto it = ByKeyPrefixMap.lower_bound(prefix); end != it;) {
                if (ev->Get()->Key.starts_with(it->first)) {
                    if (const auto sub = it->second.lock()) {
                        if (auto res = process(ev, *sub)) {
                            if (!Ctx->Write(std::move(*res)))
                                return UnsubscribeAndDie(ctx);
                            TimeOfLastWrite = TMonotonic::Now();
                        }
                    } else {
                        const bool updateMinPrefixSize = MinSizeOfPrefix <= it->first.size();
                        it = ByKeyPrefixMap.erase(it);
                        if (updateMinPrefixSize) {
                            MinSizeOfPrefix = ByKeyPrefixMap.empty() ? 0U : ByKeyPrefixMap.cbegin()->first.size();
                            for (const auto& item : ByKeyPrefixMap)
                                MinSizeOfPrefix = std::min(MinSizeOfPrefix, item.first.size());
                        }
                        continue;
                    }
                }
                ++it;
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
        std::cerr << "Watch " << (ev->Get()->Success ? "finished." : "failed!") << std::endl;
        return UnsubscribeAndDie(ctx);
    }

    void UnsubscribeAndDie(const TActorContext& ctx) {
        ctx.Send(Watchtower, new TEvSubscribe);
        return Die(ctx);
    }

    const TIntrusivePtr<IStreamCtx> Ctx;
    const TActorId Watchtower;
    const TSharedStuff::TPtr Stuff;

    TByExactKeyMap ByExactKeyMap;
    TByKeyPrefixMap ByKeyPrefixMap;

    size_t MinSizeOfPrefix = 0U;
    TUserSubscriptionsMap UserSubscriptionsMap;
    TMonotonic TimeOfLastWrite;
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

    using TWatchmanSubscriptionsMap = std::unordered_map<TActorId, TSubscriptions::TPtr>;

    using TByExactKeyMap = std::unordered_multimap<std::string, TSubscriptions::TWeakPtr>;
    using TByKeyPrefixMap = std::multimap<std::string, TSubscriptions::TWeakPtr>;
    // TODO: Add by range.

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvWatchRequest, Handle);
            HFunc(TEvLeaseKeepAliveRequest, Handle);

            HFunc(TEvSubscribe, Handle);
            HFunc(TEvChange, Handle);

            CFunc(TEvents::TEvWakeup::EventType, Wakeup);

            HFunc(TEvQueryResult, Handle);
            HFunc(TEvQueryError, Handle);
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
            if (const auto& key = std::make_pair(ev->Get()->Key, ev->Get()->RangeEnd); ins.first->second->Subscriptions.emplace(key).second)
                if (key.second.empty())
                    ByExactKeyMap.emplace(key.first, ins.first->second);
                else if (key.first == key.second) {
                    ByKeyPrefixMap.emplace(key.first, ins.first->second);
                    if (!MinSizeOfPrefix || MinSizeOfPrefix > key.first.size())
                        MinSizeOfPrefix = key.first.size();
                }
        }
    }

    void Handle(TEvChange::TPtr& ev, const TActorContext& ctx) {
        if (!ByExactKeyMap.empty()) {
            const auto range = ByExactKeyMap.equal_range(ev->Get()->Key);
            for (auto it = range.first; range.second != it;) {
                if (ev->Get()->Key == it->first) {
                    if (const auto sub = it->second.lock()) {
                        ctx.Send(sub->Watchman, new TEvChange(*ev->Get()));
                    } else {
                        it = ByExactKeyMap.erase(it);
                        continue;
                    }
                }
                ++it;
            }
        }

        if (!ByKeyPrefixMap.empty()) {
            const auto& prefix = ev->Get()->Key.substr(0, MinSizeOfPrefix);
            const auto end = ByKeyPrefixMap.lower_bound(IncrementKey(prefix));
            for (auto it = ByKeyPrefixMap.lower_bound(prefix); end != it;) {
                if (ev->Get()->Key.starts_with(it->first)) {
                    if (const auto sub = it->second.lock()) {
                        ctx.Send(sub->Watchman, new TEvChange(*ev->Get()));
                    } else {
                        const bool updateMinPrefixSize = MinSizeOfPrefix <= it->first.size();
                        it = ByKeyPrefixMap.erase(it);
                        if (updateMinPrefixSize) {
                            MinSizeOfPrefix = ByKeyPrefixMap.empty() ? 0U : ByKeyPrefixMap.cbegin()->first.size();
                            for (const auto& item : ByKeyPrefixMap)
                                MinSizeOfPrefix = std::min(MinSizeOfPrefix, item.first.size());
                        }
                        continue;
                    }
                }
                ++it;
            }
        }
    }

    void Wakeup(const TActorContext&) {
        std::ostringstream sql;
        NYdb::TParamsBuilder params;
        Revision = Stuff->Revision.fetch_add(1LL);
        params.AddParam("$Revision").Int64(Revision).Build();

        sql << "$Expired = select `id` from `leases` where unwrap(interval('PT1S') * `ttl` + `updated`) < CurrentUtcDatetime();" << std::endl;
        sql << "$Victims = select `key`, `value`, `created`, `modified`, `version`, `lease` from `huidig` as h" << std::endl;
        sql << '\t' << "left semi join $Expired as l on h.`lease` = l.`id`;" << std::endl;
        sql << "insert into `verhaal`" << std::endl;
        sql << "select `key`, `created`, $Revision as `modified`, 0L as `version`, `value`, `lease` from $Victims;" << std::endl;

        if constexpr (NotifyWatchtower) {
            sql << "select `key`, `value`, `created`, `modified`, `version`, `lease` from $Victims;" << std::endl;
        } else {
            sql << "select count(*) from $Victims;" << std::endl;
        }

        sql << "delete from `huidig` on select `key` from $Victims;" << std::endl;
        sql << "delete from `leases` on select `id` from $Expired;" << std::endl;

        const auto my = this->SelfId();
        const auto ass = NActors::TlsActivationContext->ExecutorThread.ActorSystem;
        Stuff->Client->ExecuteQuery(sql.str(), NYdb::NQuery::TTxControl::BeginTx().CommitTx(), params.Build()).Subscribe([my, ass](const auto& future) {
            if (const auto res = future.GetValueSync(); res.IsSuccess())
                ass->Send(my, new TEvQueryResult(res.GetResultSets()));
            else
                ass->Send(my, new TEvQueryError(res.GetIssues()));
        });
    }

    void Handle(TEvQueryResult::TPtr &ev, const TActorContext& ctx) {
        i64 deleted = 0ULL;
        if constexpr (NotifyWatchtower) {
            for (auto parser = NYdb::TResultSetParser(ev->Get()->Results.front()); parser.TryNextRow(); ++deleted) {
                TData oldData;
                oldData.Value = NYdb::TValueParser(parser.GetValue("value")).GetString();
                oldData.Created = NYdb::TValueParser(parser.GetValue("created")).GetInt64();
                oldData.Modified = NYdb::TValueParser(parser.GetValue("modified")).GetInt64();
                oldData.Version = NYdb::TValueParser(parser.GetValue("version")).GetInt64();
                oldData.Lease = NYdb::TValueParser(parser.GetValue("lease")).GetInt64();
                auto key = NYdb::TValueParser(parser.GetValue("key")).GetString();

                ctx.Send(ctx.SelfID, std::make_unique<TEvChange>(std::move(key), std::move(oldData)));
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

    void Handle(TEvQueryError::TPtr &ev, const TActorContext& ctx) {
        std::cerr << "Watch error received " << ev->Get()->Issues.ToString() << std::endl;
        ctx.Schedule(TDuration::Seconds(7), new TEvents::TEvWakeup);
    }

    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    const TSharedStuff::TPtr Stuff;

    TWatchmanSubscriptionsMap WatchmanSubscriptionsMap;

    TByExactKeyMap ByExactKeyMap;
    TByKeyPrefixMap ByKeyPrefixMap;

    size_t MinSizeOfPrefix = 0U;
    i64 Revision = 0LL;
};

}

NActors::IActor* BuildWatchtower(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TSharedStuff::TPtr stuff) {
    return new TWatchtower(std::move(counters), std::move(stuff));

}

}

