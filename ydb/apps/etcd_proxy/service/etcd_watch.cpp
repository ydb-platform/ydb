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
        std::cout << "Keeper started." << std::endl;
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

            if (!Ctx->Write(std::move(response)))
                return Die(ctx);
        }
    }

    void Handle(TEvQueryError::TPtr &ev) {
        std::cout << "Keep error received: " << ev->Get()->Issues.ToString() << std::endl;
    }

    void Handle(IStreamCtx::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
        if (!ev->Get()->Success)
            return Die(ctx);

        NYdb::TParamsBuilder params;
        const auto& leasePraramName = AddParam<i64>("Lease", params, ev->Get()->Record.id());

        std::ostringstream sql;
        sql << "update `leases` set `updated` = CurrentUtcDatetime() where " << leasePraramName << " = `id`;" << std::endl;
        sql << "select `id`, `ttl` - unwrap(cast(CurrentUtcDatetime() - `updated` as Int64) / 1000000L) as `granted` from `leases` where " << leasePraramName << " = `id`;" << std::endl;

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
        std::cout << "Keep " << (ev->Get()->Success ? "finished." : "failed!") << std::endl;
        return Die(ctx);
    }

    const TIntrusivePtr<IStreamCtx> Ctx;
    const TSharedStuff::TPtr Stuff;
};

class TWatch : public TActorBootstrapped<TWatch> {
public:
    using IStreamCtx = NKikimr::NGRpcServer::IGRpcStreamingContext<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>;

    TWatch(
        TActorId watchtower,
        TActorId watchman,
        TSharedStuff::TPtr stuff,
        std::string key,
        std::string rangeEnd,
        EWatchKind kind,
        i64 fromRevision,
        bool withPrevious,
        bool makeFragmets,
        bool sendProgress
    ) : Watchtower(std::move(watchtower)),
        Watchman(std::move(watchman)),
        Stuff(std::move(stuff)),
        Key(std::move(key)),
        RangeEnd(std::move(rangeEnd)),
        Kind(kind),
        FromRevision(fromRevision),
        WithPrevious(withPrevious),
        MakeFragmets(makeFragmets),
        SendProgress(sendProgress),
        AwaitHistory(fromRevision > 0LL)
    {
        Y_UNUSED(MakeFragmets);
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateFunc);
        TimeOfLastSent = TMonotonic::Now();
        ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
        ctx.Send(Watchtower, new TEvSubscribe(Key, RangeEnd, Kind, WithPrevious));
        if (AwaitHistory)
            RequestHistory();
    }
private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TEvWakeup::EventType, Wakeup);
            CFunc(TEvCancel::EventType, UnsubscribeAndDie);

            HFunc(TEvChange, Handle);

            HFunc(TEvQueryResult, Handle);
            HFunc(TEvQueryError, Handle);
        }
    }

    void Wakeup(const TActorContext& ctx) {
        if (SendProgress && TMonotonic::Now() - TimeOfLastSent > TDuration::Seconds(13)) {
            ctx.Send(Watchman, new TEvChanges);
            TimeOfLastSent = TMonotonic::Now();
        }
        ctx.Schedule(TDuration::Seconds(11), new TEvents::TEvWakeup);
    }

    void RequestHistory() {
        NYdb::TParamsBuilder params;
        const auto& revName = AddParam("Revision", params, FromRevision);

        std::ostringstream where;
        MakeSimplePredicate(Key, RangeEnd, where, params);

        std::ostringstream sql;
        sql << "select * from (select max_by(TableRow(), `modified`) from `verhaal` where " << revName << " > `modified` and " << where.view() << " group by `key`) flatten columns" << std::endl;
        sql << "union all" << std::endl;
        sql << "select `key`, `value`, `created`, `modified`, `version`, `lease` from `verhaal` where " << revName << " <= `modified` and " << where.view() << std::endl;
        sql << "order by `modified` asc;" << std::endl;
//      std::cout << std::endl << sql.view() << std::endl;

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
        std::vector<TChange> changes;
        changes.reserve(ev->Get()->Results.front().RowsCount());
        for (auto parser = NYdb::TResultSetParser(ev->Get()->Results.front()); parser.TryNextRow();) {
            auto key = NYdb::TValueParser(parser.GetValue("key")).GetString();
            TData data {
                .Value = NYdb::TValueParser(parser.GetValue("value")).GetString(),
                .Created = NYdb::TValueParser(parser.GetValue("created")).GetInt64(),
                .Modified = NYdb::TValueParser(parser.GetValue("modified")).GetInt64(),
                .Version = NYdb::TValueParser(parser.GetValue("version")).GetInt64(),
                .Lease = NYdb::TValueParser(parser.GetValue("lease")).GetInt64()
            };

            auto& buff = Buffer[key];

            while (!buff.second.empty() && buff.second.top().NewData.Modified <= data.Modified)
                buff.second.pop();

            if ((EWatchKind::OnChanges == Kind || (data.Version ? EWatchKind::OnUpdates : EWatchKind::OnDeletions) == Kind)
                && data.Modified >= FromRevision && (!WithPrevious || buff.first || data.Created == data.Modified))
                changes.emplace_back(std::move(key), data.Modified, WithPrevious && buff.first ? std::move(*buff.first) : TData(), data.Version > 0LL ? TData(data) : TData());

            if (data.Version > 0LL)
                buff.first.emplace(std::move(data));
            else
                buff.first.reset();
        }

        changes.reserve(changes.size() + Buffer.size());
        for (auto& [_, buff] : Buffer)
            for (; !buff.second.empty(); buff.second.pop())
                changes.emplace_back(std::move(buff.second.top()));

        std::sort(changes.begin(), changes.end(), TChange::TOrder());

        if (!changes.empty() || SendProgress) {
            ctx.Send(Watchman, new TEvChanges(std::move(changes)));
            TimeOfLastSent = TMonotonic::Now();
        }
        AwaitHistory = false;
    }

    void Handle(TEvQueryError::TPtr &ev, const TActorContext& ctx) {
        DumpKeyRange(std::cout << "Watch ", Key, RangeEnd) << " error received: " << ev->Get()->Issues.ToString() << std::endl;
        UnsubscribeAndDie(ctx);
    }

    void Handle(TEvChange::TPtr& ev, const TActorContext& ctx) {
        if (EWatchKind::OnChanges == Kind ||
            (ev->Get()->NewData.Version ? EWatchKind::OnUpdates : EWatchKind::OnDeletions) == Kind) {

            if (AwaitHistory)
                Buffer[ev->Get()->Key].second.emplace(std::move(*ev->Get()));
            else
                ctx.Send(Watchman, new TEvChanges({TChange(std::move(ev->Get()->Key), ev->Get()->Revision, WithPrevious && ev->Get()->OldData.Version ? std::move(ev->Get()->OldData) : TData(), std::move(ev->Get()->NewData))}));
        }
    }

    void UnsubscribeAndDie(const TActorContext& ctx) {
        ctx.Send(Watchtower, new TEvSubscribe);
        return Die(ctx);
    }

    const TActorId Watchtower, Watchman;
    const TSharedStuff::TPtr Stuff;

    const std::string Key, RangeEnd;
    const EWatchKind Kind;
    const i64 FromRevision;
    const bool WithPrevious;
    const bool MakeFragmets;
    const bool SendProgress;

    TMonotonic TimeOfLastSent;

    bool AwaitHistory;
    std::unordered_map<std::string, std::pair<std::optional<TData>, std::priority_queue<TChange, std::vector<TChange>, TChange::TOrder>>> Buffer;
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
    }
private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(IStreamCtx::TEvReadFinished, Handle);
            HFunc(IStreamCtx::TEvWriteFinished, Handle);
            HFunc(IStreamCtx::TEvNotifiedWhenDone, Handle);

            HFunc(TEvChanges, Handle);
        }
    }

    void Create(const etcdserverpb::WatchCreateRequest& req, etcdserverpb::WatchResponse& res, const TActorContext& ctx)  {
        auto key = req.key();
        auto rangeEnd = DecrementKey(req.range_end());

        if (!rangeEnd.empty() && rangeEnd != key) {
            DumpKeyRange(std::cout << "Watch(", key, rangeEnd) << ") isn't implemented." << std::endl;
            return UnsubscribeAndDie(ctx);
        }

        const auto watchId = req.watch_id();
        const auto revision = req.start_revision();
        const bool withPrevious = req.prev_kv();
        const bool makeFragmets = req.fragment();
        const bool sendProgress = req.progress_notify();

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
        if (revision)
            std::cout << ",rev=" << revision;
        if (withPrevious)
            std::cout << ",previous";
        if (watchId)
            std::cout << ",id=" << watchId;
        if (ignoreUpdate)
            std::cout << ",w/o updates";
        if (ignoreDelete)
            std::cout << ",w/o deletes";
        if (makeFragmets)
            std::cout << ",fragment";
        if (sendProgress)
            std::cout << ",progress";
        std::cout << ')' << std::endl;

        if (!(ignoreUpdate && ignoreDelete)) {
            const auto kind = ignoreDelete && !ignoreUpdate ?
                EWatchKind::OnUpdates :
                    ignoreUpdate && !ignoreDelete ?
                        EWatchKind::OnDeletions : EWatchKind::OnChanges;

            Watches.emplace(watchId, ctx.RegisterWithSameMailbox(new TWatch(Watchtower, ctx.SelfID, Stuff, std::move(key), std::move(rangeEnd), kind, revision, withPrevious, makeFragmets, sendProgress)));
        }

        res.set_created(true);
        if (watchId)
            res.set_watch_id(watchId);

        if (!Ctx->Write(std::move(res)))
            return UnsubscribeAndDie(ctx);
        TimeOfLastWrite = TMonotonic::Now();
    }

    void Cancel(const etcdserverpb::WatchCancelRequest& req, etcdserverpb::WatchResponse& res, const TActorContext& ctx)  {
        const auto watchId = req.watch_id();
        std::cout << __func__ << '(' << watchId << ')' << std::endl;

        const auto range = Watches.equal_range(watchId);
        std::for_each(range.first, range.second, [&ctx](const std::pair<i64, TActorId>& watch) {
            ctx.Send(watch.second, new TEvCancel);
        });
        Watches.erase(range.first, range.second);

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

    void Handle(IStreamCtx::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
        if (!ev->Get()->Success)
            return Die(ctx);

        etcdserverpb::WatchResponse response;
        const auto header = response.mutable_header();
        header->set_revision(Stuff->Revision.load());

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
        std::cout << "Watch " << (ev->Get()->Success ? "finished." : "failed!") << std::endl;
        return UnsubscribeAndDie(ctx);
    }

    void Handle(TEvChanges::TPtr& ev, const TActorContext& ctx) {
        etcdserverpb::WatchResponse response;
        const auto header = response.mutable_header();
        header->set_revision(Stuff->Revision.load());

        for (const auto& change : ev->Get()->Changes) {
            const auto event = response.add_events();
            event->set_type(change.NewData.Version ? mvccpb::Event_EventType_PUT : mvccpb::Event_EventType_DELETE);

            if (change.OldData.Version > 0LL) {
                const auto kv = event->mutable_prev_kv();
                kv->set_key(change.Key);
                kv->set_value(change.OldData.Value);
                kv->set_version(change.OldData.Version);
                kv->set_lease(change.OldData.Lease);
                kv->set_mod_revision(change.OldData.Modified);
                kv->set_create_revision(change.OldData.Created);
            }

            const auto kv = event->mutable_kv();
            kv->set_key(change.Key);
            if (change.NewData.Version > 0LL) {
                kv->set_value(change.NewData.Value);
                kv->set_version(change.NewData.Version);
                kv->set_lease(change.NewData.Lease);
                kv->set_mod_revision(change.NewData.Modified);
                kv->set_create_revision(change.NewData.Created);
            } else
                kv->set_mod_revision(change.Revision);

            std::cout << (change.NewData.Version ? change.OldData.Version ? "Created" : "Updated" : "Deleted") << '(' << change.Key;
            if (change.OldData.Version) {
                std::cout << ", old " << change.OldData.Version << ',' << change.OldData.Created << ',' << change.OldData.Modified << ',' << change.OldData.Value.size() << ',' << change.OldData.Lease;
            }
            if (change.NewData.Version) {
                std::cout << ", new " << change.NewData.Version << ',' << change.NewData.Created << ',' << change.NewData.Modified << ',' << change.NewData.Value.size() << ',' << change.NewData.Lease;
            }
            std::cout << ')' << std::endl;
        }

        if (!Ctx->Write(std::move(response)))
            return UnsubscribeAndDie(ctx);
        TimeOfLastWrite = TMonotonic::Now();
    }

    void UnsubscribeAndDie(const TActorContext& ctx) {
        for (const auto& watch : Watches)
            ctx.Send(watch.second, new TEvCancel);
        Watches.clear();

        return Die(ctx);
    }

    const TIntrusivePtr<IStreamCtx> Ctx;
    const TActorId Watchtower;
    const TSharedStuff::TPtr Stuff;

    std::unordered_multimap<i64, TActorId> Watches;

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
        ctx.Schedule(TDuration::Seconds(11), new TEvents::TEvWakeup);
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
        Revision = Stuff->Revision.fetch_add(1LL) + 1LL;

        std::ostringstream sql;
        NYdb::TParamsBuilder params;
        const auto& revName = AddParam("Revision", params, Revision);

        sql << "$Leases = select 0L as `lease` union all select `id` as `lease` from `leases` where unwrap(interval('PT1S') * `ttl` + `updated`) > CurrentUtcDatetime();" << std::endl;
        sql << "$Victims = select `key`, `value`, `created`, `modified`, `version`, `lease` from `huidig` as h left only join $Leases as l using(`lease`);" << std::endl;

        sql << "insert into `verhaal`" << std::endl;
        sql << "select `key`, `created`, " << revName << " as `modified`, 0L as `version`, `value`, `lease` from $Victims;" << std::endl;

        if constexpr (NotifyWatchtower) {
            sql << "select `key`, `value`, `created`, `modified`, `version`, `lease` from $Victims;" << std::endl;
        } else {
            sql << "select count(*) from $Victims;" << std::endl;
        }

        sql << "delete from `huidig` on select `key` from $Victims;" << std::endl;
        sql << "delete from `leases` where `id` not in $Leases;" << std::endl;

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

                ctx.Send(ctx.SelfID, std::make_unique<TEvChange>(std::move(key), Revision, std::move(oldData)));
            }
        } else {
            if (auto parser = NYdb::TResultSetParser(ev->Get()->Results.front()); parser.TryNextRow()) {
                deleted = NYdb::TValueParser(parser.GetValue(0)).GetUint64();
            }
        }

        if (deleted) {
            std::cout << deleted <<  " leases expired." << std::endl;
        } else {
            Stuff->Revision.compare_exchange_weak(Revision, Revision - 1LL);
        }

        ctx.Schedule(TDuration::Seconds(3), new TEvents::TEvWakeup);
    }

    void Handle(TEvQueryError::TPtr &ev, const TActorContext& ctx) {
        std::cout << "Check leases SQL error received " << ev->Get()->Issues.ToString() << std::endl;
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
