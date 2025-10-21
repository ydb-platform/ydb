#include "etcd_watch.h"
#include "etcd_shared.h"
#include "etcd_events.h"
#include "etcd_impl.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/executor_thread.h>

#include <ydb/public/sdk/cpp/src/library/string_utils/base64/base64.h>

#include <library/cpp/json/json_reader.h>

namespace NEtcd {

using namespace NActors;
using namespace NYdb::NQuery;
using namespace NYdb::NTopic;

namespace {

class TWatch : public TActorBootstrapped<TWatch> {
public:
    using IStreamCtx = NKikimr::NGRpcServer::IGRpcStreamingContext<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>;

    TWatch(
        i64 id,
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
    ) : Id(id),
        Watchtower(std::move(watchtower)),
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
        ctx.Send(Watchtower, new TEvSubscribe(Key, RangeEnd, Kind, WithPrevious));
        if (AwaitHistory)
            RequestHistory();
        else
            ctx.Schedule(TDuration::Seconds(3), new TEvents::TEvWakeup);
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
        if (SendProgress && TMonotonic::Now() - TimeOfLastSent > TDuration::Seconds(11)) {
            ctx.Send(Watchman, new TEvChanges(Id));
            TimeOfLastSent = TMonotonic::Now();
        }
        ctx.Schedule(TDuration::Seconds(3), new TEvents::TEvWakeup);
    }

    void RequestHistory() {
        NYdb::TParamsBuilder params;
        const auto& revName = AddParam("Revision", params, FromRevision);

        std::ostringstream where;
        MakeSimplePredicate(Key, RangeEnd, where, params);

        std::ostringstream sql;
        sql << Stuff->TablePrefix;
        if (WithPrevious) {
            sql << "select * from (select max_by(TableRow(), `modified`) from `history` where " << revName << " > `modified` and " << where.view() << " group by `key`) flatten columns union all" << std::endl;
        }
        sql << "select * from `history` where " << revName << " <= `modified` and " << where.view() << " order by `modified` asc;" << std::endl;
//      std::cout << std::endl << sql.view() << std::endl;

        TQueryClient::TQueryResultFunc callback = [query = sql.str(), args = params.Build()](TQueryClient::TSession session) -> TAsyncExecuteQueryResult {
            return session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(), args);
        };
        Stuff->Client->RetryQuery(std::move(callback)).Subscribe([my = this->SelfId(), stuff = TSharedStuff::TWeakPtr(Stuff)](const auto& future) {
            if (const auto lock = stuff.lock()) {
                if (const auto res = future.GetValue(); res.IsSuccess())
                    lock->ActorSystem->Send(my, new TEvQueryResult(res.GetResultSets()));
                else
                    lock->ActorSystem->Send(my, new TEvQueryError(res.GetIssues()));
            }
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
                changes.emplace_back(std::move(key), WithPrevious && buff.first ? std::move(*buff.first) : TData(), data.Version > 0LL ? TData(data) : TData());

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
        auto total = std::accumulate(changes.cbegin(), changes.cend(), 0ULL,
            [](size_t size, const TChange& change) { return size + change.Key.size() + change.OldData.Value.size() + change.NewData.Value.size(); });

        if (!changes.empty()) {
            while (total > DataSizeLimit) {
                size_t size = 0U;
                auto it = changes.begin();
                for (; size < DataSizeLimit && changes.end() != it; ++it)
                    size += it->Key.size() + it->OldData.Value.size() + it->NewData.Value.size();

                if (changes.end() == it)
                    break;

                std::vector<TChange> part;
                part.reserve(std::distance(changes.begin(), it));
                std::move(changes.begin(), it, std::back_inserter(part));
                changes.erase(changes.begin(), it);
                total -= size;
                ctx.Send(Watchman, new TEvChanges(Id, std::move(part)));
            }
            ctx.Send(Watchman, new TEvChanges(Id, std::move(changes)));
            TimeOfLastSent = TMonotonic::Now();
        }
        AwaitHistory = false;
        ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
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
            else if (ev->Get()->NewData.Modified >= FromRevision)
                ctx.Send(Watchman, new TEvChanges(Id, {TChange(std::move(ev->Get()->Key), WithPrevious && ev->Get()->OldData.Version ? std::move(ev->Get()->OldData) : TData(), std::move(ev->Get()->NewData))}));
        }
    }

    void UnsubscribeAndDie(const TActorContext& ctx) {
        ctx.Send(Watchtower, new TEvSubscribe);
        return Die(ctx);
    }

    const i64 Id;
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

            Watches.emplace(watchId, ctx.RegisterWithSameMailbox(new TWatch(watchId, Watchtower, ctx.SelfID, Stuff, std::move(key), std::move(rangeEnd), kind, revision, withPrevious, makeFragmets, sendProgress)));
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
        if (const auto watchId = ev->Get()->Id)
            response.set_watch_id(watchId);

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
            }

            std::cout << (change.NewData.Version ? change.OldData.Version ? "Updated" : "Created" : "Deleted") << '(' << change.Key;
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

    void Bootstrap(const TActorContext&) {
        Become(&TThis::StateFunc);

        TReadSessionSettings settings;
        settings.WithoutConsumer().ReadFromTimestamp(TInstant::Now()).AppendTopics(TTopicReadSettings(Stuff->Folder + "/current/changes").AppendPartitionIds(0ULL));
        settings.EventHandlers_.SimpleDataHandlers(
        [my = this->SelfId(), stuff = TSharedStuff::TWeakPtr(Stuff)](TReadSessionEvent::TDataReceivedEvent& event) {
            if (const auto lock = stuff.lock()) {
                NJson::TJsonReaderConfig config;
                config.MaxDepth = 3UL;

                for (const auto& message : event.GetMessages()) {
                    if (NJson::TJsonValue v; NJson::ReadJsonTree(message.GetData(), &config, &v)) try {
                        TData oldData, newData;
                        if (v.Has("oldImage")) {
                            const auto& map = v["oldImage"];
                            oldData.Version = map["version"].GetIntegerSafe();
                            oldData.Created = map["created"].GetIntegerSafe();
                            oldData.Modified = map["modified"].GetIntegerSafe();
                            oldData.Lease = map["lease"].GetIntegerSafe();
                            oldData.Value = Base64Decode(map["value"].GetStringSafe());
                        }

                        if (v.Has("newImage")) {
                            const auto& map = v["newImage"];
                            newData.Version = map["version"].GetIntegerSafe();
                            newData.Created = map["created"].GetIntegerSafe();
                            newData.Modified = map["modified"].GetIntegerSafe();
                            newData.Lease = map["lease"].GetIntegerSafe();
                            newData.Value = Base64Decode(map["value"].GetStringSafe());
                        }

                        lock->ActorSystem->Send(my, new TEvChange(Base64Decode(v["key"].Back().GetStringSafe()), std::move(oldData), std::move(newData)));
                    } catch (const NJson::TJsonException& ex) {
                        std::cout << "Error on parsing json: " << ex.what() << std::endl;
                    } else {
                        std::cout << "Invalid message format." << std::endl;
                    }
                }
            }
        }, false, false);

        ReadSession = Stuff->TopicClient->CreateReadSession(settings);
        Y_ABORT_UNLESS(ReadSession);
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

            HFunc(TEvSubscribe, Handle);
            HFunc(TEvChange, Handle);
        }
    }

    void Handle(TEvWatchRequest::TPtr& ev, const TActorContext& ctx) {
        ctx.RegisterWithSameMailbox(new TWatchman(ev->Get()->GetStreamCtx(), ctx.SelfID, Stuff));
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

    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    const TSharedStuff::TPtr Stuff;

    std::shared_ptr<IReadSession> ReadSession;

    TWatchmanSubscriptionsMap WatchmanSubscriptionsMap;

    TByExactKeyMap ByExactKeyMap;
    TByKeyPrefixMap ByKeyPrefixMap;

    size_t MinSizeOfPrefix = 0U;
};

}

NActors::IActor* BuildWatchtower(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TSharedStuff::TPtr stuff) {
    return new TWatchtower(std::move(counters), std::move(stuff));

}

}
