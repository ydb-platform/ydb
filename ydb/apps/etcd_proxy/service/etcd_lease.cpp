#include "etcd_watch.h"
#include "etcd_shared.h"
#include "etcd_events.h"
#include "etcd_impl.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/executor_thread.h>

namespace NEtcd {

using namespace NActors;
using namespace NYdb::NQuery;

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
        auto ttlParser = NYdb::TResultSetParser(ev->Get()->Results.front());
        auto revParser = NYdb::TResultSetParser(ev->Get()->Results.back());
        if (ttlParser.TryNextRow() && revParser.TryNextRow() && 2ULL == ttlParser.ColumnsCount()) {
            etcdserverpb::LeaseKeepAliveResponse response;
            response.set_id(NYdb::TValueParser(ttlParser.GetValue(0)).GetInt64());
            response.set_ttl(NYdb::TValueParser(ttlParser.GetValue(1)).GetInt64());
            const auto header = response.mutable_header();
            header->set_revision(NYdb::TValueParser(revParser.GetValue(0)).GetInt64());

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
        sql << Stuff->TablePrefix;
        sql << "update `leases` set `updated` = CurrentUtcDatetime(`id`) where " << leasePraramName << " = `id`;" << std::endl;
        sql << "select `id`, `ttl` - unwrap(cast(CurrentUtcDatetime(`id`) - `updated` as Int64) / 1000000L) as `granted` from `leases` where " << leasePraramName << " = `id`;" << std::endl;
        sql << "select nvl(max(`revision`), 0L) from `commited`;" << std::endl;

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

class TCollector : public TActorBootstrapped<TCollector> {
public:
    TCollector(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TSharedStuff::TPtr stuff)
        : Counters(std::move(counters)), Stuff(std::move(stuff))
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateFunc);
        ctx.Schedule(TDuration::Seconds(11), new TEvents::TEvWakeup);
    }
private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvWakeup::EventType, Wakeup);

            HFunc(TEvQueryResult, Handle);
            HFunc(TEvQueryError, Handle);

            hFunc(TEvReturnRevision, Handle);
        }
    }

    void Handle(TEvLeaseKeepAliveRequest::TPtr& ev, const TActorContext& ctx) {
        ctx.RegisterWithSameMailbox(new TKeysKeeper(ev->Get()->GetStreamCtx(), Stuff));
    }

    void Wakeup() {
        Stuff->Client->ExecuteQuery(Stuff->TablePrefix + NResource::Find("leases.sql"sv), TTxControl::BeginTx().CommitTx()).Subscribe([my = this->SelfId(), stuff = TSharedStuff::TWeakPtr(Stuff)](const auto& future) {
            if (const auto lock = stuff.lock()) {
                if (const auto res = future.GetValue(); res.IsSuccess())
                    lock->ActorSystem->Send(my, new TEvQueryResult(res.GetResultSets()));
                else
                    lock->ActorSystem->Send(my, new TEvQueryError(res.GetIssues()));
            }
        });
    }

    void Handle(TEvReturnRevision::TPtr& ev) {
        Revision = ev->Get()->Revision;
        std::ostringstream sql;
        sql << Stuff->TablePrefix;
        NYdb::TParamsBuilder params;
        const auto& revName = AddParam("Revision", params, Revision);
        auto& list = params.AddParam("$Expired").BeginList();
        for (const auto lease : ExpiredLeases)
            list.AddListItem().Int64(lease);
        list.EndList().Build();

        sql << "$Leases = select `id` as `lease` from `leases` where  `id` in $Expired and unwrap(interval('PT1S') * `ttl` + `updated`) <= CurrentUtcDatetime(`id`);" << std::endl;
        sql << "$Victims = select `key`, `value`, `created`, `modified`, `version`, `lease` from `current` view `lease` as h left semi join $Leases as l using(`lease`);" << std::endl;
        sql << "insert into `history`" << std::endl;
        sql << "select `key`, `created`, " << revName << " as `modified`, 0L as `version`, `value`, `lease` from $Victims;" << std::endl;
        sql << "delete from `current` on select `key` from $Victims;" << std::endl;
        sql << "delete from `leases` where `id` in $Leases;" << std::endl;
        sql << "select count(*) from $Victims;" << std::endl;

        Stuff->Client->ExecuteQuery(sql.str(), TTxControl::BeginTx().CommitTx(), params.Build()).Subscribe([my = this->SelfId(), stuff = TSharedStuff::TWeakPtr(Stuff)](const auto& future) {
            if (const auto lock = stuff.lock()) {
                if (const auto res = future.GetValue(); res.IsSuccess())
                    lock->ActorSystem->Send(my, new TEvQueryResult(res.GetResultSets()));
                else
                    lock->ActorSystem->Send(my, new TEvQueryError(res.GetIssues()));
            }
        });
    }

    void Handle(TEvQueryResult::TPtr &ev, const TActorContext& ctx) {
        ui64 deleted = 0ULL;

        if (ExpiredLeases.empty()) {
            if (auto parser = NYdb::TResultSetParser(ev->Get()->Results.front()); parser.TryNextRow()) {
                deleted = NYdb::TValueParser(parser.GetValue(0)).GetUint64();
            }
            for (auto parser = NYdb::TResultSetParser(ev->Get()->Results.back()); parser.TryNextRow();) {
                ExpiredLeases.emplace_back(NYdb::TValueParser(parser.GetValue(0)).GetInt64());
            }
        } else {
            ExpiredLeases.clear();
            if (auto parser = NYdb::TResultSetParser(ev->Get()->Results.front()); parser.TryNextRow()) {
                deleted = NYdb::TValueParser(parser.GetValue(0)).GetUint64();
            }
        }

        if (deleted) {
            std::cout << deleted <<  " leases expired." << std::endl;
        }

        if (ExpiredLeases.empty())
            ctx.Schedule(TDuration::Seconds(3), new TEvents::TEvWakeup);
        else
            ctx.Send(Stuff->MainGate, new TEvRequestRevision);
    }

    void Handle(TEvQueryError::TPtr &ev, const TActorContext& ctx) {
        std::cout << "Check leases SQL error received " << ev->Get()->Issues.ToString() << std::endl;
        Revision = 0LL;
        ExpiredLeases.clear();
        ctx.Schedule(TDuration::Seconds(7), new TEvents::TEvWakeup);
    }

    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    const TSharedStuff::TPtr Stuff;

    i64 Revision = 0LL;
    std::vector<i64> ExpiredLeases;
};

class THolderHouse : public TActorBootstrapped<THolderHouse> {
public:
    THolderHouse(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TSharedStuff::TPtr stuff)
        : Counters(std::move(counters)), Stuff(std::move(stuff))
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateFunc);
        Stuff->HolderHouse = SelfId();
        ctx.RegisterWithSameMailbox(new TCollector(Counters, Stuff));
        ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
    }
private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvLeaseKeepAliveRequest, Handle);

            cFunc(TEvents::TEvWakeup::EventType, SendDatabaseRequestIfNeeded);

            HFunc(TEvRequestRevision, Handle);

            HFunc(TEvQueryResult, Handle);
            HFunc(TEvQueryError, Handle);
        }
    }

    void Handle(TEvLeaseKeepAliveRequest::TPtr& ev, const TActorContext& ctx) {
        ctx.RegisterWithSameMailbox(new TKeysKeeper(ev->Get()->GetStreamCtx(), Stuff));
    }

    void Handle(TEvRequestRevision::TPtr &ev, const TActorContext& ctx) {
        if (Current < Limit)
            ctx.Send(ev->Sender, new TEvReturnRevision(++Current));
        else {
            Queue.emplace(ev->Sender);
            SendDatabaseRequestIfNeeded();
        }
    }

    void SendDatabaseRequestIfNeeded() {
        if (!InFlight && Limit <= Current) {
            InFlight = true;
            const auto count = 7LL + Current - Limit + Queue.size();

            NYdb::TParamsBuilder params;
            std::ostringstream sql;
            sql << Stuff->TablePrefix;
            sql << "$Next = select `revision` as `current`, `revision` + " << AddParam<i64>("Count", params, count) << " as `limit`, CurrentUtcTimestamp(`timestamp`) as `timestamp` from `revision` where `stub`;" << std::endl;
            sql << "update `revision` on select true as `stub`, `limit` as `revision`, `timestamp` from $Next;" << std::endl;
            sql << "select `current`, `limit` from $Next;" << std::endl;

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
    }

    void Handle(TEvQueryResult::TPtr &ev, const TActorContext& ctx) {
        InFlight = false;

        if (auto parser = NYdb::TResultSetParser(ev->Get()->Results.front()); parser.TryNextRow()) {
            Current = NYdb::TValueParser(parser.GetValue("current")).GetInt64();
            Limit = NYdb::TValueParser(parser.GetValue("limit")).GetInt64();
        }

        while (!Queue.empty() && Current < Limit) {
            ctx.Send(Queue.front(), new TEvReturnRevision(++Current));
            Queue.pop();
        }

        if (Limit <= Current)
            ctx.Send(ctx.SelfID, new TEvents::TEvWakeup);
    }

    void Handle(TEvQueryError::TPtr &ev, const TActorContext& ctx) {
        InFlight = false;
        std::cout << "Request next leases SQL error received " << ev->Get()->Issues.ToString() << std::endl;
        ctx.Schedule(TDuration::Seconds(3), new TEvents::TEvWakeup);
    }

    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    const TSharedStuff::TPtr Stuff;

    i64 Current = 0LL, Limit = 0LL;
    std::queue<TActorId> Queue;
    bool InFlight = false;
};

}

NActors::IActor* BuildHolderHouse(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TSharedStuff::TPtr stuff) {
    return new THolderHouse(std::move(counters), std::move(stuff));

}

}

