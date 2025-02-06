#include "etcd_lease.h"
#include "etcd_shared.h"
#include "etcd_events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/executor_thread.h>

namespace NEtcd {

using namespace NActors;

namespace {

class TLeaseKeeper : public TActorBootstrapped<TLeaseKeeper> {
public:
    using IStreamCtx = NKikimr::NGRpcServer::IGRpcStreamingContext<etcdserverpb::LeaseKeepAliveRequest, etcdserverpb::LeaseKeepAliveResponse>;

    TLeaseKeeper(TIntrusivePtr<IStreamCtx> ctx, TActorId office)
        : Ctx(std::move(ctx)), Office(office)
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
            header->set_revision(TSharedStuff::Get()->Revision.load());
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
        NEtcd::TSharedStuff::Get()->Client->ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), params.Build()).Subscribe([my, ass](const auto& future) {
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
    const TActorId Office;
};

class TLeasingOffice : public TActorBootstrapped<TLeasingOffice> {
public:
    TLeasingOffice(TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
        : Counters(std::move(counters))
    {}

    void Bootstrap(const TActorContext& ctx) {
        Become(&TThis::StateFunc);
        TSharedStuff::Get()->LeasingOffice = SelfId();
        ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
    }
private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TEvWakeup::EventType, Wakeup);

            HFunc(TEvLeaseKeepAliveRequest, Handle);

            HFunc(NEtcd::TEvQueryResult, Handle);
            HFunc(NEtcd::TEvQueryError, Handle);
        }
    }

    void Handle(TEvLeaseKeepAliveRequest::TPtr& ev, const TActorContext& ctx) {
        ctx.RegisterWithSameMailbox(new TLeaseKeeper(ev->Get()->GetStreamCtx(), ctx.SelfID));
    }

    void Wakeup(const TActorContext&) {
        TStringBuilder sql;
        NYdb::TParamsBuilder params;
        Revision = TSharedStuff::Get()->Revision.fetch_add(1LL);
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
        NEtcd::TSharedStuff::Get()->Client->ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), params.Build()).Subscribe([my, ass](const auto& future) {
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

                ctx.Send(NEtcd::TSharedStuff::Get()->Watchtower, std::make_unique<NEtcd::TEvChange>(std::move(key), std::move(oldData)));
            }
        } else {
            if (auto parser = NYdb::TResultSetParser(ev->Get()->Results.front()); parser.TryNextRow()) {
                deleted = NYdb::TValueParser(parser.GetValue(0)).GetUint64();
            }
        }

        if (!deleted) {
            auto expected = Revision + 1U;
            NEtcd::TSharedStuff::Get()->Revision.compare_exchange_strong(expected, Revision);
        }

        ctx.Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
    }

    void Handle(NEtcd::TEvQueryError::TPtr &ev, const TActorContext& ctx) {
        Cerr << __func__ << ' ' << ev->Get()->Issues.ToString() << Endl;
        ctx.Schedule(TDuration::Seconds(7), new TEvents::TEvWakeup);
    }

    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
    i64 Revision = 0LL;
};

}

NActors::IActor* CreateEtcdLeasingOffice(TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
    return new TLeasingOffice(std::move(counters));

}

}


