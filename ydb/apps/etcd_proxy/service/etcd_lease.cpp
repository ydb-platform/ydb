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

    void Bootstrap(const TActorContext&) {
        Become(&TThis::StateFunc);
        TSharedStuff::Get()->Watchtower = SelfId();
    }

private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            CFunc(TEvents::TEvWakeup::EventType, Wakeup);

            HFunc(TEvLeaseKeepAliveRequest, Handle);
        }
    }

    void Handle(TEvLeaseKeepAliveRequest::TPtr& ev, const TActorContext& ctx) {
        ctx.RegisterWithSameMailbox(new TLeaseKeeper(ev->Get()->GetStreamCtx(), ctx.SelfID));
    }

    void Wakeup(const TActorContext&) {
    }

    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
};

}

NActors::IActor* CreateEtcdLeasingOffice(TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
    return new TLeasingOffice(std::move(counters));

}

}


