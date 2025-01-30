#include "etcd_watch.h"
#include "etcd_shared.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NEtcd {

using namespace NActors;

namespace {
class TWatchman : public TActorBootstrapped<TWatchman> {
public:
    using IStreamCtx = NKikimr::NGRpcServer::IGRpcStreamingContext<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>;

    TWatchman(TIntrusivePtr<IStreamCtx> ctx)
        : Ctx(std::move(ctx))
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
//            HFunc(TEvents::TEvWakeup, Handle);

            HFunc(IStreamCtx::TEvReadFinished, Handle);
            HFunc(IStreamCtx::TEvWriteFinished, Handle);
            HFunc(IStreamCtx::TEvNotifiedWhenDone, Handle);
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
    }

    void Create(const etcdserverpb::WatchCreateRequest& req, etcdserverpb::WatchResponse& res, const TActorContext& ctx)  {
        Key = req.key();
        RangeEnd = NEtcd::DecrementKey(req.range_end());
        FromRevision = req.start_revision();
        WithPrevious = req.prev_kv();
        WatchId = req.watch_id();
        MakeFragmets = req.fragment();
        SendProgress = req.progress_notify();
        for (const auto f : req.filters()) {
            switch (f) {
                case etcdserverpb::WatchCreateRequest_FilterType_NOPUT: IgnoreUpdate = true; break;
                case etcdserverpb::WatchCreateRequest_FilterType_NODELETE: IgnoreDelete = false; break;
                default: break;
            }
        }

        Cerr << __func__ << '(' << Key << ',' << RangeEnd << ',' << FromRevision<< ',' << WatchId << ',' << WithPrevious<< ',' << IgnoreUpdate<< ',' << IgnoreDelete<< ',' << MakeFragmets << ',' << SendProgress << ')' << Endl;

        res.set_created(true);
        if (WatchId)
            res.set_watch_id(WatchId);

        if (!Ctx->Write(std::move(res)))
            return Die(ctx);
    }

    void Cancel(const etcdserverpb::WatchCancelRequest&, etcdserverpb::WatchResponse& res, const TActorContext& ctx)  {
        Cerr << __func__  << Endl;
        res.set_canceled(true);
        if (WatchId)
            res.set_watch_id(WatchId);

        if (!Ctx->Write(std::move(res)))
            return Die(ctx);
    }

    void Progress(const etcdserverpb::WatchProgressRequest&, etcdserverpb::WatchResponse& res, const TActorContext& ctx)  {
        if (WatchId)
            res.set_watch_id(WatchId);

        AddEvents(res);

        if (!Ctx->Write(std::move(res)))
            return Die(ctx);
    }

    void AddEvents(etcdserverpb::WatchResponse& res) {
        const auto event = res.add_events();
        event->set_type(mvccpb::Event_EventType_PUT);

        const auto kv = event->mutable_kv();
        kv->set_key("New Key !!1!");
        kv->set_value("New value");
        kv->set_mod_revision(789);
        kv->set_create_revision(123);
        kv->set_version(1);
        kv->set_lease(12);

        if (WithPrevious) {
            const auto kv = event->mutable_prev_kv();
            kv->set_key("Old Key !!1!");
            kv->set_value("Old value");
            kv->set_mod_revision(123);
            kv->set_create_revision(123);
            kv->set_version(1);
            kv->set_lease(42);
        }
    }

    void Handle(IStreamCtx::TEvWriteFinished::TPtr& ev, const TActorContext& ctx) {
        if (!ev->Get()->Success)
            return Die(ctx);

        if (!Ctx->Read())
            return Die(ctx);
    }

    void Handle(IStreamCtx::TEvNotifiedWhenDone::TPtr& ev, const TActorContext& ctx) {
        Cerr << (ev->Get()->Success ? "Finished." : "Failed!") << Endl;
        return Die(ctx);
    }

    const TIntrusivePtr<IStreamCtx> Ctx;

    TString Key, RangeEnd;
    i64 FromRevision = 0LL;
    i64 WatchId = 0LL;
    bool SendProgress = false;
    bool WithPrevious = false;
    bool IgnoreUpdate = false;
    bool IgnoreDelete = false;
    bool MakeFragmets = false;
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
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NKikimr::NGRpcService::TEvWatchRequest, Handle);
        }
    }

    void Handle(NKikimr::NGRpcService::TEvWatchRequest::TPtr& ev, const TActorContext& ctx) {
        ctx.RegisterWithSameMailbox(new TWatchman(ev->Get()->ReleaseStreamCtx()));
    }

    const TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
};

}

NActors::IActor* CreateEtcdWatchtower(TIntrusivePtr<NMonitoring::TDynamicCounters> counters) {
    return new TWatchtower(std::move(counters));

}

}

