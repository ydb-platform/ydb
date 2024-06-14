#include "tablet_flat_executed.h"
#include "flat_executor.h"
#include "flat_executor_counters.h"
#include <ydb/core/base/appdata.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {
namespace NTabletFlatExecutor {

using IExecutor = NFlatExecutorSetup::IExecutor;

TTabletExecutedFlat::TTabletExecutedFlat(TTabletStorageInfo *info, const TActorId &tablet, IMiniKQLFactory *factory)
    : ITablet(info, tablet)
    , Factory(factory)
    , Executor0(nullptr)
    , StartTime0(TAppData::TimeProvider->Now())
{}

IExecutor* TTabletExecutedFlat::CreateExecutor(const TActorContext &ctx) {
    if (!Executor()) {
        IActor *executor = NFlatExecutorSetup::CreateExecutor(this, ctx.SelfID);
        const TActorId executorID = ctx.RegisterWithSameMailbox(executor);
        Executor0 = dynamic_cast<TExecutor *>(executor);
        Y_ABORT_UNLESS(Executor0);

        ITablet::ExecutorActorID = executorID;
    }

    return Executor();
}

void TTabletExecutedFlat::Execute(TAutoPtr<ITransaction> transaction, const TActorContext &ctx) {
    Y_UNUSED(ctx);
    Execute(transaction);
}

void TTabletExecutedFlat::Execute(TAutoPtr<ITransaction> transaction) {
    if (transaction)
        static_cast<TExecutor*>(Executor())->Execute(transaction, ExecutorCtx(*TlsActivationContext));
}

void TTabletExecutedFlat::EnqueueExecute(TAutoPtr<ITransaction> transaction) {
    if (transaction)
        static_cast<TExecutor*>(Executor())->Enqueue(transaction, ExecutorCtx(*TlsActivationContext));
}

const NTable::TScheme& TTabletExecutedFlat::Scheme() const noexcept {
    return static_cast<TExecutor*>(Executor())->Scheme();
}

void TTabletExecutedFlat::Handle(TEvTablet::TEvBoot::TPtr &ev, const TActorContext &ctx) {
    // Notify sys tablet that leader supports graceful shutdown
    ctx.Send(Tablet(), new TEvTablet::TEvFeatures(TEvTablet::TEvFeatures::GracefulStop));

    const auto& msg = *ev->Get();
    UpdateTabletInfo(msg.TabletStorageInfo, msg.Launcher);
    CreateExecutor(ctx)->Boot(ev, ExecutorCtx(ctx));
    TxCacheQuota = ev->Get()->TxCacheQuota;
}

void TTabletExecutedFlat::Handle(TEvTablet::TEvRestored::TPtr &ev, const TActorContext &ctx) {
    Executor()->Restored(ev, ExecutorCtx(ctx));
}

void TTabletExecutedFlat::Handle(TEvTablet::TEvFBoot::TPtr &ev, const TActorContext &ctx) {
    UpdateTabletInfo(ev->Get()->TabletStorageInfo);
    CreateExecutor(ctx)->FollowerBoot(ev, ExecutorCtx(ctx));
    TxCacheQuota = ev->Get()->TxCacheQuota;
}

void TTabletExecutedFlat::Handle(TEvTablet::TEvFUpdate::TPtr &ev) {
    Executor()->FollowerUpdate(std::move(ev->Get()->Update));
}

void TTabletExecutedFlat::Handle(TEvTablet::TEvFAuxUpdate::TPtr &ev) {
    Executor()->FollowerAuxUpdate(std::move(ev->Get()->AuxUpdate));
}

void TTabletExecutedFlat::Handle(TEvTablet::TEvNewFollowerAttached::TPtr &ev) {
    if (Executor())
        Executor()->FollowerAttached(ev->Get()->TotalFollowers);
}

void TTabletExecutedFlat::Handle(TEvTablet::TEvFollowerDetached::TPtr &ev) {
    if (Executor())
        Executor()->FollowerDetached(ev->Get()->TotalFollowers);
}

void TTabletExecutedFlat::Handle(TEvTablet::TEvFollowerSyncComplete::TPtr &ev) {
    Y_UNUSED(ev);
    if (Executor())
        Executor()->FollowerSyncComplete();
}

void TTabletExecutedFlat::Handle(TEvTablet::TEvFollowerGcApplied::TPtr &ev) {
    auto *msg = ev->Get();
    Executor()->FollowerGcApplied(msg->Step, msg->FollowerSyncDelay);
}

void TTabletExecutedFlat::Handle(TEvTablet::TEvUpdateConfig::TPtr &ev) {
    if (Executor())
        Executor()->UpdateConfig(ev);
}

void TTabletExecutedFlat::OnTabletStop(TEvTablet::TEvTabletStop::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ev);
    // Default implementation just confirms it's ok to be stopped
    ctx.Send(Tablet(), new TEvTablet::TEvTabletStopped());
}

void TTabletExecutedFlat::HandlePoison(const TActorContext &ctx) {
    if (Executor0) {
        Executor0->DetachTablet(ExecutorCtx(ctx));
        Executor0 = nullptr;
    }

    Detach(ctx);
}

void TTabletExecutedFlat::HandleTabletStop(TEvTablet::TEvTabletStop::TPtr &ev, const TActorContext &ctx) {
    if (Executor() && Executor()->GetStats().IsActive) {
        OnTabletStop(ev, ctx);
    } else {
        ctx.Send(Tablet(), new TEvTablet::TEvTabletStopped());
    }
}

void TTabletExecutedFlat::HandleTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) {
    if (Executor0) {
        Executor0->DetachTablet(ExecutorCtx(ctx));
        Executor0 = nullptr;
    }

    OnTabletDead(ev, ctx);
}

void TTabletExecutedFlat::HandleLocalMKQL(TEvTablet::TEvLocalMKQL::TPtr &ev, const TActorContext &ctx) {
    Y_ABORT_UNLESS(Factory, "Need IMiniKQLFactory to execute MKQL query");

    Execute(Factory->Make(ev), ctx);
}

void TTabletExecutedFlat::HandleLocalSchemeTx(TEvTablet::TEvLocalSchemeTx::TPtr &ev, const TActorContext &ctx) {
    Y_ABORT_UNLESS(Factory, "Need IMiniKQLFactory to execute scheme query");

    Execute(Factory->Make(ev), ctx);
}

void TTabletExecutedFlat::HandleLocalReadColumns(TEvTablet::TEvLocalReadColumns::TPtr &ev, const TActorContext &ctx) {
    Y_ABORT_UNLESS(Factory, "Need IMiniKQLFactory to execute read columns query");

    Execute(Factory->Make(ev), ctx);
}

void TTabletExecutedFlat::SignalTabletActive(const TActorIdentity &id) {
    id.Send(Tablet(), new TEvTablet::TEvTabletActive());
}

void TTabletExecutedFlat::SignalTabletActive(const TActorContext &ctx) {
    ctx.Send(Tablet(), new TEvTablet::TEvTabletActive());
}

void TTabletExecutedFlat::Enqueue(STFUNC_SIG) {
    Y_UNUSED(ev);
    Y_DEBUG_ABORT("Unhandled StateInit event 0x%08" PRIx32, ev->GetTypeRewrite());
}

void TTabletExecutedFlat::ActivateExecutor(const TActorContext &ctx) {
    OnActivateExecutor(ctx);
}

void TTabletExecutedFlat::Detach(const TActorContext &ctx) {
    Executor0 = nullptr;
    ctx.Send(Tablet(), new TEvents::TEvPoison());
    OnDetach(ctx);
}

bool TTabletExecutedFlat::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext &ctx) {
    if (ev) {
        TStringStream str;
        HTML(str) {str << "nothing to see here...";}
        ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
    }
    return false;
}

void TTabletExecutedFlat::RenderHtmlPage(NMon::TEvRemoteHttpInfo::TPtr &ev, const TActorContext &ctx) {
    LOG_NOTICE_S(ctx, NKikimrServices::TABLET_EXECUTOR, "RenderHtmlPage for tablet " << TabletID());
    auto cgi = ev->Get()->Cgi();
    auto path = ev->Get()->PathInfo();
    TString queryString = cgi.Print();

    if (path == "/app") {
        OnRenderAppHtmlPage(ev, ctx);
        return;
    } else if (path == "/executorInternals") {
        Executor()->RenderHtmlPage(ev);
        return;
    } else if (path == "/counters") {
        Executor()->RenderHtmlCounters(ev);
        return;
    } else if (path == "/db") {
        Executor()->RenderHtmlDb(ev, ExecutorCtx(ctx));
        return;
    } else {
        const TDuration uptime = TAppData::TimeProvider->Now() - StartTime0;
        TStringStream str;
        HTML(str) {
            DIV_CLASS("row") {
                DIV_CLASS("col-md-12") {str << "NodeID: " << ctx.SelfID.NodeId(); }
            }
            DIV_CLASS("row") {
                DIV_CLASS("col-md-12") {str << "Uptime: " << uptime.ToString(); }
            }
            DIV_CLASS("row") {
                DIV_CLASS("col-md-12") {str << "Tablet type: " << TTabletTypes::TypeToStr((TTabletTypes::EType)TabletType()); }
            }
            DIV_CLASS("row") {
                DIV_CLASS("col-md-12") {str << "Tablet id: " << TabletID() << (Executor()->GetStats().IsFollower ? " Follower" : " Leader"); }
            }
            DIV_CLASS("row") {
                DIV_CLASS("col-md-12") {str << "Tablet generation: " << Executor()->Generation();}
            }
            DIV_CLASS("row") {
                DIV_CLASS("col-md-12") { str << "Tenant id: " << Info()->TenantPathId; }
            }

            if (OnRenderAppHtmlPage(nullptr, ctx)) {
                DIV_CLASS("row") {
                    DIV_CLASS("col-md-12") {str << "<a href=\"tablets/app?" << queryString << "\">App</a>";}
                }
            }

            DIV_CLASS("row") {
                DIV_CLASS("col-md-12") {str << "<a href=\"tablets/counters?" << queryString << "\">Counters</a>"; }
            }
            DIV_CLASS("row") {
                DIV_CLASS("col-md-12") {str << "<a href=\"tablets/executorInternals?" << queryString << "\">Executor DB internals</a>";}
            }
            DIV_CLASS("row") {
                DIV_CLASS("col-md-12") {str << "<a href=\"tablets?FollowerID=" << TabletID() << "\">Connect to follower</a>";}
            }
            DIV_CLASS("row") {
                DIV_CLASS("col-md-12") {str << "<a href=\"tablets?SsId=" << TabletID() << "\">State Storage</a>";}
            }
            DIV_CLASS("row") {
                DIV_CLASS("col-md-12") {str << "<a href=\"tablets?RestartTabletID=" << TabletID() << "\">Restart</a>";}
            }
        }

        ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(str.Str()));
        return;
    }
}

void TTabletExecutedFlat::HandleGetCounters(TEvTablet::TEvGetCounters::TPtr &ev) {
    Executor()->GetTabletCounters(ev);
}

bool TTabletExecutedFlat::HandleDefaultEvents(TAutoPtr<IEventHandle>& ev, const TActorIdentity& id) {
    auto ctx(NActors::TActivationContext::ActorContextFor(id));
    switch (ev->GetTypeRewrite()) {
        CFuncCtx(TEvents::TEvPoison::EventType, HandlePoison, ctx);
        HFuncCtx(TEvTablet::TEvBoot, Handle, ctx);
        HFuncCtx(TEvTablet::TEvRestored, Handle, ctx);
        HFuncCtx(TEvTablet::TEvFBoot, Handle, ctx);
        hFunc(TEvTablet::TEvFUpdate, Handle);
        hFunc(TEvTablet::TEvFAuxUpdate, Handle);
        hFunc(TEvTablet::TEvFollowerGcApplied, Handle);
        hFunc(TEvTablet::TEvNewFollowerAttached, Handle);
        hFunc(TEvTablet::TEvFollowerDetached, Handle);
        hFunc(TEvTablet::TEvFollowerSyncComplete, Handle);
        HFuncCtx(TEvTablet::TEvTabletStop, HandleTabletStop, ctx);
        HFuncCtx(TEvTablet::TEvTabletDead, HandleTabletDead, ctx);
        HFuncCtx(TEvTablet::TEvLocalMKQL, HandleLocalMKQL, ctx);
        HFuncCtx(TEvTablet::TEvLocalSchemeTx, HandleLocalSchemeTx, ctx);
        HFuncCtx(TEvTablet::TEvLocalReadColumns, HandleLocalReadColumns, ctx);
        hFunc(TEvTablet::TEvGetCounters, HandleGetCounters);
        hFunc(TEvTablet::TEvUpdateConfig, Handle);
        HFuncCtx(NMon::TEvRemoteHttpInfo, RenderHtmlPage, ctx);
        IgnoreFunc(TEvTablet::TEvReady);
    default:
        return false;
    }
    return true;
}

void TTabletExecutedFlat::StateInitImpl(TAutoPtr<IEventHandle>& ev, const TActorIdentity& id) {
    auto ctx(NActors::TActivationContext::ActorContextFor(id));
    switch (ev->GetTypeRewrite()) {
        CFuncCtx(TEvents::TEvPoison::EventType, HandlePoison, ctx);
        HFuncCtx(TEvTablet::TEvBoot, Handle, ctx);
        HFuncCtx(TEvTablet::TEvRestored, Handle, ctx);
        HFuncCtx(TEvTablet::TEvFBoot, Handle, ctx);
        hFunc(TEvTablet::TEvFUpdate, Handle);
        hFunc(TEvTablet::TEvFAuxUpdate, Handle);
        hFunc(TEvTablet::TEvFollowerGcApplied, Handle);
        hFunc(TEvTablet::TEvNewFollowerAttached, Handle);
        hFunc(TEvTablet::TEvFollowerDetached, Handle);
        hFunc(TEvTablet::TEvFollowerSyncComplete, Handle);
        HFuncCtx(TEvTablet::TEvTabletStop, HandleTabletStop, ctx);
        HFuncCtx(TEvTablet::TEvTabletDead, HandleTabletDead, ctx);
        hFunc(TEvTablet::TEvUpdateConfig, Handle);
        HFuncCtx(NMon::TEvRemoteHttpInfo, RenderHtmlPage, ctx);
        IgnoreFunc(TEvTablet::TEvReady);
    default:
        return Enqueue(ev);
    }
}

}}
