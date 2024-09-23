#pragma once
#include "defs.h"
#include "tablet_flat_executor.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

class TExecutor;

struct IMiniKQLFactory {
    virtual ~IMiniKQLFactory() = default;

    virtual TAutoPtr<ITransaction> Make(TEvTablet::TEvLocalMKQL::TPtr&) = 0;
    virtual TAutoPtr<ITransaction> Make(TEvTablet::TEvLocalSchemeTx::TPtr&) = 0;
    virtual TAutoPtr<ITransaction> Make(TEvTablet::TEvLocalReadColumns::TPtr&) = 0;
};

class TTabletExecutedFlat : public NFlatExecutorSetup::ITablet {
protected:
    using IExecutor = NFlatExecutorSetup::IExecutor;

    TTabletExecutedFlat(TTabletStorageInfo *info, const TActorId &tablet, IMiniKQLFactory *factory);
    IExecutor* Executor() const { return Executor0; }
    const TInstant StartTime() const { return StartTime0; }

    void Execute(TAutoPtr<ITransaction> transaction, const TActorContext &ctx);
    void Execute(TAutoPtr<ITransaction> transaction);
    void EnqueueExecute(TAutoPtr<ITransaction> transaction);

    const NTable::TScheme& Scheme() const noexcept;

    TActorContext ExecutorCtx(const TActivationContext &ctx) {
        return TActorContext(ctx.Mailbox, ctx.ExecutorThread, ctx.EventStart, ExecutorID());
    }

    virtual void OnActivateExecutor(const TActorContext &ctx) = 0;
    virtual void OnDetach(const TActorContext &ctx) = 0;
    virtual void OnTabletStop(TEvTablet::TEvTabletStop::TPtr &ev, const TActorContext &ctx);
    virtual void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) = 0;
    virtual bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext &ctx);

    /**
     * Signal tablet as active and ready to process requests (from pipes).
     */
    void SignalTabletActive(const TActorIdentity &id);
    void SignalTabletActive(const TActorContext &ctx);

    /**
     * Must be overriden as an empty method. Previously default implementation
     * was calling SignalTabletActive, but this proved to be error prone. For
     * compatibility reasons an empty implementation is mandatory. This method
     * is never called, and will be removed in the future.
     */
    virtual void DefaultSignalTabletActive(const TActorContext &ctx) = 0;

    /**
     * Called by StateInitImpl for unhandled non-system events. Used to delay
     * processing of requests until tablet implementation is fully initialized.
     * Default implementation will abort when compiled in debug mode.
     * It is recommended to delay SignalTabletActive until tablet is ready to
     * process incoming requests instead of using Enqueue.
     */
    virtual void Enqueue(STFUNC_SIG);

    void Handle(TEvTablet::TEvBoot::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTablet::TEvRestored::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTablet::TEvFollowerSyncComplete::TPtr&);
    void Handle(TEvTablet::TEvFBoot::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTablet::TEvFUpdate::TPtr&);
    void Handle(TEvTablet::TEvFAuxUpdate::TPtr&);
    void Handle(TEvTablet::TEvFollowerGcApplied::TPtr&);
    void Handle(TEvTablet::TEvNewFollowerAttached::TPtr&);
    void Handle(TEvTablet::TEvFollowerDetached::TPtr&);
    void Handle(TEvTablet::TEvUpdateConfig::TPtr&);

    /**
     * Common handler for TEvPoison, detaches from executor and calls Detach,
     * which is expected to Die/PassAway in OnDetach.
     */
    void HandlePoison(const TActorContext &ctx);

    void HandleTabletStop(TEvTablet::TEvTabletStop::TPtr &ev, const TActorContext &ctx);
    void HandleTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx);
    void HandleLocalMKQL(TEvTablet::TEvLocalMKQL::TPtr &ev, const TActorContext &ctx);
    void HandleLocalSchemeTx(TEvTablet::TEvLocalSchemeTx::TPtr &ev, const TActorContext &ctx);
    void HandleLocalReadColumns(TEvTablet::TEvLocalReadColumns::TPtr &ev, const TActorContext &ctx);
    void HandleGetCounters(TEvTablet::TEvGetCounters::TPtr &ev);

    void StateInitImpl(TAutoPtr<IEventHandle>&, const TActorIdentity&);

    void ActivateExecutor(const TActorContext &ctx) override; // executor is active after this point
    void Detach(const TActorContext &ctx) override; // executor is dead after this point

    bool HandleDefaultEvents(TAutoPtr<IEventHandle>&, const TActorIdentity&);
    virtual void RenderHtmlPage(NMon::TEvRemoteHttpInfo::TPtr&, const TActorContext &ctx);

    bool TryCaptureTxCache(ui64 size) {
        if (!TxCacheQuota)
            return false;
        return TxCacheQuota->TryCaptureQuota(size);
    }
    void ReleaseTxCache(ui64 size) {
        if (size)
            TxCacheQuota->ReleaseQuota(size);
    }

private:
    IExecutor* CreateExecutor(const TActorContext &ctx);

private:
    TAutoPtr<IMiniKQLFactory> Factory;

    IExecutor *Executor0;
    TInstant StartTime0;
    TSharedQuotaPtr TxCacheQuota;
};

}}

#define STFUNC_TABLET_INIT(NAME, HANDLERS)                                                           \
    void NAME(STFUNC_SIG) {                                                                         \
        switch (const ui32 etype = ev->GetTypeRewrite()) {                                          \
            HANDLERS                                                                                \
            default:                                                                                \
                TTabletExecutedFlat::StateInitImpl(ev, SelfId());                                             \
        }                                                                                           \
    }

#define STFUNC_TABLET_DEF(NAME, HANDLERS)                                                            \
    void NAME(STFUNC_SIG) {                                                                         \
        switch (const ui32 etype = ev->GetTypeRewrite()) {                                          \
            HANDLERS                                                                                \
            default:                                                                                \
                if (!TTabletExecutedFlat::HandleDefaultEvents(ev, SelfId()))                             \
                    Y_DEBUG_ABORT("%s: unexpected event type: %" PRIx32 " event: %s",       \
                                   __func__, ev->GetTypeRewrite(),                                  \
                                   ev->ToString().data());                                          \
        }                                                                                           \
    }

#define STFUNC_TABLET_IGN(NAME, HANDLERS)                                                           \
    void NAME(STFUNC_SIG) {                                                                         \
        switch (const ui32 etype = ev->GetTypeRewrite()) {                                          \
            HANDLERS                                                                                \
            default:                                                                                \
                TTabletExecutedFlat::HandleDefaultEvents(ev, SelfId());                                  \
        }                                                                                           \
    }
