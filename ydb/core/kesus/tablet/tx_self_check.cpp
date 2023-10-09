#include "tablet_impl.h"

#include "schema.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxSelfCheck : public TTxBase {
    TSchedulerCookieHolder Cookie;

    TTxSelfCheck(TSelf* self, ISchedulerCookie* cookie)
        : TTxBase(self)
        , Cookie(cookie)
    {}

    TTxType GetTxType() const override { return TXTYPE_SELF_CHECK; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::KESUS_TABLET, "[%lu] TTxSelfCheck::Execute", Self->TabletID());
        Y_ABORT_UNLESS(Self->SelfCheckPending);

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistSysParam(db, Schema::SysParam_SelfCheckCounter, ToString(++Self->SelfCheckCounter));
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::KESUS_TABLET, "[%lu] TTxSelfCheck::Complete", Self->TabletID());
        Y_ABORT_UNLESS(Self->SelfCheckPending);
        Cookie.Detach();
        Self->SelfCheckPending = false;
        Self->ScheduleSelfCheck(ctx);
    }
};

bool TKesusTablet::ScheduleSelfCheck(const TActorContext& ctx) {
    if (SelfCheckPending || Sessions.empty()) {
        return false;
    }
    SelfCheckPending = true;
    TDuration selfCheckPeriod = Max(Min(SelfCheckPeriod, MAX_SELF_CHECK_PERIOD), MIN_SELF_CHECK_PERIOD);
    TDuration sessionGraceTimeout = Max(Min(SessionGracePeriod, MAX_SESSION_GRACE_PERIOD), selfCheckPeriod + MIN_SESSION_GRACE_PERIOD);
    TInstant deadline = ctx.Now() + sessionGraceTimeout;
    CreateLongTimer(ctx, selfCheckPeriod,
        new IEventHandle(SelfId(), SelfId(),
            new TEvPrivate::TEvSelfCheckStart(deadline)),
        AppData(ctx)->SystemPoolId);
    return true;
}

void TKesusTablet::Handle(TEvPrivate::TEvSelfCheckStart::TPtr& ev) {
    auto* msg = ev->Get();
    Y_ABORT_UNLESS(SelfCheckPending);
    const auto& ctx = TActivationContext::AsActorContext();
    TSchedulerCookieHolder cookie(ISchedulerCookie::Make3Way());
    TInstant now = ctx.Now();
    TDuration timeout = msg->Deadline > now ? msg->Deadline - now : TDuration::Zero();
    CreateLongTimer(ctx, timeout,
        new IEventHandle(SelfId(), SelfId(),
            new TEvPrivate::TEvSelfCheckTimeout(cookie.Get())),
        AppData(ctx)->SystemPoolId,
        cookie.Get());
    Execute(new TTxSelfCheck(this, cookie.Release()), ctx);
}

void TKesusTablet::Handle(TEvPrivate::TEvSelfCheckTimeout::TPtr& ev) {
    auto* msg = ev->Get();
    if (msg->Cookie.DetachEvent()) {
        // Try to die as soon as possible
        const auto& ctx = TActivationContext::AsActorContext();
        LOG_ERROR_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << TabletID() << "] Self-check timeout, attempting suicide");
        HandlePoison(TActivationContext::ActorContextFor(SelfId()));
    }
}

}
}
