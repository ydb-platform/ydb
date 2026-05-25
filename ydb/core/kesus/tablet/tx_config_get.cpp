#include "tablet_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KESUS_TABLET

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxConfigGet : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;

    THolder<TEvKesus::TEvGetConfigResult> Reply;

    TTxConfigGet(TSelf* self, const TActorId& sender, ui64 cookie)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
    {}

    TTxType GetTxType() const override { return TXTYPE_CONFIG_GET; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "] TTxConfigGet::Execute",
            {"TabletID", Self->TabletID()},
            {"(sender", Sender},
            {"cookie", Cookie});

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistStrictMarker(db);

        Reply.Reset(new TEvKesus::TEvGetConfigResult());
        auto* config = Reply->Record.MutableConfig();
        config->set_path(Self->KesusPath); // TODO: remove legacy field eventually
        config->set_self_check_period_millis(Self->SelfCheckPeriod.MilliSeconds());
        config->set_session_grace_period_millis(Self->SessionGracePeriod.MilliSeconds());
        config->set_read_consistency_mode(Self->ReadConsistencyMode);
        config->set_attach_consistency_mode(Self->AttachConsistencyMode);
        config->set_rate_limiter_counters_mode(Self->RateLimiterCountersMode);
        Reply->Record.SetVersion(Self->ConfigVersion);
        Reply->Record.SetPath(Self->KesusPath);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "] TTxConfigGet::Complete",
            {"TabletID", Self->TabletID()},
            {"(sender", Sender},
            {"cookie", Cookie});
        Y_ABORT_UNLESS(Reply);
        ctx.Send(Sender, Reply.Release(), 0, Cookie);
    }
};

void TKesusTablet::Handle(TEvKesus::TEvGetConfig::TPtr& ev) {
    Execute(new TTxConfigGet(this, ev->Sender, ev->Cookie), TActivationContext::AsActorContext());
}

}
}
