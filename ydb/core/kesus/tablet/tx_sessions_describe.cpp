#include "tablet_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KESUS_TABLET

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxSessionsDescribe : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;

    THolder<TEvKesus::TEvDescribeSessionsResult> Reply;

    TTxSessionsDescribe(TSelf* self, const TActorId& sender, ui64 cookie)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
    {}

    TTxType GetTxType() const override { return TXTYPE_SESSIONS_DESCRIBE; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "] TTxSessionsDescribe::Execute",
            {"TabletID", Self->TabletID()},
            {"(sender", Sender},
            {"cookie", Cookie});

        NIceDb::TNiceDb db(txc.DB);
        if (Self->UseStrictRead()) {
            Self->PersistStrictMarker(db);
        }

        Reply.Reset(new TEvKesus::TEvDescribeSessionsResult());
        for (const auto& kv : Self->Sessions) {
            const auto* session = &kv.second;
            auto* sessionInfo = Reply->Record.AddSessions();
            sessionInfo->SetSessionId(session->Id);
            sessionInfo->SetTimeoutMillis(session->TimeoutMillis);
            sessionInfo->SetDescription(session->Description);
            if (session->OwnerProxy) {
                ActorIdToProto(session->OwnerProxy->ActorID, sessionInfo->MutableOwnerProxy());
            }
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "] TTxSessionsDescribe::Complete",
            {"TabletID", Self->TabletID()},
            {"(sender", Sender},
            {"cookie", Cookie});

        Y_ABORT_UNLESS(Reply);
        ctx.Send(Sender, Reply.Release(), 0, Cookie);
    }
};

void TKesusTablet::Handle(TEvKesus::TEvDescribeSessions::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    VerifyKesusPath(record.GetKesusPath());

    Execute(new TTxSessionsDescribe(this, ev->Sender, ev->Cookie), TActivationContext::AsActorContext());
}

}
}
