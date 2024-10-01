#include "cms_impl.h"
#include "scheme.h"

#include <google/protobuf/text_format.h>

namespace NKikimr::NCms {

class TCms::TTxRejectNotification : public TTransactionBase<TCms> {
public:
    TTxRejectNotification(TCms *self, TEvCms::TEvManageNotificationRequest::TPtr ev)
        : TBase(self)
        , Event(ev)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_REJECT_NOTIFICATION; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxRejectNotification Execute");

        auto &rec = Event->Get()->Record;
        Response = new TEvCms::TEvManageNotificationResponse;

        auto &id = rec.GetNotificationId();
        const TString &user = rec.GetUser();
        bool dry = rec.GetDryRun();

        TErrorInfo error;
        if (Self->RemoveNotification(id, user, !dry, error)) {
            if (!dry) {
                Self->AuditLog(ctx, TStringBuilder() << "Remove notification"
                    << ": id# " << id
                    << ", reason# " << "explicit remove");

                NIceDb::TNiceDb db(txc.DB);
                db.Table<Schema::Notification>().Key(id).Delete();
            }

            Response->Record.MutableStatus()->SetCode(NKikimrCms::TStatus::OK);
        } else {
            Response->Record.MutableStatus()->SetCode(error.Code);
            Response->Record.MutableStatus()->SetReason(error.Reason);
        }

        LOG_INFO(ctx, NKikimrServices::CMS, "Response status: %s %s",
                  ToString(Response->Record.GetStatus().GetCode()).data(),
                  Response->Record.GetStatus().GetReason().data());

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxRejectNotification Complete");

        Self->Reply(Event, std::move(Response), ctx);
    }

private:
    TEvCms::TEvManageNotificationRequest::TPtr Event;
    TAutoPtr<TEvCms::TEvManageNotificationResponse> Response;
};

ITransaction *TCms::CreateTxRejectNotification(TEvCms::TEvManageNotificationRequest::TPtr &ev) {
    return new TTxRejectNotification(this, ev.Release());
}

} // namespace NKikimr::NCms
