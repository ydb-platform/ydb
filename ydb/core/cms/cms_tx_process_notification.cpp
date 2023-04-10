#include "cms_impl.h"
#include "scheme.h"

#include <google/protobuf/text_format.h>

namespace NKikimr::NCms {

class TCms::TTxProcessNotification : public TTransactionBase<TCms> {
public:
    TTxProcessNotification(TCms *self, TEvCms::TEvNotification::TPtr ev)
        : TBase(self)
        , Event(ev)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_PROCESS_NOTIFICATION; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxProcessNotification Execute");

        auto &rec = Event->Get()->Record;
        Response = new TEvCms::TEvNotificationResponse;

        LOG_INFO(ctx, NKikimrServices::CMS, "Processing notification from %s (time=%s reason='%s')",
                  rec.GetUser().data(), TInstant::MicroSeconds(rec.GetTime()).ToStringLocalUpToSeconds().data(),
                  rec.GetReason().data());

        if (Self->CheckNotification(rec, Response->Record, ctx)) {
            TString id = Self->AcceptNotification(rec, ctx);
            Response->Record.SetNotificationId(id);

            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::Param>().Key(1)
                .Update(NIceDb::TUpdate<Schema::Param::NextNotificationID>(Self->State->NextNotificationId));

            TString notificationStr;
            google::protobuf::TextFormat::PrintToString(rec, &notificationStr);

            auto row = db.Table<Schema::Notification>().Key(id);
            row.Update(NIceDb::TUpdate<Schema::Notification::Owner>(rec.GetUser()),
                       NIceDb::TUpdate<Schema::Notification::NotificationProto>(notificationStr));

            Self->AuditLog(ctx, TStringBuilder() << "Store notification"
                << ": id# " << id
                << ", body# " << notificationStr);
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::CMS, "TTxProcessNotification complete with response: %s",
                  Response->Record.ShortDebugString().data());

        Self->Reply(Event, std::move(Response), ctx);
        Self->ScheduleNotificationsCleanup(ctx);
    }

private:
    TEvCms::TEvNotification::TPtr Event;
    TAutoPtr<TEvCms::TEvNotificationResponse> Response;
};

ITransaction *TCms::CreateTxProcessNotification(TEvCms::TEvNotification::TPtr &ev) {
    return new TTxProcessNotification(this, ev.Release());
}

} // namespace NKikimr::NCms
