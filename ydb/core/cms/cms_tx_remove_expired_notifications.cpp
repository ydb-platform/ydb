#include "cms_impl.h"
#include "scheme.h"

#include <google/protobuf/text_format.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS

namespace NKikimr::NCms {

class TCms::TTxRemoveExpiredNotifications : public TTransactionBase<TCms> {
public:
    TTxRemoveExpiredNotifications(TCms *self)
        : TBase(self)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_REMOVE_EXPIRED_NOTIFICATION; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "TTxRemoveExpiredNotifications Execute");

        TInstant now = ctx.Now();
        for (auto entry = Self->State->Notifications.begin(); entry != Self->State->Notifications.end();) {
            auto &info = entry->second;
            TInstant time = TInstant::MicroSeconds(info.Notification.GetTime());
            bool modified = false;

            auto next = entry;
            ++next;

            auto *actions = info.Notification.MutableActions();
            for (auto i = actions->begin(); i != actions->end(); ) {
                TInstant deadline = time + TDuration::MicroSeconds(i->GetDuration());

                if (deadline <= now) {
                    YDB_LOG_CTX_INFO(ctx, "Removing expired action from notification",
                        {"NotificationId", info.NotificationId.data()},
                        {"Action", i->ShortDebugString().data()});

                    i = actions->erase(i);
                    modified = true;
                } else
                    ++i;
            }

            if (actions->empty()) {
                Self->AuditLog(ctx, TStringBuilder() << "Remove notification"
                    << ": id# " << info.NotificationId
                    << ", reason# " << "scheduled cleanup");

                NIceDb::TNiceDb db(txc.DB);
                db.Table<Schema::Notification>().Key(info.NotificationId).Delete();

                Self->State->Notifications.erase(entry);
            } else if (modified) {
                TString notificationStr;
                google::protobuf::TextFormat::PrintToString(info.Notification, &notificationStr);

                NIceDb::TNiceDb db(txc.DB);
                auto row = db.Table<Schema::Notification>().Key(info.NotificationId);
                row.Update(NIceDb::TUpdate<Schema::Notification::NotificationProto>(notificationStr));

                Self->AuditLog(ctx, TStringBuilder() << "Update notification"
                    << ": id# " << info.NotificationId
                    << ", body# " << notificationStr);
            }

            entry = next;
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override {
        YDB_LOG_CTX_DEBUG(ctx, "TTxRemoveExpiredNotifications Complete");
    }
};

ITransaction *TCms::CreateTxRemoveExpiredNotifications() {
    return new TTxRemoveExpiredNotifications(this);
}

} // namespace NKikimr::NCms
