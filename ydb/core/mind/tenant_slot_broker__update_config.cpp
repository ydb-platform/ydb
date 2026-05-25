#include "tenant_slot_broker_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TENANT_SLOT_BROKER

namespace NKikimr {
namespace NTenantSlotBroker {

class TTenantSlotBroker::TTxUpdateConfig : public TTransactionBase<TTenantSlotBroker> {
public:
    TTxUpdateConfig(TTenantSlotBroker *self, TEvConsole::TEvConfigNotificationRequest::TPtr ev)
        : TBase(self)
        , Event(std::move(ev))
        , Modify(false)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        auto &rec = Event->Get()->Record;

        YDB_LOG_CTX_DEBUG(ctx, "TTxUpdateConfig Execute",
            {"ShortDebugString", rec.ShortDebugString()});

        NIceDb::TNiceDb db(txc.DB);

        const auto &config = Event->Get()->GetConfig().GetTenantSlotBrokerConfig();

        if (!google::protobuf::util::MessageDifferencer::Equals(config, Self->Config)) {
            TString serializedConfig = config.SerializeAsString();
            db.Table<Schema::Config>().Key(ConfigKey_Config)
                .Update(NIceDb::TUpdate<Schema::Config::Value>(serializedConfig));

            Modify = true;
        }

        auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);
        Response = new IEventHandle(Event->Sender, Self->SelfId(), resp.Release(),
                                        0, Event->Cookie);

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::TENANT_SLOT_BROKER, "TTxUpdateConfig Complete");

        if (Modify) {
            auto &rec = Event->Get()->Record;
            Self->LoadConfigFromProto(rec.GetConfig().GetTenantSlotBrokerConfig());
        }

        if (Response)
            ctx.Send(Response);

        Self->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvConfigNotificationRequest::TPtr Event;
    TAutoPtr<IEventHandle> Response;
    bool Modify;
};

ITransaction *TTenantSlotBroker::CreateTxUpdateConfig(TEvConsole::TEvConfigNotificationRequest::TPtr &ev)
{
    return new TTxUpdateConfig(this, std::move(ev));
}

} // NTenantSlotBroker
} // NKikimr
