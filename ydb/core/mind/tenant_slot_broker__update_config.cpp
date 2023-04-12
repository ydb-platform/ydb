#include "tenant_slot_broker_impl.h"

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

        LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                    "TTxUpdateConfig Execute " << rec.ShortDebugString());

        if (rec.GetSubscriptionId() != Self->ConfigSubscriptionId) {
            LOG_ERROR_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                        "Config subscription id mismatch (" << rec.GetSubscriptionId()
                        << " vs expected " << Self->ConfigSubscriptionId << ")");
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        TString config;
        Y_PROTOBUF_SUPPRESS_NODISCARD rec.GetConfig().GetTenantSlotBrokerConfig().SerializeToString(&config);
        db.Table<Schema::Config>().Key(ConfigKey_Config)
            .Update(NIceDb::TUpdate<Schema::Config::Value>(config));

        Modify = true;

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::TENANT_SLOT_BROKER, "TTxUpdateConfig Complete");

        if (Modify) {
            auto &rec = Event->Get()->Record;
            Self->LoadConfigFromProto(rec.GetConfig().GetTenantSlotBrokerConfig());

            auto resp = MakeHolder<TEvConsole::TEvConfigNotificationResponse>(rec);
            ctx.Send(Event->Sender, resp.Release(), 0, Event->Cookie);
        }

        Self->TxCompleted(this, ctx);
    }

private:
    TEvConsole::TEvConfigNotificationRequest::TPtr Event;
    bool Modify;
};

ITransaction *TTenantSlotBroker::CreateTxUpdateConfig(TEvConsole::TEvConfigNotificationRequest::TPtr &ev)
{
    return new TTxUpdateConfig(this, std::move(ev));
}

} // NTenantSlotBroker
} // NKikimr
